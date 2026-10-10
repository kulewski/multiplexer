"""A change of the routing or the key filters, once acknowledged, as an
event in every Python class: on_acknowledged runs once, after every
message routed to the peer before the change was handed out (received
from SyncClient, passed to ThreadedClient's on_message, delivered on
AsyncClient's loop, handled by the server classes, on their loop's
thread), and the server classes' loops wake at the acknowledgement rather
than at their next poll, an hour away here, for a drain as for a change of
filters, and a receive's wait as well when the receive before it, taking
the last message routed before the change, is what acknowledged it.
Counted, not timed: the sender's own marker coming back says the
multiplexer routed everything it sent before; a held handler says what a
threaded server still has; a bound on a wait only detects a failure.
"""

import asyncio
import threading
import unittest

from multiplexer.aio import AsyncClient
from multiplexer.clients import SyncClient
from multiplexer.Multiplexer_pb2 import Routing
from multiplexer.multiplexer_constants import peers, types
from multiplexer.mxclient import OperationTimedOut
from multiplexer.servers import BaseMultiplexerServer
from multiplexer.testing import BackendThread, Cluster, runfile, wait_until
from multiplexer.testing.raw_peer import RawPeer
from multiplexer.threaded_client import ThreadedClient
from multiplexer.threaded_server import BaseThreadedMultiplexerServer

RULES = runfile("tests/testing.rules")
EVENT = types.TEST_EVENT  # whom: ALL to TEST_EVENT_BACKEND
REQUEST = types.PYTHON_TEST_REQUEST  # whom: ANY to PYTHON_TEST_SERVER
RESPONSE = types.PYTHON_TEST_RESPONSE
EVENTS = 50
HOUR = 3600.0  # a poll no test waits out: the acknowledgement has to end the wait
BOUND = 30.0  # what only a failure reaches


def routed(sender: RawPeer) -> None:
    """Returns once the multiplexer has routed everything `sender` sent: a
    marker it addresses to itself comes back after it, since frames from
    one connection are routed in their order."""
    marker = sender.send(b"marker", EVENT, to=sender.instance_id)
    while sender.receive_type(EVENT).id != marker:
        pass


class Counter:
    """A count other threads wait for."""

    def __init__(self) -> None:
        self._changed = threading.Condition()
        self.value = 0
        self.threads: list[int] = []

    def add(self) -> None:
        """One more, from the calling thread."""
        with self._changed:
            self.value += 1
            self.threads.append(threading.get_ident())
            self._changed.notify_all()

    def wait_past(self, value: int) -> int:
        """Waits until the count is past `value`; the count."""
        with self._changed:
            assert self._changed.wait_for(lambda: self.value > value, timeout=BOUND), "nothing came"
            return self.value


class AcknowledgedTest(unittest.TestCase):
    """One multiplexer; a raw sender of events and a receiver of each class."""

    def setUp(self) -> None:
        self.cluster = Cluster(1, rules=RULES).__enter__()
        self.addCleanup(self.cluster.__exit__, None, None, None)
        self.endpoint = self.cluster.endpoints[0]

    def sender(self) -> RawPeer:
        """A raw event client, its handshake done."""
        sender = RawPeer(self.endpoint, peers.TEST_EVENT_CLIENT)
        self.addCleanup(sender.close)
        sender.handshake()
        return sender

    def send_routed(self) -> RawPeer:
        """EVENTS events, numbered, routed to every event backend there is;
        their sender."""
        sender = self.sender()
        for index in range(EVENTS):
            sender.send(b"%d" % index, EVENT)
        routed(sender)
        return sender

    def change_read_with_what_came_before(self, receiver: SyncClient, on_acknowledged) -> None:
        """EVENTS events routed to `receiver`, then a change of its routing,
        `on_acknowledged` its callback, whose confirmation the receiver reads
        with the events, before any of them is received. What the receiver
        sends after the change reaches the sender after the multiplexer took
        the change, whose confirmation is on its way to the receiver by then;
        the next send polls the receiver's sockets first, as a server's reply
        does, and reads it with the events."""
        sender = self.send_routed()
        receiver.set_routing(Routing(all=False), on_acknowledged=on_acknowledged)
        receiver.send_message(b"after", type=EVENT, to=sender.instance_id, flush=True)
        while sender.receive_type(EVENT).message != b"after":
            pass
        receiver.send_message(b"poll", type=EVENT, to=sender.instance_id, flush=True)

    def test_sync_client_hears_it_in_the_receive_after_everything_received(self) -> None:
        receiver = SyncClient([self.endpoint], peers.TEST_EVENT_BACKEND)  # connected when made
        self.addCleanup(receiver.shutdown)
        received: list[bytes] = []
        heard: list[int] = []
        self.change_read_with_what_came_before(receiver, lambda: heard.append(len(received)))
        while len(received) < EVENTS:  # every one routed before the change, then nothing
            received.append(receiver.receive_message(timeout=BOUND)[0].message)
            self.assertEqual([], heard, "not while a message routed before was still to be received")

        def came_back() -> bool:
            """The caller back for more, as a loop of receives is."""
            try:
                received.append(receiver.receive_message(timeout=0.05)[0].message)
            except OperationTimedOut:
                pass
            return bool(heard)

        wait_until(came_back, BOUND, "the acknowledgement, in a receive")
        self.assertEqual([EVENTS], heard)
        self.assertEqual([b"%d" % index for index in range(EVENTS)], received)
        self.assertTrue(receiver.routing_acknowledged())

    def test_a_wait_ends_at_an_acknowledgement_the_last_receive_made(self) -> None:
        """The confirmation came while messages routed before the change were
        still queued, so the receive that took the last of them is what
        acknowledged it: the next receive that ends at an acknowledgement
        runs the callback and ends at once, as at a confirmation, rather than
        wait out its hour. A serve loop's wait is that receive."""
        receiver = SyncClient([self.endpoint], peers.TEST_EVENT_BACKEND)
        self.addCleanup(receiver.shutdown)
        heard: list[int] = []
        self.change_read_with_what_came_before(receiver, lambda: heard.append(1))
        for _ in range(EVENTS):
            receiver.receive_message(timeout=BOUND)
        self.assertTrue(receiver.routing_acknowledged())
        self.assertEqual([], heard, "its callback waits for the next receive")
        with self.assertRaises(OperationTimedOut):
            receiver.receive_message(timeout=HOUR, end_on_acknowledgement=True)
        self.assertEqual([1], heard, "run before the wait ended")

    def test_threaded_client_hears_it_after_on_message_had_everything(self) -> None:
        got: list[bytes] = []
        receiver = ThreadedClient(
            [self.endpoint], peers.TEST_EVENT_BACKEND, on_message=lambda mxmsg: got.append(mxmsg.message)
        )
        self.addCleanup(receiver.shutdown)
        self.cluster.wait_for_peer(peers.TEST_EVENT_BACKEND)
        self.send_routed()
        heard = Counter()
        counted: list[int] = []

        def acknowledged() -> None:
            """What on_message had by then."""
            counted.append(len(got))
            heard.add()

        receiver.set_routing(Routing(all=False), on_acknowledged=acknowledged)
        heard.wait_past(0)
        self.assertEqual([EVENTS], counted)  # every one went to on_message first, on the same io thread
        receiver.set_key_filters({EVENT: []}, on_acknowledged=heard.add)  # and the next change, again
        self.assertEqual(2, heard.wait_past(1))

    def test_async_client_hears_it_on_its_loop_after_the_deliveries(self) -> None:
        async def run() -> tuple[int, int]:
            client = await AsyncClient.create([self.endpoint], peers.TEST_EVENT_BACKEND)
            try:
                got: list[bytes] = []
                client.subscribe(EVENT, lambda mxmsg: got.append(mxmsg.message))
                await asyncio.get_running_loop().run_in_executor(
                    None, self.cluster.wait_for_peer, peers.TEST_EVENT_BACKEND
                )
                await asyncio.get_running_loop().run_in_executor(None, self.send_routed)
                done: asyncio.Future = asyncio.get_running_loop().create_future()
                client.set_routing(
                    Routing(all=False), on_acknowledged=lambda: done.set_result((len(got), threading.get_ident()))
                )
                return await asyncio.wait_for(done, BOUND)
            finally:
                await client.aclose()

        count, thread = asyncio.run(run())
        self.assertEqual(EVENTS, count)
        self.assertEqual(threading.get_ident(), thread, "on the loop's thread")

    def test_a_server_drains_at_the_acknowledgement_not_at_its_poll(self) -> None:
        class Leaving(BaseMultiplexerServer):
            """Answers, and is asked to leave by its first request."""

            multiplexer_client_type = peers.PYTHON_TEST_SERVER

            def handle_message(self, mxmsg) -> None:
                self.send_message(message=b"ok", type=RESPONSE)
                self.start_draining()

        served = BackendThread(lambda: Leaving([self.endpoint]), poll=HOUR, drain_seconds=HOUR).start()
        self.addCleanup(served.stop)
        asking = ThreadedClient([self.endpoint], peers.WEBSITE)
        self.addCleanup(asking.shutdown)
        self.assertEqual(b"ok", asking.query(b"leave", type=REQUEST, timeout=BOUND).message)
        wait_until(lambda: not served.running, BOUND, "the drain over at the multiplexer's confirmation")

    def test_a_servers_callback_runs_on_its_loop_between_messages(self) -> None:
        heard = Counter()

        class Filtering(BaseMultiplexerServer):
            """Changes its filters when asked, the callback noting where it runs."""

            multiplexer_client_type = peers.PYTHON_TEST_SERVER
            handling = False

            def handle_message(self, mxmsg) -> None:
                self.handling = True
                if mxmsg.message == b"stop":
                    self.stop()  # the loop's wait is an hour: it stops itself, at once
                else:
                    self.set_key_filters({REQUEST: ["one.*"]}, on_acknowledged=self.acknowledged)
                self.send_message(message=b"ok", type=RESPONSE)
                self.handling = False

            def acknowledged(self) -> None:
                assert not self.handling, "between messages, not in a handler"
                heard.add()

        served = BackendThread(lambda: Filtering([self.endpoint]), poll=HOUR).start()
        self.addCleanup(served.stop)
        asking = ThreadedClient([self.endpoint], peers.WEBSITE)
        self.addCleanup(asking.shutdown)
        self.assertEqual(b"ok", asking.query(b"change", type=REQUEST, timeout=BOUND, key="one").message)
        heard.wait_past(0)
        self.assertEqual(served._thread.ident, heard.threads[0], "on the loop's thread")
        self.assertEqual(b"ok", asking.query(b"stop", type=REQUEST, timeout=BOUND, key="one").message)
        wait_until(lambda: not served.running, BOUND, "stopped")

    def test_a_threaded_server_hears_it_once_its_workers_finished_what_came_before(self) -> None:
        heard = Counter()
        turns = Counter()

        class Held(BaseThreadedMultiplexerServer):
            """Holds the request it is told to until the test releases it."""

            multiplexer_client_type = peers.PYTHON_TEST_SERVER

            def __init__(self, addresses) -> None:
                super().__init__(addresses, workers=2)
                self.holding = threading.Event()
                self.release = threading.Event()

            def handle_message(self, request) -> None:
                if request.mxmsg.message == b"hold":
                    self.holding.set()
                    self.release.wait(BOUND)
                request.reply(b"ok", type=RESPONSE)

            def periodic_task(self) -> None:
                turns.add()

        served = BackendThread(lambda: Held([self.endpoint]), poll=HOUR).start()
        server = served.backend
        assert server is not None
        self.addCleanup(served.stop)
        self.addCleanup(server.release.set)
        asking = ThreadedClient([self.endpoint], peers.WEBSITE)
        self.addCleanup(asking.shutdown)
        answers: list = []
        asking.query(b"hold", type=REQUEST, timeout=BOUND, callback=answers.append)
        self.assertTrue(server.holding.wait(BOUND), "the request reached a worker, which holds it")
        before = turns.value
        server.set_key_filters({REQUEST: []}, on_acknowledged=heard.add)
        turns.wait_past(before)  # woken by the acknowledgement, which it has looked at
        self.assertEqual(0, heard.value, "not while a request from before is in a worker")
        server.release.set()
        heard.wait_past(0)
        self.assertEqual(served._thread.ident, heard.threads[0], "on serve_forever()'s thread")
        wait_until(lambda: answers, BOUND, "the held request answered")


if __name__ == "__main__":
    unittest.main()
