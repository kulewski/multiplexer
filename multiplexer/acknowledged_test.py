"""A change of the routing or the key filters, once acknowledged, as an
event in every Python class: on_acknowledged runs once, after every
message routed to the peer before the change was handed out (received
from SyncClient, passed to ThreadedClient's on_message, delivered on
AsyncClient's loop, handled by the server classes, on their loop's
thread), and the server classes' loops wake at the acknowledgement rather
than at their next poll, an hour away here, for a drain as for a change of
filters, and a receive's wait as well when the receive before it, taking
the last message routed before the change, is what acknowledged it. A
connection's welcome confirms the routing it carries, so a client is
acknowledged once connected, a change made before connecting is heard
after it, and a drain survives its multiplexer's restart. A drain also
waits for what its server holds for a connection, every multiplexer down,
and ends on its cap whatever the poll. Counted, not
timed: the sender's own marker coming back says the
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
from multiplexer.mxclient import NotConnected, OperationTimedOut
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


class Held(BaseThreadedMultiplexerServer):
    """Holds every request until the test releases it, then answers it."""

    multiplexer_client_type = peers.PYTHON_TEST_SERVER

    def __init__(self, addresses) -> None:
        super().__init__(addresses, workers=1)
        self.holding = threading.Event()
        self.release = threading.Event()

    def handle_message(self, request) -> None:
        self.holding.set()
        self.release.wait(BOUND)
        request.reply(b"ok", type=RESPONSE)


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

    def test_a_connections_welcome_confirms_the_routing(self) -> None:
        """A connection's welcome carries the client's routing, which the
        multiplexer takes with it: once connected, with no change made and
        nothing received, the routing reads acknowledged."""
        sync = SyncClient([self.endpoint], peers.TEST_EVENT_BACKEND)  # connected when made
        self.addCleanup(sync.shutdown)
        self.assertTrue(sync.routing_acknowledged())
        threaded = ThreadedClient([self.endpoint], peers.TEST_EVENT_BACKEND)  # likewise
        self.addCleanup(threaded.shutdown)
        self.assertTrue(threaded.routing_acknowledged())

    def test_a_change_made_before_connecting_is_heard_after(self) -> None:
        """A change made with no connection is in effect at once, and its
        callback runs at the next receive; a connection made before that
        receive carries the change in its welcome and takes nothing back."""
        receiver = SyncClient([], peers.TEST_EVENT_BACKEND)
        self.addCleanup(receiver.shutdown)
        heard: list[int] = []
        receiver.set_key_filters({EVENT: []}, on_acknowledged=lambda: heard.append(1))
        receiver.connect(self.endpoint)

        def came_back() -> bool:
            """The caller back for more, as a loop of receives is."""
            try:
                receiver.receive_message(timeout=0.05)
            except OperationTimedOut:
                pass
            return bool(heard)

        wait_until(came_back, BOUND, "the callback, at a receive after the connection")
        self.assertEqual([1], heard)
        self.assertTrue(receiver.routing_acknowledged())

    def test_a_drain_across_a_restart_ends_once_its_work_is_done(self) -> None:
        """A threaded server drains with a request in its worker, the drain
        confirmed; its multiplexer restarts, and the connection that comes
        back carries the drain routing in its welcome, which confirms it
        again: the drain ends once the worker is done, not at its cap, an
        hour away. The worker is let go only once the connection is back,
        so that the drain cannot end while no multiplexer is connected."""
        served = BackendThread(lambda: Held([self.endpoint]), poll=HOUR, drain_seconds=HOUR).start()
        server = served.backend
        assert server is not None
        self.addCleanup(served.stop)
        self.addCleanup(server.release.set)
        asking = SyncClient([self.endpoint], peers.WEBSITE)
        self.addCleanup(asking.shutdown)
        asking.send_message(b"hold", type=REQUEST, flush=True)
        self.assertTrue(server.holding.wait(BOUND), "the request reached the worker, which holds it")
        server.start_draining()
        wait_until(server.client.routing_acknowledged, BOUND, "the drain confirmed")
        self.cluster.mx[0].restart()
        self.cluster.wait_for_peer(peers.PYTHON_TEST_SERVER, timeout=BOUND)  # back on the restarted one
        wait_until(lambda: server.client.connections_count() == 1, BOUND, "registered on the server's side")
        server.release.set()
        wait_until(lambda: not served.running, BOUND, "the drain over once the worker is done")

    def test_a_drain_waits_for_what_it_holds_for_a_connection(self) -> None:
        """A threaded server drains while its only multiplexer is down, a
        request in its worker: the worker's reply is held for the next
        connection, and the drain waits for it, the multiplexer back, where
        the confirmation, with no connection, held at once and the close
        dropped the reply. Counted: what the server's client gave up on."""
        served = BackendThread(lambda: Held([self.endpoint]), poll=HOUR, drain_seconds=HOUR).start()
        server = served.backend
        assert server is not None
        self.addCleanup(served.stop)
        self.addCleanup(server.release.set)
        client = server.client  # gone from the server at its close: what it dropped is read after
        asking = SyncClient([self.endpoint], peers.WEBSITE)
        self.addCleanup(asking.shutdown)
        asking.send_message(b"hold", type=REQUEST, flush=True)
        self.assertTrue(server.holding.wait(BOUND), "the request reached the worker, which holds it")
        multiplexer = self.cluster.mx[0]
        multiplexer.stop()  # every connection to it ends
        wait_until(lambda: client.connections_count() == 0, BOUND, "the connection gone")
        server.start_draining()
        server.release.set()

        def held() -> bool:
            """The reply held; a server that did not wait for it is closed
            by now, and what it dropped is checked below."""
            try:
                return client.held_for_a_connection() == 1
            except NotConnected:
                return True

        wait_until(held, BOUND, "the reply held")
        multiplexer.start()  # back on the same address
        wait_until(lambda: not served.running, BOUND, "the drain over once the reply went")
        self.assertEqual(0, client.dropped, "the reply written once a connection was back, not dropped")

    def test_a_plain_drain_waits_for_what_it_holds_for_a_connection(self) -> None:
        """The same for the plain server: its reply, made as its only
        multiplexer went down, held for the next connection, which the
        drain waits for."""
        held = threading.Event()

        class HeldPlain(BaseMultiplexerServer):
            """Starts draining at the request it is told to hold, holds it
            until the test releases it, then answers it; says, on its
            loop's thread, when its client holds a message for a
            connection."""

            multiplexer_client_type = peers.PYTHON_TEST_SERVER

            def __init__(self, addresses) -> None:
                super().__init__(addresses)
                self.holding = threading.Event()
                self.release = threading.Event()

            def handle_message(self, mxmsg) -> None:
                if mxmsg.message == b"hold":
                    self.start_draining()
                    self.holding.set()
                    self.release.wait(BOUND)
                self.send_message(message=b"ok", type=RESPONSE)

            def periodic_task(self) -> None:
                if self.conn.held_for_a_connection():
                    held.set()

        served = BackendThread(lambda: HeldPlain([self.endpoint]), poll=HOUR, drain_seconds=HOUR).start()
        server = served.backend
        assert server is not None
        self.addCleanup(served.stop)
        self.addCleanup(server.release.set)
        asking = SyncClient([self.endpoint], peers.WEBSITE)
        self.addCleanup(asking.shutdown)
        asking.send_message(b"hold", type=REQUEST, flush=True)
        self.assertTrue(server.holding.wait(BOUND), "the request reached the server, which holds it, draining")
        multiplexer = self.cluster.mx[0]
        multiplexer.stop()  # every connection to it ends
        server.release.set()
        self.assertTrue(held.wait(BOUND), "the reply held")
        multiplexer.start()  # back on the same address
        wait_until(lambda: not served.running, BOUND, "the drain over once the reply went")
        self.assertEqual(0, server.conn.dropped, "the reply written once a connection was back, not dropped")

    def test_a_plain_drain_waits_for_what_waits_in_a_batch(self) -> None:
        """The same for an event the server sends into a batch, its delay
        an hour, its only multiplexer down: the drain lets the batch go,
        held for the next connection, and waits for it, where it ended at
        once, nothing being held, and the close dropped the batch."""
        held = threading.Event()

        class HeldBatch(BaseMultiplexerServer):
            """As HeldPlain, sending once released an event into a batch."""

            multiplexer_client_type = peers.PYTHON_TEST_SERVER

            def __init__(self, addresses) -> None:
                super().__init__(addresses)
                self.conn.set_batching(EVENT, max_delay=HOUR)
                self.holding = threading.Event()
                self.release = threading.Event()

            def handle_message(self, mxmsg) -> None:
                if mxmsg.message == b"hold":
                    self.start_draining()
                    self.holding.set()
                    self.release.wait(BOUND)
                self.conn.send_message(b"batched", type=EVENT)

            def periodic_task(self) -> None:
                if self.conn.held_for_a_connection():
                    held.set()

        served = BackendThread(lambda: HeldBatch([self.endpoint]), poll=HOUR, drain_seconds=HOUR).start()
        server = served.backend
        assert server is not None
        self.addCleanup(served.stop)
        self.addCleanup(server.release.set)
        asking = SyncClient([self.endpoint], peers.WEBSITE)
        self.addCleanup(asking.shutdown)
        asking.send_message(b"hold", type=REQUEST, flush=True)
        self.assertTrue(server.holding.wait(BOUND), "the request reached the server, which holds it, draining")
        multiplexer = self.cluster.mx[0]
        multiplexer.stop()  # every connection to it ends
        server.release.set()
        self.assertTrue(held.wait(BOUND), "the batch let go, and held")
        multiplexer.start()  # back on the same address
        wait_until(lambda: not served.running, BOUND, "the drain over once the batch went")
        self.assertEqual(0, server.conn.dropped, "the batch written once a connection was back, not dropped")

    def test_a_plain_drain_ends_on_its_cap_whatever_the_poll(self) -> None:
        """A drain that keeps a path open ends on its cap alone: with a poll
        of an hour the loop's wait ends at the cap, where nothing woke the
        loop after the confirmation and the drain went on for good."""

        class Leaving(BaseMultiplexerServer):
            """Answers, and is asked to leave by its first request."""

            multiplexer_client_type = peers.PYTHON_TEST_SERVER

            def handle_message(self, mxmsg) -> None:
                self.send_message(message=b"ok", type=RESPONSE)
                self.start_draining()

        served = BackendThread(
            lambda: Leaving([self.endpoint], drain_routing=Routing(any=False)), poll=HOUR, drain_seconds=1.0
        ).start()
        self.addCleanup(served.stop)
        asking = ThreadedClient([self.endpoint], peers.WEBSITE)
        self.addCleanup(asking.shutdown)
        self.assertEqual(b"ok", asking.query(b"leave", type=REQUEST, timeout=BOUND).message)
        wait_until(lambda: not served.running, BOUND, "the drain over at its cap")

    def test_a_threaded_drain_ends_on_its_cap_whatever_the_poll(self) -> None:
        """The same for the threaded server."""

        class Open(BaseThreadedMultiplexerServer):
            """Takes events through its drain, so only the cap can end it."""

            multiplexer_client_type = peers.PYTHON_TEST_SERVER

            def handle_message(self, request) -> None:
                request.no_response()

        served = BackendThread(
            lambda: Open([self.endpoint], drain_routing=Routing(any=False)), poll=HOUR, drain_seconds=1.0
        ).start()
        server = served.backend
        assert server is not None
        self.addCleanup(served.stop)
        server.start_draining()
        wait_until(lambda: not served.running, BOUND, "the drain over at its cap")


if __name__ == "__main__":
    unittest.main()
