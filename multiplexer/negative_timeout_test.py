"""A negative timeout is no deadline, as math.inf is, in every Python class
and call that takes one: a SyncClient send holds its message with no
deadline, where it had DEFAULT_TIMEOUT; a ThreadedClient query and an
AsyncClient flushing send and flush_all() wait, where their deadline was
already past and they gave up at once, a request already sent; a threaded
server's serve_forever(poll=-1) waits until woken, where it called
periodic_task() over and over; both servers' drain_seconds=-1 sets the
drain no cap, where it ended the drain as it began; stall_seconds=-1 arms
no dump, where faulthandler refused it; and recording.status(timeout=-1)
waits for every multiplexer's status, where it returned none. Ordered, not
timed: a backend that holds its answer until the test lets go, and a
multiplexer frozen before it welcomes the client, hold every call at the
point where its old deadline ended it.
"""

import asyncio
import threading
import unittest
from typing import Any

from multiplexer import recording
from multiplexer.Multiplexer_pb2 import MultiplexerMessage, Routing
from multiplexer.aio import AsyncClient
from multiplexer.clients import Client
from multiplexer.multiplexer_constants import peers, types
from multiplexer.mxclient import NotConnected
from multiplexer.servers import BaseMultiplexerServer
from multiplexer.testing import BackendThread, Cluster, FakePeer, runfile, wait_until
from multiplexer.threaded_client import ThreadedClient
from multiplexer.threaded_server import BaseThreadedMultiplexerServer, Request

RULES = runfile("tests/testing.rules")
NO_DEADLINE = -1  # the timeout every test gives
OPEN_DRAIN = Routing(any=False, all=True)  # events kept: only a cap could end the drain


class ThreadedLeaver(BaseThreadedMultiplexerServer):
    """Answers every request with its payload, counts its periodic_task()
    calls, and once `leave` is set starts a drain there that keeps a path
    open, noting whether drained() held right after."""

    def __init__(self, addresses: list[tuple[str, int]]):
        super().__init__(addresses, type=peers.PYTHON_TEST_SERVER, drain_routing=OPEN_DRAIN)
        self.calls = 0
        self.leave = False
        self.drained_at_once: bool | None = None

    def handle_message(self, request: Request) -> None:
        """The payload back to the requester."""
        request.reply(request.mxmsg.message, type=types.PYTHON_TEST_RESPONSE)

    def periodic_task(self) -> None:
        """Counted; the drain begins here once asked for."""
        self.calls += 1
        if self.leave and not self.draining:
            self.start_draining()
            self.drained_at_once = self.drained()


class SyncLeaver(BaseMultiplexerServer):
    """ThreadedLeaver on one thread: answers every request with its
    payload, and once `leave` is set starts a drain from periodic_task()
    that keeps a path open, noting whether drained() held right after."""

    def __init__(self, addresses: list[tuple[str, int]]):
        super().__init__(addresses, type=peers.PYTHON_TEST_SERVER, drain_routing=OPEN_DRAIN)
        self.leave = False
        self.drained_at_once: bool | None = None

    def handle_message(self, mxmsg: MultiplexerMessage) -> None:
        """The payload back to the requester."""
        self.send_message(message=mxmsg.message, type=types.PYTHON_TEST_RESPONSE)

    def periodic_task(self) -> None:
        """The drain begins here once asked for."""
        if self.leave and not self.draining:
            self.start_draining()
            self.drained_at_once = self.drained()


class NegativeTimeoutTest(unittest.TestCase):
    """See the module docstring."""

    def test_a_sync_send_holds_its_message_with_no_deadline(self) -> None:
        """Without flush the message is held for a connection with no
        deadline: nothing is left for the loop to wait for, so a receive on
        a client with no connection, nor one on its way, raises NotConnected
        at once with nothing dropped, where the message's DEFAULT_TIMEOUT
        kept the receive running the loop until it was dropped. It goes out
        once a connection comes up."""
        client = Client([], type=peers.WEBSITE)
        try:
            client.send_message(b"held", type=types.PYTHON_TEST_REQUEST, timeout=NO_DEADLINE)
            with self.assertRaises(NotConnected):
                client.receive_message(timeout=NO_DEADLINE)
            self.assertEqual(0, client.dropped, "held, not given up on")
            with Cluster(1, rules=RULES) as cluster, FakePeer(cluster, peers.PYTHON_TEST_SERVER) as backend:
                client.connect(cluster.endpoints[0])
                self.assertTrue(client.flush_all(timeout=10))
                backend.wait_for(types.PYTHON_TEST_REQUEST, matching=lambda mxmsg: mxmsg.message == b"held")
        finally:
            client.shutdown()

    def test_a_threaded_query_waits_for_its_reply(self) -> None:
        """The reply comes once the backend lets go of it: every stage's
        timer had expired as it was set, so the request went out and
        OperationTimedOut followed at once."""
        release = threading.Event()

        def answer(mxmsg: MultiplexerMessage) -> bytes:
            """The payload back, once the test lets go."""
            release.wait(30)
            return mxmsg.message

        with Cluster(1, rules=RULES) as cluster:
            backend = FakePeer(cluster, peers.PYTHON_TEST_SERVER).on(
                types.PYTHON_TEST_REQUEST, answer, reply_type=types.PYTHON_TEST_RESPONSE
            )
            with backend, ThreadedClient(cluster.endpoints, peers.PYTHON_TEST_CLIENT) as client:
                results: list[Any] = []
                ended = threading.Event()

                def on_result(result: Any) -> None:
                    """The query's end, on the io thread."""
                    results.append(result)
                    ended.set()

                client.query(b"question", types.PYTHON_TEST_REQUEST, timeout=NO_DEADLINE, callback=on_result)
                backend.wait_for(types.PYTHON_TEST_REQUEST)
                release.set()
                self.assertTrue(ended.wait(30))
                self.assertNotIsInstance(results[0], Exception)
                self.assertEqual(b"question", results[0].message)

    def test_an_async_client_waits_for_a_connection(self) -> None:
        """A flushing send and flush_all() wait for the connection a frozen
        multiplexer has yet to welcome, and return once what they waited for
        is written: their deadline was past, so the send raised NotConnected
        and the flush said False."""
        with Cluster(1, rules=RULES) as cluster, FakePeer(cluster, peers.PYTHON_TEST_SERVER) as backend:
            frozen = cluster.mx[0]

            async def send() -> None:
                """Both calls made while the multiplexer is frozen, awaited once it thaws."""
                frozen.pause()
                thawed = False
                client: AsyncClient | None = None
                try:
                    client = AsyncClient(cluster.endpoints, peers.PYTHON_TEST_CLIENT, timeout=0)  # on its way
                    flushed = asyncio.ensure_future(
                        client.send_message(b"flushed", type=types.PYTHON_TEST_REQUEST, flush=True, timeout=NO_DEADLINE)
                    )
                    await asyncio.sleep(0)  # the flushing send is on the io thread
                    await client.send_message(b"sent before", type=types.PYTHON_TEST_REQUEST)
                    everything = asyncio.ensure_future(client.flush_all(timeout=NO_DEADLINE))
                    await asyncio.sleep(0)  # and so is the flush
                    self.assertEqual(0, client.connections_count())  # a round trip through the io thread, after both
                    frozen.resume()
                    thawed = True
                    await flushed
                    self.assertTrue(await everything)
                finally:
                    if not thawed:
                        frozen.resume()
                    if client is not None:
                        await client.aclose()

            asyncio.run(send())
            for payload in (b"flushed", b"sent before"):
                backend.wait_for(
                    types.PYTHON_TEST_REQUEST, matching=lambda mxmsg, payload=payload: mxmsg.message == payload
                )

    def test_a_threaded_server_polls_until_woken(self) -> None:
        """serve_forever(poll=-1) calls periodic_task() only when woken, by a
        drain, stop() or close(): Python's wait ended at once, and it called
        periodic_task() over and over. A query answered meanwhile shows it
        serves."""
        with Cluster(1, rules=RULES) as cluster:
            served = BackendThread(lambda: ThreadedLeaver(cluster.endpoints), poll=NO_DEADLINE).start()
            server = served.backend
            assert server is not None
            cluster.wait_for_peer(peers.PYTHON_TEST_SERVER)
            with ThreadedClient(cluster.endpoints, peers.PYTHON_TEST_CLIENT) as client:
                self.assertEqual(b"served", client.query(b"served", types.PYTHON_TEST_REQUEST).message)
            self.assertEqual(0, server.calls, "nothing woke it")
            served.stop()
            self.assertEqual(0, server.calls, "a stop ends the loop before periodic_task()")

    def test_a_threaded_server_drain_has_no_cap(self) -> None:
        """drain_seconds=-1 sets the drain no cap: with a path kept open it
        is not over by time, where the negative cap had passed as it began."""
        with Cluster(1, rules=RULES) as cluster:
            with BackendThread(lambda: ThreadedLeaver(cluster.endpoints), drain_seconds=NO_DEADLINE) as served:
                server = served.backend
                assert server is not None
                server.leave = True
                wait_until(lambda: server.drained_at_once is not None, 10, "the drain begun")
                self.assertFalse(server.drained_at_once, "no cap")

    def test_a_sync_server_drain_has_no_cap_and_stalls_arm_no_dump(self) -> None:
        """drain_seconds=-1 sets the drain no cap, as on the threaded server,
        and stall_seconds=-1 arms no dump, where faulthandler refused it and
        serve_forever() raised ValueError at its first iteration."""
        with Cluster(1, rules=RULES) as cluster:
            server = SyncLeaver(cluster.endpoints)
            server.connect()  # here, so that it is registered before it serves; serve_forever() adopts it
            raised: list[BaseException] = []

            def serve() -> None:
                """The server's thread; what serving raised is kept."""
                try:
                    server.serve_forever(poll=0.05, drain_seconds=NO_DEADLINE, stall_seconds=NO_DEADLINE)
                except BaseException as error:
                    raised.append(error)

            serving = threading.Thread(target=serve, name="mx-sync-leaver", daemon=True)
            serving.start()
            try:
                with ThreadedClient(cluster.endpoints, peers.PYTHON_TEST_CLIENT) as client:
                    self.assertEqual(b"served", client.query(b"served", types.PYTHON_TEST_REQUEST).message)
                server.leave = True
                wait_until(lambda: server.drained_at_once is not None or raised, 10, "the drain begun")
                self.assertEqual([], raised)
                self.assertFalse(server.drained_at_once, "no cap")
            finally:
                server.stop()
                serving.join(10)
            self.assertEqual([], raised)

    def test_a_recording_call_waits_for_every_status(self) -> None:
        """recording.status(timeout=-1) waits for the status of every
        multiplexer: its deadline was past, so it returned none."""
        with Cluster(1, rules=RULES, remote_recording=True) as cluster:
            controller = Client(cluster.endpoints, type=recording.RECORDING_CONTROLLER)
            try:
                self.assertEqual(1, len(recording.status(controller, timeout=NO_DEADLINE)))
            finally:
                controller.shutdown()


if __name__ == "__main__":
    unittest.main()
