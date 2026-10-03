"""A timeout of 0 or NaN is no time at all, "don't wait", in every Python
class and call that takes one. A message sent with one, on SyncClient,
ThreadedClient or AsyncClient, goes now or is dropped and reported at
once, where it was held DEFAULT_TIMEOUT or a millisecond; both server
classes end a drain given NaN as it begins, where NaN was no cap;
stall_seconds of 0 or NaN arms no dump, where faulthandler refused it; a
recording call given NaN reads nothing, where it read once; a
TimeoutTicker of NaN has no time left, where it never ran out; and a
connect given NaN starts the attempt and returns, where an assertion
threw. Counted, not timed: drops are counted inside the call, or after a
round trip through the io thread, and a recording call's reads are
counted by the client it is given.
"""

import asyncio
import math
import threading
import unittest
from typing import Any

from multiplexer import recording
from multiplexer.Multiplexer_pb2 import MultiplexerMessage, Routing
from multiplexer.aio import AsyncClient
from multiplexer.clients import Client
from multiplexer.multiplexer_constants import peers, types
from multiplexer.mxclient import DropReason, OperationTimedOut, TimeoutTicker
from multiplexer.servers import BaseMultiplexerServer
from multiplexer.testing import BackendThread, Cluster, runfile, wait_until
from multiplexer.threaded_client import ThreadedClient
from multiplexer.threaded_server import BaseThreadedMultiplexerServer, Request

RULES = runfile("tests/testing.rules")
NO_TIME = (0, math.nan)
OPEN_DRAIN = Routing(any=False, all=True)  # events kept: only a cap could end the drain


class ThreadedLeaver(BaseThreadedMultiplexerServer):
    """Once `leave` is set, starts a drain from periodic_task() that keeps
    a path open, noting whether drained() held right after."""

    def __init__(self, addresses: list[tuple[str, int]]):
        super().__init__(addresses, type=peers.PYTHON_TEST_SERVER, drain_routing=OPEN_DRAIN)
        self.leave = False
        self.drained_at_once: bool | None = None

    def handle_message(self, request: Request) -> None:
        """The payload back to the requester."""
        request.reply(request.mxmsg.message, type=types.PYTHON_TEST_RESPONSE)

    def periodic_task(self) -> None:
        """The drain begins here once asked for."""
        if self.leave and not self.draining:
            self.start_draining()
            self.drained_at_once = self.drained()


class SyncLeaver(BaseMultiplexerServer):
    """ThreadedLeaver on one thread."""

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


class CountedReads:
    """The client a recording call is given: every call passed on to
    `client`, the reads counted."""

    def __init__(self, client: Client):
        self.client = client
        self.reads = 0

    def new_message(self, **kwargs: Any) -> MultiplexerMessage:
        """As the client's."""
        return self.client.new_message(**kwargs)

    def _schedule_all(self, serialized: bytes, message_id: int, message_type: int, timeout: float) -> int:
        """As the client's."""
        return self.client._schedule_all(serialized, message_id, message_type, timeout)

    def _receive(self, *args: Any, **kwargs: Any) -> Any:
        """As the client's, counted."""
        self.reads += 1
        return self.client._receive(*args, **kwargs)


class ZeroOrNanTimeoutTest(unittest.TestCase):
    """See the module docstring."""

    def test_a_sync_send_with_no_time_is_dropped_at_once(self) -> None:
        """With no connection live, a message sent with 0 or NaN is dropped
        and told inside the call, NO_CONNECTION, a send to ALL once, and its
        callback hears 0 in the next call that runs the loop: it was held
        for DEFAULT_TIMEOUT."""
        told: list[DropReason] = []
        heard: list[int] = []
        client = Client([], type=peers.WEBSITE, on_drop=lambda message_id, reason: told.append(reason))
        try:
            client.send_message(b"zero", type=types.TEST_UNROUTED, timeout=0, callback=heard.append)
            self.assertEqual([DropReason.NO_CONNECTION], told, "held for a connection")
            client.send_message(b"nan", type=types.TEST_UNROUTED, timeout=math.nan, multiplexer=Client.ALL)
            self.assertEqual([DropReason.NO_CONNECTION] * 2, told)
            self.assertEqual(2, client.dropped)
            with self.assertRaises(OperationTimedOut):
                client.receive_message(timeout=0)  # runs the loop once
            self.assertEqual([0], heard)
        finally:
            client.shutdown()

    def test_a_threaded_send_with_no_time_is_dropped_at_once(self) -> None:
        """ThreadedClient and AsyncClient, with no connection live: a send
        with 0 or NaN is dropped and told by the time the io thread has
        handled it, as a round trip through that thread shows, where it was
        held a millisecond."""
        for timeout in NO_TIME:
            with self.subTest(client="ThreadedClient", timeout=timeout):
                with ThreadedClient([], peers.PYTHON_TEST_CLIENT) as threaded:
                    threaded.send_message(b"no time", type=types.TEST_UNROUTED, timeout=timeout)
                    threaded.connections_count()  # a round trip through the io thread
                    self.assertEqual(1, threaded.dropped, "held for a connection")

            with self.subTest(client="AsyncClient", timeout=timeout):

                async def send(timeout: float = timeout) -> int:
                    """The send, then the round trip; what was dropped by then."""
                    client = AsyncClient([], peers.PYTHON_TEST_CLIENT)
                    try:
                        await client.send_message(b"no time", type=types.TEST_UNROUTED, timeout=timeout)
                        client.connections_count()
                        return client.dropped
                    finally:
                        await client.aclose()

                self.assertEqual(1, asyncio.run(send()), "held for a connection")

    def test_both_servers_end_a_drain_with_no_time_at_once(self) -> None:
        """drain_seconds=NaN, as 0, ends a drain as it begins, whatever path
        it keeps open: NaN, compared with what had passed, was no cap."""
        with Cluster(1, rules=RULES) as cluster:
            for server_class in (SyncLeaver, ThreadedLeaver):
                with self.subTest(server=server_class.__name__):
                    self.assertTrue(self.drained_at_once(cluster, server_class), "no time")

    def drained_at_once(self, cluster: Cluster, server_class: type[SyncLeaver] | type[ThreadedLeaver]) -> bool | None:
        """Whether a server of `server_class`, served with drain_seconds=NaN,
        found its drain over as it began it."""
        served = BackendThread(lambda: server_class(cluster.endpoints), drain_seconds=math.nan).start()
        server = served.backend
        assert server is not None
        server.leave = True
        wait_until(lambda: server.drained_at_once is not None, 10, "the drain begun")
        served.stop()
        return server.drained_at_once

    def test_stall_seconds_of_no_time_arm_no_dump(self) -> None:
        """serve_forever(stall_seconds=0 or NaN) serves, arming no dump,
        where faulthandler refused both and serve_forever() raised at its
        first iteration."""
        with Cluster(1, rules=RULES) as cluster:
            for stall in NO_TIME:
                with self.subTest(stall_seconds=stall):
                    self.assertEqual([], self.served_with_stall(cluster, stall))

    def served_with_stall(self, cluster: Cluster, stall: float) -> list[BaseException]:
        """What serve_forever(stall_seconds=`stall`) raised by the time a
        query it answered and a stop() were done; the query must be answered."""
        server = SyncLeaver(cluster.endpoints)
        server.connect()  # here, so that it is registered before it serves; serve_forever() adopts it
        raised: list[BaseException] = []

        def serve() -> None:
            """The server's thread; what serving raised is kept."""
            try:
                server.serve_forever(poll=0.05, stall_seconds=stall)
            except BaseException as error:
                raised.append(error)

        serving = threading.Thread(target=serve, name="mx-stall", daemon=True)
        serving.start()
        try:
            with ThreadedClient(cluster.endpoints, peers.PYTHON_TEST_CLIENT) as client:
                self.assertEqual(b"served", client.query(b"served", types.PYTHON_TEST_REQUEST, timeout=10).message)
        finally:
            server.stop()
            serving.join(10)
        return raised

    def test_a_recording_call_with_no_time_reads_nothing(self) -> None:
        """recording.status(timeout=0 or NaN) sends its control and reads
        nothing, returning no status: given NaN it read once."""
        with Cluster(1, rules=RULES, remote_recording=True) as cluster:
            controller = Client(cluster.endpoints, type=recording.RECORDING_CONTROLLER)
            try:
                for timeout in NO_TIME:
                    with self.subTest(timeout=timeout):
                        counted = CountedReads(controller)
                        self.assertEqual([], recording.status(counted, timeout=timeout))  # type: ignore[arg-type]
                        self.assertEqual(0, counted.reads)
            finally:
                controller.shutdown()

    def test_a_timeout_ticker_of_nan_has_no_time(self) -> None:
        """TimeoutTicker, the deadline a synchronous query's steps share,
        reads NaN as 0, as mx::from_seconds does: where it never ran out."""
        ticker = TimeoutTicker(math.nan)
        self.assertEqual(0, ticker())
        self.assertFalse(ticker.permit())

    def test_a_connect_with_nan_starts_the_attempt_and_returns(self) -> None:
        """SyncClient.connect(timeout=NaN), as with 0: the attempt starts and
        the call returns, where an assertion threw."""
        with Cluster(1, rules=RULES) as cluster:
            client = Client([], type=peers.WEBSITE)
            try:
                connection = client.connect(cluster.endpoints[0], timeout=math.nan)
                self.assertTrue(client.wait_for_connection(connection, 10), "the attempt went on")
            finally:
                client.shutdown()


if __name__ == "__main__":
    unittest.main()
