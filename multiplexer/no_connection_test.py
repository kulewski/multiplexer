"""A Client with no connections must time out, call after call, never spin.

asio marks an io_service stopped once it runs out of work, which a client
without connections does at the end of its first timed wait; a second wait
on a stopped service used to loop forever at full CPU instead of raising.
And a wait for a write of a message held with nothing connected and
nothing on its way ends at once, whatever its timeout: with no deadline
the loop had no work and spun, and math.inf's waited 31 years.
"""

import math
import os
import threading
import time
import unittest
from collections.abc import Callable

from multiplexer.clients import SyncClient
from multiplexer.multiplexer_constants import peers, types
from multiplexer.mxclient import Client, NotConnected, OperationTimedOut
from multiplexer.servers import BaseMultiplexerServer
from multiplexer.testing import Cluster, runfile

RULES = runfile("tests/testing.rules")  # the file the constants were generated from


class NoConnectionTest(unittest.TestCase):
    """Repeated timed waits on a connection-less Client."""

    def test_read_message_times_out_every_time(self) -> None:
        """Three reads in a row each raise OperationTimedOut after about the
        timeout, and each costs CPU time in the microseconds, not the whole
        wait: a wait that spins burns as much CPU as it lasts."""
        client = Client(1)
        for _ in range(3):
            started, cpu_started = time.monotonic(), time.process_time()
            with self.assertRaises(OperationTimedOut):
                client.read_message(timeout=0.1)
            self.assertLess(time.monotonic() - started, 1.0)
            self.assertLess(time.process_time() - cpu_started, 0.02, "the wait spun instead of sleeping")
        client.shutdown()

    def test_query_raises_not_connected_every_time(self) -> None:
        """Two queries in a row each raise NotConnected within their timeout, without spinning."""
        client = Client(1)
        for _ in range(2):
            started, cpu_started = time.monotonic(), time.process_time()
            with self.assertRaises(NotConnected):
                client.query(b"", type=1, timeout=0.2)
            self.assertLess(time.monotonic() - started, 1.5)
            self.assertLess(time.process_time() - cpu_started, 0.04, "the wait spun instead of sleeping")
        client.shutdown()


def thread_cpu_seconds(native_id: int) -> float:
    """The CPU time thread `native_id` of this process has used, as /proc
    counts it: a busy loop's grows as fast as the clock, a blocked wait's
    not at all."""
    with open("/proc/self/task/%d/stat" % native_id) as stat:
        fields = stat.read().rsplit(")", 1)[1].split()  # after the name, which may hold spaces
    return (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK")  # utime, stime


class EndsTest(unittest.TestCase):
    """Runs a wait that must end at once on a thread of its own."""

    def ended(self, call: Callable[[], object]) -> str:
        """What `call` ended in, run on a thread of its own so that a spin
        fails the test instead of hanging it: the repr of what it returned,
        or the name of what it raised. A wait that never ends fails with the
        CPU time its thread used meanwhile, which tells a busy loop from a
        wait blocked for good."""
        outcome: list[str] = []

        def run() -> None:
            try:
                outcome.append(repr(call()))
            except Exception as error:
                outcome.append(type(error).__name__)

        thread = threading.Thread(target=run, daemon=True)
        thread.start()
        thread.join(10)
        if thread.is_alive():
            assert thread.native_id is not None
            self.fail("the wait never ended: its thread used %.1f s of CPU" % thread_cpu_seconds(thread.native_id))
        return outcome[0]


class NothingToReceiveFromTest(EndsTest):
    """A wait with no deadline on a client with nothing it could receive
    from, no connection and none on its way, ends at once, where it spun at
    full CPU forever."""

    def test_a_read_on_a_client_with_no_address_raises_not_connected(self) -> None:
        self.assertEqual("NotConnected", self.ended(lambda: Client(1).receive_message()))

    def test_waiting_for_a_connection_that_cannot_come_returns_false(self) -> None:
        self.assertEqual("False", self.ended(lambda: Client(1).wait_for_any_connection(-1)))

    def test_a_read_after_shutdown_raises_not_connected(self) -> None:
        with Cluster(1, rules=RULES) as cluster:

            def read_after_shutdown() -> object:
                client = SyncClient(cluster.endpoints, type=peers.PYTHON_TEST_CLIENT)
                client.shutdown()
                return client.receive_message()

            self.assertEqual("NotConnected", self.ended(read_after_shutdown))

    def test_a_backend_never_connected_raises_not_connected_from_loop_iter(self) -> None:
        """A loop of a program's own around loop_iter(), without the
        connect() this release asks for before it."""
        with Cluster(1, rules=RULES) as cluster:
            self.assertEqual(
                "NotConnected",
                self.ended(lambda: BaseMultiplexerServer(cluster.endpoints, type=peers.PYTHON_TEST_SERVER).loop_iter()),
            )


class NothingToWriteToTest(EndsTest):
    """A wait for a write on a client holding a message, with nothing
    connected and nothing on its way, ends at once, whatever its timeout:
    flush_all() says False, a flushing send and a query raise NotConnected,
    shutdown() returns. With no deadline the loop had no work and spun at
    full CPU forever, and math.inf's timer blocked for 31 years."""

    def holding(self) -> Client:
        """A client that never connected, holding a message with no
        deadline, which nothing could ever write."""
        client = Client(peers.WEBSITE)
        client.send_message(b"held", type=types.TEST_UNROUTED, timeout=-1)
        return client

    def ended_on(self, client: Client, call: Callable[[], object]) -> str:
        """ended(), `client` driven from the thread that runs `call`."""

        def driven() -> object:
            client.bind_to_current_thread()
            return call()

        return self.ended(driven)

    def test_flush_all_says_false(self) -> None:
        for timeout in (-1, math.inf):
            with self.subTest(timeout=timeout):
                client = self.holding()
                self.assertEqual(
                    "False", self.ended_on(client, lambda client=client, timeout=timeout: client.flush_all(timeout))
                )

    def test_a_flushing_send_raises_not_connected_and_drops_its_message(self) -> None:
        client = Client(peers.WEBSITE)
        call = lambda: client.send_message(b"flushed", type=types.TEST_UNROUTED, flush=True, timeout=-1)
        self.assertEqual("NotConnected", self.ended_on(client, call))
        self.assertEqual(1, client.dropped, "not to go out after the send said NotConnected")

    def test_a_query_raises_not_connected(self) -> None:
        client = Client(peers.WEBSITE)
        call = lambda: client.query(b"", type=types.PYTHON_TEST_REQUEST, timeout=-1)
        self.assertEqual("NotConnected", self.ended_on(client, call))

    def test_shutdown_returns(self) -> None:
        client = self.holding()
        self.assertEqual("None", self.ended_on(client, lambda: client.shutdown(-1)))
        self.assertEqual(1, client.dropped, "the held message, dropped at the shutdown")


if __name__ == "__main__":
    unittest.main()
