"""A Client with no connections must time out, call after call, never spin.

asio marks an io_service stopped once it runs out of work, which a client
without connections does at the end of its first timed wait; a second wait
on a stopped service used to loop forever at full CPU instead of raising.
"""

import threading
import time
import unittest
from collections.abc import Callable

from multiplexer.clients import SyncClient
from multiplexer.multiplexer_constants import peers
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


class NothingToReceiveFromTest(unittest.TestCase):
    """A wait with no deadline on a client with nothing it could receive
    from, no connection and none on its way, ends at once, where it spun at
    full CPU forever."""

    def ended(self, call: Callable[[], object]) -> str:
        """What `call` ended in, run on a thread of its own so that a spin
        fails the test instead of hanging it: the repr of what it returned,
        or the name of what it raised."""
        outcome: list[str] = []

        def run() -> None:
            try:
                outcome.append(repr(call()))
            except Exception as error:
                outcome.append(type(error).__name__)

        thread = threading.Thread(target=run, daemon=True)
        thread.start()
        thread.join(10)
        self.assertFalse(thread.is_alive(), "the wait never ended")
        return outcome[0]

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


if __name__ == "__main__":
    unittest.main()
