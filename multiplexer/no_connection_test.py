"""A Client with no connections must time out, call after call, never spin.

asio marks an io_service stopped once it runs out of work, which a client
without connections does at the end of its first timed wait; a second wait
on a stopped service used to loop forever at full CPU instead of raising.
"""

import time
import unittest

from multiplexer.mxclient import Client, NotConnected, OperationTimedOut


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


if __name__ == "__main__":
    unittest.main()
