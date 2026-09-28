"""disconnect() drops a multiplexer a client was given, in every Python class.

The target is gone, observed without waiting: with the only one dropped,
nothing is connected or on its way, so a SyncClient's flushing send
raises NotConnected at once, where a reconnect would have reached the
multiplexer listening there; connections_count() is down by the time the
call returns; a second disconnect() finds nothing, no reconnect left.
From a ThreadedClient's callback, on the io thread, it raises
RuntimeError, and after a shutdown NotConnected, as connect() does.
"""

import asyncio
import socket
import unittest

from multiplexer.aio import AsyncClient
from multiplexer.clients import SyncClient
from multiplexer.multiplexer_constants import peers, types
from multiplexer.mxclient import NotConnected
from multiplexer.testing import Cluster, runfile
from multiplexer.threaded_client import ThreadedClient

RULES = runfile("tests/testing.rules")


def free_port() -> int:
    """A port of 127.0.0.1 nothing listens on, as the kernel picks one."""
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


class SyncClientDisconnectTest(unittest.TestCase):
    """SyncClient, whose loop runs inside its calls only."""

    def test_the_only_multiplexer_dropped_leaves_nothing_coming(self) -> None:
        """Its connection live: dropped, nothing is left, so the flushing
        send raises NotConnected at once, where a reconnect would have
        reached the multiplexer, still up, and written it."""
        with Cluster(1, rules=RULES) as cluster:
            endpoint = cluster.endpoints[0]
            client = SyncClient([endpoint], type=peers.WEBSITE)
            try:
                self.assertTrue(client.disconnect(endpoint))
                self.assertEqual(0, client.connections_count())
                with self.assertRaises(NotConnected):
                    client.send_message(b"after", type=types.TEST_UNROUTED, flush=True, timeout=60)
                self.assertFalse(client.disconnect(endpoint), "gone, reconnect and all")
                self.assertFalse(client.disconnect(("127.0.0.1", 1)), "never given")
            finally:
                client.shutdown()
            with self.assertRaises(NotConnected):
                client.disconnect(endpoint)

    def test_a_failed_connections_reconnect_ends_with_its_target(self) -> None:
        """A connection refused has a reconnect armed: dropped, it never
        reaches the multiplexer that comes up there since."""
        with Cluster(1, rules=RULES) as cluster:
            endpoint = cluster.endpoints[0]
            cluster.mx[0].stop()
            client = SyncClient([], type=peers.WEBSITE)
            try:
                self.assertFalse(client.wait_for_connection(client.connect(endpoint, timeout=5), 0), "refused")
                cluster.mx[0].start()  # what the reconnect would reach, on the same address
                self.assertTrue(client.disconnect(endpoint))
                with self.assertRaises(NotConnected):
                    client.send_message(b"after", type=types.TEST_UNROUTED, flush=True, timeout=60)
                self.assertFalse(client.disconnect(endpoint))
            finally:
                client.shutdown()


class ThreadedDisconnectTest(unittest.TestCase):
    """ThreadedClient and AsyncClient, which drop on the io thread."""

    def test_a_threaded_client_drops_on_its_io_thread(self) -> None:
        """Done before the call returns: the count is down, and no
        reconnect is left; a refused connection's reconnect is found once;
        connect() gives the target again."""
        with Cluster(1, rules=RULES) as cluster:
            endpoint = cluster.endpoints[0]
            with ThreadedClient([endpoint], peers.PYTHON_TEST_CLIENT) as client:
                self.assertTrue(client.disconnect(endpoint))
                self.assertEqual(0, client.connections_count())
                self.assertFalse(client.disconnect(endpoint), "no reconnect left")
                refused = ("127.0.0.1", free_port())
                self.assertFalse(client.connect(refused, 5))
                self.assertTrue(client.disconnect(refused))
                self.assertFalse(client.disconnect(refused))
                self.assertTrue(client.connect(endpoint, 5))

    def test_a_threaded_client_refuses_from_a_callback_and_after_shutdown(self) -> None:
        """From a send's callback, on the io thread, RuntimeError, as
        connect() raises there; after shutdown() NotConnected."""
        with Cluster(1, rules=RULES) as cluster:
            endpoint = cluster.endpoints[0]
            client = ThreadedClient([endpoint], peers.PYTHON_TEST_CLIENT)
            outcome: list[str] = []

            def on_written(written: int) -> None:
                """disconnect() on the io thread."""
                try:
                    client.disconnect(endpoint)
                    outcome.append("returned")
                except RuntimeError:
                    outcome.append("RuntimeError")

            client.send_message(b"x", type=types.TEST_UNROUTED, callback=on_written)
            client.flush_all(10)  # once the callbacks of what was sent have run
            self.assertEqual(["RuntimeError"], outcome)
            self.assertEqual(1, client.connections_count(), "still connected")
            client.shutdown()
            with self.assertRaises(NotConnected):
                client.disconnect(endpoint)

    def test_an_async_client_drops_as_its_threaded_client(self) -> None:
        """The same call on AsyncClient, which blocks the loop for the
        round trip; NotConnected after close()."""
        with Cluster(1, rules=RULES) as cluster:
            endpoint = cluster.endpoints[0]

            async def drop() -> list[object]:
                """The outcomes, in order."""
                client = AsyncClient([endpoint], peers.PYTHON_TEST_CLIENT)
                outcomes: list[object] = [client.disconnect(endpoint), client.connections_count()]
                outcomes.append(client.disconnect(endpoint))
                await client.aclose()
                try:
                    client.disconnect(endpoint)
                    outcomes.append("returned")
                except NotConnected:
                    outcomes.append("NotConnected")
                return outcomes

            self.assertEqual([True, 0, False, "NotConnected"], asyncio.run(drop()))


if __name__ == "__main__":
    unittest.main()
