"""connect() to a multiplexer a client has a connection to keeps that
connection, in SyncClient and ThreadedClient: the second call returns it,
and a pinned lane through it still sends, where the call closed the live
connection and opened another, ending the pinned lane for good. Counted,
not timed: a flushing send through the lane says whether its connection
lived. AsyncClient has no connect() of its own: it connects to each of
its addresses once, at construction, through the same code.
"""

import unittest

from multiplexer.clients import SyncClient
from multiplexer.multiplexer_constants import peers, types
from multiplexer.testing import Cluster, runfile
from multiplexer.threaded_client import ThreadedClient

RULES = runfile("tests/testing.rules")
UNROUTED = types.TEST_UNROUTED  # routed nowhere: a message whose only job is to be written


class ConnectAgainTest(unittest.TestCase):
    """See the module docstring."""

    def test_a_sync_client_keeps_its_live_connection(self) -> None:
        """The second connect() returns the live connection, and the pinned
        lane on it writes on, where the send raised NotConnected."""
        with Cluster(1, rules=RULES) as cluster, SyncClient(cluster.endpoints, type=peers.WEBSITE) as client:
            lane = client.lane(pinned=True)
            client.send_message(b"before", type=UNROUTED, multiplexer=lane, flush=True)
            again = client.connect(cluster.endpoints[0])
            self.assertTrue(again.is_same_connection(lane.connection))
            self.assertFalse(lane.closed, "the pinned lane's connection was closed")
            client.send_message(b"after", type=UNROUTED, multiplexer=lane, flush=True)

    def test_a_threaded_client_keeps_its_live_connection(self) -> None:
        """The second connect() is True at once, the connection kept, and the
        pinned lane on it writes on, where the send raised NotConnected."""
        with Cluster(1, rules=RULES) as cluster, ThreadedClient(cluster.endpoints, peers.WEBSITE) as client:
            lane = client.lane(pinned=True)
            client.send_message(b"before", type=UNROUTED, multiplexer=lane, flush=True)
            self.assertTrue(client.connect(cluster.endpoints[0]))
            self.assertFalse(lane.closed, "the pinned lane's connection was closed")
            client.send_message(b"after", type=UNROUTED, multiplexer=lane, flush=True)


if __name__ == "__main__":
    unittest.main()
