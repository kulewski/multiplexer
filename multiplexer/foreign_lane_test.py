"""A connection or a lane belongs to its client: given another client's,
a send or a query raises ValueError, the binding's name for the C++
std::invalid_argument, in SyncClient and ThreadedClient, where the message
went out on the other client's connection, driven from this client's
thread. multiplexer/foreign_lane_test.cc has the C++ calls.
"""

import unittest

from multiplexer.clients import SyncClient
from multiplexer.multiplexer_constants import peers, types
from multiplexer.testing import Cluster, runfile
from multiplexer.threaded_client import ThreadedClient

RULES = runfile("tests/testing.rules")
REQUEST = types.PYTHON_TEST_REQUEST


class ForeignLaneTest(unittest.TestCase):
    """See the module docstring."""

    def test_another_clients_lane_or_connection_is_refused(self) -> None:
        with Cluster(1, rules=RULES) as cluster:
            with SyncClient(cluster.endpoints, type=peers.WEBSITE) as owner:
                lane = owner.lane()
                owner.send_message(b"first", type=REQUEST, multiplexer=lane, flush=True)
                connection = lane.connection
                with SyncClient(cluster.endpoints, type=peers.WEBSITE) as other:
                    with self.assertRaises(ValueError):
                        other.send_message(b"foreign", type=REQUEST, multiplexer=lane)
                    with self.assertRaises(ValueError):
                        other.send_message(b"foreign", type=REQUEST, multiplexer=connection, flush=True)
                    with self.assertRaises(ValueError):
                        other.query(b"foreign", REQUEST, multiplexer=lane, timeout=1)
                threaded = ThreadedClient(cluster.endpoints, type=peers.WEBSITE)
                try:
                    with self.assertRaises(ValueError):
                        threaded.send_message(b"foreign", type=REQUEST, multiplexer=lane)
                    with self.assertRaises(ValueError):
                        threaded.send_message(b"foreign", type=REQUEST, multiplexer=connection, flush=True)
                    with self.assertRaises(ValueError):
                        threaded.query(b"foreign", REQUEST, multiplexer=lane, timeout=1)
                finally:
                    threaded.shutdown()
                owner.send_message(b"again", type=REQUEST, multiplexer=lane, flush=True)  # still the owner's


if __name__ == "__main__":
    unittest.main()
