"""The peer type streamlogs connects as, LOG_STREAMER, is not passive: the
multiplexer sends an idle streamer a heartbeat every HEARTBIT_INTERVAL,
as it does every peer that sends its own, and drops one that falls
silent. The system rules declared it passive, from when streamlogs was a
synchronous client, so a hung streamer was never dropped for silence and
was sent at most one heartbeat for each frame it sent. Counted, not
timed: two heartbeats on a connection that sends nothing after its
welcome.
"""

import unittest

from multiplexer import _native
from multiplexer.multiplexer_constants import peers, types
from multiplexer.testing import Cluster, runfile
from multiplexer.testing.raw_peer import RawPeer

RULES = runfile("tests/testing.rules")  # the system rules first, as every rules file


class LogStreamerTypeTest(unittest.TestCase):
    """See the module docstring."""

    def test_an_idle_streamer_is_sent_heartbeats(self) -> None:
        with Cluster(1, rules=RULES) as cluster:
            streamer = RawPeer(cluster.endpoints[0], peers.LOG_STREAMER)
            try:
                streamer.handshake()
                for _ in range(2):
                    streamer.receive_type(types.HEARTBIT, timeout=3 * _native.HEARTBIT_INTERVAL)
            finally:
                streamer.close()


if __name__ == "__main__":
    unittest.main()
