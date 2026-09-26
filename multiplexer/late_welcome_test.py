"""A peer may send its welcome long after its connection opened: a
synchronous client sends it only when a call of its runs the loop, which
can be minutes after its connect completed. The multiplexer keeps such a
connection open past the heartbeat drop interval, the silence it allows a
peer that has introduced itself, and registers the welcome when it comes.
About 95 s; tagged slow."""

import time
import unittest

from multiplexer import _native
from multiplexer.multiplexer_constants import peers
from multiplexer.testing import Cluster, runfile
from multiplexer.testing.raw_peer import RawPeer

RULES = runfile("tests/testing.rules")  # the file the constants were generated from
# The drop interval and a margin: past it, a deadline for the welcome would have struck.
SILENCE = _native.NO_HEARTBIT_SO_PREPARE_DROP_INTERVAL + _native.NO_HEARTBIT_SO_REALLY_DROP_INTERVAL + 5


class LateWelcomeTest(unittest.TestCase):
    def test_a_welcome_after_the_drop_interval_still_registers(self) -> None:
        """A connection silent for longer than the drop interval, then a
        welcome: the multiplexer answers it with its own."""
        with Cluster(1, rules=RULES) as cluster:
            peer = RawPeer(cluster.endpoints[0], peers.PYTHON_TEST_CLIENT)
            time.sleep(SILENCE)
            _, theirs = peer.handshake()
            self.assertEqual(peers.MULTIPLEXER, theirs.type)
            peer.close()


if __name__ == "__main__":
    unittest.main()
