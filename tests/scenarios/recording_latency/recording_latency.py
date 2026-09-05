"""recording costs the routed message nothing a client can measure.

The same client runs the same queries against a multiplexer with --record
and one without; the median round trip differs by less than half a
millisecond, ten times the cost of the buffered write the recording adds.
"""

import statistics
import unittest

from tests import harness
from tests.harness import Cluster, constants as C, spawn

QUERIES = 300


class RecordingLatency(unittest.TestCase):
    """Compares medians with and without recording."""

    def median_ms(self, record: bool) -> float:
        """The median query round trip over QUERIES queries against one multiplexer."""
        cfg = harness.CONFIG
        with Cluster(1, record=record) as cluster:
            backend = spawn(
                "backend",
                cfg.lang("backend"),
                mx=cluster.addresses,
                type=C.peers.TEST_BACKEND_A,
                serves={C.types.TEST_REQUEST_A: C.types.TEST_RESPONSE},
            )
            backend.wait_for("connected")
            client = spawn(
                "client",
                cfg.lang("client"),
                mx=cluster.addresses,
                type=C.peers.TEST_CLIENT,
                count=QUERIES,
                query=[(C.types.TEST_REQUEST_A, "q{round}")],
            )
            self.assertEqual(0, client.wait())
            responses = client.events_of("response")
            self.assertEqual(QUERIES, len(responses))
            self.assertEqual(0, backend.stop())
            return statistics.median(r["ms"] for r in responses)

    def test_recording_does_not_slow_routing(self):
        plain = self.median_ms(record=False)
        recorded = self.median_ms(record=True)
        self.assertLess(abs(recorded - plain), 0.5, "medians: plain %.2f ms, recorded %.2f ms" % (plain, recorded))


if __name__ == "__main__":
    harness.main()
