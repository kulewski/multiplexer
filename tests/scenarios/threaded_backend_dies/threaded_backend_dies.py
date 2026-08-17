"""a ThreadedClient with many queries in flight when one of two backends crashes.

Every query is answered, by the survivor after the crash; the ones that were
inside the dead backend are found again through the search.
"""

import unittest

from tests import harness
from tests.harness import Cluster, constants as C, spawn

QUERIES = 40
IN_FLIGHT = 6
CRASH_AFTER = 5


class ThreadedBackendDies(unittest.TestCase):
    """Checks that a ThreadedClient with many queries in flight when one of two backends crashes."""

    def test_all_queries_are_answered(self):
        cfg = harness.CONFIG
        with Cluster(1) as cluster:
            doomed = spawn(
                "backend",
                cfg.lang("backend"),
                mx=cluster.addresses,
                name="doomed",
                type=C.peers.TEST_BACKEND_A,
                serves={C.types.TEST_REQUEST_A: C.types.TEST_RESPONSE},
                behaviour="upper",
                crash_after=CRASH_AFTER,
            )
            doomed.wait_for("connected", connections=1)
            survivor = spawn(
                "backend",
                cfg.lang("backend"),
                mx=cluster.addresses,
                name="survivor",
                type=C.peers.TEST_BACKEND_A,
                serves={C.types.TEST_REQUEST_A: C.types.TEST_RESPONSE},
                behaviour="upper",
            )
            survivor.wait_for("connected", connections=1)

            client = spawn(
                "client",
                cfg.lang("client"),
                mx=cluster.addresses,
                type=C.peers.TEST_ACTIVE_CLIENT,
                threaded=True,
                **{"async": IN_FLIGHT},
                count=QUERIES,
                timeout=5,
                query=[(C.types.TEST_REQUEST_A, "q{round}")],
            )
            self.assertEqual(0, client.wait(timeout=120))
            self.assertEqual(3, doomed.wait(), "the doomed backend exits with 3")
            self.assertEqual([], client.events_of("error"), "query() recovers on its own")
            responses = client.events_of("response")
            self.assertEqual(QUERIES, len(responses))
            self.assertEqual(sorted("Q%d" % i for i in range(QUERIES)), sorted(r["payload"] for r in responses))
            self.assertEqual(0, survivor.stop())


if __name__ == "__main__":
    harness.main()
