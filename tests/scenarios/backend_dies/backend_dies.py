"""one of two backends crashes mid-run; the survivor takes over."""

import unittest

from tests import harness
from tests.harness import Cluster, constants as C, spawn

QUERIES = 20
CRASH_AFTER = 5


class BackendDies(unittest.TestCase):
    """Checks that one of two backends crashes mid-run; the survivor takes over."""

    def test_survivor_answers_the_rest(self):
        cfg = harness.CONFIG
        with Cluster(cfg.mx) as cluster:
            doomed = spawn(
                "backend",
                cfg.lang("backend"),
                mx=cluster.addresses,
                name="doomed",
                type=C.peers.TEST_BACKEND_A,
                crash_after=CRASH_AFTER,
                serves={C.types.TEST_REQUEST_A: C.types.TEST_RESPONSE},
                behaviour="upper",
            )
            survivor = spawn(
                "backend",
                cfg.lang("backend"),
                mx=cluster.addresses,
                name="survivor",
                type=C.peers.TEST_BACKEND_A,
                serves={C.types.TEST_REQUEST_A: C.types.TEST_RESPONSE},
                behaviour="upper",
            )
            doomed.wait_for("connected", connections=cfg.mx)
            survivor.wait_for("connected", connections=cfg.mx)
            survivor_id = survivor.events_of("connected")[0]["instance_id"]

            client = spawn(
                "client",
                cfg.lang("client"),
                mx=cluster.addresses,
                type=C.peers.TEST_CLIENT,
                count=QUERIES,
                timeout=5,
                query=[(C.types.TEST_REQUEST_A, "q{round}")],
            )
            self.assertEqual(0, client.wait(timeout=120))
            self.assertEqual(3, doomed.wait(), "the doomed backend exits with 3")
            self.assertEqual(CRASH_AFTER, doomed.events_of("crash")[0]["handled"])

            responses = client.events_of("response")
            self.assertEqual([], client.events_of("error"), "query() recovers on its own")
            self.assertEqual(QUERIES, len(responses))
            self.assertEqual(["Q%d" % i for i in range(QUERIES)], [r["payload"] for r in responses])
            late = [r for r in responses if r["round"] >= CRASH_AFTER * 2]
            self.assertTrue(
                late and all(r["from_"] == survivor_id for r in late), "after the crash only the survivor answers"
            )


if __name__ == "__main__":
    harness.main()
