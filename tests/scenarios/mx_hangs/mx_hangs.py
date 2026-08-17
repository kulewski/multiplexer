"""one of two multiplexers hangs without closing its sockets; queries survive, some slowly.

A frozen multiplexer (SIGSTOP) is worse than a dead one: nothing tells the
peers, so a client keeps using the connection until its heartbeats go
unanswered. A query that picks it waits out its timeout, then finds the
backend through the search on the other multiplexer and is answered. After
the heartbeat drop interval the client's own timers close the connection
and nothing is slow any more. When the multiplexer comes back, the client
reconnects. This characterizes the cost: every query is answered, the slow
ones happen only before the drop, and none is slower than timeout plus a
little. Slow: it waits out the drop interval.
"""

import time
import unittest

from tests import harness
from tests.harness import Cluster, constants as C, spawn

QUERIES = 80
TIMEOUT = 4.0
HANG_FOR = 100  # past NO_HEARTBIT_SO_PREPARE_DROP + NO_HEARTBIT_SO_REALLY_DROP (30 + 60)


class MxHangs(unittest.TestCase):
    """Checks that a hung multiplexer only slows queries until its heartbeats are missed."""

    def test_queries_survive_a_frozen_multiplexer(self):
        cfg = harness.CONFIG
        with Cluster(2) as cluster:
            backend = spawn(
                "backend",
                cfg.lang("backend"),
                mx=cluster.addresses,
                type=C.peers.TEST_BACKEND_A,
                serves={C.types.TEST_REQUEST_A: C.types.TEST_RESPONSE},
                behaviour="upper",
            )
            backend.wait_for("connected", connections=2)
            client = spawn(
                "client",
                cfg.lang("client"),
                mx=cluster.addresses,
                type=C.peers.TEST_ACTIVE_CLIENT,
                threaded=True,
                count=QUERIES,
                sleep_between=1.5,
                timeout=TIMEOUT,
                query=[(C.types.TEST_REQUEST_A, "q{round}")],
            )
            client.wait_for("response", round=1)
            frozen_at = time.time()
            cluster.mx[0].pause()
            time.sleep(HANG_FOR)
            cluster.mx[0].resume()
            resumed_at = time.time()
            self.assertEqual(0, client.wait(timeout=300))

            self.assertEqual([], client.events_of("error"), "every query is answered, slowly or not")
            responses = client.events_of("response")
            self.assertEqual(QUERIES, len(responses))
            slow = [r for r in responses if r["ms"] > 1000]
            self.assertTrue(slow, "some query must have picked the frozen connection")
            self.assertLess(
                max(r["ms"] for r in slow), (TIMEOUT + 1.5) * 1000, "a slow query costs one timeout, not more"
            )
            # Round robin over two connections: every other query pays the
            # timeout until the heartbeat drop, about 90 s, then none does.
            slowest_possible = int((30 + 60 + 5) / (1.5 + TIMEOUT / 2)) + 2
            self.assertLessEqual(len(slow), slowest_possible, "slow queries kept coming after the drop interval")
            self.assertLess(max(r["round"] for r in slow), QUERIES - 15, "the last queries, after the drop, are fast")
            self.assertGreater(resumed_at, frozen_at)
            self.assertEqual(
                2, client.events_of("done")[0]["connections"], "reconnected after the multiplexer came back"
            )
            self.assertEqual(0, backend.stop())


if __name__ == "__main__":
    harness.main()
