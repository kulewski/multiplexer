"""a backend asked to leave drains: the multiplexer routes it nothing new, it serves what it holds, then exits.

Two backends share the requests of a client. One is asked to leave, the
way a preStop hook asks, by a file its periodic_task() notices, and it has
a drain period: it tells the multiplexer to route it nothing new by the
rules, the multiplexer confirms, and from that moment every request goes
to the other backend. The leaving one serves what was already on its way,
refuses nothing, and exits as soon as its work is done, long before the
period is up. Every query is answered and the client never waited for a
timeout: the graceful shape of a backend restart.
"""

import time
import unittest

from tests import harness
from tests.harness import Cluster, constants as C, spawn

QUERIES = 60
DRAIN_SECONDS = 10.0  # the cap; the drain ends when the multiplexer has confirmed and the work is done


class BackendDrains(unittest.TestCase):
    """Checks that a draining backend gets nothing new, serves what it holds and costs nobody a timeout."""

    def test_drain_then_exit(self):
        cfg = harness.CONFIG
        with Cluster(1) as cluster:
            leaving = spawn(
                "backend",
                cfg.lang("backend"),
                mx=cluster.addresses,
                name="leaving",
                type=C.peers.TEST_BACKEND_A,
                serves={C.types.TEST_REQUEST_A: C.types.TEST_RESPONSE},
                behaviour="upper",
                drain_seconds=DRAIN_SECONDS,
            )
            leaving.wait_for("connected", connections=1)
            staying = spawn(
                "backend",
                cfg.lang("backend"),
                mx=cluster.addresses,
                name="staying",
                type=C.peers.TEST_BACKEND_A,
                serves={C.types.TEST_REQUEST_A: C.types.TEST_RESPONSE},
                behaviour="upper",
            )
            staying.wait_for("connected", connections=1)
            client = spawn(
                "client",
                cfg.lang("client"),
                mx=cluster.addresses,
                type=C.peers.TEST_ACTIVE_CLIENT,
                threaded=True,
                **{"async": 4},
                count=QUERIES,
                sleep_between=0.1,
                timeout=10,
                query=[(C.types.TEST_REQUEST_A, "q{round}")],
            )
            client.wait_for("response", round=8)
            asked = time.time()
            leaving.request_drain()
            leaving.wait_for("draining")
            acked = leaving.wait_for("acked")
            self.assertEqual(0, leaving.wait(timeout=30), "the backend exits cleanly once drained")
            self.assertLess(time.time() - asked, DRAIN_SECONDS, "it left once confirmed and idle, not after the period")
            after_ack = leaving.events[leaving.events.index(acked) + 1 :]
            self.assertEqual(
                [], [event for event in after_ack if event["event"] == "request"], "nothing new after the confirmation"
            )
            self.assertEqual(0, client.wait(timeout=120))
            self.assertEqual([], client.events_of("error"))
            responses = client.events_of("response")
            self.assertEqual(QUERIES, len(responses))
            self.assertLess(max(r["ms"] for r in responses), 2000, "no query paid a timeout for the restart")
            served = len(staying.wait_for_count("request", QUERIES - leaving.events_of("stopped")[0]["handled"]))
            self.assertGreaterEqual(served + leaving.events_of("stopped")[0]["handled"], QUERIES)
            self.assertEqual(0, staying.stop())


if __name__ == "__main__":
    harness.main()
