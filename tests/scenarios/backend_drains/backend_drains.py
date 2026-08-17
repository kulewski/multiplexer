"""a backend asked to leave drains: it declines searches, serves what arrives, then exits.

Two backends share the requests of a client. One is asked to leave, the
way a preStop hook asks, by a file its periodic_task() notices, and it has a
drain period: from then on it does not answer the search clients use to
find a backend, so no retried request is sent to it, but it keeps serving
the requests the multiplexer still routes to it until the period ends and
it exits. Every query is answered and the client never had to wait for a
timeout: the graceful shape of a backend restart.
"""

import unittest

from tests import harness
from tests.harness import Cluster, constants as C, spawn

QUERIES = 60
DRAIN_SECONDS = 3.0


class BackendDrains(unittest.TestCase):
    """Checks that a draining backend keeps serving and costs nobody a timeout."""

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
            leaving.request_drain()
            self.assertEqual(0, leaving.wait(timeout=30), "the backend exits cleanly once drained")
            draining = leaving.events_of("draining")
            self.assertEqual(1, len(draining))
            after_drain = leaving.events[leaving.events.index(draining[0]) :]
            self.assertTrue(
                [event for event in after_drain if event["event"] == "request"],
                "the draining backend kept serving what was routed to it",
            )
            self.assertEqual(0, client.wait(timeout=120))
            self.assertEqual([], client.events_of("error"))
            responses = client.events_of("response")
            self.assertEqual(QUERIES, len(responses))
            self.assertLess(max(r["ms"] for r in responses), 2000, "no query paid a timeout for the restart")
            self.assertEqual(0, staying.stop())


if __name__ == "__main__":
    harness.main()
