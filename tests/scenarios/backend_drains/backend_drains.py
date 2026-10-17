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

import subprocess
import unittest

from tests import harness
from tests.harness import Cluster, constants as C, spawn

QUERIES = 60
# The cap, longer than any wait here: the drain ends when the multiplexer
# has confirmed and the work is done, and one that waited for its cap
# would outlast the wait for the backend's exit.
DRAIN_SECONDS = 120.0
TIMEOUT = 60  # s a query's stage, which a query that waited for it would take whole
BOUND = 30  # s a wait for something that must happen may take: a failure detector


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
                timeout=TIMEOUT,
                query=[(C.types.TEST_REQUEST_A, "q{round}")],
            )
            client.wait_for("response", round=8)
            leaving.request_drain()
            leaving.wait_for("draining")
            acked = leaving.wait_for("acked")
            try:
                code = leaving.wait(timeout=BOUND)
            except subprocess.TimeoutExpired:
                self.fail(
                    "the backend did not leave once confirmed and idle: it waits for its %d s period" % DRAIN_SECONDS
                )
            self.assertEqual(0, code, "the backend exits cleanly once drained")
            after_ack = leaving.events[leaving.events.index(acked) + 1 :]
            self.assertEqual(
                [], [event for event in after_ack if event["event"] == "request"], "nothing new after the confirmation"
            )
            self.assertEqual(0, client.wait(timeout=120))
            self.assertEqual([], client.events_of("error"))
            responses = client.events_of("response")
            self.assertEqual(QUERIES, len(responses))
            self.assertLess(max(r["ms"] for r in responses), TIMEOUT * 1000, "no query paid a timeout for the restart")
            handled = leaving.events_of("stopped")[0]["handled"]
            served = len(staying.wait_for_count("request", QUERIES - handled, timeout=BOUND))
            self.assertGreaterEqual(served + handled, QUERIES)
            self.assertEqual(0, staying.stop())


if __name__ == "__main__":
    harness.main()
