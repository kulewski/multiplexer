"""a backend decides for itself when its drain is over, by overriding drained(); alone of its type, it drains as the last resort.

The only backend is asked to leave after a few requests, with a drain period
of half a second but a rule of its own: it refuses to leave before it has
served every request of the run. Being alone of its type it drains as the
last resort, so the multiplexer keeps routing to it what nobody else could
take. A first client's five requests come before it is asked, a second
client's twenty after its drain began, one every 100 ms, the last of them
seconds after the period is over. The drain lasts as long as the rule
says, the backend serves everything, and exits cleanly only then.
"""

import unittest

from tests import harness
from tests.harness import Cluster, constants as C, spawn

QUERIES = 25  # the backend's rule: it does not leave before it served this many
BEFORE = 5  # of them from a first client, before the backend is asked to leave
TIMEOUT = 30  # s a query's stage: a failure detector


class BackendDrainsUntilDone(unittest.TestCase):
    """Checks that an overridden drained() keeps the backend serving past the drain period."""

    def ask(self, cluster: Cluster, name: str, count: int, sleep_between: float = 0.0):
        """`count` queries from a fresh client, answered or failed: the client role."""
        client = spawn(
            "client",
            harness.CONFIG.lang("client"),
            mx=cluster.addresses,
            name=name,
            type=C.peers.TEST_CLIENT,
            count=count,
            sleep_between=sleep_between,
            timeout=TIMEOUT,
            query=[(C.types.TEST_REQUEST_A, "q{round}")],
        )
        self.assertEqual(0, client.wait(timeout=120))
        return client

    def test_drain_waits_for_the_backends_own_condition(self):
        cfg = harness.CONFIG
        with Cluster(1) as cluster:
            backend = spawn(
                "backend",
                cfg.lang("backend"),
                mx=cluster.addresses,
                type=C.peers.TEST_BACKEND_A,
                serves={C.types.TEST_REQUEST_A: C.types.TEST_RESPONSE},
                behaviour="upper",
                drain_seconds=0.5,
                drain_min_handled=QUERIES,
                drain_routing="last_resort",
            )
            backend.wait_for("connected", connections=1)
            before = self.ask(cluster, "before", BEFORE)
            backend.request_drain()
            backend.wait_for("draining")
            after = self.ask(cluster, "after", QUERIES - BEFORE, sleep_between=0.1)
            clients = (before, after)
            responses = [event for client in clients for event in client.events_of("response")]
            self.assertEqual(QUERIES, len(responses), "every query was answered")
            self.assertEqual([], [event for client in clients for event in client.events_of("error")])
            self.assertEqual(0, backend.wait(timeout=30), "the backend exits cleanly once its own condition holds")
            self.assertEqual(QUERIES, backend.events_of("stopped")[0]["handled"])
            served_after_drain = [
                event
                for event in backend.events[backend.events.index(backend.events_of("draining")[0]) :]
                if event["event"] == "request"
            ]
            self.assertGreaterEqual(
                len(served_after_drain), QUERIES - BEFORE, "it kept serving well past the half-second period"
            )


if __name__ == "__main__":
    harness.main()
