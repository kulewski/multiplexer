"""a backend decides for itself when its drain is over, by overriding drained().

The only backend is asked to leave after a few requests, with a drain period
of half a second but a rule of its own: it refuses to leave before it has
served every request of the run. The drain lasts as long as the rule says,
the backend serves everything, and exits cleanly only then.
"""

import unittest

from tests import harness
from tests.harness import Cluster, constants as C, spawn

QUERIES = 25


class BackendDrainsUntilDone(unittest.TestCase):
    """Checks that an overridden drained() keeps the backend serving past the drain period."""

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
            )
            backend.wait_for("connected", connections=1)
            client = spawn(
                "client",
                cfg.lang("client"),
                mx=cluster.addresses,
                type=C.peers.TEST_CLIENT,
                count=QUERIES,
                sleep_between=0.1,
                timeout=10,
                query=[(C.types.TEST_REQUEST_A, "q{round}")],
            )
            client.wait_for("response", round=4)
            backend.request_drain()
            backend.wait_for("draining")
            self.assertEqual(0, client.wait(timeout=60))
            self.assertEqual(QUERIES, len(client.events_of("response")), "every query was answered")
            self.assertEqual([], client.events_of("error"))
            self.assertEqual(0, backend.wait(timeout=30), "the backend exits cleanly once its own condition holds")
            self.assertEqual(QUERIES, backend.events_of("stopped")[0]["handled"])
            served_after_drain = [
                event
                for event in backend.events[backend.events.index(backend.events_of("draining")[0]) :]
                if event["event"] == "request"
            ]
            self.assertGreater(len(served_after_drain), 10, "it kept serving well past the half-second period")


if __name__ == "__main__":
    harness.main()
