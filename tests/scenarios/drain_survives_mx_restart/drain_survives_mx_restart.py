"""a multiplexer restarts under a draining backend: the backend registers again as draining, and nothing new reaches it.

One multiplexer, two backends of one type. One drains with a long period
and a rule of its own that keeps it from leaving, so it stays draining.
The multiplexer restarts; both backends reconnect within 3 s, the draining
one with its routing in its welcome, so the multiplexer routes it nothing
from its first request on: every query goes to the other backend, none
fails, and the draining one served nothing after the restart.
"""

import unittest

from tests import harness
from tests.harness import Cluster, constants as C, spawn


class DrainSurvivesMxRestart(unittest.TestCase):
    """The drain routing travels in the welcome, so a reconnect cannot lose it."""

    def backend(self, cluster: Cluster, name: str, **options):
        """A TEST_BACKEND_A serving TEST_REQUEST_A, connected."""
        role = spawn(
            "backend",
            harness.CONFIG.lang("backend"),
            mx=cluster.addresses,
            name=name,
            type=C.peers.TEST_BACKEND_A,
            serves={C.types.TEST_REQUEST_A: C.types.TEST_RESPONSE},
            **options,
        )
        role.wait_for("connected", connections=1)
        return role

    def ask(self, cluster: Cluster, count: int) -> list:
        """`count` queries from a fresh client, all answered: the responses."""
        client = spawn(
            "client",
            harness.CONFIG.lang("client"),
            mx=cluster.addresses,
            type=C.peers.TEST_CLIENT,
            count=count,
            timeout=10,
            query=[(C.types.TEST_REQUEST_A, "q{round}")],
        )
        self.assertEqual(0, client.wait())
        self.assertEqual([], client.events_of("error"))
        responses = client.events_of("response")
        self.assertEqual(count, len(responses))
        return responses

    def test_the_drain_routing_is_in_the_welcome_after_a_restart(self):
        with Cluster(1) as cluster:
            draining = self.backend(cluster, "draining", drain_seconds=60, drain_min_handled=1000)
            staying = self.backend(cluster, "staying")
            self.ask(cluster, 6)
            draining.request_drain()
            acked = draining.wait_for("acked")
            served_before = len(draining.events_of("request"))
            self.assertGreater(served_before, 0, "it took its share before the drain")

            cluster.mx[0].restart()
            cluster.wait_for_peer(C.peers.TEST_BACKEND_A, count=2, timeout=15)  # both back, within 3 s
            responses = self.ask(cluster, 10)
            self.assertEqual({staying.events_of("connected")[0]["instance_id"]}, {r["sender"] for r in responses})
            after = draining.events[draining.events.index(acked) + 1 :]
            self.assertEqual([], [event for event in after if event["event"] == "request"], "nothing after the restart")
            self.assertEqual(served_before, len(draining.events_of("request")))
            self.assertEqual(0, staying.stop())
            draining.kill()  # its rule never lets it leave, and a draining role ignores a second request


if __name__ == "__main__":
    harness.main()
