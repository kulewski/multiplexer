"""a passive client idles past both drop intervals and still works."""

import unittest

from tests import harness
from tests.harness import Cluster, constants as C, spawn

IDLE = 95  # > NO_HEARTBIT_SO_PREPARE_DROP_INTERVAL + NO_HEARTBIT_SO_REALLY_DROP_INTERVAL


class IdleClient(unittest.TestCase):
    """Checks that a passive client idles past both drop intervals and still works."""

    def test_second_query_after_long_idle_succeeds(self):
        cfg = harness.CONFIG
        with Cluster(1) as cluster:
            backend = spawn(
                "backend",
                cfg.lang("backend"),
                mx=cluster.addresses,
                type=C.peers.TEST_BACKEND_A,
                serves={C.types.TEST_REQUEST_A: C.types.TEST_RESPONSE},
                behaviour="upper",
            )
            backend.wait_for("connected", connections=1)
            client = spawn(
                "client",
                cfg.lang("client"),
                mx=cluster.addresses,
                type=C.peers.TEST_CLIENT,
                count=2,
                sleep_between=IDLE,
                timeout=10,
                query=[(C.types.TEST_REQUEST_A, "q{round}")],
            )
            self.assertEqual(0, client.wait(timeout=IDLE + 60))
            self.assertEqual([], client.events_of("error"))
            self.assertEqual(["Q0", "Q1"], [r["payload"] for r in client.events_of("response")])
            done = client.events_of("done")[0]
            self.assertEqual(1, done["connections"], "same connection, never dropped")
            self.assertEqual(client.events_of("connected")[0]["instance_id"], done["instance_id"])


if __name__ == "__main__":
    harness.main()
