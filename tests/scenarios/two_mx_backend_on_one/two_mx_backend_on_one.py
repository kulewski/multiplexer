"""two multiplexers, the backend behind only one; the client finds it."""

import unittest

from tests import harness
from tests.harness import Cluster, constants as C, spawn

QUERIES = 6


class TwoMxBackendOnOne(unittest.TestCase):
    """Checks that two multiplexers, the backend behind only one; the client finds it."""

    def test_queries_find_the_backend_behind_the_other_mx(self):
        cfg = harness.CONFIG
        with Cluster(2) as cluster:
            backend = spawn(
                "backend",
                cfg.lang("backend"),
                mx=[cluster.mx[1].address],
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
                count=QUERIES,
                timeout=5,
                query=[(C.types.TEST_REQUEST_A, "q{round}")],
            )
            client.wait_for("connected", connections=2)
            self.assertEqual(0, client.wait())
            self.assertEqual([], client.events_of("error"))
            responses = client.events_of("response")
            self.assertEqual(QUERIES, len(responses))
            self.assertEqual(["Q%d" % i for i in range(QUERIES)], [r["payload"] for r in responses])
            self.assertEqual(QUERIES, len(backend.events_of("request")))


if __name__ == "__main__":
    harness.main()
