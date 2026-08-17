"""two backends of one type share the queries of one client."""

import unittest

from tests import harness
from tests.harness import Cluster, constants as C, spawn

QUERIES = 20


class RoundRobin(unittest.TestCase):
    """Checks that two backends of one type share the queries of one client."""

    def test_queries_are_spread_over_both_backends(self):
        cfg = harness.CONFIG
        with Cluster(cfg.mx) as cluster:
            backends = []
            for i in range(2):
                b = spawn(
                    "backend",
                    cfg.lang("backend"),
                    mx=cluster.addresses,
                    type=C.peers.TEST_BACKEND_A,
                    name="backend%d" % i,
                    serves={C.types.TEST_REQUEST_A: C.types.TEST_RESPONSE},
                    behaviour="echo",
                )
                b.wait_for("connected", connections=cfg.mx)
                backends.append(b)
            ids = {b.events_of("connected")[0]["instance_id"] for b in backends}
            self.assertEqual(2, len(ids))

            client = spawn(
                "client",
                cfg.lang("client"),
                mx=cluster.addresses,
                type=C.peers.TEST_CLIENT,
                count=QUERIES,
                query=[(C.types.TEST_REQUEST_A, "q{round}")],
            )
            self.assertEqual(0, client.wait())
            responses = client.events_of("response")
            self.assertEqual([], client.events_of("error"))
            self.assertEqual(QUERIES, len(responses))

            by_backend = {}
            for r in responses:
                by_backend[r["from_"]] = by_backend.get(r["from_"], 0) + 1
            self.assertEqual(ids, set(by_backend), "every backend answered something")
            self.assertGreaterEqual(min(by_backend.values()), QUERIES // 4, "split is roughly even: %r" % by_backend)
            for b in backends:
                self.assertEqual(0, b.stop())


if __name__ == "__main__":
    harness.main()
