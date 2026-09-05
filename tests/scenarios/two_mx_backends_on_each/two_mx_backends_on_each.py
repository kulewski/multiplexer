"""two multiplexers with a backend behind each; a client on both."""

import unittest

from tests import harness
from tests.harness import Cluster, constants as C, spawn

QUERIES = 10


class TwoMxBackendsOnEach(unittest.TestCase):
    """Checks that two multiplexers with a backend behind each; a client on both."""

    def test_every_query_is_answered_by_a_backend(self):
        cfg = harness.CONFIG
        with Cluster(2) as cluster:
            backends = []
            for i, mx in enumerate(cluster.mx):
                b = spawn(
                    "backend",
                    cfg.lang("backend"),
                    mx=[mx.address],
                    name="backend%d" % i,
                    type=C.peers.TEST_BACKEND_A,
                    serves={C.types.TEST_REQUEST_A: C.types.TEST_RESPONSE},
                    behaviour="upper",
                )
                b.wait_for("connected", connections=1)
                backends.append(b)
            ids = {b.events_of("connected")[0]["instance_id"] for b in backends}

            client = spawn(
                "client",
                cfg.lang("client"),
                mx=cluster.addresses,
                type=C.peers.TEST_CLIENT,
                count=QUERIES,
                timeout=5,
                query=[(C.types.TEST_REQUEST_A, "q{round}")],
            )
            self.assertEqual(0, client.wait())
            self.assertEqual([], client.events_of("error"))
            responses = client.events_of("response")
            self.assertEqual(QUERIES, len(responses))
            for r in responses:
                self.assertIn(r["from_"], ids)
            handled = sum(len(b.events_of("request")) for b in backends)
            self.assertEqual(QUERIES, handled)


if __name__ == "__main__":
    harness.main()
