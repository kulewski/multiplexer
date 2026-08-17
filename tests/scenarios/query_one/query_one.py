"""one multiplexer, one backend, one client, one query."""

import unittest

from tests import harness
from tests.harness import Cluster, constants as C, spawn


class QueryOne(unittest.TestCase):
    """Checks that one multiplexer, one backend, one client, one query."""

    def test_one_query_is_answered(self):
        cfg = harness.CONFIG
        with Cluster(cfg.mx) as cluster:
            backend = spawn(
                "backend",
                cfg.lang("backend"),
                mx=cluster.addresses,
                type=C.peers.TEST_BACKEND_A,
                serves={C.types.TEST_REQUEST_A: C.types.TEST_RESPONSE},
                behaviour="upper",
            )
            backend.wait_for("connected", connections=cfg.mx)

            client = spawn(
                "client",
                cfg.lang("client"),
                mx=cluster.addresses,
                type=C.peers.TEST_CLIENT,
                query=[(C.types.TEST_REQUEST_A, "hello multiplexer")],
            )
            self.assertEqual(0, client.wait())

            self.assertEqual([], client.events_of("error"))
            responses = client.events_of("response")
            self.assertEqual(1, len(responses))
            self.assertEqual(C.types.TEST_RESPONSE, responses[0]["type"])
            self.assertEqual("HELLO MULTIPLEXER", responses[0]["payload"])
            self.assertEqual(1, len(backend.events_of("request", type=C.types.TEST_REQUEST_A)))
            self.assertEqual(0, backend.stop(), "backend exits cleanly on SIGTERM")


if __name__ == "__main__":
    harness.main()
