"""several clients query one backend at the same time without cross-talk."""

import unittest

from tests import harness
from tests.harness import Cluster, constants as C, spawn

WORKERS = 3
ROUNDS = 5


class ConcurrentClients(unittest.TestCase):
    """Checks that several clients query one backend at the same time without cross-talk."""

    def test_every_worker_gets_its_own_answers(self):
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
                parallel=WORKERS,
                count=ROUNDS,
                query=[(C.types.TEST_REQUEST_A, "w{worker}-r{round}")],
            )
            self.assertEqual(0, client.wait())
            self.assertEqual([], client.events_of("error"))
            responses = client.events_of("response")
            self.assertEqual(WORKERS * ROUNDS, len(responses))
            for r in responses:
                self.assertEqual("W%d-R%d" % (r["worker"], r["round"]), r["payload"])
            self.assertEqual(WORKERS * ROUNDS, len(backend.events_of("request")))
            self.assertEqual(0, backend.stop())


if __name__ == "__main__":
    harness.main()
