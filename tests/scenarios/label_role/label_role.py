"""a binary named by its label plays the backend role next to the shipped client.

mx_integration_test's roles may name a binary of your own instead of "py"
or "cc": here upper_backend.py, the smallest program following the role
contract, plays the backend while the shipped client role queries it.
"""

import unittest

from tests import harness
from tests.harness import Cluster, constants as C, spawn


class LabelRole(unittest.TestCase):
    """The scenario sees the label-named role as "bin" and spawns it like any other."""

    def test_own_binary_answers_the_shipped_client(self):
        cfg = harness.CONFIG
        self.assertEqual("bin", cfg.lang("backend"))
        with Cluster(cfg.mx) as cluster:
            backend = spawn("backend", cfg.lang("backend"), mx=cluster.addresses, type=C.peers.TEST_BACKEND_A)
            backend.wait_for("connected", connections=cfg.mx)
            cluster.wait_for_peer("TEST_BACKEND_A")
            client = spawn(
                "client",
                cfg.lang("client"),
                mx=cluster.addresses,
                type=C.peers.TEST_CLIENT,
                query=[(C.types.TEST_REQUEST_A, "hello")],
            )
            self.assertEqual(0, client.wait())
            self.assertEqual("HELLO", client.events_of("response")[0]["payload"])
            backend.wait_for("request", type=C.types.TEST_REQUEST_A)
            self.assertEqual(0, backend.stop())


if __name__ == "__main__":
    harness.main()
