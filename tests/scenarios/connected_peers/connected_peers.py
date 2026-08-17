"""the multiplexer's peers file lists who is connected, rewritten on every change.

Two multiplexers each keep a --peers-file. A backend connecting to both
appears in each file with its instance id and type name; the harness's
wait_for_peer waits on exactly that; when the backend leaves, both files
drop it and wait_for_peer_gone returns.
"""

import unittest

from tests import harness
from tests.harness import Cluster, constants as C, spawn


class ConnectedPeers(unittest.TestCase):
    """Checks the peers file and wait_for_peer on a two-multiplexer cluster."""

    def test_peers_file_follows_registrations(self):
        cfg = harness.CONFIG
        with Cluster(2) as cluster:
            for multiplexer in cluster.mx:
                self.assertEqual([], multiplexer.connected_peers(), "nobody yet")
            backend = spawn(
                "backend",
                cfg.lang("backend"),
                mx=cluster.addresses,
                type=C.peers.TEST_BACKEND_A,
                serves={C.types.TEST_REQUEST_A: C.types.TEST_RESPONSE},
            )
            backend_id = backend.wait_for("connected", connections=2)["instance_id"]
            cluster.wait_for_peer("TEST_BACKEND_A")
            cluster.wait_for_peer(C.peers.TEST_BACKEND_A)
            for multiplexer in cluster.mx:
                self.assertEqual(
                    [(backend_id, "TEST_BACKEND_A", C.peers.TEST_BACKEND_A)], multiplexer.connected_peers()
                )
            with self.assertRaises(TimeoutError):
                cluster.wait_for_peer("TEST_BACKEND_B", timeout=0.5)
            self.assertEqual(0, backend.stop())
            cluster.wait_for_peer_gone("TEST_BACKEND_A", timeout=5)
            self.assertTrue(all(multiplexer.connected_peers() == [] for multiplexer in cluster.mx))


if __name__ == "__main__":
    harness.main()
