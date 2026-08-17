"""A plain py_test, not under mx_integration_test: Cluster() with no rules
argument still runs the multiplexer with this workspace's rules file, the
one the generated constants come from, because the testing package looks
it up through the @mx//:multiplexer_rules flag."""

import unittest

from multiplexer import testing
from multiplexer.multiplexer_constants import peers, types
from multiplexer.testing import Cluster, FakePeer, TestClient


class DefaultRulesTest(unittest.TestCase):
    def test_cluster_runs_with_this_workspace_s_rules(self):
        self.assertIsNone(testing.CONFIG, "no macro configured this test")
        self.assertTrue(testing.default_rules().endswith("echo.rules"), testing.default_rules())
        with (
            Cluster(1) as cluster,
            FakePeer(cluster, peers.ECHO_BACKEND) as backend,
            TestClient(cluster, peers.ECHO_CLIENT) as client,
        ):
            backend.reply_with(types.ECHO_REQUEST, b"routed by echo.rules", types.ECHO_RESPONSE)
            self.assertEqual(b"routed by echo.rules", client.query(b"?", types.ECHO_REQUEST).message)


if __name__ == "__main__":
    unittest.main()
