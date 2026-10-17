"""two request types are routed to their own backend types."""

import unittest

from tests import harness
from tests.harness import Cluster, constants as C, spawn

BOUND = 30  # s a wait for the backends' request events may take: a failure detector


class TwoRequestTypes(unittest.TestCase):
    """Checks that two request types are routed to their own backend types."""

    def test_routing_by_message_type(self):
        cfg = harness.CONFIG
        with Cluster(cfg.mx) as cluster:
            a = spawn(
                "backend",
                cfg.lang("backend_a"),
                mx=cluster.addresses,
                name="backend-a",
                type=C.peers.TEST_BACKEND_A,
                serves={C.types.TEST_REQUEST_A: C.types.TEST_RESPONSE},
                behaviour="upper",
            )
            b = spawn(
                "backend",
                cfg.lang("backend_b"),
                mx=cluster.addresses,
                name="backend-b",
                type=C.peers.TEST_BACKEND_B,
                serves={C.types.TEST_REQUEST_B: C.types.TEST_RESPONSE},
                behaviour="echo",
            )
            a.wait_for("connected", connections=cfg.mx)
            b.wait_for("connected", connections=cfg.mx)
            a_id = a.events_of("connected")[0]["instance_id"]
            b_id = b.events_of("connected")[0]["instance_id"]

            client = spawn(
                "client",
                cfg.lang("client"),
                mx=cluster.addresses,
                type=C.peers.TEST_CLIENT,
                count=3,
                query=[(C.types.TEST_REQUEST_A, "abc"), (C.types.TEST_REQUEST_B, "abc")],
            )
            self.assertEqual(0, client.wait())
            self.assertEqual([], client.events_of("error"))
            for r in client.events_of("response", query_type=C.types.TEST_REQUEST_A):
                self.assertEqual((a_id, "ABC"), (r["sender"], r["payload"]))
            for r in client.events_of("response", query_type=C.types.TEST_REQUEST_B):
                self.assertEqual((b_id, "abc"), (r["sender"], r["payload"]))
            self.assertEqual(3, len(client.events_of("response", query_type=C.types.TEST_REQUEST_A)))
            self.assertEqual(3, len(client.events_of("response", query_type=C.types.TEST_REQUEST_B)))
            self.assertGreaterEqual(len(a.wait_for_count("request", 3, timeout=BOUND)), 3, "at least once each")
            self.assertGreaterEqual(len(b.wait_for_count("request", 3, timeout=BOUND)), 3, "at least once each")
            # Stopped, their output read to its end: however many times a
            # request of its own type came, none of the other type did.
            for backend, own in ((a, C.types.TEST_REQUEST_A), (b, C.types.TEST_REQUEST_B)):
                self.assertEqual(0, backend.stop())
                self.assertEqual({own}, {r["type"] for r in backend.events_of("request")}, "only its own type")


if __name__ == "__main__":
    harness.main()
