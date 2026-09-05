"""a 16 MiB query and its echo make the round trip intact."""

import unittest

from tests import harness
from tests.harness import Cluster, constants as C, spawn

SIZE = 16 * 1024 * 1024


class LargePayload(unittest.TestCase):
    """Checks that a 16 MiB query and its echo make the round trip intact."""

    def test_round_trip_of_a_large_message(self):
        cfg = harness.CONFIG
        with Cluster(1) as cluster:
            backend = spawn(
                "backend",
                cfg.lang("backend"),
                mx=cluster.addresses,
                type=C.peers.TEST_BACKEND_A,
                serves={C.types.TEST_REQUEST_A: C.types.TEST_RESPONSE},
                behaviour="echo",
            )
            backend.wait_for("connected", connections=1)
            client = spawn(
                "client",
                cfg.lang("client"),
                mx=cluster.addresses,
                type=C.peers.TEST_CLIENT,
                payload_size=SIZE,
                timeout=60,
                query=[(C.types.TEST_REQUEST_A, "ignored")],
            )
            self.assertEqual(0, client.wait(timeout=120))
            self.assertEqual([], client.events_of("error"))
            responses = client.events_of("response")
            self.assertEqual(1, len(responses))
            self.assertEqual(SIZE, responses[0]["size"])
            self.assertEqual(SIZE, backend.events_of("request")[0]["size"])


if __name__ == "__main__":
    harness.main()
