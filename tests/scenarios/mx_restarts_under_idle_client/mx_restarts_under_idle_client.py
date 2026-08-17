"""the multiplexer restarts while a passive client is idle; its next call still works.

The client learns of the dead connection inside that call, waits for the
reconnect there and sends again. A fresh client works too.
"""

import unittest

from tests import harness
from tests.harness import Cluster, constants as C, spawn

PAUSE = 8


class MxRestartsUnderIdleClient(unittest.TestCase):
    """Checks that the multiplexer restarts while a passive client is idle; its next call still works."""

    def test_next_call_is_recorded_and_new_client_works(self):
        cfg = harness.CONFIG
        with Cluster(1) as cluster:
            backend = spawn(
                "backend",
                cfg.lang("backend"),
                mx=cluster.addresses,
                type=C.peers.TEST_BACKEND_A,
                serves={C.types.TEST_REQUEST_A: C.types.TEST_RESPONSE},
                behaviour="upper",
            )
            backend.wait_for("connected", connections=1)
            client = spawn(
                "client",
                cfg.lang("client"),
                mx=cluster.addresses,
                name="old",
                type=C.peers.TEST_CLIENT,
                count=2,
                sleep_between=PAUSE,
                timeout=5,
                query=[(C.types.TEST_REQUEST_A, "q{round}")],
            )
            client.wait_for("response", round=0)
            cluster.mx[0].restart()
            self.assertEqual(0, client.wait(timeout=60))
            self.assertEqual([], client.events_of("error"), "the call after the restart succeeds")
            self.assertEqual("Q1", client.events_of("response", round=1)[0]["payload"])

            fresh = spawn(
                "client",
                cfg.lang("client"),
                mx=cluster.addresses,
                name="fresh",
                type=C.peers.TEST_CLIENT,
                timeout=10,
                query=[(C.types.TEST_REQUEST_A, "again")],
            )
            self.assertEqual(0, fresh.wait())
            self.assertEqual("AGAIN", fresh.events_of("response")[0]["payload"])


if __name__ == "__main__":
    harness.main()
