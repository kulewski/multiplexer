"""the multiplexer restarts under a live backend and a passive client; nobody notices.

The backend, which runs the loop, reconnects after AUTO_RECONNECT_TIME. The
client's next call finds its connection dead, waits for the reconnect inside
the call and sends again, so every query is answered and none fails.
"""

import unittest

from tests import harness
from tests.harness import Cluster, constants as C, spawn

PAUSE = 8


class MxRestarts(unittest.TestCase):
    """Checks that the multiplexer restarts under a live backend and a passive client; nobody notices."""

    def test_queries_resume_after_restart(self):
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
                type=C.peers.TEST_CLIENT,
                count=3,
                sleep_between=PAUSE,
                timeout=5,
                query=[(C.types.TEST_REQUEST_A, "q{round}")],
            )
            client.wait_for("response", round=0)
            cluster.mx[0].restart()
            self.assertEqual(0, client.wait(timeout=120))

            self.assertEqual([], client.events_of("error"), "no query fails across the restart")
            self.assertEqual(
                ["Q0", "Q1", "Q2"], [client.events_of("response", round=r)[0]["payload"] for r in range(3)]
            )
            second = client.events_of("response", round=1)[0]
            self.assertLess(second["ms"], 5000, "the second query waits for the reconnect, not for its timeout")
            self.assertEqual(
                3,
                len(backend.events_of("request")),
            )


if __name__ == "__main__":
    harness.main()
