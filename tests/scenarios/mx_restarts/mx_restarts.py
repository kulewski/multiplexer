"""the multiplexer restarts under a live backend and a passive client; nobody notices.

The backend, a `BaseMultiplexerServer` that runs its loop all the time,
reconnects after AUTO_RECONNECT_TIME. The client's next call finds its
connection dead before it writes, waits for the reconnect inside the call
and sends then, once, so every query is answered and none fails. The
client is frozen in its idle time across the restart, until the backend
is registered again: a passive client notices nothing meanwhile, and its
next call comes after both, however long the restart takes.
"""

import unittest

from tests import harness
from tests.harness import Cluster, constants as C, spawn

PAUSE = 8  # s the client idles between queries, in which the test freezes it after an answer
TIMEOUT = 30  # s a query's stage: a failure detector, which a query that waited for it would take whole
BOUND = 30  # s a wait for a request event may take: a failure detector


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
                timeout=TIMEOUT,
                query=[(C.types.TEST_REQUEST_A, "q{round}")],
            )
            client.wait_for("response", round=0)
            client.pause()
            cluster.mx[0].restart()
            cluster.wait_for_peer(C.peers.TEST_BACKEND_A)
            client.resume()
            self.assertEqual(0, client.wait(timeout=120))

            self.assertEqual([], client.events_of("error"), "no query fails across the restart")
            self.assertEqual(
                ["Q0", "Q1", "Q2"], [client.events_of("response", round=r)[0]["payload"] for r in range(3)]
            )
            second = client.events_of("response", round=1)[0]
            self.assertLess(
                second["ms"], TIMEOUT * 1000, "the second query waits for the reconnect, not for its timeout"
            )
            self.assertGreaterEqual(len(backend.wait_for_count("request", 3, timeout=BOUND)), 3)


if __name__ == "__main__":
    harness.main()
