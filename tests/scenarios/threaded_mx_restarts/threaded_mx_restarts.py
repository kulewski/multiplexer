"""the multiplexer restarts with a ThreadedClient's queries in flight; all are answered.

Queries whose request was on the wire when the connection died are sent
again as soon as the client is reconnected, without waiting for a timeout;
the backend reconnects on its own as well.
"""

import unittest

from tests import harness
from tests.harness import Cluster, constants as C, spawn

QUERIES = 40
IN_FLIGHT = 4


class ThreadedMxRestarts(unittest.TestCase):
    """Checks that the multiplexer restarts with a ThreadedClient's queries in flight; all are answered."""

    def test_every_query_survives_the_restart(self):
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
                type=C.peers.TEST_ACTIVE_CLIENT,
                threaded=True,
                **{"async": IN_FLIGHT},
                count=QUERIES,
                sleep_between=0.2,
                timeout=15,
                query=[(C.types.TEST_REQUEST_A, "q{round}")],
            )
            client.wait_for("response", round=5)
            cluster.mx[0].restart()
            self.assertEqual(0, client.wait(timeout=120))
            self.assertEqual([], client.events_of("error"), "no query fails across the restart")
            responses = client.events_of("response")
            self.assertEqual(QUERIES, len(responses))
            self.assertEqual(sorted("Q%d" % index for index in range(QUERIES)), sorted(r["payload"] for r in responses))
            self.assertLess(max(r["ms"] for r in responses), 10000, "delayed by the reconnect, not by a timeout")
            self.assertEqual(1, client.events_of("done")[0]["connections"])
            self.assertEqual(0, backend.stop())


if __name__ == "__main__":
    harness.main()
