"""a multiplexer restarts with a ThreadedClient's queries in flight: what survives.

With one multiplexer, the only promise is that nothing hangs: the client
reconnects on its own, a query caught by the restart is sent again once
reconnected and is answered if the backend is back by then, or fails at
once with OperationFailed if the client reconnected first and the fresh
multiplexer had nobody of the type yet. Which order happens is chance; the
second test forces the good one by pausing the client until the backend
is registered again. With two multiplexers the queries in flight on the
dead connection go out again through the live one at once, and none fails
or waits: the deployment docs/semantics.md is written for.
"""

import unittest

from tests import harness
from tests.harness import Cluster, constants as C, spawn

QUERIES = 40
IN_FLIGHT = 4
TIMEOUT = 15


class ThreadedMxRestarts(unittest.TestCase):
    """Checks what a ThreadedClient's in-flight queries see across a multiplexer restart."""

    def start_backend(self, cluster, cfg):
        """One backend of type A on every multiplexer."""
        backend = spawn(
            "backend",
            cfg.lang("backend"),
            mx=cluster.addresses,
            type=C.peers.TEST_BACKEND_A,
            serves={C.types.TEST_REQUEST_A: C.types.TEST_RESPONSE},
            behaviour="upper",
        )
        backend.wait_for("connected", connections=len(cluster.mx))
        return backend

    def start_client(self, cluster, cfg):
        """A threaded client with IN_FLIGHT queries in flight, 200 ms apart."""
        client = spawn(
            "client",
            cfg.lang("client"),
            mx=cluster.addresses,
            type=C.peers.TEST_ACTIVE_CLIENT,
            threaded=True,
            **{"async": IN_FLIGHT},
            count=QUERIES,
            sleep_between=0.2,
            timeout=TIMEOUT,
            query=[(C.types.TEST_REQUEST_A, "q{round}")],
        )
        client.wait_for("connected", connections=len(cluster.mx))
        return client

    def test_one_multiplexer_nothing_hangs(self):
        """Every query resolves, by a reply or by an immediate failure, never by a timeout."""
        cfg = harness.CONFIG
        with Cluster(1) as cluster:
            backend = self.start_backend(cluster, cfg)
            client = self.start_client(cluster, cfg)
            client.wait_for("response", round=5)
            cluster.mx[0].restart()
            self.assertEqual(0, client.wait(timeout=120))
            responses = client.events_of("response")
            errors = client.events_of("error")
            self.assertEqual(QUERIES, len(responses) + len(errors), "every query resolved")
            for error in errors:
                self.assertEqual("OperationFailed", error["kind"], "a query caught by the restart fails at once")
            self.assertLess(
                max(r["ms"] for r in responses + errors), 10000, "delayed by the reconnect, not by a timeout"
            )
            tail = {r["round"] for r in responses if r["round"] >= QUERIES - 2 * IN_FLIGHT}
            self.assertEqual(
                set(range(QUERIES - 2 * IN_FLIGHT, QUERIES)), tail, "everything issued well after is answered"
            )
            self.assertEqual(1, client.events_of("done")[0]["connections"])
            self.assertEqual(0, backend.stop())

    def test_one_multiplexer_backend_back_first(self):
        """With the backend registered before the client reconnects, every query is answered."""
        cfg = harness.CONFIG
        with Cluster(1) as cluster:
            backend = self.start_backend(cluster, cfg)
            client = self.start_client(cluster, cfg)
            client.wait_for("response", round=5)
            client.pause()
            cluster.mx[0].restart()
            cluster.wait_for_peer(C.peers.TEST_BACKEND_A)
            client.resume()
            self.assertEqual(0, client.wait(timeout=120))
            self.assertEqual([], client.events_of("error"), "no query fails when the backend is back first")
            responses = client.events_of("response")
            self.assertEqual(QUERIES, len(responses))
            self.assertEqual(sorted("Q%d" % index for index in range(QUERIES)), sorted(r["payload"] for r in responses))
            self.assertLess(max(r["ms"] for r in responses), 10000, "delayed by the reconnect, not by a timeout")
            self.assertEqual(1, client.events_of("done")[0]["connections"])
            self.assertEqual(0, backend.stop())

    def test_one_of_two_multiplexers(self):
        """The queries in flight on the dead connection go through the other one at once."""
        cfg = harness.CONFIG
        with Cluster(2) as cluster:
            backend = self.start_backend(cluster, cfg)
            client = self.start_client(cluster, cfg)
            client.wait_for("response", round=5)
            cluster.mx[0].restart()
            self.assertEqual(0, client.wait(timeout=120))
            self.assertEqual([], client.events_of("error"), "no query fails while another multiplexer is up")
            responses = client.events_of("response")
            self.assertEqual(QUERIES, len(responses))
            self.assertEqual(sorted("Q%d" % index for index in range(QUERIES)), sorted(r["payload"] for r in responses))
            slow = [(r["round"], round(r["ms"])) for r in responses if r["ms"] > 2500]
            self.assertEqual([], slow, "queries that waited for the reconnect instead of using the other connection")
            self.assertEqual(2, client.events_of("done")[0]["connections"], "the client is back on both")
            self.assertEqual(0, backend.stop())


if __name__ == "__main__":
    harness.main()
