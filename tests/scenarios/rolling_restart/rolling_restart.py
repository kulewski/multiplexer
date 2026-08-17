"""three multiplexers restarted one after another under traffic; nobody notices.

The rolling restart of a datacenter deployment: every peer is connected to
every multiplexer, one multiplexer goes down and comes back, then the next,
then the last, with enough time between them for everyone to reconnect. A
client keeps querying throughout, and a sender keeps sending events. Every
query is answered, every send succeeds, and no query has to wait for a
reconnect: there is always another connection to use.
"""

import time
import unittest

from tests import harness
from tests.harness import Cluster, constants as C, spawn

MULTIPLEXERS = 3
QUERIES = 60
BETWEEN_RESTARTS = 5.0  # AUTO_RECONNECT_TIME plus margin, so everyone is back before the next
FAST_ENOUGH_MS = 2500  # far below a timeout, and below the 3 s reconnect: another connection served it


class RollingRestart(unittest.TestCase):
    """Checks that a rolling restart of every multiplexer costs nothing."""

    def start_backends(self, cluster, cfg):
        """Two backends, each connected to every multiplexer."""
        backends = []
        for name in ("a", "b"):
            backend = spawn(
                "backend",
                cfg.lang("backend"),
                mx=cluster.addresses,
                name=name,
                type=C.peers.TEST_BACKEND_A,
                serves={C.types.TEST_REQUEST_A: C.types.TEST_RESPONSE},
                behaviour="upper",
            )
            backend.wait_for("connected", connections=MULTIPLEXERS)
            backends.append(backend)
        return backends

    def roll(self, cluster, client):
        """Restart each multiplexer in turn while `client` keeps working."""
        for multiplexer in cluster.mx:
            client.wait_for("response", timeout=60, round=min(QUERIES - 1, len(client.events_of("response")) + 3))
            multiplexer.restart()
            time.sleep(BETWEEN_RESTARTS)

    def check_queries(self, client, backends):
        """Every query answered, none slow, everything reconnected."""
        self.assertEqual(0, client.wait(timeout=120))
        self.assertEqual([], client.events_of("error"), "no query fails during the rolling restart")
        responses = client.events_of("response")
        self.assertEqual(QUERIES, len(responses))
        slow = [(r["round"], round(r["ms"])) for r in responses if r["ms"] > FAST_ENOUGH_MS]
        self.assertEqual([], slow, "queries that had to wait for a reconnect instead of using another connection")
        self.assertEqual(MULTIPLEXERS, client.events_of("done")[0]["connections"], "the client is back on all of them")
        self.assertEqual(QUERIES, sum(len(b.events_of("request")) for b in backends))
        for backend in backends:
            self.assertEqual(0, backend.stop())

    def test_synchronous_client(self):
        cfg = harness.CONFIG
        with Cluster(MULTIPLEXERS) as cluster:
            backends = self.start_backends(cluster, cfg)
            client = spawn(
                "client",
                cfg.lang("client"),
                mx=cluster.addresses,
                type=C.peers.TEST_CLIENT,
                count=QUERIES,
                sleep_between=0.3,
                timeout=10,
                query=[(C.types.TEST_REQUEST_A, "q{round}")],
            )
            client.wait_for("connected", connections=MULTIPLEXERS)
            self.roll(cluster, client)
            self.check_queries(client, backends)

    def test_threaded_client(self):
        cfg = harness.CONFIG
        with Cluster(MULTIPLEXERS) as cluster:
            backends = self.start_backends(cluster, cfg)
            client = spawn(
                "client",
                cfg.lang("client"),
                mx=cluster.addresses,
                type=C.peers.TEST_ACTIVE_CLIENT,
                threaded=True,
                **{"async": 3},
                count=QUERIES,
                sleep_between=0.3,
                timeout=10,
                query=[(C.types.TEST_REQUEST_A, "q{round}")],
            )
            client.wait_for("connected", connections=MULTIPLEXERS)
            self.roll(cluster, client)
            self.check_queries(client, backends)

    def test_event_sender(self):
        cfg = harness.CONFIG
        with Cluster(MULTIPLEXERS) as cluster:
            backend = spawn(
                "event_backend",
                cfg.lang("backend"),
                mx=cluster.addresses,
                type=C.peers.TEST_EVENT_BACKEND,
                **{"for": 60},
            )
            backend.wait_for("connected", connections=MULTIPLEXERS)
            sender = spawn(
                "event_client",
                cfg.lang("client"),
                mx=cluster.addresses,
                type=C.peers.TEST_EVENT_CLIENT,
                interval=0.3,
                send=[(C.types.TEST_EVENT, "e%d" % index) for index in range(QUERIES)],
            )
            for multiplexer in cluster.mx:
                sender.wait_for("sent", timeout=60)
                multiplexer.restart()
                time.sleep(BETWEEN_RESTARTS)
            self.assertEqual(0, sender.wait(timeout=120))
            self.assertEqual([], sender.events_of("error"), "no send fails during the rolling restart")
            self.assertEqual(QUERIES, len(sender.events_of("sent")))
            self.assertEqual(MULTIPLEXERS, sender.events_of("done")[0]["connections"])
            self.assertEqual(0, backend.stop())
            received = backend.events_of("received")
            # Events sent through a multiplexer the backend had not rejoined yet
            # are dropped by design; the vast majority must arrive.
            self.assertGreaterEqual(len(received), QUERIES - 3 * 3, "received only %d of %d" % (len(received), QUERIES))


if __name__ == "__main__":
    harness.main()
