"""a ThreadedClient answers queries one at a time and many at once, on the full mesh.

The client keeps an io thread of its own, so its peer type need not be
passive; here it uses the active TEST_ACTIVE_CLIENT type.
"""

import unittest

from tests import harness
from tests.harness import Cluster, constants as C, spawn

QUERIES = 30
IN_FLIGHT = 8


class ThreadedQueries(unittest.TestCase):
    """Checks that a ThreadedClient answers queries one at a time and many at once, on the full mesh."""

    def start_backends(self, cluster, cfg):
        """Two backends of the same type, connected to every multiplexer."""
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
            backend.wait_for("connected", connections=cfg.mx)
            backends.append(backend)
        return backends

    def check(self, client, backends):
        """Every query answered exactly once, and the backends stop cleanly."""
        self.assertEqual(0, client.wait())
        self.assertEqual([], client.events_of("error"))
        responses = client.events_of("response")
        self.assertEqual(QUERIES, len(responses))
        self.assertEqual(sorted("Q%d" % i for i in range(QUERIES)), sorted(r["payload"] for r in responses))
        self.assertEqual(QUERIES, sum(len(b.events_of("request")) for b in backends))
        for b in backends:
            self.assertEqual(0, b.stop())

    def test_one_at_a_time(self):
        cfg = harness.CONFIG
        with Cluster(cfg.mx) as cluster:
            backends = self.start_backends(cluster, cfg)
            client = spawn(
                "client",
                cfg.lang("client"),
                mx=cluster.addresses,
                type=C.peers.TEST_ACTIVE_CLIENT,
                threaded=True,
                count=QUERIES,
                query=[(C.types.TEST_REQUEST_A, "q{round}")],
            )
            client.wait_for("connected", connections=cfg.mx)
            self.check(client, backends)

    def test_many_in_flight(self):
        cfg = harness.CONFIG
        with Cluster(cfg.mx) as cluster:
            backends = self.start_backends(cluster, cfg)
            client = spawn(
                "client",
                cfg.lang("client"),
                mx=cluster.addresses,
                type=C.peers.TEST_ACTIVE_CLIENT,
                threaded=True,
                **{"async": IN_FLIGHT},
                count=QUERIES,
                query=[(C.types.TEST_REQUEST_A, "q{round}")],
            )
            client.wait_for("connected", connections=cfg.mx)
            self.check(client, backends)


if __name__ == "__main__":
    harness.main()
