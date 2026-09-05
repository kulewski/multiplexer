"""whom: ALL reaches every backend, whom: ANY reaches exactly one."""

import unittest

from tests import harness
from tests.harness import Cluster, constants as C, spawn

N = 4


class AnyVsAll(unittest.TestCase):
    """Checks that whom: ALL reaches every backend, whom: ANY reaches exactly one."""

    def test_all_and_any_routing(self):
        cfg = harness.CONFIG
        with Cluster(cfg.mx) as cluster:
            event_backends = []
            for i in range(2):
                l = spawn(
                    "event_backend",
                    cfg.lang("event_backend"),
                    mx=cluster.addresses,
                    name="event_backend%d" % i,
                    type=C.peers.TEST_EVENT_BACKEND,
                    **{"for": 3},
                )
                l.wait_for("connected", connections=cfg.mx)
                event_backends.append(l)

            sends = [(C.types.TEST_EVENT, "all%d" % i) for i in range(N)]
            sends += [(C.types.TEST_EVENT_ANY, "any%d" % i) for i in range(N)]
            event_client = spawn(
                "event_client",
                cfg.lang("event_client"),
                mx=cluster.addresses,
                type=C.peers.TEST_EVENT_CLIENT,
                send=sends,
            )
            self.assertEqual(0, event_client.wait())
            self.assertEqual(2 * N, len(event_client.events_of("sent")))

            for l in event_backends:
                self.assertEqual(0, l.wait())
            for l in event_backends:
                got = [r["payload"] for r in l.events_of("received", type=C.types.TEST_EVENT)]
                self.assertEqual(["all%d" % i for i in range(N)], got)
            any_ids = [r["id"] for l in event_backends for r in l.events_of("received", type=C.types.TEST_EVENT_ANY)]
            self.assertEqual(N, len(any_ids), "each ANY event delivered once in total")
            self.assertEqual(N, len(set(any_ids)))


if __name__ == "__main__":
    harness.main()
