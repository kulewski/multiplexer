"""a client's events reach every event backend (whom: ALL)."""

import unittest

from tests import harness
from tests.harness import Cluster, constants as C, spawn

EVENTS = ["one", "two", "three"]


class EventAllBackends(unittest.TestCase):
    """Checks that a client's events reach every event backend (whom: ALL)."""

    def test_every_backend_gets_every_event(self):
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
                    until=len(EVENTS),
                )
                l.wait_for("connected", connections=cfg.mx)
                event_backends.append(l)

            event_client = spawn(
                "event_client",
                cfg.lang("event_client"),
                mx=cluster.addresses,
                type=C.peers.TEST_EVENT_CLIENT,
                send=[(C.types.TEST_EVENT, e) for e in EVENTS],
            )
            self.assertEqual(0, event_client.wait())
            self.assertEqual([], event_client.events_of("error"))
            self.assertEqual(len(EVENTS), len(event_client.events_of("sent")))

            for l in event_backends:
                self.assertEqual(0, l.wait(), "event_backend exits after receiving all events")
                received = l.events_of("received", type=C.types.TEST_EVENT)
                self.assertEqual(EVENTS, [r["payload"] for r in received])


if __name__ == "__main__":
    harness.main()
