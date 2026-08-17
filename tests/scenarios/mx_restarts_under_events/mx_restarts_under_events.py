"""the multiplexer restarts between two events; a flushing sender loses none of its calls.

A passive event client sends one event every second and flushes each. The
multiplexer restarts after the second one. The send that finds its
connection dead waits for the reconnect inside the call and writes the
event again, so every send succeeds; the backend, which reconnects on its
own, receives every event sent after both are back.
"""

import unittest

from tests import harness
from tests.harness import Cluster, constants as C, spawn

EVENTS = 6
INTERVAL = 1.0


class MxRestartsUnderEvents(unittest.TestCase):
    """Checks that a flushing sender survives a restart between two events."""

    def test_every_send_succeeds(self):
        cfg = harness.CONFIG
        with Cluster(1) as cluster:
            backend = spawn(
                "event_backend",
                cfg.lang("event_backend"),
                mx=cluster.addresses,
                type=C.peers.TEST_EVENT_BACKEND,
                until=EVENTS,
                **{"for": 60},
            )
            backend.wait_for("connected", connections=1)
            client = spawn(
                "event_client",
                cfg.lang("event_client"),
                mx=cluster.addresses,
                type=C.peers.TEST_EVENT_CLIENT,
                interval=INTERVAL,
                send=[(C.types.TEST_EVENT, "e%d" % index) for index in range(EVENTS)],
            )
            client.wait_for("sent", timeout=30)
            client.wait_for("sent", timeout=30)
            cluster.mx[0].restart()
            self.assertEqual(0, client.wait(timeout=120))
            self.assertEqual([], client.events_of("error"), "no send fails across the restart")
            sent = client.events_of("sent")
            self.assertEqual(EVENTS, len(sent))
            self.assertTrue(all(event["is_sent"] for event in sent), sent)
            self.assertEqual(1, client.events_of("done")[0]["connections"])
            self.assertEqual(0, backend.stop())
            received = [event["payload"] for event in backend.events_of("received")]
            self.assertGreaterEqual(len(received), 3, "events after both peers reconnected must arrive: %s" % received)
            self.assertIn("e%d" % (EVENTS - 1), received, "the last event arrived")


if __name__ == "__main__":
    harness.main()
