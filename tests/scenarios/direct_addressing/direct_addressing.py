"""a message with `to` set reaches only that instance, whatever the rules say."""

import unittest

from tests import harness
from tests.harness import Cluster, constants as C, spawn


class DirectAddressing(unittest.TestCase):
    """Checks that a message with `to` set reaches only that instance, whatever the rules say."""

    def test_only_the_addressed_backend_receives(self):
        cfg = harness.CONFIG
        with Cluster(1) as cluster:
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
                l.wait_for("connected", connections=1)
                event_backends.append(l)
            target = event_backends[0].events_of("connected")[0]["instance_id"]

            event_client = spawn(
                "event_client",
                cfg.lang("event_client"),
                mx=cluster.addresses,
                type=C.peers.TEST_EVENT_CLIENT,
                to=target,
                send=[(C.types.TEST_EVENT, "just for you")],
            )
            self.assertEqual(0, event_client.wait())
            for l in event_backends:
                self.assertEqual(0, l.wait())
            got0 = event_backends[0].events_of("received")
            self.assertEqual(1, len(got0))
            self.assertEqual(("just for you", target), (got0[0]["payload"], got0[0]["to"]))
            self.assertEqual([], event_backends[1].events_of("received"))


if __name__ == "__main__":
    harness.main()
