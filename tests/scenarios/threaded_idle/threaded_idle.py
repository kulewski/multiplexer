"""a ThreadedClient of an active peer type idles past the drop intervals and still works.

Its io thread answers heartbeats while the program does nothing, which is
what lets the peer type be non-passive. A client the multiplexer dropped
would reconnect on its own and answer just the same, so the multiplexer's
log is what tells: the client, and the backend idling beside it,
registered once each. Slow: waits out 30 s + 60 s and more.
"""

import unittest

from tests import harness
from tests.harness import Cluster, constants as C, spawn

IDLE = 100


def registered(instance_id: int) -> str:
    """The start of the line the multiplexer logs each time a peer with `instance_id` is in."""
    return '"registered connection id=%d ' % instance_id


class ThreadedIdle(unittest.TestCase):
    """Checks that a ThreadedClient of an active peer type idles past the drop intervals and still works."""

    def test_still_connected_after_idling(self):
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
                sleep_before=IDLE,
                query=[(C.types.TEST_REQUEST_A, "after idling")],
            )
            self.assertEqual(0, client.wait(timeout=IDLE + 60))
            self.assertEqual([], client.events_of("error"))
            self.assertEqual("AFTER IDLING", client.events_of("response")[0]["payload"])
            self.assertEqual(1, client.events_of("done")[0]["connections"], "the connection was never dropped")
            # A registration is logged before the peer is welcomed, so both
            # are in the log by the time the client had its answer.
            with open(cluster.mx[0].log_path) as log:
                text = log.read()
            for role in (client, backend):
                instance_id = role.events_of("connected")[0]["instance_id"]
                self.assertEqual(
                    1, text.count(registered(instance_id)), "%s registered once: never dropped" % role.name
                )
            self.assertEqual(0, backend.stop())


if __name__ == "__main__":
    harness.main()
