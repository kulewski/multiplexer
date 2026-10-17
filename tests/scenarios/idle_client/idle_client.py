"""a passive client idles past both drop intervals and still works.

A client the multiplexer dropped would reconnect on its own and answer
just the same, so the multiplexer's log is what tells: the client, and the
backend idling beside it, registered once each, never dropped and back.
"""

import unittest

from tests import harness
from tests.harness import Cluster, constants as C, spawn

IDLE = 95  # > NO_HEARTBIT_SO_PREPARE_DROP_INTERVAL + NO_HEARTBIT_SO_REALLY_DROP_INTERVAL


def registered(instance_id: int) -> str:
    """The start of the line the multiplexer logs each time a peer with `instance_id` is in."""
    return '"registered connection id=%d ' % instance_id


class IdleClient(unittest.TestCase):
    """Checks that a passive client idles past both drop intervals and still works."""

    def test_second_query_after_long_idle_succeeds(self):
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
                count=2,
                sleep_between=IDLE,
                timeout=10,
                query=[(C.types.TEST_REQUEST_A, "q{round}")],
            )
            self.assertEqual(0, client.wait(timeout=IDLE + 60))
            self.assertEqual([], client.events_of("error"))
            self.assertEqual(["Q0", "Q1"], [r["payload"] for r in client.events_of("response")])
            done = client.events_of("done")[0]
            self.assertEqual(1, done["connections"], "same connection, never dropped")
            self.assertEqual(client.events_of("connected")[0]["instance_id"], done["instance_id"])
            # A registration is logged before the peer is welcomed, so both
            # are in the log by the time the client had its answer.
            with open(cluster.mx[0].log_path) as log:
                text = log.read()
            for role in (client, backend):
                instance_id = role.events_of("connected")[0]["instance_id"]
                self.assertEqual(
                    1, text.count(registered(instance_id)), "%s registered once: never dropped" % role.name
                )


if __name__ == "__main__":
    harness.main()
