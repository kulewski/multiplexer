"""SIGHUP makes a multiplexer read its rules file again: a request type rerouted to another backend type takes effect at once, with the periodic check off.

One multiplexer runs a copy of the rules file with `--rules-check-interval
0`, so nothing but a signal or a request reloads it, the way an operator
who wants edits applied on their say-so runs it. Two backends of different
types both serve TEST_REQUEST_A; the file routes it to the first. The rule
is edited to name the second type: queries keep going to the first until
SIGHUP, and to the second from then on. A SIGHUP with the file unchanged is
logged as such, and so is one with a file that does not parse. A peer
whose type an edit removed stays connected, counted in the log, and still
gets what is addressed to it, while a new peer of that type is refused.
"""

import os
import re
import unittest

from tests import harness
from tests.harness import Cluster, constants as C, output_dir, spawn, wait_until

REROUTE = re.compile(r'(name: "TEST_REQUEST_A"\s*to \{\s*peer: )"TEST_BACKEND_A"')
PEER_A = re.compile(r'peer \{\s*type: 201\s*name: "TEST_BACKEND_A"\s*\}\n')
BROKEN_ENTRY = '\ntype {\n    type: 252\n    name: "TEST_BROKEN"\n    to {\n        peer: "NOBODY"\n    }\n}\n'


class RulesReloadOnSighup(unittest.TestCase):
    """A rule edited, applied by SIGHUP and not before."""

    def ask(self, cluster: Cluster, payload: str) -> None:
        """One TEST_REQUEST_A query from a fresh client, answered."""
        client = spawn(
            "client",
            harness.CONFIG.lang("client"),
            mx=cluster.addresses,
            type=C.peers.TEST_CLIENT,
            query=[(C.types.TEST_REQUEST_A, payload)],
        )
        self.assertEqual(0, client.wait())
        self.assertEqual(payload.upper(), client.events_of("response")[0]["payload"])

    def test_a_rerouted_type_takes_effect_on_sighup(self):
        cfg = harness.CONFIG
        with open(cfg.rules) as shipped:
            original = shipped.read()
        path = os.path.join(output_dir(), "sighup.rules")
        with open(path, "w") as rules:
            rules.write(original)
        with Cluster(1, rules=path, rules_check_interval=0) as cluster:
            multiplexer = cluster.mx[0]
            multiplexer.reload_rules()  # the moment the port file is there, the signal is handled
            wait_until(lambda: multiplexer.log_contains("the rules file is the rules in use"), 15, "an early SIGHUP")
            first = spawn(
                "backend",
                cfg.lang("backend"),
                mx=cluster.addresses,
                type=C.peers.TEST_BACKEND_A,
                serves={C.types.TEST_REQUEST_A: C.types.TEST_RESPONSE},
                name="first",
            )
            second = spawn(
                "backend",
                cfg.lang("backend"),
                mx=cluster.addresses,
                type=C.peers.TEST_BACKEND_B,
                serves={C.types.TEST_REQUEST_A: C.types.TEST_RESPONSE},
                name="second",
            )
            first.wait_for("connected", connections=1)
            second.wait_for("connected", connections=1)
            self.ask(cluster, "one")
            first.wait_for_count("request", 1)
            self.assertEqual([], second.events_of("request"))

            rerouted, count = REROUTE.subn(r'\1"TEST_BACKEND_B"', original)
            self.assertEqual(1, count, "the rule of TEST_REQUEST_A now names TEST_BACKEND_B")
            with open(path, "w") as rules:
                rules.write(rerouted)
            self.ask(cluster, "two")
            first.wait_for_count("request", 2)
            self.assertEqual([], second.events_of("request"), "the edit is not in use: nothing reads the file")

            multiplexer.reload_rules()
            wait_until(lambda: multiplexer.log_contains("rules reloaded from"), 15, "the reload in the log")
            self.ask(cluster, "three")
            second.wait_for_count("request", 1)
            self.assertEqual(2, len(first.events_of("request")), "the first backend gets nothing more")

            multiplexer.reload_rules()
            wait_until(
                lambda: multiplexer.log_contains("received SIGHUP; the rules file is the rules in use"),
                15,
                "the unchanged file noted in the log",
            )

            # A broken file is said on every SIGHUP, whether or not it is news.
            with open(path, "w") as rules:
                rules.write(rerouted + BROKEN_ENTRY)
            multiplexer.reload_rules()
            wait_until(
                lambda: multiplexer.log_contains("received SIGHUP; the rules file is not in use: Unknown peer"),
                15,
                "the refusal noted in the log",
            )

            # A connected peer whose type the file no longer names stays,
            # and still gets what is addressed to it; a new one is refused.
            without_a, count = PEER_A.subn("", rerouted)
            self.assertEqual(1, count, "the TEST_BACKEND_A peer entry is gone")
            with open(path, "w") as rules:
                rules.write(without_a)
            multiplexer.reload_rules()
            wait_until(
                lambda: multiplexer.log_contains("1 connected peer(s) of types the file no longer names, kept"),
                15,
                "the kept peer counted in the log",
            )
            first_id = first.events_of("connected")[0]["instance_id"]
            self.assertIn(first_id, [peer_id for peer_id, _, _ in multiplexer.connected_peers()], "still connected")
            addressed = spawn(
                "event_client",
                cfg.lang("event_client"),
                mx=cluster.addresses,
                type=C.peers.TEST_EVENT_CLIENT,
                to=first_id,
                send=[(C.types.TEST_REQUEST_A, "for-first")],
            )
            self.assertEqual(0, addressed.wait())
            self.assertEqual(3, len(first.wait_for_count("request", 3)), "addressed messages still arrive")
            late = spawn(
                "backend",
                cfg.lang("backend"),
                mx=cluster.addresses,
                type=C.peers.TEST_BACKEND_A,
                serves={C.types.TEST_REQUEST_A: C.types.TEST_RESPONSE},
                name="late",
            )
            late.wait_for("connected", connections=0)  # a type the file no longer names is refused
            wait_until(lambda: multiplexer.log_contains("invalid peer type %d" % C.peers.TEST_BACKEND_A), 15, "refused")


if __name__ == "__main__":
    harness.main()
