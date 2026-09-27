"""`mxcontrol rules reload` puts an edited rules file in use on every multiplexer and reports which rules each runs; a file that does not parse is refused and the rules in use stay.

Two multiplexers run a copy of the rules file with the periodic check off.
`mxcontrol rules status` reports the same fingerprint on both, the one the
generated constants carry. A peer type and a request type are added to the
file: `reload` answers "reloaded" with the new fingerprint from each, a
backend of the new type connects, and a query of the new type is answered.
A recording session open across the reload notes the new fingerprint
where the change happened. An edit naming a peer that does not exist is
refused: `reload` exits 1 with
the reason from each multiplexer, `status` says the file on disk is not in
use, and the query still works. The file put back as it was is "unchanged",
and the status is clean again; a file that is missing is refused the same
way and changes nothing, and the status repeats the reason until a reload
finds the file back; so are an empty file, one without a peer type and
one that repeats a number or a name. An address nobody listens on fails
the command while the reachable multiplexer still answers.
"""

import os
import re
import unittest
import zlib

from tests import harness
from tests.harness import Cluster, constants as C, mxcontrol, output_dir, spawn

NEW_BACKEND, NEW_REQUEST = 250, 250
NEW_ENTRIES = (
    '\npeer {\n    type: 250\n    name: "TEST_NEW_BACKEND"\n}\n'
    'type {\n    type: 250\n    name: "TEST_NEW_REQUEST"\n    to {\n        peer: "TEST_NEW_BACKEND"\n    }\n}\n'
)
BROKEN_ENTRY = '\ntype {\n    type: 252\n    name: "TEST_BROKEN"\n    to {\n        peer: "NOBODY"\n    }\n}\n'
STATUS_LINE = re.compile(r"multiplexer (\d+): (.*)")


def fingerprint(text: str) -> str:
    """What the multiplexer reports for a rules file: the CRC-32 of its bytes, eight hex digits."""
    return "%08x" % zlib.crc32(text.encode())


def parse(output: str) -> dict[int, str]:
    """{multiplexer id: the rest of its line} from mxcontrol's output."""
    return {int(match.group(1)): match.group(2) for match in map(STATUS_LINE.match, output.splitlines()) if match}


class RulesReloadByMxcontrol(unittest.TestCase):
    """status, a reload that applies, a reload that is refused, a reload of the file put back."""

    def ask(self, cluster: Cluster, payload: str) -> str:
        """One query of the new type from a fresh client: the answer's payload, or the error's kind."""
        client = spawn(
            "client",
            harness.CONFIG.lang("client"),
            mx=cluster.addresses,
            type=C.peers.TEST_CLIENT,
            timeout=3,
            query=[(NEW_REQUEST, payload)],
        )
        self.assertEqual(0, client.wait())
        responses = client.events_of("response")
        return responses[0]["payload"] if responses else client.events_of("error")[0]["kind"]

    def test_reload_and_status_on_every_multiplexer(self):
        cfg = harness.CONFIG
        with open(cfg.rules) as shipped:
            original = shipped.read()
        path = os.path.join(output_dir(), "mxcontrol.rules")
        with open(path, "w") as rules:
            rules.write(original)
        with Cluster(2, rules=path, rules_check_interval=0, remote_recording=True) as cluster:
            addresses = ["-M", cluster.addresses[0], "-M", cluster.addresses[1]]
            mxcontrol("recording", "start", "--label", "across", *addresses)  # a session that spans the reload
            before = parse(mxcontrol("rules", "status", *addresses).stdout)
            self.assertEqual(2, len(before), before)
            for line in before.values():
                self.assertRegex(
                    line,
                    r"^rules %s \(\d+ message types, \d+ peer types\) from %s, loaded "
                    % (fingerprint(original), re.escape(path)),
                )
            self.assertEqual("OperationFailed", self.ask(cluster, "early"), "the new type is not in the file yet")

            with open(path, "w") as rules:
                rules.write(original + NEW_ENTRIES)
            reloaded = parse(mxcontrol("rules", "reload", *addresses).stdout)
            self.assertEqual(sorted(before), sorted(reloaded))
            for line in reloaded.values():
                self.assertTrue(line.startswith("reloaded; rules %s " % fingerprint(original + NEW_ENTRIES)), line)
            backend = spawn(
                "backend",
                cfg.lang("backend"),
                mx=cluster.addresses,
                type=NEW_BACKEND,
                serves={NEW_REQUEST: C.types.TEST_RESPONSE},
            )
            backend.wait_for("connected", connections=2)
            self.assertEqual("HELLO", self.ask(cluster, "hello"))
            mxcontrol("recording", "stop", *addresses)
            dump = mxcontrol("dump_recording", "--rules", path, *cluster.recording_files()).stdout
            self.assertEqual(
                2,
                dump.count("rules %s from %s (" % (fingerprint(original + NEW_ENTRIES), path)),
                "each recording says where the rules changed:\n" + dump,
            )

            with open(path, "w") as rules:
                rules.write(original + NEW_ENTRIES + BROKEN_ENTRY)
            refused = mxcontrol("rules", "reload", *addresses, expect=1)
            for line in parse(refused.stdout).values():
                self.assertRegex(
                    line,
                    r"^error: Unknown peer definition: 'NOBODY' \(content [0-9a-f]{8}\); keeps rules %s "
                    % fingerprint(original + NEW_ENTRIES),
                )
            for line in parse(mxcontrol("rules", "status", *addresses).stdout).values():
                self.assertIn("; the file on disk is not in use: Unknown peer definition: 'NOBODY'", line)
            self.assertEqual("STILL", self.ask(cluster, "still"), "the rules in use serve on")

            with open(path, "w") as rules:
                rules.write(original + NEW_ENTRIES)
            for line in parse(mxcontrol("rules", "reload", *addresses).stdout).values():
                self.assertTrue(line.startswith("unchanged; rules %s " % fingerprint(original + NEW_ENTRIES)), line)
                self.assertNotIn("not in use", line)

            os.rename(path, path + ".away")
            for line in parse(mxcontrol("rules", "reload", *addresses, expect=1).stdout).values():
                self.assertTrue(line.startswith("error: cannot read %s; keeps rules " % path), line)
            self.assertEqual("GONE", self.ask(cluster, "gone"), "a missing file changes nothing either")
            os.rename(path + ".away", path)
            for line in parse(mxcontrol("rules", "status", *addresses).stdout).values():
                self.assertIn("the file on disk is not in use: cannot read", line, "until something reads it again")
            for line in parse(mxcontrol("rules", "reload", *addresses).stdout).values():
                self.assertTrue(line.startswith("unchanged; rules %s " % fingerprint(original + NEW_ENTRIES)), line)
                self.assertNotIn("not in use", line)

            # An empty file, what a truncating editor leaves for a moment, and
            # a file without a peer type are no rules files either.
            open(path, "w").close()
            for line in parse(mxcontrol("rules", "reload", *addresses, expect=1).stdout).values():
                self.assertTrue(line.startswith("error: empty rules file %s; keeps rules " % path), line)
            with open(path, "w") as rules:
                rules.write('type {\n    type: 300\n    name: "TEST_NO_PEERS"\n}\n')
            for line in parse(mxcontrol("rules", "reload", *addresses, expect=1).stdout).values():
                self.assertTrue(line.startswith("error: no peer types in %s (content " % path), line)
            with open(path, "w") as rules:
                rules.write(original + NEW_ENTRIES + NEW_ENTRIES)  # the same numbers twice
            for line in parse(mxcontrol("rules", "reload", *addresses, expect=1).stdout).values():
                self.assertTrue(line.startswith("error: duplicate peer type %d (content " % NEW_BACKEND), line)
            with open(path, "w") as rules:  # a message name twice, under two numbers, as generate_constants refuses
                rules.write(original + NEW_ENTRIES + 'type {\n    type: 251\n    name: "TEST_NEW_REQUEST"\n}\n')
            for line in parse(mxcontrol("rules", "reload", *addresses, expect=1).stdout).values():
                self.assertTrue(line.startswith("error: duplicate message name TEST_NEW_REQUEST (content "), line)
            with open(path, "w") as rules:  # a peer type whose queue holds nothing
                rules.write(
                    original
                    + NEW_ENTRIES.replace('"TEST_NEW_BACKEND"\n}', '"TEST_NEW_BACKEND"\n    queue_size: 0\n}', 1)
                )
            for line in parse(mxcontrol("rules", "reload", *addresses, expect=1).stdout).values():
                prefix = "error: peer type %d (TEST_NEW_BACKEND): queue_size 0 holds no message (content " % NEW_BACKEND
                self.assertTrue(line.startswith(prefix), line)
            self.assertEqual("LAST", self.ask(cluster, "last"), "the rules in use serve on")
            with open(path, "w") as rules:
                rules.write(original + NEW_ENTRIES)
            for line in parse(mxcontrol("rules", "reload", *addresses).stdout).values():
                self.assertTrue(line.startswith("unchanged; rules %s " % fingerprint(original + NEW_ENTRIES)), line)

            # An address nobody listens on fails the command, the reachable one still answers.
            unreachable = mxcontrol("rules", "status", "-M", cluster.addresses[0], "-M", "127.0.0.1:1", expect=1)
            self.assertEqual(1, len(parse(unreachable.stdout)), "the reachable multiplexer answered")
            self.assertIn("1 of 2 multiplexer address(es) could not be reached", unreachable.stderr)


if __name__ == "__main__":
    harness.main()
