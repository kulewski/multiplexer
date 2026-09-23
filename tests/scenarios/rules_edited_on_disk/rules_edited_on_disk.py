"""types added to the rules file on disk are in use within seconds, without a restart: edited in place, then swapped the way a Kubernetes ConfigMap update is.

Two multiplexers read a copy of the rules file laid out the way the kubelet
mounts a ConfigMap (the file a symlink through a `..data` link to a
timestamped directory), checking it every 0.1 s instead of the default 2 s.
A backend of a type the file does not name is refused, reports zero
connections and keeps trying every 3 s, and a query of a type the file
does not name fails at once. The peer type and a request type routed to it
are appended in place: both multiplexers put the file in use, the backend
is admitted at its next attempt, and the query is answered. Then the file is replaced the way a
ConfigMap update arrives, a new directory and one atomic rename of the
`..data` link, with a second pair of types, and that is seen too. A file
that does not parse changes nothing, and neither does one caught empty
between a truncate and a write: the last good rules stay in use, the log
says so once, and `mxcontrol rules status` names the error until the file
is fixed. A change is put in use once two checks have read the same new
bytes, so nothing half written is ever applied.
"""

import os
import unittest
import zlib

from tests import harness
from tests.harness import Cluster, Mx, constants as C, mxcontrol, output_dir, spawn, wait_until

CHECK_INTERVAL = 0.1  # seconds between the multiplexers' reads of the file
LATE_BACKEND, LATE_REQUEST = 250, 250  # the types the file gets in place
LATER_BACKEND, LATER_REQUEST = 251, 251  # the types it gets by the swap
BROKEN_ENTRY = '\ntype {\n    type: 252\n    name: "TEST_BROKEN"\n    to {\n        peer: "NOBODY"\n    }\n}\n'


def fingerprint(text: str) -> str:
    """What the multiplexer logs and reports for a rules file: the CRC-32 of its bytes, eight hex digits."""
    return "%08x" % zlib.crc32(text.encode())


def entries(backend: int, request: int) -> str:
    """A peer type and a request type routed to it, as rules file text."""
    return (
        '\npeer {\n    type: %d\n    name: "TEST_LATE_BACKEND_%d"\n}\n'
        'type {\n    type: %d\n    name: "TEST_LATE_REQUEST_%d"\n'
        '    to {\n        peer: "TEST_LATE_BACKEND_%d"\n        whom: ANY\n    }\n}\n'
    ) % (backend, backend, request, request, backend)


class ConfigMapVolume:
    """The rules file laid out as the kubelet mounts a ConfigMap:
    `<dir>/multiplexer.rules` is a symlink to `..data/multiplexer.rules`, and
    `..data` a symlink to a directory holding the real file. An update writes
    a new directory and renames a new `..data` link over the old one, so a
    reader sees the old file or the new one, never a partial one."""

    def __init__(self, directory: str, text: str):
        self.directory = directory
        self.versions = 0
        os.makedirs(directory, exist_ok=True)
        self.path = os.path.join(directory, "multiplexer.rules")
        self.update(text)
        os.symlink(os.path.join("..data", "multiplexer.rules"), self.path)

    def update(self, text: str) -> None:
        """A ConfigMap update as the kubelet applies it."""
        self.versions += 1
        version = "..%d" % self.versions
        os.makedirs(os.path.join(self.directory, version))
        with open(os.path.join(self.directory, version, "multiplexer.rules"), "w") as rules:
            rules.write(text)
        tmp = os.path.join(self.directory, "..data_tmp")
        os.symlink(version, tmp)
        os.rename(tmp, os.path.join(self.directory, "..data"))

    def edit_in_place(self, text: str) -> None:
        """An editor replacing the real file where it is, written next to it
        and renamed over, the way editors that never leave a torn file do."""
        real = os.path.realpath(self.path)
        with open(real + ".new", "w") as rules:
            rules.write(text)
        os.replace(real + ".new", real)

    def truncate(self) -> None:
        """What `cp` or a truncating editor leaves for a moment: nothing."""
        open(os.path.realpath(self.path), "w").close()

    def read(self) -> str:
        """The file as the multiplexers see it."""
        with open(self.path) as rules:
            return rules.read()


class RulesEditedOnDisk(unittest.TestCase):
    """Types appended to the file, then a swap of the file, then a broken file."""

    def ask(self, cluster: Cluster, request_type: int, payload: str) -> str:
        """One query of `request_type` from a fresh client: the answer's payload, or the error's kind."""
        client = spawn(
            "client",
            harness.CONFIG.lang("client"),
            mx=cluster.addresses,
            type=C.peers.TEST_CLIENT,
            timeout=3,
            query=[(request_type, payload)],
        )
        self.assertEqual(0, client.wait())
        responses = client.events_of("response")
        return responses[0]["payload"] if responses else client.events_of("error")[0]["kind"]

    def wait_for_log(self, multiplexer: Mx, text: str, timeout: float = 15) -> None:
        """Block until the multiplexer's log holds `text`."""
        wait_until(lambda: multiplexer.log_contains(text), timeout, "%r in mx%d's log" % (text, multiplexer.index))

    def test_types_added_to_the_file_are_in_use_without_a_restart(self):
        cfg = harness.CONFIG
        with open(cfg.rules) as shipped:
            original = shipped.read()
        volume = ConfigMapVolume(os.path.join(output_dir(), "configmap"), original)
        with Cluster(2, rules=volume.path, rules_check_interval=CHECK_INTERVAL) as cluster:
            late = spawn(
                "backend",
                cfg.lang("backend"),
                mx=cluster.addresses,
                type=LATE_BACKEND,
                serves={LATE_REQUEST: C.types.TEST_RESPONSE},
                name="late",
            )
            late.wait_for("connected", connections=0)  # a type the file does not name is refused
            for multiplexer in cluster.mx:
                self.wait_for_log(multiplexer, "invalid peer type %d" % LATE_BACKEND)
            self.assertEqual(
                "OperationFailed", self.ask(cluster, LATE_REQUEST, "early"), "an unknown type fails at once"
            )

            # Appended in place: the types are in use at the next check.
            volume.edit_in_place(original + entries(LATE_BACKEND, LATE_REQUEST))
            for multiplexer in cluster.mx:
                self.wait_for_log(multiplexer, "-> %s" % fingerprint(volume.read()))
            cluster.wait_for_peer(LATE_BACKEND)  # admitted at its next attempt, within 3 s
            self.assertEqual("HELLO", self.ask(cluster, LATE_REQUEST, "hello"))

            # Replaced the way a ConfigMap update arrives.
            volume.update(volume.read() + entries(LATER_BACKEND, LATER_REQUEST))
            for multiplexer in cluster.mx:
                self.wait_for_log(multiplexer, "-> %s" % fingerprint(volume.read()))
            later = spawn(
                "backend",
                cfg.lang("backend"),
                mx=cluster.addresses,
                type=LATER_BACKEND,
                serves={LATER_REQUEST: C.types.TEST_RESPONSE},
                name="later",
            )
            later.wait_for("connected", connections=2)
            self.assertEqual("WORLD", self.ask(cluster, LATER_REQUEST, "world"))
            self.assertEqual("AGAIN", self.ask(cluster, LATE_REQUEST, "again"), "the first pair is still there")

            # A file that does not parse: the last good rules stay in use.
            good = volume.read()
            volume.edit_in_place(good + BROKEN_ENTRY)
            for multiplexer in cluster.mx:
                self.wait_for_log(multiplexer, "rules file not put in use: Unknown peer definition: 'NOBODY'")
            self.assertEqual("STILL", self.ask(cluster, LATER_REQUEST, "still"))
            addresses = ["-M", cluster.addresses[0], "-M", cluster.addresses[1]]
            status = mxcontrol("rules", "status", *addresses).stdout
            self.assertEqual(2, status.count("rules %s " % fingerprint(good)), status)
            self.assertEqual(
                2, status.count("the file on disk is not in use: Unknown peer definition: 'NOBODY'"), status
            )

            volume.edit_in_place(good)
            wait_until(
                lambda: "not in use" not in mxcontrol("rules", "status", *addresses).stdout,
                15,
                "the status clean again",
            )
            for multiplexer in cluster.mx:
                with open(multiplexer.log_path, "rb") as log:
                    self.assertEqual(1, log.read().count(b"Unknown peer definition"), "said once, not every check")

            # A file caught empty, between a truncate and the write: not a rules file either.
            volume.truncate()
            for multiplexer in cluster.mx:
                self.wait_for_log(multiplexer, "rules file not put in use: empty rules file")
            self.assertEqual("EMPTY", self.ask(cluster, LATER_REQUEST, "empty"), "the rules in use serve on")
            volume.edit_in_place(good)
            wait_until(
                lambda: "not in use" not in mxcontrol("rules", "status", *addresses).stdout,
                15,
                "the status clean again",
            )
            self.assertEqual("BACK", self.ask(cluster, LATER_REQUEST, "back"))


if __name__ == "__main__":
    harness.main()
