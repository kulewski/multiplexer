"""`mxcontrol recording start --stay` and `mxcontrol recording tap` follow a replica to a new address of their name, let the old one go, and start with nothing reachable.

A replica replaced behind a name, a rescheduled pod of a headless service
say, comes back under another address. Both commands look the names up
again at every poll and connect to each address that is new, so --stay
starts its session on the replacement and the tap subscribes there. The
old address, which no name resolves to any more and whose connection is
down, is dropped: a multiplexer that comes up there later, another
deployment's that got the address, never hears from the command, which
would start a session there or mix its records into the tap's. A name
that stops resolving for a while takes no connection away. Started with
nothing reachable, the name not published yet or nothing listening at its
address, both commands wait for the polls to find a replica, --stay
starting its session there though the replica had one of an earlier run,
and end with 0 once stopped. The name lives in a hosts file the scenario rewrites,
which the test build of mxcontrol, tests/fake_dns, reads at every lookup
and counts the lookups in, one a poll; the replicas listen on 127.0.0.2
and 127.0.0.3, loopback addresses that need no configuration.
"""

import os
import signal
import socket
import subprocess
import unittest

from multiplexer import recording
from multiplexer.clients import Client
from tests import harness
from tests.harness import Mx, child_env, mx_runfile, output_dir, wait_until

# Under .test, a name no real resolver has: the hosts file alone answers it.
NAME = "replicas.test"

# How many polls a multiplexer at a dropped address is watched for: four
# lookups, one a poll, span three whole polls at least, 6 s, past the 3 s
# a reconnect armed before the drop would take and the poll after it,
# which would start a session or a tap there.
WATCHED_POLLS = 4


def free_port(host: str) -> int:
    """A port of `host` nothing listens on, as the kernel picks one."""
    with socket.socket() as probe:
        probe.bind((host, 0))
        return probe.getsockname()[1]


def status(multiplexer: Mx) -> recording.RecordingStatus:
    """The multiplexer's recording status, through a controller of its own."""
    controller = Client([multiplexer.endpoint], type=recording.RECORDING_CONTROLLER)
    try:
        (answer,) = recording.status(controller)
        return answer
    finally:
        controller.shutdown()


def recording_now(multiplexer: Mx) -> recording.RecordingStatus | None:
    """Its status while it records a session, else None."""
    answer = status(multiplexer)
    return answer if answer.recording else None


def tapped_now(multiplexer: Mx) -> recording.RecordingStatus | None:
    """Its status while it has one tap, else None."""
    answer = status(multiplexer)
    return answer if answer.taps == 1 else None


def streamed_from(path: str, multiplexer_id: int) -> bool:
    """Whether the tap's output holds a record of `multiplexer_id` yet."""
    records = recording.read(path, check_rules=False) if os.path.exists(path) else []
    return any(record.multiplexer_id == multiplexer_id for record in records)


class RemoteRecordingNewAddress(unittest.TestCase):
    """The one replica behind the name replaced by another under a new address, with --stay and with a tap."""

    def setUp(self) -> None:
        """The hosts file and the recording directory the replicas share."""
        assert harness.CONFIG is not None and harness.CONFIG.rules
        self.rules = harness.CONFIG.rules
        self.hosts = os.path.join(output_dir(), "hosts")
        self.recording_dir = os.path.join(output_dir(), "recordings-" + self._testMethodName)
        os.makedirs(self.recording_dir, exist_ok=True)
        self.replicas: list[Mx] = []
        self.command: subprocess.Popen | None = None
        self.log = ""  # the command's output, run_command's

    def tearDown(self) -> None:
        """The command killed if still running, every replica stopped."""
        if self.command is not None and self.command.poll() is None:
            self.command.kill()
            self.command.wait()
        for replica in self.replicas:
            replica.stop()

    def replica(self, address: str) -> Mx:
        """A multiplexer on `address` that takes sessions and taps, started."""
        replica = Mx(
            len(self.replicas),
            self.rules,
            address=address,
            recording_dir=self.recording_dir,
            allow_tap=True,
            prefix=self._testMethodName + "-",
        )
        self.replicas.append(replica)
        return replica.start()

    def publish(self, host: str | None) -> None:
        """From now on the name resolves to `host` alone, or with None to
        nothing; the file is replaced whole, so a lookup reads the old one
        or the new."""
        with open(self.hosts + ".new", "w") as hosts:
            hosts.write("%s %s\n" % (host, NAME) if host else "# %s does not resolve\n" % NAME)
        os.replace(self.hosts + ".new", self.hosts)

    def lookups(self) -> int:
        """How many lookups of the name the command has made so far, one a poll."""
        try:
            with open(self.hosts + ".lookups") as lookups:
                return len(lookups.read().splitlines())
        except FileNotFoundError:
            return 0

    def assert_left_alone(self, old: Mx) -> None:
        """Another multiplexer started at `old`'s address, once the command
        has found the replacement, by the lookup that showed `old`'s
        address gone, hears nothing from the command for WATCHED_POLLS
        polls: no controller connects, nothing records or taps there. The
        command said it dropped the address."""
        newcomer = self.replica(old.address)
        seen = self.lookups()
        wait_until(lambda: self.lookups() >= seen + WATCHED_POLLS, 30, "%d more polls" % WATCHED_POLLS)
        controllers = [peer for peer in newcomer.connected_peers() if peer[2] == recording.RECORDING_CONTROLLER]
        self.assertEqual([], controllers, "the command connected to the old address")
        answer = status(newcomer)
        self.assertFalse(answer.recording, "a session started at the old address")
        self.assertEqual(0, answer.taps, "a tap at the old address")
        self.assertIn("dropping %s" % old.address, self.output())

    def run_command(self, port: int, log: str, *args: str) -> subprocess.Popen:
        """mxcontrol with `args` and -M the name, its output to `log`."""
        self.log = os.path.join(output_dir(), log)
        with open(self.log, "w") as output:
            self.command = subprocess.Popen(
                [mx_runfile("tests/fake_dns/mxcontrol_fake_dns"), "--hosts", self.hosts]
                + list(args)
                + ["-M", "%s:%d" % (NAME, port)],
                stdout=output,
                stderr=subprocess.STDOUT,
                env=child_env(native=True),
            )
        return self.command

    def output(self) -> str:
        """What the command has printed so far."""
        with open(self.log) as output:
            return output.read()

    def test_stay_starts_the_session_on_a_replica_back_under_a_new_address(self):
        first = self.replica("127.0.0.2:0")
        self.publish(first.host)
        stay = self.run_command(first.port, "stay.log", "recording", "start", "--stay", "--label", "kept")
        wait_until(lambda: recording_now(first), 15, "the first replica recording")

        first.stop()  # replaced: the new one comes up under another address, then the name says so
        second = self.replica("127.0.0.3:%d" % first.port)
        self.publish(second.host)
        started = wait_until(lambda: recording_now(second), 30, "the replacement recording")
        self.assertEqual("kept", started.label)
        self.assert_left_alone(first)  # another deployment's multiplexer gets the address

        self.publish(None)  # a name server down for a while: the connection that works stays
        failed = "cannot resolve %s:%d" % (NAME, first.port)
        wait_until(lambda: failed in self.output(), 15, "the failed lookup said")

        stay.send_signal(signal.SIGINT)  # stop every session and leave, through that connection
        self.assertEqual(0, stay.wait(15))
        final = status(second)
        self.assertFalse(final.recording)
        self.assertTrue(final.stopped.startswith("stopped by peer"), final)
        self.assertEqual(started.path, final.path)
        files = [name for name in os.listdir(self.recording_dir) if name.endswith(".rec")]
        self.assertEqual(2, len(files), "the first replica's file and the replacement's, none at the old address")
        text = self.output()
        self.assertIn("%s:%d: new address %s, connecting" % (NAME, first.port, second.address), text)
        self.assertIn("no session yet; starting one", text)

    def test_tap_subscribes_on_a_replica_back_under_a_new_address(self):
        first = self.replica("127.0.0.2:0")
        self.publish(first.host)
        out = os.path.join(output_dir(), "new_address_tap.rec")
        tap = self.run_command(first.port, "tap.log", "recording", "tap", "--out", out)
        wait_until(lambda: tapped_now(first), 15, "the first replica tapped")

        first.stop()
        second = self.replica("127.0.0.3:%d" % first.port)
        self.publish(second.host)
        tapped = wait_until(lambda: tapped_now(second), 30, "the replacement tapped")
        self.assert_left_alone(first)
        # Its records stream: the arrival of each controller that asks it, say.
        wait_until(lambda: streamed_from(out, tapped.multiplexer_id), 15, "a record of the replacement in the output")

        tap.send_signal(signal.SIGINT)  # untap and leave
        self.assertEqual(0, tap.wait(15))
        self.assertEqual(0, status(second).taps, "untapped on the way out")
        text = self.output()
        self.assertIn("%s:%d: new address %s, connecting" % (NAME, first.port, second.address), text)
        self.assertIn("records written", text)

    def test_stay_starts_with_nothing_reachable_and_waits_for_a_replica(self):
        """The name not published yet: --stay keeps running, and starts its
        session once a replica is up and the name says where, though the
        replica had a session before, an earlier run's, stopped; SIGINT
        stops it, and it exits 0. It exited 1 at once, and started no
        session on a multiplexer that had had one."""
        port = free_port("127.0.0.2")
        self.publish(None)
        stay = self.run_command(port, "stay_from_nothing.log", "recording", "start", "--stay", "--label", "later")
        failed = "cannot resolve %s:%d" % (NAME, port)
        wait_until(lambda: failed in self.output(), 15, "the first lookup failed")

        replica = self.replica("127.0.0.2:%d" % port)
        earlier = Client([replica.endpoint], type=recording.RECORDING_CONTROLLER)
        try:
            (session,) = recording.start(earlier, "earlier")
            self.assertTrue(session.recording, session)
            recording.stop(earlier)
        finally:
            earlier.shutdown()
        self.assertTrue(status(replica).stopped, "a session of an earlier run, stopped")
        self.publish(replica.host)
        started = wait_until(lambda: recording_now(replica), 30, "the replica recording")
        self.assertEqual("later", started.label)

        stay.send_signal(signal.SIGINT)
        self.assertEqual(0, stay.wait(15))
        final = status(replica)
        self.assertTrue(final.stopped.startswith("stopped by peer"), final)
        self.assertEqual(started.path, final.path)
        text = self.output()
        self.assertIn("no multiplexer reachable yet", text)
        self.assertIn("%s:%d: new address %s, connecting" % (NAME, port, replica.address), text)
        self.assertIn("no session of this run; starting one", text)

    def test_tap_starts_with_nothing_listening_and_waits_for_a_replica(self):
        """The name says where, but nothing listens there yet: the tap keeps
        running, and taps the replica once it is up; SIGINT untaps it, and
        it exits 0. It exited 1 at once."""
        port = free_port("127.0.0.2")
        self.publish("127.0.0.2")
        out = os.path.join(output_dir(), "tap_from_nothing.rec")
        tap = self.run_command(port, "tap_from_nothing.log", "recording", "tap", "--out", out)
        refused = "cannot connect to 127.0.0.2:%d" % port
        wait_until(lambda: refused in self.output(), 15, "the first connect refused")

        replica = self.replica("127.0.0.2:%d" % port)
        wait_until(lambda: tapped_now(replica), 30, "the replica tapped")

        tap.send_signal(signal.SIGINT)
        self.assertEqual(0, tap.wait(15))
        self.assertEqual(0, status(replica).taps, "untapped on the way out")
        self.assertIn("no multiplexer reachable yet", self.output())


if __name__ == "__main__":
    harness.main()
