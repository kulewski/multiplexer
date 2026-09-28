"""`mxcontrol recording start --stay` and `mxcontrol recording tap` reach a replica that comes back under a new address of the name they were given.

A replica replaced behind a name, a rescheduled pod of a headless service
say, comes back under another address. Both commands look the names up
again at every poll and connect to each address that is new, so --stay
starts its session on the replacement and the tap subscribes there; the
old address stays, retried as any lost connection is, and a name that
stops resolving for a while takes no connection away. The name lives in a
hosts file the scenario rewrites, which the test build of mxcontrol,
tests/fake_dns, reads at every lookup; the replicas listen on 127.0.0.2 and
127.0.0.3, loopback addresses that need no configuration.
"""

import os
import signal
import subprocess
import unittest

from multiplexer import recording
from multiplexer.clients import Client
from tests import harness
from tests.harness import Mx, child_env, mx_runfile, output_dir, wait_until

# Under .test, a name no real resolver has: the hosts file alone answers it.
NAME = "replicas.test"


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

    def publish(self, replica: Mx | None) -> None:
        """From now on the name resolves to `replica`'s address alone, or with
        None to nothing; the file is replaced whole, so a lookup reads the
        old one or the new."""
        with open(self.hosts + ".new", "w") as hosts:
            hosts.write("%s %s\n" % (replica.host, NAME) if replica else "# %s does not resolve\n" % NAME)
        os.replace(self.hosts + ".new", self.hosts)

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
        self.publish(first)
        stay = self.run_command(first.port, "stay.log", "recording", "start", "--stay", "--label", "kept")
        wait_until(lambda: recording_now(first), 15, "the first replica recording")

        first.stop()  # replaced: the new one comes up under another address, then the name says so
        second = self.replica("127.0.0.3:%d" % first.port)
        self.publish(second)
        started = wait_until(lambda: recording_now(second), 30, "the replacement recording")
        self.assertEqual("kept", started.label)

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
        self.assertEqual(2, len(files), "the first replica's file and the replacement's")
        text = self.output()
        self.assertIn("%s:%d: new address %s, connecting" % (NAME, first.port, second.address), text)
        self.assertIn("no session yet; starting one", text)

    def test_tap_subscribes_on_a_replica_back_under_a_new_address(self):
        first = self.replica("127.0.0.2:0")
        self.publish(first)
        out = os.path.join(output_dir(), "new_address_tap.rec")
        tap = self.run_command(first.port, "tap.log", "recording", "tap", "--out", out)
        wait_until(lambda: tapped_now(first), 15, "the first replica tapped")

        first.stop()
        second = self.replica("127.0.0.3:%d" % first.port)
        self.publish(second)
        tapped = wait_until(lambda: tapped_now(second), 30, "the replacement tapped")
        # Its records stream: the arrival of each controller that asks it, say.
        wait_until(lambda: streamed_from(out, tapped.multiplexer_id), 15, "a record of the replacement in the output")

        tap.send_signal(signal.SIGINT)  # untap and leave
        self.assertEqual(0, tap.wait(15))
        self.assertEqual(0, status(second).taps, "untapped on the way out")
        text = self.output()
        self.assertIn("%s:%d: new address %s, connecting" % (NAME, first.port, second.address), text)
        self.assertIn("records written", text)


if __name__ == "__main__":
    harness.main()
