"""mxcontrol recording tap and start --stay: a TAP or a START a multiplexer
refuses makes the command exit 1 at its end, and a refused TAP is not
sent again, where both exited 0 whatever was refused and tap asked again
every 2 s; and tap --out appends after the last whole record of its file,
cutting one left half written, so that the file reads to its end, where
the records after the torn one were unreadable. Counted, not timed: each
command is ended once its output says what is checked, a refusal or a
tap in place, and the records are counted in the file.
"""

import os
import signal
import subprocess
import tempfile
import threading
import unittest

from multiplexer import recording
from multiplexer.clients import Client
from multiplexer.multiplexer_constants import peers, types
from multiplexer.testing import Mx, mx_runfile, runfile, wait_until

MXCONTROL = mx_runfile("mxcontrol/mxcontrol")
RULES = runfile("tests/testing.rules")  # the file the constants were generated from
BOUND = 30  # seconds a wait may take before the test calls it stuck: a failure detector only


class Command:
    """mxcontrol run with `args`, its stdout and stderr read as they come."""

    def __init__(self, *args: str):
        self.proc = subprocess.Popen([MXCONTROL, *args], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        self.lines: list[str] = []
        self.lock = threading.Lock()
        self.readers = [
            threading.Thread(target=self._read, args=(stream,), daemon=True)
            for stream in (self.proc.stdout, self.proc.stderr)
        ]
        for reader in self.readers:
            reader.start()

    def _read(self, stream) -> None:
        """Every line of `stream`, kept."""
        for line in stream:
            with self.lock:
                self.lines.append(line)

    def output(self) -> str:
        """Everything it printed so far, both streams."""
        with self.lock:
            return "".join(self.lines)

    def wait_for(self, text: str) -> None:
        """Until it printed `text`."""
        wait_until(lambda: text in self.output(), BOUND, "%r in the output of mxcontrol" % text)

    def end(self) -> int:
        """SIGTERM, as a supervisor stops it, and its exit status."""
        self.proc.send_signal(signal.SIGTERM)
        code = self.proc.wait(BOUND)
        for reader in self.readers:
            reader.join(BOUND)
        return code


def whole_records(path: str) -> tuple[int, bool]:
    """How many records the file holds before its end or a broken one, and
    whether it read to its end."""
    count = 0
    try:
        for _ in recording.read(path):
            count += 1
    except recording.TruncatedRecording:
        return count, False
    return count, True


class RecordingLoopsTest(unittest.TestCase):
    """See the module docstring."""

    def setUp(self) -> None:
        self.scratch = tempfile.TemporaryDirectory()
        self.addCleanup(self.scratch.cleanup)

    def multiplexer(self, **options) -> Mx:
        """A multiplexer of the test's own, stopped at the end."""
        mx = Mx(0, RULES, prefix="loops%d-" % id(self), **options).start()
        self.addCleanup(mx.stop)
        return mx

    def test_a_refused_tap_is_not_asked_again_and_fails_the_command(self) -> None:
        """Taps off: the refusal is said once, and the command exits 1."""
        files_only = os.path.join(self.scratch.name, "files")
        os.makedirs(files_only)
        mx = self.multiplexer(recording_dir=files_only)
        tap = Command("recording", "tap", "-M", mx.address)
        tap.wait_for("error:")
        self.assertEqual(1, tap.end(), tap.output())
        self.assertIn("not asking it again", tap.output())
        self.assertIn("1 multiplexer(s) refused the tap", tap.output())

    def test_a_refused_start_fails_stay(self) -> None:
        """No --recording-dir: start --stay says the refusal and exits 1."""
        mx = self.multiplexer(allow_tap=True)
        stay = Command("recording", "start", "--stay", "--label", "refused", "-M", mx.address)
        stay.wait_for("error:")
        self.assertEqual(1, stay.end(), stay.output())
        self.assertIn("refused the session", stay.output())

    def test_tap_out_appends_after_the_last_whole_record(self) -> None:
        """A tap's file cut one byte short, as a tap that died writing leaves
        it: the next tap on it cuts the torn record and appends, and the
        file reads to its end."""
        mx = self.multiplexer(allow_tap=True)
        out = os.path.join(self.scratch.name, "tap.rec")

        def tap_until_recorded(more_than: int) -> None:
            """A tap on `out` until a record of a client's arrival is in it."""
            tap = Command("recording", "tap", "-M", mx.address, "--out", out)
            tap.wait_for(": tapping")
            client = Client([mx.endpoint], type=peers.WEBSITE)
            try:
                client.send_message(b"recorded", type=types.TEST_UNROUTED, flush=True)
                wait_until(lambda: whole_records(out)[0] > more_than, BOUND, "the records in the file")
            finally:
                client.shutdown()
            self.assertEqual(0, tap.end(), tap.output())

        tap_until_recorded(0)
        os.truncate(out, os.path.getsize(out) - 1)
        torn, whole = whole_records(out)
        self.assertFalse(whole, "the last record is torn")
        tap_until_recorded(torn)
        self.assertTrue(whole_records(out)[1], "the file reads to its end")


if __name__ == "__main__":
    unittest.main()
