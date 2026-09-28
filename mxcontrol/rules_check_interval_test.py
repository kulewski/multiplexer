"""`mxcontrol run_multiplexer --rules-check-interval` refuses a positive
interval below the shortest, 0.01 s, as a malformed command line: exit 1
with the reason, nothing listening, where a multiplexer started and read
its rules file over and over on its io thread, the interval truncated to
nothing. The shortest itself, and 0, start one. Each run is told apart by
its port file, which a multiplexer writes once listening, not by time."""

import os
import signal
import subprocess
import tempfile
import unittest

from multiplexer.testing import mx_runfile, runfile, wait_until

MXCONTROL = mx_runfile("mxcontrol/mxcontrol")
TESTING_RULES = runfile("tests/testing.rules")


class RulesCheckIntervalTest(unittest.TestCase):
    """See the module docstring."""

    def setUp(self) -> None:
        """A directory for the port files."""
        self.out = tempfile.mkdtemp()

    def start(self, interval: str) -> tuple[subprocess.Popen, str]:
        """run_multiplexer on a free port with `interval`, and the port file
        it writes once listening."""
        port_file = os.path.join(self.out, "mx-%s.port" % interval)
        process = subprocess.Popen(
            [MXCONTROL, "run_multiplexer", "--rules", TESTING_RULES, "--address", "127.0.0.1:0"]
            + ["--port-file", port_file, "--rules-check-interval", interval],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        self.addCleanup(self.end, process)
        return process, port_file

    @staticmethod
    def end(process: subprocess.Popen) -> None:
        """The process ended, killed if it still runs."""
        if process.poll() is None:
            process.kill()
        process.wait()
        assert process.stderr is not None
        process.stderr.close()

    def ended_or_listening(self, process: subprocess.Popen, port_file: str) -> None:
        """Waits until the process has ended, or listens."""
        wait_until(lambda: process.poll() is not None or os.path.exists(port_file), 60, "an end or a port file")

    def test_below_the_shortest_is_refused(self):
        for interval in ("1e-7", "0.009"):
            with self.subTest(interval=interval):
                process, port_file = self.start(interval)
                self.ended_or_listening(process, port_file)
                self.assertFalse(os.path.exists(port_file), "a multiplexer started with %s" % interval)
                self.assertEqual(1, process.wait())
                assert process.stderr is not None
                error = process.stderr.read().decode()
                self.assertIn("--rules-check-interval: ", error)
                self.assertIn("below the shortest rules check interval, 0.01 s", error)

    def test_the_shortest_and_off_start(self):
        for interval in ("0.01", "0"):
            with self.subTest(interval=interval):
                process, port_file = self.start(interval)
                self.ended_or_listening(process, port_file)
                self.assertTrue(os.path.exists(port_file), "no multiplexer with %s" % interval)
                process.send_signal(signal.SIGTERM)
                self.assertEqual(0, process.wait(30))


if __name__ == "__main__":
    unittest.main()
