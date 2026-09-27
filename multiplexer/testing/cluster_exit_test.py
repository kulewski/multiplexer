"""Leaving a Cluster checks how its processes ended: a multiplexer that
crashed, exited other than 0 at its stop or did not exit at all, and a role
whose SIGTERM did not end it with 0, fail the with-block with
AssertionError naming them; an end the test caused, Mx.kill() or
Mx.expect_exit(), does not. A stand-in multiplexer, given as MXCONTROL,
plays the exit codes a real one gives only under a sanitizer.
"""

import contextlib
import io
import json
import os
import resource
import shutil
import signal
import sys
import tempfile
import unittest
from unittest import mock

from multiplexer import testing
from multiplexer.testing import Cluster, Mx, configure, runfile, spawn

RULES = runfile("tests/testing.rules")  # the file the constants were generated from

# A process that plays a multiplexer for the harness, or a role: SIGTERM
# handled as {on_sigterm} says before anything else, the port file written
# when one is passed, a "connected" event, then a wait for signals.
STAND_IN = """#!{python}
import os, signal, sys
signal.signal(signal.SIGTERM, {on_sigterm})
if "--port-file" in sys.argv:
    port_file = sys.argv[sys.argv.index("--port-file") + 1]
    with open(port_file + ".tmp", "w") as out:
        out.write("127.0.0.1:9")
    os.rename(port_file + ".tmp", port_file)
print('event: "connected"', flush=True)
while True:
    signal.pause()
"""


def name(multiplexer: Mx) -> str:
    """The multiplexer's name in a report: its log file's, c<n>-mx<i>."""
    return os.path.basename(multiplexer.log_path)[: -len(".log")]


class ClusterExitTest(unittest.TestCase):
    """What leaving the with-block reports, and what it lets pass."""

    def setUp(self):
        self.directory = tempfile.mkdtemp(dir=os.environ.get("TEST_TMPDIR"))
        self.addCleanup(shutil.rmtree, self.directory)

    def tearDown(self):
        testing.CONFIG = None

    def stand_in(self, on_sigterm: str) -> str:
        """The path of an executable STAND_IN whose SIGTERM handler is
        `on_sigterm`, Python source."""
        path = os.path.join(self.directory, "stand_in_%d" % len(os.listdir(self.directory)))
        with open(path, "w") as script:
            script.write(STAND_IN.format(python=sys.executable, on_sigterm=on_sigterm))
        os.chmod(path, 0o755)
        return path

    def test_a_crashed_multiplexer_fails_the_block_and_a_killed_one_does_not(self):
        """Of two multiplexers one dies of SIGSEGV, as in a crash, and the
        test kills the other: the report names the first and how it ended."""
        cluster = Cluster(2, rules=RULES)
        crashed, killed = cluster.mx
        with self.assertRaises(AssertionError) as raised:
            with cluster:
                assert crashed.proc is not None
                resource.prlimit(crashed.proc.pid, resource.RLIMIT_CORE, (0, 0))  # no core file
                os.kill(crashed.proc.pid, signal.SIGSEGV)
                crashed.proc.wait(30)
                killed.kill()
        report = str(raised.exception)
        assert crashed.proc is not None
        self.assertNotEqual(0, crashed.proc.returncode)
        self.assertTrue(report.startswith("1 process(es) did not end cleanly"), report)
        self.assertIn("%s exited on its own with %d" % (name(crashed), crashed.proc.returncode), report)
        self.assertNotIn(name(killed), report)

    def test_a_multiplexer_exiting_other_than_0_at_its_stop_fails_the_block(self):
        """Exit code 23 on SIGTERM, what LeakSanitizer gives a leak at exit
        under check.sh --leaks."""
        cluster = Cluster(1, rules=RULES)
        with mock.patch.dict(os.environ, {"MXCONTROL": self.stand_in("lambda *_: os._exit(23)")}):
            with self.assertRaises(AssertionError) as raised:
                with cluster:
                    pass
        self.assertIn("%s exited with 23 on SIGTERM" % name(cluster.mx[0]), str(raised.exception))

    def test_a_multiplexer_that_does_not_exit_fails_the_block(self):
        """SIGTERM ignored: the stop runs out of time and kills it."""
        cluster = Cluster(1, rules=RULES)
        with mock.patch.dict(os.environ, {"MXCONTROL": self.stand_in("signal.SIG_IGN")}):
            with self.assertRaises(AssertionError) as raised:
                with cluster:
                    pass
        self.assertIn(
            "%s did not exit within 10s of SIGTERM and was killed" % name(cluster.mx[0]), str(raised.exception)
        )

    def test_a_stop_waits_out_the_drain(self):
        """A multiplexer started with drain_seconds may take them all to
        stop: the stop waits drain_seconds + 5 s, where it gave up after
        10 s and reported one that drained 11 s as hung."""
        drains = self.stand_in("lambda *_: (__import__('time').sleep(11), os._exit(0))")
        with mock.patch.dict(os.environ, {"MXCONTROL": drains}):
            with Cluster(1, rules=RULES, drain_seconds=11):
                pass

    def test_a_paused_multiplexer_ends_cleanly(self):
        """A multiplexer pause() froze when the block ends is continued after
        its SIGTERM and handles it, where it stayed frozen until the stop
        ran out of time and killed it."""
        with Cluster(1, rules=RULES) as cluster:
            cluster.mx[0].pause()

    def test_a_paused_role_ends_cleanly(self):
        """The same for a role still running, frozen with pause()."""
        configure(["--role-binaries", json.dumps({"backend": self.stand_in("lambda *_: os._exit(0)")})])
        with Cluster(1, rules=RULES) as cluster:
            role = spawn("backend", "bin", mx=cluster.addresses, type=1)
            role.wait_for("connected")  # its handler is in place
            role.pause()

    def test_a_role_exiting_other_than_0_on_sigterm_fails_the_block(self):
        """A role still running when the block ends is stopped with SIGTERM
        and must exit 0, or die of the signal if it does not catch it."""
        configure(["--role-binaries", json.dumps({"backend": self.stand_in("lambda *_: os._exit(3)")})])
        with self.assertRaises(AssertionError) as raised:
            with Cluster(1, rules=RULES) as cluster:
                spawn("backend", "bin", mx=cluster.addresses, type=1).wait_for("connected")  # its handler is in place
        self.assertRegex(str(raised.exception), r"backend-bin-\d+ exited with 3 on SIGTERM; the end of backend-bin-")

    def test_an_end_the_test_expected_is_not_reported(self):
        """expect_exit(): the test ends the multiplexer with a signal of its own."""
        with Cluster(1, rules=RULES) as cluster:
            multiplexer = cluster.mx[0]
            assert multiplexer.proc is not None
            multiplexer.expect_exit()
            multiplexer.proc.send_signal(signal.SIGKILL)
            multiplexer.proc.wait(30)

    def test_a_failing_block_keeps_its_own_error_and_the_report_goes_to_stderr(self):
        """A block that raises keeps its error; the multiplexer killed
        behind the harness's back is reported on stderr beside it."""
        cluster = Cluster(1, rules=RULES)
        stderr = io.StringIO()
        with self.assertRaises(ValueError), contextlib.redirect_stderr(stderr):
            with cluster:
                assert cluster.mx[0].proc is not None
                cluster.mx[0].proc.send_signal(signal.SIGKILL)
                cluster.mx[0].proc.wait(30)
                raise ValueError("the test's own failure")
        self.assertIn("%s exited on its own with -9 (SIGKILL)" % name(cluster.mx[0]), stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
