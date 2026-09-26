"""multiplexer.mxcontrol finds the mxcontrol that came with the package, the
wheel's before Bazel's runfiles one, and runs it in its own process's place:
the same pid, argv[0] `mxcontrol`, SIGPIPE and SIGXFSZ at their defaults.
Each case lays a copy of the module out as a wheel or as runfiles in a
scratch directory, with a copy of /bin/sh standing in for the binary, and
runs a fresh interpreter on it, so that nothing installed stands in. And
the harness names MXCONTROL when there is nothing to run, before it opens
anything."""

import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

import multiplexer.mxcontrol
from multiplexer.testing import Cluster, runfile

# Signals 13 and 25 in the SigIgn mask of /proc/PID/status, bit n-1 for signal n.
SIGPIPE_BIT, SIGXFSZ_BIT = 1 << 12, 1 << 24


class LauncherTest(unittest.TestCase):
    """See the module docstring."""

    def setUp(self):
        self.tree = tempfile.mkdtemp()
        os.makedirs(os.path.join(self.tree, "multiplexer"))
        shutil.copy(multiplexer.mxcontrol.__file__, os.path.join(self.tree, "multiplexer", "mxcontrol.py"))
        open(os.path.join(self.tree, "multiplexer", "__init__.py"), "w").close()

    def stand_in(self, *where: str) -> str:
        """A copy of /bin/sh at `where` in the tree, executable."""
        path = os.path.join(self.tree, *where)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        shutil.copy("/bin/sh", path)
        os.chmod(path, 0o755)
        return path

    def command(self, *args: str) -> list[str]:
        """A fresh interpreter's command line, without site packages."""
        return [sys.executable, "-S", *args]

    def environment(self) -> dict[str, str]:
        """An environment in which the tree is all the interpreter sees."""
        return {"PYTHONPATH": self.tree, "PATH": os.environ.get("PATH", "/usr/bin:/bin")}

    def found(self) -> subprocess.CompletedProcess:
        """binary_path() of the tree's copy of the module, printed."""
        return subprocess.run(
            self.command("-c", "import multiplexer.mxcontrol as m; print(m.binary_path())"),
            cwd=self.tree,
            env=self.environment(),
            capture_output=True,
            text=True,
        )

    def test_the_package_copy_comes_before_the_runfiles_one(self):
        packaged = self.stand_in("multiplexer", "bin", "mxcontrol")
        self.stand_in("mxcontrol", "mxcontrol")
        result = self.found()
        self.assertEqual(packaged, result.stdout.strip(), result.stderr)

    def test_the_runfiles_copy_alone(self):
        runfiles = self.stand_in("mxcontrol", "mxcontrol")
        result = self.found()
        self.assertEqual(runfiles, result.stdout.strip(), result.stderr)

    def test_neither_is_an_error_naming_both(self):
        result = self.found()
        self.assertIn("FileNotFoundError", result.stderr)
        self.assertIn(os.path.join(self.tree, "multiplexer", "bin", "mxcontrol"), result.stderr)
        self.assertIn(os.path.join(self.tree, "mxcontrol", "mxcontrol"), result.stderr)
        command = subprocess.run(
            self.command("-m", "multiplexer.mxcontrol", "help"),
            cwd=self.tree,
            env=self.environment(),
            capture_output=True,
        )
        self.assertEqual(127, command.returncode)

    def test_the_command_runs_the_binary_in_its_place(self):
        self.stand_in("multiplexer", "bin", "mxcontrol")
        script = 'echo "$0 $$"; grep ^SigIgn /proc/$$/status'
        process = subprocess.Popen(
            self.command("-m", "multiplexer.mxcontrol", "-c", script),
            cwd=self.tree,
            env=self.environment(),
            stdout=subprocess.PIPE,
            text=True,
        )
        output, _ = process.communicate()
        name_and_pid, ignored = output.splitlines()
        self.assertEqual("mxcontrol %d" % process.pid, name_and_pid)
        mask = int(ignored.split()[1], 16)
        self.assertEqual(0, mask & SIGPIPE_BIT, ignored)
        self.assertEqual(0, mask & SIGXFSZ_BIT, ignored)
        self.assertEqual(0, process.returncode)


class HarnessTest(unittest.TestCase):
    """See the module docstring."""

    def test_nothing_to_run_names_mxcontrol_and_opens_nothing(self):
        rules = runfile("tests/testing.rules")
        open_before = sorted(os.listdir("/proc/self/fd"))
        with mock.patch.dict(os.environ, {"MXCONTROL": "/nonexistent/mxcontrol"}):
            with self.assertRaises(FileNotFoundError) as raised:
                with Cluster(1, rules=rules):
                    pass
        self.assertIn("MXCONTROL", str(raised.exception))
        self.assertEqual(open_before, sorted(os.listdir("/proc/self/fd")))


if __name__ == "__main__":
    unittest.main()
