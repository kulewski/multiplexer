"""run_multiplexer's port file is written whole or not at all: a write that
fails, as on a full disk, ends the start with exit 1 and the reason, the
temporary file removed and the port file as it was, where the temporary
file, empty or cut short, was renamed over it and the multiplexer ran on,
a reader of the file left with no address. Counted, not timed: the
temporary file's name is taken by a link to /dev/full, which opens and
fails every write with ENOSPC, as a full disk does; the bound on the run
only detects a failure."""

import os
import subprocess
import tempfile
import unittest

from multiplexer.testing import mx_runfile, runfile

MXCONTROL = mx_runfile("mxcontrol/mxcontrol")
RULES = runfile("tests/testing.rules")
BOUND = 30  # seconds the start may take: a failure detector only


class PortFileTest(unittest.TestCase):
    """See the module docstring."""

    def test_a_write_that_fails_ends_the_start_and_leaves_the_file(self) -> None:
        directory = tempfile.mkdtemp(dir=os.environ.get("TEST_TMPDIR"))
        port_file = os.path.join(directory, "mx.port")
        with open(port_file, "w") as before:
            before.write("127.0.0.1:1\n")  # what a run before left
        os.symlink("/dev/full", port_file + ".tmp")
        run = subprocess.Popen(
            [MXCONTROL, "run_multiplexer", "--rules", RULES, "--address", "127.0.0.1:0", "--port-file", port_file],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        try:
            output, _ = run.communicate(timeout=BOUND)
        finally:
            if run.poll() is None:  # running on, the bad file in place
                run.kill()
                run.communicate()
        self.assertEqual(1, run.returncode, output[-2000:])
        self.assertIn("cannot write port file", output)
        self.assertIn("No space left on device", output)
        self.assertFalse(os.path.islink(port_file), "the temporary file went over the port file")
        with open(port_file) as after:
            self.assertEqual("127.0.0.1:1\n", after.read())
        self.assertFalse(os.path.lexists(port_file + ".tmp"), "the temporary file was left")


if __name__ == "__main__":
    unittest.main()
