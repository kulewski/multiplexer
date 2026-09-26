"""`mxcontrol generate_rules` writes the system rules, this repository's
multiplexer.rules byte for byte, to a new file or to stdout, and never
replaces a file that exists; and the tests' own rules file starts from
them, as every rules file does."""

import os
import subprocess
import tempfile
import unittest

from multiplexer.testing import mx_runfile, runfile

MXCONTROL = mx_runfile("mxcontrol/mxcontrol")
SYSTEM_RULES = runfile("multiplexer.rules")
TESTING_RULES = runfile("tests/testing.rules")


def read(path: str) -> bytes:
    """The file's bytes."""
    with open(path, "rb") as handle:
        return handle.read()


class GenerateRulesTest(unittest.TestCase):
    """See the module docstring."""

    def setUp(self):
        self.out = tempfile.mkdtemp()

    def run_it(self, *args: str) -> subprocess.CompletedProcess:
        """The subcommand with `args`, its output captured as bytes."""
        return subprocess.run([MXCONTROL, "generate_rules", *args], capture_output=True)

    def test_a_new_file_holds_the_system_rules(self):
        path = os.path.join(self.out, "multiplexer.rules")
        result = self.run_it(path)
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual(path.encode() + b"\n", result.stdout)
        self.assertEqual(read(SYSTEM_RULES), read(path))

    def test_an_existing_file_is_never_replaced(self):
        path = os.path.join(self.out, "multiplexer.rules")
        with open(path, "wb") as handle:
            handle.write(b"# a deployment's rules, with its own types\n")
        result = self.run_it(path)
        self.assertEqual(1, result.returncode)
        self.assertIn(b"never replaced", result.stderr)
        self.assertEqual(b"# a deployment's rules, with its own types\n", read(path))

    def test_a_dash_writes_stdout(self):
        result = self.run_it("-")
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual(read(SYSTEM_RULES), result.stdout)
        self.assertEqual([], os.listdir(self.out))

    def test_the_tests_rules_start_from_the_system_rules(self):
        self.assertTrue(read(TESTING_RULES).startswith(read(SYSTEM_RULES)))


if __name__ == "__main__":
    unittest.main()
