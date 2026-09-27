"""`mxcontrol generate_constants` writes the files a Bazel build writes, byte
for byte: the Python module, its stub, and the C++ header, whose include
guard comes from the rules fingerprint where it was random, read from
/dev/urandom; it refuses a rules file with a repeated name, and a call
that names no output."""

import os
import subprocess
import tempfile
import unittest

from multiplexer.testing import mx_runfile, runfile

MXCONTROL = mx_runfile("mxcontrol/mxcontrol")
# The build runs the tool from the workspace root on the rules file's path,
# which the generated files' signature lines record; the same here. The
# repository's build generates the constants from the tests' rules file
# (.bazelrc).
RULES = "tests/testing.rules"
ROOT = os.path.dirname(os.path.dirname(runfile(RULES)))
GENERATED = {
    "py": runfile("multiplexer/multiplexer_constants.py"),
    "pyi": runfile("multiplexer/multiplexer_constants.pyi"),
    "h": runfile("multiplexer/multiplexer.constants.h"),
}


def read(path: str) -> str:
    """The file's text."""
    with open(path, encoding="utf-8") as handle:
        return handle.read()


class GenerateConstantsTest(unittest.TestCase):
    """See the module docstring."""

    def setUp(self):
        self.out = tempfile.mkdtemp()

    def run_it(self, *args: str) -> subprocess.CompletedProcess:
        """The subcommand with `args`, its output captured."""
        return subprocess.run([MXCONTROL, "generate_constants", *args], capture_output=True, text=True, cwd=ROOT)

    def test_the_three_files_match_the_build(self):
        paths = {ext: os.path.join(self.out, "constants." + ext) for ext in GENERATED}
        result = self.run_it(RULES, "--python", paths["py"], "--pyi", paths["pyi"], "--cxx", paths["h"])
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual([paths["py"], paths["pyi"], paths["h"]], result.stdout.split())
        self.assertEqual(read(GENERATED["py"]), read(paths["py"]))
        self.assertEqual(read(GENERATED["pyi"]), read(paths["pyi"]))
        self.assertEqual(read(GENERATED["h"]), read(paths["h"]))
        self.assertIn("RULES_FINGERPRINT", read(paths["py"]))

    def test_one_file_alone(self):
        path = os.path.join(self.out, "only.py")
        self.assertEqual(0, self.run_it(RULES, "--python", path).returncode)
        self.assertEqual(read(GENERATED["py"]), read(path))
        self.assertEqual(
            [path], os.listdir(self.out) and [os.path.join(self.out, name) for name in os.listdir(self.out)]
        )

    def test_nothing_to_write_is_an_error(self):
        result = self.run_it(RULES)
        self.assertEqual(2, result.returncode)
        self.assertIn("nothing to write", result.stderr)
        self.assertEqual([], os.listdir(self.out))

    def test_a_repeated_name_is_refused(self):
        rules = os.path.join(self.out, "dup.rules")
        with open(rules, "w") as handle:
            handle.write('peer { type: 100 name: "A" }\npeer { type: 101 name: "A" }\n')
        result = self.run_it(rules, "--python", os.path.join(self.out, "dup.py"))
        self.assertNotEqual(0, result.returncode)
        self.assertIn("repeats", result.stderr)
        self.assertFalse(os.path.exists(os.path.join(self.out, "dup.py")), "nothing written for a bad file")

    def test_an_unknown_extension_is_refused(self):
        result = self.run_it(RULES, "--python", os.path.join(self.out, "constants.txt"))
        self.assertNotEqual(0, result.returncode)
        self.assertIn(".h, .py or .pyi", result.stderr)


if __name__ == "__main__":
    unittest.main()
