"""`mxcontrol generate_constants` writes the files a Bazel build writes, byte
for byte: the Python module, its stub, and the C++ header, whose include
guard comes from the rules fingerprint where it was random, read from
/dev/urandom; it refuses a rules file with a repeated name or a name no
constant can have, and a call that names no output. And it reads the rules file once, so that one given
as a pipe has the fingerprint of its text, where a second read for the
fingerprint found the pipe empty and wrote 00000000."""

import os
import subprocess
import tempfile
import unittest
import zlib

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

    def test_rules_from_a_pipe_are_read_once(self):
        """The tests' rules given on /dev/stdin, a pipe: the module carries
        the CRC-32 of the text piped, and the build's types."""
        with open(runfile(RULES), "rb") as rules:
            text = rules.read()
        path = os.path.join(self.out, "piped.py")
        result = subprocess.run(
            [MXCONTROL, "generate_constants", "/dev/stdin", "--python", path], input=text, capture_output=True, cwd=ROOT
        )
        self.assertEqual(0, result.returncode, result.stderr)
        generated = read(path)
        self.assertIn('RULES_FINGERPRINT = "%08x"' % zlib.crc32(text), generated)
        self.assertEqual(read(GENERATED["py"]).split("RULES_FINGERPRINT")[1], generated.split("RULES_FINGERPRINT")[1])

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

    def test_a_name_no_constant_can_have_is_refused(self):
        """A name the generated files cannot hold as it is: not an
        identifier, a keyword of Python or C++, two underscores first, or a
        name the generated code uses where the constants are. Each was
        written as it was, into a module that did not import, a get_name()
        shadowed, or a header that did not compile."""
        for name in ("MY-REQUEST", "1ST", "class", "delete", "__hidden", "idtoname", "get_name", "t", "default_"):
            with self.subTest(name=name):
                rules = os.path.join(self.out, "bad.rules")
                with open(rules, "w") as handle:
                    handle.write('peer { type: 100 name: "A" }\ntype { type: 150 name: "%s" }\n' % name)
                written = os.path.join(self.out, "bad.py")
                result = self.run_it(rules, "--python", written)
                self.assertNotEqual(0, result.returncode)
                self.assertIn("the name '%s' of type 150 cannot be a constant" % name, result.stderr)
                self.assertFalse(os.path.exists(written), "nothing written for a bad file")

    def test_an_unknown_extension_is_refused(self):
        result = self.run_it(RULES, "--python", os.path.join(self.out, "constants.txt"))
        self.assertNotEqual(0, result.returncode)
        self.assertIn(".h, .py or .pyi", result.stderr)


if __name__ == "__main__":
    unittest.main()
