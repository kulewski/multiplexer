"""The crate's generated files are what their generators write from the
files of this repository, checked where no Rust toolchain is needed: the
constants, src/constants.rs, are `mxcontrol generate_constants
multiplexer.rules --rust` byte for byte, and the integration tests' ones,
tests/support/testing_constants.rs, the same of tests/testing.rules; the
protocol code in src/proto names in its header the CRC-32 of each .proto file it was generated from,
which must be the file's as it is (rust/codegen writes the code itself and
format.sh checks it where cargo is installed). The crate's version, in
Cargo.toml and in the BUILD file, is the repository's, which a release's
"Version" commit raises in every place at once."""

import os
import re
import subprocess
import tempfile
import unittest
import zlib

from multiplexer import release
from multiplexer.testing import mx_runfile, runfile

MXCONTROL = mx_runfile("mxcontrol/mxcontrol")
CRATE = "rust/mx-multiplexer"
PROTO_FILES = ["multiplexer.rs", "mx.logging.rs"]


def read(path: str) -> bytes:
    """The file's bytes."""
    with open(path, "rb") as handle:
        return handle.read()


class GeneratedTest(unittest.TestCase):
    """See the module docstring."""

    def generated(self, rules: str) -> bytes:
        """What `mxcontrol generate_constants RULES --rust` writes, run from
        the root on the path the header records, as it was generated."""
        written = os.path.join(tempfile.mkdtemp(), "constants.rs")
        command = [MXCONTROL, "generate_constants", rules, "--rust", written]
        root = os.path.dirname(runfile("multiplexer.rules"))
        subprocess.run(command, cwd=root, check=True, capture_output=True)
        return read(written)

    def test_the_constants_are_generated_from_the_system_rules(self):
        self.assertEqual(read(runfile(f"{CRATE}/src/constants.rs")), self.generated("multiplexer.rules"))

    def test_the_tests_constants_are_generated_from_the_tests_rules(self):
        self.assertEqual(
            read(runfile(f"{CRATE}/tests/support/testing_constants.rs")), self.generated("tests/testing.rules")
        )

    def test_the_crate_has_the_repository_version(self):
        manifest = read(runfile(f"{CRATE}/Cargo.toml")).decode()
        build = read(runfile(f"{CRATE}/BUILD")).decode()
        self.assertIn(f'\nversion = "{release.version}"\n', manifest)
        self.assertIn(f'    version = "{release.version}",\n', build)

    def test_the_protocol_code_is_generated_from_the_proto_files_as_they_are(self):
        for name in PROTO_FILES:
            header = read(runfile(f"{CRATE}/src/proto/{name}")).decode()
            inputs = re.findall(r"^// input (\S+) ([0-9a-f]{8})$", header, re.MULTILINE)
            self.assertTrue(inputs, name)
            for proto, crc in inputs:
                self.assertEqual(
                    f"{zlib.crc32(read(runfile(proto))):08x}",
                    crc,
                    f"{name} is stale: run cargo run --manifest-path rust/codegen/Cargo.toml",
                )


if __name__ == "__main__":
    unittest.main()
