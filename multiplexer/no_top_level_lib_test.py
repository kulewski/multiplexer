"""The package needs no top-level `lib` package: the LogEntry module is
multiplexer.Logging_pb2, where it was lib.logging.Logging_pb2, which an
installed wheel put on sys.path as a package named `lib`, and which a
program's own `lib` package shadowed, the protocol buffers then failing
to import. Shown in a child interpreter whose path puts a `lib` package of
its own first.
"""

import os
import subprocess
import sys
import tempfile
import unittest


class NoTopLevelLibTest(unittest.TestCase):
    """See the module docstring."""

    def test_a_program_with_a_lib_package_of_its_own_imports_the_protocol(self) -> None:
        with tempfile.TemporaryDirectory(dir=os.environ.get("TEST_TMPDIR")) as directory:
            os.mkdir(os.path.join(directory, "lib"))
            open(os.path.join(directory, "lib", "__init__.py"), "w").close()  # a program's own, with no logging in it
            environment = dict(os.environ, PYTHONPATH=os.pathsep.join([directory] + sys.path))
            child = subprocess.run(
                [sys.executable, "-c", "import multiplexer.Multiplexer_pb2"],
                env=environment,
                capture_output=True,
                text=True,
            )
            self.assertEqual(0, child.returncode, child.stderr)


if __name__ == "__main__":
    unittest.main()
