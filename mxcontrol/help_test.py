"""mxcontrol help and a usage error list the options of every subcommand
that takes any, generate_constants' and receivelogs' among them, which
printed their text alone: help receivelogs never showed -M, and
generate_constants' --python, --pyi and --cxx were names in the synopsis
without their descriptions.
"""

import subprocess
import unittest

from multiplexer.testing import mx_runfile

MXCONTROL = mx_runfile("mxcontrol/mxcontrol")


def output(*args: str) -> str:
    """What mxcontrol printed, both streams, whatever its exit."""
    run = subprocess.run([MXCONTROL, *args], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=60)
    return run.stdout


class HelpTest(unittest.TestCase):
    """See the module docstring."""

    def test_the_options_are_listed(self) -> None:
        """The help, and the refusal of an option the subcommand lacks."""
        expected = {
            "generate_constants": ["--python ARG", "--pyi ARG", "--cxx ARG", "write the C++ header here"],
            "receivelogs": ["-M, --multiplexer ARG"],
        }
        for subcommand, lines in expected.items():
            for args in (("help", subcommand), (subcommand, "--no-such-option")):
                with self.subTest(args=args):
                    printed = output(*args)
                    self.assertIn("Options:", printed)
                    for line in lines:
                        self.assertIn(line, printed)


if __name__ == "__main__":
    unittest.main()
