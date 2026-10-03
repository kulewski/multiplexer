"""mxcontrol's cap on DEBUG entries without --verbosity is its default,
MEDIUMVERBOSITY, which MX_LOG_VERBOSITY replaces only where it names DEBUG
or every level: a spec naming another level only, INFO:LOW say, or one
that does not parse, which the library ignores with a warning, left
DEBUG at the library's HIGHVERBOSITY. And --verbosity wins over the
variable. Told apart by a DEBUG line at HIGHVERBOSITY that every command
connecting to a multiplexer logs, a connection made, which a multiplexer
that is not there does not stop.
"""

import os
import subprocess
import unittest

from multiplexer.testing import mx_runfile

MXCONTROL = mx_runfile("mxcontrol/mxcontrol")
HIGH_LINE = "created new Connection"  # DEBUG at HIGHVERBOSITY


def logs_debug_high(spec: str | None, *general: str) -> bool:
    """Whether `mxcontrol rules status` against nothing logs HIGH_LINE,
    with MX_LOG_VERBOSITY `spec` (unset for None) and `general` options."""
    environment = dict(os.environ)
    environment.pop("MX_LOG_VERBOSITY", None)
    if spec is not None:
        environment["MX_LOG_VERBOSITY"] = spec
    run = subprocess.run(
        [MXCONTROL, *general, "rules", "status", "-M", "127.0.0.1:1", "--timeout", "0.1"],
        env=environment,
        capture_output=True,
        text=True,
        timeout=60,
    )
    return HIGH_LINE in run.stderr


class VerbosityDefaultTest(unittest.TestCase):
    """See the module docstring."""

    def test_the_variable_replaces_the_default_where_it_names_debug(self) -> None:
        cases = [
            (None, (), False),  # the default: MEDIUMVERBOSITY
            ("INFO:LOW", (), False),  # another level only
            ("NO SUCH SPEC", (), False),  # does not parse: ignored
            ("DEBUG:HIGH", (), True),
            ("HIGH", (), True),  # every level
            ("DEBUG:HIGH", ("--verbosity", "MEDIUMVERBOSITY"), False),  # the flag wins
        ]
        for spec, general, logged in cases:
            with self.subTest(spec=spec, general=general):
                self.assertEqual(logged, logs_debug_high(spec, *general))


if __name__ == "__main__":
    unittest.main()
