"""This repository's integration test harness: multiplexer.testing plus the
peers and types of tests/testing.rules as `constants`.

Everything a scenario uses (Cluster, spawn, Role, RawPeer, wait_until, ...)
is multiplexer.testing's, re-exported here; tests/README.md shows a
scenario. main() parses what mx_integration_test passed into CONFIG, here
and in multiplexer.testing, and runs unittest.
"""

import sys
import unittest

from multiplexer import testing
from multiplexer.testing import *  # noqa: F401, F403  (the harness API, one import for scenarios)
from multiplexer.testing import Config, Mx, Role, mx_runfile, output_dir, runfile  # noqa: F401
from tests import testing_constants as constants  # noqa: F401  (peers, types)

CONFIG: Config | None = None


def main() -> None:
    """Entry point for scenario files: parse the harness config, run unittest."""
    global CONFIG
    CONFIG = testing.configure()
    unittest.main(argv=[sys.argv[0]] + CONFIG.rest)
