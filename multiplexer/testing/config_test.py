"""The command line mx_integration_test builds, as Config parses it, and how
runfiles paths resolve for a test's own workspace and for @mx."""

import os
import unittest
from unittest import mock

from multiplexer import testing
from multiplexer.testing import Config, Role, configure, mx_runfile, runfile


class ConfigTest(unittest.TestCase):
    """--mx, --roles, --role-binaries, --rules and --params; the rest is unittest's."""

    def test_parses_every_option(self):
        with mock.patch.dict(os.environ, {"TEST_SRCDIR": "/rf", "TEST_WORKSPACE": "app"}):
            config = Config(
                [
                    "--mx",
                    "2",
                    "--roles",
                    '{"backend": "bin", "client": "cc"}',
                    "--role-binaries",
                    '{"backend": "services/search/fake_index"}',
                    "--rules",
                    "config/multiplexer.rules",
                    "--params",
                    '{"count": 7}',
                    "-v",
                ]
            )
        self.assertEqual(2, config.mx)
        self.assertEqual("bin", config.lang("backend"))
        self.assertEqual("cc", config.lang("client"))
        self.assertEqual("py", config.lang("event_client"), "the default player")
        self.assertEqual({"backend": "services/search/fake_index"}, config.role_binaries)
        self.assertEqual("/rf/app/config/multiplexer.rules", config.rules)
        self.assertEqual(7, config.param("count"))
        self.assertIsNone(config.param("missing"))
        self.assertEqual(["-v"], config.rest)

    def test_defaults(self):
        config = Config([])
        self.assertEqual(
            (1, {}, {}, None, {}), (config.mx, config.roles, config.role_binaries, config.rules, config.params)
        )


class RunfilesTest(unittest.TestCase):
    """Paths of the test's workspace, of another repository, and of @mx."""

    def test_runfile_resolves_under_the_test_workspace(self):
        with mock.patch.dict(os.environ, {"TEST_SRCDIR": "/rf", "TEST_WORKSPACE": "app"}):
            self.assertEqual("/rf/app/services/fake", runfile("services/fake"))
            self.assertEqual("/rf/mx/tests/roles/py/backend", runfile("external/mx/tests/roles/py/backend"))
        with mock.patch.dict(os.environ, {"TEST_SRCDIR": ""}):
            self.assertEqual(os.path.abspath("services/fake"), runfile("services/fake"))

    def test_mx_runfile_is_the_repository_root(self):
        """mx_runfile() finds this repository by where this package sits, so
        it works whatever the consuming workspace calls it."""
        self.assertTrue(os.path.exists(mx_runfile("multiplexer.rules")))
        self.assertTrue(os.path.exists(mx_runfile("mxcontrol/mxcontrol")))
        self.assertEqual(mx_runfile("multiplexer/testing/__init__.py"), os.path.abspath(testing.__file__))


class RoleExecutableTest(unittest.TestCase):
    """Who plays a role: the shipped roles, or a binary named in the BUILD file."""

    def tearDown(self):
        testing.CONFIG = None

    def test_shipped_roles(self):
        self.assertEqual([mx_runfile("tests/roles/py/backend")], Role.executable("backend", "py"))
        self.assertEqual([mx_runfile("tests/roles/cc/mxtestroles"), "client"], Role.executable("client", "cc"))

    def test_named_binary(self):
        with mock.patch.dict(os.environ, {"TEST_SRCDIR": "/rf", "TEST_WORKSPACE": "app"}):
            configure(["--role-binaries", '{"backend": "services/fake"}'])
            self.assertEqual(["/rf/app/services/fake"], Role.executable("backend", "bin"))
            with self.assertRaises(ValueError):
                Role.executable("client", "bin")
        with self.assertRaises(ValueError):
            Role.executable("client", "rust")


if __name__ == "__main__":
    unittest.main()
