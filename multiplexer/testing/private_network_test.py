"""Every test runs on a network of its own, a loopback and nothing else
(.bazelrc: --sandbox_default_allow_network=false, under Bazel's
linux-sandbox): tests running at once never take each other's ports, the
one a restarted multiplexer binds again included, which another test's
bind(0) could take while it was free. With MX_REQUIRE_PRIVATE_NETWORK set,
as check.sh and the release's tests set it, a test that shares the host's
network fails; without it, on a machine whose sandbox cannot make a
network namespace, it is skipped.
"""

import os
import unittest


def interfaces() -> list[str]:
    """The network interfaces of this process's network namespace, from /proc/net/dev."""
    with open("/proc/net/dev") as devices:
        return [line.split(":", 1)[0].strip() for line in devices.readlines()[2:]]


class PrivateNetworkTest(unittest.TestCase):
    """See the module docstring."""

    def test_a_test_has_a_loopback_of_its_own(self) -> None:
        seen = interfaces()
        if seen != ["lo"] and not os.environ.get("MX_REQUIRE_PRIVATE_NETWORK"):
            self.skipTest("this test shares the host's network: %s" % seen)
        self.assertEqual(["lo"], seen, "the test shares the host's network, and other tests' ports")


if __name__ == "__main__":
    unittest.main()
