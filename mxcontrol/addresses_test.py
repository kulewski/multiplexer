"""mxcontrol takes an address as host:port, or [address]:port for an IPv6
address, in every subcommand: run_multiplexer's --address, and -M in
streamlogs and receivelogs (Task's client), in rules and recording
(EveryAddress). A malformed one is a malformed command line, exit 1 with
the reason, where run_multiplexer stopped on an assertion and the log
commands warned and went on without it; nothing listens or connects.
Over IPv6, where the machine has a loopback for it: a multiplexer on
[::1] writes its port file in brackets, `rules status` reaches it, and
receivelogs registers with it."""

import os
import socket
import subprocess
import tempfile
import unittest

from multiplexer.testing import Cluster, mx_runfile, runfile, wait_until

MXCONTROL = mx_runfile("mxcontrol/mxcontrol")
TESTING_RULES = runfile("tests/testing.rules")
BOUND = 30  # seconds a run may take: a failure detector only


def ipv6_loopback() -> bool:
    """Whether this machine has ::1 to listen on."""
    try:
        with socket.socket(socket.AF_INET6) as probe:
            probe.bind(("::1", 0))
        return True
    except OSError:
        return False


class AddressesTest(unittest.TestCase):
    """See the module docstring."""

    def test_a_malformed_address_is_a_malformed_command_line(self):
        port_file = os.path.join(tempfile.mkdtemp(), "mx.port")
        run = [MXCONTROL, "run_multiplexer", "--rules", TESTING_RULES, "--port-file", port_file, "--address"]
        for command, reason in (
            (run + ["::1:1980"], "--address: '::1:1980': an IPv6 address goes in brackets, as in [::1]:1980"),
            (run + ["localhost:0"], "--address: 'localhost:0': not an IP address"),
            (run + ["127.0.0.1:-1"], "--address: '127.0.0.1:-1': the port is a number from 0 to 65535"),
            (run + ["[::1]:65536"], "--address: '[::1]:65536': the port is a number from 0 to 65535"),
            ([MXCONTROL, "streamlogs", "-M", "::1:1980"], "--multiplexer: '::1:1980': an IPv6 address goes in"),
            ([MXCONTROL, "receivelogs", "-M", "[::1"], "--multiplexer: '[::1': no ] after the IPv6 address"),
            ([MXCONTROL, "rules", "status", "-M", "host"], "--multiplexer: 'host': no port"),
            (
                [MXCONTROL, "recording", "status", "-M", "[127.0.0.1]:1980"],
                "--multiplexer: '[127.0.0.1]:1980': brackets hold an IPv6 address",
            ),
        ):
            with self.subTest(command=command[1:]):
                ended = subprocess.run(command, stdin=subprocess.DEVNULL, capture_output=True, text=True, timeout=BOUND)
                self.assertEqual(1, ended.returncode, ended.stderr)
                self.assertIn(reason, ended.stderr)
                self.assertFalse(os.path.exists(port_file), "a multiplexer listened")

    @unittest.skipUnless(ipv6_loopback(), "no IPv6 loopback")
    def test_over_ipv6(self):
        with Cluster(1, rules=TESTING_RULES, host="::1") as cluster:
            multiplexer = cluster.mx[0]
            self.assertEqual("[::1]:%d" % multiplexer.port, multiplexer.address)  # as the port file has it
            status = subprocess.run(
                [MXCONTROL, "rules", "status", "-M", multiplexer.address],
                capture_output=True,
                text=True,
                timeout=BOUND,
            )
            self.assertEqual(0, status.returncode, status.stderr)
            self.assertRegex(status.stdout, r"^multiplexer \d+: rules ")
            receiver = subprocess.Popen(
                [MXCONTROL, "receivelogs", "-M", multiplexer.address],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            try:
                wait_until(
                    lambda: any(name == "LOG_RECEIVER_EXAMPLE" for _, name, _ in multiplexer.connected_peers()),
                    BOUND,
                    "receivelogs registered",
                )
            finally:
                receiver.terminate()
                receiver.wait(BOUND)


if __name__ == "__main__":
    unittest.main()
