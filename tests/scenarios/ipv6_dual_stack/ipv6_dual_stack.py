"""a multiplexer listening on [::] serves peers over IPv6 and over IPv4 at once.

One multiplexer listens on every address, Cluster(host="::"), and its
port file says [::]:PORT. The backend role is given [::1]:PORT, so it
reaches the multiplexer over IPv6 only; then one client role is given
127.0.0.1:PORT, IPv4 only, and another [::1]:PORT, and each one's queries
are answered by that backend. `mxcontrol rules status -M [::1]:PORT`
reaches the multiplexer too. Every role reads its --mx with the brackets,
the Python ones with multiplexer.endpoints, the C++ ones with
multiplexer/endpoint.h. The scenario needs an IPv6 loopback and IPv4
mapped onto an IPv6 socket bound to ::, which Linux does unless
net.ipv6.bindv6only is set; it is skipped without them, in a container
with IPv6 off say.
"""

import socket
import unittest

from tests import harness
from tests.harness import Cluster, constants as C, mxcontrol, spawn

QUERIES = 2


def dual_stack() -> bool:
    """Whether an IPv6 socket bound to :: takes IPv4 too on this machine:
    an IPv6 loopback, and net.ipv6.bindv6only off."""
    try:
        with socket.socket(socket.AF_INET6) as probe:
            probe.bind(("::1", 0))
        with open("/proc/sys/net/ipv6/bindv6only") as setting:
            return setting.read().strip() == "0"
    except OSError:
        return False


@unittest.skipUnless(dual_stack(), "no IPv6 loopback, or no IPv4 on an IPv6 socket")
class Ipv6DualStack(unittest.TestCase):
    """One multiplexer on [::], its peers over IPv6 and over IPv4."""

    def test_ipv6_and_ipv4_peers_of_one_multiplexer(self):
        cfg = harness.CONFIG
        with Cluster(1, host="::") as cluster:
            multiplexer = cluster.mx[0]
            self.assertEqual("[::]:%d" % multiplexer.port, multiplexer.address)  # as the port file has it
            ipv6 = "[::1]:%d" % multiplexer.port
            ipv4 = "127.0.0.1:%d" % multiplexer.port
            backend = spawn(
                "backend",
                cfg.lang("backend"),
                mx=[ipv6],
                type=C.peers.TEST_BACKEND_A,
                serves={C.types.TEST_REQUEST_A: C.types.TEST_RESPONSE},
                behaviour="upper",
            )
            backend.wait_for("connected", connections=1)
            backend_id = backend.events_of("connected")[0]["instance_id"]
            for address in (ipv4, ipv6):
                client = spawn(
                    "client",
                    cfg.lang("client"),
                    mx=[address],
                    type=C.peers.TEST_CLIENT,
                    count=QUERIES,
                    query=[(C.types.TEST_REQUEST_A, "abc")],
                )
                self.assertEqual(0, client.wait(), address)
                self.assertEqual([], client.events_of("error"), address)
                responses = client.events_of("response")
                self.assertEqual(QUERIES, len(responses), address)
                for response in responses:
                    self.assertEqual((backend_id, "ABC"), (response["sender"], response["payload"]), address)
            self.assertEqual(QUERIES * 2, len(backend.wait_for_count("request", QUERIES * 2)))
            status = mxcontrol("rules", "status", "-M", ipv6).stdout
            self.assertRegex(status, r"^multiplexer \d+: rules ")


if __name__ == "__main__":
    harness.main()
