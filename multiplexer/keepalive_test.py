"""The multiplexer turns TCP keepalive on for every connection it accepts,
so that a peer whose host or network is gone gives its descriptor back
after 90 s, as the heartbeats make an active peer do: a passive peer, and
one that has not sent its welcome, owe no heartbeats, nothing asked them
anything, and such a socket stayed for good. Read from /proc/net/tcp, where
the kernel shows the timer a socket has armed: keepalive's, with the idle
time the multiplexer set, on a connection that never sends its welcome.
"""

import os
import unittest

from multiplexer import _native
from multiplexer.multiplexer_constants import peers
from multiplexer.testing import Cluster, runfile
from multiplexer.testing.raw_peer import RawPeer

RULES = runfile("tests/testing.rules")
KEEPALIVE_TIMER = 2  # /proc/net/tcp's "tr" for the keepalive timer
TICKS = os.sysconf("SC_CLK_TCK")  # the unit of its "tm->when"


def timer_of(local_port: int, remote_port: int) -> tuple[int, int]:
    """The timer the IPv4 socket with these ports has armed, and the
    ticks until it fires, from /proc/net/tcp."""
    with open("/proc/net/tcp") as table:
        next(table)  # the header
        for line in table:
            fields = line.split()
            local, remote, timer = fields[1], fields[2], fields[5]
            if int(local.split(":")[1], 16) == local_port and int(remote.split(":")[1], 16) == remote_port:
                which, when = timer.split(":")
                return int(which, 16), int(when, 16)
    raise LookupError("no socket from port %d to port %d" % (local_port, remote_port))


class KeepaliveTest(unittest.TestCase):
    """See the module docstring."""

    def test_a_connection_that_never_welcomes_has_keepalive_armed(self) -> None:
        with Cluster(1, rules=RULES) as cluster:
            port = cluster.endpoints[0][1]
            silent = RawPeer(cluster.endpoints[0], peers.PYTHON_TEST_CLIENT)
            later = RawPeer(cluster.endpoints[0], peers.PYTHON_TEST_CLIENT)
            try:
                # Accepted one at a time, in order: once the later one is
                # answered, the silent one is accepted and set up.
                later.handshake()
                which, when = timer_of(port, silent.sock.getsockname()[1])
                self.assertEqual(KEEPALIVE_TIMER, which, "the keepalive timer armed")
                self.assertGreater(when, 0)
                self.assertLessEqual(when, _native.NO_HEARTBIT_SO_PREPARE_DROP_INTERVAL * TICKS)
            finally:
                silent.close()
                later.close()


if __name__ == "__main__":
    unittest.main()
