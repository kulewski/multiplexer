"""A synchronous Client whose connection cannot take more: messages beyond
its queue wait for room rather than raising NotConnected while the
connection lives, and a flushing send waits for that room asleep, where it
went round a loop at full speed. The multiplexer is frozen with its socket
open, which fills the queue whatever the machine's speed.
"""

import time
import unittest

from multiplexer.clients import Client
from multiplexer.multiplexer_constants import peers, types
from multiplexer.mxclient import OperationTimedOut
from multiplexer.testing import Cluster, FakePeer
from multiplexer.testing import runfile

RULES = runfile("tests/testing.rules")  # the file the constants were generated from

EVENT = types.PYTHON_TEST_REQUEST
CHUNK = b"x" * (16 * 1024)


def frames_to_fill(size: int) -> int:
    """How many messages of `size` bytes a frozen multiplexer's connection
    cannot take: twice what the two sockets may buffer, the largest the
    kernel allows each, and twice the queue."""
    buffers = 0
    for path in ("/proc/sys/net/ipv4/tcp_wmem", "/proc/sys/net/ipv4/tcp_rmem"):
        try:
            with open(path) as limits:
                buffers += int(limits.read().split()[2])
        except (OSError, IndexError, ValueError):
            buffers += 8 << 20  # a guess where /proc does not say
    return 2 * buffers // size + 2 * 1024


class FullConnectionTest(unittest.TestCase):
    """The multiplexer frozen with its socket open."""

    def test_sends_wait_for_room_and_a_flushing_one_sleeps(self) -> None:
        """Every send is taken while the connection lives, the ones beyond
        the queue waiting; a flushing send then times out having used next
        to no CPU for its half second; and once the multiplexer reads again,
        flush_all() sees everything written."""
        with Cluster(1, rules=RULES) as cluster, FakePeer(cluster, peers.PYTHON_TEST_SERVER):
            client = Client(cluster.endpoints, type=peers.WEBSITE)
            try:
                cluster.mx[0].pause()
                try:
                    for _ in range(frames_to_fill(len(CHUNK))):
                        client.send_message(CHUNK, type=EVENT)
                    cpu = time.process_time()
                    with self.assertRaises(OperationTimedOut):
                        client.send_message(b"last", type=EVENT, flush=True, timeout=0.5)
                    self.assertLess(time.process_time() - cpu, 0.1, "of the 0.5 s it waited")
                finally:
                    cluster.mx[0].resume()
                self.assertTrue(client.flush_all(60))
            finally:
                client.shutdown()


if __name__ == "__main__":
    unittest.main()
