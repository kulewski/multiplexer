"""A synchronous Client whose connection cannot take more: messages beyond
its queue wait for room rather than raising NotConnected while the
connection lives, and a flushing send waits for that room asleep, where it
went round a loop at full speed. The multiplexer is frozen with its socket
open, which fills the queue whatever the machine's speed.
"""

import time
import unittest

from multiplexer._native import ROOM_GRACE_SECONDS
from multiplexer.clients import Client
from multiplexer.multiplexer_constants import peers, types
from multiplexer.mxclient import OperationTimedOut
from multiplexer.testing import Cluster, FakePeer
from multiplexer.testing import runfile
from multiplexer.testing.buffers import past_the_queue

RULES = runfile("tests/testing.rules")  # the file the constants were generated from

EVENT = types.PYTHON_TEST_REQUEST
CHUNK = b"x" * (16 * 1024)
FILLER_TIMEOUT = (
    120  # seconds a message filling a connection may wait for room: none is given up on, however slow the fill
)
# Times a wait asleep for half a second may be woken: at its deadline, by a
# heartbeat, for the GIL; a loop that polled every few milliseconds, a hundred.
WAKEUPS = 20


def voluntary_switches() -> int:
    """How often the calling thread has blocked and been woken so far: its
    voluntary_ctxt_switches in /proc/thread-self/status."""
    with open("/proc/thread-self/status") as status:
        for line in status:
            if line.startswith("voluntary_ctxt_switches:"):
                return int(line.split()[1])
    raise RuntimeError("/proc/thread-self/status has no voluntary_ctxt_switches")


class FullConnectionTest(unittest.TestCase):
    """The multiplexer frozen with its socket open."""

    def test_sends_wait_for_room_and_a_flushing_one_sleeps(self) -> None:
        """Every send is taken while the connection lives, the ones beyond
        the queue waiting; a flushing send then times out having used next
        to no CPU of its thread's own for its half second, where the
        process's counted the backend's thread too, and woken a few times
        at most; and once the multiplexer reads again, flush_all() sees
        everything written but that send's message, given up on as its
        moment past the call's deadline (ROOM_GRACE_SECONDS) is over, and so
        returns False. The test waits out that moment with the multiplexer
        still frozen: within it, room would still take the message. The
        backend receives every chunk, and that message not, as a marker
        sent after it on the same connection, which comes behind it, shows."""
        with Cluster(1, rules=RULES) as cluster, FakePeer(cluster, peers.PYTHON_TEST_SERVER) as backend:
            client = Client(cluster.endpoints, type=peers.WEBSITE)
            sending = past_the_queue(CHUNK)
            try:
                cluster.mx[0].pause()
                try:
                    for payload in sending:
                        client.send_message(payload, type=EVENT, timeout=FILLER_TIMEOUT)
                    cpu, woken = time.thread_time(), voluntary_switches()
                    with self.assertRaises(OperationTimedOut):
                        client.send_message(b"last", type=EVENT, flush=True, timeout=0.5)
                    self.assertLess(time.thread_time() - cpu, 0.1, "of the 0.5 s it waited")
                    self.assertLessEqual(voluntary_switches() - woken, WAKEUPS, "woken while it waited")
                    # Past the message's own deadline, still frozen: in flush_all() below its drop
                    # comes before any room, whichever the loop sees first. A lower bound: the
                    # longer, the surer.
                    time.sleep(10 * ROOM_GRACE_SECONDS)
                finally:
                    cluster.mx[0].resume()
                self.assertFalse(client.flush_all(60))
                self.assertEqual(1, client.dropped)
                client.send_message(b"marker", type=EVENT, flush=True)
                backend.wait_for(EVENT, timeout=60, matching=lambda mxmsg: mxmsg.message == b"marker")
                arrived = backend.messages(EVENT, lambda mxmsg: mxmsg.message not in (b"last", b"marker"))
                self.assertEqual(len(sending), len(arrived))
                self.assertEqual([], backend.messages(EVENT, lambda mxmsg: mxmsg.message == b"last"))
            finally:
                client.shutdown()


if __name__ == "__main__":
    unittest.main()
