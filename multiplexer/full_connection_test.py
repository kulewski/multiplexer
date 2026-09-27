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
from multiplexer.testing.buffers import past_the_queue

RULES = runfile("tests/testing.rules")  # the file the constants were generated from

EVENT = types.PYTHON_TEST_REQUEST
CHUNK = b"x" * (16 * 1024)
FILLER_TIMEOUT = (
    120  # seconds a message filling a connection may wait for room: none is given up on, however slow the fill
)


class FullConnectionTest(unittest.TestCase):
    """The multiplexer frozen with its socket open."""

    def test_sends_wait_for_room_and_a_flushing_one_sleeps(self) -> None:
        """Every send is taken while the connection lives, the ones beyond
        the queue waiting; a flushing send then times out having used next
        to no CPU for its half second; and once the multiplexer reads again,
        flush_all() sees everything written but that send's message, given
        up on right after it timed out, and so returns False. The backend
        receives every chunk, and that message not, as a marker sent after
        it on the same connection, which comes behind it, shows."""
        with Cluster(1, rules=RULES) as cluster, FakePeer(cluster, peers.PYTHON_TEST_SERVER) as backend:
            client = Client(cluster.endpoints, type=peers.WEBSITE)
            sending = past_the_queue(CHUNK)
            try:
                cluster.mx[0].pause()
                try:
                    for payload in sending:
                        client.send_message(payload, type=EVENT, timeout=FILLER_TIMEOUT)
                    cpu = time.process_time()
                    with self.assertRaises(OperationTimedOut):
                        client.send_message(b"last", type=EVENT, flush=True, timeout=0.5)
                    self.assertLess(time.process_time() - cpu, 0.1, "of the 0.5 s it waited")
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
