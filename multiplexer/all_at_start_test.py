"""A message sent to ALL just after a client starts reaches every
multiplexer, the one whose first connection is still in its handshake
included: its copy waits in that connection's backlog and goes in at the
welcome, where only the multiplexers that had welcomed the client got
one. A multiplexer frozen before the client connects to it welcomes the
client only once resumed, the connect accepted by the kernel meanwhile;
a receiver connected to it before the freeze gets the event then.
Counted, not timed: the receiver's wait only detects a failure.
"""

import unittest

from multiplexer.multiplexer_constants import peers, types
from multiplexer.testing import Cluster, runfile
from multiplexer.testing.raw_peer import RawPeer
from multiplexer.threaded_client import ThreadedClient

RULES = runfile("tests/testing.rules")
BOUND = 30  # seconds the receiver waits for the event: a failure detector only


class AllAtStartTest(unittest.TestCase):
    """See the module docstring."""

    def test_an_all_send_during_a_first_handshake_reaches_that_multiplexer(self):
        with Cluster(2, rules=RULES) as cluster:
            late = cluster.mx[1]
            receiver = RawPeer(cluster.endpoints[1], peers.TEST_EVENT_BACKEND)
            receiver.handshake()
            late.pause()  # it welcomes nobody from here, the client included
            with ThreadedClient([cluster.endpoints[0]], peers.TEST_CLIENT) as client:
                client.connect(cluster.endpoints[1], timeout=0)  # started; its handshake waits for the frozen one
                client.send_message(message=b"at start", type=types.TEST_EVENT, multiplexer=ThreadedClient.ALL)
                late.resume()
                event = receiver.receive_type(types.TEST_EVENT, timeout=BOUND)
                self.assertEqual(b"at start", event.message)
            receiver.close()


if __name__ == "__main__":
    unittest.main()
