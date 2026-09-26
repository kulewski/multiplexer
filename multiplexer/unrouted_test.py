"""What the multiplexer logs when a rule queues a message nowhere, and why:
no peer of the type connected, routing off on every one, or the queue full
on every one that takes it, which a receiver that never reads brings about
whatever the machine's speed. Under the rule's defaults the sender gets a
DELIVERY_ERROR in each case."""

import unittest

from multiplexer.Multiplexer_pb2 import Routing
from multiplexer.multiplexer_constants import peers, types
from multiplexer.testing import Cluster, runfile, wait_until
from multiplexer.testing.buffers import fill_frames
from multiplexer.testing.raw_peer import RawPeer
from multiplexer.threaded_client import ThreadedClient

RULES = runfile("tests/testing.rules")  # the file the constants were generated from
PAYLOAD = b"x" * (1024 * 1024)
TYPE = "of type 207 (TEST_TINY_QUEUE)"


class UnroutedTest(unittest.TestCase):
    def assert_logged(self, cluster: Cluster, line: str) -> None:
        """Waits until the multiplexer's log holds `line`, and checks it
        never said that nobody of the type was there."""
        multiplexer = cluster.mx[0]
        wait_until(lambda: multiplexer.log_contains(line), 20, repr(line))
        self.assertFalse(multiplexer.log_contains("routing while none present " + TYPE))

    def test_no_peer_of_the_type(self):
        """With nobody of the type connected, the line is the one earlier releases logged."""
        with Cluster(1, rules=RULES) as cluster:
            sender = RawPeer(cluster.endpoints[0], peers.TEST_EVENT_CLIENT)
            sender.handshake()
            sender.send(b"hello", types.TEST_TINY_QUEUE_EVENT)
            sender.receive_type(types.DELIVERY_ERROR, timeout=20)
            wait_until(
                lambda: cluster.mx[0].log_contains("routing while none present " + TYPE), 20, "the line for nobody"
            )
            sender.close()

    def test_routing_off_on_every_peer(self):
        """A peer of the type whose routing takes nothing, as while it drains, is there but not taking."""
        with Cluster(1, rules=RULES) as cluster:
            receiver = ThreadedClient(cluster.endpoints, type=peers.TEST_TINY_QUEUE)
            try:
                receiver.set_routing(Routing(any=False, all=False, last_resort=False))
                wait_until(receiver.routing_acknowledged, 20, "the routing acknowledged")
                sender = RawPeer(cluster.endpoints[0], peers.TEST_EVENT_CLIENT)
                sender.handshake()
                sender.send(b"hello", types.TEST_TINY_QUEUE_EVENT)
                sender.receive_type(types.DELIVERY_ERROR, timeout=20)
                self.assert_logged(cluster, "routing off on every peer " + TYPE)
                sender.close()
            finally:
                receiver.shutdown()

    def test_every_queue_full(self):
        """A receiver that never reads fills its sockets and then the multiplexer's queue of one."""
        with Cluster(1, rules=RULES) as cluster:
            receiver = RawPeer(cluster.endpoints[0], peers.TEST_TINY_QUEUE)
            receiver.handshake()
            sender = RawPeer(cluster.endpoints[0], peers.TEST_EVENT_CLIENT)
            sender.handshake()
            for payload in fill_frames() + [PAYLOAD] * 4:  # the sockets, then past the queue of one
                sender.send(payload, types.TEST_TINY_QUEUE_EVENT)
            sender.receive_type(types.DELIVERY_ERROR, timeout=20)
            self.assert_logged(cluster, "queue full on every peer " + TYPE + " that takes it")
            receiver.close()
            sender.close()


if __name__ == "__main__":
    unittest.main()
