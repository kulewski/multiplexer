"""a directly addressed message dropped for a full queue is reported as a delivery error.

A receiver of a peer type with queue_size 1 never reads. A sender addresses
it by instance id with report_delivery_error set and pushes far more than
the socket buffers hold; the multiplexer drops what it cannot queue and
sends the sender a DELIVERY_ERROR naming the receiver, the same as for an
absent peer.
"""

import unittest

from tests import harness
from tests.harness import Cluster, constants as C
from multiplexer.testing.raw_peer import RawPeer
from multiplexer.Multiplexer_pb2 import DeliveryError

MESSAGES = 24
PAYLOAD = b"x" * (1024 * 1024)


class DirectQueueFull(unittest.TestCase):
    """Checks that a directly addressed message dropped for a full queue is reported as a delivery error."""

    def test_dropped_message_is_reported(self):
        with Cluster(1) as cluster:
            receiver = RawPeer(cluster.endpoints[0], C.peers.TEST_TINY_QUEUE)
            receiver.handshake()
            sender = RawPeer(cluster.endpoints[0], C.peers.TEST_EVENT_CLIENT)
            sender.handshake()
            for _ in range(MESSAGES):
                sender.send(PAYLOAD, C.types.TEST_EVENT, to=receiver.instance_id, report_delivery_error=True)
            error = sender.receive_type(C.types.DELIVERY_ERROR, timeout=20)
            report = DeliveryError()
            report.ParseFromString(error.message)
            self.assertEqual(receiver.instance_id, report.failed_to)
            receiver.close()
            sender.close()


if __name__ == "__main__":
    harness.main()
