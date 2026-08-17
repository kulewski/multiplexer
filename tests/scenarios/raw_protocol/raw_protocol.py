"""a peer speaking the wire format directly, without the client library.

Handshake, framing and CRC, then two TEST_EVENT messages, one with every byte
value and one of 1 MiB, must reach a backend intact.
"""

import hashlib
import os
import time
import unittest

from tests import harness
from tests.harness import Cluster, constants as C, spawn
from multiplexer.testing.raw_peer import RawPeer


class RawProtocol(unittest.TestCase):
    """Checks that a peer speaking the wire format directly, without the client library."""

    def test_handshake_framing_and_payload_integrity(self):
        cfg = harness.CONFIG
        with Cluster(1) as cluster:
            event_backend = spawn("event_backend", "py", mx=cluster.addresses, type=C.peers.TEST_EVENT_BACKEND, until=2)
            event_backend.wait_for("connected", connections=1)

            peer = RawPeer(cluster.mx[0].endpoint, C.peers.TEST_EVENT_CLIENT)
            answer, theirs = peer.handshake()
            self.assertEqual(C.types.CONNECTION_WELCOME, answer.type)
            self.assertEqual(C.peers.MULTIPLEXER, theirs.type, "we talk to a multiplexer")
            self.assertNotEqual(0, theirs.id)

            every_byte = bytes(range(256)) * 2
            big = os.urandom(1024 * 1024)
            peer.send(every_byte, C.types.TEST_EVENT)
            peer.send(big, C.types.TEST_EVENT)
            time.sleep(0.5)  # let the multiplexer read everything before we close
            peer.close()

            self.assertEqual(0, event_backend.wait())
            received = event_backend.events_of("received", type=C.types.TEST_EVENT)
            self.assertEqual([len(every_byte), len(big)], [r["size"] for r in received])
            self.assertEqual(
                [hashlib.sha256(every_byte).hexdigest(), hashlib.sha256(big).hexdigest()],
                [r["sha256"] for r in received],
            )
            self.assertTrue(all(r["from_"] == peer.instance_id for r in received))


if __name__ == "__main__":
    harness.main()
