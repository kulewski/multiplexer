"""malformed and out-of-order input must cost the peer its connection,
never the multiplexer.

Each case sends bytes that used to crash the server or bypass the handshake,
then proves the server is still serving by completing a fresh handshake.
"""

import unittest

from tests import harness
from tests.harness import Cluster, constants as C, spawn
from multiplexer.Multiplexer_pb2 import WelcomeMessage
from multiplexer.testing.raw_peer import HEADER, RawPeer, frame


def registered(instance_id: int) -> str:
    """The start of the line the multiplexer logs when a peer with `instance_id` is in."""
    return '"registered connection id=%d ' % instance_id


def unregistered(instance_id: int) -> str:
    """The start of the line the multiplexer logs when a peer with `instance_id` leaves."""
    return '"unregistered connection id=%d ' % instance_id


class HostileInput(unittest.TestCase):
    """Checks that malformed and out-of-order input must cost the peer its connection,."""

    @staticmethod
    def log(cluster: Cluster) -> str:
        """The multiplexer's log so far: a line about a registration is written
        before the peer is sent its welcome, one about a departure before the
        connection is closed."""
        with open(cluster.mx[0].log_path) as log:
            return log.read()

    def assert_still_serving(self, cluster):
        """The multiplexer is alive and still completes a handshake."""
        self.assertTrue(cluster.mx[0].running, "the multiplexer process is alive")
        probe = RawPeer(cluster.mx[0].endpoint, C.peers.TEST_EVENT_CLIENT)
        _, theirs = probe.handshake()
        self.assertEqual(C.peers.MULTIPLEXER, theirs.type)
        probe.close()

    def test_oversized_length_header(self):
        with Cluster(1) as cluster:
            peer = RawPeer(cluster.mx[0].endpoint, C.peers.TEST_EVENT_CLIENT)
            peer.send_raw(HEADER.pack(0xFFFFFFFF, 0))
            self.assertTrue(peer.closed_by_peer(), "the offending connection is dropped")
            self.assert_still_serving(cluster)

    def test_zero_length_frame(self):
        with Cluster(1) as cluster:
            peer = RawPeer(cluster.mx[0].endpoint, C.peers.TEST_EVENT_CLIENT)
            peer.send_raw(HEADER.pack(0, 0))
            self.assertTrue(peer.closed_by_peer())
            self.assert_still_serving(cluster)

    def test_unknown_peer_type_in_override_rules(self):
        with Cluster(1) as cluster:
            peer = RawPeer(cluster.mx[0].endpoint, C.peers.TEST_EVENT_CLIENT)
            peer.handshake()
            mxmsg = peer.message(b"x", C.types.TEST_EVENT, report_delivery_error=True)
            mxmsg.override_rrules.add(peer_type=424242, whom=1)
            peer.send_raw(frame(mxmsg.SerializeToString()))
            error = peer.receive_type(C.types.DELIVERY_ERROR)
            self.assertEqual(mxmsg.id, error.references, "answered with a delivery error, not a crash")
            self.assert_still_serving(cluster)

    def test_message_before_welcome_is_not_routed(self):
        with Cluster(1) as cluster:
            event_backend = spawn("event_backend", "py", mx=cluster.addresses, type=C.peers.TEST_EVENT_BACKEND, until=1)
            event_backend.wait_for("connected", connections=1)
            intruder = RawPeer(cluster.mx[0].endpoint, C.peers.TEST_EVENT_CLIENT)
            intruder.send(b"INJECTED", C.types.TEST_EVENT)  # no handshake
            self.assertTrue(intruder.closed_by_peer())
            # The backend's first message is one a peer with a handshake
            # sends once the intruder is gone: INJECTED, had it been
            # routed, would have come before it.
            welcomed = RawPeer(cluster.mx[0].endpoint, C.peers.TEST_EVENT_CLIENT)
            welcomed.handshake()
            welcomed.send(b"after the intruder", C.types.TEST_EVENT)
            self.assertEqual(0, event_backend.wait())
            self.assertEqual(["after the intruder"], [r["payload"] for r in event_backend.events_of("received")])
            welcomed.close()
            self.assert_still_serving(cluster)

    def test_live_id_claimed_from_another_address_is_refused(self):
        with Cluster(1) as cluster:
            event_backend = spawn("event_backend", "py", mx=cluster.addresses, type=C.peers.TEST_EVENT_BACKEND, until=1)
            event_backend.wait_for("connected", connections=1)
            owner = RawPeer(cluster.mx[0].endpoint, C.peers.TEST_EVENT_CLIENT, source_address="127.0.0.1")
            owner.handshake()
            impostor = RawPeer(
                cluster.mx[0].endpoint,
                C.peers.TEST_EVENT_CLIENT,
                instance_id=owner.instance_id,
                source_address="127.0.0.2",
            )
            impostor.send(
                WelcomeMessage(type=C.peers.TEST_EVENT_CLIENT, id=owner.instance_id).SerializeToString(),
                C.types.CONNECTION_WELCOME,
            )
            self.assertTrue(impostor.closed_by_peer(), "a different host cannot take a live id")
            owner.send(b"still mine", C.types.TEST_EVENT)
            self.assertEqual(0, event_backend.wait())
            self.assertEqual(["still mine"], [r["payload"] for r in event_backend.events_of("received")])
            self.assert_still_serving(cluster)
            log = self.log(cluster)
            self.assertIn("refusing connection that claims live id %d" % owner.instance_id, log)
            self.assertEqual(1, log.count(registered(owner.instance_id)), "the impostor never registered")

    def test_same_address_reconnect_replaces_the_stale_connection(self):
        with Cluster(1) as cluster:
            event_backend = spawn("event_backend", "py", mx=cluster.addresses, type=C.peers.TEST_EVENT_BACKEND, until=1)
            event_backend.wait_for("connected", connections=1)
            stale = RawPeer(cluster.mx[0].endpoint, C.peers.TEST_EVENT_CLIENT)
            stale.handshake()
            fresh = RawPeer(cluster.mx[0].endpoint, C.peers.TEST_EVENT_CLIENT, instance_id=stale.instance_id)
            fresh.handshake()
            self.assertTrue(stale.closed_by_peer(), "the stale connection is replaced")
            fresh.send(b"reconnected", C.types.TEST_EVENT)
            self.assertEqual(0, event_backend.wait())
            self.assertEqual(["reconnected"], [r["payload"] for r in event_backend.events_of("received")])
            self.assert_still_serving(cluster)
            log = self.log(cluster)
            last = log.rindex(registered(stale.instance_id))
            self.assertLess(log.rindex(unregistered(stale.instance_id)), last, "the stale one leaves first")
            self.assertIn("; replaces its earlier connection", log[last : log.index("\n", last)])


if __name__ == "__main__":
    harness.main()
