"""A rule that queues a message nowhere is recorded with the reason its log
line gives, in one record for the rule, no recipient, the peer type and
whether the sender was told: NO_RECIPIENT only when nobody of the type is
connected, QUEUE_FULL when every peer that takes the message has its queue
full, NOT_ACCEPTED when none takes it by its routing; a fan-out (whom:
ALL) also records each peer it passed over. Every such rule was recorded
NO_RECIPIENT, which Recording.proto says is nobody connected. Counted, not
timed: the peer whose queue is filled never reads, and the sender's
marker to itself comes back only after everything it sent before was
routed, so the recording holds all of it.
"""

import random
import unittest

from multiplexer import recording
from multiplexer.Multiplexer_pb2 import Routing, WelcomeMessage
from multiplexer.multiplexer_constants import peers, types
from multiplexer.Recording_pb2 import RoutedMessage
from multiplexer.testing import Cluster, runfile
from multiplexer.testing.buffers import fill_frames
from multiplexer.testing.raw_peer import RawPeer

RULES = runfile("tests/testing.rules")  # the file the constants were generated from
FILLER = b"x" * (64 * 1024)


def welcomed(endpoint: tuple[str, int], peer_type: int, routing: Routing | None = None) -> RawPeer:
    """A raw peer of `peer_type`, its welcome carrying `routing` when given,
    once the multiplexer's welcome came back."""
    peer = RawPeer(endpoint, peer_type)
    welcome = WelcomeMessage(type=peer_type, id=peer.instance_id)
    if routing is not None:
        welcome.routing.CopyFrom(routing)
    peer.send(welcome.SerializeToString(), types.CONNECTION_WELCOME)
    peer.receive_type(types.CONNECTION_WELCOME)
    return peer


class UnroutedRecordsTest(unittest.TestCase):
    """See the module docstring."""

    def test_a_rule_that_queued_nothing_is_recorded_with_its_reason(self) -> None:
        """A request nobody of its type can take, one to a type whose only
        peer is full, and one and a fan-out to a type whose only peer turned
        its routing off."""
        with Cluster(1, rules=RULES, record=True, record_payload_bytes=1) as cluster:
            endpoint = cluster.endpoints[0]
            full = welcomed(endpoint, peers.TEST_TINY_QUEUE)  # reads nothing
            closed = welcomed(endpoint, peers.TEST_EVENT_BACKEND, Routing(any=False, all=False))
            sender = welcomed(endpoint, peers.TEST_EVENT_CLIENT)
            sender.sock.settimeout(60)
            for payload in fill_frames() + [FILLER] * 4:  # the sockets, then past its queue of one
                sender.send(payload, types.TEST_EVENT, to=full.instance_id)
            nobody = sender.send(b"n", types.PYTHON_TEST_REQUEST)
            queue_full = sender.send(b"q", types.TEST_TINY_QUEUE_EVENT)
            any_off = sender.send(b"a", types.TEST_EVENT_ANY)
            all_off = sender.send(b"e", types.TEST_EVENT)
            marker = random.randint(1, 2**62)
            sender.send(str(marker).encode(), types.TEST_EVENT, to=sender.instance_id)
            while True:  # the delivery errors come back first, then the marker
                mxmsg = sender.receive(timeout=60)
                if mxmsg.type == types.TEST_EVENT and mxmsg.message == str(marker).encode():
                    break
            cluster.mx[0].stop()
            routed = [
                record.routed for record in recording.read(cluster.mx[0].record_file) if record.HasField("routed")
            ]
            for sent in (full, closed, sender):
                sent.close()

        def records_of(message_id: int) -> list[tuple[int, int, int]]:
            """(disposition, recipient, recipient's type) of each record of the message."""
            return [(r.disposition, r.recipient, r.recipient_peer_type) for r in routed if r.id == message_id]

        self.assertEqual([(RoutedMessage.NO_RECIPIENT, 0, peers.PYTHON_TEST_SERVER)], records_of(nobody))
        self.assertEqual([(RoutedMessage.QUEUE_FULL, 0, peers.TEST_TINY_QUEUE)], records_of(queue_full))
        self.assertEqual([(RoutedMessage.NOT_ACCEPTED, 0, peers.TEST_EVENT_BACKEND)], records_of(any_off))
        self.assertEqual(
            [
                (RoutedMessage.NOT_ACCEPTED, closed.instance_id, peers.TEST_EVENT_BACKEND),
                (RoutedMessage.NOT_ACCEPTED, 0, peers.TEST_EVENT_BACKEND),
            ],
            records_of(all_off),
        )
        told = {r.id: r.error_reported for r in routed if r.recipient == 0}
        self.assertEqual(
            {nobody: True, queue_full: True, any_off: True, all_off: True},
            {message_id: told[message_id] for message_id in (nobody, queue_full, any_off, all_off)},
            "the rule's record says the sender was told",
        )


if __name__ == "__main__":
    unittest.main()
