"""Status replies are forced past a full queue, but not without bound.

The multiplexer answers a peer's control request (PEER_CONTROL,
RULES_CONTROL, RECORDING_CONTROL) even when the peer's queue is full, so
that a draining backend hears its confirmation behind the work queued for
it. Forced frames go past the queue's limit by FORCED_FRAMES_PAST_FULL_QUEUE
at most: past that a reply is dropped, logged as any full queue's drop,
where every reply used to be queued, and a peer sending requests and never
reading grew the multiplexer's memory until it ran out.

A reply carries the request's workflow, as every reply does, which the
tests make large: the replies to FILL_FRAMES requests whose workflow is
fill_size() bytes outgrow what the kernel may buffer on this machine
(its tcp_wmem and tcp_rmem limits), whatever those are, and the small
ones after them overflow the queue.
"""

import unittest

from multiplexer._native import FORCED_FRAMES_PAST_FULL_QUEUE
from multiplexer.Multiplexer_pb2 import MultiplexerMessage, PeerControl, Routing
from multiplexer.multiplexer_constants import peers, types
from multiplexer.testing import Cluster, runfile, wait_until
from multiplexer.testing.buffers import FILL_FRAMES, fill_frames, fill_size
from multiplexer.testing.raw_peer import RawPeer, frame

RULES = runfile("tests/testing.rules")
TINY_QUEUE = 1  # TEST_TINY_QUEUE's queue_size in the rules
WORKFLOW = b"w" * (16 * 1024)  # copied into every reply
CHUNK = b"x" * (1024 * 1024)  # what fills a peer's queue past its socket buffers


def queue_full_line(peer: RawPeer) -> str:
    """What the multiplexer logs when the peer's full queue drops a message."""
    return "outgoing queue full, dropping message to peer %d" % peer.instance_id


def status_request(peer: RawPeer, workflow: bytes = b"") -> MultiplexerMessage:
    """A PEER_CONTROL from `peer` that changes nothing, asking for the
    default routing, which the multiplexer answers with a PEER_STATUS
    carrying `workflow` back."""
    control = PeerControl(routing=Routing())
    return peer.message(control.SerializeToString(), types.PEER_CONTROL, workflow=workflow)


class ForcedFramesTest(unittest.TestCase):
    """See the module docstring."""

    def test_a_status_reply_goes_past_a_full_queue(self) -> None:
        """Why replies are forced: a peer whose queue is full of what was
        routed to it, a draining backend under load say, still gets the
        answer to its request, behind that work. The code before the cap
        passed this too; it guards the room the cap leaves."""
        with Cluster(1, rules=RULES) as cluster:
            peer = RawPeer(cluster.endpoints[0], peers.TEST_TINY_QUEUE)
            sender = RawPeer(cluster.endpoints[0], peers.WEBSITE)
            try:
                peer.handshake()
                sender.handshake()
                for payload in fill_frames() + [CHUNK] * 2:  # the sockets, then past the queue of one
                    sender.send(payload, types.PYTHON_TEST_REQUEST, to=peer.instance_id)
                wait_until(lambda: cluster.mx[0].log_contains(queue_full_line(peer)), 30, "the peer's queue full")
                request = status_request(peer)
                peer.send_raw(frame(request.SerializeToString()))
                self.assertEqual(request.id, peer.receive_type(types.PEER_STATUS, timeout=30).references)
            finally:
                peer.close()
                sender.close()

    def test_a_peer_that_never_reads_cannot_grow_its_queue_with_status_requests(self) -> None:
        """Requests sent and never read: once FORCED_FRAMES_PAST_FULL_QUEUE
        replies wait past the full queue, the next ones are dropped and
        logged. Before, every one was queued, however many came."""
        with Cluster(1, rules=RULES) as cluster:
            flood = RawPeer(cluster.endpoints[0], peers.TEST_TINY_QUEUE)
            try:
                flood.handshake()
                request = frame(status_request(flood, WORKFLOW).SerializeToString())
                flood.send_raw(request)
                flood.receive_type(types.PEER_STATUS)
                # Twice what the kernel may buffer, in replies, then the queue and its forced room twice over.
                filling = frame(status_request(flood, b"w" * fill_size()).SerializeToString())
                for _ in range(FILL_FRAMES):
                    flood.send_raw(filling)
                flood.send_raw(request * 2 * (TINY_QUEUE + FORCED_FRAMES_PAST_FULL_QUEUE))
                wait_until(lambda: cluster.mx[0].log_contains(queue_full_line(flood)), 60, "a status reply dropped")
            finally:
                flood.close()


if __name__ == "__main__":
    unittest.main()
