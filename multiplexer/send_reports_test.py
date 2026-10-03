"""What a send reports, against real multiplexers: a frame whose write had
started when its connection closed reads what that write did, where it
read lost although it arrived; a flushing send whose message a dead
connection handed to another reports that other connection, where it
reported the dead one, and a reply then came back unawaited; and a pinned
lane's flushing send that runs out of time times out, where it raised
NotConnected with the lane open.
"""

import subprocess
import unittest

from multiplexer.clients import Client
from multiplexer.multiplexer_constants import peers, types
from multiplexer.mxclient import OperationTimedOut
from multiplexer.testing import Cluster, FakePeer
from multiplexer.testing import runfile
from multiplexer.testing.buffers import past_the_queue

RULES = runfile("tests/testing.rules")  # the file the constants were generated from

EVENT = types.PYTHON_TEST_REQUEST
REQUEST = types.PYTHON_TEST_REQUEST
RESPONSE = types.PYTHON_TEST_RESPONSE
UNROUTED = types.TEST_UNROUTED  # routed nowhere: fills a connection without a backend answering
CHUNK = b"x" * (16 * 1024)
FILLER_TIMEOUT = (
    120  # seconds a message filling a connection may wait for room: none is given up on, however slow the fill
)


class FrameBeingWrittenTest(unittest.TestCase):
    """The frame a connection was writing when it closed."""

    def test_a_frame_written_inside_its_call_reads_sent_after_shutdown(self) -> None:
        """On an idle connection the write happens inside the call that
        queues the frame, and its handler runs only in a later call: the
        shutdown that comes first used to report the frame lost although
        the backend got it."""
        with Cluster(1, rules=RULES) as cluster, FakePeer(cluster, peers.PYTHON_TEST_SERVER) as backend:
            client = Client(cluster.endpoints, type=peers.WEBSITE)
            mxmsg = client.new_message(message=b"last words", type=EVENT)
            heard: list[int] = []
            client.send_message(mxmsg, callback=heard.append)
            self.assertEqual([], heard, "its handler has not run yet")
            client.shutdown()
            self.assertEqual([1], heard, "written, not lost")
            backend.wait_for(EVENT, matching=lambda received: received.id == mxmsg.id)


class HandedOverTest(unittest.TestCase):
    """A message its connection died with before writing it, written by
    another."""

    def test_a_handed_over_request_is_answered_without_a_resend(self) -> None:
        """send_and_receive through a lane whose multiplexer is frozen with
        its connection full: the request waits there, the multiplexer is
        killed, and the request is handed to the other connection and
        written. The flushing send now reports that connection, and the
        reply is awaited there; it reported the dead one, so the request
        was taken for lost and went out again under a new id."""
        with (
            Cluster(2, rules=RULES) as cluster,
            FakePeer(cluster, peers.PYTHON_TEST_SERVER).reply_with(REQUEST, b"answer", RESPONSE) as backend,
        ):
            client = Client(cluster.endpoints, type=peers.WEBSITE)
            try:
                lane = client.lane()
                client.send_message(b"first", type=UNROUTED, multiplexer=lane, flush=True)
                frozen = cluster.multiplexer_at(lane.connection.endpoint)
                frozen.pause()
                for payload in past_the_queue(CHUNK):
                    client.send_message(payload, type=UNROUTED, multiplexer=lane, timeout=FILLER_TIMEOUT)
                assert frozen.proc is not None
                # another process kills it during the call below, whatever the call holds meanwhile
                frozen.expect_exit()
                killer = subprocess.Popen(["sh", "-c", "sleep 0.5; kill -9 %d" % frozen.proc.pid])
                question = client.new_message(message=b"question", type=REQUEST)
                attempts: dict = {}
                try:
                    reply, _ = client._send_and_receive(question, multiplexer=lane, attempts=attempts, timeout=30)
                finally:
                    killer.wait()
                self.assertEqual(len(attempts), 1, "sent once, not again under a new id")
                self.assertEqual(reply.references, next(iter(attempts)))
                self.assertEqual(len(backend.messages(REQUEST)), 1)
            finally:
                client.shutdown()


class PinnedLaneTimeoutTest(unittest.TestCase):
    """A pinned lane whose connection lives but has no room in time."""

    def test_a_pinned_lane_that_runs_out_of_time_times_out(self) -> None:
        """OperationTimedOut, as through any lane and as on ThreadedClient,
        the lane still open."""
        with Cluster(1, rules=RULES) as cluster, FakePeer(cluster, peers.PYTHON_TEST_SERVER):
            client = Client(cluster.endpoints, type=peers.WEBSITE)
            try:
                lane = client.lane(pinned=True)
                client.send_message(b"first", type=EVENT, multiplexer=lane, flush=True)
                cluster.mx[0].pause()
                try:
                    for payload in past_the_queue(CHUNK):
                        client.send_message(payload, type=UNROUTED, multiplexer=lane, timeout=FILLER_TIMEOUT)
                    with self.assertRaises(OperationTimedOut):
                        client.send_message(b"late", type=EVENT, multiplexer=lane, flush=True, timeout=0.2)
                    self.assertFalse(lane.closed)
                finally:
                    cluster.mx[0].resume()
            finally:
                client.shutdown()


if __name__ == "__main__":
    unittest.main()
