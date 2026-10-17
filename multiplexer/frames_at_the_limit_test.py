"""Frames built around a peer's message near MAX_MESSAGE_SIZE, from Python:
a PING or a search whose echo would be over the limit is answered by a
Python BaseMultiplexerServer with BACKEND_ERROR saying so, and a tap's
record of a message near the limit has its payload cut to the longest that
fits, marked truncated. Both used to go over the limit: the backend raised
where it built the echo, and the multiplexer threw while routing, so the
message reached no tap and its sender's connection was never read again;
and a guess at what a tap's frame adds kept tens of bytes less than fit.
And a search comes back carrying its payload, as a PING does. And a
PEER_CONTROL whose answer, which repeats its key filters, would be over the
limit is refused with an error, the filters as they were, where the
multiplexer applied it and threw building the answer, which never came.
"""

import unittest

from multiplexer import recording
from multiplexer._native import MAX_MESSAGE_SIZE
from multiplexer.clients import BackendError, Client
from multiplexer.Multiplexer_pb2 import KeyFilter, MultiplexerMessage, PeerControl, PeerStatus, Routing
from multiplexer.Recording_pb2 import RECORDING_CONTROLLER, RECORDING_RECORD, Record
from multiplexer.multiplexer_constants import peers, types
from multiplexer.servers import BaseMultiplexerServer
from multiplexer.testing import BackendThread, Cluster, runfile
from multiplexer.testing.raw_peer import RawPeer, frame

RULES = runfile("tests/testing.rules")


class Quiet(BaseMultiplexerServer):
    """Answers nothing itself and records every handler exception."""

    def __init__(self, *args, **kwargs):
        """As BaseMultiplexerServer."""
        super().__init__(*args, **kwargs)
        self.exceptions: list[BaseException] = []

    def handle_message(self, mxmsg) -> None:
        """No request of the test's reaches it."""
        self.no_response()

    def on_handler_exception(self, exc) -> bool:
        """Count it and go on."""
        self.exceptions.append(exc)
        return True


def fill_to_the_limit(message) -> None:
    """Make `message` exactly MAX_MESSAGE_SIZE bytes long, serialized."""
    message.message = b"p" * (MAX_MESSAGE_SIZE - message.ByteSize() - 16)
    while message.ByteSize() < MAX_MESSAGE_SIZE:
        message.message += b"p"


def peer_control(peer: RawPeer, patterns: list[str], short_of_the_limit: int = 0) -> MultiplexerMessage:
    """A PEER_CONTROL from `peer` filtering PYTHON_TEST_REQUEST by
    `patterns`; with `short_of_the_limit`, exact keys of about 1000 bytes
    besides, as many as make the frame that many bytes short of
    MAX_MESSAGE_SIZE, the rest spread over them a few bytes each, so that
    every pattern is within MAX_KEY_PATTERN_SIZE, 1024. Every size is
    protobuf's: a key of 1000 bytes give or take 24 takes as many bytes
    more or less as it has."""
    control = PeerControl(routing=Routing())
    key_filter = control.key_filter.add(type=types.PYTHON_TEST_REQUEST, pattern=patterns)
    mxmsg = peer.message(b"", types.PEER_CONTROL)
    mxmsg.message = control.SerializeToString()
    if short_of_the_limit:
        target = MAX_MESSAGE_SIZE - short_of_the_limit
        key = "k" * 1000
        one = len(KeyFilter(type=1, pattern=[key, key]).SerializeToString()) - len(
            KeyFilter(type=1, pattern=[key]).SerializeToString()
        )
        key_filter.pattern.extend([key] * ((target - mxmsg.ByteSize()) // one))
        mxmsg.message = control.SerializeToString()
        missing = target - mxmsg.ByteSize()  # less than a key, or the few bytes the lengths grew by over
        for index in range(len(key_filter.pattern)):
            step = max(-24, min(24, missing))
            if not step:
                break
            key_filter.pattern[index] = "k" * (len(key_filter.pattern[index]) + step)
            missing -= step
        mxmsg.message = control.SerializeToString()
    return mxmsg


def reply_to(client: Client, message_id: int, timeout: float = 20.0):
    """The next message `client` reads that references `message_id`."""
    while True:
        mxmsg = client.read_message(timeout=timeout)
        if mxmsg.references == message_id:
            return mxmsg


class FramesAtTheLimitTest(unittest.TestCase):
    """See the module docstring."""

    def test_a_ping_too_big_to_echo_is_answered_with_backend_error(self) -> None:
        """The answer says why, the handler saw no exception, and the next PING has its echo."""
        with Cluster(1, rules=RULES) as cluster:
            with BackendThread(lambda: Quiet(cluster.endpoints, type=peers.PYTHON_TEST_SERVER)) as served:
                backend = served.backend
                assert backend is not None
                client = Client(cluster.endpoints, type=peers.WEBSITE)
                try:
                    ping = client.new_message(message=b"", type=types.PING, to=backend.conn.instance_id)
                    fill_to_the_limit(ping)
                    client.send_message(ping, flush=True, timeout=20)
                    with self.assertRaises(BackendError) as answered:  # how the client reads BACKEND_ERROR
                        reply_to(client, ping.id)
                    self.assertIn(b"echo of a PING", answered.exception.args[0])
                    small = client.send_message(b"bounce", type=types.PING, to=backend.conn.instance_id, flush=True)
                    self.assertEqual(b"bounce", reply_to(client, small).message)
                    self.assertEqual([], backend.exceptions)
                finally:
                    client.shutdown()

    def test_a_search_is_answered_with_its_payload_echoed(self) -> None:
        """A search comes back carrying its payload, as a PING does; one too big to come back gets BACKEND_ERROR."""
        with Cluster(1, rules=RULES) as cluster:
            with BackendThread(lambda: Quiet(cluster.endpoints, type=peers.PYTHON_TEST_SERVER)) as served:
                backend = served.backend
                assert backend is not None
                client = Client(cluster.endpoints, type=peers.WEBSITE)
                try:
                    to = backend.conn.instance_id
                    search = client.send_message(
                        b"what the searcher sent", type=types.BACKEND_FOR_PACKET_SEARCH, to=to, flush=True
                    )
                    answer = reply_to(client, search)
                    self.assertEqual(types.PING, answer.type)
                    self.assertEqual(b"what the searcher sent", answer.message)
                    big = client.new_message(message=b"", type=types.BACKEND_FOR_PACKET_SEARCH, to=to)
                    fill_to_the_limit(big)
                    client.send_message(big, flush=True, timeout=20)
                    with self.assertRaises(BackendError) as answered:
                        reply_to(client, big.id)
                    self.assertIn(b"echo of a search", answered.exception.args[0])
                    self.assertEqual([], backend.exceptions)
                finally:
                    client.shutdown()

    def test_a_tap_record_too_big_is_truncated_and_the_sender_goes_on(self) -> None:
        """A message at the limit comes to a tap in a record cut and marked,
        its payload the longest a tap's frame carries within the limit: the
        frame of the record as the tap got it fits, and one more byte of
        payload would not, both measured by protobuf, the frame's id and
        addressee at their largest, as the multiplexer measures it, where a
        guess at what the frame adds kept tens of bytes less. Every size
        here is protobuf's, never reckoned. The sender's next message is
        routed and recorded whole."""
        with Cluster(1, rules=RULES, remote_recording=True) as cluster:
            tapper = Client(cluster.endpoints, type=RECORDING_CONTROLLER)
            sender = Client(cluster.endpoints, type=peers.WEBSITE)
            try:
                records = recording.tap(tapper, timeout=20)
                big = sender.new_message(type=types.PYTHON_TEST_REQUEST, message=b"")
                big.message = b"x" * (MAX_MESSAGE_SIZE - big.ByteSize() - 16)
                big.message = b"x" * (len(big.message) + MAX_MESSAGE_SIZE - big.ByteSize())
                self.assertEqual(MAX_MESSAGE_SIZE, big.ByteSize(), "the message itself at the limit")
                sender.send_message(big, flush=True, timeout=20)
                sender.send_message(b"small", type=types.PYTHON_TEST_REQUEST, flush=True)
                seen: list[Record] = []
                for record in records:
                    if record.HasField("routed") and record.routed.type == types.PYTHON_TEST_REQUEST:
                        seen.append(record)
                        if len(seen) == 2:
                            break
                self.assertTrue(seen[0].routed.truncated)
                self.assertLess(len(seen[0].routed.payload), len(big.message))
                frame = MultiplexerMessage(id=2**64 - 1, to=2**64 - 1, type=RECORDING_RECORD)
                frame.sender = seen[0].multiplexer_id
                frame.message = seen[0].SerializeToString()
                self.assertLessEqual(frame.ByteSize(), MAX_MESSAGE_SIZE, "the tap's frame within the limit")
                longer = Record()
                longer.CopyFrom(seen[0])
                longer.routed.payload += b"x"
                frame.message = longer.SerializeToString()
                self.assertGreater(frame.ByteSize(), MAX_MESSAGE_SIZE, "a byte more of payload would have fitted")
                self.assertFalse(seen[1].routed.truncated)
                self.assertEqual(b"small", seen[1].routed.payload)
            finally:
                sender.shutdown()
                tapper.shutdown()

    def test_a_peer_control_whose_answer_would_not_fit_is_refused(self) -> None:
        """Key filters that make a PEER_CONTROL 8 bytes short of the limit,
        exact keys of 1000 bytes: its PEER_STATUS, which repeats them and
        adds the multiplexer's id, `to` and `references`, would be over it.
        The answer comes with the error, no filters in effect, as before;
        then a small change is applied and answered."""
        with Cluster(1, rules=RULES) as cluster:
            peer = RawPeer(cluster.endpoints[0], peers.PYTHON_TEST_SERVER)
            try:
                peer.handshake()
                big = peer_control(peer, [], short_of_the_limit=8)
                self.assertEqual(MAX_MESSAGE_SIZE - 8, big.ByteSize())
                self.assertLessEqual(
                    max(len(p) for p in PeerControl.FromString(big.message).key_filter[0].pattern), 1024
                )
                peer.sock.settimeout(60)
                peer.send_raw(frame(big.SerializeToString()))
                status = self.answer(peer, big.id)
                self.assertIn("over MAX_MESSAGE_SIZE", status.error)
                self.assertEqual([], list(status.key_filter), "the filters as they were")
                small = peer_control(peer, ["model-a"])
                peer.send_raw(frame(small.SerializeToString()))
                status = self.answer(peer, small.id)
                self.assertFalse(status.HasField("error"), status.error)
                self.assertEqual(["model-a"], [p for f in status.key_filter for p in f.pattern])
            finally:
                peer.close()

    def answer(self, peer: RawPeer, control_id: int) -> PeerStatus:
        """The PEER_STATUS answering the PEER_CONTROL `control_id`."""
        while True:
            mxmsg = peer.receive(timeout=60)
            if mxmsg.type == types.PEER_STATUS and mxmsg.references == control_id:
                return PeerStatus.FromString(mxmsg.message)


if __name__ == "__main__":
    unittest.main()
