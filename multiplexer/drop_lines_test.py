"""What the multiplexer and the client library log when messages go nowhere:
the first line of a kind at once, the rest counted into one line a second,
"<the line> [N more in the last 1.0 s]" (LogSummary). Each test makes a
known number of messages go nowhere, known by construction or counted on
the wire as the DELIVERY_ERRORs a sender gets back, and checks that the
lines stand for exactly that many messages while being a handful, where
the code before logged one to three lines per message."""

import contextlib
import os
import random
import re
import sys
import tempfile
import threading
import unittest
from collections.abc import Iterator

from multiplexer.clients import Client
from multiplexer.multiplexer_constants import peers, types
from multiplexer.mxclient import OperationTimedOut
from multiplexer.testing import Cluster, runfile, wait_until
from multiplexer.testing.buffers import fill_frames
from multiplexer.testing.raw_peer import RawPeer
from multiplexer.threaded_client import ThreadedClient

RULES = runfile("tests/testing.rules")  # the file the constants were generated from
COUNT = 3000  # messages that go nowhere in each test
FEW = 50  # lines at most for COUNT messages: about two a second per kind, for far less than 25 s
BATCH = 100  # messages a RawPeer sends before reading what came back for them
NOBODY = "of type 207 (TEST_TINY_QUEUE)"


def said(log: str, text: str) -> tuple[int, int]:
    """How many messages the lines about `text` stand for, the first line
    of a kind counting one and each summary its N, and how many lines
    there were."""
    total = lines = 0
    for line in log.splitlines():
        if text not in line:
            continue
        lines += 1
        more = re.search(re.escape(text) + r" \[(\d+) more in the last \d+\.\d s\]", line)
        total += int(more.group(1)) if more else 1
    return total, lines


def read(path: str) -> str:
    """The file's text; empty while it does not exist."""
    try:
        with open(path, "rb") as log:
            return log.read().decode("utf-8", "replace")
    except OSError:
        return ""


def wait_for_said(path: str, text: str, count: int) -> tuple[int, int]:
    """Waits until the lines about `text` in the log at `path` stand for
    `count` messages, the last summary coming within a second, and returns
    said(): the total, which a test then compares, and the lines."""
    wait_until(lambda: said(read(path), text)[0] >= count, 20, "lines standing for %d messages: %r" % (count, text))
    return said(read(path), text)


@contextlib.contextmanager
def stderr_to(path: str) -> Iterator[None]:
    """Descriptor 2, which the C++ side of an in-process client logs to,
    goes to `path` meanwhile."""
    sys.stderr.flush()
    saved = os.dup(2)
    with open(path, "wb") as target:
        os.dup2(target.fileno(), 2)
    try:
        yield
    finally:
        sys.stderr.flush()
        os.dup2(saved, 2)
        os.close(saved)


def send_and_count_errors(sender: RawPeer, count: int, type_: int, **fields: object) -> None:
    """Sends `count` messages in batches, reading the DELIVERY_ERROR each
    one brings back before the next batch, so that no queue on the way
    back fills."""
    for start in range(0, count, BATCH):
        batch = min(BATCH, count - start)
        for _ in range(batch):
            sender.send(b"x", type_, **fields)
        for _ in range(batch):
            sender.receive_type(types.DELIVERY_ERROR, timeout=20)


class MultiplexerLinesTest(unittest.TestCase):
    """The multiplexer's lines, read from its log."""

    def test_a_type_nobody_is_connected_as(self) -> None:
        """COUNT messages of a type nobody takes: one line about it at once,
        the rest in summaries, and no line per message saying a
        DELIVERY_ERROR went back."""
        with Cluster(1, rules=RULES) as cluster:
            sender = RawPeer(cluster.endpoints[0], peers.TEST_EVENT_CLIENT)
            sender.handshake()
            send_and_count_errors(sender, COUNT, types.TEST_TINY_QUEUE_EVENT)
            total, lines = wait_for_said(cluster.mx[0].log_path, "routing while none present " + NOBODY, COUNT)
            self.assertEqual(COUNT, total)
            self.assertLessEqual(lines, FEW)
            self.assertEqual((0, 0), said(read(cluster.mx[0].log_path), "errors when delivering"))
            sender.close()

    def test_an_addressee_that_is_not_connected(self) -> None:
        """COUNT messages to an instance id nobody has: the same, for the line naming that id."""
        with Cluster(1, rules=RULES) as cluster:
            sender = RawPeer(cluster.endpoints[0], peers.TEST_EVENT_CLIENT)
            sender.handshake()
            nobody = random.randint(1, 2**62)
            send_and_count_errors(sender, COUNT, types.TEST_EVENT, to=nobody, report_delivery_error=True)
            line = "message to %d which is not connected; dropping" % nobody
            total, lines = wait_for_said(cluster.mx[0].log_path, line, COUNT)
            self.assertEqual(COUNT, total)
            self.assertLessEqual(lines, FEW)
            sender.close()

    def test_a_peer_whose_queue_is_full(self) -> None:
        """A peer that never reads, sent messages addressed to it until its
        sockets and its queue of one are full: the line naming it stands
        for as many messages as DELIVERY_ERRORs came back, counted by a
        reader on the sender's socket until a message the sender addressed
        to itself after the last one, which the multiplexer routes after
        every error it sent back."""
        with Cluster(1, rules=RULES) as cluster:
            receiver = RawPeer(cluster.endpoints[0], peers.TEST_TINY_QUEUE)
            receiver.handshake()
            sender = RawPeer(cluster.endpoints[0], peers.TEST_EVENT_CLIENT)
            sender.handshake()
            sender.sock.settimeout(60)
            errors = []
            marker = random.randint(1, 2**62)

            def count_errors() -> None:
                while True:
                    mxmsg = sender.receive(timeout=60)
                    if mxmsg.type == types.DELIVERY_ERROR:
                        errors.append(mxmsg.id)
                    elif mxmsg.type == types.TEST_EVENT and mxmsg.message == str(marker).encode():
                        return

            reader = threading.Thread(target=count_errors)
            reader.start()
            for payload in fill_frames() + [b"x" * (64 * 1024)] * (COUNT // 10):  # the sockets, then past its queue
                sender.send(payload, types.TEST_EVENT, to=receiver.instance_id, report_delivery_error=True)
            sender.send(str(marker).encode(), types.TEST_EVENT, to=sender.instance_id)
            reader.join(60)
            self.assertFalse(reader.is_alive(), "the marker came back")
            self.assertGreaterEqual(len(errors), COUNT // 10, "the queue was full for the last ones at least")
            line = "outgoing queue full, dropping message to peer %d %s" % (receiver.instance_id, NOBODY)
            total, lines = wait_for_said(cluster.mx[0].log_path, line, len(errors))
            self.assertEqual(len(errors), total)
            self.assertLessEqual(lines, FEW)
            receiver.close()
            sender.close()


class ClientLinesTest(unittest.TestCase):
    """The client library's lines, from this process's own descriptor 2."""

    def test_a_send_only_client_whose_errors_fill_its_queue(self) -> None:
        """A synchronous client that only sends, to a type nobody takes, and
        never reads: the DELIVERY_ERRORs fill its incoming queue and the
        rest are dropped. Every error is accounted for: held in the queue,
        said dropped by the client, or said dropped by the multiplexer when
        the client's queue there was full; and the client's lines are few."""
        with Cluster(1, rules=RULES) as cluster, tempfile.TemporaryDirectory() as scratch:
            log_path = os.path.join(scratch, "client.log")
            with stderr_to(log_path):
                client = Client(cluster.endpoints, type=peers.WEBSITE)
                try:
                    for _ in range(COUNT):
                        client.send_message(b"x", type=types.TEST_TINY_QUEUE_EVENT, flush=True)
                    wait_for_said(cluster.mx[0].log_path, "routing while none present " + NOBODY, COUNT)
                    held = 0
                    while True:  # the rest arrives while this runs the loop; then nothing for a second
                        try:
                            client.read_message(timeout=1)
                        except OperationTimedOut:
                            break
                        held += 1
                finally:
                    client.shutdown()  # says what is still counted
            dropped, lines = said(read(log_path), "incoming_queue_full, dropping")
            at_multiplexer, _ = said(read(cluster.mx[0].log_path), "outgoing queue full, dropping message to peer")
            self.assertEqual(COUNT, held + dropped + at_multiplexer, read(log_path)[-2000:])
            self.assertGreater(dropped, 0, "the queue was full")
            self.assertLessEqual(lines, FEW)

    def test_a_threaded_client_without_on_message(self) -> None:
        """COUNT messages addressed to a ThreadedClient given no on_message
        callback, which drops each: the lines stand for COUNT and are few."""
        with Cluster(1, rules=RULES) as cluster, tempfile.TemporaryDirectory() as scratch:
            log_path = os.path.join(scratch, "client.log")
            with stderr_to(log_path):
                client = ThreadedClient(cluster.endpoints, type=peers.TEST_ACTIVE_CLIENT)
                try:
                    cluster.wait_for_peer(peers.TEST_ACTIVE_CLIENT)
                    sender = RawPeer(cluster.endpoints[0], peers.TEST_EVENT_CLIENT)
                    sender.handshake()
                    for _ in range(COUNT):
                        sender.send(b"x", types.TEST_EVENT, to=client.instance_id)
                    line = "messages of type %d dropped: this client has no on_message callback" % types.TEST_EVENT
                    total, lines = wait_for_said(log_path, line, COUNT - 1)  # the first line names the message
                    sender.close()
                finally:
                    client.shutdown()
            first = "message #"  # the first line, which names the one message it is about
            self.assertEqual(
                1, sum(1 for text in read(log_path).splitlines() if first in text and "no on_message" in text)
            )
            self.assertEqual(COUNT - 1, total)
            self.assertLessEqual(lines, FEW)


if __name__ == "__main__":
    unittest.main()
