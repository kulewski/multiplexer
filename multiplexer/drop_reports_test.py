"""Every message a client gives up on is counted and told to its on_drop,
with its id and why, where most went without a word: what waited for room
past its timeout, what waited for a connection past its timeout, a pinned
lane's messages lost with their connection, and what a shutdown leaves. A
client that shuts down with its multiplexer frozen has every message it
sent either delivered or reported, and none of those reported arrives. And
flush_all() returns False when a message it waited for was given up on,
in every client, where it counted the drop as done and returned True.
And whichever way a client sent a message, its drop line names the id and
the type the library was given with the serialized message, which it
parses nothing for, and on_drop hears that id.
"""

import asyncio
import contextlib
import itertools
import os
import re
import sys
import tempfile
import threading
import time
import unittest
from collections.abc import Callable, Iterator

from multiplexer import recording
from multiplexer.aio import AsyncClient
from multiplexer.clients import Client
from multiplexer.multiplexer_constants import peers, types
from multiplexer.mxclient import DropReason, MultiplexerClientError, NotConnected
from multiplexer.Recording_pb2 import RECORDING_CONTROL, RecordingControl
from multiplexer.testing import Cluster, FakePeer, Mx, wait_until
from multiplexer.testing import runfile
from multiplexer.testing.buffers import past_the_queue
from multiplexer.threaded_client import ThreadedClient

RULES = runfile("tests/testing.rules")  # the file the constants were generated from

EVENT = types.PYTHON_TEST_REQUEST
CHUNK = b"x" * (16 * 1024)
FILLER_TIMEOUT = (
    120  # seconds a message filling a connection may wait for room: none is given up on, however slow the fill
)
FIRST_TYPE = 3000  # DropNamesTest's message types, one each, from this one up: nothing routes them
QUERY = types.TEST_UNROUTED  # a query's type, which none of the messages filling a connection has


class Drops:
    """What on_drop heard, (message id, reason) in order; from any thread."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.heard: list[tuple[int, DropReason]] = []

    def __call__(self, message_id: int, reason: DropReason) -> None:
        with self.lock:
            self.heard.append((message_id, reason))

    def wait_for(self, message_id: int, timeout: float = 10) -> list[tuple[int, DropReason]]:
        """Everything heard, once `message_id` was among it or `timeout` passed."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with self.lock:
                if any(heard_id == message_id for heard_id, _ in self.heard):
                    return list(self.heard)
            time.sleep(0.01)
        with self.lock:
            return list(self.heard)


def thawing(drops: Drops, frozen: Mx) -> Callable[[int, DropReason], None]:
    """An on_drop that keeps every drop in `drops` and thaws `frozen` at the
    first SHUT_DOWN, so that the multiplexer reads again while the client's
    close still reads on for its end."""
    thawed = threading.Event()

    def heard(message_id: int, reason: DropReason) -> None:
        """Keeps the drop; the shutdown's first thaws the multiplexer."""
        drops(message_id, reason)
        if reason == DropReason.SHUT_DOWN and not thawed.is_set():
            thawed.set()
            frozen.resume()

    return heard


class DropReportsTest(unittest.TestCase):
    """One reason at a time, then the whole of a shutdown."""

    def test_flush_all_is_false_when_a_message_it_waited_for_was_dropped(self) -> None:
        """A flush of written messages is True; with the only multiplexer
        gone, a message held past its 0.3 s timeout is dropped while the
        next flush waits for it, which then returns False, at the drop, not
        at its own 30 s."""
        for name in ("SyncClient", "ThreadedClient", "AsyncClient"):
            with self.subTest(client=name):
                drops = Drops()
                with Cluster(1, rules=RULES) as cluster:
                    if name == "AsyncClient":

                        async def flushes(drops: Drops = drops) -> tuple[bool, bool, float]:
                            client = AsyncClient(cluster.endpoints, peers.TEST_ACTIVE_CLIENT, on_drop=drops)
                            try:
                                await client.send_message(b"written", type=EVENT)
                                written = await client.flush_all(10)
                                cluster.mx[0].stop()
                                await asyncio.to_thread(wait_until, lambda: client.connections_count() == 0, 30, "gone")
                                await client.send_message(b"held", type=EVENT, timeout=0.3)
                                started = time.monotonic()
                                held = await client.flush_all(30)
                                return written, held, time.monotonic() - started
                            finally:
                                await client.aclose(timeout=0)

                        written, held, waited = asyncio.run(flushes())
                    else:
                        client: "Client | ThreadedClient"
                        if name == "SyncClient":
                            client = Client(cluster.endpoints, type=peers.WEBSITE, on_drop=drops)
                        else:
                            client = ThreadedClient(cluster.endpoints, type=peers.TEST_ACTIVE_CLIENT, on_drop=drops)
                        try:
                            client.send_message(b"written", type=EVENT)
                            written = client.flush_all(10)
                            cluster.mx[0].stop()
                            if name == "ThreadedClient":
                                wait_until(
                                    lambda client=client: client.connections_count() == 0, 30, "the connection gone"
                                )
                            client.send_message(b"held", type=EVENT, timeout=0.3)
                            started = time.monotonic()
                            held = client.flush_all(30)
                            waited = time.monotonic() - started
                        finally:
                            client.shutdown(timeout=0)
                    self.assertTrue(written)
                    self.assertFalse(held)
                    self.assertLess(waited, 20, "ended at the drop")
                    self.assertEqual([DropReason.NO_CONNECTION], [reason for _, reason in drops.heard])

    def test_a_message_that_waited_for_room_past_its_timeout(self) -> None:
        drops = Drops()
        with Cluster(1, rules=RULES) as cluster, FakePeer(cluster, peers.PYTHON_TEST_SERVER):
            client = Client(cluster.endpoints, type=peers.WEBSITE, on_drop=drops)
            try:
                cluster.mx[0].pause()
                try:
                    for payload in past_the_queue(CHUNK):
                        client.send_message(payload, type=EVENT, timeout=FILLER_TIMEOUT)
                    late = client.send_message(b"late", type=EVENT, timeout=0.3)
                    client.flush_all(0.6)  # the loop runs past its deadline
                finally:
                    cluster.mx[0].resume()
                self.assertEqual([(late, DropReason.NO_ROOM)], drops.heard)
                self.assertEqual(1, client.dropped)
            finally:
                client.shutdown()

    def test_a_message_that_waited_for_a_connection_past_its_timeout(self) -> None:
        drops = Drops()
        with Cluster(1, rules=RULES) as cluster:
            cluster.mx[0].stop()
            client = ThreadedClient(cluster.endpoints, type=peers.TEST_ACTIVE_CLIENT, timeout=0.2, on_drop=drops)
            try:
                waiting = client.send_message(b"waits", type=EVENT, timeout=0.3)
                self.assertEqual([(waiting, DropReason.NO_CONNECTION)], drops.wait_for(waiting))
                self.assertEqual(1, client.dropped)
            finally:
                client.shutdown()

    def test_a_pinned_lanes_messages_lost_with_their_connection(self) -> None:
        drops = Drops()
        with Cluster(2, rules=RULES) as cluster, FakePeer(cluster, peers.PYTHON_TEST_SERVER):
            client = ThreadedClient(cluster.endpoints, type=peers.TEST_ACTIVE_CLIENT, on_drop=drops)
            try:
                lane = client.lane(pinned=True)
                client.send_message(b"first", type=EVENT, multiplexer=lane, flush=True)
                frozen = cluster.multiplexer_at(lane.connection.endpoint)
                frozen.pause()
                for payload in past_the_queue(CHUNK):
                    client.send_message(payload, type=EVENT, multiplexer=lane, timeout=FILLER_TIMEOUT)
                last = client.send_message(b"last", type=EVENT, multiplexer=lane, timeout=FILLER_TIMEOUT)
                frozen.kill()
                heard = drops.wait_for(last)
                self.assertIn((last, DropReason.CONNECTION_LOST), heard)
                self.assertEqual({DropReason.CONNECTION_LOST}, {reason for _, reason in heard})
                self.assertEqual(len(heard), client.dropped)
            finally:
                client.shutdown()

    def test_every_message_is_delivered_or_reported_at_shutdown(self) -> None:
        """Sent to a frozen multiplexer, then the client shuts down: what was
        written arrives, the rest is reported, and nothing is both. The
        multiplexer reads again at the first drop reported, while the
        client's close still reads on for the multiplexer's end, so that it
        takes what was written within the close's bound, however long the
        freeze took: one still stalled past CLOSE_READ_SECONDS may lose what
        the client's kernel held (semantics.md), as in
        threaded_client_test.cc's twin."""
        for name in ("SyncClient", "ThreadedClient"):
            with self.subTest(client=name):
                drops = Drops()
                with (
                    Cluster(1, rules=RULES) as cluster,
                    FakePeer(cluster, peers.PYTHON_TEST_SERVER) as receiver,
                ):
                    frozen = cluster.mx[0]
                    heard = thawing(drops, frozen)
                    if name == "SyncClient":
                        client: Client | ThreadedClient = Client(cluster.endpoints, type=peers.WEBSITE, on_drop=heard)
                    else:
                        client = ThreadedClient(cluster.endpoints, type=peers.TEST_ACTIVE_CLIENT, on_drop=heard)
                    frozen.pause()
                    sent = set()
                    for payload in past_the_queue(CHUNK):
                        sent.add(client.send_message(payload, type=EVENT, timeout=FILLER_TIMEOUT))
                    client.shutdown()
                    frozen.resume()  # thawed already, unless nothing was dropped
                    reported = {message_id for message_id, _ in drops.heard}
                    self.assertTrue(reported, "a frozen multiplexer left something to report")
                    self.assertEqual({DropReason.SHUT_DOWN}, {reason for _, reason in drops.heard})
                    self.assertEqual(len(drops.heard), client.dropped)
                    arrived = {
                        mxmsg.id for mxmsg in receiver.wait_for(EVENT, count=len(sent) - len(reported), timeout=60)
                    }
                    self.assertEqual(set(), arrived & reported, "reported dropped, yet it arrived")
                    self.assertEqual(sent, arrived | reported, "neither delivered nor reported")


@contextlib.contextmanager
def stderr_to(path: str) -> Iterator[None]:
    """Descriptor 2, which the C++ side of an in-process client logs to,
    goes to `path` meanwhile; as in drop_lines_test.py."""
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


def named(path: str) -> set[tuple[int, int]]:
    """The (id, type) each drop line in the log at `path` names: the first
    line of a kind does, a kind being the reason and the type (LogSummary)."""
    with open(path, "rb") as log:
        text = log.read().decode("utf-8", "replace")
    found = re.finditer(r"message dropped: [^\n]*; id (\d+), type (\d+)", text)
    return {(int(match.group(1)), int(match.group(2))) for match in found}


class DropNamesTest(unittest.TestCase):
    """Every way a Python client sends gives the library the message's id
    and type with the serialized message: its drop line names both, and
    on_drop hears the id. Each message has a type of its own, so that its
    line is the first of its kind, which names it."""

    def test_every_send_with_nothing_connected(self) -> None:
        """A SyncClient's and a ThreadedClient's sends, to one connection
        and to ALL, flushing or not, with no time to wait, and an
        AsyncClient's flushing send: each message is dropped at once, at
        its deadline or at the shutdown, and named."""
        drops = Drops()
        message_types = itertools.count(FIRST_TYPE)
        sent: set[tuple[int, int]] = set()
        with tempfile.TemporaryDirectory() as scratch:
            log_path = os.path.join(scratch, "client.log")
            with stderr_to(log_path):
                clients: list[Client | ThreadedClient] = [
                    Client([], type=peers.WEBSITE, on_drop=drops),
                    ThreadedClient([], type=peers.TEST_ACTIVE_CLIENT, on_drop=drops),
                ]
                for client in clients:
                    try:
                        for multiplexer in (client.ONE, client.ALL):
                            for flush in (False, True):
                                mxmsg = client.new_message(message=b"", type=next(message_types))
                                sent.add((mxmsg.id, mxmsg.type))
                                with contextlib.suppress(NotConnected):  # a flushing send's, nothing connected
                                    client.send_message(mxmsg, multiplexer=multiplexer, flush=flush, timeout=0)
                    finally:
                        client.shutdown()

                async def flushing_send() -> None:
                    """The AsyncClient's flushing send, awaiting the io thread."""
                    client = AsyncClient([], peers.TEST_ACTIVE_CLIENT, on_drop=drops)
                    try:
                        mxmsg = client.new_message(message=b"", type=next(message_types))
                        sent.add((mxmsg.id, mxmsg.type))
                        with self.assertRaises(NotConnected):
                            await client.send_message(mxmsg, flush=True, timeout=0)
                    finally:
                        await client.aclose()

                asyncio.run(flushing_send())
            self.assertEqual(9, len(sent))
            self.assertEqual(sent, {name for name in named(log_path) if name[1] >= FIRST_TYPE})
            self.assertEqual({message_id for message_id, _ in sent}, {message_id for message_id, _ in drops.heard})

    def test_a_query_and_a_recording_control_on_a_full_connection(self) -> None:
        """A SyncClient's connection full, its multiplexer frozen: a typed
        query's request and then its search wait for room past their time,
        or until the shutdown, as a recording control's copy does, and each
        is named by its type and by an id on_drop heard; the client makes
        their ids, so the test cannot know them. The query's outcome is not
        what this is about."""
        drops = Drops()
        with Cluster(1, rules=RULES) as cluster, tempfile.TemporaryDirectory() as scratch:
            log_path = os.path.join(scratch, "client.log")
            with stderr_to(log_path):
                client = Client(cluster.endpoints, type=peers.WEBSITE, on_drop=drops)
                cluster.mx[0].pause()
                try:
                    for payload in past_the_queue(CHUNK):
                        client.send_message(payload, type=EVENT, timeout=FILLER_TIMEOUT)
                    with self.assertRaises(MultiplexerClientError):
                        client.query(b"", QUERY, timeout=0.1)
                    self.assertEqual([], recording.control(client, RecordingControl.STATUS, timeout=0))
                finally:
                    client.shutdown(0)  # what still waits for room is dropped now
                    cluster.mx[0].resume()
            names = named(log_path)
            heard = {message_id for message_id, _ in drops.heard}
            for message_type in (QUERY, types.BACKEND_FOR_PACKET_SEARCH, RECORDING_CONTROL):
                ids = {message_id for message_id, named_type in names if named_type == message_type}
                self.assertEqual(1, len(ids), names)
                self.assertLessEqual(ids, heard)


if __name__ == "__main__":
    unittest.main()
