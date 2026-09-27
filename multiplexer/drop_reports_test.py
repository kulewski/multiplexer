"""Every message a client gives up on is counted and told to its on_drop,
with its id and why, where most went without a word: what waited for room
past its timeout, what waited for a connection past its timeout, a pinned
lane's messages lost with their connection, and what a shutdown leaves. A
client that shuts down with its multiplexer frozen has every message it
sent either delivered or reported, and none of those reported arrives. And
flush_all() returns False when a message it waited for was given up on,
in every client, where it counted the drop as done and returned True.
"""

import asyncio

import threading
import time
import unittest

from multiplexer.aio import AsyncClient
from multiplexer.clients import Client
from multiplexer.multiplexer_constants import peers, types
from multiplexer.mxclient import DropReason
from multiplexer.testing import Cluster, FakePeer, wait_until
from multiplexer.testing import runfile
from multiplexer.threaded_client import ThreadedClient

RULES = runfile("tests/testing.rules")  # the file the constants were generated from

EVENT = types.PYTHON_TEST_REQUEST
CHUNK = b"x" * (16 * 1024)


def frames_to_fill(size: int) -> int:
    """How many messages of `size` bytes a frozen multiplexer's connection
    cannot take: twice what the two sockets may buffer, the largest the
    kernel allows each, and twice the queue."""
    buffers = 0
    for path in ("/proc/sys/net/ipv4/tcp_wmem", "/proc/sys/net/ipv4/tcp_rmem"):
        try:
            with open(path) as limits:
                buffers += int(limits.read().split()[2])
        except (OSError, IndexError, ValueError):
            buffers += 8 << 20  # a guess where /proc does not say
    return 2 * buffers // size + 2 * 1024


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

                        async def flushes() -> tuple[bool, bool, float]:
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
                                wait_until(lambda: client.connections_count() == 0, 30, "the connection gone")
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
                    for _ in range(frames_to_fill(len(CHUNK))):
                        client.send_message(CHUNK, type=EVENT)
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
                for _ in range(frames_to_fill(len(CHUNK))):
                    client.send_message(CHUNK, type=EVENT, multiplexer=lane)
                last = client.send_message(b"last", type=EVENT, multiplexer=lane)
                frozen.kill()
                heard = drops.wait_for(last)
                self.assertIn((last, DropReason.CONNECTION_LOST), heard)
                self.assertEqual({DropReason.CONNECTION_LOST}, {reason for _, reason in heard})
                self.assertEqual(len(heard), client.dropped)
            finally:
                client.shutdown()

    def test_every_message_is_delivered_or_reported_at_shutdown(self) -> None:
        """Sent to a frozen multiplexer, then the client shuts down, then the
        multiplexer reads again: what was written arrives, the rest is
        reported, and nothing is both."""
        for name in ("SyncClient", "ThreadedClient"):
            with self.subTest(client=name):
                drops = Drops()
                with (
                    Cluster(1, rules=RULES) as cluster,
                    FakePeer(cluster, peers.PYTHON_TEST_SERVER) as receiver,
                ):
                    if name == "SyncClient":
                        client: Client | ThreadedClient = Client(cluster.endpoints, type=peers.WEBSITE, on_drop=drops)
                    else:
                        client = ThreadedClient(cluster.endpoints, type=peers.TEST_ACTIVE_CLIENT, on_drop=drops)
                    cluster.mx[0].pause()
                    sent = set()
                    for _ in range(frames_to_fill(len(CHUNK))):
                        sent.add(client.send_message(CHUNK, type=EVENT))
                    client.shutdown()
                    cluster.mx[0].resume()
                    reported = {message_id for message_id, _ in drops.heard}
                    self.assertTrue(reported, "a frozen multiplexer left something to report")
                    self.assertEqual({DropReason.SHUT_DOWN}, {reason for _, reason in drops.heard})
                    self.assertEqual(len(drops.heard), client.dropped)
                    arrived = {
                        mxmsg.id for mxmsg in receiver.wait_for(EVENT, count=len(sent) - len(reported), timeout=60)
                    }
                    self.assertEqual(set(), arrived & reported, "reported dropped, yet it arrived")
                    self.assertEqual(sent, arrived | reported, "neither delivered nor reported")


if __name__ == "__main__":
    unittest.main()
