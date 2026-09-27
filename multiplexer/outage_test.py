"""What a client does while no multiplexer is reachable, the same in every
client: a send returns at once and its message is held until a connection
comes up, written then, or dropped and reported at its timeout, where
the synchronous client raised NotConnected; a flushing send to every
connection waits for one through a restart, and returns once the first
copy is written; the messages a dead connection had not written wait for
the next connection, where they were dropped with it; a dead
connection's copies of messages sent to every connection are dropped and
reported, where they went to another connection, which then had two; and
a flush under way when a connection dies does not wait for what the dead
connection hands over that was sent after it began, where the hand-over
numbered those messages 0, which every flush waits for.
"""

import threading
import time
import unittest

from multiplexer.clients import Client
from multiplexer.multiplexer_constants import peers, types
from multiplexer.mxclient import DropReason
from multiplexer.testing import Cluster, FakePeer, wait_until
from multiplexer.testing import runfile
from multiplexer.testing.raw_peer import RawPeer
from multiplexer.threaded_client import ThreadedClient

RULES = runfile("tests/testing.rules")  # the file the constants were generated from

EVENT = types.PYTHON_TEST_REQUEST
CHUNK = b"x" * (16 * 1024)


def socket_buffers() -> int:
    """What the two ends of a connection may buffer at most, the largest
    the kernel allows each."""
    buffers = 0
    for path in ("/proc/sys/net/ipv4/tcp_wmem", "/proc/sys/net/ipv4/tcp_rmem"):
        try:
            with open(path) as limits:
                buffers += int(limits.read().split()[2])
        except (OSError, IndexError, ValueError):
            buffers += 8 << 20  # a guess where /proc does not say
    return buffers


def frames_to_fill(size: int) -> int:
    """How many messages of `size` bytes a frozen multiplexer's connection
    cannot take: twice what the two sockets may buffer and twice the queue."""
    return 2 * socket_buffers() // size + 2 * 1024


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


def make_client(name: str, cluster: Cluster, drops: Drops) -> "Client | ThreadedClient":
    """The client `name` on `cluster`, reporting to `drops`."""
    if name == "SyncClient":
        return Client(cluster.endpoints, type=peers.WEBSITE, on_drop=drops)
    return ThreadedClient(cluster.endpoints, type=peers.TEST_ACTIVE_CLIENT, on_drop=drops)


class HeldTest(unittest.TestCase):
    """No multiplexer reachable when the message is sent."""

    def test_a_message_sent_with_none_reachable_is_held_until_one_comes_up(self) -> None:
        """The synchronous client reconnects only inside its calls, so the
        receiver is back on the multiplexer before the message goes: it
        arrives. The threaded client may beat the receiver back, so for it
        what counts is that the message was written and not dropped."""
        for name in ("SyncClient", "ThreadedClient"):
            with self.subTest(client=name):
                drops = Drops()
                with Cluster(1, rules=RULES) as cluster, FakePeer(cluster, peers.PYTHON_TEST_SERVER) as receiver:
                    client = make_client(name, cluster, drops)
                    try:
                        cluster.mx[0].stop()
                        held = client.send_message(b"held", type=EVENT, timeout=30)
                        cluster.mx[0].start()
                        cluster.wait_for_peer(peers.PYTHON_TEST_SERVER)
                        self.assertTrue(client.flush_all(15), "written once a connection came up")
                        self.assertEqual(0, client.dropped)
                        if name == "SyncClient":
                            receiver.wait_for(EVENT, matching=lambda received: received.id == held)
                    finally:
                        client.shutdown()

    def test_a_message_held_past_its_timeout_is_reported(self) -> None:
        for name in ("SyncClient", "ThreadedClient"):
            with self.subTest(client=name):
                drops = Drops()
                with Cluster(1, rules=RULES) as cluster:
                    client = make_client(name, cluster, drops)
                    try:
                        cluster.mx[0].stop()
                        held = client.send_message(b"held", type=EVENT, timeout=0.3)
                        if name == "SyncClient":
                            client.flush_all(0.6)  # its loop runs only inside calls
                        self.assertEqual([(held, DropReason.NO_CONNECTION)], drops.wait_for(held))
                        self.assertEqual(1, client.dropped)
                    finally:
                        client.shutdown()


class FlushingToAllTest(unittest.TestCase):
    """A flushing send to every connection, on the synchronous client as on
    the threaded one."""

    def test_it_waits_for_a_connection_through_a_restart(self) -> None:
        """The only multiplexer is down when the send starts and back half a
        second later: the send waits for it and returns once written, where
        it raised NotConnected at once."""
        with Cluster(1, rules=RULES) as cluster, FakePeer(cluster, peers.PYTHON_TEST_SERVER):
            client = Client(cluster.endpoints, type=peers.WEBSITE)
            try:
                cluster.mx[0].stop()
                restart = threading.Timer(0.5, cluster.mx[0].start)
                restart.start()
                try:
                    client.send_message(b"through", type=EVENT, multiplexer=Client.ALL, flush=True, timeout=15)
                finally:
                    restart.join()
            finally:
                client.shutdown()

    def test_it_returns_once_one_copy_is_written(self) -> None:
        """One of two multiplexers frozen with its socket open: once its
        socket buffer is full, a flushing send through ALL still ends as
        soon as the other copy is written, rather than at its timeout, where
        it waited for every copy and every message sent before it."""
        with Cluster(2, rules=RULES) as cluster, FakePeer(cluster, peers.PYTHON_TEST_SERVER):
            client = Client(cluster.endpoints, type=peers.WEBSITE)
            try:
                cluster.mx[1].pause()
                chunk = b"x" * (256 * 1024)
                started = time.monotonic()
                try:
                    for _ in range(60):  # 15 MB: more than the frozen one's socket takes
                        client.send_message(chunk, type=EVENT, multiplexer=Client.ALL, flush=True, timeout=30)
                    self.assertLess(time.monotonic() - started, 30, "no send waited for its timeout")
                finally:
                    cluster.mx[1].resume()
            finally:
                client.shutdown()


class DeadConnectionTest(unittest.TestCase):
    """The only multiplexer dies with messages the client had not written."""

    def test_its_unwritten_messages_wait_for_the_next_connection(self) -> None:
        """Frozen, filled and killed, then started again: what the client
        still held for it, queued or waiting for room, is held for the next
        connection and written to it, nothing reported dropped, where all of
        it was dropped with the connection. What was in the dead process's
        socket is gone unreported: written, as far as any library can tell."""
        drops = Drops()
        with Cluster(1, rules=RULES) as cluster, FakePeer(cluster, peers.PYTHON_TEST_SERVER) as receiver:
            client = Client(cluster.endpoints, type=peers.WEBSITE, on_drop=drops)
            try:
                cluster.mx[0].pause()
                for _ in range(frames_to_fill(len(CHUNK))):
                    client.send_message(CHUNK, type=EVENT)
                cluster.mx[0].kill()
                cluster.mx[0].start()
                cluster.wait_for_peer(peers.PYTHON_TEST_SERVER)
                self.assertTrue(client.flush_all(60), "written once a connection came up")
                self.assertEqual([], drops.heard)
                receiver.wait_for(EVENT, count=1024, timeout=60)  # at least the queue's worth arrived
            finally:
                client.shutdown()


class FlushDuringFailoverTest(unittest.TestCase):
    """A flush under way while a connection dies."""

    def test_what_a_dead_connection_hands_over_after_a_flush_began_is_not_waited_for(self) -> None:
        """Three multiplexers, frozen in turn. A flush waits for messages
        filling the first; messages sent after it began fill the second,
        which is then killed, and go to the third, frozen too. Once the
        first reads again, the flush ends, everything it waited for
        written, where it waited for those handed over as well and ran out
        of time on the third."""
        chunk = b"x" * max(16 * 1024, 2 * socket_buffers() // 1024)  # a dead queue's 1024 outweigh the buffers
        with Cluster(3, rules=RULES) as cluster:
            client = ThreadedClient(cluster.endpoints, type=peers.TEST_ACTIVE_CLIENT)
            try:
                lanes = {}  # a lane on each multiplexer's connection
                for _ in range(30):
                    if len(lanes) == 3:
                        break
                    lane = client.lane()
                    client.send_message(b"adopt", type=EVENT, multiplexer=lane, flush=True)
                    lanes.setdefault(cluster.multiplexer_at(lane.connection.endpoint), lane)
                self.assertEqual(3, len(lanes), "a lane on every multiplexer")
                waited, dying, taking = lanes
                try:
                    waited.pause()
                    for _ in range(frames_to_fill(len(chunk))):
                        client.send_message(chunk, type=EVENT, multiplexer=lanes[waited], timeout=120)
                    flushed: list[bool] = []
                    ended = threading.Event()

                    def heard(result: bool) -> None:
                        flushed.append(result)
                        ended.set()

                    client._flush_all_and_notify(60, heard)  # posted ahead of what follows, which it does not wait for
                    dying.pause()
                    taking.pause()
                    for _ in range(frames_to_fill(len(chunk))):
                        client.send_message(chunk, type=EVENT, multiplexer=lanes[dying], timeout=120)
                    dying.kill()
                    wait_until(lambda: client.connections_count() == 2, 30, "the dead connection gone")
                    waited.resume()
                    self.assertTrue(ended.wait(90), "the flush ended")
                    self.assertEqual([True], flushed)
                finally:
                    waited.resume()
                    taking.resume()
            finally:
                client.shutdown(timeout=0)


class AllCopiesTest(unittest.TestCase):
    """Messages sent to every connection, one of which dies."""

    def test_a_dead_connections_copies_go_to_no_other(self) -> None:
        """One of two multiplexers frozen, filled with copies and killed:
        the copies the client had not written to it are dropped and
        reported, each, the other multiplexer having had its own. They were
        handed to the other connection, so that a receiver behind it got two
        copies of a message; this one reads frames itself, without the
        library's filter of repeated ids."""
        drops = Drops()
        with Cluster(2, rules=RULES) as cluster:
            receiver = RawPeer(cluster.mx[0].endpoint, peers.PYTHON_TEST_SERVER)
            receiver.handshake()
            received: list[int] = []

            def read() -> None:
                """Every event's id, until the marker arrives or nothing does for 30 s."""
                while True:
                    try:
                        mxmsg = receiver.receive(timeout=30)
                    except OSError:  # a timeout, or the connection closed
                        return
                    if mxmsg.type == EVENT:
                        received.append(mxmsg.id)
                        if mxmsg.message == b"marker":
                            return

            reader = threading.Thread(target=read)
            reader.start()
            client = ThreadedClient(cluster.endpoints, type=peers.TEST_ACTIVE_CLIENT, on_drop=drops)
            try:
                cluster.mx[1].pause()
                sent = {
                    client.send_message(CHUNK, type=EVENT, multiplexer=ThreadedClient.ALL)
                    for _ in range(frames_to_fill(len(CHUNK)))
                }
                cluster.mx[1].kill()
                # False when the dead connection's copies were given up on while it
                # waited, True when before it began: either way everything was out
                client.flush_all(60)
                sent.add(client.send_message(b"marker", type=EVENT, flush=True, timeout=10))
                reader.join(90)
                self.assertFalse(reader.is_alive(), "the marker arrived, everything before it too")
            finally:
                client.shutdown()
                receiver.close()
                reader.join()
            self.assertEqual(len(received), len(set(received)), "a message arrived twice")
            self.assertTrue(set(received) <= sent)
            heard = list(drops.heard)
            self.assertTrue(heard, "the dead connection's copies were reported")
            self.assertEqual({DropReason.CONNECTION_LOST}, {reason for _, reason in heard})


if __name__ == "__main__":
    unittest.main()
