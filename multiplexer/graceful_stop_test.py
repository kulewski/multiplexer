"""How a multiplexer stops on SIGTERM (Server::stop, --drain-seconds): what
is queued for a peer that reads is written before its connection closes; a
peer that does not read ends the stop at the deadline, its queue dropped and
counted; a connection that never sent its welcome does not hold the stop up;
a second signal stops at once. And a connection whose first frame is a
heartbeat rather than its welcome, which used to stay forever, and the
accept loop that spun when descriptors ran out. Counts, never speed: a receiver that does not read
until the stop has begun holds whatever the machine's speed, and a message
the sender addresses to itself last says the multiplexer has routed every
one before it."""

import os
import random
import re
import resource
import signal
import socket
import threading
import time
import unittest

from multiplexer.multiplexer_constants import peers, types
from multiplexer.testing import Cluster, runfile, wait_until
from multiplexer.testing.raw_peer import RawPeer, frame
from multiplexer.Multiplexer_pb2 import MultiplexerMessage

RULES = runfile("tests/testing.rules")  # the file the constants were generated from
PAYLOAD = b"x" * (64 * 1024)
QUEUED = 200  # messages beyond what the sockets hold, in the multiplexer's queue at the stop


def beyond_the_sockets() -> int:
    """How many PAYLOADs fill both sockets of a receiver that does not read,
    the largest the kernel allows each, twice, and QUEUED more for the
    multiplexer's queue: PYTHON_TEST_SERVER's holds a million."""
    buffers = 0
    for path in ("/proc/sys/net/ipv4/tcp_wmem", "/proc/sys/net/ipv4/tcp_rmem"):
        try:
            with open(path) as limits:
                buffers += int(limits.read().split()[2])
        except (OSError, IndexError, ValueError):
            buffers += 8 << 20  # a guess where /proc does not say
    return 2 * buffers // len(PAYLOAD) + QUEUED


def read(path: str) -> str:
    """The file's text; empty while it does not exist."""
    try:
        with open(path, "rb") as log:
            return log.read().decode("utf-8", "replace")
    except OSError:
        return ""


def queue_for_a_receiver(cluster: Cluster) -> tuple[RawPeer, RawPeer, int]:
    """A receiver that does not read and a sender that sent it more than its
    sockets hold, the rest queued in the multiplexer: (receiver, sender,
    how many were sent). Returns once the multiplexer routed them all,
    which the sender knows from a message it addressed to itself last."""
    receiver = RawPeer(cluster.endpoints[0], peers.PYTHON_TEST_SERVER)
    receiver.handshake()
    sender = RawPeer(cluster.endpoints[0], peers.PYTHON_TEST_CLIENT)
    sender.handshake()
    sent = beyond_the_sockets()
    for _ in range(sent):
        sender.send(PAYLOAD, types.PYTHON_TEST_REQUEST)
    marker = sender.send(b"routed", types.PYTHON_TEST_RESPONSE, to=sender.instance_id)
    while sender.receive(timeout=60).id != marker:
        pass
    return receiver, sender, sent


def count_until_the_end(peer: RawPeer, type_: int) -> int:
    """Reads until the multiplexer's end of the stream and returns how many
    messages of `type_` came; a reset instead of the end raises."""
    received = 0
    while True:
        try:
            mxmsg = peer.receive(timeout=60)
        except ConnectionError as error:
            if isinstance(error, ConnectionResetError):
                raise
            return received  # the end of the stream, as the multiplexer closed it
        if mxmsg.type == type_:
            received += 1


class GracefulStopTest(unittest.TestCase):
    def test_what_is_queued_for_a_reader_is_written_before_it_closes(self) -> None:
        """The receiver starts reading only once the stop began: it gets
        every message, then the end of the stream, and the multiplexer says
        it dropped nothing. The code before closed the connection with the
        queue in it."""
        with Cluster(1, rules=RULES) as cluster:
            multiplexer = cluster.mx[0]
            receiver, sender, sent = queue_for_a_receiver(cluster)
            codes = []
            stopping = threading.Thread(target=lambda: codes.append(multiplexer.stop(timeout=60)))
            stopping.start()
            wait_until(lambda: multiplexer.log_contains("stopping:"), 20, "the stop began")
            self.assertEqual(sent, count_until_the_end(receiver, types.PYTHON_TEST_REQUEST))
            receiver.close()
            self.assertEqual(0, count_until_the_end(sender, types.PYTHON_TEST_REQUEST))
            sender.close()
            stopping.join(60)
            self.assertEqual([0], codes)
            log = read(multiplexer.log_path)
            self.assertIn("stopped: every connection closed in", log)
            self.assertNotIn("dropped", log[log.index("stopping:") :])

    def test_a_receiver_that_does_not_read_ends_the_stop_at_the_deadline(self) -> None:
        """With --drain-seconds 1, a receiver that never reads: the stop
        ends at the deadline with the queue dropped, and says how much."""
        with Cluster(1, rules=RULES, drain_seconds=1) as cluster:
            multiplexer = cluster.mx[0]
            receiver, sender, _ = queue_for_a_receiver(cluster)
            self.assertEqual(0, multiplexer.stop(timeout=30))
            log = read(multiplexer.log_path)
            self.assertIn("1 connection(s) still held messages at the deadline", log)
            dropped = re.search(r"dropped (\d+) message\(s\) still queued for peers", log)
            assert dropped is not None, log[-2000:]
            self.assertGreaterEqual(int(dropped.group(1)), QUEUED)
            receiver.close()
            sender.close()

    def test_a_connection_without_its_welcome_does_not_hold_the_stop_up(self) -> None:
        """A socket that connected and said nothing: the stop closes it with
        the rest. The code before waited for it forever."""
        with Cluster(1, rules=RULES) as cluster:
            silent = socket.create_connection(cluster.endpoints[0], timeout=10)
            # Accepted in order: once a later connection is answered, the
            # silent one is the multiplexer's, not the kernel's backlog's.
            later = RawPeer(cluster.endpoints[0], peers.PYTHON_TEST_CLIENT)
            later.handshake()
            self.assertEqual(0, cluster.mx[0].stop(timeout=30))
            self.assertIn("stopped: every connection closed in", read(cluster.mx[0].log_path))
            later.close()
            silent.close()

    def test_a_second_signal_stops_at_once(self) -> None:
        """A drain of a minute, held up by a receiver that does not read: the
        second SIGTERM ends it, long before the minute is up."""
        with Cluster(1, rules=RULES, drain_seconds=60) as cluster:
            multiplexer = cluster.mx[0]
            receiver, sender, _ = queue_for_a_receiver(cluster)
            assert multiplexer.proc is not None
            multiplexer.proc.send_signal(signal.SIGTERM)
            wait_until(lambda: multiplexer.log_contains("stopping:"), 20, "the stop began")
            multiplexer.proc.send_signal(signal.SIGTERM)
            self.assertEqual(0, multiplexer.proc.wait(30), "well within the drain's 60 s")
            self.assertTrue(multiplexer.log_contains("again, stopping at once"))
            receiver.close()
            sender.close()

    def test_a_heartbeat_before_the_welcome_ends_the_connection(self) -> None:
        """A connection's first frame must be its welcome; a heartbeat, which
        used to keep a connection that never introduced itself alive for
        good, ends it."""
        with Cluster(1, rules=RULES) as cluster:
            peer = RawPeer(cluster.endpoints[0], peers.PYTHON_TEST_CLIENT)
            heartbeat = MultiplexerMessage(id=0, type=types.HEARTBIT)
            peer.send_raw(frame(heartbeat.SerializeToString()))
            self.assertTrue(peer.closed_by_peer(timeout=10))
            peer.close()


class AcceptTest(unittest.TestCase):
    def test_out_of_descriptors_the_accept_loop_waits(self) -> None:
        """The multiplexer limited to a few more descriptors than it has,
        and more connections than that: it says it cannot accept, and waits
        between tries instead of spinning, as it did, on its only thread;
        given descriptors again, it accepts the ones that waited."""
        with Cluster(1, rules=RULES) as cluster:
            multiplexer = cluster.mx[0]
            assert multiplexer.proc is not None
            pid = multiplexer.proc.pid
            limits = resource.prlimit(pid, resource.RLIMIT_NOFILE)
            in_use = len(os.listdir("/proc/%d/fd" % pid))
            resource.prlimit(pid, resource.RLIMIT_NOFILE, (in_use + 3, limits[1]))
            waiting = [socket.create_connection(cluster.endpoints[0], timeout=10) for _ in range(10)]
            try:
                wait_until(
                    lambda: multiplexer.log_contains("cannot accept a connection"), 20, "the line about descriptors"
                )
                before = multiplexer.cpu_seconds()
                time.sleep(2)
                self.assertLess(multiplexer.cpu_seconds() - before, 0.5, "of 2 s; spinning takes all of it")
                resource.prlimit(pid, resource.RLIMIT_NOFILE, limits)
                peer = RawPeer(cluster.endpoints[0], peers.PYTHON_TEST_CLIENT, instance_id=random.randint(1, 2**62))
                peer.handshake()  # accepted once the waiting ones were
                peer.close()
            finally:
                for connection in waiting:
                    connection.close()


if __name__ == "__main__":
    unittest.main()
