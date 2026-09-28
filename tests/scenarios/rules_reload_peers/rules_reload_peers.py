"""a rules reload gives the peers already connected their type's new queue size and passive flag, a controller staying passive.

One multiplexer runs a copy of the rules file with `--rules-check-interval
0`, so that only SIGHUP reloads it. The copy gives TEST_TINY_QUEUE a large
queue: a receiver of that type that never reads takes what is addressed to
it past what its socket holds. The reload puts the type's queue of one
back, and the next message addressed to the receiver draws a
DELIVERY_ERROR naming it, as in direct_queue_full, where the large queue
took it. The reload also makes TEST_EVENT_CLIENT, a passive type, active:
a passive peer is sent one heartbeat per frame it sends, so one that sent
only its welcome gets one; once active, it gets them though it sends
nothing more.

A controller is passive whatever the file says, when it registers and at
every reload. The second test reloads a file that names RULES_CONTROLLER,
peer type 4, without is_passive, while a raw controller is connected. A
raw peer of the active type TEST_ACTIVE_CLIENT is the clock: it is sent a
heartbeat every 3 s of silence, so three of them after the reload put 6 s
behind it, by when the controller, had the reload made it active, would
have been sent one 3 s after the reload. It has been sent nothing, and a
frame it sends draws the one heartbeat it is owed. Counted, not timed:
every wait is for a message, and its bound only detects a failure.
"""

import os
import re
import select
import unittest
import zlib

from tests import harness
from tests.harness import Cluster, constants as C, output_dir, wait_until
from multiplexer.Multiplexer_pb2 import RULES_CONTROLLER, DeliveryError
from multiplexer.testing.buffers import fill_frames
from multiplexer.testing.raw_peer import RawPeer

TINY_QUEUE = re.compile(r'(name: "TEST_TINY_QUEUE"\s*queue_size: )1\n')
EVENT_CLIENT_PASSIVE = re.compile(r'(name: "TEST_EVENT_CLIENT")\s*is_passive: true\n')
PAYLOAD = b"x" * (1024 * 1024)
BOUND = 30  # seconds a wait may take: a failure detector only


def fingerprint(text: str) -> str:
    """What the multiplexer reports for a rules file: the CRC-32 of its bytes, eight hex digits."""
    return "%08x" % zlib.crc32(text.encode())


def waiting(peer: RawPeer) -> bool:
    """Whether anything the multiplexer sent waits in the peer's socket, unread."""
    return bool(select.select([peer.sock], [], [], 0)[0])


class RulesReloadPeers(unittest.TestCase):
    """A reload's queue size and passive flag reach the peers already connected, a controller kept passive."""

    def test_connected_peers_take_the_new_queue_size_and_passive_flag(self):
        with open(harness.CONFIG.rules) as shipped:
            original = shipped.read()
        self.assertTrue(TINY_QUEUE.search(original) and EVENT_CLIENT_PASSIVE.search(original), "the test rules")
        large_queue = TINY_QUEUE.sub(r"\g<1>1048576\n", original)
        reloaded = EVENT_CLIENT_PASSIVE.sub(r"\1\n", original)
        path = os.path.join(output_dir(), "peers.rules")
        with open(path, "w") as rules:
            rules.write(large_queue)
        with Cluster(1, rules=path, rules_check_interval=0) as cluster:
            multiplexer = cluster.mx[0]
            receiver = RawPeer(cluster.endpoints[0], C.peers.TEST_TINY_QUEUE)
            receiver.handshake()
            sender = RawPeer(cluster.endpoints[0], C.peers.TEST_CLIENT)
            sender.handshake()
            quiet = RawPeer(cluster.endpoints[0], C.peers.TEST_EVENT_CLIENT)
            quiet.handshake()
            for payload in fill_frames():  # what the sockets do not hold waits in the receiver's queue
                sender.send(payload, C.types.TEST_EVENT, to=receiver.instance_id, report_delivery_error=True)
            quiet.receive_type(C.types.HEARTBIT, timeout=BOUND)  # the one its welcome is owed

            with open(path, "w") as rules:
                rules.write(reloaded)
            multiplexer.reload_rules()
            wait_until(
                lambda: multiplexer.log_contains("-> %s," % fingerprint(reloaded)), BOUND, "the reload in the log"
            )
            sender.send(PAYLOAD, C.types.TEST_EVENT, to=receiver.instance_id, report_delivery_error=True)
            report = DeliveryError()
            report.ParseFromString(sender.receive_type(C.types.DELIVERY_ERROR, timeout=BOUND).message)
            self.assertEqual(receiver.instance_id, report.failed_to, "the receiver's queue of one is full")
            quiet.receive_type(C.types.HEARTBIT, timeout=BOUND)  # an active peer's, though it sent nothing more
            for peer in (receiver, sender, quiet):
                peer.close()

    def test_a_controller_stays_passive_when_the_file_names_its_type(self):
        with open(harness.CONFIG.rules) as shipped:
            original = shipped.read()
        naming = original + 'peer {\n    type: %d\n    name: "RULES_CONTROLLER"\n}\n' % RULES_CONTROLLER
        path = os.path.join(output_dir(), "controller.rules")
        with open(path, "w") as rules:
            rules.write(original)
        with Cluster(1, rules=path, rules_check_interval=0) as cluster:
            multiplexer = cluster.mx[0]
            controller = RawPeer(cluster.endpoints[0], RULES_CONTROLLER)
            controller.handshake()
            controller.receive_type(C.types.HEARTBIT, timeout=BOUND)  # the one its welcome is owed
            clock = RawPeer(cluster.endpoints[0], C.peers.TEST_ACTIVE_CLIENT)
            clock.handshake()

            with open(path, "w") as rules:
                rules.write(naming)
            multiplexer.reload_rules()
            wait_until(lambda: multiplexer.log_contains("-> %s," % fingerprint(naming)), BOUND, "the reload in the log")
            while waiting(clock):  # what the clock was sent until now: the three below follow the reload
                clock.receive(timeout=BOUND)
            for _ in range(3):  # 3 s of silence each: 6 s past the reload at least
                clock.receive_type(C.types.HEARTBIT, timeout=BOUND)
            self.assertFalse(waiting(controller), "the controller was sent a heartbeat it did not ask for: made active")
            controller.send(b"", C.types.HEARTBIT)
            controller.receive_type(C.types.HEARTBIT, timeout=BOUND)  # the one this frame is owed
            for peer in (controller, clock):
                peer.close()


if __name__ == "__main__":
    harness.main()
