"""What a multiplexer's /metrics counts, read through the harness's
Mx.metric() (docs/metrics.md): every frame a peer sent to be routed, and
its bytes, the multiplexer's own DELIVERY_ERRORs never among them; each
message that went nowhere, under its reason, its type and the peer type
it was meant for, for every reason a raw peer brings about; the peers
connected and the frames queued for them; the rules file that stops
parsing; and the loops and the process. Counted, never timed: a message
the sender addresses to itself after the others comes back once the
multiplexer has routed every one before it, so that a scrape after it
sees them all, and the copies a full queue refused are the DELIVERY_ERRORs
that came back for them."""

import os
import random
import shutil
import tempfile
import threading
import time
import unittest

from multiplexer._native import MAX_KEY_SIZE
from multiplexer.multiplexer_constants import peers, types
from multiplexer.Multiplexer_pb2 import DeliveryError
from multiplexer.testing import Cluster, runfile, wait_until
from multiplexer.testing.buffers import fill_frames
from multiplexer.testing.raw_peer import RawPeer, frame

RULES = runfile("tests/testing.rules")  # the file the constants were generated from
COUNT = 50  # messages each kind of check sends
OUTSIDE_THE_RULES = 987654  # a message type the rules do not have


def routed(sender: RawPeer) -> None:
    """Returns once the multiplexer has routed everything `sender` sent
    before: a message it addresses to itself comes back after them; what
    else came back is read and dropped."""
    marker = sender.send(b"routed", types.TEST_EVENT, to=sender.instance_id)
    while sender.receive(timeout=60).id != marker:
        pass


def dropped(multiplexer) -> dict[tuple[str, str, str], float]:
    """mx_messages_dropped_total, by (reason, type, peer type)."""
    return {
        (labels["reason"], labels["type"], labels["peer_type"]): value
        for name, labels, value in multiplexer.metrics()
        if name == "mx_messages_dropped_total"
    }


class MetricsTest(unittest.TestCase):
    """See the module docstring."""

    def test_every_frame_a_peer_sends_is_counted_with_its_bytes(self) -> None:
        """COUNT frames of known sizes, each addressed back to their sender:
        received counts them and their bytes, headers included, the
        process's figures are there, and a lag for every loop."""
        with Cluster(1, rules=RULES, monitor=True) as cluster:
            multiplexer = cluster.mx[0]
            sender = RawPeer(cluster.endpoints[0], peers.TEST_EVENT_CLIENT)
            sender.handshake()
            sent = 0
            for size in range(COUNT):
                data = frame(sender.message(b"x" * size, types.TEST_EVENT, to=sender.instance_id).SerializeToString())
                sender.send_raw(data)
                sent += len(data)
            for _ in range(COUNT):
                sender.receive(timeout=60)
            self.assertEqual(COUNT, multiplexer.metric("mx_messages_received_total"))
            self.assertEqual(sent, multiplexer.metric("mx_received_bytes_total"))
            self.assertEqual({}, dropped(multiplexer))
            self.assertEqual(1, multiplexer.metric("mx_peers", peer_type="TEST_EVENT_CLIENT"))
            samples = multiplexer.metrics()
            figure = {name: value for name, labels, value in samples if not labels}
            self.assertGreater(figure["process_open_fds"], 0)
            self.assertGreaterEqual(figure["process_max_fds"], figure["process_open_fds"])
            self.assertGreater(figure["process_resident_memory_bytes"], 0)
            self.assertLessEqual(figure["process_start_time_seconds"], time.time())
            self.assertGreater(figure["process_start_time_seconds"], 0)
            loops = int(os.environ.get("MX_SERVER_THREADS") or 1)
            lags = [labels["loop"] for name, labels, _ in samples if name == "mx_event_loop_lag_seconds"]
            self.assertEqual([str(loop) for loop in range(loops)], lags)
            self.assertEqual(0, figure["mx_stopping"])
            sender.close()

    def test_drops_are_counted_by_reason_type_and_peer_type(self) -> None:
        """COUNT messages of each kind that goes nowhere, from a sender a
        raw peer: each counted under its reason, its type and the peer
        type its rule names, a type outside the rules as "unknown", a frame
        claiming the multiplexer's own instance id a protocol error."""
        with Cluster(1, rules=RULES, monitor=True) as cluster:
            multiplexer = cluster.mx[0]
            sender = RawPeer(cluster.endpoints[0], peers.TEST_EVENT_CLIENT)
            _, welcome = sender.handshake()
            choosy = RawPeer(cluster.endpoints[0], peers.TEST_EVENT_BACKEND)
            choosy.handshake(key_filters={types.TEST_EVENT: ["wanted"]})
            cluster.wait_for_peer(peers.TEST_EVENT_BACKEND)
            nobody = random.randint(1, 2**62)
            for _ in range(COUNT):
                sender.send(b"x", types.TEST_TINY_QUEUE_EVENT)  # nobody of TEST_TINY_QUEUE is connected
                sender.send(b"x", types.TEST_EVENT, to=nobody)
                sender.send(b"x", OUTSIDE_THE_RULES)
                sender.send(b"x", types.TEST_UNROUTED)
                sender.send(b"x", types.TEST_EVENT, key="k" * (MAX_KEY_SIZE + 1))
                sender.send(b"x", types.TEST_EVENT, key="unwanted")
                impostor = sender.message(b"x", types.TEST_EVENT)
                impostor.sender = welcome.id  # the multiplexer's own
                sender.send_raw(frame(impostor.SerializeToString()))
            routed(sender)
            self.assertEqual(
                {
                    ("no_receiver", "TEST_TINY_QUEUE_EVENT", "TEST_TINY_QUEUE"): COUNT,
                    ("not_connected", "TEST_EVENT", ""): COUNT,
                    ("unknown_type", "unknown", ""): COUNT,
                    ("no_rule", "TEST_UNROUTED", ""): COUNT,
                    ("key_too_long", "TEST_EVENT", ""): COUNT,
                    ("key_not_taken", "TEST_EVENT", "TEST_EVENT_BACKEND"): COUNT,
                    ("protocol_error", "TEST_EVENT", ""): COUNT,
                },
                dropped(multiplexer),
            )
            choosy.close()
            sender.close()

    def test_a_full_queue_and_a_peer_that_goes_with_frames_queued(self) -> None:
        """A receiver that never reads, its queue of one full: every copy
        addressed to it after is queue_full, as many as the DELIVERY_ERRORs
        that came back for them, every TEST_TINY_QUEUE_EVENT all_full, and
        the queue shows in mx_queued_messages; once the receiver is gone,
        what its queue held is peer_gone, and it is no peer any more."""
        with Cluster(1, rules=RULES, monitor=True) as cluster:
            multiplexer = cluster.mx[0]
            receiver = RawPeer(cluster.endpoints[0], peers.TEST_TINY_QUEUE)
            receiver.handshake()
            sender = RawPeer(cluster.endpoints[0], peers.TEST_EVENT_CLIENT)
            sender.handshake()
            sender.sock.settimeout(60)
            cluster.wait_for_peer(peers.TEST_TINY_QUEUE)
            self.assertEqual(1, multiplexer.metric("mx_peers", peer_type="TEST_TINY_QUEUE"))
            to_receiver: set[int] = set()
            refused: list[int] = []
            marker = random.randint(1, 2**62)

            def count_refusals() -> None:
                """The DELIVERY_ERRORs for copies addressed to the receiver,
                until the marker the sender addresses to itself last."""
                while True:
                    mxmsg = sender.receive(timeout=60)
                    if mxmsg.type == types.DELIVERY_ERROR:
                        error = DeliveryError()
                        error.ParseFromString(mxmsg.message)
                        if error.packet_id in to_receiver:
                            refused.append(error.packet_id)
                    elif mxmsg.type == types.TEST_EVENT and mxmsg.message == str(marker).encode():
                        return

            reader = threading.Thread(target=count_refusals)
            reader.start()
            for payload in fill_frames() + [b"x"] * COUNT:  # its sockets, then its queue, then past it
                to_receiver.add(
                    sender.send(payload, types.TEST_EVENT, to=receiver.instance_id, report_delivery_error=True)
                )
            for _ in range(COUNT):
                sender.send(b"x", types.TEST_TINY_QUEUE_EVENT)
            sender.send(str(marker).encode(), types.TEST_EVENT, to=sender.instance_id)
            reader.join(60)
            self.assertFalse(reader.is_alive(), "the marker came back")
            self.assertGreaterEqual(len(refused), COUNT, "the queue was full for the last ones at least")
            counts = dropped(multiplexer)
            self.assertEqual(len(refused), counts.pop(("queue_full", "TEST_EVENT", "TEST_TINY_QUEUE")))
            self.assertEqual(COUNT, counts.pop(("all_full", "TEST_TINY_QUEUE_EVENT", "TEST_TINY_QUEUE")))
            self.assertEqual({}, counts)
            queued = multiplexer.metric("mx_queued_messages", peer_type="TEST_TINY_QUEUE")
            self.assertGreaterEqual(queued, 1)
            receiver.close()

            def gone() -> float:
                """What the receiver's queue held when it went."""
                return multiplexer.metric("mx_messages_dropped_total", reason="peer_gone", peer_type="TEST_TINY_QUEUE")

            wait_until(
                lambda: multiplexer.metric("mx_peers", peer_type="TEST_TINY_QUEUE") == 0 and gone() >= 1,
                30,
                "the receiver gone and its queue counted",
            )
            self.assertLessEqual(gone(), queued)
            self.assertEqual(0, multiplexer.metric("mx_queued_messages", peer_type="TEST_TINY_QUEUE"))
            sender.close()

    def test_a_rules_file_that_stops_parsing(self) -> None:
        """mx_rules_file_failing says 1 once a reload found the file broken,
        while mx_rules_info keeps the fingerprint of the rules still in use."""
        rules = os.path.join(tempfile.mkdtemp(), "testing.rules")
        shutil.copy(RULES, rules)
        with Cluster(1, rules=rules, rules_check_interval=0, monitor=True) as cluster:
            multiplexer = cluster.mx[0]
            self.assertEqual(0, multiplexer.metric("mx_rules_file_failing"))
            [(fingerprint, _)] = [
                (labels["fingerprint"], value)
                for name, labels, value in multiplexer.metrics()
                if name == "mx_rules_info"
            ]
            with open(rules, "w") as broken:
                broken.write("this does not parse\n")
            multiplexer.reload_rules()
            wait_until(lambda: multiplexer.metric("mx_rules_file_failing") == 1, 30, "the failed reload counted")
            self.assertEqual(1, multiplexer.metric("mx_rules_info", fingerprint=fingerprint))


if __name__ == "__main__":
    unittest.main()
