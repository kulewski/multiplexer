"""Batching from Python, against a real multiplexer: the events SyncClient,
ThreadedClient and AsyncClient send without flush, of a type or key given
to set_batching(), go several in one message, and a receiver of each class
gets them back one at a time, in order, with the ids and payloads they were
sent with. A raw peer (RawPeer) reads what went on the wire: a batch is one
message of the events' type and key, their serialized forms in
MultiplexerMessage.batch. A send that does not batch lets the batch go
first; each message's callback hears how it ended, and a batch given up on
is told for each message in it (on_drop); the settings are checked where
they are given. A multiplexer recording what it routes records a batch as
one message, marked a batch, with its key, and both readers say so.
Counted, not timed: every batch here goes at a flush or at a send that does
not batch, its delay an hour.
"""

import asyncio
import gc
import math
import queue
import subprocess
import unittest

from multiplexer._native import MAX_KEY_SIZE, MAX_MESSAGE_SIZE, MAX_SECONDS
from multiplexer.aio import AsyncClient
from multiplexer.clients import SyncClient
from multiplexer.Multiplexer_pb2 import DeliveryError, MessageBatch, MultiplexerMessage
from multiplexer.multiplexer_constants import peers, types
from multiplexer import recording
from multiplexer.mxclient import DropReason
from multiplexer.testing import Cluster, mxcontrol_path, runfile
from multiplexer.testing.raw_peer import RawPeer
from multiplexer.threaded_client import ThreadedClient

RULES = runfile("tests/testing.rules")
EVENT = types.TEST_EVENT  # whom: ALL to TEST_EVENT_BACKEND
HOUR = 3600.0  # a batch's delay that never runs out here
BOUND = 30.0  # what only a failure reaches


def unpacked(envelope: MultiplexerMessage) -> list[MultiplexerMessage]:
    """The messages of a batch as it went on the wire."""
    batch = MessageBatch()
    batch.ParseFromString(envelope.batch)
    messages = []
    for body in batch.message:
        mxmsg = MultiplexerMessage()
        mxmsg.ParseFromString(body)
        messages.append(mxmsg)
    return messages


class BatchingTest(unittest.TestCase):
    """See the module docstring."""

    def setUp(self) -> None:
        self.cluster = Cluster(1, rules=RULES)
        self.cluster.__enter__()
        self.addCleanup(self.cluster.__exit__, None, None, None)
        self.wire = RawPeer(self.cluster.endpoints[0], peers.TEST_EVENT_BACKEND)
        self.addCleanup(self.wire.close)
        self.wire.handshake()

    def assert_batch(self, ids: list[int], payloads: list[bytes], key: str | None = None) -> None:
        """The next event on the wire is a batch of these messages, in this
        order, with the batch's type and `key`, or none."""
        envelope = self.wire.receive_type(EVENT)
        self.assertTrue(envelope.HasField("batch"), "a message on its own, not a batch")
        self.assertEqual(key is not None, envelope.HasField("key"))
        self.assertEqual(key or "", envelope.key)
        messages = unpacked(envelope)
        self.assertEqual(ids, [mxmsg.id for mxmsg in messages])
        self.assertEqual(payloads, [mxmsg.message for mxmsg in messages])
        self.assertEqual([envelope.sender] * len(ids), [mxmsg.sender for mxmsg in messages])
        self.assertEqual([key or ""] * len(ids), [mxmsg.key for mxmsg in messages])

    def assert_alone(self, message_id: int) -> None:
        """The next event on the wire is this message, on its own."""
        mxmsg = self.wire.receive_type(EVENT)
        self.assertFalse(mxmsg.HasField("batch"), "a batch, not the message on its own")
        self.assertEqual(message_id, mxmsg.id)

    def test_a_sync_clients_batch_goes_at_a_flush(self) -> None:
        """Five events sent without flush wait in one batch, which the
        flush lets go; each message's callback hears it written."""
        payloads = [b"%d" % number for number in range(5)]
        written: list[int] = []
        with SyncClient(self.cluster.endpoints, type=peers.TEST_EVENT_CLIENT) as client:
            client.set_batching(EVENT, max_delay=HOUR)
            ids = [client.send_message(payload, type=EVENT, callback=written.append) for payload in payloads]
            self.assertEqual([], written, "nothing went before the flush")
            self.assertTrue(client.flush_all(timeout=BOUND))
            self.assertEqual([1] * len(payloads), written)
        self.assert_batch(ids, payloads)

    def test_a_threaded_clients_batch_goes_at_a_flush(self) -> None:
        """As a SyncClient's: the io thread batches what send_message()
        hands it, and flush_all() lets the batch go."""
        payloads = [b"%d" % number for number in range(5)]
        with ThreadedClient(self.cluster.endpoints, peers.TEST_EVENT_CLIENT) as client:
            client.set_batching(EVENT, max_delay=HOUR)
            ids = [client.send_message(payload, type=EVENT) for payload in payloads]
            self.assertTrue(client.flush_all(BOUND))
        self.assert_batch(ids, payloads)

    def test_an_async_clients_batch_goes_at_a_flush(self) -> None:
        """As a ThreadedClient's, through the AsyncClient's calls, whose
        set_batching() checks its settings as the others do."""
        payloads = [b"%d" % number for number in range(5)]

        async def send() -> list[int]:
            async with await AsyncClient.create(self.cluster.endpoints, peers.TEST_EVENT_CLIENT) as client:
                with self.assertRaises(ValueError):
                    client.set_batching(EVENT, max_records=0)
                client.set_batching(EVENT, max_delay=HOUR)
                ids = [await client.send_message(payload, type=EVENT) for payload in payloads]
                self.assertTrue(await client.flush_all(BOUND))
                return ids

        self.assert_batch(asyncio.run(send()), payloads)

    def test_a_keys_batch_goes_before_a_send_that_does_not_batch(self) -> None:
        """Key "a" is batched and key "b" is not: the batch of "a" goes
        before the "b" sent after it, and a flushing send of "a" goes on its
        own, after."""
        with SyncClient(self.cluster.endpoints, type=peers.TEST_EVENT_CLIENT) as client:
            client.set_batching(EVENT, "a", max_delay=HOUR)
            batched = [client.send_message(b"a%d" % number, type=EVENT, key="a") for number in range(3)]
            other = client.send_message(b"b", type=EVENT, key="b")
            flushed = client.send_message(b"a-flushed", type=EVENT, key="a", flush=True)
            self.assertTrue(client.flush_all(timeout=BOUND))
        self.assert_batch(batched, [b"a0", b"a1", b"a2"], key="a")
        self.assert_alone(other)
        self.assert_alone(flushed)

    def test_every_class_receives_a_batch_one_message_at_a_time(self) -> None:
        """A SyncClient, a ThreadedClient and an AsyncClient of the type the
        events go to each get the three messages of the batch the raw peer
        saw, one at a time, in order, with their ids, payloads and sender."""
        payloads = [b"first", b"second", b"third"]
        threaded_arrived: queue.Queue[MultiplexerMessage] = queue.Queue()
        endpoints = self.cluster.endpoints

        async def receive() -> None:
            with (
                SyncClient(endpoints, type=peers.TEST_EVENT_BACKEND) as sync_receiver,
                ThreadedClient(endpoints, peers.TEST_EVENT_BACKEND, on_message=threaded_arrived.put),
            ):
                async with await AsyncClient.create(endpoints, peers.TEST_EVENT_BACKEND) as async_receiver:
                    stream = async_receiver.messages()
                    self.cluster.wait_for_peer(peers.TEST_EVENT_BACKEND, count=4)  # the raw peer and these
                    with SyncClient(endpoints, type=peers.TEST_EVENT_CLIENT) as sender:
                        sender.set_batching(EVENT, max_delay=HOUR)
                        ids = [sender.send_message(payload, type=EVENT) for payload in payloads]
                        self.assertTrue(sender.flush_all(timeout=BOUND))
                        sender_id = sender.instance_id
                    self.assert_batch(ids, payloads)
                    received = {
                        "SyncClient": [sync_receiver.read_message(timeout=BOUND) for _ in payloads],
                        "ThreadedClient": [threaded_arrived.get(timeout=BOUND) for _ in payloads],
                        "AsyncClient": [await asyncio.wait_for(stream.__anext__(), BOUND) for _ in payloads],
                    }
            for name, messages in received.items():
                with self.subTest(name):
                    self.assertEqual(ids, [mxmsg.id for mxmsg in messages])
                    self.assertEqual(payloads, [mxmsg.message for mxmsg in messages])
                    self.assertEqual([sender_id] * len(ids), [mxmsg.sender for mxmsg in messages])
                    self.assertEqual([False] * len(ids), [mxmsg.HasField("batch") for mxmsg in messages])

        asyncio.run(receive())

    def test_a_batch_given_up_on_is_told_for_each_message(self) -> None:
        """With no multiplexer, the batch a flush lets go is held for a
        connection, one message, and the flush says not all was written;
        the shutdown gives it up, and on_drop hears of each message in it,
        in order, and each one's callback hears 0."""
        told: list[tuple[int, DropReason]] = []
        written: list[int] = []
        client = SyncClient(
            [], type=peers.TEST_EVENT_CLIENT, on_drop=lambda message_id, reason: told.append((message_id, reason))
        )
        client.set_batching(EVENT, max_delay=HOUR)
        ids = [client.send_message(b"%d" % number, type=EVENT, callback=written.append) for number in range(3)]
        self.assertFalse(client.flush_all(timeout=0))
        self.assertEqual(1, client.held_for_a_connection(), "one batch, held")
        self.assertEqual([], told)
        client.shutdown()
        self.assertEqual([(message_id, DropReason.SHUT_DOWN) for message_id in ids], told)
        self.assertEqual([0, 0, 0], written)
        self.assertEqual(3, client.dropped)

    def test_a_send_with_no_time_to_wait_goes_alone(self) -> None:
        """timeout=0 waits for nothing: such a send goes alone, the batch
        before it first, where in the batch it left the batch no time."""
        with SyncClient(self.cluster.endpoints, type=peers.TEST_EVENT_CLIENT) as client:
            client.set_batching(EVENT, max_delay=HOUR)
            first = client.send_message(b"first", type=EVENT)
            now = client.send_message(b"now", type=EVENT, timeout=0)
            last = client.send_message(b"last", type=EVENT)
            self.assertTrue(client.flush_all(timeout=BOUND))
        self.assert_alone(first)
        self.assert_alone(now)
        self.assert_alone(last)

    def test_no_callback_runs_while_a_collected_client_goes(self) -> None:
        """A client collected with a batch open lets its batch go, as its
        destructor's shutdown writes what was sent, and lets go of the
        callbacks first, as it does of every send's: none runs while the
        client is being destroyed."""
        heard: list[int] = []
        client = SyncClient(self.cluster.endpoints, type=peers.TEST_EVENT_CLIENT)
        client.set_batching(EVENT, max_delay=HOUR)
        sent = client.send_message(b"collected", type=EVENT, callback=heard.append)
        del client
        gc.collect()
        self.assertEqual([], heard)
        self.assert_alone(sent)

    def test_a_delivery_error_for_a_batch_is_told_for_each_message(self) -> None:
        """Nobody takes the type: the multiplexer sends the batch back
        whole in its DELIVERY_ERROR, and the program gets one DELIVERY_ERROR
        for each message, naming it, with its workflow and the message as
        the original, as without batching, in SyncClient and in
        ThreadedClient."""
        arrived: queue.Queue[MultiplexerMessage] = queue.Queue()
        with Cluster(1, rules=RULES) as cluster:  # no event backend: nobody takes TEST_EVENT
            with SyncClient(cluster.endpoints, type=peers.TEST_EVENT_CLIENT) as client:
                client.set_batching(EVENT, max_records=3, max_delay=HOUR)
                sync_ids = [
                    client.send_message(b"%d" % number, type=EVENT, workflow="flow %d" % number) for number in range(3)
                ]
                sync_errors = [client.read_message(timeout=BOUND) for _ in sync_ids]
            with ThreadedClient(cluster.endpoints, peers.TEST_EVENT_CLIENT, on_message=arrived.put) as threaded:
                threaded.set_batching(EVENT, max_records=3, max_delay=HOUR)
                threaded_ids = [
                    threaded.send_message(b"%d" % number, type=EVENT, workflow="flow %d" % number)
                    for number in range(3)
                ]
                threaded_errors = [arrived.get(timeout=BOUND) for _ in threaded_ids]
        for name, ids, errors in (
            ("SyncClient", sync_ids, sync_errors),
            ("ThreadedClient", threaded_ids, threaded_errors),
        ):
            with self.subTest(name):
                self.assertEqual([types.DELIVERY_ERROR] * 3, [error.type for error in errors])
                self.assertEqual(ids, [error.references for error in errors])
                self.assertEqual(["flow 0", "flow 1", "flow 2"], [error.workflow for error in errors])
                reports = [DeliveryError.FromString(error.message) for error in errors]
                self.assertEqual(ids, [report.packet_id for report in reports])
                self.assertEqual([[peers.TEST_EVENT_BACKEND]] * 3, [list(report.failed_type) for report in reports])
                self.assertEqual(ids, [report.original_message.id for report in reports])
                self.assertEqual([b"0", b"1", b"2"], [report.original_message.message for report in reports])

    def test_a_batch_is_recorded_as_one_message_marked_a_batch(self) -> None:
        """The multiplexer's record of a batch is one routed message of its
        type and key, `batch` set, its payload the batch's bytes, the
        messages inside whole; recording.describe() and mxcontrol
        dump_recording print the key and the mark."""
        with Cluster(1, rules=RULES, record=True, record_payload_bytes=1 << 16) as cluster:
            wire = RawPeer(cluster.endpoints[0], peers.TEST_EVENT_BACKEND)
            self.addCleanup(wire.close)
            wire.handshake()
            with SyncClient(cluster.endpoints, type=peers.TEST_EVENT_CLIENT) as client:
                client.set_batching(EVENT, max_delay=HOUR)
                ids = [client.send_message(b"%d" % number, type=EVENT, key="k") for number in range(3)]
                self.assertTrue(client.flush_all(timeout=BOUND))
            envelope = wire.receive_type(EVENT)
            wire.close()
            cluster.mx[0].stop()
            path = cluster.mx[0].record_file
            records = [record for record in recording.read(path) if record.HasField("routed")]
        batch = [record for record in records if record.routed.id == envelope.id]
        self.assertEqual(1, len(batch), "one record for the batch")
        routed = batch[0].routed
        self.assertEqual((True, EVENT, "k", False), (routed.batch, routed.type, routed.key, routed.truncated))
        self.assertEqual(ids, [mxmsg.id for mxmsg in unpacked(MultiplexerMessage(batch=routed.payload))])
        self.assertEqual([], [record.routed.id for record in records if record.routed.id in ids], "none alone")
        line = recording.describe(batch[0])
        self.assertIn(" key='k' batch ", line)
        dumped = subprocess.run([mxcontrol_path(), "dump_recording", path], capture_output=True, text=True, timeout=30)
        self.assertEqual(0, dumped.returncode, dumped.stderr)
        self.assertEqual(
            1,
            len(
                [row for row in dumped.stdout.splitlines() if "id=%d " % envelope.id in row and " key=k batch " in row]
            ),
        )

    def test_the_settings_are_checked(self) -> None:
        """A protocol type, a key over MAX_KEY_SIZE, no record or byte in a
        batch, more than MAX_MESSAGE_SIZE, and a delay negative, over
        MAX_SECONDS or not finite are a ValueError, in each class, where
        they are given."""
        refused: list[tuple[int, str | None, dict[str, float]]] = [
            (types.DELIVERY_ERROR, None, {}),
            (EVENT, "k" * (MAX_KEY_SIZE + 1), {}),
            (EVENT, None, {"max_records": 0}),
            (EVENT, None, {"max_records": -1}),
            (EVENT, None, {"max_bytes": 0}),
            (EVENT, None, {"max_bytes": -1}),
            (EVENT, None, {"max_bytes": MAX_MESSAGE_SIZE + 1}),
            (EVENT, None, {"max_delay": -1.0}),
            (EVENT, None, {"max_delay": MAX_SECONDS * 2}),
            (EVENT, None, {"max_delay": math.inf}),
            (EVENT, None, {"max_delay": math.nan}),
        ]
        with (
            SyncClient([], type=peers.TEST_EVENT_CLIENT) as sync_client,
            ThreadedClient([], peers.TEST_EVENT_CLIENT) as threaded_client,
        ):
            for client in (sync_client, threaded_client):
                for type_, key, settings in refused:
                    with self.subTest(client=type(client).__name__, type=type_, key=key, **settings):
                        with self.assertRaises(ValueError):
                            client.set_batching(type_, key, **settings)  # type: ignore[arg-type]
                client.set_batching(EVENT, "k" * MAX_KEY_SIZE, max_bytes=MAX_MESSAGE_SIZE)
                client.clear_batching(EVENT)


if __name__ == "__main__":
    unittest.main()
