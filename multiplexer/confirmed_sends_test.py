"""Confirmed sends from Python (set_delivery's `confirm`, docs/delivery.md),
against a real multiplexer: a producer of each class keeps its messages
until their consumer's library confirms them, its send's callback and its
flush waiting for that, and on_drop hearing what failed, in batches or,
not batched, one at a time; a consumer of
each class hands every message of a confirmed batch out with its number
and confirms it: a SyncClient's program with processed(), sent once its
receive waits; a ThreadedClient once on_message returned, FAILED when it
raised, or, made not to, its program from any thread; an AsyncClient once
its handlers, and their tasks, are done; the server classes as their
handlers return, or their Requests go. A raw peer (RawPeer) plays the
other library by hand on the wire. Counted, not timed: every wait is for a
frame or a callback, bounded only to detect a failure.
"""

import asyncio
import queue
import threading
import unittest

from multiplexer.aio import AsyncClient
from multiplexer.clients import SyncClient
from multiplexer.Multiplexer_pb2 import Confirmation, MessageBatch, MultiplexerMessage
from multiplexer.multiplexer_constants import peers, types
from multiplexer.mxclient import Confirmed, DropReason, OperationTimedOut
from multiplexer.servers import BaseMultiplexerServer
from multiplexer.testing import BackendThread, Cluster, runfile
from multiplexer.testing.raw_peer import RawPeer, frame
from multiplexer.threaded_client import ThreadedClient
from multiplexer.threaded_server import BaseThreadedMultiplexerServer, Request

RULES = runfile("tests/testing.rules")
EVENT = types.TEST_EVENT_ANY  # to any one TEST_EVENT_BACKEND
HOUR = 3600.0  # a batch's delay, or a retry_after, that never runs out here
BOUND = 30.0  # what only a failure reaches


def numbers(pairs) -> list[int]:
    """The numbers (first, count) pairs stand for."""
    return [
        number for first, count in zip(pairs[::2], pairs[1::2], strict=True) for number in range(first, first + count)
    ]


class ConfirmedSendsTest(unittest.TestCase):
    """See the module docstring."""

    def setUp(self) -> None:
        self.cluster = Cluster(1, rules=RULES)
        self.cluster.__enter__()
        self.addCleanup(self.cluster.__exit__, None, None, None)

    def raw(self, peer_type: int) -> RawPeer:
        """A raw peer of `peer_type`, welcomed."""
        peer = RawPeer(self.cluster.endpoints[0], peer_type)
        self.addCleanup(peer.close)
        peer.handshake()
        return peer

    def next_batch(self, consumer: RawPeer) -> tuple[list[MultiplexerMessage], list[int]]:
        """The next batch `consumer` reads: its messages and their numbers."""
        envelope = consumer.receive_type(EVENT, BOUND)
        self.assertTrue(envelope.HasField("batch"))
        batch = MessageBatch()
        batch.ParseFromString(envelope.batch)
        messages = []
        for body in batch.message:
            mxmsg = MultiplexerMessage()
            mxmsg.ParseFromString(body)
            messages.append(mxmsg)
        return messages, numbers(list(envelope.confirm))

    @staticmethod
    def confirm(consumer: RawPeer, producer: int, processed=(), refused=(), failed=()) -> None:
        """A CONFIRMATION from `consumer` to `producer`, each list (first, count) pairs."""
        confirmation = Confirmation(processed=processed, refused=refused, failed=failed)
        consumer.send(confirmation.SerializeToString(), types.CONFIRMATION, to=producer)

    @staticmethod
    def send_batch(producer: RawPeer, first: int, payloads: list[bytes]) -> list[int]:
        """A confirmed batch of `payloads` by hand, the first numbered
        `first`; the messages' ids."""
        messages = [producer.message(payload, EVENT) for payload in payloads]
        batch = MessageBatch(message=[mxmsg.SerializeToString() for mxmsg in messages])
        envelope = producer.message(b"", EVENT, batch=batch.SerializeToString(), confirm=[first, len(payloads)])
        envelope.ClearField("message")
        producer.sock.sendall(frame(envelope.SerializeToString()))
        return [mxmsg.id for mxmsg in messages]

    def confirmations(self, producer: RawPeer, count: int) -> tuple[list[int], list[int], list[int]]:
        """What the CONFIRMATIONs `producer` reads say, until they name
        `count` numbers: processed, refused and failed, each sorted."""
        processed: list[int] = []
        refused: list[int] = []
        failed: list[int] = []
        while len(processed) + len(refused) + len(failed) < count:
            message = producer.receive_type(types.CONFIRMATION, BOUND)
            self.assertEqual(producer.instance_id, message.to)
            confirmation = Confirmation()
            confirmation.ParseFromString(message.message)
            processed += numbers(list(confirmation.processed))
            refused += numbers(list(confirmation.refused))
            failed += numbers(list(confirmation.failed))
        return sorted(processed), sorted(refused), sorted(failed)

    # The producer's side.

    def test_a_sync_clients_sends_end_at_their_confirmation(self) -> None:
        """The batch goes at the flush, which waits for the confirmation:
        each callback hears 1 then, a failed one 0, told to on_drop."""
        consumer = self.raw(peers.TEST_EVENT_BACKEND)
        dropped: list[tuple[int, DropReason]] = []
        with SyncClient(
            self.cluster.endpoints,
            type=peers.TEST_EVENT_CLIENT,
            on_drop=lambda message_id, reason: dropped.append((message_id, reason)),
        ) as producer:
            producer.set_delivery(EVENT, batch=True, max_delay=HOUR, confirm=True, retry_after=HOUR, timeout=-1)
            ended: list[int] = []
            ids = [producer.send_message(b"%d" % n, type=EVENT, callback=ended.append) for n in range(4)]

            def far_side() -> None:
                messages, given = self.next_batch(consumer)
                if [mxmsg.id for mxmsg in messages] == ids and given == [1, 2, 3, 4]:
                    self.confirm(consumer, producer.instance_id, processed=[1, 3], failed=[4, 1])

            confirming = threading.Thread(target=far_side)
            confirming.start()
            self.assertFalse(producer.flush_all(timeout=BOUND), "one was given up on")
            confirming.join()
            self.assertEqual([1, 1, 1, 0], ended)
            self.assertEqual([(ids[3], DropReason.FAILED)], dropped)
            self.assertEqual(0, producer.unconfirmed())

    def test_a_threaded_clients_refused_message_goes_again(self) -> None:
        """A refused message goes again at once, alone, with its id and
        number; its callback hears 1 once that copy is confirmed."""
        consumer = self.raw(peers.TEST_EVENT_BACKEND)
        ended: queue.Queue[int] = queue.Queue()
        producer = ThreadedClient(self.cluster.endpoints, peers.TEST_EVENT_CLIENT)
        self.addCleanup(producer.shutdown)
        producer.set_delivery(
            EVENT, batch=True, max_records=3, max_delay=HOUR, confirm=True, retry_after=HOUR, timeout=-1
        )
        ids = [producer.send_message(b"%d" % n, type=EVENT, callback=ended.put) for n in range(3)]
        messages, given = self.next_batch(consumer)
        self.assertEqual(ids, [mxmsg.id for mxmsg in messages])
        self.assertEqual([1, 2, 3], given)
        self.confirm(consumer, producer.instance_id, processed=[1, 1, 3, 1], refused=[2, 1])
        again, given_again = self.next_batch(consumer)
        self.assertEqual([ids[1]], [mxmsg.id for mxmsg in again])
        self.assertEqual([2], given_again)
        self.assertEqual(1, producer.sent_again())
        self.confirm(consumer, producer.instance_id, processed=[2, 1])
        self.assertTrue(producer.flush_all(timeout=BOUND))
        self.assertEqual([1, 1, 1], [ended.get(timeout=BOUND) for _ in range(3)])

    def test_without_batch_each_message_goes_alone_and_is_confirmed(self) -> None:
        """A type confirmed and not batched: each message goes at once in
        an envelope of its own, which numbers it, and its callback hears 1
        once confirmed."""
        consumer = self.raw(peers.TEST_EVENT_BACKEND)
        ended: queue.Queue[int] = queue.Queue()
        producer = ThreadedClient(self.cluster.endpoints, peers.TEST_EVENT_CLIENT)
        self.addCleanup(producer.shutdown)
        producer.set_delivery(EVENT, confirm=True, retry_after=HOUR, timeout=-1)
        ids = [producer.send_message(b"%d" % n, type=EVENT, callback=ended.put) for n in range(3)]
        for number, message_id in enumerate(ids, start=1):
            messages, given = self.next_batch(consumer)
            self.assertEqual([message_id], [mxmsg.id for mxmsg in messages], "alone")
            self.assertEqual([number], given)
            self.confirm(consumer, producer.instance_id, processed=[number, 1])
        self.assertTrue(producer.flush_all(timeout=BOUND))
        self.assertEqual([1, 1, 1], [ended.get(timeout=BOUND) for _ in range(3)])
        self.assertEqual(0, producer.unconfirmed())

    def test_set_delivery_checks_retry_after(self) -> None:
        """retry_after, with `confirm`, is a number of seconds over 0."""
        with SyncClient(self.cluster.endpoints, type=peers.TEST_EVENT_CLIENT) as producer:
            for retry_after in (0.0, -1.0, float("nan"), float("inf")):
                with self.assertRaises(ValueError, msg=repr(retry_after)):
                    producer.set_delivery(EVENT, batch=True, confirm=True, retry_after=retry_after)
            producer.set_delivery(EVENT, batch=True, retry_after=0.0)  # read only with `confirm`

    # The consumer's side.

    def test_a_sync_client_confirms_what_its_program_processed_in_one_message(self) -> None:
        """Each message carries its number; processed() gathers them, sent
        in one CONFIRMATION once a receive waits."""
        producer = self.raw(peers.TEST_EVENT_CLIENT)
        with SyncClient(self.cluster.endpoints, type=peers.TEST_EVENT_BACKEND) as consumer:
            self.cluster.wait_for_peer("TEST_EVENT_BACKEND")
            ids = self.send_batch(producer, 40, [b"%d" % n for n in range(20)])
            for index, message_id in enumerate(ids):
                mxmsg = consumer.receive_message(timeout=BOUND)[0]
                self.assertEqual(message_id, mxmsg.id)
                self.assertEqual(40 + index, mxmsg.confirmation)
                consumer.processed(mxmsg)
            self.assertEqual(20, consumer.confirmations_owed())
            with self.assertRaises(OperationTimedOut):
                consumer.receive_message(timeout=0)  # a receive that waits sends them
            self.assertEqual(0, consumer.confirmations_owed())
            message = producer.receive_type(types.CONFIRMATION, BOUND)
            confirmation = Confirmation()
            confirmation.ParseFromString(message.message)
            self.assertEqual(list(range(40, 60)), numbers(list(confirmation.processed)), "every one, in one message")

    def test_a_threaded_client_confirms_what_on_message_returned_from(self) -> None:
        """Processed once on_message returned, FAILED where it raised."""
        producer = self.raw(peers.TEST_EVENT_CLIENT)

        def on_message(mxmsg: MultiplexerMessage) -> None:
            if mxmsg.message == b"2":
                raise ValueError("as asked")

        consumer = ThreadedClient(self.cluster.endpoints, peers.TEST_EVENT_BACKEND, on_message=on_message)
        self.addCleanup(consumer.shutdown)
        self.send_batch(producer, 1, [b"%d" % n for n in range(5)])
        self.assertEqual(([1, 2, 4, 5], [], [3]), self.confirmations(producer, 5))

    def test_a_threaded_client_that_queues_its_messages_confirms_them_itself(self) -> None:
        """Made not to confirm after on_message, the client confirms what
        its program says, from the program's thread."""
        producer = self.raw(peers.TEST_EVENT_CLIENT)
        inbox: queue.Queue[MultiplexerMessage] = queue.Queue()
        consumer = ThreadedClient(
            self.cluster.endpoints, peers.TEST_EVENT_BACKEND, on_message=inbox.put, confirm_after_on_message=False
        )
        self.addCleanup(consumer.shutdown)
        self.send_batch(producer, 1, [b"%d" % n for n in range(3)])
        taken = [inbox.get(timeout=BOUND) for _ in range(3)]
        self.assertEqual(0, consumer.confirmations_owed(), "nothing confirmed by on_message's return")
        consumer.processed(taken[0])
        consumer.confirm(taken[1], Confirmed.REFUSED)
        consumer.confirm(taken[2], Confirmed.FAILED)
        self.assertEqual(([1], [2], [3]), self.confirmations(producer, 3))

    def test_an_async_clients_handlers_confirm_once_done(self) -> None:
        """A message is confirmed once every handler, and the task of each
        coroutine handler, is done with it, FAILED where one raised."""
        producer = self.raw(peers.TEST_EVENT_CLIENT)

        async def run() -> None:
            consumer = await AsyncClient.create(self.cluster.endpoints, peers.TEST_EVENT_BACKEND)
            try:
                seen: list[bytes] = []

                async def handle(mxmsg: MultiplexerMessage) -> None:
                    await asyncio.sleep(0)
                    seen.append(mxmsg.message)
                    if mxmsg.message == b"1":
                        raise ValueError("as asked")

                consumer.subscribe(EVENT, handle)
                consumer.subscribe(EVENT, lambda mxmsg: None)
                await asyncio.get_running_loop().run_in_executor(
                    None, lambda: self.send_batch(producer, 7, [b"%d" % n for n in range(3)])
                )
                result = await asyncio.get_running_loop().run_in_executor(None, self.confirmations, producer, 3)
                self.assertEqual(([7, 9], [], [8]), result)
                self.assertEqual([b"0", b"1", b"2"], seen)
            finally:
                await consumer.aclose()

        asyncio.run(run())

    def test_the_server_classes_confirm_what_their_handlers_return_from(self) -> None:
        """The plain server as handle_message returns or raises, the
        threaded one as its Request goes, a Request kept by the handler
        confirmed once let go of."""

        class Plain(BaseMultiplexerServer):
            multiplexer_client_type = peers.TEST_EVENT_BACKEND

            def handle_message(self, mxmsg: MultiplexerMessage) -> None:
                self.no_response()
                if mxmsg.message == b"1":
                    raise ValueError("as asked")

        class Threaded(BaseThreadedMultiplexerServer):
            multiplexer_client_type = peers.TEST_EVENT_BACKEND

            def __init__(self, addresses, **kwargs):
                super().__init__(addresses, **kwargs)
                self.kept: list[Request] = []

            def handle_message(self, request: Request) -> None:
                request.no_response()
                if request.mxmsg.message == b"1":
                    raise ValueError("as asked")
                if request.mxmsg.message == b"2":
                    self.kept.append(request)

        producer = self.raw(peers.TEST_EVENT_CLIENT)
        served = BackendThread(lambda: Plain(self.cluster.endpoints)).start()
        try:
            self.send_batch(producer, 1, [b"0", b"1", b"2"])
            self.assertEqual(([1, 3], [], [2]), self.confirmations(producer, 3))
        finally:
            served.stop()
        self.cluster.wait_for_peer_gone("TEST_EVENT_BACKEND", timeout=BOUND)
        threaded = BackendThread(lambda: Threaded(self.cluster.endpoints, workers=2)).start()
        try:
            self.send_batch(producer, 10, [b"0", b"1", b"2", b"3"])
            self.assertEqual(([10, 13], [], [11]), self.confirmations(producer, 3), "but the request kept")
            assert threaded.backend is not None
            threaded.backend.kept.clear()  # let go of: confirmed
            self.assertEqual(([12], [], []), self.confirmations(producer, 1))
        finally:
            threaded.stop()

    # Both sides.

    def test_the_libraries_agree(self) -> None:
        """A ThreadedClient producer and a SyncClient consumer: every send
        ends confirmed, none sent again."""
        ended: queue.Queue[int] = queue.Queue()
        with SyncClient(self.cluster.endpoints, type=peers.TEST_EVENT_BACKEND) as consumer:
            producer = ThreadedClient(self.cluster.endpoints, peers.TEST_EVENT_CLIENT)
            self.addCleanup(producer.shutdown)
            producer.set_delivery(
                EVENT, batch=True, max_records=10, max_delay=HOUR, confirm=True, retry_after=HOUR, timeout=-1
            )
            for number in range(50):
                producer.send_message(b"%d" % number, type=EVENT, callback=ended.put)
            producer.flush_all(timeout=0)  # the last batch goes
            for number in range(50):
                mxmsg = consumer.receive_message(timeout=BOUND)[0]
                self.assertEqual(b"%d" % number, mxmsg.message)
                consumer.processed(mxmsg)
            with self.assertRaises(OperationTimedOut):
                consumer.receive_message(timeout=0)
            self.assertTrue(producer.flush_all(timeout=BOUND))
            self.assertEqual([1] * 50, [ended.get(timeout=BOUND) for _ in range(50)])
            self.assertEqual(0, producer.unconfirmed())
            self.assertEqual(0, producer.sent_again())


if __name__ == "__main__":
    unittest.main()
