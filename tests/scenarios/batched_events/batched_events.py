"""events one library sends in batches reach a backend of any library one at a time, in order, as they were sent."""

import unittest

from multiplexer.Multiplexer_pb2 import MessageBatch, MultiplexerMessage
from multiplexer.testing.raw_peer import RawPeer
from tests import harness
from tests.harness import Cluster, constants as C, spawn

EVENT = C.types.TEST_EVENT  # whom: ALL to TEST_EVENT_BACKEND: the raw peer and the event backend
KEY = "sensor.7"
PER_BATCH = 4
PAYLOADS = ["event%d" % number for number in range(10)]
# What goes on the wire: two full batches and the one the flush lets go.
BATCHES = [PAYLOADS[start : start + PER_BATCH] for start in range(0, len(PAYLOADS), PER_BATCH)]
HOUR = 3600.0  # the batches' delay, which never runs out here: they go full or at the flush
MARKER = b"marker"


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


class BatchedEvents(unittest.TestCase):
    """An event client, its batching set for TEST_EVENT, four messages a batch
    with a delay of an hour, which never runs out here, sends ten events keyed
    sensor.7 without waiting for them, and then flushes: two batches go full
    and the flush lets the third, of two, go. A raw peer of the event
    backends' type reads what went on the wire: three messages of the events'
    type and key, each from the client with an id of its own, their `batch`
    holding four, four and two of the events, as they were sent. An event
    backend, of the client's library or another, gets the ten events one at a
    time, in order, each with the id, the payload, the sender and the key it
    was sent with, and then a marker that a raw peer sends once the backend
    has the last event: an event handed out twice, or a batch handed out
    whole, would have come before the marker, and so would anything sent
    alone, on the wire.
    """

    def test_batched_events_arrive_one_at_a_time(self):
        cfg = harness.CONFIG
        with Cluster(1) as cluster:
            wire = RawPeer(cluster.endpoints[0], C.peers.TEST_EVENT_BACKEND)
            self.addCleanup(wire.close)
            wire.handshake()
            event_backend = spawn(
                "event_backend",
                cfg.lang("event_backend"),
                mx=cluster.addresses,
                type=C.peers.TEST_EVENT_BACKEND,
                until=len(PAYLOADS) + 1,  # the events and the marker
            )
            event_backend.wait_for("connected", connections=1)

            event_client = spawn(
                "event_client",
                cfg.lang("event_client"),
                mx=cluster.addresses,
                type=C.peers.TEST_EVENT_CLIENT,
                send=[(EVENT, payload) for payload in PAYLOADS],
                key=KEY,
                batch_records=PER_BATCH,
                batch_delay=HOUR,
            )
            self.assertEqual(0, event_client.wait())
            self.assertEqual([], event_client.events_of("error"))
            self.assertEqual([True], [event["is_sent"] for event in event_client.events_of("flushed")])
            sender = event_client.events_of("connected")[0]["instance_id"]
            ids = [event["id"] for event in event_client.events_of("sent")]
            self.assertEqual(len(PAYLOADS), len(set(ids)))
            sent = list(zip(ids, PAYLOADS, strict=True))

            # The marker goes once the backend has the last event, which came
            # in the last batch: what the batches brought it came before.
            event_backend.wait_for("received", id=ids[-1])
            marker = RawPeer(cluster.endpoints[0], C.peers.TEST_EVENT_CLIENT)
            self.addCleanup(marker.close)
            marker.handshake()
            marker_id = marker.send(MARKER, EVENT)

            # The backend: every event once, in order, as it was sent, and then the marker.
            self.assertEqual(0, event_backend.wait())
            expected = [(EVENT, message_id, payload, sender, KEY) for message_id, payload in sent]
            expected.append((EVENT, marker_id, MARKER.decode(), marker.instance_id, None))
            self.assertEqual(
                expected,
                [
                    (event["type"], event["id"], event.get("payload"), event["sender"], event.get("key"))
                    for event in event_backend.events_of("received")
                ],
            )

            # The wire: the batches, each one message of the events' type and
            # key with an id of its own, and nothing alone before the marker.
            on_the_wire = []
            for _ in range(len(PAYLOADS) + 1):  # every event alone, at worst, and the marker
                mxmsg = wire.receive_type(EVENT)
                if mxmsg.id == marker_id:
                    break
                on_the_wire.append(mxmsg)
            else:
                self.fail("no marker on the wire after %d messages" % len(on_the_wire))
            self.assertEqual(
                [(True, EVENT, KEY, sender, len(batch)) for batch in BATCHES],
                [
                    (mxmsg.HasField("batch"), mxmsg.type, mxmsg.key, mxmsg.sender, len(unpacked(mxmsg)))
                    for mxmsg in on_the_wire
                ],
            )
            self.assertEqual(
                len(BATCHES), len({mxmsg.id for mxmsg in on_the_wire} - set(ids)), "each batch has an id of its own"
            )
            self.assertEqual(
                [(message_id, EVENT, payload.encode(), sender, KEY) for message_id, payload in sent],
                [
                    (message.id, message.type, message.message, message.sender, message.key)
                    for mxmsg in on_the_wire
                    for message in unpacked(mxmsg)
                ],
            )
            # Closed before the multiplexer stops, which would wait for them.
            marker.close()
            wire.close()


if __name__ == "__main__":
    harness.main()
