"""confirmed events reach a backend at least once, though the one they first went to dies holding them."""

import unittest

from multiplexer import recording
from multiplexer.Recording_pb2 import RoutedMessage
from tests import harness
from tests.harness import Cluster, constants as C, spawn

EVENT = C.types.TEST_EVENT_ANY  # to any one TEST_EVENT_BACKEND, in turn
PER_BATCH = 5
PAYLOADS = ["event%d" % number for number in range(20)]
HOUR = 3600.0  # the batches' delay, which never runs out here: they go full; twice that, their timeout
RETRY_AFTER = 1.0  # what nothing confirmed waits before it goes again


class ConfirmedEvents(unittest.TestCase):
    """An event client sends twenty events as confirmed sends, five a batch,
    to two event backends the multiplexer gives the batches to in turn, and
    flushes, which waits for every confirmation. The first backend dies at
    the first event it gets, before handling it, so that nothing of what it
    held is confirmed: what it held goes again once nothing confirmed it for
    retry_after, to the backend left. The flush ends with every event
    confirmed, none given up on; the survivor has every event, at least
    once; and the multiplexer's recording holds no more CONFIRMATIONs than
    batches reached the survivor, each confirming a batch at once. Then
    the same with the events confirmed one by one, each in a batch of its
    own, not batched.
    """

    def test_every_event_is_confirmed_though_a_backend_dies(self):
        self._every_event_confirmed(
            dict(batch_records=PER_BATCH, batch_delay=HOUR, batch_timeout=2 * HOUR),
            least_batches=len(PAYLOADS) // PER_BATCH // 2,
        )

    def test_events_confirmed_one_by_one_though_a_backend_dies(self):
        self._every_event_confirmed(dict(batch_timeout=2 * HOUR), least_batches=len(PAYLOADS))  # each its own

    def _every_event_confirmed(self, delivery, least_batches):
        """The story, with the client's delivery flags `delivery`, at least
        `least_batches` batches reaching the survivor."""
        cfg = harness.CONFIG
        with Cluster(1, record=True) as cluster:
            doomed = spawn(
                "event_backend",
                cfg.lang("event_backend"),
                mx=cluster.addresses,
                name="doomed",
                type=C.peers.TEST_EVENT_BACKEND,
                crash_at=1,
            )
            survivor = spawn(
                "event_backend",
                cfg.lang("event_backend"),
                mx=cluster.addresses,
                name="survivor",
                type=C.peers.TEST_EVENT_BACKEND,
            )
            doomed.wait_for("connected", connections=1)
            survivor_id = survivor.wait_for("connected", connections=1)["instance_id"]

            event_client = spawn(
                "event_client",
                cfg.lang("event_client"),
                mx=cluster.addresses,
                type=C.peers.TEST_EVENT_CLIENT,
                send=[(EVENT, payload) for payload in PAYLOADS],
                confirm=True,
                retry_after=RETRY_AFTER,
                **delivery,
            )
            self.assertEqual(0, event_client.wait())
            self.assertEqual([], event_client.events_of("error"))
            self.assertEqual(
                [True], [event["is_sent"] for event in event_client.events_of("flushed")], "every event confirmed"
            )
            client_id = event_client.events_of("connected")[0]["instance_id"]
            self.assertEqual(3, doomed.wait(), "the doomed backend dies at its first event")
            self.assertEqual(0, doomed.events_of("crash")[0].get("received", 0), "before handling it")

            # Every event reached the survivor, those the doomed one held sent
            # again: at least once, in the order of each batch.
            got = [event.get("payload") for event in survivor.events_of("received")]
            self.assertEqual(set(PAYLOADS), set(got))
            self.assertGreaterEqual(len(got), len(PAYLOADS))
            self.assertEqual(0, survivor.stop())
            cluster.mx[0].stop()
            routed = [
                record.routed
                for record in recording.read(cluster.mx[0].record_file, constants=C)
                if record.HasField("routed")
            ]

        # The confirmations went batched: one for a batch, or for several,
        # never one for each event.
        batches = [
            r for r in routed if r.type == EVENT and r.batch and r.recipient == survivor_id and r.sender == client_id
        ]
        confirmations = [
            r
            for r in routed
            if r.type == C.types.CONFIRMATION
            and r.sender == survivor_id
            and r.recipient == client_id
            and r.disposition == RoutedMessage.DELIVERED
        ]
        self.assertGreaterEqual(len(batches), least_batches, "the survivor's batches")
        self.assertTrue(0 < len(confirmations) <= len(batches), "%d for %d" % (len(confirmations), len(batches)))


if __name__ == "__main__":
    harness.main()
