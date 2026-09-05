"""the multiplexer records every peer event and delivery attempt, and the recording reads back.

A query with its reply, a broadcast to two backends, a request nobody
serves and a peer connecting and leaving all pass through a multiplexer
started with --record. The file is read back with multiplexer.recording:
the header names the rules, every record has the fields routing resolved,
and they come in the order things happened.
"""

import unittest

from multiplexer import recording
from multiplexer.Recording_pb2 import PeerEvent, RoutedMessage
from tests import harness
from tests.harness import Cluster, constants as C, spawn


class Recording(unittest.TestCase):
    """Checks the recording's contents against what the roles reported."""

    def test_records_everything_routed(self):
        cfg = harness.CONFIG
        with Cluster(1, record=True) as cluster:
            backend = spawn(
                "backend",
                cfg.lang("backend"),
                mx=cluster.addresses,
                type=C.peers.TEST_BACKEND_A,
                serves={C.types.TEST_REQUEST_A: C.types.TEST_RESPONSE},
            )
            backend_id = backend.wait_for("connected")["instance_id"]
            subscribers = []
            for index in range(2):
                subscriber = spawn(
                    "event_backend",
                    cfg.lang("event_backend"),
                    mx=cluster.addresses,
                    name="subscriber%d" % index,
                    type=C.peers.TEST_EVENT_BACKEND,
                    **{"for": 3},
                )
                subscribers.append(subscriber.wait_for("connected")["instance_id"])
            client = spawn(
                "client",
                cfg.lang("client"),
                mx=cluster.addresses,
                type=C.peers.TEST_CLIENT,
                timeout=5,
                query=[(C.types.TEST_REQUEST_A, "hello"), (C.types.TEST_REQUEST_B, "nobody serves this")],
            )
            self.assertEqual(0, client.wait())
            client_id = client.events_of("connected")[0]["instance_id"]
            request_id = client.events_of("response")[0]["references"]
            event_client = spawn(
                "event_client",
                cfg.lang("event_client"),
                mx=cluster.addresses,
                type=C.peers.TEST_EVENT_CLIENT,
                send=[(C.types.TEST_EVENT, "to everyone")],
            )
            self.assertEqual(0, event_client.wait())
            event_id = event_client.events_of("sent")[0]["id"]
            self.assertEqual(0, backend.stop())
            cluster.mx[0].stop()

            records = list(recording.read(cluster.mx[0].record_file, constants=C))
            kinds = [record.WhichOneof("event") for record in records]
            self.assertEqual("header", kinds[0])
            self.assertEqual(C.RULES_SHA1, records[0].header.rules_sha1)

            peers = [(record.peer.kind, record.peer.peer_id) for record in records if record.HasField("peer")]
            self.assertIn((PeerEvent.CONNECTED, backend_id), peers)
            self.assertIn((PeerEvent.DISCONNECTED, backend_id), peers)
            self.assertLess(
                peers.index((PeerEvent.CONNECTED, backend_id)), peers.index((PeerEvent.DISCONNECTED, backend_id))
            )

            routed = [record.routed for record in records if record.HasField("routed")]
            request = [r for r in routed if r.id == request_id]
            self.assertEqual(1, len(request), "one delivery attempt for the request")
            self.assertEqual(RoutedMessage.DELIVERED, request[0].disposition)
            self.assertEqual((client_id, C.peers.TEST_CLIENT), (getattr(request[0], "from"), request[0].from_peer_type))
            self.assertEqual(
                (backend_id, C.peers.TEST_BACKEND_A), (request[0].recipient, request[0].recipient_peer_type)
            )
            self.assertEqual(b"hello", request[0].payload)
            reply = [r for r in routed if r.references == request_id and r.type == C.types.TEST_RESPONSE]
            self.assertEqual(1, len(reply))
            self.assertEqual((backend_id, client_id), (getattr(reply[0], "from"), reply[0].recipient))
            self.assertLess(
                routed.index(request[0]), routed.index(reply[0]), "the request is recorded before its reply"
            )

            broadcast = [r for r in routed if r.id == event_id]
            self.assertEqual(RoutedMessage.DELIVERED, broadcast[0].disposition)
            self.assertEqual(sorted(subscribers), sorted(r.recipient for r in broadcast), "one record per subscriber")

            unserved = [r for r in routed if r.type == C.types.TEST_REQUEST_B]
            self.assertTrue(unserved)
            self.assertEqual(RoutedMessage.NO_RECIPIENT, unserved[0].disposition)
            self.assertEqual(C.peers.TEST_BACKEND_B, unserved[0].recipient_peer_type)
            self.assertTrue(unserved[0].error_reported)
            errors = [r for r in routed if r.type == C.types.DELIVERY_ERROR and r.recipient == client_id]
            self.assertTrue(errors, "the delivery error sent back is a routed message too")

    def test_payloads_can_be_truncated(self):
        cfg = harness.CONFIG
        with Cluster(1, record=True, record_payload_bytes=3) as cluster:
            backend = spawn(
                "backend",
                cfg.lang("backend"),
                mx=cluster.addresses,
                type=C.peers.TEST_BACKEND_A,
                serves={C.types.TEST_REQUEST_A: C.types.TEST_RESPONSE},
            )
            backend.wait_for("connected")
            client = spawn(
                "client",
                cfg.lang("client"),
                mx=cluster.addresses,
                type=C.peers.TEST_CLIENT,
                query=[(C.types.TEST_REQUEST_A, "a long payload")],
            )
            self.assertEqual(0, client.wait())
            self.assertEqual(0, backend.stop())
            cluster.mx[0].stop()
            records = list(recording.read(cluster.mx[0].record_file, constants=C))
            self.assertEqual(3, records[0].header.payload_limit)
            requests = [r.routed for r in records if r.HasField("routed") and r.routed.type == C.types.TEST_REQUEST_A]
            self.assertEqual(b"a l", requests[0].payload)
            self.assertTrue(requests[0].truncated)


if __name__ == "__main__":
    harness.main()
