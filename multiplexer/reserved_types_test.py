"""A message of a number the protocol keeps, sent without `to`, is told to
its sender with a DELIVERY_ERROR and recorded, as one of a type the rules
file names with no rule, or does not name, is: type 0 and 13 to 99, which
the protocol does not use, as a type of no entry, is_known_type false and
UNKNOWN_TYPE; a PING or a status, which go somewhere only with `to`, as a
type with no rule, is_known_type true and NO_RULE. They were dropped with
a log line alone, their senders left to their timeouts. A DELIVERY_ERROR
without `to` gets nothing back, never answered with another. Counted, not
timed: the sender's marker to itself comes back after everything it sent
before was routed, the answers ahead of it on its one connection.
"""

import random
import unittest

from multiplexer import recording
from multiplexer.Multiplexer_pb2 import DeliveryError
from multiplexer.multiplexer_constants import peers, types
from multiplexer.Recording_pb2 import RoutedMessage
from multiplexer.testing import Cluster, runfile
from multiplexer.testing.raw_peer import RawPeer

RULES = runfile("tests/testing.rules")  # the file the constants were generated from


class ReservedTypesTest(unittest.TestCase):
    """See the module docstring."""

    def test_a_reserved_type_without_to_is_told_and_recorded(self) -> None:
        with Cluster(1, rules=RULES, record=True, record_payload_bytes=1) as cluster:
            sender = RawPeer(cluster.endpoints[0], peers.TEST_EVENT_CLIENT)
            sender.handshake()
            unknown = [sender.send(b"u", number) for number in (0, 13, types.MAX_MULTIPLEXER_META_PACKET)]
            no_rule = [sender.send(b"n", number) for number in (types.PING, types.PEER_STATUS, types.RULES_STATUS)]
            silent = sender.send(b"d", types.DELIVERY_ERROR)
            marker = random.randint(1, 2**62)
            sender.send(str(marker).encode(), types.TEST_EVENT, to=sender.instance_id)
            told: dict[int, bool] = {}  # the message each DELIVERY_ERROR references, and is_known_type
            while True:
                mxmsg = sender.receive(timeout=60)
                if mxmsg.type == types.TEST_EVENT and mxmsg.message == str(marker).encode():
                    break
                if mxmsg.type == types.DELIVERY_ERROR:
                    error = DeliveryError()
                    error.ParseFromString(mxmsg.message)
                    told[mxmsg.references] = error.is_known_type
            sender.close()
            cluster.mx[0].stop()
            routed = [
                record.routed for record in recording.read(cluster.mx[0].record_file) if record.HasField("routed")
            ]

        self.assertEqual(dict.fromkeys(unknown, False) | dict.fromkeys(no_rule, True), told)
        self.assertNotIn(silent, told, "a DELIVERY_ERROR answered with another")
        records = {r.id: (r.disposition, r.error_reported) for r in routed}
        for message_id in unknown:
            self.assertEqual((RoutedMessage.UNKNOWN_TYPE, True), records.get(message_id))
        for message_id in no_rule:
            self.assertEqual((RoutedMessage.NO_RULE, True), records.get(message_id))


if __name__ == "__main__":
    unittest.main()
