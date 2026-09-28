"""A rule for every peer type, ALL_TYPES, reaches the ordinary peer types
only: the reserved ones, the controllers mxcontrol connects as (rules,
recording, a recording tap), get answers to what they ask, and a tap its
records, never what rules route, where every ALL_TYPES message went to them
too. And it never reports a delivery error: a type that came and went,
with nobody connected now, is no failure, where the sender got a
DELIVERY_ERROR for each such type although others received the message.
Ordered, not timed: once an ordinary peer got the message, the controller
asks for the rules' status, or the sender sends itself a message, and the
answer comes after anything routed to it before.
"""

import unittest

from multiplexer.Multiplexer_pb2 import (
    RULES_CONTROL,
    RULES_CONTROLLER,
    RULES_STATUS,
    MultiplexerMessage,
    MultiplexerMessageDescription,
    RulesControl,
)
from multiplexer.multiplexer_constants import peers, types
from multiplexer.testing import Cluster, runfile
from multiplexer.testing.raw_peer import RawPeer, frame

RULES = runfile("tests/testing.rules")


def to_all_types(sender: RawPeer, payload: bytes) -> MultiplexerMessage:
    """An event from `sender` that its own rule sends to every peer type,
    asking for a report when nobody takes it."""
    event = sender.message(payload, types.PYTHON_TEST_REQUEST)
    event.override_rrules.add(
        peer_type=peers.ALL_TYPES, whom=MultiplexerMessageDescription.RoutingRule.ALL, report_delivery_error=True
    )
    return event


class AllTypesTest(unittest.TestCase):
    """See the module docstring."""

    def test_a_rule_for_all_types_reaches_no_controller(self) -> None:
        with Cluster(1, rules=RULES) as cluster:
            endpoint = cluster.endpoints[0]
            controller = RawPeer(endpoint, RULES_CONTROLLER)
            receiver = RawPeer(endpoint, peers.PYTHON_TEST_SERVER)
            sender = RawPeer(endpoint, peers.WEBSITE)
            try:
                for peer in (controller, receiver, sender):
                    peer.handshake()
                event = to_all_types(sender, b"to every type")
                sender.send_raw(frame(event.SerializeToString()))
                self.assertEqual(event.id, receiver.receive_type(types.PYTHON_TEST_REQUEST).id)
                asked = controller.send(RulesControl(action=RulesControl.STATUS).SerializeToString(), RULES_CONTROL)
                while True:
                    mxmsg = controller.receive()
                    self.assertNotEqual(types.PYTHON_TEST_REQUEST, mxmsg.type, "the controller was routed the event")
                    if mxmsg.type == RULES_STATUS and mxmsg.references == asked:
                        break
            finally:
                for peer in (controller, receiver, sender):
                    peer.close()

    def test_a_rule_for_all_types_reports_no_delivery_error(self) -> None:
        with Cluster(1, rules=RULES) as cluster:
            endpoint = cluster.endpoints[0]
            sender = RawPeer(endpoint, peers.WEBSITE)
            receiver = RawPeer(endpoint, peers.PYTHON_TEST_SERVER)
            gone = RawPeer(endpoint, peers.PYTHON_TEST_CLIENT)
            try:
                for peer in (sender, receiver, gone):
                    peer.handshake()
                gone.close()
                cluster.wait_for_peer_gone(peers.PYTHON_TEST_CLIENT)
                event = to_all_types(sender, b"to whoever is there")
                sender.send_raw(frame(event.SerializeToString()))
                self.assertEqual(event.id, receiver.receive_type(types.PYTHON_TEST_REQUEST).id)
                last = sender.send(b"the last", types.PYTHON_TEST_REQUEST, to=sender.instance_id)
                while True:
                    mxmsg = sender.receive()
                    self.assertNotEqual(types.DELIVERY_ERROR, mxmsg.type, "a delivery error for an ALL_TYPES rule")
                    if mxmsg.id == last:
                        break
            finally:
                for peer in (sender, receiver, gone):
                    peer.close()


if __name__ == "__main__":
    unittest.main()
