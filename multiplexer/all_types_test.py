"""A rule for every peer type, ALL_TYPES, reaches the ordinary peer types
only: the reserved ones, the controllers mxcontrol connects as (rules,
recording, a recording tap), get answers to what they ask, and a tap its
records, never what rules route, where every ALL_TYPES message went to them
too. Ordered, not timed: once an ordinary peer got the message, the
controller asks for the rules' status, and the answer comes after
anything routed to it before.
"""

import unittest

from multiplexer.Multiplexer_pb2 import (
    RULES_CONTROL,
    RULES_CONTROLLER,
    RULES_STATUS,
    MultiplexerMessageDescription,
    RulesControl,
)
from multiplexer.multiplexer_constants import peers, types
from multiplexer.testing import Cluster, runfile
from multiplexer.testing.raw_peer import RawPeer, frame

RULES = runfile("tests/testing.rules")


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
                event = sender.message(b"to every type", types.PYTHON_TEST_REQUEST)
                event.override_rrules.add(peer_type=peers.ALL_TYPES, whom=MultiplexerMessageDescription.RoutingRule.ALL)
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


if __name__ == "__main__":
    unittest.main()
