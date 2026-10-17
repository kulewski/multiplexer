"""a query of a type with no routing rule fails at once, not after a timeout.

The multiplexer answers a message whose type has no rule (and no `to`)
with DELIVERY_ERROR right away; query() then searches for a backend, every
multiplexer says no, and the call raises OperationFailed within milliseconds.
"""

import unittest

from tests import harness
from tests.harness import Cluster, constants as C, spawn

# A stage's time, a failure detector: the kind of the error tells "at once"
# without a clock, since a query whose request stage ran out could still
# be answered late, waits for that, and ends OperationTimedOut.
TIMEOUT = 30


class UnroutedType(unittest.TestCase):
    """Checks that a query of a type nobody serves ends in an error, not a hang."""

    def test_query_fails_at_once(self):
        cfg = harness.CONFIG
        with Cluster(1) as cluster:
            client = spawn(
                "client",
                cfg.lang("client"),
                mx=cluster.addresses,
                type=C.peers.TEST_CLIENT,
                timeout=TIMEOUT,
                query=[(C.types.TEST_UNROUTED, "anyone?")],
            )
            self.assertEqual(0, client.wait(timeout=120))
            self.assertEqual([], client.events_of("response"))
            errors = client.events_of("error")
            self.assertEqual(1, len(errors))
            self.assertEqual("OperationFailed", errors[0]["kind"], "a delivery error at once, not a timeout")


if __name__ == "__main__":
    harness.main()
