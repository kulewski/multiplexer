"""serve_forever(stall_seconds=...) watches the handling of a message and
the periodic_task() after it, never the poll's wait, and cancels the
watchdog however they end. It armed faulthandler's before the wait, so an
idle poll longer than stall_seconds dumped every thread for a stall there
was not, and cancelled it after periodic_task() only, so one that raised
left it armed. faulthandler is stood in for by a recorder of the arming
and the cancelling, and the client's receive is wrapped to note each
wait: ordered, not timed.
"""

import threading
import unittest
from typing import Any

from multiplexer import mxclient, servers
from multiplexer.Multiplexer_pb2 import MultiplexerMessage
from multiplexer.multiplexer_constants import peers, types
from multiplexer.servers import BaseMultiplexerServer
from multiplexer.testing import Cluster, runfile
from multiplexer.threaded_client import ThreadedClient

RULES = runfile("tests/testing.rules")
STALL = 60.0  # the watchdog never fires: its arming and cancelling are what is checked


class Watchdog:
    """faulthandler's two calls serve_forever() makes, noted in `events`."""

    def __init__(self, events: list[str]):
        self.events = events

    def dump_traceback_later(self, timeout: float, file: Any = None) -> None:
        """Noted as "arm"."""
        self.events.append("arm")

    def cancel_dump_traceback_later(self) -> None:
        """Noted as "cancel"."""
        self.events.append("cancel")


class Noted(BaseMultiplexerServer):
    """A backend that notes each message it handles and each periodic_task();
    one that raises from periodic_task() when `fail` is set."""

    def __init__(self, addresses: list[tuple[str, int]], events: list[str], fail: bool = False):
        super().__init__(addresses, type=peers.PYTHON_TEST_SERVER)
        self.events = events
        self.fail = fail

    def handle_message(self, mxmsg: MultiplexerMessage) -> None:
        """Noted as "handle"; the payload back to the requester."""
        self.events.append("handle")
        self.send_message(message=mxmsg.message, type=types.PYTHON_TEST_RESPONSE)

    def periodic_task(self) -> None:
        """Noted as "periodic"; raises when `fail` is set."""
        self.events.append("periodic")
        if self.fail:
            raise RuntimeError("periodic_task failed")


class StallSecondsTest(unittest.TestCase):
    """See the module docstring."""

    def setUp(self) -> None:
        """The recorder in faulthandler's place, the client's receive noting each wait."""
        self.events: list[str] = []
        events = self.events
        receive = mxclient.Client.receive_message

        def noted_receive(conn: Any, *args: Any, **kwargs: Any) -> Any:
            """The receive, noted as "wait" before it waits."""
            events.append("wait")
            return receive(conn, *args, **kwargs)

        self.addCleanup(setattr, servers, "faulthandler", servers.faulthandler)
        self.addCleanup(setattr, mxclient.Client, "receive_message", receive)
        servers.faulthandler = Watchdog(events)  # type: ignore[assignment]
        mxclient.Client.receive_message = noted_receive  # type: ignore[method-assign]

    def armed_at_each(self) -> list[tuple[str, bool]]:
        """Each noted step but the arming and cancelling, with whether the watchdog was armed then."""
        armed = False
        steps = []
        for event in self.events:
            if event in ("arm", "cancel"):
                self.assertNotEqual(armed, event == "arm", "armed twice, or cancelled unarmed: %s" % self.events)
                armed = event == "arm"
            else:
                steps.append((event, armed))
        self.assertFalse(armed, "left armed: %s" % self.events)
        return steps

    def test_the_handling_is_watched_and_the_wait_is_not(self):
        with Cluster(1, rules=RULES) as cluster:
            server = Noted(cluster.endpoints, self.events)
            server.connect()  # here, so that it is registered before it serves; serve_forever() adopts it
            serving = threading.Thread(
                target=server.serve_forever, kwargs={"poll": 0.05, "stall_seconds": STALL}, daemon=True
            )
            serving.start()
            try:
                with ThreadedClient(cluster.endpoints, peers.PYTHON_TEST_CLIENT) as client:
                    self.assertEqual(b"x", client.query(b"x", types.PYTHON_TEST_REQUEST, timeout=10).message)
            finally:
                server.stop()
                serving.join(10)
        steps = self.armed_at_each()
        self.assertIn(("handle", True), steps)
        for step, armed in steps:
            self.assertEqual(step != "wait", armed, "%s with the watchdog %s" % (step, "armed" if armed else "off"))

    def test_a_periodic_task_that_raises_leaves_no_watchdog_armed(self):
        with Cluster(1, rules=RULES) as cluster:
            server = Noted(cluster.endpoints, self.events, fail=True)
            with self.assertRaises(RuntimeError):
                server.serve_forever(poll=0.05, stall_seconds=STALL)
        self.assertEqual([("wait", False), ("periodic", True)], self.armed_at_each())


if __name__ == "__main__":
    unittest.main()
