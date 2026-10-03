"""A draining BaseMultiplexerServer serves what it had already read: the
requests the synchronous client pulled off the socket while a reply was
being sent are handled before the connections close, whether the drain
ends on the multiplexer's confirmation or on its cap. Every request sent
is therefore either answered or, routed after the multiplexer applied the
drain routing, refused by the multiplexer with a delivery error: none
vanishes into a timeout, and the backend refuses none of what it had read,
its refusals told from the multiplexer's by their `from`.
"""

import threading
import time
import unittest

from multiplexer.Multiplexer_pb2 import Routing
from multiplexer.multiplexer_constants import peers, types
from multiplexer.mxclient import OperationTimedOut
from multiplexer.servers import BaseMultiplexerServer
from multiplexer.testing import BackendThread, Cluster, TestClient, runfile, wait_until

RULES = runfile("tests/testing.rules")  # the file the constants were generated from

REQUEST = types.PYTHON_TEST_REQUEST
RESPONSE = types.PYTHON_TEST_RESPONSE
REQUESTS = 12


class Flood:
    """The counts a flood's sender and the Slow backend it floods keep
    against each other, so that the sender is ahead by construction,
    whatever the speed of either thread: the backend handles a request only
    once the sender has sent at least twice as many as it had handled, and
    four more, and the sender sends only while it is fewer than eight
    beyond that, so that what comes back to it stays far below what a
    client holds unread. The flood is over once the backend has left or the
    sender gave up, which ends every wait; each wait also has a bound only
    a failure reaches."""

    def __init__(self) -> None:
        self._changed = threading.Condition()
        self._sent = 0
        self._handled = 0
        self._over = False
        self.left = False  # the backend left while the sender was still at it

    def wait_to_handle(self, handled: int) -> None:
        """The backend, before it handles a request, having handled `handled`."""
        with self._changed:
            self._handled = handled
            self._changed.notify_all()  # room for the sender
            self._changed.wait_for(lambda: self._sent >= 2 * handled + 4 or self._over, timeout=30)

    def wait_to_send(self, until: float) -> bool:
        """The sender, before it sends one more: False once the flood is
        over or `until`, a time.time(), passed."""
        with self._changed:
            self._changed.wait_for(
                lambda: self._sent < 2 * self._handled + 8 or self._over, timeout=max(0.0, until - time.time())
            )
            return not self._over and time.time() < until

    def sent_one(self) -> None:
        """The sender sent one more."""
        with self._changed:
            self._sent += 1
            self._changed.notify_all()

    def end(self, left: bool) -> None:
        """The flood is over: `left` when the backend left, else the sender gave up."""
        with self._changed:
            self._over = True
            self.left = self.left or left
            self._changed.notify_all()


class Slow(BaseMultiplexerServer):
    """Answers every request after a pause, so that the rest queue up; with
    a `flood`, only once the flood's sender is far enough ahead (Flood).
    Given `drain_after`, it starts its own drain, from periodic_task(),
    once it has handled that many requests: a count, not another thread's
    timing. With `drain_ends_with_waiting`, its drain is over, besides the
    default's cap, only when requests are read and waiting, so that the
    drain ends with something for what follows the loop to serve, by
    construction. Notes the moment its drain ended: how many requests it
    had handled by then, and whether more were read and waiting."""

    multiplexer_client_type = peers.PYTHON_TEST_SERVER

    def __init__(
        self,
        addresses,
        drain_after: int | None = None,
        drain_ends_with_waiting: bool = False,
        flood: Flood | None = None,
        **kwargs,
    ):
        super().__init__(addresses, **kwargs)
        self.handled: list[bytes] = []
        self.flood = flood
        self.drain_after = drain_after
        self.drain_ends_with_waiting = drain_ends_with_waiting
        self.instance_id = self.conn.instance_id  # the `from` of its refusals, after close() too
        self.handled_when_drained: int | None = None
        self.waiting_when_drained = False

    def serve_forever(self, *args, **kwargs) -> None:
        """serve_forever(), then the end of the flood, if any: its sender stops."""
        try:
            super().serve_forever(*args, **kwargs)
        finally:
            if self.flood is not None:
                self.flood.end(left=True)

    def handle_message(self, mxmsg):
        if self.flood is not None:
            self.flood.wait_to_handle(len(self.handled))
        self.handled.append(mxmsg.message)
        time.sleep(0.03)
        self.send_message(message=mxmsg.message.upper(), type=RESPONSE)

    def periodic_task(self):
        if self.drain_after is not None and not self.draining and len(self.handled) >= self.drain_after:
            self.start_draining()

    def drained(self) -> bool:
        """The default, with requests read and waiting too when asked for,
        noting the moment it first holds, when serve_forever() leaves its
        loop with what was read still to serve."""
        over = super().drained() and (not self.drain_ends_with_waiting or self.conn.has_incoming_messages())
        if over and self.handled_when_drained is None:
            self.handled_when_drained = len(self.handled)
            self.waiting_when_drained = self.conn.has_incoming_messages()
        return over


class ServersDrainTest(unittest.TestCase):
    """The queue the drain must not drop, on the confirmation and on the cap."""

    @classmethod
    def setUpClass(cls):
        cls.cluster = Cluster(1, rules=RULES).__enter__()

    @classmethod
    def tearDownClass(cls):
        cls.cluster.__exit__(None, None, None)

    def drain(self, drain_seconds: float, drain_ends_with_waiting: bool) -> tuple[float, Slow, int]:
        """Twelve requests at once to a backend that starts its drain once
        it has handled two, the drain capped at `drain_seconds` and, with
        `drain_ends_with_waiting`, over only with requests read and waiting:
        how long the backend took to leave, from the first request, the
        backend, and how many requests it refused itself. Every request has
        one outcome: a response from the backend, or a delivery error, the
        backend's or the multiplexer's, told apart by their `from`; none
        vanishes into a timeout, and what the backend served was answered."""
        served = BackendThread(
            lambda: Slow(self.cluster.endpoints, drain_after=2, drain_ends_with_waiting=drain_ends_with_waiting),
            drain_seconds=drain_seconds,
        ).start()
        self.cluster.wait_for_peer(peers.PYTHON_TEST_SERVER)
        assert isinstance(served.backend, Slow)
        server = served.backend
        with TestClient(self.cluster, peers.WEBSITE) as client:
            started = time.time()
            sent = {client.send(b"r%d" % index, REQUEST) for index in range(REQUESTS)}
            wait_until(lambda: not served.running, 30, "serve_forever() returned")
            took = time.time() - started
            self.assertIsNone(served.error)
            responses = refused_by_backend = refused_by_multiplexer = 0
            deadline = time.time() + 5
            while sent and time.time() < deadline:
                try:
                    reply = client.receive(timeout=0.5)
                except OperationTimedOut:
                    continue
                if reply.references not in sent:
                    continue
                sent.discard(reply.references)
                if reply.type != types.DELIVERY_ERROR:
                    responses += 1
                elif reply.sender == server.instance_id:
                    refused_by_backend += 1
                else:
                    refused_by_multiplexer += 1
            self.assertEqual(
                REQUESTS, responses + refused_by_backend + refused_by_multiplexer, "none vanished into a timeout"
            )
            self.assertEqual(len(server.handled), responses, "what the backend served was answered")
        self.cluster.wait_for_peer_gone(peers.PYTHON_TEST_SERVER)
        return took, server, refused_by_backend

    def test_a_drain_on_the_confirmation_serves_what_was_read(self):
        """The drain routing turns every path off, so the drain ends on the
        multiplexer's confirmation, which comes behind every request routed
        to the backend and counts only once those read are handled: well
        within the cap, nothing left waiting, and nothing the backend had
        read refused by it, where the close refused what the loop had not
        handled."""
        took, server, refused_by_backend = self.drain(10, drain_ends_with_waiting=False)
        self.assertLess(took, 5, "it left on the confirmation, not at the cap")
        self.assertFalse(server.waiting_when_drained, "the confirmation counted with requests waiting")
        self.assertEqual(0, refused_by_backend, "the backend refused what it had read when the drain ended")

    def test_a_drain_on_its_cap_serves_what_was_read(self):
        """A drain on its cap, 0.2 s, that the backend says is over only
        with requests read and waiting, so that they are waiting when it
        ends by construction: it serves them after the drain ended, the
        loop gone, where the close refused them."""
        _, server, _ = self.drain(0.2, drain_ends_with_waiting=True)
        self.assertTrue(server.waiting_when_drained, "nothing waiting when the drain ended")
        assert server.handled_when_drained is not None
        self.assertGreater(
            len(server.handled),
            server.handled_when_drained,
            "what it had read when the drain ended was refused, not served",
        )

    def test_a_stop_without_a_drain_refuses_what_was_read(self):
        """stop() with requests read and not handled yet: the close refuses
        them, so that their senders retry elsewhere at once, where they
        vanished into the senders' timeouts. Every request has one outcome."""
        served = BackendThread(lambda: Slow(self.cluster.endpoints)).start()
        self.cluster.wait_for_peer(peers.PYTHON_TEST_SERVER)
        assert isinstance(served.backend, Slow)
        server = served.backend
        with TestClient(self.cluster, peers.WEBSITE) as client:
            sent = {client.send(b"r%d" % index, REQUEST) for index in range(REQUESTS)}
            wait_until(lambda: len(server.handled) >= 2, 10, "the first replies, with the rest read into the queue")
            server.stop()
            wait_until(lambda: not served.running, 10, "serve_forever() returned")
            self.assertIsNone(served.error)
            responses = refused = 0
            deadline = time.time() + 5
            while sent and time.time() < deadline:
                try:
                    reply = client.receive(timeout=0.5)
                except OperationTimedOut:
                    continue
                if reply.references not in sent:
                    continue
                sent.discard(reply.references)
                if reply.type == types.DELIVERY_ERROR:
                    refused += 1
                else:
                    responses += 1
            self.assertEqual(REQUESTS, responses + refused, "none vanished into a timeout")
            self.assertGreater(refused, 0, "refused at the close")
        self.cluster.wait_for_peer_gone(peers.PYTHON_TEST_SERVER)

    def test_a_drain_under_a_flood_ends(self):
        """A lone backend that drains keeping its last-resort path open,
        under a flood that stays ahead of it by count (Flood), whatever the
        speed of either thread: it leaves at the end of its drain, having
        handled what it had read by then and refused what arrived later,
        where every reply's turn of the loop read more, handled in turn, so
        that it never left. The backend starts its drain once it has
        handled two, and says it is over, at its cap, only with requests
        read and waiting, so that there are some when it ends by
        construction: they are served after the loop, where the close
        refused them. Every request the flood sent has one outcome: a
        response; a delivery error, the backend's or, once it is gone, the
        multiplexer's; or, for one the multiplexer routed to the backend,
        still its last resort, in the moment it closed, a drop the backend
        counts and logs, which no answer follows."""
        routing = Routing(any=False, all=False, last_resort=True)
        flood = Flood()
        served = BackendThread(
            lambda: Slow(
                self.cluster.endpoints,
                drain_after=2,
                drain_ends_with_waiting=True,
                flood=flood,
                drain_routing=routing,
            ),
            drain_seconds=0.3,
        ).start()
        self.cluster.wait_for_peer(peers.PYTHON_TEST_SERVER)
        assert isinstance(served.backend, Slow)
        server = served.backend
        with TestClient(self.cluster, peers.WEBSITE) as client:
            sent: set[int] = set()
            until = time.time() + 20
            while flood.wait_to_send(until):  # as fast as the lockstep lets it, until the backend has left
                sent.add(client.send(b"f%d" % len(sent), REQUEST))
                flood.sent_one()
            left_under_the_flood = flood.left
            flood.end(left=False)  # the sender stopped: whatever waits for it stops waiting
            if not left_under_the_flood:
                server.stop()
            wait_until(lambda: not served.running, 10, "the backend's thread ended")
            self.assertTrue(left_under_the_flood, "serve_forever() went on under the flood")
            self.assertIsNone(served.error)
            self.assertTrue(server.waiting_when_drained, "nothing waiting when the drain ended")
            # Counted, not waited out: the answers come until only the drops
            # are left, which get none.
            dropped = server.conn.dropped_while_closing()
            responses = refused = 0
            waiting = set(sent)
            answers_by = time.time() + 10
            while len(waiting) > dropped and time.time() < answers_by:
                try:
                    reply = client.receive(timeout=0.5)
                except OperationTimedOut:
                    continue
                if reply.references not in waiting:
                    continue
                waiting.discard(reply.references)
                if reply.type == types.DELIVERY_ERROR:
                    refused += 1
                else:
                    responses += 1
            self.assertEqual(
                len(sent),
                responses + refused + dropped,
                "none vanished into a timeout unless the backend counted it dropped at its close",
            )
            self.assertEqual(len(server.handled), responses, "what the backend served was answered")
            assert server.handled_when_drained is not None
            self.assertGreater(
                len(server.handled),
                server.handled_when_drained,
                "what it had read when the drain ended was refused, not served",
            )
            # The backend handled each request only once more than twice as
            # many as it had handled were sent, so more of the flood was
            # refused or dropped than served, by construction; which of the
            # two is not, a request that reaches the backend as it closes
            # being dropped.
            self.assertGreater(refused + dropped, len(server.handled), "the flood outran it")
        self.cluster.wait_for_peer_gone(peers.PYTHON_TEST_SERVER)


if __name__ == "__main__":
    unittest.main()
