"""FakePeer, BackendThread and TestClient against a real multiplexer.

Uses the peers and types of multiplexer.rules: PYTHON_TEST_SERVER answers
PYTHON_TEST_REQUEST, WEBSITE is the passive peer type a synchronous client
needs.
"""

import time
import unittest

from multiplexer.clients import BackendError, MxClient
from multiplexer.multiplexer_constants import peers, types
from multiplexer.mxclient import OperationFailed, OperationTimedOut
from multiplexer.servers import BaseMultiplexerServer
from multiplexer.testing import BackendThread, Cluster, FakePeer, TestClient, wait_until


class FakePeerTest(unittest.TestCase):
    """A scripted backend serving a test client on a one-multiplexer cluster."""

    @classmethod
    def setUpClass(cls):
        cls.cluster = Cluster(1).__enter__()

    @classmethod
    def tearDownClass(cls):
        cls.cluster.__exit__(None, None, None)

    def test_reply_with_answers_every_request(self):
        """reply_with() answers with the payload as the reply type given;
        the peer keeps what it received, and the multiplexer lists it."""
        with (
            FakePeer(self.cluster, peers.PYTHON_TEST_SERVER) as peer,
            TestClient(self.cluster, peers.WEBSITE) as client,
        ):
            peer.reply_with(types.PYTHON_TEST_REQUEST, b"pong", types.PYTHON_TEST_RESPONSE)
            self.assertIn(
                "PYTHON_TEST_SERVER", [name for _, name, _ in self.cluster.mx[0].connected_peers()], "registered"
            )
            for _ in range(3):
                reply = client.query(b"ping", types.PYTHON_TEST_REQUEST)
                self.assertEqual((types.PYTHON_TEST_RESPONSE, b"pong"), (reply.type, reply.message))
            self.assertEqual([b"ping"] * 3, [mxmsg.message for mxmsg in peer.messages(types.PYTHON_TEST_REQUEST)])
            self.assertEqual(3, len(peer.received))

    def test_reply_type_defaults_to_the_request_type(self):
        """Without a reply type the reply carries the request's type."""
        with (
            FakePeer(self.cluster, peers.PYTHON_TEST_SERVER) as peer,
            TestClient(self.cluster, peers.WEBSITE) as client,
        ):
            peer.reply_with(types.PYTHON_TEST_REQUEST, "same type")
            reply = client.query(b"ping", types.PYTHON_TEST_REQUEST)
            self.assertEqual((types.PYTHON_TEST_REQUEST, b"same type"), (reply.type, reply.message))

    def test_handler_sees_the_request_and_may_drop_it(self):
        """on() gets the MultiplexerMessage; a None result means no reply,
        so a query times out while an event is simply counted."""
        with (
            FakePeer(self.cluster, peers.PYTHON_TEST_SERVER) as peer,
            TestClient(self.cluster, peers.WEBSITE) as client,
        ):
            peer.on(types.PYTHON_TEST_REQUEST, lambda mxmsg: mxmsg.message.upper() if mxmsg.message else None)
            self.assertEqual(b"HELLO", client.query(b"hello", types.PYTHON_TEST_REQUEST).message)
            with self.assertRaises((OperationTimedOut, OperationFailed)):
                client.query(b"", types.PYTHON_TEST_REQUEST, timeout=0.5)
            client.send(b"event", types.PYTHON_TEST_REQUEST)
            client.send(b"event", types.PYTHON_TEST_REQUEST)

            # The unanswered query reaches the peer once per attempt: after its
            # timeout the client searches for a backend and sends it again
            # (docs/faq.md, "Can a backend receive the same request twice?"),
            # so its count is one or two depending on timing. The answered
            # query and the two events arrive exactly once each; wait for the
            # events, the last to be sent, rather than for a total.
            def payloads():
                return [mxmsg.message for mxmsg in peer.messages(types.PYTHON_TEST_REQUEST)]

            events = peer.wait_for(types.PYTHON_TEST_REQUEST, count=2, matching=lambda mxmsg: mxmsg.message == b"event")
            self.assertEqual([b"event", b"event"], [mxmsg.message for mxmsg in events])
            self.assertEqual(1, payloads().count(b"hello"))
            self.assertIn(payloads().count(b""), (1, 2), "the dropped query, once per attempt")
            self.assertEqual(2, payloads().count(b"event"))

    def test_wait_for_matching_names_the_message_it_means(self):
        """wait_for(matching=) returns the messages the predicate selects,
        counts only those, and times out saying so; messages() filters alike."""
        with (
            FakePeer(self.cluster, peers.PYTHON_TEST_SERVER) as peer,
            TestClient(self.cluster, peers.WEBSITE) as client,
        ):
            for payload in (b"one", b"two", b"three"):
                client.send(payload, types.PYTHON_TEST_REQUEST)
            (two,) = peer.wait_for(types.PYTHON_TEST_REQUEST, matching=lambda mxmsg: mxmsg.message == b"two")
            self.assertEqual(b"two", two.message)
            short = peer.wait_for(types.PYTHON_TEST_REQUEST, count=2, matching=lambda mxmsg: len(mxmsg.message) == 3)
            self.assertEqual([b"one", b"two"], [mxmsg.message for mxmsg in short])
            with self.assertRaises(TimeoutError) as raised:
                peer.wait_for(types.PYTHON_TEST_REQUEST, matching=lambda mxmsg: mxmsg.message == b"four", timeout=0.2)
            self.assertIn("0 of 1 message(s) of type 110 matching", str(raised.exception))
            peer.wait_for(types.PYTHON_TEST_REQUEST, count=3)
            self.assertEqual(
                [b"two", b"three"],
                [
                    m.message
                    for m in peer.messages(types.PYTHON_TEST_REQUEST, matching=lambda m: m.message.startswith(b"t"))
                ],
            )

    def test_unscripted_types_are_received_and_dropped(self):
        """A type with no handler is kept in `received` and gets no reply."""
        with (
            FakePeer(self.cluster, peers.PYTHON_TEST_SERVER) as peer,
            TestClient(self.cluster, peers.WEBSITE) as client,
        ):
            client.send(b"event", types.PYTHON_TEST_REQUEST)
            self.assertEqual(b"event", peer.wait_for(types.PYTHON_TEST_REQUEST)[0].message)
            with self.assertRaises(TimeoutError) as raised:
                peer.wait_for(types.PYTHON_TEST_RESPONSE, timeout=0.2)
            self.assertIn("0 of 1", str(raised.exception))

    def test_a_raising_handler_fails_the_test_at_stop(self):
        """The requester gets BACKEND_ERROR at once; stop() re-raises the
        handler's exception so that the test does not pass by accident."""
        peer = FakePeer(self.cluster, peers.PYTHON_TEST_SERVER).start()
        peer.on(types.PYTHON_TEST_REQUEST, lambda mxmsg: 1 / 0)
        with TestClient(self.cluster, peers.WEBSITE) as client:
            with self.assertRaises(BackendError):
                client.query(b"boom", types.PYTHON_TEST_REQUEST)
        with self.assertRaises(ZeroDivisionError):
            peer.stop()

    def test_direct_send_by_instance_id(self):
        """TestClient.send(to=) addresses one peer by its instance id."""
        with (
            FakePeer(self.cluster, peers.PYTHON_TEST_SERVER) as first,
            FakePeer(self.cluster, peers.PYTHON_TEST_SERVER) as second,
            TestClient(self.cluster, peers.WEBSITE) as client,
        ):
            self.cluster.wait_for_peer(peers.PYTHON_TEST_SERVER, count=2)
            assert second.backend is not None
            client.send(b"direct", types.PYTHON_TEST_RESPONSE, to=second.backend.conn.instance_id)
            self.assertEqual(b"direct", second.wait_for(types.PYTHON_TEST_RESPONSE)[0].message)
            self.assertEqual([], first.received)


class SendAfterRestartTest(unittest.TestCase):
    """A synchronous client that sat idle while a multiplexer restarted:
    the connection the multiplexer closed must not swallow the next send."""

    def test_the_closed_connection_is_retired_before_the_next_send(self):
        """Two multiplexers; one restarts while the client is idle. Both of
        the client's next two sends arrive: the dead connection is noticed
        when a connection is chosen, not after a write into it succeeded."""
        with Cluster(2) as cluster, FakePeer(cluster, peers.PYTHON_TEST_SERVER) as peer:
            client = TestClient(cluster, peers.WEBSITE)
            try:
                client.send(b"warm-up", types.PYTHON_TEST_REQUEST)
                peer.wait_for(types.PYTHON_TEST_REQUEST, matching=lambda m: m.message == b"warm-up")
                cluster.mx[0].restart()
                cluster.wait_for_peer(peers.PYTHON_TEST_SERVER)  # the fake is back on both
                # The client ran no loop since: its connection to the restarted
                # multiplexer still looks alive to it.
                client.send(b"first", types.PYTHON_TEST_REQUEST)
                client.send(b"second", types.PYTHON_TEST_REQUEST)
                peer.wait_for(types.PYTHON_TEST_REQUEST, matching=lambda m: m.message == b"first")
                peer.wait_for(types.PYTHON_TEST_REQUEST, matching=lambda m: m.message == b"second")
            finally:
                client.shutdown()

    def test_with_one_multiplexer_the_send_waits_for_the_reconnect(self):
        """The only multiplexer restarted: the send notices, waits for the
        client's reconnect timer, and the message arrives."""
        with Cluster(1) as cluster, FakePeer(cluster, peers.PYTHON_TEST_SERVER) as peer:
            client = TestClient(cluster, peers.WEBSITE)
            try:
                client.send(b"warm-up", types.PYTHON_TEST_REQUEST)
                peer.wait_for(types.PYTHON_TEST_REQUEST, matching=lambda m: m.message == b"warm-up")
                cluster.mx[0].restart()
                cluster.wait_for_peer(peers.PYTHON_TEST_SERVER)
                started = time.monotonic()
                client.send(b"after", types.PYTHON_TEST_REQUEST)
                self.assertLess(time.monotonic() - started, 9, "sent after one reconnect delay, not a timeout")
                peer.wait_for(types.PYTHON_TEST_REQUEST, matching=lambda m: m.message == b"after")
            finally:
                client.shutdown()

    def test_the_holder_keeps_one_client_across_a_restart(self):
        """MxClient hands out the same Client after a multiplexer restart;
        the client reconnects inside the next call, within the reconnect
        delay, and the query goes through."""
        with Cluster(1) as cluster, FakePeer(cluster, peers.PYTHON_TEST_SERVER) as peer:
            peer.reply_with(types.PYTHON_TEST_REQUEST, b"pong", types.PYTHON_TEST_RESPONSE)
            holder = MxClient(peers.WEBSITE, lambda: cluster.endpoints)
            try:
                first = holder.get()
                self.assertEqual(b"pong", first.query(b"ping", types.PYTHON_TEST_REQUEST).message)
                cluster.mx[0].restart()
                cluster.wait_for_peer(peers.PYTHON_TEST_SERVER)
                started = time.monotonic()
                again = holder.get()
                self.assertIs(first, again, "no rebuild: the client reconnects itself")
                self.assertEqual(b"pong", again.query(b"ping again", types.PYTHON_TEST_REQUEST).message)
                self.assertLess(time.monotonic() - started, 9, "one reconnect delay, not a timeout")
            finally:
                holder.shutdown()


class Upper(BaseMultiplexerServer):
    """A backend of the test's own: replies with the payload in upper case."""

    def __init__(self, addresses):
        super().__init__(addresses, type=peers.PYTHON_TEST_SERVER)
        self.handled = 0

    def handle_message(self, mxmsg):
        self.handled += 1
        self.send_message(message=mxmsg.message.upper(), type=types.PYTHON_TEST_RESPONSE, flush=True)


class BackendThreadTest(unittest.TestCase):
    """A backend built and served on its own thread."""

    def test_serves_until_stopped(self):
        """The factory runs on the serving thread; start() returns with the
        backend connected; stop() returns once the loop has left."""
        with Cluster(1) as cluster:
            served = BackendThread(lambda: Upper(cluster.endpoints)).start()
            cluster.wait_for_peer("PYTHON_TEST_SERVER")
            with TestClient(cluster, peers.WEBSITE) as client:
                self.assertEqual(b"ABC", client.query(b"abc", types.PYTHON_TEST_REQUEST).message)
            self.assertTrue(served.running)
            served.stop()
            self.assertFalse(served.running)
            assert served.backend is not None
            self.assertEqual(1, served.backend.handled)
            cluster.wait_for_peer_gone("PYTHON_TEST_SERVER", timeout=5)
            self.assertEqual([], cluster.mx[0].connected_peers())
            with BackendThread(lambda: Upper(cluster.endpoints)):
                cluster.wait_for_peer(peers.PYTHON_TEST_SERVER)
                with self.assertRaises(TimeoutError) as raised:
                    cluster.wait_for_peer_gone(peers.PYTHON_TEST_SERVER, timeout=0.2)
                self.assertIn("saw [1]", str(raised.exception))

    def test_factory_errors_surface_in_start(self):
        """What the factory raises comes out of start(), with `error` set."""

        def broken():
            raise ValueError("no backend today")

        served = BackendThread(broken)
        with self.assertRaises(ValueError):
            served.start()
        self.assertIsInstance(served.error, ValueError)


class WaitUntilTest(unittest.TestCase):
    """wait_until returns the first true value and names what it waited for."""

    def test_returns_the_value_and_names_the_wait(self):
        counter = iter(range(5))
        self.assertEqual(3, wait_until(lambda: next(counter) == 3 and 3, 5, "three"))
        with self.assertRaises(TimeoutError) as raised:
            wait_until(lambda: False, 0.1, "the impossible")
        self.assertIn("the impossible", str(raised.exception))


if __name__ == "__main__":
    unittest.main()
