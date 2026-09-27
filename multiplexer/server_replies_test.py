"""What a BaseMultiplexerServer sends, against a real multiplexer: a reply
that raised is no answer, so the requester gets BACKEND_ERROR, where the
reply counted as sent before it was; a report of a handler's exception
that fails leaves the loop serving and on_handler_exception() told, where
it ended serve_forever(); what periodic_task() sends is routed by its
type, where it went to the last requester; the requester's
send_and_receive() takes a payload, where it raised AttributeError; and
the replies the server sends itself, an echo, a report, a pickle reply,
are queued as in the C++ class, where each waited for its write.
"""

import pickle
import threading
import unittest

from multiplexer.clients import Client
from multiplexer.multiplexer_constants import peers, types
from multiplexer.mxclient import NotConnected, OperationFailed, OperationTimedOut
from multiplexer.servers import BackendError, BaseMultiplexerServer
from multiplexer.testing import BackendThread, Cluster, FakePeer
from multiplexer.testing import runfile

RULES = runfile("tests/testing.rules")  # the file the constants were generated from

REQUEST = types.PYTHON_TEST_REQUEST
RESPONSE = types.PYTHON_TEST_RESPONSE


class Backend(BaseMultiplexerServer):
    """Answers a request as its payload says: b"answer", b"fail to reply"
    (a reply that raises before anything goes) or b"raise"; announces each
    answered request with a TEST_EVENT from periodic_task(). Its
    report_error() fails when `failing_reports` is set."""

    def __init__(self, addresses: list[tuple[str, int]]):
        super().__init__(addresses, type=peers.PYTHON_TEST_SERVER)
        self.failing_reports = False
        self.exceptions: list[Exception] = []
        self.announcements = 0
        self.lock = threading.Lock()

    def handle_message(self, mxmsg) -> None:
        """Reply as the payload says."""
        if mxmsg.message == b"pickle":
            self.send_pickle({"answer": 42})
            return
        if mxmsg.message == b"raise":
            raise RuntimeError("from the handler")
        if mxmsg.message == b"raise oddly":
            raise RuntimeError("unknown name \ud800")  # a lone surrogate, as echoed from a request
        if mxmsg.message == b"fail to reply":
            self.send_message(message=b"answer", type=RESPONSE, no_such_field=1)  # raises where it is built
        self.send_message(message=b"answer", type=RESPONSE)
        self.announcements += 1

    def report_error(self, *args, **kwargs) -> None:
        """The library's, or a report that fails, as one through a connection that is gone does."""
        if self.failing_reports:
            raise NotConnected()
        super().report_error(*args, **kwargs)

    def on_handler_exception(self, exc: Exception) -> bool:
        """Keep the exception; serve on."""
        with self.lock:
            self.exceptions.append(exc)
        return True

    def periodic_task(self) -> None:
        """Announce what was answered: an event, not a reply."""
        while self.announcements:
            self.announcements -= 1
            self.send_message(message=b"announced", type=types.TEST_EVENT)


def reply_to(client: Client, message_id: int, timeout: float = 10.0):
    """The next message `client` reads that references `message_id`."""
    while True:
        mxmsg = client.read_message(timeout=timeout)
        if mxmsg.references == message_id:
            return mxmsg


class ServerRepliesTest(unittest.TestCase):
    """One backend, one client, and a listener for the events."""

    def test_a_reply_that_raised_is_answered_with_backend_error(self) -> None:
        with Cluster(1, rules=RULES) as cluster, BackendThread(lambda: Backend(cluster.endpoints)) as served:
            client = Client(cluster.endpoints, type=peers.WEBSITE)
            try:
                with self.assertRaises(BackendError):
                    client.query(b"fail to reply", REQUEST, timeout=2)
                assert served.backend is not None
                self.assertEqual(1, len(served.backend.exceptions))
            finally:
                client.shutdown()

    def test_an_error_text_utf8_cannot_carry_still_reaches_the_requester(self) -> None:
        """A handler's exception whose text holds a lone surrogate: the
        requester gets BACKEND_ERROR with it escaped, where the report
        failed to encode and the requester waited out its timeout."""
        with Cluster(1, rules=RULES) as cluster, BackendThread(lambda: Backend(cluster.endpoints)):
            client = Client(cluster.endpoints, type=peers.WEBSITE)
            try:
                with self.assertRaises(BackendError) as raised:
                    client.query(b"raise oddly", REQUEST, timeout=5)
                self.assertIn(b"\\ud800", raised.exception.args[0])
            finally:
                client.shutdown()

    def test_the_replies_it_sends_itself_are_queued(self) -> None:
        """The echo of a PING and of a search, the report of a handler's
        exception and a pickle reply, each counted as it is sent: none
        flushes, where each waited for its write and held the loop."""
        with Cluster(1, rules=RULES) as cluster, BackendThread(lambda: Backend(cluster.endpoints)) as served:
            backend = served.backend
            assert backend is not None
            flushed: list[bool] = []
            send_message = backend.conn.send_message

            def counting(*args, **kwargs):
                flushed.append(bool(kwargs.get("flush")))
                return send_message(*args, **kwargs)

            backend.conn.send_message = counting
            client = Client(cluster.endpoints, type=peers.WEBSITE)
            try:
                to = backend.conn.instance_id
                ping = client.send_message(b"echo", type=types.PING, to=to, flush=True)
                self.assertEqual(b"echo", reply_to(client, ping).message)
                search = client.send_message(b"search", type=types.BACKEND_FOR_PACKET_SEARCH, to=to, flush=True)
                self.assertEqual(types.PING, reply_to(client, search).type)
                with self.assertRaises(BackendError):
                    client.query(b"raise", REQUEST, timeout=5)
                self.assertEqual({"answer": 42}, pickle.loads(client.query(b"pickle", REQUEST, timeout=5).message))
                self.assertGreaterEqual(len(flushed), 4)
                self.assertNotIn(True, flushed)
            finally:
                client.shutdown()

    def test_a_report_that_fails_leaves_the_backend_serving(self) -> None:
        with Cluster(1, rules=RULES) as cluster, BackendThread(lambda: Backend(cluster.endpoints)) as served:
            backend = served.backend
            assert backend is not None
            backend.failing_reports = True
            client = Client(cluster.endpoints, type=peers.WEBSITE)
            try:
                with self.assertRaises((OperationTimedOut, OperationFailed)):
                    client.query(b"raise", REQUEST, timeout=0.5)
                self.assertEqual(b"answer", client.query(b"answer", REQUEST, timeout=5).message, "still serving")
                with backend.lock:
                    exceptions = list(backend.exceptions)
                self.assertTrue(exceptions, "on_handler_exception() was told")
                self.assertTrue(all(isinstance(exc, RuntimeError) for exc in exceptions), exceptions)
            finally:
                client.shutdown()

    def test_what_periodic_task_sends_is_routed_by_its_type(self) -> None:
        with (
            Cluster(1, rules=RULES) as cluster,
            FakePeer(cluster, peers.TEST_EVENT_BACKEND) as listener,
            BackendThread(lambda: Backend(cluster.endpoints)),
        ):
            client = Client(cluster.endpoints, type=peers.WEBSITE)
            try:
                self.assertEqual(b"answer", client.query(b"answer", REQUEST, timeout=5).message)
                listener.wait_for(types.TEST_EVENT)
            finally:
                client.shutdown()

    def test_send_and_receive_takes_a_payload(self) -> None:
        with Cluster(1, rules=RULES) as cluster, BackendThread(lambda: Backend(cluster.endpoints)):
            client = Client(cluster.endpoints, type=peers.WEBSITE)
            try:
                reply, _ = client.send_and_receive(b"answer", type=REQUEST, timeout=5)
                self.assertEqual(b"answer", reply.message)
            finally:
                client.shutdown()


if __name__ == "__main__":
    unittest.main()
