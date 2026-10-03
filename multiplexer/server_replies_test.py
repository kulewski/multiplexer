"""What a BaseMultiplexerServer sends, against a real multiplexer: a reply
that raised is no answer, so the requester gets BACKEND_ERROR, where the
reply counted as sent before it was; a report of a handler's exception
that fails leaves the loop serving and on_handler_exception() told, where
it ended serve_forever(); what periodic_task() sends is routed by its
type, where it went to the last requester; the requester's
_send_and_receive() takes a payload, where it raised AttributeError; and
the replies the server sends itself, an echo, a report, a pickle reply,
are queued as in the C++ class, where each waited for its write; every
client raises the one BackendError for a BACKEND_ERROR reply; and the
replies to requests a connection brought that died before they were
handled all go through one new connection once its multiplexer is back,
which the multiplexer registers once, where each reply opened a
connection of its own to the address and closed the one before, a
registration per reply, and the requests each new connection brought
kept the loop going for good.
"""

import asyncio
import pickle
import queue
import re
import threading
import time
import unittest

from multiplexer.aio import AsyncClient
from multiplexer.clients import Client
from multiplexer.multiplexer_constants import peers, types
from multiplexer.mxclient import NotConnected, OperationFailed, OperationTimedOut
from multiplexer.servers import BackendError, BaseMultiplexerServer
from multiplexer.testing import BackendThread, Cluster, FakePeer, Mx, TestClient, wait_until
from multiplexer.testing import runfile
from multiplexer.threaded_client import ThreadedClient

RULES = runfile("tests/testing.rules")  # the file the constants were generated from

REQUEST = types.PYTHON_TEST_REQUEST
RESPONSE = types.PYTHON_TEST_RESPONSE
HELD = 5  # requests a connection brought that died before they were handled


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


class HoldingBackend(BaseMultiplexerServer):
    """Holds its first request until `release` is set, then answers every
    request with its payload upper-cased, the default way: through the
    connection the request came on. Notes its live connections at every
    turn of its loop, on its own thread, where its client may be asked."""

    def __init__(self, addresses: list[tuple[str, int]]):
        super().__init__(addresses, type=peers.PYTHON_TEST_SERVER)
        self.holding = threading.Event()  # the first request is being handled
        self.release = threading.Event()  # the test's: answer it
        self.turns = 0  # periodic_task() calls so far
        self.connections = -1  # live connections at the last of them

    def handle_message(self, mxmsg) -> None:
        """Wait for the release on the first request, 60 s at most, a
        failure detector's bound; answer each."""
        if not self.holding.is_set():
            self.holding.set()
            self.release.wait(60)
        self.send_message(message=mxmsg.message.upper(), type=RESPONSE)

    def periodic_task(self) -> None:
        """Note the live connections, then count the turn."""
        self.connections = self.conn.connections_count()
        self.turns += 1


def written_out(client: Client, timeout: float = 30.0) -> None:
    """Return once the multiplexer has written out what `client` sent
    before, to the peers it routed it to: a PING the client sends itself,
    routed after it, came back. The multiplexer handles a connection's
    frames one at a time, and writes a frame to a receiver's socket that
    has room before it handles the next one from the same connection, so
    what came before the PING reaches its receivers whatever becomes of
    the multiplexer afterwards. OperationTimedOut after `timeout`, a
    failure detector's bound."""
    marker = client.send_message(b"written out?", type=types.PING, to=client.instance_id, flush=True)
    deadline = time.monotonic() + timeout
    while client.read_message(timeout=max(0.0, deadline - time.monotonic())).id != marker:
        pass


def registrations(multiplexer: Mx, instance_id: int, since: int) -> int:
    """How many connections of the peer `instance_id` the multiplexer's log
    says it registered past `since`, a log_mark(); an "unregistered" line
    is not one."""
    with open(multiplexer.log_path, "rb") as log:
        log.seek(since)
        text = log.read().decode(errors="replace")
    return len(re.findall(r"\bregistered connection id=%d\b" % instance_id, text))


def replies(client: Client, sent: list[int], timeout: float = 30.0) -> dict[int, bytes]:
    """The payloads of the replies `client` reads to the messages `sent`,
    by the id each references, once every one came or `timeout` seconds
    passed, a failure detector's bound."""
    got: dict[int, bytes] = {}
    deadline = time.monotonic() + timeout
    while len(got) < len(sent):
        try:
            mxmsg = client.read_message(timeout=max(0.0, deadline - time.monotonic()))
        except OperationTimedOut:
            break
        if mxmsg.references in sent:
            got[mxmsg.references] = mxmsg.message
    return got


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

    def test_every_client_raises_the_one_backend_error(self) -> None:
        """A BACKEND_ERROR reply is the same BackendError on every client,
        the class multiplexer.clients names, where ThreadedClient and
        AsyncClient raised one of their own, which an `except` for that
        one missed."""
        with Cluster(1, rules=RULES) as cluster, BackendThread(lambda: Backend(cluster.endpoints)):
            with Client(cluster.endpoints, type=peers.WEBSITE) as client:
                with self.assertRaises(BackendError):
                    client.query(b"raise", REQUEST, timeout=10)
            with ThreadedClient(cluster.endpoints, type=peers.TEST_ACTIVE_CLIENT) as threaded:
                with self.assertRaises(BackendError):
                    threaded.query(b"raise", REQUEST, timeout=10)
                heard: queue.Queue = queue.Queue()
                threaded.query(b"raise", REQUEST, timeout=10, callback=heard.put)
                self.assertIsInstance(heard.get(timeout=10), BackendError)

            async def ask() -> None:
                async with AsyncClient(cluster.endpoints, peers.TEST_ACTIVE_CLIENT) as asynchronous:
                    await asynchronous.query(b"raise", REQUEST, timeout=10)

            with self.assertRaises(BackendError):
                asyncio.run(ask())

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
                reply, _ = client._send_and_receive(b"answer", type=REQUEST, timeout=5)
                self.assertEqual(b"answer", reply.message)
            finally:
                client.shutdown()

    def test_replies_to_what_a_dead_connection_brought_take_one_new_connection(self) -> None:
        """Requests a connection brought, still to handle when its
        multiplexer was killed and started again: their replies all go
        through one new connection, which the multiplexer registers once.
        Each reply opened a connection of its own to the old address and
        closed the one before: a registration per reply, and the requests
        each new connection brought would have kept that going. A literal
        address, as the releases that did so resolved a name to one; the
        name's case is connect_by_name_test.py's. Counted, not timed: the
        requests are in the backend's socket before the kill
        (written_out), and the multiplexer's log has its registrations."""
        with Cluster(1, rules=RULES) as cluster:
            multiplexer = cluster.mx[0]
            address = cluster.endpoints[0]
            with (
                BackendThread(lambda: HoldingBackend([address])) as served,
                TestClient(cluster, peers.WEBSITE) as requester,
            ):
                backend = served.backend
                assert backend is not None
                try:
                    sent = [requester.send(b"request 0", REQUEST)]
                    self.assertTrue(backend.holding.wait(30), "the backend took up the first request")
                    sent += [requester.send(b"request %d" % index, REQUEST) for index in range(1, HELD)]
                    written_out(requester.client)
                    since = multiplexer.log_mark()
                    multiplexer.kill()
                    multiplexer.start()
                    requester.client.disconnect(address)  # back at once, where its reconnect comes 3 s on
                    requester.client.connect(address)
                    cluster.wait_for_peer(peers.WEBSITE)
                finally:
                    backend.release.set()
                answered = replies(requester.client, sent)
                self.assertEqual(
                    1,
                    registrations(multiplexer, backend.conn.instance_id, since),
                    "the backend's connections the multiplexer registered since its restart",
                )
                self.assertEqual({request: b"REQUEST %d" % index for index, request in enumerate(sent)}, answered)
                turns = backend.turns
                wait_until(lambda: backend.turns > turns, 30, "a turn of the backend's loop after the replies")
                self.assertEqual(1, backend.connections, "the backend's live connections")


if __name__ == "__main__":
    unittest.main()
