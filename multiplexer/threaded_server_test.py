"""BaseThreadedMultiplexerServer against a real multiplexer: serial order
with one worker, parallel handling with four, a reply from another thread
later, the search answered while every worker is busy unless told to
decline, a full queue dropping, a handler that raises, draining, and
leaving.
"""

import threading
import time
import unittest
from unittest import mock

from multiplexer.Multiplexer_pb2 import BackendForPacketSearch, DeliveryError, MultiplexerMessage, Routing
from multiplexer.clients import BackendError, Client
from multiplexer.multiplexer_constants import peers, types
from multiplexer.mxclient import OperationFailed, OperationTimedOut
from multiplexer.testing import BackendThread, Cluster, TestClient, wait_until
from multiplexer.threaded_client import ThreadedClient
import multiplexer.threaded_server as threaded_server
from multiplexer.threaded_server import BaseThreadedMultiplexerServer, Request, _DropLines
from multiplexer.testing import runfile

RULES = runfile("tests/testing.rules")  # the file the constants were generated from

REQUEST = types.PYTHON_TEST_REQUEST
RESPONSE = types.PYTHON_TEST_RESPONSE


class Scripted(BaseThreadedMultiplexerServer):
    """A backend the tests steer by payload: "block" waits for `release`,
    "wait" joins a barrier, "later" is answered by another thread, "raise"
    raises, "event" gets no reply, anything else is upper-cased."""

    multiplexer_client_type = peers.PYTHON_TEST_SERVER

    def __init__(self, addresses, **kwargs):
        super().__init__(addresses, **kwargs)
        self.handled: list[bytes] = []
        self.release = threading.Event()
        self.barrier = threading.Barrier(4, timeout=10)
        self.kept: list[Request] = []
        self.keep_serving_after_error = True

    def handle_message(self, request: Request) -> None:
        payload = request.mxmsg.message
        self.handled.append(payload)
        if payload == b"block":
            self.release.wait(10)
            request.no_response()
        elif payload == b"wait":
            self.barrier.wait()
            request.reply(b"passed", type=RESPONSE)
        elif payload == b"later":
            self.kept.append(request)  # answered by the test, from its thread
        elif payload == b"raise":
            raise ValueError("as asked")
        elif payload == b"close":
            self.close()  # from a worker: an error, not a deadlock
        elif payload.startswith(b"event"):
            request.no_response()
        else:
            request.reply(payload.upper(), type=RESPONSE)

    def on_handler_exception(self, exc: Exception) -> bool:
        return self.keep_serving_after_error


class Choking(BaseThreadedMultiplexerServer):
    """A peer whose handler takes every message for a request of its own
    and raises on anything else, as one that parses every payload does:
    a DELIVERY_ERROR it gets is answered with BACKEND_ERROR."""

    multiplexer_client_type = peers.WEBSITE

    def __init__(self, addresses, **kwargs):
        super().__init__(addresses, **kwargs)
        self.refusals = 0

    def handle_message(self, request: Request) -> None:
        if request.mxmsg.type == types.DELIVERY_ERROR:
            self.refusals += 1
        raise ValueError("not a request of mine")


class ThreadedServerTest(unittest.TestCase):
    """One multiplexer for the class, a server per test."""

    @classmethod
    def setUpClass(cls):
        cls.cluster = Cluster(1, rules=RULES).__enter__()

    @classmethod
    def tearDownClass(cls):
        cls.cluster.__exit__(None, None, None)

    def serve(self, drain_seconds: float = 0.0, **kwargs) -> tuple[BackendThread, Scripted]:
        """A Scripted server on its own thread, registered; stopped at the end of the test."""
        served = BackendThread(lambda: Scripted(self.cluster.endpoints, **kwargs), drain_seconds=drain_seconds).start()
        self.cluster.wait_for_peer(peers.PYTHON_TEST_SERVER)
        self.addCleanup(lambda: served.stop() if served.running else None)
        assert isinstance(served.backend, Scripted)
        return served, served.backend

    def test_one_worker_handles_in_arrival_order_and_answers(self):
        _, server = self.serve()
        with TestClient(self.cluster, peers.WEBSITE) as client:
            lane = client.lane()
            for index in range(100):
                client.send(b"event-%03d" % index, REQUEST, multiplexer=lane)
            self.assertEqual(b"HELLO", client.query(b"hello", REQUEST, multiplexer=lane).message)
        self.assertEqual([b"event-%03d" % index for index in range(100)] + [b"hello"], server.handled)
        wait_until(lambda: server.pending == 0, 10, "the worker done with the query it answered")

    def test_four_workers_handle_four_requests_at_once(self):
        _, server = self.serve(workers=4)
        client = ThreadedClient(self.cluster.endpoints, type=peers.PYTHON_TEST_CLIENT)
        try:
            replies = []
            done = threading.Semaphore(0)
            for _ in range(4):
                client.query(
                    b"wait", REQUEST, timeout=10, callback=lambda result: (replies.append(result), done.release())
                )
            for _ in range(4):
                self.assertTrue(done.acquire(timeout=15), "a request never came back: the barrier was not passed")
            self.assertEqual([b"passed"] * 4, [reply.message for reply in replies])
            self.assertFalse(server.barrier.broken)
        finally:
            client.shutdown()

    def test_a_request_may_be_answered_later_from_another_thread(self):
        _, server = self.serve()

        def answer_later():
            request = wait_until(lambda: server.kept and server.kept[0], 10, "the kept request")
            time.sleep(0.3)
            request.reply(b"finally", type=RESPONSE)

        thread = threading.Thread(target=answer_later)
        thread.start()
        with TestClient(self.cluster, peers.WEBSITE) as client:
            self.assertEqual(b"finally", client.query(b"later", REQUEST, timeout=10).message)
        thread.join()

    def search(self, client: Client, timeout: float):
        """A client's search for a backend of the request type, by hand: the reply or the exception."""
        query = BackendForPacketSearch()
        query.packet_type = REQUEST
        message = client.new_message(message=query, type=types.BACKEND_FOR_PACKET_SEARCH)
        return client.send_and_receive(message, multiplexer=Client.ALL, handle_delivery_errors=True, timeout=timeout)[0]

    def test_the_search_is_answered_while_every_worker_is_busy(self):
        _, server = self.serve()
        with TestClient(self.cluster, peers.WEBSITE) as client:
            client.send(b"block", REQUEST)
            client.send(b"event-queued", REQUEST)
            wait_until(lambda: server.pending == 2, 10, "the worker busy and one request waiting")
            self.assertEqual(types.PING, self.search(client.client, 5).type, "answered at once, from the io thread")
            server.release.set()
            self.assertEqual(b"AFTER", client.query(b"after", REQUEST).message)

    def test_declining_the_search_when_full_is_an_option(self):
        _, server = self.serve(decline_searches_when_full=True)
        with TestClient(self.cluster, peers.WEBSITE) as client:
            client.send(b"block", REQUEST)
            # In the handler, not merely queued: a queued request with the
            # worker still on its way to it is "nothing waiting" too.
            wait_until(lambda: server.handled == [b"block"], 10, "the worker in the handler")
            self.assertEqual(types.PING, self.search(client.client, 5).type, "busy but nothing waiting: answered")
            client.send(b"event-queued", REQUEST)
            wait_until(lambda: server.pending == 2, 10, "one request waiting")
            with self.assertRaises(OperationTimedOut):
                self.search(client.client, 1.0)
            server.release.set()
            self.assertEqual(b"AFTER", client.query(b"after", REQUEST).message)
            self.assertEqual(types.PING, self.search(client.client, 5).type, "idle again: answered")

    def test_a_full_queue_drops_and_the_rest_is_handled_in_order(self):
        _, server = self.serve(queue_size=2)
        with TestClient(self.cluster, peers.WEBSITE) as client:
            lane = client.lane()
            client.send(b"block", REQUEST, multiplexer=lane)
            wait_until(lambda: server.pending == 1, 10, "the worker busy")
            for index in range(5):
                client.send(b"event-%d" % index, REQUEST, multiplexer=lane)
            # A flushing send only says the bytes left; the drops say the rest arrived.
            wait_until(lambda: server.pending == 3 and server.dropped == 3, 10, "two waiting, three dropped")
            server.release.set()
            self.assertEqual(b"MARKER", client.query(b"marker", REQUEST, multiplexer=lane).message)
        self.assertEqual([b"block", b"event-0", b"event-1", b"marker"], server.handled)

    def test_a_raising_handler_reports_backend_error_and_serves_on(self):
        served, server = self.serve()
        with TestClient(self.cluster, peers.WEBSITE) as client:
            with self.assertRaises(BackendError) as raised:
                client.query(b"raise", REQUEST)
            self.assertIn(b"as asked", raised.exception.args[0])
            self.assertEqual(b"STILL", client.query(b"still", REQUEST).message)
            server.keep_serving_after_error = False
            with self.assertRaises(BackendError):
                client.query(b"raise", REQUEST)
        with self.assertRaises(ValueError):
            served.stop()  # serve_forever() raised what the handler raised

    def test_draining_takes_the_backend_out_of_routing_and_finishes_the_queue(self):
        """start_draining() tells the multiplexer to route nothing new by
        the rules: a request and a search sent after the multiplexer
        confirmed come back as DELIVERY_ERROR from the multiplexer itself,
        the queue is finished, and the server leaves."""
        served, server = self.serve()
        with TestClient(self.cluster, peers.WEBSITE) as client:
            client.send(b"block", REQUEST)
            for index in range(3):
                client.send(b"event-%d" % index, REQUEST)
            wait_until(lambda: server.pending == 4, 10, "the worker busy and three waiting")
            server.start_draining()
            wait_until(server.client.routing_acknowledged, 10, "the multiplexer confirmed the drain routing")
            message = client.client.new_message(message=b"late", type=REQUEST)
            reply = client.client.send_and_receive(message, timeout=5)[0]
            self.assertEqual(types.DELIVERY_ERROR, reply.type, "nobody takes it by the rules")
            self.assertEqual([peers.PYTHON_TEST_SERVER], list(DeliveryError.FromString(reply.message).failed_type))
            self.assertEqual(types.DELIVERY_ERROR, self.search(client.client, 5.0).type, "the search is not forwarded")
            server.release.set()
            served.stop()
        self.assertEqual([b"block", b"event-0", b"event-1", b"event-2"], server.handled, "the queue was finished")
        self.assertEqual(0, server.dropped, "nothing had to be refused")
        self.cluster.wait_for_peer_gone(peers.PYTHON_TEST_SERVER)

    def test_a_request_arriving_while_leaving_is_refused_at_once(self):
        """close() with the worker still busy, on a server whose drain
        routing keeps every path open, as a request routed before the
        multiplexer applied the usual one would arrive: a request that
        arrives then is answered with DELIVERY_ERROR, the multiplexer's
        own "nobody there", so a query retries through the search at once
        instead of waiting out its timeout."""
        served, server = self.serve(drain_routing=Routing())
        with TestClient(self.cluster, peers.WEBSITE) as client:
            client.send(b"block", REQUEST)
            wait_until(lambda: server.pending == 1, 10, "the worker busy")
            closing = threading.Thread(target=server.close)
            closing.start()
            wait_until(lambda: server.draining, 10, "close() under way")
            message = client.client.new_message(message=b"late", type=REQUEST)
            reply = client.client.send_and_receive(message, timeout=5)[0]
            self.assertEqual(types.DELIVERY_ERROR, reply.type, "refused, not dropped")
            self.assertEqual(message.id, reply.references)
            self.assertEqual(message.id, DeliveryError.FromString(reply.message).packet_id)
            server.release.set()
            closing.join()
        self.assertEqual([b"block"], server.handled)
        self.assertEqual(1, server.dropped)
        self.cluster.wait_for_peer_gone(peers.PYTHON_TEST_SERVER)

    def test_a_closing_server_answers_no_search(self):
        """close() under way, on a server whose drain routing keeps the
        multiplexer offering it: a search goes unanswered, since the
        request that would follow is refused."""
        _, server = self.serve(drain_routing=Routing())
        with TestClient(self.cluster, peers.WEBSITE) as client:
            self.assertEqual(types.PING, self.search(client.client, 5).type, "serving: answered")
            client.send(b"block", REQUEST)
            wait_until(lambda: server.pending == 1, 10, "the worker busy")
            closing = threading.Thread(target=server.close)
            closing.start()
            wait_until(lambda: server.draining, 10, "close() under way")
            with self.assertRaises(OperationTimedOut):
                self.search(client.client, 1.0)
            server.release.set()
            closing.join()

    def test_a_reply_arriving_while_leaving_is_dropped_not_refused(self):
        """A message that answers another, arriving while the server is
        leaving, is dropped: nobody retries a reply, and refusing one could
        start a loop. The peer here raises on what it does not expect, so
        the refusal of its event comes back to the server as a
        BACKEND_ERROR, a reply; refused in turn, that would bring another
        DELIVERY_ERROR, and so on until the close ended."""
        _, server = self.serve()
        peer = BackendThread(lambda: Choking(self.cluster.endpoints)).start()
        self.addCleanup(lambda: peer.stop() if peer.running else None)
        choking = peer.backend
        assert isinstance(choking, Choking)
        with TestClient(self.cluster, peers.WEBSITE) as client:
            client.send(b"block", REQUEST)
            wait_until(lambda: server.pending == 1, 10, "the worker busy")
            closing = threading.Thread(target=server.close)
            closing.start()
            wait_until(lambda: server.draining, 10, "close() under way")
            choking.client.send_message(b"event-late", type=REQUEST, to=server.instance_id)
            wait_until(lambda: choking.refusals >= 1, 10, "the event refused")
            time.sleep(0.5)  # long enough for a loop to go round many times
            self.assertEqual(1, choking.refusals, "one refusal, not a loop")
            server.release.set()
            closing.join()
        self.assertEqual(2, server.dropped, "the event refused, the BACKEND_ERROR dropped")

    def test_routing_is_the_backends_to_turn_off_and_on(self):
        """set_routing() on the server's client, outside any drain: with
        `any` off a request by the rules fails at once, with everything on
        again it is served, and as the last resort it is served while the
        server is alone of its type."""
        _, server = self.serve()
        with TestClient(self.cluster, peers.WEBSITE) as client:
            self.assertEqual(b"FIRST", client.query(b"first", REQUEST, timeout=5).message)
            server.client.set_routing(Routing(any=False))
            wait_until(server.client.routing_acknowledged, 10, "the multiplexer confirmed")
            with self.assertRaises(OperationFailed):
                client.query(b"second", REQUEST, timeout=5)
            server.client.set_routing(Routing())
            wait_until(server.client.routing_acknowledged, 10, "the multiplexer confirmed again")
            self.assertEqual(b"THIRD", client.query(b"third", REQUEST, timeout=5).message)
            server.client.set_routing(Routing(any=False, all=False, last_resort=True))
            wait_until(server.client.routing_acknowledged, 10, "the multiplexer confirmed the last resort")
            self.assertEqual(b"FOURTH", client.query(b"fourth", REQUEST, timeout=5).message)
            self.assertEqual(types.PING, self.search(client.client, 5.0).type, "a search finds the last resort")
        self.assertEqual([b"first", b"third", b"fourth"], server.handled)

    def test_the_last_reply_is_written_before_the_close(self):
        """A reply sent just before close() reaches the requester: close()
        writes what is queued, up to a second, before it shuts the sockets."""
        served, server = self.serve()
        with TestClient(self.cluster, peers.WEBSITE) as client:
            client.send(b"later", REQUEST)
            wait_until(lambda: server.kept, 10, "the request kept for a later answer")
            server.kept[0].reply(b"at-the-last-moment", type=RESPONSE)
            served.stop()  # close() follows at once
            self.assertEqual(b"at-the-last-moment", client.receive(timeout=5).message)

    def test_a_drain_ends_when_confirmed_and_the_queue_is_empty(self):
        """With a 10 s cap, the drain ends as soon as the multiplexer confirmed
        the routing and the workers finished the queue: within a second,
        with every queued request handled and nothing refused."""
        served, server = self.serve(drain_seconds=10)
        with TestClient(self.cluster, peers.WEBSITE) as client:
            client.send(b"block", REQUEST)
            for index in range(3):
                client.send(b"event-%d" % index, REQUEST)
            wait_until(lambda: server.pending == 4, 10, "the worker busy and three waiting")
            started = time.time()
            server.start_draining()
            server.release.set()
            wait_until(lambda: not served.running, 10, "serve_forever() returned")
            self.assertLess(time.time() - started, 5, "on the confirmation and the empty queue, not the cap")
        self.assertEqual([b"block", b"event-0", b"event-1", b"event-2"], server.handled)
        self.assertEqual(0, server.dropped)
        self.assertIsNone(served.error)

    def test_a_drain_keeping_a_path_open_lasts_its_period(self):
        """With `all` kept on, work may keep arriving, so the drain runs to its cap."""
        served, server = self.serve(drain_seconds=1.5, drain_routing=Routing(any=False))
        started = time.time()
        server.start_draining()
        wait_until(server.client.routing_acknowledged, 10, "the multiplexer confirmed")
        time.sleep(0.5)
        self.assertTrue(served.running, "confirmed, but events may still come: the drain goes on")
        wait_until(lambda: not served.running, 10, "serve_forever() returned at the cap")
        self.assertGreaterEqual(time.time() - started, 1.4)

    def test_the_routing_reaches_a_multiplexer_past_a_full_queue(self):
        """A saturated backend's PEER_CONTROL is forced past its full outgoing
        queue: with the multiplexer frozen and the queue full, the routing
        is still confirmed once the multiplexer is back."""
        _, server = self.serve()
        multiplexer = self.cluster.mx[0]
        multiplexer.pause()
        try:
            for _ in range(1200):  # the socket buffers, then the 1024 the queue holds, then drops
                server.client.send_message(b"x" * 8192, type=9999, multiplexer=server.client.ONE)
            server.client.set_routing(Routing(any=False))
        finally:
            multiplexer.resume()
        wait_until(server.client.routing_acknowledged, 15, "the routing confirmed after the thaw")

    def test_close_from_a_handler_is_an_error_not_a_deadlock(self):
        served, server = self.serve()
        with TestClient(self.cluster, peers.WEBSITE) as client:
            with self.assertRaises(BackendError) as raised:
                client.query(b"close", REQUEST, timeout=10)
            self.assertIn(b"worker thread", raised.exception.args[0])
            self.assertEqual(b"STILL", client.query(b"still", REQUEST).message, "still serving")
        self.assertTrue(served.running)

    def test_stop_from_a_handler_and_the_instance_id(self):
        served, server = self.serve()
        with TestClient(self.cluster, peers.WEBSITE) as client:
            reply = client.query(b"who", REQUEST, to=server.instance_id)
            self.assertEqual((b"WHO", server.instance_id), (reply.message, reply.from_))
        server.stop()
        wait_until(lambda: not served.running, 10, "serve_forever() returned")
        self.cluster.wait_for_peer_gone(peers.PYTHON_TEST_SERVER)

    def test_nothing_is_connected_or_handled_before_serve_forever(self):
        server = Scripted(self.cluster.endpoints)  # built, as a subclass's __init__ leaves it
        self.addCleanup(server.close)
        self.assertTrue(server.instance_id, "the id is known before serving")
        self.assertEqual(0, server.client.connections_count(), "and nothing is connected")
        with TestClient(self.cluster, peers.WEBSITE) as client:
            server.connect()  # a program that announces itself before serving: the workers are up
            self.assertEqual(1, server.client.connections_count())
            self.cluster.wait_for_peer(peers.PYTHON_TEST_SERVER)
            self.assertEqual(types.PING, self.search(client.client, 5).type, "the search answered")
            thread = threading.Thread(target=lambda: server.serve_forever(0.05), daemon=True)  # connects nothing more
            thread.start()
            self.assertEqual(b"LATE", client.query(b"late", REQUEST).message)
        server.stop()
        thread.join(10)


class DropLinesTest(unittest.TestCase):
    """The lines about requests a full queue dropped, on a clock of the
    test's own: what each line stands for adds up to every drop."""

    def test_the_first_at_once_one_count_a_second_and_the_rest_when_taken(self) -> None:
        """Twenty-one drops within a second: one line, then one count of
        twenty; two more, then the queue takes a request: their count;
        and the next drop starts a burst of its own."""
        now = [100.0]
        said: list[str] = []
        lines = _DropLines(clock=lambda: now[0])
        request = MultiplexerMessage(id=7, type=types.PYTHON_TEST_REQUEST)
        with mock.patch.object(threaded_server, "log", lambda level, verbosity, text: said.append(text)):
            for _ in range(10):
                lines.dropped(request)
            now[0] += 0.5
            for _ in range(10):
                lines.dropped(request)
            self.assertEqual(["request #7 of type %d dropped: queue full" % types.PYTHON_TEST_REQUEST], said)
            now[0] += 0.5
            lines.dropped(request)  # a second after the first line: the count so far
            self.assertEqual("requests dropped: queue full [20 more in the last 1.0 s]", said[-1])
            now[0] += 0.3
            lines.dropped(request)
            lines.dropped(request)
            lines.accepted()  # the burst is over: its rest
            self.assertEqual("requests dropped: queue full [2 more in the last 0.3 s]", said[-1])
            lines.accepted()
            lines.dropped(request)  # a new burst starts with a line of its own
            self.assertEqual(4, len(said))
            self.assertTrue(said[-1].startswith("request #7"))


if __name__ == "__main__":
    unittest.main()
