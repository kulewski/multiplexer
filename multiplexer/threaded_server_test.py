"""BaseThreadedMultiplexerServer against a real multiplexer: serial order
with one worker, parallel handling with four, a reply from another thread
later, the search answered while every worker is busy unless told to
decline, a full queue dropping, a queue of none still giving every worker
a request, a handler that raises, one whose
SystemExit, or an exception out of on_handler_exception(), ends
serve_forever(), draining, and leaving. What a request's reply is, as in C++: a reply that raised is no
answer, so the requester gets BACKEND_ERROR, where the request counted as
answered; a whole MultiplexerMessage is filled in from the request, where
it went out without `to` and `references`; and a report that fails is
logged, on_handler_exception() still told, where it ended the worker.
"""

import socket
import threading
import time
import unittest
from unittest import mock

from multiplexer.Multiplexer_pb2 import BackendForPacketSearch, DeliveryError, MultiplexerMessage, Routing
from multiplexer.clients import BackendError, Client
from multiplexer.multiplexer_constants import peers, types
from multiplexer.mxclient import NotConnected, OperationFailed, OperationTimedOut
from multiplexer.testing import BackendThread, Cluster, TestClient, wait_until
from multiplexer.testing.buffers import fill_frames, past_the_queue
from multiplexer.threaded_client import ThreadedClient
import multiplexer.threaded_server as threaded_server
from multiplexer.threaded_server import BaseThreadedMultiplexerServer, Request, _DropLines
from multiplexer.testing import runfile

RULES = runfile("tests/testing.rules")  # the file the constants were generated from

REQUEST = types.PYTHON_TEST_REQUEST
RESPONSE = types.PYTHON_TEST_RESPONSE
FILLER_TIMEOUT = (
    120  # seconds a message filling a connection may wait for room: none is given up on, however slow the fill
)


def shutting_down(client: ThreadedClient) -> bool:
    """Whether `client`'s shutdown() has begun: its calls raise
    NotConnected from then on."""
    try:
        client.connections_count()
    except NotConnected:
        return True
    return False


class Scripted(BaseThreadedMultiplexerServer):
    """A backend the tests steer by payload: "block" waits for `release`,
    "block, then close" calls close() after that, "block, then exit" raises
    SystemExit(3) after that, "wait" joins a barrier,
    "later" is answered by another thread, "raise"
    raises, "fail to reply" replies with a field that does not exist,
    "whole" replies with a MultiplexerMessage built without `to` and
    `references`, "shut down and raise" shuts the client down and raises,
    "event" gets no reply, anything else is upper-cased. `turns` counts
    the turns of the serving loop, the calls of periodic_task()."""

    multiplexer_client_type = peers.PYTHON_TEST_SERVER

    def __init__(self, addresses, **kwargs):
        super().__init__(addresses, **kwargs)
        self.handled: list[bytes] = []
        self.release = threading.Event()
        self.barrier = threading.Barrier(4, timeout=10)
        self.kept: list[Request] = []
        self.keep_serving_after_error = True
        self.raise_from_on_handler_exception = False
        self.exceptions: list[Exception] = []
        self.turns = 0

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
        elif payload == b"raise oddly":
            raise ValueError("unknown name \ud800")  # a lone surrogate, as echoed from a request
        elif payload == b"fail to reply":
            request.reply(b"answer", type=RESPONSE, no_such_field=1)  # raises where it is built
        elif payload == b"whole":
            request.reply(MultiplexerMessage(type=RESPONSE, message=b"WHOLE"))
        elif payload == b"shut down and raise":
            self.client.shutdown(timeout=0)
            raise RuntimeError("after the client")
        elif payload == b"close":
            self.close()  # from a worker: an error, not a deadlock
        elif payload == b"block, then close":
            self.release.wait(10)
            self.close()  # from a worker while another thread closes: an error too
        elif payload == b"block, then exit":
            self.release.wait(10)
            raise SystemExit(3)
        elif payload.startswith(b"event"):
            request.no_response()
        else:
            request.reply(payload.upper(), type=RESPONSE)

    def on_handler_exception(self, exc: Exception) -> bool:
        self.exceptions.append(exc)
        if self.raise_from_on_handler_exception:
            raise RuntimeError("from on_handler_exception")
        return self.keep_serving_after_error

    def periodic_task(self) -> None:
        """Counts a turn of the serving loop."""
        self.turns += 1


class Choking(BaseThreadedMultiplexerServer):
    """A peer whose handler takes every message for a request of its own
    and reports anything else with BACKEND_ERROR itself, a reply to it: a
    DELIVERY_ERROR it gets is answered that way. (The library sends no
    report of its own for a message that answers another.)"""

    multiplexer_client_type = peers.WEBSITE

    def __init__(self, addresses, **kwargs):
        super().__init__(addresses, **kwargs)
        self.refusals = 0

    def handle_message(self, request: Request) -> None:
        if request.mxmsg.type == types.DELIVERY_ERROR:
            self.refusals += 1
        request.report_error("not a request of mine")


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
        self.cluster.wait_for_peer(kwargs.get("type", peers.PYTHON_TEST_SERVER))
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
        return client._send_and_receive(message, multiplexer=Client.ALL, handle_delivery_errors=True, timeout=timeout)[
            0
        ]

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
            # In the handler, not merely queued, so that the queue has its two
            # places for the events: pending counts a request still queued too.
            wait_until(lambda: server.handled == [b"block"], 10, "the worker in the handler")
            for index in range(5):
                client.send(b"event-%d" % index, REQUEST, multiplexer=lane)
            # A flushing send only says the bytes left; the drops say the rest arrived.
            wait_until(lambda: server.pending == 3 and server.dropped == 3, 10, "two waiting, three dropped")
            server.release.set()
            # The queue drained first, so that the marker finds a place in it.
            wait_until(lambda: server.pending == 0, 10, "the events handled")
            self.assertEqual(b"MARKER", client.query(b"marker", REQUEST, multiplexer=lane).message)
        self.assertEqual([b"block", b"event-0", b"event-1", b"marker"], server.handled)

    def test_a_queue_of_none_lets_every_worker_take_a_request(self):
        # queue_size counts what waits for a worker, and a request an idle
        # worker is about to take waits for none, where it was counted and
        # queue_size=0 dropped every request.
        _, server = self.serve(workers=2, queue_size=0)
        with TestClient(self.cluster, peers.WEBSITE) as client:
            client.send(b"block", REQUEST)
            client.send(b"block", REQUEST)
            wait_until(lambda: server.handled == [b"block", b"block"], 10, "both workers in their handlers")
            client.send(b"event-dropped", REQUEST)
            wait_until(lambda: server.dropped == 1, 10, "the third dropped")
            server.release.set()
            wait_until(lambda: server.pending == 0, 10, "both workers idle again")
            self.assertEqual(b"MARKER", client.query(b"marker", REQUEST).message)
        self.assertEqual([b"block", b"block", b"marker"], server.handled)

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

    def test_a_handlers_system_exit_ends_serve_forever_and_the_queue_is_refused(self):
        """SystemExit from a handler is not the worker's to swallow:
        serve_forever() raises it, as the plain server's does, and the
        request queued behind it, which no worker is left to take, is
        refused at once. The worker used to die alone, the server serving
        on, registered and answering searches, handling nothing."""
        served, server = self.serve()
        with TestClient(self.cluster, peers.WEBSITE) as client:  # one multiplexer: one connection, in order
            client.send(b"block, then exit", REQUEST)
            wait_until(lambda: server.pending == 1, 10, "the worker busy")

            def release_once_queued() -> None:
                wait_until(lambda: server.pending == 2, 10, "the request queued behind it")
                server.release.set()

            releasing = threading.Thread(target=release_once_queued)
            releasing.start()
            message = client.client.new_message(message=b"after", type=REQUEST)
            reply = client.client._send_and_receive(message, timeout=5)[0]
            releasing.join()
            self.assertEqual(types.DELIVERY_ERROR, reply.type, "refused, not left waiting")
            self.assertEqual(message.id, reply.references)
        wait_until(lambda: not served.running, 10, "serve_forever() to end")
        with self.assertRaises(SystemExit) as raised:
            served.stop()
        self.assertEqual(3, raised.exception.code)
        self.assertEqual([b"block, then exit"], server.handled)

    def test_an_exception_out_of_on_handler_exception_ends_serve_forever(self):
        """on_handler_exception() raising, after the requester heard
        BACKEND_ERROR: serve_forever() raises it, where the worker died
        and the server served on."""
        served, server = self.serve()
        server.raise_from_on_handler_exception = True
        with TestClient(self.cluster, peers.WEBSITE) as client:
            with self.assertRaises(BackendError):
                client.query(b"raise", REQUEST)
        wait_until(lambda: not served.running, 10, "serve_forever() to end")
        with self.assertRaisesRegex(RuntimeError, "from on_handler_exception"):
            served.stop()

    def test_an_error_text_utf8_cannot_carry_still_reaches_the_requester(self):
        """A lone surrogate in the handler's exception: BACKEND_ERROR with it
        escaped, where the report failed to encode and the requester waited
        out its timeout."""
        self.serve()
        with TestClient(self.cluster, peers.WEBSITE) as client:
            with self.assertRaises(BackendError) as raised:
                client.query(b"raise oddly", REQUEST, timeout=5)
            self.assertIn(b"\\ud800", raised.exception.args[0])

    def test_a_reply_that_raised_is_answered_with_backend_error(self):
        self.serve()
        with TestClient(self.cluster, peers.WEBSITE) as client:
            with self.assertRaises(BackendError):
                client.query(b"fail to reply", REQUEST, timeout=3)

    def test_a_whole_message_reply_is_filled_in_from_the_request(self):
        self.serve()
        with TestClient(self.cluster, peers.WEBSITE) as client:
            self.assertEqual(b"WHOLE", client.query(b"whole", REQUEST, timeout=3).message)

    def test_a_report_that_fails_still_tells_on_handler_exception(self):
        _, server = self.serve()
        with TestClient(self.cluster, peers.WEBSITE) as client:
            client.send(b"shut down and raise", REQUEST)
        wait_until(lambda: server.exceptions, 10, "on_handler_exception() told")
        self.assertIsInstance(server.exceptions[0], RuntimeError)

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
            reply = client.client._send_and_receive(message, timeout=5)[0]
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
            reply = client.client._send_and_receive(message, timeout=5)[0]
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
        start a loop. The peer here answers what it does not expect with a
        BACKEND_ERROR of its own, so the refusal of its event comes back to
        the server as that reply; refused in turn, it would bring another
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
        """A reply still being written when close() begins reaches the
        requester: close() writes out what was sent before it, `timeout`
        seconds at most, and only then shuts the sockets. The multiplexer is
        frozen while the reply, behind more than the two sockets between
        them hold, events nobody takes, is sent and close() begins, and runs
        again once the client's shutdown is under way: the reply arrives
        after close() began, where a close that shut the sockets at once
        cut it off. A reply with nothing before it is written before close()
        gets there, and passes either way."""
        _, server = self.serve()
        with TestClient(self.cluster, peers.WEBSITE) as client:
            request = client.send(b"later", REQUEST)
            wait_until(lambda: server.kept, 10, "the request kept for a later answer")
            threaded = server.client  # the server's until close() is done
            payload = b"r" * 1024
            closing = threading.Thread(target=server.close, args=(60,))  # a write-out deadline only a failure reaches
            multiplexer = self.cluster.mx[0]
            multiplexer.pause()
            try:
                for filler in fill_frames():  # more than the sockets hold, ahead of the reply
                    threaded.send_message(filler, type=types.TEST_UNROUTED, multiplexer=threaded.ONE)
                server.kept[0].reply(payload, type=RESPONSE)
                self.assertFalse(threaded.flush_all(0), "the reply written at once: nothing left for close()")
                closing.start()
                wait_until(lambda: shutting_down(threaded), 10, "close() at the client's shutdown")
            finally:
                multiplexer.resume()
                if closing.is_alive():
                    closing.join()
            try:
                reply = client.receive(timeout=10)
            except OperationTimedOut:
                self.fail("the reply cut off by the close")
            self.assertEqual((request, RESPONSE, len(payload)), (reply.references, reply.type, len(reply.message)))

    def test_a_drain_ends_when_confirmed_and_the_queue_is_empty(self):
        """The drain ends once the multiplexer confirmed the routing and the
        workers finished the queue, not before: confirmed while "block"
        holds the worker, it goes on turn after turn of the loop, and a
        request addressed to the server, which the drain routing still
        delivers, is queued behind and served, where the drain ended on the
        confirmation alone and the close refused it. With no cap nothing
        else ends the drain: serve_forever() returns once the queue is
        empty, every request handled and nothing refused."""
        served, server = self.serve(drain_seconds=-1)  # no cap
        with TestClient(self.cluster, peers.WEBSITE) as client:
            client.send(b"block", REQUEST)
            for index in range(3):
                client.send(b"event-%d" % index, REQUEST)
            wait_until(lambda: server.pending == 4, 10, "the worker busy and three waiting")
            server.start_draining()
            wait_until(server.client.routing_acknowledged, 10, "the multiplexer confirmed the drain routing")
            # Two turns of the loop from here: drained() was asked at least
            # once since the confirmation, and said no.
            turns = server.turns
            wait_until(lambda: server.turns >= turns + 2, 5, "the drain going on with the worker busy")
            addressed = client.send(b"addressed", REQUEST, to=server.instance_id)
            # Queued behind "block", or refused by a server that took its drain for over.
            wait_until(lambda: server.pending == 5 or server.dropped, 10, "the addressed request queued")
            server.release.set()
            reply = client.receive(timeout=10)
            self.assertEqual((addressed, RESPONSE), (reply.references, reply.type), "served, not refused")
            wait_until(lambda: not served.running, 10, "serve_forever() returned with the queue empty")
        self.assertEqual([b"block", b"event-0", b"event-1", b"event-2", b"addressed"], server.handled)
        self.assertEqual(0, server.dropped)
        self.assertIsNone(served.error)

    def test_a_drain_keeping_a_path_open_lasts_its_period(self):
        """With `all` kept on, work may keep arriving, so the drain runs to
        its cap: an event the multiplexer sends every backend of the type,
        sent after it confirmed the drain routing, is still handled, which
        a closing server would refuse, and serve_forever() returns once the
        cap has passed since the drain began, on the monotonic clock. The
        cap is far above what the confirmation and the event take, where
        the check slept 0.5 s of a 1.5 s cap and a slow confirmation
        failed it."""
        cap = 5.0
        served, server = self.serve(drain_seconds=cap, drain_routing=Routing(any=False), type=peers.TEST_EVENT_BACKEND)
        started = time.monotonic()
        server.start_draining()
        wait_until(server.client.routing_acknowledged, 10, "the multiplexer confirmed")
        with TestClient(self.cluster, peers.WEBSITE) as client:
            client.send(b"event", types.TEST_EVENT)
            wait_until(lambda: b"event" in server.handled, cap, "the event through the path kept open, handled")
        wait_until(lambda: not served.running, cap + 10, "serve_forever() returned at the cap")
        self.assertGreaterEqual(time.monotonic() - started, cap, "not before the drain's deadline")

    def test_the_routing_reaches_a_multiplexer_past_a_full_queue(self):
        """A saturated backend's PEER_CONTROL is forced past its full outgoing
        queue: with the multiplexer frozen and the queue full, whatever the
        machine's socket buffers, the routing is still confirmed once the
        multiplexer is back."""
        _, server = self.serve()
        multiplexer = self.cluster.mx[0]
        chunk = b"x" * 8192
        multiplexer.pause()
        try:
            # The sockets' buffers, then the 1024 the queue holds, then the
            # rest waits for room behind them; the routing request comes
            # after all of it on the io thread, and finds the queue full.
            for payload in past_the_queue(chunk):
                server.client.send_message(payload, type=9999, multiplexer=server.client.ONE, timeout=FILLER_TIMEOUT)
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

    def test_close_from_a_handler_during_another_close_is_an_error_not_a_deadlock(self):
        """A worker that calls close() while another thread's close() joins
        it raises, as from a handler at any time, rather than wait for the
        close() that waits for it."""
        served, server = self.serve()
        with TestClient(self.cluster, peers.WEBSITE) as client:
            client.send(b"block, then close", REQUEST)
            wait_until(lambda: server.pending == 1, 10, "the request with a worker")
            closing = threading.Thread(target=server.close, daemon=True)
            closing.start()
            wait_until(lambda: server.draining, 10, "close() under way")
            server.release.set()
            closing.join(10)
            self.assertFalse(closing.is_alive(), "close() waited for the worker, which waited for it")
        self.assertEqual(["RuntimeError"], [type(exc).__name__ for exc in server.exceptions])
        wait_until(lambda: not served.running, 10, "serve_forever() returned")
        self.assertIsNone(served.error)

    def test_close_from_another_thread_ends_serve_forever_quietly(self):
        """close() on another thread while serve_forever() polls, with the
        multiplexer frozen so that the client's shutdown lasts its round
        trip: serve_forever() returns, where its poll asked the client being
        shut down whether the drain was confirmed, and NotConnected escaped."""
        served, server = self.serve(drain_seconds=30)  # a drain that only the confirmation, frozen, could end
        multiplexer = self.cluster.mx[0]
        multiplexer.pause()
        try:
            server.close()
        finally:
            multiplexer.resume()
        wait_until(lambda: not served.running, 10, "serve_forever() returned")
        self.assertIsNone(served.error)

    def test_a_close_while_serve_forever_connects_ends_it_quietly(self):
        """close() on another thread while serve_forever() still connects,
        to a multiplexer that never answers: the connect under way ends, the
        next is not made, and serve_forever() returns, where the next
        connect raised NotConnected out of it."""
        outcome: list[BaseException | None] = []
        with socket.create_server(("127.0.0.1", 0)) as silent:
            server = Scripted([silent.getsockname()[:2]] + list(self.cluster.endpoints), timeout=30)

            def serve() -> None:
                try:
                    server.serve_forever(0.05)
                    outcome.append(None)
                except BaseException as error:  # what the test reports
                    outcome.append(error)

            thread = threading.Thread(target=serve, daemon=True)
            thread.start()
            silent.settimeout(10)
            accepted, _ = silent.accept()  # the first connect is under way, waiting for a welcome
            with accepted:
                server.close()
                thread.join(10)
        self.assertEqual([None], outcome)

    def test_a_request_arriving_during_the_closes_write_out_is_refused(self):
        """A request addressed to the server while close() writes out what
        was sent before: refused with a delivery error, so that its sender
        retries elsewhere at once, where the refusal threw NotConnected on
        the io thread and the sender waited out its timeout. The first
        multiplexer, paused with copies of a send to every multiplexer
        queued for it, holds the write-out open; the request comes through
        the second."""
        with Cluster(2, rules=RULES) as cluster:
            served = BackendThread(lambda: Scripted(cluster.endpoints)).start()
            assert isinstance(served.backend, Scripted)
            server = served.backend
            requester = Client([cluster.endpoints[1]], type=peers.WEBSITE)
            first = cluster.mx[0]
            first.pause()
            try:
                for payload in fill_frames():  # past the sockets, the rest queued
                    server.client.send_message(
                        payload, type=9999, multiplexer=server.client.ALL, report_delivery_error=False
                    )
                closing = threading.Thread(target=lambda: server.close(10), daemon=True)
                closing.start()

                def shutting_down() -> bool:
                    try:
                        server.client.connections_count()
                        return False
                    except NotConnected:
                        return True

                wait_until(shutting_down, 10, "the client's shutdown began")
                request = requester.new_message(message=b"late", type=REQUEST, to=server.instance_id)
                requester.send_message(request, flush=True)
                answer = None
                deadline = time.time() + 5
                while answer is None and time.time() < deadline:
                    try:
                        received, _ = requester.receive_message(timeout=0.5)
                    except OperationTimedOut:
                        continue
                    if received.references == request.id:
                        answer = received
                self.assertIsNotNone(answer, "no refusal: it threw NotConnected on the io thread")
                assert answer is not None
                self.assertEqual(types.DELIVERY_ERROR, answer.type)
            finally:
                first.resume()
                requester.shutdown()
            closing.join(10)
            wait_until(lambda: not served.running, 10, "serve_forever() returned")
            self.assertIsNone(served.error)

    def test_stop_from_a_handler_and_the_instance_id(self):
        served, server = self.serve()
        with TestClient(self.cluster, peers.WEBSITE) as client:
            reply = client.query(b"who", REQUEST, to=server.instance_id)
            self.assertEqual((b"WHO", server.instance_id), (reply.message, reply.sender))
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
