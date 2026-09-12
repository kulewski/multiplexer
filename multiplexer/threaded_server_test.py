"""BaseThreadedMultiplexerServer against a real multiplexer: serial order
with one worker, parallel handling with four, a reply from another thread
later, the search answered while every worker is busy unless told to
decline, a full queue dropping, a handler that raises, draining, and
leaving.
"""

import threading
import time
import unittest

from multiplexer.Multiplexer_pb2 import BackendForPacketSearch
from multiplexer.clients import BackendError, Client
from multiplexer.multiplexer_constants import peers, types
from multiplexer.mxclient import OperationTimedOut
from multiplexer.testing import BackendThread, Cluster, TestClient, wait_until
from multiplexer.threaded_client import ThreadedClient
from multiplexer.threaded_server import BaseThreadedMultiplexerServer, Request

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


class ThreadedServerTest(unittest.TestCase):
    """One multiplexer for the class, a server per test."""

    @classmethod
    def setUpClass(cls):
        cls.cluster = Cluster(1).__enter__()

    @classmethod
    def tearDownClass(cls):
        cls.cluster.__exit__(None, None, None)

    def serve(self, **kwargs) -> tuple[BackendThread, Scripted]:
        """A Scripted server on its own thread, registered; stopped at the end of the test."""
        served = BackendThread(lambda: Scripted(self.cluster.endpoints, **kwargs)).start()
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
            wait_until(lambda: server.pending == 1, 10, "the worker busy")
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

    def test_draining_declines_searches_and_finishes_the_queue(self):
        served, server = self.serve()
        with TestClient(self.cluster, peers.WEBSITE) as client:
            client.send(b"block", REQUEST)
            for index in range(3):
                client.send(b"event-%d" % index, REQUEST)
            wait_until(lambda: server.pending == 4, 10, "the worker busy and three waiting")
            server.start_draining()
            with self.assertRaises(OperationTimedOut):
                self.search(client.client, 1.0)
            server.release.set()
            served.stop()
        self.assertEqual([b"block", b"event-0", b"event-1", b"event-2"], server.handled, "the queue was finished")
        self.cluster.wait_for_peer_gone(peers.PYTHON_TEST_SERVER)

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


if __name__ == "__main__":
    unittest.main()
