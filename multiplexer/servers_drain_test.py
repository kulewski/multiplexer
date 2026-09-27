"""A draining BaseMultiplexerServer serves what it had already read: the
requests the synchronous client pulled off the socket while a reply was
being sent are handled before the connections close, whether the drain
ends on the multiplexer's confirmation or on its cap. Every request sent
is therefore either answered or, routed after the multiplexer applied the
drain routing, refused by the multiplexer with a delivery error: none
vanishes into a timeout.
"""

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


class Slow(BaseMultiplexerServer):
    """Answers every request after a pause, so that the rest queue up; drains when `leave` is set."""

    multiplexer_client_type = peers.PYTHON_TEST_SERVER

    def __init__(self, addresses, **kwargs):
        super().__init__(addresses, **kwargs)
        self.handled: list[bytes] = []
        self.leave = False

    def handle_message(self, mxmsg):
        self.handled.append(mxmsg.message)
        time.sleep(0.03)
        self.send_message(message=mxmsg.message.upper(), type=RESPONSE)

    def periodic_task(self):
        if self.leave and not self.draining:
            self.start_draining()


class ServersDrainTest(unittest.TestCase):
    """The queue the drain must not drop, on the confirmation and on the cap."""

    @classmethod
    def setUpClass(cls):
        cls.cluster = Cluster(1, rules=RULES).__enter__()

    @classmethod
    def tearDownClass(cls):
        cls.cluster.__exit__(None, None, None)

    def drain(self, drain_seconds: float, **kwargs) -> tuple[float, Slow]:
        """Twelve requests at once, a drain once they are on their way; how
        long the backend took to leave, and the backend. Every request has
        an outcome: a response from the backend, or a delivery error from
        the multiplexer for one routed after the drain."""
        served = BackendThread(lambda: Slow(self.cluster.endpoints, **kwargs), drain_seconds=drain_seconds).start()
        self.cluster.wait_for_peer(peers.PYTHON_TEST_SERVER)
        assert isinstance(served.backend, Slow)
        server = served.backend
        with TestClient(self.cluster, peers.WEBSITE) as client:
            sent = {client.send(b"r%d" % index, REQUEST) for index in range(REQUESTS)}
            wait_until(lambda: len(server.handled) >= 2, 10, "the first replies, with the rest read into the queue")
            started = time.time()
            server.leave = True
            wait_until(lambda: not served.running, 30, "serve_forever() returned")
            took = time.time() - started
            self.assertIsNone(served.error)
            responses = refused = 0
            deadline = time.time() + 5
            while responses + refused < REQUESTS and time.time() < deadline:
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
            self.assertEqual(len(server.handled), responses, "what the backend served was answered")
            self.assertGreaterEqual(len(server.handled), 2)
        self.cluster.wait_for_peer_gone(peers.PYTHON_TEST_SERVER)
        return took, server

    def test_a_drain_on_the_confirmation_serves_what_was_read(self):
        took, _ = self.drain(10)
        self.assertLess(took, 5, "it left on the confirmation, not at the cap")

    def test_a_drain_on_its_cap_serves_what_was_read(self):
        self.drain(0.2, drain_routing=Routing(any=False))

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
        under a flood twice as fast as it serves: it leaves at the end of its
        drain, having handled what it had read by then and refused what
        arrived later, where every reply's turn of the loop read more,
        handled in turn, so that it never left. Every request the flood sent
        has one outcome: a response, or a delivery error, the backend's or,
        once it is gone, the multiplexer's."""
        routing = Routing(any=False, all=False, last_resort=True)
        served = BackendThread(lambda: Slow(self.cluster.endpoints, drain_routing=routing), drain_seconds=0.3).start()
        self.cluster.wait_for_peer(peers.PYTHON_TEST_SERVER)
        assert isinstance(served.backend, Slow)
        server = served.backend
        with TestClient(self.cluster, peers.WEBSITE) as client:
            sent: set[int] = set()
            deadline = time.time() + 20
            while served.running and time.time() < deadline:  # the flood, until the backend has left
                sent.add(client.send(b"f%d" % len(sent), REQUEST))
                if len(server.handled) >= 2:
                    server.leave = True
                time.sleep(0.015)
            self.assertFalse(served.running, "serve_forever() went on under the flood")
            self.assertIsNone(served.error)
            responses = refused = 0
            waiting = set(sent)
            answers_by = time.time() + 10
            while waiting and time.time() < answers_by:
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
            self.assertEqual(len(sent), responses + refused, "none vanished into a timeout")
            self.assertEqual(len(server.handled), responses, "what the backend served was answered")
            self.assertGreater(refused, 0, "the flood outran it")
        self.cluster.wait_for_peer_gone(peers.PYTHON_TEST_SERVER)


if __name__ == "__main__":
    unittest.main()
