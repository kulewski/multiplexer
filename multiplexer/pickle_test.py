"""The pickle convention end to end: query_pickle, blocking and with a callback,
send_pickle and a MultiplexerServer, over a real multiplexer; and a
MultiplexerServer takes what every backend takes, its peer type from the
class and a drain routing."""

import os
import pickle
import queue
import subprocess
import threading
import time
import unittest

from multiplexer.clients import Client
from multiplexer.Multiplexer_pb2 import Routing
from multiplexer.multiplexer_constants import peers, types
from multiplexer.servers import MultiplexerServer
from multiplexer.threaded_client import ThreadedClient


def runfile(path: str) -> str:
    """A file of this repository inside the test's runfiles tree."""
    return os.path.join(os.environ["TEST_SRCDIR"], os.environ.get("TEST_WORKSPACE", "mx"), path)


class Doubler(MultiplexerServer):
    """Answers every pickled request with its value doubled."""

    def process_pickle(self, data):
        """Double the numbers, keep the rest."""
        return {key: value * 2 for key, value in data.items()}


class ClassTyped(Doubler):
    """A Doubler whose peer type is the class's, given no `type`: the one
    the rules send TEST_REQUEST_A to, which no other peer here has."""

    multiplexer_client_type = peers.TEST_BACKEND_A


class PickleTest(unittest.TestCase):
    """See the module docstring."""

    def setUp(self):
        """Start a multiplexer on a free port and a Doubler on a thread of its own."""
        self.port_file = os.path.join(os.environ["TEST_TMPDIR"], "mx.port")
        if os.path.exists(self.port_file):
            os.unlink(self.port_file)
        self.mx = subprocess.Popen(
            [
                runfile("mxcontrol/mxcontrol"),
                "run_multiplexer",
                "--address",
                "127.0.0.1:0",
                "--rules",
                runfile("tests/testing.rules"),
                "--port-file",
                self.port_file,
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        deadline = time.time() + 15
        while not os.path.exists(self.port_file):
            self.assertLess(time.time(), deadline, "multiplexer did not start")
            time.sleep(0.02)
        with open(self.port_file) as port_file:
            host, port = port_file.read().strip().rsplit(":", 1)
        self.endpoint = (host, int(port))
        self.backend = Doubler([self.endpoint], type=peers.PYTHON_TEST_SERVER)
        self.backend.connect()  # registered before the first query, however late the thread below runs
        self.backend_thread = threading.Thread(target=self.backend.serve_forever, kwargs={"poll": 0.1}, daemon=True)
        self.backend_thread.start()

    def tearDown(self):
        """Stop the backend and the multiplexer."""
        self.backend.stop()
        self.backend_thread.join(10)
        self.mx.terminate()
        self.mx.wait(10)

    def test_synchronous_query_pickle(self) -> None:
        """A Client sends a pickle and gets the doubled dict back, unpickled."""
        client = Client([self.endpoint], type=peers.WEBSITE)
        self.assertEqual({"a": 2, "b": 4}, client.query_pickle({"a": 1, "b": 2}, type=types.PYTHON_TEST_REQUEST))
        client.shutdown()

    def test_threaded_query_pickle_and_async(self) -> None:
        """The same through a ThreadedClient, blocking and with a callback."""
        client = ThreadedClient([self.endpoint], type=peers.WEBSITE)
        self.assertEqual({"x": 6}, client.query_pickle({"x": 3}, type=types.PYTHON_TEST_REQUEST))
        results: queue.Queue = queue.Queue()
        client.query_pickle({"y": 5}, type=types.PYTHON_TEST_REQUEST, callback=results.put)
        self.assertEqual({"y": 10}, results.get(timeout=10))
        client.shutdown()

    def test_send_pickle_reaches_on_message(self) -> None:
        """send_pickle() to a peer's instance id arrives as bytes that unpickle."""
        incoming: queue.Queue = queue.Queue()
        receiver = ThreadedClient([self.endpoint], type=peers.WEBSITE, on_message=incoming.put)
        sender = ThreadedClient([self.endpoint], type=peers.WEBSITE)
        sender.send_pickle(["an", "event"], type=types.PYTHON_TEST_REQUEST, to=receiver.instance_id, flush=True)
        self.assertEqual(["an", "event"], pickle.loads(incoming.get(timeout=10).message))
        sender.shutdown()
        receiver.shutdown()

    def test_the_peer_type_may_be_the_class_s(self) -> None:
        """A MultiplexerServer subclass that sets multiplexer_client_type is
        built without `type`, as any backend may be, and registers as that
        type: a query the rules send to that type alone is answered."""
        backend = ClassTyped([self.endpoint])
        backend.connect()  # registered before the query
        serving = threading.Thread(target=backend.serve_forever, kwargs={"poll": 0.1}, daemon=True)
        serving.start()
        client = Client([self.endpoint], type=peers.WEBSITE)
        try:
            self.assertEqual({"a": 2}, client.query_pickle({"a": 1}, type=types.TEST_REQUEST_A))
        finally:
            client.shutdown()
            backend.stop()
            serving.join(10)

    def test_a_drain_routing_is_taken(self) -> None:
        """A MultiplexerServer takes the drain_routing any backend takes,
        what its start_draining() tells the multiplexers."""
        drain = Routing(any=False, all=False, last_resort=True)
        with Doubler([self.endpoint], type=peers.PYTHON_TEST_SERVER, drain_routing=drain) as backend:
            self.assertEqual(drain, backend.drain_routing)


if __name__ == "__main__":
    unittest.main()
