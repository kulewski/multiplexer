"""The pickle convention end to end: query_pickle, blocking and with a callback,
send_pickle and a MultiplexerServer, over a real multiplexer."""

import os
import pickle
import queue
import subprocess
import threading
import time
import unittest

from multiplexer.clients import Client
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
                runfile("multiplexer.rules"),
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


if __name__ == "__main__":
    unittest.main()
