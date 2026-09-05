"""ThreadedClient from Python: on_message, pings, and the io-thread rule.

Against a real multiplexer started for the test: a message addressed to the
client's instance id reaches on_message, a PING is answered by the client
itself, and the blocking query() raises RuntimeError from a callback.
"""

import os
import queue
import subprocess
import threading
import time
import unittest

from multiplexer.clients import Client
from multiplexer.multiplexer_constants import peers, types
from multiplexer.threaded_client import ThreadedClient


def runfile(path: str) -> str:
    """A file of this repository inside the test's runfiles tree."""
    return os.path.join(os.environ["TEST_SRCDIR"], os.environ.get("TEST_WORKSPACE", "mx"), path)


class ThreadedClientTest(unittest.TestCase):
    """See the module docstring."""

    def setUp(self):
        """Start a multiplexer on a free port."""
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

    def tearDown(self):
        """Stop the multiplexer."""
        self.mx.terminate()
        self.mx.wait(10)

    def test_on_message_gets_what_is_addressed_to_it(self) -> None:
        """A message sent to the client's instance id lands in on_message, here a queue."""
        incoming: queue.Queue = queue.Queue()
        client = ThreadedClient([self.endpoint], type=peers.WEBSITE, on_message=incoming.put)
        peer = Client([self.endpoint], type=peers.WEBSITE)
        peer.send_message(message=b"for you", type=types.PYTHON_TEST_REQUEST, to=client.instance_id, flush=True)
        mxmsg = incoming.get(timeout=5)
        self.assertEqual(b"for you", mxmsg.message)
        self.assertEqual(peer.instance_id, mxmsg.from_)
        client.shutdown()
        peer.shutdown()

    def test_ping_is_answered_by_the_client_itself(self) -> None:
        """A PING addressed to the client comes back as a PING referencing it, and on_message never sees it."""
        handed_on = []
        client = ThreadedClient([self.endpoint], type=peers.WEBSITE, on_message=handed_on.append)
        peer = Client([self.endpoint], type=peers.WEBSITE)
        ping_id = peer.send_message(message=b"echo me", type=types.PING, to=client.instance_id, flush=True)
        pong = peer.read_message(timeout=5)
        self.assertEqual(types.PING, pong.type)
        self.assertEqual(ping_id, pong.references)
        self.assertEqual(b"echo me", pong.message)
        self.assertEqual([], handed_on)
        client.shutdown()
        peer.shutdown()

    def test_blocking_query_from_a_callback_raises(self) -> None:
        """query() on the io thread, from a query callback, raises RuntimeError instead of deadlocking."""
        client = ThreadedClient([self.endpoint], type=peers.WEBSITE)
        outcome: queue.Queue = queue.Queue()

        def on_result(_result) -> None:
            """Try the forbidden call and report what happened."""
            try:
                client.query(b"nested", type=types.PYTHON_TEST_REQUEST, timeout=1)
                outcome.put("returned")
            except RuntimeError as error:
                outcome.put("RuntimeError: %s" % error)

        client.query(b"hello", type=types.PYTHON_TEST_REQUEST, callback=on_result, timeout=2)
        self.assertTrue(outcome.get(timeout=10).startswith("RuntimeError"))
        client.shutdown()

    def test_many_threads_query_at_once(self) -> None:
        """Twenty threads query one client at the same time; each gets its own outcome."""
        client = ThreadedClient([self.endpoint], type=peers.WEBSITE)
        outcomes: queue.Queue = queue.Queue()

        def ask(index: int) -> None:
            """One query per thread; no backend exists, so each fails, on its own."""
            try:
                client.query(b"%d" % index, type=types.PYTHON_TEST_REQUEST, timeout=5)
                outcomes.put("replied")
            except Exception as error:  # noqa: BLE001  the outcome is the point
                outcomes.put(type(error).__name__)

        threads = [threading.Thread(target=ask, args=(index,)) for index in range(20)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(30)
        results = [outcomes.get(timeout=1) for _ in threads]
        self.assertEqual({"OperationFailed"}, set(results))
        client.shutdown()


if __name__ == "__main__":
    unittest.main()
