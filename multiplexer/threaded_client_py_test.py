"""ThreadedClient from Python: on_message, pings, and the io-thread rule.

Against a real multiplexer started for the test: a message addressed to the
client's instance id reaches on_message, a PING is answered by the client
itself, a search policy's answer is read by its truth, the blocking
query() raises RuntimeError from a callback, and the io thread keeps one
Python thread state from its first callback to its end.
"""

import os
import queue
import subprocess
import sys
import threading
import time
import unittest
import weakref

from multiplexer.clients import Client
from multiplexer.Multiplexer_pb2 import BackendForPacketSearch
from multiplexer.multiplexer_constants import peers, types
from multiplexer.testing import Cluster
from multiplexer.testing.buffers import past_the_queue
from multiplexer.threaded_client import ThreadedClient


def runfile(path: str) -> str:
    """A file of this repository inside the test's runfiles tree."""
    return os.path.join(os.environ["TEST_SRCDIR"], os.environ.get("TEST_WORKSPACE", "mx"), path)


# A program whose client is kept alive only by its own pending query: the
# last reference goes when the query's callback has run, on the io thread.
DROPPED_IN_A_CALLBACK = """
import sys
import time

from multiplexer.threaded_client import ThreadedClient

host, port = sys.argv[1].rsplit(":", 1)


def ask_and_forget():
    client = ThreadedClient([(host, int(port))], type=%(website)d)
    client.query(b"x", %(request)d, timeout=1, callback=lambda result: print("callback ran", flush=True))


ask_and_forget()
time.sleep(2)
print("still running", flush=True)
"""


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
        self.assertEqual(peer.instance_id, mxmsg.sender)
        client.shutdown()
        peer.shutdown()

    def test_the_io_threads_python_state_lasts_from_its_first_callback_to_its_end(self) -> None:
        """A thread-local a callback sets is there in the next callback, and
        goes when the client shuts down, the io thread's Python thread state
        with it: each callback had a state made and destroyed for it, an
        mmap, a munmap and a gettid per message on CPython 3.11 and later."""
        local = threading.local()
        seen: list[bool] = []
        markers: list[weakref.ref] = []
        arrived = threading.Semaphore(0)

        class Marker:
            """What the callback keeps in its thread-local."""

        def on_message(mxmsg) -> None:
            seen.append(hasattr(local, "marker"))
            if not hasattr(local, "marker"):
                local.marker = Marker()
                markers.append(weakref.ref(local.marker))
            arrived.release()

        client = ThreadedClient([self.endpoint], type=peers.WEBSITE, on_message=on_message)
        peer = Client([self.endpoint], type=peers.WEBSITE)
        for _ in range(3):
            peer.send_message(message=b"for you", type=types.PYTHON_TEST_REQUEST, to=client.instance_id, flush=True)
        for _ in range(3):
            self.assertTrue(arrived.acquire(timeout=5), "a message did not arrive")
        self.assertEqual([False, True, True], seen, "the callback before's thread-local was gone")
        client.shutdown()  # the io thread ends, joined
        self.assertEqual(1, len(markers))
        self.assertIsNone(markers[0](), "the io thread's thread state outlived the thread")
        peer.shutdown()

    def test_a_shutdown_after_one_begun_in_a_callback_waits_for_the_end(self) -> None:
        """A shutdown begun in a callback, on the io thread, returns at once;
        one from another thread after it waits for the write-out and the
        close the first began, and returns once the io thread has ended, the
        callbacks let go of: it returned at once, and a program that then
        exited cut the write-out. The first multiplexer, frozen with what the
        client sent queued for it, holds the write-out open until the
        shutdown's deadline; the word to quit comes through the second."""

        class Marker:
            """What only the callback holds."""

        marker = Marker()
        held = weakref.ref(marker)
        begun = threading.Event()
        clients: list[ThreadedClient] = []

        def on_message(mxmsg, kept=marker) -> None:
            if mxmsg.message == b"quit":
                clients[0].shutdown(1)  # on the io thread: it does not wait
                begun.set()

        del marker
        with Cluster(2, rules=runfile("tests/testing.rules")) as cluster:
            first, second = cluster.endpoints
            client = ThreadedClient([first, second], type=peers.PYTHON_TEST_SERVER, on_message=on_message)
            clients.append(client)
            del on_message  # the client's alone
            peer = Client([second], type=peers.WEBSITE)
            cluster.mx[0].pause()
            try:
                for payload in past_the_queue(b"x" * 16):
                    client.send_message(payload, multiplexer=ThreadedClient.ALL, type=types.TEST_UNROUTED)
                peer.send_message(message=b"quit", type=types.PYTHON_TEST_REQUEST, to=client.instance_id, flush=True)
                self.assertTrue(begun.wait(30), "the callback never ran")
                client.shutdown()
                ended = held() is None
            finally:
                cluster.mx[0].resume()
            peer.shutdown()
        self.assertTrue(ended, "shutdown() returned before the io thread ended")

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

    def test_a_search_policy_is_read_by_its_truth(self) -> None:
        """A search policy's answer is read as Python's `if` reads it: an
        empty list declines a search, a non-empty one takes it, answered with
        a PING, where a value that is not a bool failed to convert, logged on
        every search, and declined it. Two searches, the first declined and
        the second taken: the one PING references the second, and the
        connection's order says none came for the first."""
        answers: list[list[str]] = [[], ["work"]]
        client = ThreadedClient([self.endpoint], type=peers.PYTHON_TEST_SERVER, search_policy=lambda: answers.pop(0))
        peer = Client([self.endpoint], type=peers.WEBSITE)
        search = BackendForPacketSearch(packet_type=types.PYTHON_TEST_REQUEST)
        peer.send_message(search, type=types.BACKEND_FOR_PACKET_SEARCH, flush=True)
        taken = peer.send_message(search, type=types.BACKEND_FOR_PACKET_SEARCH, flush=True)
        pong = peer.read_message(timeout=10)
        self.assertEqual((types.PING, taken), (pong.type, pong.references))
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

    def test_a_client_dropped_in_its_own_callback_ends_quietly(self) -> None:
        """A client whose last reference goes in its own callback, as when a
        callback query outlives the caller's reference, used to abort the
        process: its destructor joined the io thread it ran on."""
        env = dict(os.environ, PYTHONPATH=os.pathsep.join(sys.path))
        program = DROPPED_IN_A_CALLBACK % {"website": peers.WEBSITE, "request": types.PYTHON_TEST_REQUEST}
        result = subprocess.run(
            [sys.executable, "-c", program, "%s:%d" % self.endpoint],
            env=env,
            capture_output=True,
            text=True,
            timeout=60,
        )
        said = [line for line in result.stderr.splitlines() if not line.startswith(("[DEBUG]", "[INFO]"))]
        self.assertEqual(0, result.returncode, "exit %d\n%s" % (result.returncode, "\n".join(said[-20:])))
        self.assertEqual(["callback ran", "still running"], result.stdout.split("\n")[:2])

    def test_many_threads_query_at_once(self) -> None:
        """Twenty threads query one client at the same time; each gets its own outcome."""
        client = ThreadedClient([self.endpoint], type=peers.WEBSITE)
        outcomes: queue.Queue = queue.Queue()

        def ask(index: int) -> None:
            """One query per thread; no backend exists, so each fails, on its own."""
            try:
                client.query(b"%d" % index, type=types.PYTHON_TEST_REQUEST, timeout=5)
                outcomes.put("replied")
            except Exception as error:  # the outcome is the point
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
