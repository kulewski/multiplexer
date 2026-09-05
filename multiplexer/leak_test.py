"""Leak tests for the Python side of the library and the binding.

A real multiplexer and a backend thread serve requests; the tests then look
at what Python retains: tracemalloc snapshots compared by source line (which
line allocated what grew), the garbage collector's object count after a
collection, reference counts of objects handed to the binding, and weak
references that must die once the strong ones are dropped.
"""

import gc
import os
import subprocess
import sys
import threading
import time
import tracemalloc
import unittest
import weakref

from multiplexer.clients import Client
from multiplexer.multiplexer_constants import peers, types
from multiplexer.mxclient import OperationTimedOut
from multiplexer.servers import BaseMultiplexerServer
from multiplexer.threaded_client import ThreadedClient

QUERIES = 2000
ALLOWED_GROWTH = 64 * 1024  # bytes tracemalloc may report over QUERIES queries
ALLOWED_OBJECTS = 500  # a leak per query would add QUERIES; interpreter caches add up to two hundred


def runfile(path: str) -> str:
    """A file from the runfiles of this test."""
    return os.path.join(os.environ["TEST_SRCDIR"], "mx", path)


class Backend(BaseMultiplexerServer):
    """Answers PYTHON_TEST_REQUEST with the payload upper-cased."""

    def handle_message(self, mxmsg):
        """Reply to one request."""
        self.send_message(message=mxmsg.message.upper(), type=types.PYTHON_TEST_RESPONSE)


class LeakTest(unittest.TestCase):
    """See the module docstring."""

    def setUp(self):
        """Start a multiplexer on a free port and a backend on a thread of its own."""
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
        self.serving = True
        ready = threading.Event()

        def serve():
            """The backend, built and driven on this thread."""
            backend = Backend([self.endpoint], type=peers.PYTHON_TEST_SERVER)
            ready.set()
            while self.serving:
                try:
                    backend.loop_iter(timeout=0.2)
                except OperationTimedOut:
                    pass
            backend.close()

        self.backend_thread = threading.Thread(target=serve)
        self.backend_thread.start()
        ready.wait(10)

    def tearDown(self):
        """Stop the backend and the multiplexer."""
        self.serving = False
        self.backend_thread.join(10)
        self.mx.terminate()
        self.mx.wait(10)

    def assert_no_growth(self, before: tracemalloc.Snapshot, objects_before: int) -> None:
        """Compare with a snapshot taken now: total growth and the lines behind it."""
        gc.collect()
        after = tracemalloc.take_snapshot()
        objects_after = len(gc.get_objects())
        statistics = after.compare_to(before, "lineno")
        growth = sum(statistic.size_diff for statistic in statistics)
        top = "\n".join(str(statistic) for statistic in statistics[:5])
        self.assertLess(growth, ALLOWED_GROWTH, "Python allocations grew by %d bytes; top lines:\n%s" % (growth, top))
        self.assertLess(
            objects_after - objects_before, ALLOWED_OBJECTS, "objects grew by %d" % (objects_after - objects_before)
        )

    def test_synchronous_client_does_not_accumulate(self):
        """tracemalloc by line and the object count over thousands of query() calls."""
        client = Client([self.endpoint], type=peers.WEBSITE)
        for _ in range(200):  # warm-up: caches, the dedup window, interned strings
            client.query(b"hello", type=types.PYTHON_TEST_REQUEST, timeout=10)
        gc.collect()
        tracemalloc.start()
        before = tracemalloc.take_snapshot()
        objects_before = len(gc.get_objects())
        for _ in range(QUERIES):
            reply = client.query(b"hello", type=types.PYTHON_TEST_REQUEST, timeout=10)
            self.assertEqual(b"HELLO", reply.message)
        self.assert_no_growth(before, objects_before)
        tracemalloc.stop()
        client.shutdown()

    def test_threaded_client_releases_callbacks_and_replies(self):
        """The binding must drop every reference it takes: the callback's
        reference count is back where it was, and a weak reference to it
        dies once the strong one is dropped."""
        client = ThreadedClient([self.endpoint], type=peers.WEBSITE)
        done = threading.Semaphore(0)
        replies = []

        def callback(result):
            """Keep the reply, signal one completion."""
            replies.append(result)
            done.release()

        weak = weakref.ref(callback)
        refcount_before = sys.getrefcount(callback)
        for _ in range(QUERIES):
            client.query(b"hello", type=types.PYTHON_TEST_REQUEST, callback=callback, timeout=10)
        for _ in range(QUERIES):
            self.assertTrue(done.acquire(timeout=30), "a query never completed")
        self.assertEqual(QUERIES, len(replies))
        self.assertEqual(b"HELLO", replies[0].message)
        gc.collect()
        self.assertEqual(refcount_before, sys.getrefcount(callback), "the binding kept a reference to the callback")
        replies.clear()
        del callback
        gc.collect()
        self.assertIsNone(weak(), "the callback is still alive after every reference was dropped")
        client.shutdown()

    def test_clients_are_collectable_after_shutdown(self):
        """A client object is freed once shut down and dropped; nothing in the
        binding or a thread keeps it alive."""
        threaded = ThreadedClient([self.endpoint], type=peers.WEBSITE)
        threaded.query(b"hello", type=types.PYTHON_TEST_REQUEST, timeout=10)
        threaded.shutdown()
        weak_threaded = weakref.ref(threaded)
        del threaded
        synchronous = Client([self.endpoint], type=peers.WEBSITE)
        synchronous.query(b"hello", type=types.PYTHON_TEST_REQUEST, timeout=10)
        synchronous.shutdown()
        weak_synchronous = weakref.ref(synchronous)
        del synchronous
        gc.collect()
        self.assertIsNone(weak_threaded(), "ThreadedClient still referenced after shutdown")
        self.assertIsNone(weak_synchronous(), "Client still referenced after shutdown")


if __name__ == "__main__":
    unittest.main()
