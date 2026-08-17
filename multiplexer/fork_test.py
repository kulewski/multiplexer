"""Clients inherited across fork() are orphans in the child.

Every call on them raises UsedAfterFork, dropping them neither hangs nor
touches the parent's connections, a fresh client in the child works, and
the parent's clients are untouched. See lib/fork.h.
"""

import os
import subprocess
import time
import unittest

from multiplexer.clients import Client
from multiplexer.multiplexer_constants import peers, types
from multiplexer.mxclient import OperationFailed, UsedAfterFork
from multiplexer.threaded_client import ThreadedClient


def runfile(path: str) -> str:
    """A file of this repository inside the test's runfiles tree."""
    return os.path.join(os.environ["TEST_SRCDIR"], os.environ.get("TEST_WORKSPACE", "mx"), path)


class ForkTest(unittest.TestCase):
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

    def test_inherited_clients_are_orphans_and_the_parent_keeps_its_connections(self) -> None:
        """Fork with a live Client and ThreadedClient; check both sides."""
        sync = Client([self.endpoint], type=peers.WEBSITE)
        threaded = ThreadedClient([self.endpoint], type=peers.WEBSITE)
        os.environ["MX_FORK_TEST_ENDPOINT"] = "%s:%d" % self.endpoint
        read_end, write_end = os.pipe()
        pid = os.fork()
        if pid == 0:
            os.close(read_end)
            self._child(sync, threaded, write_end)
        os.close(write_end)
        _, status = os.waitpid(pid, 0)
        report = os.read(read_end, 65536).decode()
        self.assertEqual(0, os.waitstatus_to_exitcode(status), report)
        self.assertEqual(
            {
                "sync.query": "UsedAfterFork",
                "sync.send_message": "UsedAfterFork",
                "sync.connect": "UsedAfterFork",
                "threaded.query": "UsedAfterFork",
                "threaded.send_message": "UsedAfterFork",
                "threaded.connect": "UsedAfterFork",
                "dropped": "ok",
                "fresh": "OperationFailed",
            },
            dict(line.split(": ", 1) for line in report.splitlines()),
        )
        # The parent's clients still work and are still registered: the
        # child closed only its own descriptor copies.
        time.sleep(0.5)
        self.assertEqual(1, threaded.connections_count())
        self.assertEqual(1, sync.connections_count())
        with self.assertRaises(OperationFailed):
            threaded.query(b"nobody serves this", type=types.PYTHON_TEST_REQUEST, timeout=5)
        with self.assertRaises(OperationFailed):
            sync.query(b"nobody serves this", type=types.PYTHON_TEST_REQUEST, timeout=5)
        threaded.shutdown()
        sync.shutdown()

    @staticmethod
    def _child(sync: Client, threaded: ThreadedClient, write_end: int) -> None:
        """The child's checks; reports one `name: outcome` per line and exits."""
        report = []

        def attempt(name, call) -> None:
            """Record what `call` raised, or that it returned."""
            try:
                call()
                report.append("%s: returned" % name)
            except UsedAfterFork:
                report.append("%s: UsedAfterFork" % name)
            except Exception as error:  # noqa: BLE001  the outcome is the point
                report.append("%s: %s" % (name, type(error).__name__))

        try:
            attempt("sync.query", lambda: sync.query(b"x", type=types.PYTHON_TEST_REQUEST, timeout=1))
            attempt("sync.send_message", lambda: sync.send_message(message=b"x", type=types.PYTHON_TEST_REQUEST))
            attempt("sync.connect", lambda: sync.connect(("127.0.0.1", 1)))
            attempt("threaded.query", lambda: threaded.query(b"x", type=types.PYTHON_TEST_REQUEST, timeout=1))
            attempt("threaded.send_message", lambda: threaded.send_message(b"x", type=types.PYTHON_TEST_REQUEST))
            attempt("threaded.connect", lambda: threaded.connect(("127.0.0.1", 1), 0.1))
            del sync, threaded  # the orphan teardown: must neither hang nor hurt the parent
            report.append("dropped: ok")
            host, port = os.environ["MX_FORK_TEST_ENDPOINT"].rsplit(":", 1)
            fresh = ThreadedClient([(host, int(port))], type=peers.WEBSITE)
            try:
                fresh.query(b"x", type=types.PYTHON_TEST_REQUEST, timeout=5)
                report.append("fresh: returned")
            except Exception as error:  # noqa: BLE001
                report.append("fresh: %s" % type(error).__name__)
            fresh.shutdown()
            os.write(write_end, "\n".join(report).encode())
            os._exit(0)
        except BaseException as error:  # noqa: BLE001  the parent must see why the child died
            os.write(write_end, ("\n".join(report) + "\nchild failed: %r" % (error,)).encode())
            os._exit(1)


if __name__ == "__main__":
    unittest.main()
