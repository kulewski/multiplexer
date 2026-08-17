"""Starts a multiplexer and a backend in each language, and queries it with a client in each language."""

import os
import subprocess
import time
import unittest

from multiplexer.clients import Client
from multiplexer.multiplexer_constants import peers, types


def runfile(repo: str, path: str) -> str:
    """The absolute path of `path` in workspace `repo` under the test's runfiles."""
    return os.path.join(os.environ["TEST_SRCDIR"], repo, path)


class EchoTest(unittest.TestCase):
    """One multiplexer per test, started on a free port; a backend is started
    by the test itself and both are stopped afterwards."""

    def setUp(self):
        """Start the multiplexer and learn its port."""
        port_file = os.path.join(os.environ["TEST_TMPDIR"], "mx.port")
        if os.path.exists(port_file):
            os.unlink(port_file)  # a previous test's multiplexer wrote it
        self.mx = subprocess.Popen(
            [
                runfile("mx", "mxcontrol/mxcontrol"),
                "run_multiplexer",
                "--address",
                "127.0.0.1:0",
                "--rules",
                runfile("echo_example", "echo.rules"),
                "--port-file",
                port_file,
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        deadline = time.time() + 15
        while not os.path.exists(port_file):
            self.assertLess(time.time(), deadline, "multiplexer did not start")
            time.sleep(0.02)
        with open(port_file) as f:
            host, port = f.read().strip().rsplit(":", 1)
        self.endpoint = (host, int(port))
        self.backend = None

    def tearDown(self):
        """Stop the backend, if any, and the multiplexer."""
        for proc in (self.backend, self.mx):
            if proc is not None:
                proc.terminate()
                proc.wait(10)

    def start_backend(self, backend_binary: str) -> None:
        """Run one of the backend binaries and wait for its "ready" line."""
        self.backend = subprocess.Popen(
            [runfile("echo_example", backend_binary), "%s:%d" % self.endpoint],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )
        self.assertEqual(b"ready\n", self.backend.stdout.readline())

    def query_in_process(self, backend_binary: str) -> None:
        """Query the backend from this process with the Python Client."""
        self.start_backend(backend_binary)
        client = Client([self.endpoint], type=peers.ECHO_CLIENT)
        response = client.query(b"hello multiplexer", type=types.ECHO_REQUEST, timeout=10)
        client.shutdown()
        self.assertEqual(types.ECHO_RESPONSE, response.type)
        self.assertEqual(b"HELLO MULTIPLEXER", response.message)

    def query_with_binary(self, backend_binary: str, client_binary: str) -> None:
        """Query the backend by running one of the client binaries."""
        self.start_backend(backend_binary)
        output = subprocess.run(
            [runfile("echo_example", client_binary), "%s:%d" % self.endpoint, "hello multiplexer"],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            timeout=30,
            check=True,
        ).stdout
        self.assertEqual(b"HELLO MULTIPLEXER\n", output)

    def test_python_backend(self):
        self.query_in_process("backend_py")

    def test_cpp_backend(self):
        self.query_in_process("backend_cc")

    def test_cpp_client(self):
        self.query_with_binary("backend_py", "client_cc")

    def test_python_client(self):
        self.query_with_binary("backend_cc", "client_py")

    def test_worker_threads_share_one_threaded_client(self):
        self.start_backend("backend_cc")
        output = subprocess.run(
            [runfile("echo_example", "workers"), "%s:%d" % self.endpoint, "3", "4"],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            timeout=60,
            check=True,
        ).stdout.decode()
        lines = sorted(output.splitlines())
        self.assertEqual(sorted("worker-%d: W%d JOB %d" % (w, w, j) for w in range(3) for j in range(4)), lines)


if __name__ == "__main__":
    unittest.main()
