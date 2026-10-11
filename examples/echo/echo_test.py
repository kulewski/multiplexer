"""The echo binaries end to end, as a deployment runs them: a multiplexer
from multiplexer.testing's Cluster, a backend binary started with Process
and waited for by its "ready" line, and a client in each language asking
it. Leaving the Cluster's block stops the backend with SIGTERM and fails the
test unless it ended cleanly, the way docs/recipes/test_a_service.md tests
a service binary in any language."""

import subprocess
import unittest

from multiplexer.clients import SyncClient
from multiplexer.multiplexer_constants import peers, types
from multiplexer.testing import Cluster, Process, runfile

RULES = runfile("echo.rules")


class EchoTest(unittest.TestCase):
    """One Cluster per test, and the backend binary the test names."""

    def run_backend(self, cluster: Cluster, backend: str) -> Process:
        """Start one of the backend binaries against the cluster and wait
        for its "ready" line."""
        process = Process([runfile(backend), cluster.addresses[0]], name=backend)
        process.wait_for_line("^ready$")
        return process

    def ask(self, cluster: Cluster, client: str, *args: str) -> str:
        """Run one of the client binaries to its end; what it printed."""
        return subprocess.run(
            [runfile(client), cluster.addresses[0], *args], stdout=subprocess.PIPE, timeout=60, check=True, text=True
        ).stdout

    def query_in_process(self, backend: str) -> None:
        """Query the backend from this process with the Python SyncClient."""
        with Cluster(1, rules=RULES) as cluster:
            self.run_backend(cluster, backend)
            client = SyncClient(cluster.endpoints, type=peers.ECHO_CLIENT)
            response = client.query(b"hello multiplexer", type=types.ECHO_REQUEST, timeout=10)
            client.shutdown()
        self.assertEqual(types.ECHO_RESPONSE, response.type)
        self.assertEqual(b"HELLO MULTIPLEXER", response.message)

    def test_python_backend(self):
        self.query_in_process("backend_py")

    def test_cpp_backend(self):
        self.query_in_process("backend_cc")

    def test_cpp_client(self):
        with Cluster(1, rules=RULES) as cluster:
            self.run_backend(cluster, "backend_py")
            self.assertEqual("HELLO MULTIPLEXER\n", self.ask(cluster, "client_cc", "hello multiplexer"))

    def test_python_client(self):
        with Cluster(1, rules=RULES) as cluster:
            self.run_backend(cluster, "backend_cc")
            self.assertEqual("HELLO MULTIPLEXER\n", self.ask(cluster, "client_py", "hello multiplexer"))

    def test_worker_threads_share_one_threaded_client(self):
        with Cluster(1, rules=RULES) as cluster:
            self.run_backend(cluster, "backend_cc")
            lines = sorted(self.ask(cluster, "workers", "3", "4").splitlines())
        self.assertEqual(sorted("worker-%d: W%d JOB %d" % (w, w, j) for w in range(3) for j in range(4)), lines)

    def test_the_cpp_backend_answers_through_its_drain_and_exits_0(self):
        """SIGTERM starts the C++ backend's drain. Alone, the last backend
        of its type, it still answers, then exits 0."""
        with Cluster(1, rules=RULES) as cluster:
            backend = self.run_backend(cluster, "backend_cc")
            backend.proc.terminate()
            self.assertEqual("STILL HERE\n", self.ask(cluster, "client_py", "still here"))
            self.assertEqual(0, backend.wait(30))


if __name__ == "__main__":
    unittest.main()
