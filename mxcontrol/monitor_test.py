"""run_multiplexer --monitor-address serves /healthz and /readyz over HTTP,
for Kubernetes probes (docs/operations.md): both answer 200 while the
multiplexer runs, on one event loop and on several, and the monitor goes
with the multiplexer at a SIGTERM, which still exits 0; --monitor-port-file
has the address the log names, there before the port file. Without the
option nothing is served. An address that is not an IP address and a port
is a malformed command line, as --address's is, and so is a monitor port
file without a monitor; an address in use ends the start, naming the
option; nothing listens in either case."""

import http.client
import os
import re
import socket
import subprocess
import tempfile
import unittest

from multiplexer.endpoints import parse_endpoint
from multiplexer.testing import mx_runfile, runfile, wait_until

MXCONTROL = mx_runfile("mxcontrol/mxcontrol")
TESTING_RULES = runfile("tests/testing.rules")
BOUND = 30  # seconds a step may take: a failure detector only
SERVING = re.compile(r'serving /healthz and /readyz on http://([^\s"]+)')


def text(path: str) -> str:
    """The file at `path`, a multiplexer's log."""
    with open(path) as file:
        return file.read()


def get(port: int, path: str) -> tuple[int, str]:
    """The status and the body of the answer to GET `path`."""
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=BOUND)
    try:
        connection.request("GET", path)
        answer = connection.getresponse()
        return answer.status, answer.read().decode()
    finally:
        connection.close()


class MonitorTest(unittest.TestCase):
    """See the module docstring."""

    def run_multiplexer(self, *options: str) -> tuple[subprocess.Popen, str, str]:
        """A multiplexer on a free port with `options`, its process, its log
        file and its port file, once it has written the port file."""
        directory = tempfile.mkdtemp()
        log_path = os.path.join(directory, "mx.log")
        port_file = os.path.join(directory, "mx.port")
        with open(log_path, "wb") as log:
            process = subprocess.Popen(
                [MXCONTROL, "run_multiplexer", "--rules", TESTING_RULES, "--address", "127.0.0.1:0"]
                + ["--port-file", port_file]
                + list(options),
                stdin=subprocess.DEVNULL,
                stdout=log,
                stderr=log,
            )
        self.addCleanup(lambda: process.poll() is None and (process.kill(), process.wait(BOUND)))
        wait_until(lambda: os.path.exists(port_file) or process.poll() is not None, BOUND, "the port file")
        if process.poll() is not None:
            self.fail(text(log_path))
        return process, log_path, port_file

    def test_probes_answer_while_the_multiplexer_runs(self):
        for threads in ("1", "3"):
            with self.subTest(threads=threads):
                monitor_port_file = os.path.join(tempfile.mkdtemp(), "monitor.port")
                process, log_path, _ = self.run_multiplexer(
                    "--monitor-address", "127.0.0.1:0", "--monitor-port-file", monitor_port_file, "--threads", threads
                )
                address = text(monitor_port_file).strip()  # written before the port file
                log = text(log_path)
                serving = SERVING.search(log)
                if serving is None:
                    self.fail(log)
                self.assertEqual(address, serving.group(1))
                host, port = parse_endpoint(address)
                self.assertEqual("127.0.0.1", host)
                self.assertEqual((200, "ok\n"), get(port, "/healthz"))
                self.assertEqual((200, "ok\n"), get(port, "/readyz"))
                self.assertEqual(404, get(port, "/metrics")[0])
                process.terminate()
                if process.wait(BOUND) != 0:
                    self.fail(text(log_path))
                with self.assertRaises(ConnectionRefusedError):
                    get(port, "/healthz")

    def test_nothing_is_served_without_the_option(self):
        process, log_path, _ = self.run_multiplexer()
        process.terminate()
        self.assertEqual(0, process.wait(BOUND))
        self.assertNotIn("/healthz", text(log_path))

    def test_a_malformed_monitor_address_is_a_malformed_command_line(self):
        port_file = os.path.join(tempfile.mkdtemp(), "mx.port")
        for address, reason in (
            ("localhost:9180", "--monitor-address: 'localhost:9180': not an IP address"),
            ("127.0.0.1", "--monitor-address: '127.0.0.1': no port"),
            ("::1:9180", "--monitor-address: '::1:9180': an IPv6 address goes in brackets"),
            ("127.0.0.1:65536", "--monitor-address: '127.0.0.1:65536': the port is a number from 0 to 65535"),
            (None, "--monitor-port-file: there is no monitor without --monitor-address"),
        ):
            with self.subTest(address=address):
                monitor = ["--monitor-address", address] if address else []
                ended = subprocess.run(
                    [MXCONTROL, "run_multiplexer", "--rules", TESTING_RULES, "--address", "127.0.0.1:0"]
                    + ["--port-file", port_file, "--monitor-port-file", port_file + ".monitor"]
                    + monitor,
                    stdin=subprocess.DEVNULL,
                    capture_output=True,
                    text=True,
                    timeout=BOUND,
                )
                self.assertEqual(1, ended.returncode, ended.stderr)
                self.assertIn(reason, ended.stderr)
                self.assertFalse(os.path.exists(port_file), "a multiplexer listened")

    def test_a_monitor_address_in_use_ends_the_start(self):
        port_file = os.path.join(tempfile.mkdtemp(), "mx.port")
        with socket.socket() as taken:
            taken.bind(("127.0.0.1", 0))
            taken.listen()
            address = "127.0.0.1:%d" % taken.getsockname()[1]
            ended = subprocess.run(
                [MXCONTROL, "run_multiplexer", "--rules", TESTING_RULES, "--address", "127.0.0.1:0"]
                + ["--port-file", port_file, "--monitor-address", address],
                stdin=subprocess.DEVNULL,
                capture_output=True,
                text=True,
                timeout=BOUND,
            )
        self.assertEqual(1, ended.returncode, ended.stderr)
        self.assertIn("--monitor-address %s: bind: Address already in use" % address, ended.stderr)
        self.assertFalse(os.path.exists(port_file), "a multiplexer listened")


if __name__ == "__main__":
    unittest.main()
