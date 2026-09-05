"""Daemon threads blocked in native waits when the interpreter exits.

Django's autoreloader, and any program whose main thread returns while
worker threads still wait on the multiplexer, ends up here: finalization
starts while a daemon thread sits in a C++ wait with the GIL released.
When that wait returns the thread must park, never retake the GIL; on
CPython 3.11 retaking it ends the thread with pthread_exit through our C++
frames, which crashes. The binding parks on every version itself, and this
test runs the program ten times and expects ten clean exits.
"""

import os
import subprocess
import sys
import time
import unittest

from multiplexer.multiplexer_constants import peers, types


def runfile(path: str) -> str:
    """A file of this repository inside the test's runfiles tree."""
    return os.path.join(os.environ["TEST_SRCDIR"], os.environ.get("TEST_WORKSPACE", "mx"), path)


# Two daemon threads in native waits, a synchronous read and a threaded
# query, both looping; the main thread leaves while they are mid-wait.
EXIT_PROBE = """
import sys, threading, time
from multiplexer.clients import Client
from multiplexer.mxclient import OperationTimedOut, OperationFailed
from multiplexer.threaded_client import ThreadedClient
host, port = sys.argv[1].rsplit(":", 1)
endpoint = (host, int(port))

def reader():
    client = Client([endpoint], type=%(passive)d)
    while True:
        try:
            client.read_message(timeout=0.05)
        except OperationTimedOut:
            pass

def querier():
    client = ThreadedClient([endpoint], type=%(passive)d)
    while True:
        try:
            client.query(b"nobody serves this", type=%(request)d, timeout=0.05)
        except (OperationTimedOut, OperationFailed):
            pass

for target in (reader, querier):
    threading.Thread(target=target, daemon=True).start()
time.sleep(0.3)
print("main thread leaving with both daemon threads mid-wait")
"""


class InterpreterExitTest(unittest.TestCase):
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

    def test_daemon_threads_in_native_waits_do_not_crash_the_exit(self) -> None:
        """Ten runs, ten exit codes of zero."""
        env = dict(os.environ, PYTHONPATH=os.pathsep.join(sys.path))
        program = EXIT_PROBE % {"passive": peers.WEBSITE, "request": types.PYTHON_TEST_REQUEST}
        for run in range(10):
            result = subprocess.run(
                [sys.executable, "-c", program, "%s:%d" % self.endpoint],
                env=env,
                capture_output=True,
                text=True,
                timeout=60,
            )
            # The log lines drown what matters; show the rest of stderr.
            said = [line for line in result.stderr.splitlines() if not line.startswith(("[DEBUG]", "[INFO]"))]
            self.assertEqual(
                0, result.returncode, "run %d: exit %d\n%s" % (run, result.returncode, "\n".join(said[-30:]))
            )
            self.assertIn("main thread leaving", result.stdout)


if __name__ == "__main__":
    unittest.main()
