"""A backend built on one thread and served from another.

serve_forever() adopts the calling thread, so the debug-build thread checks,
which bind a client and its connections to the thread that made them, do
not fire; a program driving loop_iter() itself calls bind_to_current_thread().
A client still alive at interpreter exit is destroyed on the main thread
whichever thread drove it, and that must not fail the check either.
"""

# A program whose client served on a worker and is still alive when the
# interpreter exits: the garbage collector destroys it on the main thread.
EXIT_PROBE = """
import sys, threading
from multiplexer.clients import Client
from multiplexer.mxclient import OperationTimedOut
host, port = sys.argv[1].rsplit(":", 1)
client = Client([(host, int(port))], type=%d)
def work():
    client.bind_to_current_thread()
    try:
        client.read_message(timeout=0.1)
    except OperationTimedOut:
        pass
worker = threading.Thread(target=work)
worker.start()
worker.join()
print("exiting with the client alive")
"""

import os
import subprocess
import sys
import threading
import time
import unittest

from multiplexer.mxclient import OperationTimedOut
from multiplexer.multiplexer_constants import peers
from multiplexer.servers import BaseMultiplexerServer


def runfile(path: str) -> str:
    """A file of this repository inside the test's runfiles tree."""
    return os.path.join(os.environ["TEST_SRCDIR"], os.environ.get("TEST_WORKSPACE", "mx"), path)


class CountingBackend(BaseMultiplexerServer):
    """Serves three iterations, then stops."""

    def __init__(self, addresses):
        super().__init__(addresses, type=peers.PYTHON_TEST_SERVER)
        self.iterations = 0

    def handle_message(self, mxmsg):
        """Nothing is expected."""
        self.no_response()

    def periodic_task(self):
        """Stop after three iterations."""
        self.iterations += 1
        if self.iterations >= 3:
            self.working = False


class ServeThreadTest(unittest.TestCase):
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

    def test_built_on_one_thread_served_from_another(self) -> None:
        """serve_forever() on a worker thread adopts it and runs to the end."""
        backend = CountingBackend([self.endpoint])  # built, and connected, on the main thread
        worker = threading.Thread(target=backend.serve_forever, kwargs={"poll": 0.1})
        worker.start()
        worker.join(30)
        self.assertFalse(worker.is_alive())
        self.assertEqual(3, backend.iterations)

    def test_explicit_rebind_for_own_loop(self) -> None:
        """A program that drives loop_iter() itself rebinds first."""
        backend = CountingBackend([self.endpoint])

        def drive() -> None:
            """Three timed-out iterations on this thread."""
            backend.conn.bind_to_current_thread()
            for _ in range(3):
                try:
                    backend.loop_iter(timeout=0.1)
                except OperationTimedOut:
                    pass
            backend.close()

        worker = threading.Thread(target=drive)
        worker.start()
        worker.join(30)
        self.assertFalse(worker.is_alive())

    def test_destroyed_at_interpreter_exit_on_the_main_thread(self) -> None:
        """A client bound to a worker and left alive exits cleanly: the
        destructor adopts the destroying thread instead of failing the check."""
        env = dict(os.environ, PYTHONPATH=os.pathsep.join(sys.path))
        result = subprocess.run(
            [sys.executable, "-c", EXIT_PROBE % peers.PYTHON_TEST_SERVER, "%s:%d" % self.endpoint],
            env=env,
            capture_output=True,
            text=True,
            timeout=60,
        )
        self.assertEqual(0, result.returncode, result.stderr[-2000:])
        self.assertIn("exiting with the client alive", result.stdout)


if __name__ == "__main__":
    unittest.main()
