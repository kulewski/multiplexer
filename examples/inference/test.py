"""The example against real multiplexers, in one process: a Cluster of two,
the worker on a BackendThread, the client asking, the Django views through
Django's test client; then one multiplexer killed under the client, a
worker gone, and a worker process leaving on SIGTERM; and the committed
constants checked against the rules file. Needs torch and Django (test.sh
arranges both); the multiplexers are the mxcontrol the package installed
unless MXCONTROL names another; `python -m unittest test`."""

import os
import signal
import subprocess
import sys
import tempfile
import threading
import unittest
import warnings

HERE = os.path.dirname(os.path.abspath(__file__))
# The example's own modules import by name, as they do when its scripts run
# from its directory; this test may run from anywhere, so the example's
# directory and its web app's go first on the path.
sys.path[:0] = [HERE, os.path.join(HERE, "web")]
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "webapp.settings")

import django
from django.conf import settings
from multiplexer.testing import BackendThread, Cluster, mxcontrol_path
from multiplexer.threaded_client import ThreadedClient

import backend
import client as cli
from multiplexer_constants import peers

RULES = os.path.join(HERE, "inference.rules")
SAMPLES = {digit: open(os.path.join(HERE, "samples", f"{digit}.png"), "rb").read() for digit in range(10)}


class InferenceExampleTest(unittest.TestCase):
    """See the module docstring."""

    @classmethod
    def setUpClass(cls):
        cls.cluster = Cluster(2, rules=RULES).__enter__()
        settings.MULTIPLEXER_ADDRESSES = cls.cluster.endpoints
        django.setup()

    @classmethod
    def tearDownClass(cls):
        from classify import mx

        mx.reset()
        cls.cluster.__exit__(None, None, None)

    def setUp(self):
        self.worker = BackendThread(lambda: backend.Classifier(self.cluster.endpoints, name="test-worker")).start()
        self.cluster.wait_for_peer(peers.INFERENCE)

    def tearDown(self):
        if self.worker.running:
            self.worker.stop()

    def test_the_client_reads_every_sample(self):
        client = ThreadedClient(self.cluster.endpoints, type=peers.WEB)
        try:
            for digit, image in SAMPLES.items():
                response = cli.classify(client, image)
                self.assertEqual(digit, response.label)
                self.assertEqual("test-worker", response.worker)
                self.assertGreater(response.probabilities[digit], 0.99)
        finally:
            client.shutdown()

    def test_the_json_endpoint_answers(self):
        from django.test import Client

        with open(os.path.join(HERE, "samples", "7.png"), "rb") as image:
            response = Client().post("/classify", {"image": image})
        self.assertEqual(200, response.status_code, response.content)
        self.assertEqual(7, response.json()["label"])
        self.assertEqual("test-worker", response.json()["worker"])
        self.assertEqual(400, Client().post("/classify", {}).status_code)

    def test_an_image_over_the_limit_is_413(self):
        import io

        from django.conf import settings
        from django.test import Client

        big = io.BytesIO(b"x" * (settings.MAX_IMAGE_BYTES + 1))
        big.name = "big.png"
        response = Client().post("/classify", {"image": big})
        self.assertEqual(413, response.status_code, response.content)

    def test_the_page_renders_the_answer(self):
        from django.test import Client

        self.assertIn(b"Which digit?", Client().get("/").content)
        with open(os.path.join(HERE, "samples", "4.png"), "rb") as image:
            response = Client().post("/", {"image": image})
        self.assertIn(b"It is a <strong>4</strong>", response.content)

    def test_a_multiplexer_may_die_under_the_client(self):
        client = ThreadedClient(self.cluster.endpoints, type=peers.WEB)
        try:
            self.assertEqual(2, cli.classify(client, SAMPLES[2]).label)
            self.cluster.mx[0].kill()
            for digit in (3, 5, 9):
                self.assertEqual(digit, cli.classify(client, SAMPLES[digit]).label)
        finally:
            self.cluster.mx[0].start()
            # Both reconnect on their own; the client is shut down once it
            # has, with no reconnect pending.
            self.cluster.wait_for_peer(peers.INFERENCE)
            self.cluster.wait_for_peer(peers.WEB)
            client.shutdown()

    def test_no_worker_is_503(self):
        from django.test import Client

        self.worker.stop()
        self.cluster.wait_for_peer_gone(peers.INFERENCE)
        with open(os.path.join(HERE, "samples", "1.png"), "rb") as image:
            response = Client().post("/classify", {"image": image})
        self.assertEqual(503, response.status_code)
        self.assertEqual("OperationFailed", response.json()["exception"])

    def test_sigterm_makes_a_worker_leave(self):
        """SIGTERM, what an orchestrator sends, drains a worker process as the drain file does: its handler
        only sets a flag, which periodic_task() reads within a poll, and the worker exits on its own once the
        multiplexers have confirmed."""
        addresses = ",".join(f"{host}:{port}" for host, port in self.cluster.endpoints)
        worker = subprocess.Popen(
            [sys.executable, "backend.py", addresses, "--name", "leaving"],
            cwd=HERE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
        )
        try:
            assert worker.stdout is not None
            self.assertTrue(worker.stdout.readline().startswith("ready"), "connected")
            worker.send_signal(signal.SIGTERM)
            self.assertEqual(0, worker.wait(15), "left of its own accord")
            self.assertTrue(worker.stdout.read().startswith("left after"), "by the drain, not killed")
        finally:
            if worker.poll() is None:
                worker.kill()
                worker.wait()


class ConstantsTest(unittest.TestCase):
    """The committed constants are what mxcontrol writes from the rules file
    as it is now: a change to the file that left them behind fails here."""

    def test_the_constants_are_generated_from_the_rules_file(self):
        out = tempfile.mkdtemp()
        written = [os.path.join(out, name) for name in ("multiplexer_constants.py", "multiplexer_constants.pyi")]
        command = [
            mxcontrol_path(),
            "generate_constants",
            "inference.rules",
            "--python",
            written[0],
            "--pyi",
            written[1],
        ]
        subprocess.run(command, cwd=HERE, check=True, capture_output=True)  # the file's name as the header records it
        for path in written:
            with open(path) as generated, open(os.path.join(HERE, os.path.basename(path))) as committed:
                self.assertEqual(committed.read(), generated.read(), os.path.basename(path))


class HolderForkTest(unittest.TestCase):
    """The holder of web/classify/mx.py across a fork: the hook that drops the
    parent's client must give the child a lock of its own, since a fork
    can come while another thread holds the parent's, inside the first
    call that makes the client, and the child would then wait on it for good."""

    def test_the_child_gets_a_free_lock(self):
        from classify import mx as holder

        held, release = threading.Event(), threading.Event()

        def hold() -> None:
            with holder._lock:
                held.set()
                release.wait()

        holder_thread = threading.Thread(target=hold)
        holder_thread.start()
        held.wait()
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", DeprecationWarning)  # forking beside a thread is the point
                pid = os.fork()
            if pid == 0:
                os._exit(1 if holder._lock.locked() else 0)
        finally:
            release.set()
            holder_thread.join()
        _, status = os.waitpid(pid, 0)
        self.assertEqual(0, os.waitstatus_to_exitcode(status), "the child's lock was taken")


if __name__ == "__main__":
    unittest.main()
