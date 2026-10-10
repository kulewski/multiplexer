"""The example's test on the library's harness (docs/api_python.md,
"Testing"): two real multiplexers from `Cluster`, workers served on
threads by `BackendThread`, and a `ThreadedClient` on both multiplexers.
Each model's requests are answered only by the workers that hold it, and
a model nobody holds fails at once, a worker that holds none taking none;
a model loaded is answered by its worker from the answer to the load on,
and a model unloaded while its requests keep coming loses none of them,
none reaching its old worker once the answer to the unload came; a worker
with no filter for PREDICT takes every model. Then the two programs as
the steps run them, and the committed constants checked against the rules
file. The multiplexers are the mxcontrol the package installed unless
MXCONTROL names another; `python -m unittest -v test`.

The walkthrough's "Testing it with the harness" walks through this file."""

import os
import queue
import shutil
import subprocess
import sys
import tempfile
import threading
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
# The example's own modules import by name, as they do when its scripts run
# from its directory; this test may run from anywhere, so the example's
# directory goes first on the path.
sys.path.insert(0, HERE)

from multiplexer.mxclient import OperationFailed
from multiplexer.testing import BackendThread, Cluster, mxcontrol_path, wait_until
from multiplexer.threaded_client import ThreadedClient

from multiplexer_constants import peers, types
from worker import Worker, model_key, worker_key

RULES = os.path.join(HERE, "models.rules")  # what the constants were generated from; Cluster's multiplexers read it
# A worker process dies with the test process however that ends, where util-linux's setpriv is there.
WITH_THE_TEST = ["setpriv", "--pdeathsig", "KILL", "--"] if shutil.which("setpriv") else []


class ModelsTest(unittest.TestCase):
    """Two multiplexers, the workers each test serves on threads, and a
    client connected to both."""

    def setUp(self):
        self.cluster = Cluster(2, rules=RULES).__enter__()
        self.addCleanup(self.cluster.__exit__, None, None, None)
        self.client = ThreadedClient(self.cluster.endpoints, type=peers.MODEL_CLIENT)
        self.addCleanup(self.client.shutdown)

    def worker(self, name: str, models: list[str] | None) -> Worker:
        """A worker served on a thread of its own, registered with both
        multiplexers, its filters with it, once this returns."""
        served = BackendThread(lambda: Worker(self.cluster.endpoints, name, models)).start()
        self.addCleanup(served.stop)
        assert served.backend is not None
        return served.backend

    def predict(self, model: str, times: int = 1) -> list[str]:
        """Who answered each of `times` requests for `model`, in turn."""
        answers = [self.client.query("x", type=types.PREDICT, key=model_key(model)) for _ in range(times)]
        return [answer.message.decode().rsplit(" by ", 1)[1] for answer in answers]

    def control(self, worker: str, change: str) -> str:
        """What the worker named `worker` answers `change`, load or unload."""
        return self.client.query(change, type=types.MODEL_CONTROL, key=worker_key(worker)).message.decode()

    def test_a_model_reaches_only_the_workers_that_hold_it(self):
        self.worker("w1", ["alpha", "beta"])
        self.worker("w2", ["beta", "gamma"])
        self.worker("w3", [])  # holds nothing, so takes no PREDICT at all
        self.assertEqual({"w1"}, set(self.predict("alpha", 4)))
        self.assertEqual({"w2"}, set(self.predict("gamma", 4)))
        # Round robin between the two that hold it, on each multiplexer.
        self.assertEqual({"w1", "w2"}, set(self.predict("beta", 8)))
        with self.assertRaises(OperationFailed):  # every multiplexer says at once that nobody takes the key
            self.predict("delta")
        with self.assertRaises(OperationFailed):  # and the same for a worker nobody runs
            self.control("w9", "load delta")

    def test_a_model_moves_while_its_requests_keep_coming(self):
        w1 = self.worker("w1", ["alpha", "beta"])
        self.worker("w2", ["gamma"])
        self.assertEqual("w2 holds beta, gamma", self.control("w2", "load beta"))
        self.assertEqual({"w1", "w2"}, set(self.predict("beta", 8)))
        # Another thread asks for beta, one request after another, each
        # marked with whether it was sent after the unload was answered.
        answered: list[tuple[str, bool]] = []
        failed: list[Exception] = []
        unloaded = threading.Event()
        stop = threading.Event()

        def ask() -> None:
            """Request after request for beta until stopped."""
            while not stop.is_set():
                after = unloaded.is_set()
                try:
                    answered.append((self.predict("beta")[0], after))
                except Exception as error:  # what the test fails on
                    failed.append(error)

        asking = threading.Thread(target=ask)
        asking.start()
        self.addCleanup(asking.join, 30)
        self.addCleanup(stop.set)
        wait_until(lambda: len(answered) >= 20, 30, "requests for beta flowing")
        self.assertEqual("w1 holds alpha", self.control("w1", "unload beta"))
        unloaded.set()
        self.assertEqual({}, w1.unloading, "freed before the answer")
        wait_until(lambda: sum(after for _, after in answered) >= 20, 30, "requests sent after the answer, answered")
        stop.set()
        asking.join(30)
        self.assertEqual([], failed, "no request for beta failed while it moved")
        self.assertIn("w1", {worker for worker, after in answered if not after}, "w1 served beta before")
        self.assertEqual({"w2"}, {worker for worker, after in answered if after}, "and none after the answer")

    def test_a_worker_without_a_filter_takes_every_model(self):
        self.worker("w1", ["alpha"])
        self.worker("g1", None)
        self.assertEqual({"g1"}, set(self.predict("epsilon", 4)), "nobody else takes it, and g1 loads it")
        self.assertEqual({"w1", "g1"}, set(self.predict("alpha", 8)), "every model, the ones others hold too")
        # Unloaded, a model is freed, and still taken: the next request loads it again.
        self.assertEqual("g1 takes every model, holds alpha", self.control("g1", "unload epsilon"))
        self.assertEqual(["g1"], self.predict("epsilon"))


class HeldWorker(Worker):
    """The worker with its first prediction held until the test releases
    it, as a slow model would hold it: what reaches the worker meanwhile
    waits in its queue, in the order it came."""

    def __init__(self, addresses: list[tuple[str, int]], name: str, models: list[str] | None):
        super().__init__(addresses, name, models)
        self.held = threading.Event()  # the first prediction is in predict()
        self.release = threading.Event()  # set by the test: it goes through

    def predict(self, mxmsg) -> None:
        """Hold the first prediction until released, then answer as the worker does."""
        if not self.held.is_set():
            self.held.set()
            self.release.wait()
        super().predict(mxmsg)


class UnloadTest(unittest.TestCase):
    """One multiplexer, so that what the client sends reaches the worker in
    the order it was sent: a request routed under the old filters comes
    after the unload, and is served."""

    def test_a_request_routed_before_the_unload_took_effect_is_served(self):
        cluster = Cluster(1, rules=RULES).__enter__()
        self.addCleanup(cluster.__exit__, None, None, None)
        served = BackendThread(lambda: HeldWorker(cluster.endpoints, "w1", ["beta"])).start()
        self.addCleanup(served.stop)
        worker = served.backend
        assert worker is not None
        self.addCleanup(worker.release.set)  # before the stop: a held handler would keep the thread
        client = ThreadedClient(cluster.endpoints, type=peers.MODEL_CLIENT)
        self.addCleanup(client.shutdown)
        answers: queue.Queue = queue.Queue()
        unloaded: queue.Queue = queue.Queue()
        client.query("first", type=types.PREDICT, key=model_key("beta"), callback=answers.put)
        self.assertTrue(worker.held.wait(15), "the first request reached the worker, which holds it")
        client.query("unload beta", type=types.MODEL_CONTROL, key=worker_key("w1"), callback=unloaded.put)
        # Routed to w1 under its filters as they are, since w1 has not read
        # the unload yet: it waits in the queue behind it.
        client.query("second", type=types.PREDICT, key=model_key("beta"), callback=answers.put)
        worker.release.set()
        for text in ("first", "second"):
            reply = answers.get(timeout=15)
            self.assertNotIsInstance(reply, Exception, text)
            self.assertEqual(f"beta({text}) by w1", reply.message.decode())
        reply = unloaded.get(timeout=15)
        self.assertNotIsInstance(reply, Exception)
        self.assertEqual("w1 holds no model", reply.message.decode())


class ProgramTest(unittest.TestCase):
    """worker.py and models_cli.py as the steps run them: two workers
    started with their models, predictions asked of them, a model moved
    from one to the other."""

    def setUp(self):
        self.cluster = Cluster(2, rules=RULES).__enter__()
        self.addCleanup(self.cluster.__exit__, None, None, None)
        self.addresses = ",".join(f"{host}:{port}" for host, port in self.cluster.endpoints)

    def start_worker(self, *options: str) -> None:
        """worker.py with `options`, once it says it is ready."""
        command = WITH_THE_TEST + [sys.executable, "worker.py", self.addresses, *options]
        worker = subprocess.Popen(command, cwd=HERE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
        self.addCleanup(end, worker)
        assert worker.stdout is not None
        self.assertTrue(worker.stdout.readline().startswith("ready: "))

    def cli(self, *arguments: str) -> str:
        """What models_cli.py prints."""
        command = [sys.executable, "models_cli.py", self.addresses, *arguments]
        return subprocess.run(command, cwd=HERE, capture_output=True, text=True, check=True, timeout=60).stdout

    def test_the_steps(self):
        self.start_worker("--name", "w1", "--models", "alpha,beta")
        self.start_worker("--name", "w2", "--models", "gamma")
        self.assertRegex(self.cli("predict", "alpha", "hi"), r"^alpha\(hi\) by w1, in \d+\.\d s\n$")
        self.assertRegex(self.cli("predict", "delta"), r"^failed: OperationFailed, in \d+\.\d s\n$")
        self.assertRegex(self.cli("load", "w2", "beta"), r"^w2 holds beta, gamma, in \d+\.\d s\n$")
        self.assertRegex(self.cli("unload", "w1", "beta"), r"^w1 holds alpha, in \d+\.\d s\n$")
        self.assertEqual("4 answers, w2 4; 0 failed\n", self.cli("predict", "beta", "--repeat", "4", "--tally"))


def end(process: subprocess.Popen) -> None:
    """Kill a process the test started, unless it is gone already, and reap it."""
    if process.poll() is None:
        process.kill()
    process.wait(10)
    if process.stdout is not None:
        process.stdout.close()


class ConstantsTest(unittest.TestCase):
    """The committed constants are what mxcontrol writes from the rules file
    as it is now: a change to the file that left them behind fails here."""

    def test_the_constants_are_generated_from_the_rules_file(self):
        out = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, out, True)
        written = [os.path.join(out, name) for name in ("multiplexer_constants.py", "multiplexer_constants.pyi")]
        command = [mxcontrol_path(), "generate_constants", "models.rules", "--python", written[0], "--pyi", written[1]]
        subprocess.run(command, cwd=HERE, check=True, capture_output=True)  # the file's name as the header records it
        for path in written:
            with open(path) as generated, open(os.path.join(HERE, os.path.basename(path))) as committed:
                self.assertEqual(committed.read(), generated.read(), os.path.basename(path))


if __name__ == "__main__":
    unittest.main()
