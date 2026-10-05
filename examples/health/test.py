"""The example's test on the library's harness (docs/api_python.md,
"Testing"): a real multiplexer from `Cluster`, a worker on a
`BackendThread` whose work the test holds and releases, and its health
endpoint asked over HTTP: healthy, then unhealthy once the held handler
has kept the loop from coming round for longer than STALE_SECONDS, while
the multiplexer still lists the worker, then healthy again once
released, the request answered. Then the program as the manifest runs
it, its probe answered and its preStop hook draining it; and the
committed constants checked against the rules file. Needs PyYAML for
the manifest, which test.sh arranges; the multiplexers are the mxcontrol
the package installed unless MXCONTROL names another; `python -m
unittest -v test`.

The walkthrough's "Testing it with the harness" walks through this file."""

import http.client
import os
import queue
import shutil
import subprocess
import sys
import tempfile
import threading
import unittest

import yaml

HERE = os.path.dirname(os.path.abspath(__file__))
# The example's own modules import by name, as they do when its scripts run
# from its directory; this test may run from anywhere, so the example's
# directory goes first on the path.
sys.path.insert(0, HERE)

from multiplexer.testing import BackendThread, Cluster, mxcontrol_path, wait_until
from multiplexer.threaded_client import ThreadedClient

from backend import HEALTH_PATH, HealthEndpoint, Worker
from multiplexer_constants import peers, types

RULES = os.path.join(HERE, "health.rules")  # what the constants were generated from; Cluster's multiplexers read it
MANIFEST = os.path.join(HERE, "deployment.yaml")
STALE_SECONDS = 1.0  # the endpoint's limit in the tests; the worker's loop comes round every 0.05 s when idle
# A worker process dies with the test process however that ends, where util-linux's setpriv is there.
WITH_THE_TEST = ["setpriv", "--pdeathsig", "KILL", "--"] if shutil.which("setpriv") else []


def health_status(port: int, path: str = HEALTH_PATH) -> int:
    """The status the endpoint answers a GET with, as a probe would get it."""
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    try:
        connection.request("GET", path)
        return connection.getresponse().status
    finally:
        connection.close()


class HeldWorker(Worker):
    """The worker with its work held: a handler waits there until the test
    releases it, every handler after it too, as a call to a service that
    stopped answering would."""

    def __init__(self, addresses: list[tuple[str, int]]):
        super().__init__(addresses, name="held")
        self.instance_id = self.conn.instance_id  # read on the thread that serves it, for the test to compare
        self.holding = threading.Event()  # a handler is in work()
        self.release = threading.Event()  # set by the test: every handler goes through

    def work(self, seconds: float) -> None:
        """Wait for the test instead of working."""
        self.holding.set()
        self.release.wait()


class HealthTest(unittest.TestCase):
    """One multiplexer, the held worker served on a thread, its endpoint on
    a port the system picked, and a client to ask for work."""

    def setUp(self):
        self.cluster = Cluster(1, rules=RULES).__enter__()
        self.addCleanup(self.cluster.__exit__, None, None, None)
        served = BackendThread(lambda: HeldWorker(self.cluster.endpoints)).start()
        self.addCleanup(served.stop)
        assert served.backend is not None
        self.worker = served.backend
        self.addCleanup(self.worker.release.set)  # before the stop: a held handler would keep the thread
        self.health = HealthEndpoint(self.worker, 0, STALE_SECONDS).start()
        self.addCleanup(self.health.close)
        self.client = ThreadedClient(self.cluster.endpoints, type=peers.WORK_CLIENT)
        self.addCleanup(self.client.shutdown)

    def status(self) -> int:
        """What a probe gets now."""
        return health_status(self.health.port)

    def test_unhealthy_while_a_handler_is_held_and_healthy_again(self):
        wait_until(lambda: self.status() == 200, 15, "the endpoint healthy while the loop comes round")
        answers: queue.Queue = queue.Queue()
        self.client.query("2", type=types.WORK, timeout=60, callback=answers.put)
        self.assertTrue(self.worker.holding.wait(15), "the request reached the worker and its handler holds")
        wait_until(lambda: self.status() == 503, STALE_SECONDS + 15, "the endpoint unhealthy, the loop held")
        listed = [peer for peer, _, _ in self.cluster.mx[0].connected_peers()]
        self.assertIn(self.worker.instance_id, listed, "the multiplexer still has the worker, and routes it its share")
        self.worker.release.set()
        wait_until(lambda: self.status() == 200, 15, "the endpoint healthy again, the loop coming round")
        reply = answers.get(timeout=15)
        self.assertNotIsInstance(reply, Exception)
        self.assertEqual(b"worked 2 s, by held", reply.message)

    def test_a_path_other_than_the_probes_is_not_found(self):
        self.assertEqual(404, health_status(self.health.port, "/"))


class ProgramTest(unittest.TestCase):
    """backend.py as the manifest runs it: the container's command, with
    the test's multiplexers, a free port and a file of the test's own in
    place of the cluster's; the liveness probe's request; and the preStop
    hook, which asks the worker to leave and returns once it has."""

    def setUp(self):
        with open(MANIFEST) as f:
            deployment = yaml.safe_load(f)
        self.container = deployment["spec"]["template"]["spec"]["containers"][0]
        self.cluster = Cluster(2, rules=RULES).__enter__()
        self.addCleanup(self.cluster.__exit__, None, None, None)
        self.directory = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.directory, True)

    def test_the_manifests_command_probe_and_hook(self):
        command = list(self.container["command"])
        probe = self.container["livenessProbe"]["httpGet"]
        ports = {port["name"]: port["containerPort"] for port in self.container["ports"]}
        self.assertEqual("python", command[0])
        self.assertEqual(
            str(ports[probe["port"]]), option(command, "--health-port"), "the probe asks the worker's port"
        )
        drain_file = option(command, "--drain-file")
        hook = self.container["lifecycle"]["preStop"]["exec"]["command"]
        self.assertIn(drain_file, hook[-1], "the hook writes the file the worker watches")
        # The test's multiplexers, a port the system picks and a file of the test's own.
        leave = os.path.join(self.directory, "leave")
        addresses = ",".join(f"{host}:{port}" for host, port in self.cluster.endpoints)
        command = [sys.executable, command[1], addresses] + command[3:]
        command[command.index("--health-port") + 1] = "0"
        command[command.index("--drain-file") + 1] = leave
        worker = subprocess.Popen(
            WITH_THE_TEST + command, cwd=HERE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True
        )
        self.addCleanup(end, worker)
        assert worker.stdout is not None
        ready = worker.stdout.readline()
        self.assertTrue(ready.startswith("ready: worker"), ready)
        port = int(ready.rsplit(" ", 1)[1])
        self.cluster.wait_for_peer(peers.WORKER)
        self.assertEqual(200, health_status(port, probe["path"]))
        subprocess.run([part.replace(drain_file, leave) for part in hook], check=True, timeout=60)
        self.assertFalse(os.path.exists(leave), "the hook returned once the worker had left and removed the file")
        self.assertEqual(0, worker.wait(15))
        self.assertEqual("left after 0 requests\n", worker.stdout.read())
        self.cluster.wait_for_peer_gone(peers.WORKER)


def option(command: list[str], name: str) -> str:
    """The value that follows `name` on a command line."""
    return command[command.index(name) + 1]


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
        written = [os.path.join(out, name) for name in ("multiplexer_constants.py", "multiplexer_constants.pyi")]
        command = [mxcontrol_path(), "generate_constants", "health.rules", "--python", written[0], "--pyi", written[1]]
        subprocess.run(command, cwd=HERE, check=True, capture_output=True)  # the file's name as the header records it
        for path in written:
            with open(path) as generated, open(os.path.join(HERE, os.path.basename(path))) as committed:
                self.assertEqual(committed.read(), generated.read(), os.path.basename(path))


if __name__ == "__main__":
    unittest.main()
