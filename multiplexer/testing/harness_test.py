"""The harness itself: a RawPeer's receive_type() ends at its timeout
however many heartbeats come; a killed multiplexer lists no peers; a
recording outlives a restart of its multiplexer; a BackendThread whose
start() ran out of time closes the backend it builds after; an Event line
that does not parse is kept as such; a Cluster that cannot start every
multiplexer stops the ones it started; $MX_TEST_OUTPUT gives every process
a directory of its own; a test process that is killed takes its
multiplexers with it; and log_contains() past a log_mark() sees only what
came after the mark, where it searched the whole log, so that a wait for a
line an earlier step wrote ended at once. Each failed on the harness
before. Counted, not
timed, but for receive_type(), whose timeout is the thing tested: every
other wait is a failure detector.
"""

import os
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import unittest

from multiplexer import recording
from multiplexer.multiplexer_constants import peers, types
from multiplexer.servers import BaseMultiplexerServer
from multiplexer.testing import BackendThread, Cluster, Role, TestClient, runfile, wait_until
from multiplexer.testing.raw_peer import RawPeer

RULES = runfile("tests/testing.rules")  # the file the constants were generated from


def alive(pid: int) -> bool:
    """Whether process `pid` exists and is not a zombie."""
    try:
        with open("/proc/%d/stat" % pid) as stat:
            return stat.read().rsplit(")", 1)[1].split()[0] != "Z"
    except OSError:
        return False


class Silent(BaseMultiplexerServer):
    """A backend that serves nothing."""

    def __init__(self, addresses: list[tuple[str, int]]):
        super().__init__(addresses, type=peers.PYTHON_TEST_SERVER)


class HarnessTest(unittest.TestCase):
    """See the module docstring."""

    def test_receive_type_ends_at_its_timeout_through_heartbeats(self) -> None:
        """A raw peer of an active type gets a heartbeat every 3 s; a wait of
        4 s for a type nobody sends ends with TimeoutError, where each
        heartbeat gave it 4 s more, until the multiplexer dropped the peer."""
        with Cluster(1, rules=RULES) as cluster:
            peer = RawPeer(cluster.endpoints[0], peers.TEST_EVENT_BACKEND)
            peer.handshake()
            with self.assertRaises(TimeoutError):
                peer.receive_type(types.TEST_EVENT, timeout=4)
            peer.close()

    def test_a_killed_multiplexer_lists_no_peers(self) -> None:
        """The peers file a killed process left is gone: its peers are not
        listed, and a wait for them to be gone ends."""
        with Cluster(1, rules=RULES) as cluster:
            peer = RawPeer(cluster.endpoints[0], peers.TEST_EVENT_BACKEND)
            peer.handshake()
            cluster.wait_for_peer(peers.TEST_EVENT_BACKEND)
            cluster.mx[0].kill()
            self.assertEqual([], cluster.mx[0].connected_peers())
            cluster.wait_for_peer_gone(peers.TEST_EVENT_BACKEND, timeout=5)
            peer.close()

    def test_a_log_mark_hides_what_came_before(self) -> None:
        """The start's line is in the log, and not past a mark taken after
        it; a line logged after the mark is."""
        with Cluster(1, rules=RULES) as cluster:
            multiplexer = cluster.mx[0]
            self.assertTrue(multiplexer.log_contains("rules loaded from"))
            mark = multiplexer.log_mark()
            self.assertFalse(multiplexer.log_contains("rules loaded from", since=mark))
            peer = RawPeer(cluster.endpoints[0], peers.TEST_EVENT_BACKEND)
            peer.handshake()
            wait_until(lambda: multiplexer.log_contains("registered connection", since=mark), 30, "the arrival")
            peer.close()

    def test_a_recording_outlives_a_restart(self) -> None:
        """A message recorded before a restart is in the file after it,
        with one recorded after, each process's records after its header."""
        with Cluster(1, rules=RULES, record=True) as cluster:
            with TestClient(cluster, peers.WEBSITE) as client:
                before = client.send(b"before", types.TEST_UNROUTED)
            cluster.mx[0].restart()
            with TestClient(cluster, peers.WEBSITE) as client:
                after = client.send(b"after", types.TEST_UNROUTED)
            cluster.mx[0].stop()
            records = list(recording.read(cluster.mx[0].record_file))
        self.assertEqual(2, sum(1 for record in records if record.HasField("header")))
        routed = {record.routed.id for record in records if record.HasField("routed")}
        self.assertLessEqual({before, after}, routed)

    def test_a_backend_built_after_start_ran_out_of_time_is_closed(self) -> None:
        """start() gives up while the factory still builds; the backend it
        then builds is closed rather than served, so its peer goes."""
        with Cluster(1, rules=RULES) as cluster:
            release = threading.Event()

            def slow() -> Silent:
                """The backend, once the test lets it be built."""
                self.assertTrue(release.wait(30))
                return Silent(cluster.endpoints)

            served = BackendThread(slow)
            with self.assertRaises(TimeoutError):
                served.start(timeout=0)
            release.set()
            served._thread.join(30)
            self.assertFalse(served._thread.is_alive(), "the backend was closed, not served")
            self.assertIsNone(served.backend)
            cluster.wait_for_peer_gone(peers.PYTHON_TEST_SERVER, timeout=5)

    def test_an_event_line_that_does_not_parse_is_kept_as_such(self) -> None:
        """A string field with bytes that are not UTF-8, as the C++ roles
        printed a short binary payload, is an "unparsed" event, which fails
        a wait, where it became a "stdout" one and the event went unseen."""
        event = Role.parse_event('event: "received" payload: "\\377"')
        self.assertEqual("unparsed", event["event"])
        self.assertEqual({"event": "stdout", "line": "something else"}, Role.parse_event("something else"))

    def test_a_cluster_that_cannot_start_stops_what_it_started(self) -> None:
        """The second multiplexer cannot listen on a port that a socket of
        the test's own holds, whoever runs the test: the first, started, is
        stopped before the error goes on."""
        with socket.create_server(("127.0.0.1", 0)) as taken:
            cluster = Cluster(2, rules=RULES)
            cluster.mx[1].address = "127.0.0.1:%d" % taken.getsockname()[1]
            with self.assertRaises(RuntimeError):
                with cluster:
                    pass
        self.assertFalse(cluster.mx[0].running)

    def test_mx_test_output_gives_every_process_a_directory(self) -> None:
        """Outside Bazel, with $MX_TEST_OUTPUT set, each process writes to a
        directory of its own in it, kept, where all wrote to it alike."""
        with tempfile.TemporaryDirectory() as shared:
            env = {name: value for name, value in os.environ.items() if not name.startswith("TEST_")}
            env.update(MX_TEST_OUTPUT=shared, PYTHONPATH=os.pathsep.join(sys.path))
            command = [sys.executable, "-c", "from multiplexer.testing import output_dir; print(output_dir())"]
            first, second = (subprocess.check_output(command, env=env, text=True).strip() for _ in range(2))
            self.assertEqual((shared, shared), (os.path.dirname(first), os.path.dirname(second)))
            self.assertNotEqual(first, second)
            self.assertTrue(os.path.isdir(first), "kept")

    def test_a_killed_test_process_takes_its_multiplexer_with_it(self) -> None:
        """A process that starts a cluster and is killed: its multiplexer
        sees the harness's pipe end and exits, where it ran on for ever."""
        code = (
            "import sys, time\n"
            "from multiplexer.testing import Cluster\n"
            "cluster = Cluster(1, rules=sys.argv[1]).__enter__()\n"
            "print(cluster.mx[0].proc.pid, flush=True)\n"
            "time.sleep(600)\n"
        )
        with tempfile.TemporaryDirectory() as outputs:
            env = dict(os.environ, TEST_UNDECLARED_OUTPUTS_DIR=outputs, PYTHONPATH=os.pathsep.join(sys.path))
            parent = subprocess.Popen([sys.executable, "-c", code, RULES], stdout=subprocess.PIPE, env=env, text=True)
            assert parent.stdout is not None
            pid = int(parent.stdout.readline())
            try:
                self.assertTrue(alive(pid))
                parent.kill()
                parent.wait(30)
                wait_until(lambda: not alive(pid), 30, "the multiplexer to exit after its test process died")
            finally:
                if alive(pid):
                    os.kill(pid, signal.SIGKILL)
                parent.stdout.close()


if __name__ == "__main__":
    unittest.main()
