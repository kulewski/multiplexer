"""--peers-file is written at most once every 10 ms, not once a peer: N
peers arriving at once, or leaving at once, cost a few writes of
the file, where each arrival and each departure rewrote the whole file,
O(N^2) on the io thread at start, in reconnect storms and at stop.
Counted, not timed: the multiplexer logs each write, and the bursts are
made by freezing it (SIGSTOP) while the peers send their welcomes, or
close, so that it meets them all at once. And a reload that renames a peer
type rewrites the file with the new name, where it kept the old one until
the next arrival or departure.
"""

import os
import shutil
import tempfile
import unittest
from unittest import mock

from multiplexer.Multiplexer_pb2 import WelcomeMessage
from multiplexer.multiplexer_constants import peers, types
from multiplexer.testing import Cluster, Mx, runfile, wait_until
from multiplexer.testing.raw_peer import RawPeer

RULES = runfile("tests/testing.rules")
PEERS = 40


def writes(mx: Mx) -> int:
    """How many times the multiplexer has written its peers file so far, from its log."""
    with open(mx.log_path, "rb") as log:
        return log.read().count(b"peers file written:")


class PeersFileTest(unittest.TestCase):
    """See the module docstring."""

    def test_a_burst_of_arrivals_and_one_of_departures_cost_a_few_writes(self) -> None:
        # The multiplexer logs each write at DEBUG, HIGHVERBOSITY, which mxcontrol leaves out by default.
        with mock.patch.dict(os.environ, {"MX_LOG_VERBOSITY": "DEBUG:HIGH"}), Cluster(1, rules=RULES) as cluster:
            mx = cluster.mx[0]
            connected: list[RawPeer] = []
            try:
                before = writes(mx)
                mx.pause()
                for _ in range(PEERS):
                    peer = RawPeer(cluster.endpoints[0], peers.PYTHON_TEST_SERVER)
                    # The welcome, waiting in the socket for the frozen multiplexer.
                    peer.send(
                        WelcomeMessage(type=peer.peer_type, id=peer.instance_id).SerializeToString(),
                        types.CONNECTION_WELCOME,
                    )
                    connected.append(peer)
                mx.resume()
                cluster.wait_for_peer(peers.PYTHON_TEST_SERVER, count=PEERS)
                arrivals = writes(mx) - before
                before = writes(mx)
                mx.pause()
                for peer in connected:
                    peer.close()
                mx.resume()
                cluster.wait_for_peer_gone(peers.PYTHON_TEST_SERVER)
                departures = writes(mx) - before
            finally:
                for peer in connected:
                    peer.close()
            print("writes for %d arrivals: %d, for %d departures: %d" % (PEERS, arrivals, PEERS, departures))
            self.assertLessEqual(arrivals, PEERS // 4, "writes of the file for %d arrivals at once" % PEERS)
            self.assertLessEqual(departures, PEERS // 4, "writes of the file for %d departures at once" % PEERS)

    def test_a_reload_that_renames_a_peer_type_rewrites_the_file(self) -> None:
        directory = tempfile.mkdtemp(dir=os.environ.get("TEST_TMPDIR"))
        self.addCleanup(shutil.rmtree, directory)
        path = os.path.join(directory, "renamed.rules")
        shutil.copy(RULES, path)
        with Cluster(1, rules=path) as cluster:
            mx = cluster.mx[0]
            peer = RawPeer(cluster.endpoints[0], peers.PYTHON_TEST_SERVER)
            try:
                peer.handshake()
                cluster.wait_for_peer(peers.PYTHON_TEST_SERVER)
                with open(path) as rules:
                    renamed = rules.read().replace('"PYTHON_TEST_SERVER"', '"PYTHON_TEST_SERVER_RENAMED"')
                with open(path + ".tmp", "w") as rules:
                    rules.write(renamed)
                os.rename(path + ".tmp", path)
                mx.reload_rules()

                def renamed_in_the_file() -> bool:
                    """Whether the peers file names the peer's type as the new rules do."""
                    with open(mx.peers_file) as listed:
                        return " PYTHON_TEST_SERVER_RENAMED %d" % peers.PYTHON_TEST_SERVER in listed.read()

                wait_until(renamed_in_the_file, 30, "the peers file names the type as the reloaded rules do")
            finally:
                peer.close()


if __name__ == "__main__":
    unittest.main()
