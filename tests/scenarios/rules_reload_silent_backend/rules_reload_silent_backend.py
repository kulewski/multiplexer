"""a silent backend is dropped on time while rules reloads keep coming, and a client a reload made passive is not.

The multiplexer drops an active peer that sent nothing for 30 + 60 s. A
reload applies the file's passive flags to the peers connected: a flag
set again unchanged must not start that count over, or reloads less than
90 s apart would keep a hung backend connected for good, and a flag
turned on ends it. One multiplexer runs a copy of the rules file with
`--rules-check-interval 0`; a raw peer of the client type
TEST_ACTIVE_CLIENT and then one of the backend type TEST_BACKEND_A say
their welcomes and nothing more. Every 10 s the file, with
TEST_ACTIVE_CLIENT made passive, gets a new comment and SIGHUP reloads
it, and the backend is closed before the fifteenth reload. The client,
connected first, would have been closed first; it is still connected,
and sent a heartbeat for one it sends. Slow: it really waits out the
drop.
"""

import os
import re
import unittest
import zlib

from tests import harness
from tests.harness import Cluster, Mx, constants as C, output_dir, wait_until
from multiplexer.testing.raw_peer import RawPeer

ACTIVE_CLIENT = re.compile(r'(name: "TEST_ACTIVE_CLIENT"\n)')
RELOADS = 15  # 10 s apart: 150 s, well past the 90 s the drop takes
PERIOD = 10  # s between reloads, under the 30 s a reload used to start the count over from
BOUND = 30  # seconds a reload may take to reach the log, or a heartbeat to come: a failure detector only


def fingerprint(text: str) -> str:
    """What the multiplexer reports for a rules file: the CRC-32 of its bytes, eight hex digits."""
    return "%08x" % zlib.crc32(text.encode())


def reload(multiplexer: Mx, path: str, text: str) -> None:
    """Write `text` as the rules file, SIGHUP, and wait for the log to say it is in use."""
    with open(path, "w") as rules:
        rules.write(text)
    multiplexer.reload_rules()
    reloaded = "-> %s," % fingerprint(text)
    wait_until(lambda: multiplexer.log_contains(reloaded), BOUND, "the reload in the log")


def still_open(peer: RawPeer) -> bool:
    """Read what waits in the peer's socket, without waiting for more: False
    when it ends in the other side's close."""
    peer.sock.setblocking(False)
    try:
        while peer.sock.recv(65536):
            pass
        return False
    except BlockingIOError:
        return True
    finally:
        peer.sock.setblocking(True)


class RulesReloadSilentBackend(unittest.TestCase):
    """Reloads every 10 s, a backend and a client that send nothing: the backend dropped all the same."""

    def test_a_silent_backend_is_dropped_through_reloads(self):
        with open(harness.CONFIG.rules) as shipped:
            original = shipped.read()
        self.assertTrue(ACTIVE_CLIENT.search(original), "the test rules")
        passive_client = ACTIVE_CLIENT.sub(r"\1    is_passive: true\n", original)
        path = os.path.join(output_dir(), "silent.rules")
        with open(path, "w") as rules:
            rules.write(original)
        with Cluster(1, rules=path, rules_check_interval=0) as cluster:
            quiet = RawPeer(cluster.endpoints[0], C.peers.TEST_ACTIVE_CLIENT)  # first: its drop would come first
            quiet.handshake()
            silent = RawPeer(cluster.endpoints[0], C.peers.TEST_BACKEND_A)
            silent.handshake()
            for count in range(RELOADS):
                reload(cluster.mx[0], path, passive_client + "# reload %d\n" % count)
                if silent.closed_by_peer(timeout=PERIOD):  # the heartbeats it is sent are read and dropped
                    break
            else:
                self.fail("the silent backend still connected after %d reloads %d s apart" % (RELOADS, PERIOD))
            # Closed, the client's connection would have been closed before the
            # backend's, its end in its socket by now.
            self.assertTrue(still_open(quiet), "the client made passive was dropped for silence")
            quiet.send(b"", C.types.HEARTBIT)
            quiet.receive_type(C.types.HEARTBIT, timeout=BOUND)  # the one this frame is owed
            silent.close()
            quiet.close()


if __name__ == "__main__":
    harness.main()
