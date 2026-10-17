"""run_multiplexer outlives a file that reaches the process's size limit
(RLIMIT_FSIZE: ulimit -f, systemd's LimitFSIZE=): the recording that
reaches it stops with its ERROR, and the multiplexer serves on, where the
write past the limit raised SIGXFSZ, whose default action ended the
process. Counted, not timed: a peer sends itself messages of 64 KiB, each
one recorded whole, reading each back, until the log says the recording
stopped, which a 1 MiB limit makes about the sixteenth; then one more comes
back. The bound on each wait only detects a failure."""

import os
import resource
import subprocess
import tempfile
import time
import unittest

from multiplexer.multiplexer_constants import peers, types
from multiplexer.testing import mx_runfile, runfile
from multiplexer.testing.raw_peer import RawPeer

MXCONTROL = mx_runfile("mxcontrol/mxcontrol")
RULES = runfile("tests/testing.rules")
LIMIT = 1 << 20  # the most bytes a file of the multiplexer's may hold
PAYLOAD = b"r" * (64 * 1024)
BOUND = 30  # seconds a step may take: a failure detector only


def limited() -> None:
    """In the child, before the exec: files of at most LIMIT bytes."""
    resource.setrlimit(resource.RLIMIT_FSIZE, (LIMIT, LIMIT))


class FileSizeLimitTest(unittest.TestCase):
    """See the module docstring."""

    def test_a_recording_at_the_limit_stops_and_the_multiplexer_serves_on(self) -> None:
        directory = tempfile.mkdtemp(dir=os.environ.get("TEST_TMPDIR"))
        port_file = os.path.join(directory, "mx.port")
        log_path = os.path.join(directory, "mx.log")
        with open(log_path, "wb") as log:
            run = subprocess.Popen(
                [MXCONTROL, "run_multiplexer", "--rules", RULES, "--address", "127.0.0.1:0", "--port-file", port_file]
                + ["--record", os.path.join(directory, "mx.rec")],
                stdin=subprocess.DEVNULL,
                stdout=log,
                stderr=subprocess.STDOUT,
                preexec_fn=limited,
            )
        try:
            deadline = time.monotonic() + BOUND
            while not os.path.exists(port_file):  # renamed into place whole
                self.assertIsNone(run.poll(), "the multiplexer ended before it listened")
                self.assertLess(time.monotonic(), deadline, "no port file")
                time.sleep(0.01)
            with open(port_file) as text:
                host, port = text.read().strip().rsplit(":", 1)
            peer = RawPeer((host, int(port)), peers.TEST_EVENT_CLIENT)
            try:
                peer.handshake()
                stopped = False
                for sent in range(4 * LIMIT // len(PAYLOAD)):
                    peer.send(PAYLOAD, types.TEST_EVENT, to=peer.instance_id)
                    self.assertEqual(PAYLOAD, peer.receive(timeout=BOUND).message, "message %d" % sent)
                    with open(log_path, "rb") as written:
                        stopped = b"recording stopped" in written.read()
                    if stopped:
                        break
                self.assertTrue(stopped, "the recording never reached the limit")
                peer.send(b"after", types.TEST_EVENT, to=peer.instance_id)
                self.assertEqual(b"after", peer.receive(timeout=BOUND).message)
            finally:
                peer.close()
            self.assertIsNone(run.poll(), "the multiplexer ended")
        finally:
            run.terminate()
            try:
                run.wait(BOUND)
            except subprocess.TimeoutExpired:
                run.kill()
                run.wait()
        with open(log_path, "rb") as written:
            self.assertIn(b"write failed; recording stopped", written.read())


if __name__ == "__main__":
    unittest.main()
