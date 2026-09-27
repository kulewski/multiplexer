"""A pinned lane's flushing send with no deadline, `timeout=-1`, waits as
long as the lane's connection lives, however long its multiplexer does not
read: its message had DEFAULT_TIMEOUT, so at 10 s it was dropped and the
send raised NotConnected with the connection alive. Slow on purpose: the
multiplexer stays frozen past DEFAULT_TIMEOUT.
"""

import threading
import unittest

from multiplexer.clients import Client
from multiplexer.multiplexer_constants import peers, types
from multiplexer.mxclient import DEFAULT_TIMEOUT
from multiplexer.testing import Cluster, FakePeer, runfile
from multiplexer.testing.buffers import past_the_queue

RULES = runfile("tests/testing.rules")  # the file the constants were generated from

EVENT = types.PYTHON_TEST_REQUEST
CHUNK = b"x" * (16 * 1024)


class NoDeadlineSendTest(unittest.TestCase):
    """One frozen multiplexer and a pinned lane on it."""

    def test_a_pinned_send_with_no_deadline_outlasts_the_default(self) -> None:
        """The multiplexer frozen with the lane's connection full for
        DEFAULT_TIMEOUT and more: the send waits, nothing is dropped, and
        once the multiplexer reads again the message goes out through the
        lane's connection."""
        with Cluster(1, rules=RULES) as cluster, FakePeer(cluster, peers.PYTHON_TEST_SERVER) as backend:
            client = Client(cluster.endpoints, type=peers.WEBSITE)
            try:
                lane = client.lane(pinned=True)
                client.send_message(b"first", type=EVENT, multiplexer=lane, flush=True)
                cluster.mx[0].pause()
                resume = threading.Timer(DEFAULT_TIMEOUT + 1.5, cluster.mx[0].resume)
                resume.start()
                try:
                    for payload in past_the_queue(CHUNK):  # waiting past the freeze too
                        client.send_message(payload, type=types.TEST_UNROUTED, multiplexer=lane, timeout=60)
                    client.send_message(b"patient", type=EVENT, multiplexer=lane, flush=True, timeout=-1)
                finally:
                    resume.join()
                self.assertEqual(0, client.dropped, "nothing given up on while the connection lived")
                backend.wait_for(EVENT, matching=lambda mxmsg: mxmsg.message == b"patient", timeout=30)
            finally:
                client.shutdown()


if __name__ == "__main__":
    unittest.main()
