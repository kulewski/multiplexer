"""A plain server keeps a DELIVERY_ERROR for a message of its own to
itself, quietly: an event of its, whose rule reports delivery errors,
sent where nobody takes it, came back to BaseMultiplexerServer as an
"unknown meta-packet", logged at ERROR, and a WARNING that nothing
answered it, every time; the threaded server hands it to handle_message()
instead, as documented. Ordered, not timed: the event goes out in the
handling of a request, before its reply, so that the server takes the
DELIVERY_ERROR before a second request the client sends once that reply
came back; the log of the process is read after the second reply."""

import contextlib
import os
import sys
import tempfile
import unittest
from typing import Iterator

from multiplexer.multiplexer_constants import peers, types
from multiplexer.testing import Cluster, FakePeer, TestClient, runfile

RULES = runfile("tests/testing.rules")  # the file the constants were generated from


@contextlib.contextmanager
def stderr_to(path: str) -> Iterator[None]:
    """Descriptor 2, which the server in this process logs to, goes to `path` meanwhile."""
    sys.stderr.flush()
    saved = os.dup(2)
    with open(path, "wb") as target:
        os.dup2(target.fileno(), 2)
    try:
        yield
    finally:
        sys.stderr.flush()
        os.dup2(saved, 2)
        os.close(saved)


class PlainDeliveryErrorTest(unittest.TestCase):
    """See the module docstring."""

    def test_a_delivery_error_for_an_event_of_its_own_is_kept_quietly(self) -> None:
        log_path = os.path.join(tempfile.mkdtemp(), "stderr")
        with Cluster(1, rules=RULES) as cluster:
            with stderr_to(log_path), FakePeer(cluster, peers.PYTHON_TEST_SERVER) as peer:

                def publishes(mxmsg) -> bytes:
                    """An event nobody takes, its rule reporting that, then the reply."""
                    backend = peer.backend
                    assert backend is not None
                    backend.send_message(message=b"nobody", type=types.TEST_EVENT, to=0, references=0)
                    return mxmsg.message.upper()

                peer.on(types.PYTHON_TEST_REQUEST, publishes, types.PYTHON_TEST_RESPONSE)
                with TestClient(cluster, peers.WEBSITE) as client:
                    self.assertEqual(b"ONE", client.query(b"one", types.PYTHON_TEST_REQUEST).message)
                    self.assertEqual(b"TWO", client.query(b"two", types.PYTHON_TEST_REQUEST).message)
            with open(log_path, errors="replace") as log:
                logged = log.read()
        self.assertEqual(0, logged.count("unknown meta-packet"), "logged as an unknown meta-packet")
        self.assertEqual(0, logged.count("w/o any response"), "logged as not answered")
        self.assertEqual([], peer.messages(types.DELIVERY_ERROR), "kept, not handed to handle_message()")


if __name__ == "__main__":
    unittest.main()
