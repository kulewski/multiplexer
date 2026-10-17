"""A workflow is text, a str in Python: a request's workflow comes back on
its reply as the same str, a handler logs with it as it is, and a recording
reads it back as that str, a character beyond ASCII included. Messages
carried it as bytes, which refused a str, while the log took a str and the
recording declared a string, so that a handler logged
request.workflow.decode() and a reader of a recording got a str or bytes
by the content. Counted, not timed: the query waits for its reply, and the
recording is read once the multiplexer has stopped.
"""

import contextlib
import os
import sys
import tempfile
import unittest
from collections.abc import Iterator

from multiplexer import recording
from multiplexer.clients import Client
from multiplexer.multiplexer_constants import peers, types
from multiplexer.mxlog import LOWVERBOSITY, WARNING, log
from multiplexer.servers import BaseMultiplexerServer
from multiplexer.testing import BackendThread, Cluster, runfile

RULES = runfile("tests/testing.rules")  # the file the constants were generated from
WORKFLOW = "trace ✓ 42"


@contextlib.contextmanager
def stderr_to(path: str) -> Iterator[None]:
    """Descriptor 2, which the C++ side of an in-process peer logs to, goes
    to `path` meanwhile."""
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


class Backend(BaseMultiplexerServer):
    """Logs with the request's workflow as it came, then answers the
    request with its payload upper-cased."""

    def __init__(self, addresses: list[tuple[str, int]]):
        super().__init__(addresses, type=peers.PYTHON_TEST_SERVER)

    def handle_message(self, mxmsg) -> None:
        log(WARNING, LOWVERBOSITY, text="handling a request", flow=mxmsg.workflow)
        self.send_message(message=mxmsg.message.upper(), type=types.PYTHON_TEST_RESPONSE)


class WorkflowTest(unittest.TestCase):
    """See the module docstring."""

    def test_a_workflow_is_text_from_request_to_reply_log_and_recording(self) -> None:
        with tempfile.TemporaryDirectory(dir=os.environ.get("TEST_TMPDIR")) as scratch:
            log_path = os.path.join(scratch, "backend.log")
            with Cluster(1, rules=RULES, record=True) as cluster:
                with stderr_to(log_path), BackendThread(lambda: Backend(cluster.endpoints)):
                    with Client(cluster.endpoints, type=peers.WEBSITE) as client:
                        request = client.new_message(
                            type=types.PYTHON_TEST_REQUEST, message=b"question", workflow=WORKFLOW
                        )
                        reply = client.query(request, timeout=10)
                cluster.mx[0].stop()
                routed = [
                    record.routed
                    for record in recording.read(cluster.mx[0].record_file)
                    if record.HasField("routed") and record.routed.type == types.PYTHON_TEST_RESPONSE
                ]
            with open(log_path, "rb") as written:
                logged = written.read().decode("utf-8", "replace")
        self.assertEqual((b"QUESTION", WORKFLOW), (reply.message, reply.workflow))
        self.assertIsInstance(reply.workflow, str)
        self.assertIn('flw="%s"' % WORKFLOW, logged, "the handler's line carries the workflow")
        self.assertEqual([WORKFLOW], [r.workflow for r in routed], "the reply's record")
        self.assertIsInstance(routed[0].workflow, str)


if __name__ == "__main__":
    unittest.main()
