"""A handler's own OperationTimedOut, a query of its own that timed out
say, goes by on_handler_exception() as any exception does: returning False
ends serve_forever(), which raises it, where serve_forever() took it for
its poll's timeout and served on. The poll's own timeouts still only end
an iteration. The C++ class's twin is in backend/serve_thread_test.cc.
"""

import unittest
from typing import Any

from multiplexer.multiplexer_constants import peers, types
from multiplexer.mxclient import OperationTimedOut
from multiplexer.servers import BaseMultiplexerServer
from multiplexer.testing import BackendThread, Cluster, TestClient, runfile, wait_until

RULES = runfile("tests/testing.rules")


class TimingOut(BaseMultiplexerServer):
    """Raises an OperationTimedOut of its own on every request, and asks to
    stop on what its handler raises."""

    def __init__(self, addresses: list[tuple[str, int]]):
        super().__init__(addresses, type=peers.PYTHON_TEST_SERVER)
        self.iterations = 0

    def handle_message(self, mxmsg: Any) -> None:
        raise OperationTimedOut()

    def on_handler_exception(self, exc: Exception) -> bool:
        return False

    def periodic_task(self) -> None:
        self.iterations += 1


class HandlerTimeoutTest(unittest.TestCase):
    """See the module docstring."""

    def test_a_handlers_own_timeout_ends_serve_forever_when_asked_to(self) -> None:
        with Cluster(1, rules=RULES) as cluster:
            served = BackendThread(lambda: TimingOut(cluster.endpoints)).start()
            try:
                backend = served.backend
                assert backend is not None
                cluster.wait_for_peer(peers.PYTHON_TEST_SERVER)
                wait_until(lambda: backend.iterations >= 2, 10, "polls that ran out, the loop going on")
                self.assertTrue(served.running, "a poll's own timeout ended serve_forever()")
                with TestClient(cluster, peers.WEBSITE) as client:
                    client.send(b"request", types.PYTHON_TEST_REQUEST)
                    wait_until(lambda: not served.running, 10, "serve_forever() ended by the handler's exception")
            finally:
                if served.running:
                    served.stop()  # it served on
            self.assertIsInstance(served.error, OperationTimedOut)


if __name__ == "__main__":
    unittest.main()
