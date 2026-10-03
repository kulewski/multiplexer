"""BaseMultiplexerServer.connect(), and BaseThreadedMultiplexerServer's,
starts a connection to every address at once and waits for them all
against one deadline, as the C++ classes do, where it connected to one
address after another, each waited for up to its timeout: a multiplexer
that never welcomes held up every address after it that long. Two
listeners that accept and never answer each get their connection while
connect() still waits, where the second got nothing until the first's
timeout ran out. Once both hang up, connect() returns, raising nothing,
with nothing connected. And serve_forever() waits for no handshake: a
backend given a real multiplexer and a listener that never answers serves
what the multiplexer routes to it while the listener holds, where
serve_forever() first waited in connect() for every handshake. Counted,
not timed: the accepts, the return, the registration and the answer are
events; the bounds on waiting for them only detect a failure, each shorter
than the timeout it tells apart.
"""

import select
import socket
import threading
import unittest
from typing import Any, Callable

from multiplexer import mxclient
from multiplexer.multiplexer_constants import peers, types
from multiplexer.servers import BaseMultiplexerServer
from multiplexer.testing import Cluster, runfile
from multiplexer.threaded_client import ThreadedClient
from multiplexer.threaded_server import BaseThreadedMultiplexerServer

RULES = runfile("tests/testing.rules")
# Half of what serve_forever() first waited in connect() for a welcome that never comes.
HALF = mxclient.DEFAULT_TIMEOUT / 2


class ConnectingBackend(BaseMultiplexerServer):
    """A backend that is only connected."""

    def handle_message(self, mxmsg) -> None:
        """Nothing reaches it."""
        self.no_response()


class ThreadedConnectingBackend(BaseThreadedMultiplexerServer):
    """A threaded backend that is only connected."""

    def handle_message(self, request) -> None:
        """Nothing reaches it."""
        request.no_response()


class AnsweringBackend(BaseMultiplexerServer):
    """A backend that answers every request with "re: " and its payload;
    its loop notes once it is registered with a multiplexer, and it stops
    once `leave` is set."""

    def __init__(self, addresses: list[tuple[str, int]]):
        super().__init__(addresses, peers.PYTHON_TEST_SERVER)
        self.registered = threading.Event()
        self.leave = False

    def handle_message(self, mxmsg) -> None:
        """The payload back, after "re: "."""
        self.send_message(message=b"re: " + mxmsg.message, type=types.PYTHON_TEST_RESPONSE)

    def periodic_task(self) -> None:
        """Notes the registration; stops once asked."""
        if self.conn.connections_count():
            self.registered.set()
        if self.leave:
            self.working = False


class SilentListener:
    """A listener on 127.0.0.1 that accepts and never answers: a
    multiplexer that never sends its welcome."""

    def __init__(self) -> None:
        self.listening = socket.socket()
        self.listening.bind(("127.0.0.1", 0))
        self.listening.listen(4)
        self.port: int = self.listening.getsockname()[1]
        self.accepted: socket.socket | None = None

    def accepted_within(self, seconds: float) -> bool:
        """Whether a connection came within `seconds`, a failure detector
        only; it is held, never answered."""
        ready, _, _ = select.select([self.listening], [], [], seconds)
        if not ready:
            return False
        self.accepted, _ = self.listening.accept()
        return True

    def hang_up(self) -> None:
        """The connection held is closed, which ends its handshake."""
        if self.accepted is not None:
            self.accepted.close()
            self.accepted = None

    def close(self) -> None:
        """Every socket closed."""
        self.hang_up()
        self.listening.close()


class ServerConnectTest(unittest.TestCase):
    """See the module docstring."""

    def test_every_address_is_tried_at_once_against_one_deadline(self) -> None:
        self.tried_at_once(
            lambda addresses: ConnectingBackend(addresses, peers.PYTHON_TEST_SERVER),
            lambda backend: backend.conn.connections_count(),
        )

    def test_a_threaded_server_tries_every_address_at_once_too(self) -> None:
        self.tried_at_once(
            lambda addresses: ThreadedConnectingBackend(addresses, peers.PYTHON_TEST_SERVER),
            lambda backend: backend.client.connections_count(),
        )

    def test_serve_forever_serves_while_a_handshake_hangs(self) -> None:
        # What the multiplexer routes to the backend is answered while the
        # silent listener, its other address, still holds its handshake.
        silent = SilentListener()
        self.addCleanup(silent.close)
        with Cluster(1, rules=RULES) as cluster:
            backend = AnsweringBackend([cluster.endpoints[0], ("127.0.0.1", silent.port)])
            serving = threading.Thread(target=backend.serve_forever, kwargs={"poll": 0.05}, daemon=True)
            serving.start()
            try:
                self.assertTrue(silent.accepted_within(5))
                self.assertTrue(backend.registered.wait(HALF), "the loop did not run while a handshake hung")
                with ThreadedClient(cluster.endpoints, peers.PYTHON_TEST_CLIENT) as client:
                    self.assertEqual(b"re: x", client.query(b"x", types.PYTHON_TEST_REQUEST, timeout=HALF).message)
            finally:
                backend.leave = True
                serving.join(30)

    def tried_at_once(self, make: Callable[[list[tuple[str, int]]], Any], connections: Callable[[Any], int]) -> None:
        """A backend `make(addresses)` makes for two silent listeners: both
        get their connection while connect() still waits, and once both hang
        up it returns, `connections(backend)` none."""
        first, second = SilentListener(), SilentListener()
        self.addCleanup(first.close)
        self.addCleanup(second.close)
        outcome: list[str] = []
        returned = threading.Event()

        def connect() -> None:
            """The backend made, connected and closed on this thread, the one it belongs to."""
            backend = make([("127.0.0.1", first.port), ("127.0.0.1", second.port)])
            try:
                backend.connect()
                outcome.append("returned, %d connected" % connections(backend))
            except Exception as error:
                outcome.append("raised %s" % type(error).__name__)
            finally:
                returned.set()
                backend.close()

        connecting = threading.Thread(target=connect, daemon=True)
        connecting.start()
        self.assertTrue(first.accepted_within(5))
        self.assertTrue(second.accepted_within(5), "the second address waited for the first's handshake")
        self.assertFalse(returned.is_set(), "connect() still waits")
        first.hang_up()
        second.hang_up()
        self.assertTrue(returned.wait(5), "connect() ends with the last connection, not at its timeout")
        self.assertEqual(["returned, 0 connected"], outcome)
        connecting.join(30)


if __name__ == "__main__":
    unittest.main()
