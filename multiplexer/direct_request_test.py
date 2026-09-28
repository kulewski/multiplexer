"""A typed query's direct request, its third stage, asks for a delivery
error, so that the backend that answered the search, gone since, is
noticed: with nobody having taken the request, the query fails at once,
where the direct request, asking for none, was dropped unsaid and the stage
ran out its timeout; with a backend holding the request, the stage waits on
as before, and that backend's late reply answers the query. Against a
multiplexer of the test's own, which answers the direct request with a
delivery error only when it asks for one, as the multiplexer does. The
synchronous client here; ThreadedClient, and AsyncClient on it, take the
C++ ThreadedClient's stages, tested in threaded_client_test.cc.
"""

import socket
import threading
import unittest
import zlib

from multiplexer.clients import SyncClient
from multiplexer.Multiplexer_pb2 import MultiplexerMessage, WelcomeMessage
from multiplexer.multiplexer_constants import peers, types
from multiplexer.mxclient import OperationFailed
from multiplexer.testing.raw_peer import HEADER, frame


class DirectStage:
    """A multiplexer of the test's own for a typed query's last stage, as
    DirectStage in threaded_client_test.cc: it answers the request with a
    delivery error, nobody taking it, or, when `taken`, keeps it, as a
    backend busy with it would; answers the search with a PING from a
    backend it no longer has; answers the direct request to that backend
    with a delivery error when the request asks for one, and drops it
    otherwise; and, `taken`, then sends the reply to the first request,
    late. `asked` is whether the direct request asked for a delivery error,
    once `done` is set."""

    ID = 0x6D78  # this multiplexer
    GONE = 0x60E  # the backend that answered the search, gone since
    BACKEND = 0xBAC  # the backend that took the request

    def __init__(self, taken: bool):
        self.taken = taken
        self.listener = socket.create_server(("127.0.0.1", 0))
        self.port = self.listener.getsockname()[1]
        self.connection: socket.socket | None = None
        self.asked = False
        self.done = threading.Event()
        self.client_id = 0
        self.ids = self.ID
        self.thread = threading.Thread(target=self._script, daemon=True)
        self.thread.start()

    def close(self) -> None:
        """Ends the listener and the connection."""
        self.listener.close()
        if self.connection is not None:
            self.connection.close()
        self.thread.join(30)

    def _read_exactly(self, count: int) -> bytes:
        """`count` bytes from the client, or ConnectionError when it closes first."""
        assert self.connection is not None
        data = b""
        while len(data) < count:
            chunk = self.connection.recv(count - len(data))
            if not chunk:
                raise ConnectionError("the client closed")
            data += chunk
        return data

    def _next(self, type_: int, to: int) -> MultiplexerMessage:
        """The client's next message of `type_` addressed `to` (0 for none),
        the rest skipped."""
        while True:
            length, crc = HEADER.unpack(self._read_exactly(HEADER.size))
            body = self._read_exactly(length)
            assert zlib.crc32(body) == crc
            mxmsg = MultiplexerMessage.FromString(body)
            if mxmsg.type == type_ and mxmsg.to == to:
                return mxmsg

    def _write(self, type_: int, from_: int, references: int, payload: bytes = b"late") -> None:
        """A message of `type_` from `from_` to the client, answering `references`."""
        assert self.connection is not None
        self.ids += 1
        mxmsg = MultiplexerMessage(id=self.ids, to=self.client_id, type=type_, references=references, message=payload)
        setattr(mxmsg, "from", from_)
        self.connection.sendall(frame(mxmsg.SerializeToString()))

    def _script(self) -> None:
        """The stages, as the class docstring says."""
        try:
            self.connection, _ = self.listener.accept()
            self.connection.settimeout(30)  # a step that never comes fails the test rather than hangs it
            self._next(types.CONNECTION_WELCOME, 0)
            welcome = WelcomeMessage(type=peers.MULTIPLEXER, id=self.ID).SerializeToString()
            self._write(types.CONNECTION_WELCOME, self.ID, 0, welcome)
            request = self._next(types.PYTHON_TEST_REQUEST, 0)
            self.client_id = getattr(request, "from")
            if not self.taken:
                self._write(types.DELIVERY_ERROR, self.ID, request.id)
            search = self._next(types.BACKEND_FOR_PACKET_SEARCH, 0)
            self._write(types.PING, self.GONE, search.id)
            direct = self._next(types.PYTHON_TEST_REQUEST, self.GONE)
            self.asked = direct.report_delivery_error
            if self.asked:
                self._write(types.DELIVERY_ERROR, self.ID, direct.id)
            if self.taken:
                self._write(types.PYTHON_TEST_RESPONSE, self.BACKEND, request.id)
        except OSError:
            pass  # the test ended first; its assertions say what is missing
        finally:
            self.done.set()


class DirectRequestTest(unittest.TestCase):
    """See the module docstring."""

    def test_a_gone_backend_fails_the_query_when_nobody_took_the_request(self) -> None:
        multiplexer = DirectStage(taken=False)
        self.addCleanup(multiplexer.close)
        with SyncClient([("127.0.0.1", multiplexer.port)], type=peers.WEBSITE) as client:
            with self.assertRaises(OperationFailed):
                client.query(b"request", types.PYTHON_TEST_REQUEST, timeout=5)
        self.assertTrue(multiplexer.done.wait(30))
        self.assertTrue(multiplexer.asked, "the direct request asked for no delivery error")

    def test_a_gone_backend_waits_for_the_request_a_backend_took(self) -> None:
        multiplexer = DirectStage(taken=True)
        self.addCleanup(multiplexer.close)
        with SyncClient([("127.0.0.1", multiplexer.port)], type=peers.WEBSITE) as client:
            reply = client.query(b"request", types.PYTHON_TEST_REQUEST, timeout=1)
            self.assertEqual((types.PYTHON_TEST_RESPONSE, b"late"), (reply.type, reply.message))
        self.assertTrue(multiplexer.done.wait(30))
        self.assertTrue(multiplexer.asked, "the direct request asked for no delivery error")


if __name__ == "__main__":
    unittest.main()
