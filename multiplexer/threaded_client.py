"""ThreadedClient: a client with an io thread of its own.

The C++ ThreadedClient (multiplexer/threaded_client.h) owns the connections
on a thread that runs all the time, so heartbeats and reconnects happen
without the program calling in, and the peer type need not be passive. Any
thread may call query() and send_message(), any number of them at once:
replies are matched to queries by the ids they reference. query() blocks,
or, given a callback, returns at once and calls it with the result.
Everything else that arrives, events and requests addressed to this peer,
goes to the on_message callback given at construction, or is logged and
dropped when there is none; late replies and the protocol's own messages
never reach it. docs/api_python.md has the user's view; the synchronous
multiplexer.clients.Client remains for programs that prefer no thread.

Callbacks, on_message and query()'s, run on the io thread, with the GIL,
and must return quickly; they may call query() with a callback and
send_message() without flush, but not the blocking query() or a flushing
send_message(), which raise RuntimeError there.
"""

import pickle
import socket
from typing import Any, Callable, overload

from multiplexer import _native
from multiplexer.Multiplexer_pb2 import MultiplexerMessage
from multiplexer.mxclient import NotConnected, OperationTimedOut, make_message
from multiplexer.multiplexer_constants import types
from multiplexer.protocolbuffers import *  # noqa: F401,F403  (MultiplexerMessage.from_)

DEFAULT_TIMEOUT = _native.DEFAULT_TIMEOUT


class BackendError(Exception):
    """The backend answered with BACKEND_ERROR; the payload is its message."""


Endpoint = tuple[str, int]
QueryResult = "MultiplexerMessage | Exception"


class ThreadedClient:
    """The client; see the module docstring and docs/api_python.md."""

    ONE = 0
    ALL = 1

    def __init__(
        self,
        addresses: list[Endpoint],
        type: int,
        timeout: float = DEFAULT_TIMEOUT,
        on_message: Callable[[MultiplexerMessage], None] | None = None,
    ):
        """Start the io thread and connect to every (host, port) in `addresses`.

        `on_message(mxmsg)` runs on the io thread with every message that is
        not a reply to a query or one of the protocol's own; a program that
        wants a queue passes `queue.put`. Without it such messages are
        logged and dropped.
        """
        native_callback = None
        if on_message is not None:

            def native_callback(raw: bytes, _connection: Any) -> None:
                """The C++ side's callback: parse and hand over."""
                on_message(self._parse(raw))

        self._native = _native.ThreadedClient(type, native_callback)
        self.type = type
        for host, port in addresses:
            self.connect((host, port), timeout)

    @property
    def instance_id(self) -> int:
        """This peer's instance id, the `from` of everything it sends."""
        return self._native.instance_id()

    def connect(self, endpoint: Endpoint, timeout: float = DEFAULT_TIMEOUT) -> bool:
        """Connect and wait up to `timeout` for the handshake; False is not final,
        the io thread keeps trying."""
        return self._native.connect(socket.gethostbyname(endpoint[0]), endpoint[1], timeout)

    def connections_count(self) -> int:
        """How many multiplexers are connected right now."""
        return self._native.connections_count()

    def random(self) -> int:
        """A random 64-bit number, for message ids."""
        return self._native.random()

    def new_message(self, **kwargs: Any) -> MultiplexerMessage:
        """A MultiplexerMessage with id and from filled in; `message` may be
        bytes, str or a protocol buffer message."""
        kwargs.setdefault("id", self.random())
        kwargs.setdefault("from", self.instance_id)
        if "message" in kwargs:
            if isinstance(kwargs["message"], str):
                kwargs["message"] = kwargs["message"].encode("utf-8")
            elif not isinstance(kwargs["message"], bytes):
                kwargs["message"] = kwargs["message"].SerializeToString()
        return make_message(MultiplexerMessage, **kwargs)

    def send_message(
        self,
        message: Any,
        multiplexer: int = ONE,
        flush: bool = False,
        timeout: float = DEFAULT_TIMEOUT,
        **kwargs: Any,
    ) -> int:
        """Send an event and return its message id. `message` is a
        MultiplexerMessage or a payload wrapped with the remaining kwargs,
        such as type= and to=. It goes on one connection, or on every one
        with multiplexer=ALL; the io thread writes it right after, and a
        message that finds no live connection waits on the io thread for one
        within `timeout`. Without `flush` the call returns at once and is
        safe from callbacks. With `flush=True` it waits until the message
        reached the socket, resending through another connection if the
        first dies under it, and raises OperationTimedOut when `timeout`
        passes first, or NotConnected when no connection was live at all."""
        mxmsg = message if isinstance(message, MultiplexerMessage) else self.new_message(message=message, **kwargs)
        raw = mxmsg.SerializeToString()
        every = multiplexer == ThreadedClient.ALL
        if not flush:
            if every:
                self._native.send_all(raw)
            else:
                self._native.send(raw)
            return mxmsg.id
        if self._native.send_and_wait(raw, every, timeout) == 0:
            raise NotConnected() if self.connections_count() == 0 else OperationTimedOut()
        return mxmsg.id

    @staticmethod
    def _parse(raw: bytes) -> MultiplexerMessage:
        """A MultiplexerMessage from its serialized form."""
        mxmsg = MultiplexerMessage()
        mxmsg.ParseFromString(raw)
        return mxmsg

    QueryCallback = Callable[[MultiplexerMessage | Exception], None]

    @overload
    def query(self, message: Any, type: int, timeout: float = ...) -> MultiplexerMessage: ...

    @overload
    def query(self, message: Any, type: int, timeout: float = ..., *, callback: QueryCallback) -> None: ...

    def query(
        self, message: Any, type: int, timeout: float = DEFAULT_TIMEOUT, callback: QueryCallback | None = None
    ) -> MultiplexerMessage | None:
        """Send a request. Without `callback`, block and return the reply,
        raising OperationTimedOut, OperationFailed, NotConnected (from
        multiplexer.mxclient) or BackendError; RuntimeError when called from
        a callback, on the io thread, where it would block that thread. With
        `callback`, return None at once and call `callback(result)` on the io
        thread with the reply or with the exception instance the blocking
        form would have raised; safe from callbacks."""
        payload = self.new_message(message=message).message
        if callback is None:
            raw, _ = self._native.query(payload, type, timeout)
            reply = self._parse(raw)
            if reply.type == types.BACKEND_ERROR:
                raise BackendError(reply.message)
            return reply

        def on_result(raw: bytes | None, _connection: Any, error: Exception | None) -> None:
            """The C++ side's callback: turn bytes into a message, or pass the error on."""
            if error is not None:
                callback(error)
                return
            reply = self._parse(raw)
            callback(BackendError(reply.message) if reply.type == types.BACKEND_ERROR else reply)

        self._native.query_with_callback(payload, type, on_result, timeout)
        return None

    # The pickle convention: a payload that is a Python pickle, answered by a
    # MultiplexerServer (or any backend using send_pickle) with another. Only
    # between Python peers, and only where the network is trusted, since
    # unpickling runs code.
    @overload
    def query_pickle(self, data: Any, type: int, timeout: float = ...) -> Any: ...

    @overload
    def query_pickle(self, data: Any, type: int, timeout: float = ..., *, callback: Callable[[Any], None]) -> None: ...

    def query_pickle(
        self, data: Any, type: int, timeout: float = DEFAULT_TIMEOUT, callback: Callable[[Any], None] | None = None
    ) -> Any:
        """query() with `data` pickled as the payload. Without `callback`,
        returns the reply's payload unpickled; with one, the callback gets the
        unpickled payload or the exception instance."""
        if callback is None:
            return pickle.loads(self.query(pickle.dumps(data), type, timeout).message)

        def unpickle(result: MultiplexerMessage | Exception) -> None:
            """Unpickle a reply, pass an exception through."""
            callback(result if isinstance(result, Exception) else pickle.loads(result.message))

        self.query(pickle.dumps(data), type, timeout, callback=unpickle)
        return None

    def send_pickle(self, data: Any, **kwargs: Any) -> int:
        """send_message() with `data` pickled as the payload; the kwargs are send_message's."""
        return self.send_message(pickle.dumps(data), **kwargs)

    def shutdown(self) -> None:
        """Fail every query in flight, close the connections, stop the thread."""
        self._native.shutdown()
