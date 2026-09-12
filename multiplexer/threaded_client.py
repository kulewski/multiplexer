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

A query with `to` is addressed: only that peer gets it, located again
through a probe when a multiplexer no longer has it. `multiplexer=` on
send_message() and query() takes a Lane from lane(), one connection for a
stream of messages, pinned or following a failover, or a ConnectionWrapper
a reply came through, preferred while it is live. docs/api_python.md,
"ThreadedClient".
"""

import pickle
import socket
from typing import Any, Callable, overload

from multiplexer import _native
from multiplexer.Multiplexer_pb2 import MultiplexerMessage
from multiplexer.mxclient import ConnectionWrapper, Lane, NotConnected, OperationTimedOut, make_message
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
        on_message: Callable[..., None] | None = None,
        *,
        with_connection: bool = False,
        search_policy: Callable[[], bool] | None = None,
    ):
        """Start the io thread and connect to every (host, port) in `addresses`.

        `on_message(mxmsg)` runs on the io thread with every message that is
        not a reply to a query or one of the protocol's own; a program that
        wants a queue passes `queue.put`. Without it such messages are
        logged and dropped. With `with_connection`, it is called as
        `on_message(mxmsg, connection)`, the connection the message came on,
        for a reply that must go back the same way. `search_policy`, a
        function returning whether to answer a client's search for a
        backend, makes the client a backend: what
        multiplexer.threaded_server builds on; without it only a search
        addressed to this instance is answered.
        """
        native_callback = None
        if on_message is not None:

            def native_callback(raw: bytes, connection: Any) -> None:
                """The C++ side's callback: parse and hand over."""
                if with_connection:
                    on_message(self._parse(raw), connection)
                else:
                    on_message(self._parse(raw))

        self._native = _native.ThreadedClient(type, native_callback)
        self.type = type
        if search_policy is not None:
            self._native.set_search_policy(search_policy)
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

    def lane(self, pinned: bool = False, connection: ConnectionWrapper | None = None) -> Lane:
        """A Lane: one connection for a stream of messages, given as
        `multiplexer=` to send_message() and query(). Empty until first use,
        when it takes the connection the io thread chose; a lane that is
        not pinned follows a failover and adopts the connection a query's
        reply came through. A pinned lane keeps its first connection for
        good and fails with NotConnected once that is gone. `connection`,
        one a reply came through, seeds the lane."""
        return Lane(connection, pinned) if connection is not None else Lane(pinned)

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
        multiplexer: int | Lane | ConnectionWrapper = ONE,
        flush: bool = False,
        timeout: float = DEFAULT_TIMEOUT,
        callback: Callable[[int], None] | None = None,
        **kwargs: Any,
    ) -> int:
        """Send an event and return its message id. `message` is a
        MultiplexerMessage or a payload wrapped with the remaining kwargs,
        such as type= and to=. It goes on one connection, or on every one
        with multiplexer=ALL, on a Lane's connection (the lane taking the
        connection chosen when it has none or lost its own, unless pinned),
        or on a ConnectionWrapper's while it is live and another after;
        the io thread writes it right after, and a message that finds no
        live connection waits on the io thread for one within `timeout`.
        Without `flush` the call returns at once and is safe from
        callbacks; through a pinned lane whose connection is gone it raises
        NotConnected at once instead. With `flush=True` it waits until the
        message reached the socket, resending through another connection
        if the first dies under it, and raises OperationTimedOut when
        `timeout` passes first, or NotConnected when no connection was live
        at all, or the pinned lane given is gone. With
        `flush=True` and a `callback`, it returns at once instead and
        `callback(written)` runs on the io thread once the message reached
        the socket(s), with the number of connections written, 0 when
        `timeout` passed first; safe from callbacks, and what
        multiplexer.aio awaits."""
        mxmsg = message if isinstance(message, MultiplexerMessage) else self.new_message(message=message, **kwargs)
        raw = mxmsg.SerializeToString()
        every = multiplexer is ThreadedClient.ALL
        lane = multiplexer if isinstance(multiplexer, Lane) else None
        if isinstance(multiplexer, ConnectionWrapper):
            lane = Lane(multiplexer)  # preferred, then any
        if not flush:
            if every:
                self._native.send_all(raw)
            elif lane is not None:
                if lane.closed:
                    raise NotConnected()
                self._native.send(raw, lane)
            else:
                self._native.send(raw)
            return mxmsg.id
        if callback is not None:
            self._native.send_with_callback(raw, every, timeout, callback, lane)
            return mxmsg.id
        if self._native.send_and_wait(raw, every, timeout, lane) == 0:
            self._raise_for_nothing_written(lane)
        return mxmsg.id

    def _raise_for_nothing_written(self, lane: Lane | None) -> None:
        """A flushing send wrote nothing: the reason, as an exception."""
        if (lane is not None and lane.closed) or self.connections_count() == 0:
            raise NotConnected()
        raise OperationTimedOut()

    @staticmethod
    def _parse(raw: bytes) -> MultiplexerMessage:
        """A MultiplexerMessage from its serialized form."""
        mxmsg = MultiplexerMessage()
        mxmsg.ParseFromString(raw)
        return mxmsg

    QueryCallback = Callable[[Any], None]

    @overload
    def query(self, message: Any, type: int, timeout: float = ..., **kwargs: Any) -> MultiplexerMessage: ...

    @overload
    def query(
        self, message: Any, type: int, timeout: float = ..., *, callback: QueryCallback, **kwargs: Any
    ) -> None: ...

    def query(
        self,
        message: Any,
        type: int,
        timeout: float = DEFAULT_TIMEOUT,
        callback: QueryCallback | None = None,
        to: int = 0,
        probe: int = types.BACKEND_FOR_PACKET_SEARCH,
        multiplexer: int | Lane | ConnectionWrapper = ONE,
        with_connection: bool = False,
    ) -> Any:
        """Send a request. Without `callback`, block and return the reply,
        raising OperationTimedOut, OperationFailed, NotConnected (from
        multiplexer.mxclient) or BackendError; RuntimeError when called from
        a callback, on the io thread, where it would block that thread. With
        `callback`, return None at once and call `callback(result)` on the io
        thread with the reply or with the exception instance the blocking
        form would have raised; safe from callbacks.

        With `to`, the instance id of a peer, the request is addressed: only
        that peer ever gets it; when a multiplexer reports it is not behind
        it, or the connection dies under the wait, the peer is located with
        a `probe` addressed to it on every connection, a
        BACKEND_FOR_PACKET_SEARCH (which a draining backend declines) or a
        PING (answered as long as the peer lives), and the request goes
        again through the connection that found it. A peer nobody has is
        OperationFailed; one `timeout` covers the three stages.

        `multiplexer` is ONE, a Lane from lane() (the request goes through
        the lane's connection and the lane adopts the connection the reply
        came through; a pinned lane allows no other and ends in
        NotConnected once its own is gone) or a ConnectionWrapper
        (preferred while it is live). With `with_connection` the result is
        (reply, connection), so that a later message can go the same way."""
        if probe not in (types.BACKEND_FOR_PACKET_SEARCH, types.PING):
            raise ValueError("probe must be types.BACKEND_FOR_PACKET_SEARCH or types.PING")
        if multiplexer is ThreadedClient.ALL:
            raise ValueError("a query goes through one connection; multiplexer=ALL is for events")
        lane = multiplexer if isinstance(multiplexer, Lane) else None
        if isinstance(multiplexer, ConnectionWrapper):
            lane = Lane(multiplexer)  # preferred, then any
        fields: dict[str, Any] = {"message": message, "type": type}
        if to:
            fields["to"] = to
        raw_request = self.new_message(**fields).SerializeToString()
        if callback is None:
            raw, connection = self._native.query(raw_request, timeout, probe, lane)
            reply = self._parse(raw)
            if reply.type == types.BACKEND_ERROR:
                raise BackendError(reply.message)
            return (reply, connection) if with_connection else reply

        def on_result(raw: bytes | None, connection: Any, error: Exception | None) -> None:
            """The C++ side's callback: turn bytes into a message, or pass the error on."""
            if error is not None:
                callback(error)
                return
            reply = self._parse(raw)
            if reply.type == types.BACKEND_ERROR:
                callback(BackendError(reply.message))
            else:
                callback((reply, connection) if with_connection else reply)

        self._native.query_with_callback(raw_request, on_result, timeout, probe, lane)
        return None

    # The pickle convention: a payload that is a Python pickle, answered by a
    # MultiplexerServer (or any backend using send_pickle) with another. Only
    # between Python peers, and only where the network is trusted, since
    # unpickling runs code.
    @overload
    def query_pickle(self, data: Any, type: int, timeout: float = ..., **kwargs: Any) -> Any: ...

    @overload
    def query_pickle(
        self, data: Any, type: int, timeout: float = ..., *, callback: Callable[[Any], None], **kwargs: Any
    ) -> None: ...

    def query_pickle(
        self,
        data: Any,
        type: int,
        timeout: float = DEFAULT_TIMEOUT,
        callback: Callable[[Any], None] | None = None,
        **kwargs: Any,
    ) -> Any:
        """query() with `data` pickled as the payload. Without `callback`,
        returns the reply's payload unpickled; with one, the callback gets the
        unpickled payload or the exception instance. The kwargs are query()'s:
        `to`, `probe`, `multiplexer`."""
        if callback is None:
            return pickle.loads(self.query(pickle.dumps(data), type, timeout, **kwargs).message)

        def unpickle(result: MultiplexerMessage | Exception) -> None:
            """Unpickle a reply, pass an exception through."""
            callback(result if isinstance(result, Exception) else pickle.loads(result.message))

        self.query(pickle.dumps(data), type, timeout, callback=unpickle, **kwargs)
        return None

    def send_pickle(self, data: Any, **kwargs: Any) -> int:
        """send_message() with `data` pickled as the payload; the kwargs are send_message's."""
        return self.send_message(pickle.dumps(data), **kwargs)

    def shutdown(self) -> None:
        """Fail every query in flight, close the connections, stop the thread."""
        self._native.shutdown()
