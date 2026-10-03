"""ThreadedClient: a client with an io thread of its own.

The C++ ThreadedClient (multiplexer/threaded_client.h) owns the connections
on a thread that runs all the time, so heartbeats and reconnects happen
without the program calling in, and the peer type need not be passive. Any
thread may call query() and send_message(), any number of them at once:
replies are matched to queries by the ids they reference. query() blocks,
or, given a callback, returns at once and calls it with the result.
Everything else that arrives, events and requests addressed to this peer
and the delivery errors for messages that were not queries, goes to the
on_message callback given at construction, or is logged and dropped when
there is none; late replies, pings and searches never reach it.
docs/api_python.md has the user's view; multiplexer.clients.SyncClient
remains for programs that prefer no thread.

Callbacks, on_message and query()'s, run on the io thread, with the GIL,
and must return quickly; they may call query() with a callback and
send_message() without flush, but not the blocking query() or a flushing
send_message(), which raise RuntimeError there.

A query with `to` is addressed: only that peer gets it, located again
with a PING when a multiplexer no longer has it. `multiplexer=` on
send_message() and query() takes a Lane from lane(), one connection for a
stream of messages, pinned or following a failover, or a ConnectionWrapper
a reply came through, preferred while it is live. docs/api_python.md,
"ThreadedClient".
"""

import pickle
from typing import Any, Callable, Iterable, Literal, TypeVar, overload

from multiplexer import _native
from multiplexer.Multiplexer_pb2 import MultiplexerMessage, Routing
from multiplexer.clients import BackendError  # re-exported: the one class every client raises for BACKEND_ERROR
from multiplexer.mxclient import (
    CLOSE_FLUSH_SECONDS,
    ConnectionWrapper,
    DropReason,
    Lane,
    NotConnected,
    OperationTimedOut,
    UsedAfterFork,
    make_message,
    renamed_sender,
    stamped,
    whole,
)
from multiplexer.endpoints import Endpoint as Endpoint  # (host, port), defined there since it has text forms too
from multiplexer.multiplexer_constants import types
import multiplexer.protocolbuffers  # the read-only aliases of the sender's former names

DEFAULT_TIMEOUT = _native.DEFAULT_TIMEOUT


QueryResult = "MultiplexerMessage | Exception"
_ThreadedClientT = TypeVar("_ThreadedClientT", bound="ThreadedClient")


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
        search_policy: Callable[[], object] | None = None,
        on_drop: Callable[[int, DropReason], None] | None = None,
    ):
        """Start the io thread and connect to every (host, port) in `addresses`.

        `on_message(mxmsg)` runs on the io thread with every message that is
        not a reply to a query, the delivery errors for messages that were
        not queries included, and no ping or search; a program that wants a
        queue passes `queue.put`. Without it such messages are logged and
        dropped. With `with_connection`, it is called as
        `on_message(mxmsg, connection)`, the connection the message came on,
        for a reply that must go back the same way. `search_policy`, a
        function returning whether to answer a client's search for a
        backend, read by its truth as `if` reads it, lets requests routed
        by type find this client: what
        multiplexer.threaded_server builds on; without it no search is
        answered. `on_drop(message_id,
        reason)` runs on the io thread for every message the client gives
        up on, each copy of one sent to ALL, with a DropReason; `dropped`
        counts them. Like `on_message`, it is held until shutdown().
        """

        def native_callback(raw: bytes, connection: Any) -> None:
            """The C++ side's callback: parse and hand over."""
            assert on_message is not None
            if with_connection:
                on_message(self._parse(raw), connection)
            else:
                on_message(self._parse(raw))

        self._native = _native.ThreadedClient(type, native_callback if on_message is not None else None)
        self.type = type
        if search_policy is not None:
            self._native.set_search_policy(search_policy)
        if on_drop is not None:
            self._native._set_drop_observer(on_drop)
        for host, port in addresses:
            self.connect((host, port), timeout)

    @property
    def instance_id(self) -> int:
        """This peer's instance id, the `sender` of everything it sends."""
        return self._native.instance_id()

    @property
    def dropped(self) -> int:
        """How many messages this client gave up on so far, each copy of one
        sent to ALL; `on_drop` hears of each as it goes. From any thread."""
        return self._native._dropped()

    def orphaned(self) -> bool:
        """Whether the client was made before a fork this process is the
        child of, where its calls raise UsedAfterFork and shutdown() closes
        only the child's copies of its connections. A load and a compare:
        for a class built on this one to check first."""
        return self._native.orphaned()

    def _check_not_orphaned(self) -> None:
        """Raises UsedAfterFork when orphaned(), in the library's words: for
        the classes built on this one, before a lock of their own that the
        parent's io thread takes and may have held at the fork."""
        if self._native.orphaned():
            raise UsedAfterFork("client used after fork; create a new one in the child")

    def connect(self, endpoint: Endpoint, timeout: float = DEFAULT_TIMEOUT) -> bool:
        """Connect and wait up to `timeout` for the handshake; False is not final,
        the io thread keeps trying. The host name goes to the library, which
        resolves it on every attempt, so a name that does not resolve yet is
        retried like a port that refuses, and a multiplexer that comes back
        under another address is found."""
        return self._native.connect(endpoint[0], endpoint[1], timeout)

    def connect_all(self, endpoints: Iterable[Endpoint], timeout: float = DEFAULT_TIMEOUT) -> int:
        """connect() to every (host, port) at once, each waited for against
        the same `timeout`: a multiplexer that never welcomes costs the call
        its timeout once, not once for every address after it. How many are
        connected when it returns; the others the io thread keeps trying,
        as after connect()."""
        return self._native.connect_all([(host, port) for host, port in endpoints], timeout)

    def disconnect(self, endpoint: Endpoint) -> bool:
        """Drop the multiplexer given to connect() or to the constructor as
        `endpoint`, the same (host, port), an address in any spelling: no
        reconnect to it any more, unless connect() is called again; a live
        connection to it closed, what it had not written going to another
        connection or held, and the queries whose request it carried
        moving on as after a lost connection; a connect() waiting for it
        returns False. Returns once the io thread has done it, whether the
        client had it. Raises NotConnected after shutdown(), as connect()
        does, and RuntimeError from a callback, on the io thread."""
        return self._native.disconnect(endpoint[0], endpoint[1])

    def connections_count(self) -> int:
        """How many multiplexers are connected right now."""
        return self._native.connections_count()

    def forget_finished_ids(self) -> None:
        """Forget the ids of every query that has ended now, as if their
        time were up, giving back the memory they held: a late reply to one
        reaches on_message. For a test that measures the heap across many
        queries, the ids being kept a while by design, bounded, and no
        leak. From any thread, a callback's included; as the C++
        ThreadedClient::forget_finished_ids()."""
        self._native.forget_finished_ids()

    def set_routing(self, routing: Routing) -> None:
        """Which of a multiplexer's routing paths reach this peer, a
        `Routing` from Multiplexer.proto: `any` for rules with whom ANY,
        `all` for whom ALL, both True by default, and `last_resort` for
        getting what nobody else of the type could take; a message with
        `to` always arrives. Told to every multiplexer at once and carried
        in the welcome of every connection made from now on;
        routing_acknowledged() says when it is in effect everywhere. A
        backend draining sets any and all False; see docs/leaving.md."""
        self._native.set_routing_serialized(routing.SerializeToString())

    def routing_acknowledged(self) -> bool:
        """Whether every connected multiplexer has the routing given to
        set_routing() in effect, so that nothing routed by a path turned
        off is on its way from them, except as a last resort. Not from
        the io thread."""
        return self._native.routing_acknowledged()

    def flush_all(self, timeout: float = DEFAULT_TIMEOUT) -> bool:
        """Wait until everything sent before the call has been written or
        given up on, or `timeout` seconds; True when every one was written,
        False when one was given up on, which on_drop names, or the time
        ran out. A message still waiting for a connection or for room
        counts, as does what is queued on a connection; what is sent after
        the call does not, so a flush ends however busy the client is, a
        message a dying connection hands to another included. After
        shutdown() it returns True at once, nothing being left to wait for.
        What shutdown() does first. Not from the io thread."""
        return self._native.flush_all(timeout)

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
        """A MultiplexerMessage with id and sender filled in; `message` may be
        bytes, str or a protocol buffer message."""
        kwargs.setdefault("id", self.random())
        renamed_sender(kwargs)
        kwargs.setdefault("sender", self.instance_id)
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
        MultiplexerMessage, an empty id and sender filled in, or a payload
        wrapped with the remaining kwargs, such as type= and to=. It goes on
        one connection, or on every one with multiplexer=ALL, a multiplexer
        whose first connection is still in its handshake getting its copy
        at the welcome, on a Lane's connection (the lane taking the
        connection chosen when it has none or lost its own, unless pinned),
        or on a ConnectionWrapper's while it is live and another after; the
        io thread writes it right after. A message that cannot be queued
        yet, no connection being live or the connections' queues full,
        waits behind those sent before it, within `timeout`, and is dropped
        and reported (on_drop) after that: for ALL, a full connection gets
        its copy once it has room, and a lane waits for room on its own
        connection while that lives. Through a pinned lane whose connection
        is gone, and after shutdown(), it raises NotConnected at once.

        Without `flush` the call returns at once and is safe from
        callbacks. With `flush=True` it waits until the message reached the
        socket, for ALL until one copy did, a connection that dies under it
        handing it to another or having it held (a copy for ALL is held
        only when no connection is live, and dropped otherwise), and raises
        NotConnected when nothing wrote it with no connection live, or when
        every copy for ALL went with its connection, else OperationTimedOut;
        not from a callback. With a `callback`, flush or
        not, it returns at once and `callback(written)` runs on the io
        thread once the message's end is known: 1 once it was written, the
        first copy for ALL, 0 once it was given up on, and reported, or
        shutdown() came first; safe from callbacks. flush_all() waits for
        every copy."""
        mxmsg_id, mxmsg_type, raw, every, lane = self._prepare(message, multiplexer, kwargs)
        if callback is not None or not flush:
            if every:
                self._native.send_all(raw, mxmsg_id, mxmsg_type, timeout=timeout, callback=callback)
            else:
                self._native.send(raw, mxmsg_id, mxmsg_type, lane, timeout=timeout, callback=callback)
            return mxmsg_id
        written, not_connected = self._native.send_and_wait(raw, mxmsg_id, mxmsg_type, every, timeout, lane)
        if written == 0:
            self._raise_for_nothing_written(lane, not_connected)
        return mxmsg_id

    def _send_and_notify(
        self,
        message: Any,
        multiplexer: int | Lane | ConnectionWrapper,
        timeout: float,
        notify: Callable[[int, bool], None],
        **kwargs: Any,
    ) -> int:
        """send_message(flush=True) with `notify(written, not_connected)` on
        the io thread instead of the wait: 1 once the message was written,
        the first copy for ALL, 0 when it was given up on or `timeout`
        passed first, `not_connected` saying which exception that is; what
        multiplexer.aio awaits. Returns the message id."""
        mxmsg_id, mxmsg_type, raw, every, lane = self._prepare(message, multiplexer, kwargs)
        self._native.send_and_notify(raw, mxmsg_id, mxmsg_type, every, timeout, notify, lane)
        return mxmsg_id

    def _flush_all_and_notify(self, timeout: float, notify: Callable[[bool], None]) -> None:
        """flush_all() with `notify(flushed)` on the io thread instead of
        the wait; what multiplexer.aio awaits."""
        self._native.flush_all_and_notify(timeout, notify)

    def _prepare(
        self, message: Any, multiplexer: int | Lane | ConnectionWrapper, kwargs: dict[str, Any]
    ) -> tuple[int, int, bytes, bool, Lane | None]:
        """A send's (message id, message type, serialized message, to ALL,
        lane), the id and the type being what a drop is reported by: the
        message built from the payload and `kwargs` unless it is a whole
        one, sent as it is, `kwargs` beside it a TypeError; a
        ConnectionWrapper as a lane that prefers it. NotConnected for a
        pinned lane whose connection is gone."""
        if isinstance(message, MultiplexerMessage):
            mxmsg = whole(message, kwargs, self.instance_id, self.random)
        else:
            mxmsg = self.new_message(message=message, **kwargs)
        lane = multiplexer if isinstance(multiplexer, Lane) else None
        if lane is not None and lane.closed:
            raise NotConnected()
        if isinstance(multiplexer, ConnectionWrapper):
            lane = Lane(multiplexer)  # preferred, then any
        return mxmsg.id, mxmsg.type, mxmsg.SerializeToString(), multiplexer is ThreadedClient.ALL, lane

    def _raise_for_nothing_written(self, lane: Lane | None, not_connected: bool) -> None:
        """A flushing send wrote nothing: the reason, as an exception, the
        synchronous client's rule, which the io thread applied at the
        deadline. NotConnected when its message was given up on, a pinned
        lane's connection being gone, or no connection was live then,
        OperationTimedOut otherwise."""
        if not_connected or (lane is not None and lane.closed):
            raise NotConnected()
        raise OperationTimedOut()

    @staticmethod
    def _parse(raw: bytes) -> MultiplexerMessage:
        """A MultiplexerMessage from its serialized form."""
        mxmsg = MultiplexerMessage()
        mxmsg.ParseFromString(raw)
        return mxmsg

    QueryCallback = Callable[[Any], object]  # the result is ignored, so a lambda that returns something is fine

    @overload
    def query(
        self,
        message: Any,
        type: int | None = ...,
        timeout: float = ...,
        *,
        to: int = ...,
        multiplexer: int | Lane | ConnectionWrapper = ...,
        with_connection: Literal[False] = ...,
        on_received: Callable[[int], None] | None = ...,
    ) -> MultiplexerMessage: ...

    @overload
    def query(
        self,
        message: Any,
        type: int | None = ...,
        timeout: float = ...,
        *,
        to: int = ...,
        multiplexer: int | Lane | ConnectionWrapper = ...,
        with_connection: Literal[True],
        on_received: Callable[[int], None] | None = ...,
    ) -> tuple[MultiplexerMessage, ConnectionWrapper]: ...

    @overload
    def query(
        self,
        message: Any,
        type: int | None = ...,
        timeout: float = ...,
        *,
        callback: QueryCallback,
        to: int = ...,
        multiplexer: int | Lane | ConnectionWrapper = ...,
        with_connection: bool = ...,
        on_received: Callable[[int], None] | None = ...,
    ) -> None: ...

    def query(
        self,
        message: Any,
        type: int | None = None,
        timeout: float = DEFAULT_TIMEOUT,
        callback: QueryCallback | None = None,
        to: int = 0,
        multiplexer: int | Lane | ConnectionWrapper = ONE,
        with_connection: bool = False,
        on_received: Callable[[int], None] | None = None,
    ) -> Any:
        """Send a request: `message` itself when it is a whole
        MultiplexerMessage, typed by its own `type` and addressed by its own
        `to`, an empty id and sender filled in, `type=` or `to=` beside it a
        TypeError; else one built from the payload, bytes, str or a protocol
        buffer message, and `type`, which it needs, and `to`. Without
        `callback`, block and return the reply,
        raising OperationTimedOut, OperationFailed, NotConnected (from
        multiplexer.mxclient) or BackendError; RuntimeError when called from
        a callback, on the io thread, where it would block that thread. With
        `callback`, return None at once and call `callback(result)` on the io
        thread with the reply or with the exception instance the blocking
        form would have raised; safe from callbacks. After shutdown() the
        callback runs at once, on the calling thread, with NotConnected, so
        it must not query again then, nor take a lock its caller holds.

        With `to`, the instance id of a peer, the request is addressed: only
        that peer ever gets it; when a multiplexer reports it is not behind
        it, or the connection dies under the wait, the peer is located with
        a PING addressed to it on every connection (answered as long as the
        peer lives, by the server classes, ThreadedClient and AsyncClient
        whatever their search policy, never by a SyncClient), and the request
        goes again, its second and last time, through the connection that
        found it. A peer nobody has is OperationFailed, unless a request it
        may have taken, after a lost connection, can still be answered, which
        the query then waits for; one `timeout` covers the three stages.

        `multiplexer` is ONE, a Lane from lane() (the request goes through
        the lane's connection and the lane adopts the connection the reply
        came through; a pinned lane allows no other and ends in
        NotConnected once its own is gone) or a ConnectionWrapper
        (preferred while it is live). With `with_connection` the result is
        (reply, connection), so that a later message can go the same way.

        `on_received`, when given, is called on the io thread, so it must
        be quick, with the instance id of each backend that acknowledges
        the request with REQUEST_RECEIVED (notify_start()): once, normally,
        or again when a retry reached a backend, the same or another.
        Nothing about the query changes for it; what it raises goes to
        sys.unraisablehook, as every callback's does."""
        if multiplexer is ThreadedClient.ALL:
            raise ValueError("a query goes through one connection; multiplexer=ALL is for events")
        lane = multiplexer if isinstance(multiplexer, Lane) else None
        if isinstance(multiplexer, ConnectionWrapper):
            lane = Lane(multiplexer)  # preferred, then any
        if isinstance(message, MultiplexerMessage):
            if type is not None or to:
                raise TypeError(
                    "a whole MultiplexerMessage is the request itself: set its type and to, not type= or to="
                )
            request = stamped(message, self.instance_id, self.random)
        elif type is None:
            raise TypeError("a query of a payload needs its type")
        else:
            fields: dict[str, Any] = {"message": message, "type": type}
            if to:
                fields["to"] = to
            request = self.new_message(**fields)
        raw_request = request.SerializeToString()
        if callback is None:
            raw, connection = self._native.query(raw_request, timeout, lane, on_received)
            reply = self._parse(raw)
            if reply.type == types.BACKEND_ERROR:
                raise BackendError(reply.message)
            return (reply, connection) if with_connection else reply

        def on_result(raw: bytes | None, connection: Any, error: Exception | None) -> None:
            """The C++ side's callback: turn bytes into a message, or pass the error on."""
            if error is not None:
                callback(error)
                return
            assert raw is not None, "a result without an error carries the reply"
            reply = self._parse(raw)
            if reply.type == types.BACKEND_ERROR:
                callback(BackendError(reply.message))
            else:
                callback((reply, connection) if with_connection else reply)

        self._native.query_with_callback(raw_request, on_result, timeout, lane, on_received)
        return None

    # The pickle convention: a payload that is a Python pickle, answered by a
    # MultiplexerServer (or any backend using send_pickle) with another. Only
    # between Python peers, and only where the network is trusted, since
    # unpickling runs code.
    @overload
    def query_pickle(
        self, data: Any, type: int, timeout: float = ..., *, callback: None = ..., **kwargs: Any
    ) -> Any: ...

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
        unpickled payload or the exception instance, what unpickling raised
        included. The kwargs are query()'s: `to`, `multiplexer`,
        `on_received`; `with_connection` raises TypeError, the result being
        the payload alone."""
        if "with_connection" in kwargs:
            raise TypeError("query_pickle() returns the payload alone: no with_connection")
        if callback is None:
            return pickle.loads(self.query(pickle.dumps(data), type, timeout, **kwargs).message)

        def unpickle(result: MultiplexerMessage | Exception) -> None:
            """Unpickle a reply, pass an exception through: the query's, or what unpickling raised."""
            if not isinstance(result, Exception):
                try:
                    result = pickle.loads(result.message)
                except Exception as error:  # a reply that is no pickle
                    result = error
            callback(result)

        self.query(pickle.dumps(data), type, timeout, callback=unpickle, **kwargs)
        return None

    def send_pickle(self, data: Any, **kwargs: Any) -> int:
        """send_message() with `data` pickled as the payload; the kwargs are send_message's."""
        return self.send_message(pickle.dumps(data), **kwargs)

    def shutdown(self, timeout: float = CLOSE_FLUSH_SECONDS) -> None:
        """Fail every query in flight, write what was sent before the call,
        and what the io thread sends meanwhile, a server's refusal of what
        still arrives say, `timeout` seconds in all; any other send meanwhile
        raises NotConnected. Then close the connections and stop the
        thread; what is still unwritten then is dropped and reported
        (on_drop), at once with timeout=0. Returns once every multiplexer
        has closed its side too, a round trip, a second at most, so that
        what was written arrives. The client lets go of its callbacks then,
        so that it is freed when dropped: until shut down, one given
        on_message refers to itself through it and stays alive."""
        self._native.shutdown(timeout)

    def __enter__(self: _ThreadedClientT) -> _ThreadedClientT:
        """The client, for a `with` block, at whose end it is shut down
        however the block ended."""
        return self

    def __exit__(self, *exc: object) -> None:
        """shutdown(), at the end of a `with` block."""
        self.shutdown()
