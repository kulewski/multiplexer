"""The Python Client: a thin layer over the C++ Client in multiplexer._native.

The C++ side owns the connections and queues and runs the io_service inside
each call; this side parses and builds MultiplexerMessage objects, and
implements the query algorithm in Client.query, step for step the same as
Client::_query in multiplexer/client.h. Peers use multiplexer.clients, which
adds the backend conventions on top of this.

Messages cross the boundary as bytes: read_message() returns the serialized
frame body and it is parsed here, so the C++ and Python protobuf runtimes
never share objects.
"""

import atexit
import math
import time
import traceback
import warnings
from typing import Any, Callable, ClassVar, Literal, overload

import google.protobuf.message

import multiplexer._native as _mxclient
from multiplexer._native import (
    CLOSE_FLUSH_SECONDS,
    DEFAULT_TIMEOUT,
    MAX_SECONDS,
    ConnectionWrapper,
    DropReason,
    Lane,
    MultiplexerClientError,
    NotConnected,
    OperationFailed,
    OperationTimedOut,
    UsedAfterFork,
)
from multiplexer.Multiplexer_pb2 import BackendForPacketSearch, MultiplexerMessage, Routing
from multiplexer.multiplexer_constants import types
from multiplexer.mxlog import HIGHVERBOSITY, MEDIUMVERBOSITY, WARNING, log
import multiplexer.protocolbuffers  # the read-only aliases of the sender's former names

# What this module is imported for: the client, its exceptions and handles,
# the message helpers, the default timeout, and how long an end call writes
# what was sent before it.
__all__ = [
    "Client",
    "ConnectionWrapper",
    "DropReason",
    "Lane",
    "MultiplexerClientError",
    "NotConnected",
    "OperationFailed",
    "OperationTimedOut",
    "UsedAfterFork",
    "DEFAULT_TIMEOUT",
    "CLOSE_FLUSH_SECONDS",
    "TimeoutTicker",
    "initialize_message",
    "make_message",
    "dict_message",
    "parse_message",
]


def _begin_exit() -> None:
    """Runs from atexit, before the interpreter starts finalizing: tells the
    binding that threads coming back from blocking waits must park rather
    than take the GIL (see GilRelease in _native.cc), that no callback into
    Python may start, and waits for the callbacks in progress to finish.
    Without this a daemon thread could be killed by the interpreter inside
    the binding's C++ frames, or run Python without the GIL, which crashes."""
    _mxclient.begin_exit()


atexit.register(_begin_exit)


def initialize_message(_message, **kwargs):
    """Set the fields of a protocol buffer message from kwargs; nested
    messages from dicts, repeated fields from lists."""
    message = _message
    for key, value in kwargs.items():
        if isinstance(value, (list, tuple)):
            for element in value:
                if isinstance(element, google.protobuf.message.Message):
                    getattr(message, key).add().CopyFrom(element)

                elif isinstance(element, dict):
                    initialize_message(getattr(message, key).add(), **element)

        elif isinstance(value, dict):
            initialize_message(getattr(message, key), **value)

        else:
            setattr(message, key, value)

    return message


def make_message(_type, **kwargs):
    """
    make_message(MessageType, **kwargs) -> instance of MessageType with
    attributes set
    Create a message using factory function type and
    assign its attributes according do kwargs (attribute, value).
    """
    message = _type()
    initialize_message(message, **kwargs)
    return message


def renamed_sender(kwargs: dict) -> None:
    """A sender given to new_message() under a name the field had, `from`
    up to 2.3.1 or the `from_` alias, moved to `sender`, with a
    DeprecationWarning; a `sender` given as well wins. For a release, as
    the read aliases (protocolbuffers.py)."""
    for former in ("from", "from_"):
        if former in kwargs:
            warnings.warn("%s= is the field sender now: pass sender=" % former, DeprecationWarning, stacklevel=3)
            kwargs.setdefault("sender", kwargs.pop(former))


def wait_seconds(seconds: float) -> float:
    """`seconds` as a Python wait takes them, read as mx::from_seconds reads
    every timeout in C++: at most MAX_SECONDS, math.inf too, since Python's
    own waits refuse more; a negative one, no deadline either, MAX_SECONDS
    too, where Python's waits end at once or refuse it; NaN, no time at
    all, 0."""
    if math.isnan(seconds):
        return 0.0
    if seconds < 0:
        return MAX_SECONDS
    return min(seconds, MAX_SECONDS)


def stamped(mxmsg: MultiplexerMessage, instance_id: int, fresh_id: Callable[[], int]) -> MultiplexerMessage:
    """`mxmsg` as every send that takes a whole message sends it: itself
    when it has an id and a sender, else a copy with an empty id made fresh
    by `fresh_id` and an empty sender set to `instance_id`, as new_message()
    and a reply fill them, since every receiver drops a message without an
    id. The C++ clients frame it the same way (frame_stamped)."""
    if mxmsg.id and mxmsg.sender:
        return mxmsg
    filled = MultiplexerMessage()
    filled.CopyFrom(mxmsg)
    if not filled.id:
        filled.id = fresh_id()
    if not filled.sender:
        filled.sender = instance_id
    return filled


def whole(mxmsg: MultiplexerMessage, fields: dict, instance_id: int, fresh_id: Callable[[], int]) -> MultiplexerMessage:
    """A whole MultiplexerMessage as a send takes it: the message itself,
    stamped(); `fields`, message fields given beside it, are a TypeError,
    since nothing of the message is changed."""
    if fields:
        raise TypeError(
            "a whole MultiplexerMessage is sent as it is: set its fields, not %s" % ", ".join(sorted(fields))
        )
    return stamped(mxmsg, instance_id, fresh_id)


def as_reply(mxmsg: MultiplexerMessage, request: MultiplexerMessage, instance_id: int, fresh_id: Callable[[], int]):
    """A copy of `mxmsg` with its empty fields filled in as a reply to
    `request`: id and sender, as stamped() fills them, and to, references and
    workflow from the request. What every server class does with a whole
    message given as the reply, as C++ Request::reply does."""
    reply = MultiplexerMessage()
    reply.CopyFrom(mxmsg)
    if not reply.id:
        reply.id = fresh_id()
    if not reply.sender:
        reply.sender = instance_id
    if not reply.to:
        reply.to = request.sender
    if not reply.references:
        reply.references = request.id
    if not reply.workflow:
        reply.workflow = request.workflow
    return reply


def dict_message(m, all_fields=False, recursive=False):
    """
    Operation reverse to make_message.
    If all_fields == False, only set fields are provied.
    """
    d = {}
    if all_fields:
        for fd in m.DESCRIPTOR.fields:
            d[fd.name] = getattr(m, fd.name)
    else:
        for fd, value in m.ListFields():
            d[fd.name] = value

    if recursive:
        for k, v in d.items():
            if isinstance(v, google.protobuf.message.Message):
                d[k] = dict_message(v, all_fields=all_fields, recursive=True)

    return d


def parse_message(type, buffer):
    """An instance of the protocol buffer class `type` parsed from `buffer`."""
    t = type()
    t.ParseFromString(buffer)
    return t


# A synchronous query's stages, as __await reads what ends each.
_REQUEST, _SEARCH, _DIRECT, _LATE = "request", "search", "direct", "late"
# How a stage's wait ended, as Client::Outcome in client.cc: an answer, a
# delivery error for the stage's own attempt, the search's PING, nobody
# anywhere, or in the late wait no attempt left, or the watched connection
# lost.
_ANSWER, _REFUSED, _FOUND, _NOBODY, _LOST = "answer", "refused", "found", "nobody", "lost"


class _Ledger(object):
    """What a synchronous query sent, for __await to read what comes back
    by the table every client follows (docs/query.md), as Client::Ledger
    in client.cc: its attempts, the request and the request sent again, by
    id in the order sent, those struck off, refused by a delivery error or
    found given up on as the late wait begins, and its search, or locating
    PING, with the connections it went through that have not said nobody
    yet."""

    __slots__ = ["attempts", "struck", "search_id", "searched"]

    def __init__(self) -> None:
        """Nothing sent yet."""
        object.__init__(self)
        self.attempts: dict[int, _mxclient.Attempt] = {}
        self.struck: set[int] = set()
        self.search_id = 0
        self.searched: list[ConnectionWrapper] = []

    @property
    def last(self) -> int:
        """The id of the attempt sent last, 0 before the first."""
        return next(reversed(self.attempts), 0)

    def any_left(self) -> bool:
        """Whether an attempt is left that a backend may have: one not struck off."""
        return any(attempt_id not in self.struck for attempt_id in self.attempts)

    def strike_given_up(self) -> None:
        """Strikes off the attempts the client gave up on, or never placed:
        nobody can have them."""
        self.struck.update(attempt_id for attempt_id, attempt in self.attempts.items() if attempt.given_up)

    def answered(self, connection: ConnectionWrapper) -> bool:
        """The search's "nobody" from `connection`: struck off the
        connections it waits for; False when it waited for none from there."""
        for index, searched in enumerate(self.searched):
            if searched.is_same_connection(connection):
                del self.searched[index]
                return True
        return False


def _check_size_sent_again(request: MultiplexerMessage) -> None:
    """ValueError, before anything goes out, when the request sent again, a
    copy of `request` with an id of its own, for a typed query the `to` of
    the backend found, and a delivery error asked for, would be over
    MAX_MESSAGE_SIZE: measured at its largest, as every client measures it
    (check_size_sent_again in client.cc). `request` is as it was afterwards."""
    largest = 2**64 - 1
    id_, had_to, to = request.id, request.HasField("to"), request.to
    asked, asked_value = request.HasField("report_delivery_error"), request.report_delivery_error
    request.id = largest
    if not to:
        request.to = largest
    request.report_delivery_error = True
    size = request.ByteSize()
    request.id = id_
    if had_to:
        request.to = to
    else:
        request.ClearField("to")
    if asked:
        request.report_delivery_error = asked_value
    else:
        request.ClearField("report_delivery_error")
    if size > _mxclient.MAX_MESSAGE_SIZE:
        raise ValueError(
            "a message of %d bytes, over the limit of %d (MAX_MESSAGE_SIZE)" % (size, _mxclient.MAX_MESSAGE_SIZE)
        )


class TimeoutTicker(object):
    """One deadline shared by the steps of a call: calling it gives the seconds
    left, so each step's C++ timeout is what remains of the whole call's.
    A negative timeout never expires; NaN is no time at all, as 0. On the
    monotonic clock, as the C++ side's on steady_clock: a step of the wall
    clock neither cuts nor stretches it."""

    __slots__ = ["_expires_at", "_timeout"]

    def __init__(self, timeout):
        """`timeout` in seconds from now; negative never expires, NaN is 0."""
        object.__init__(self)
        if math.isnan(timeout):
            timeout = 0.0
        self._timeout = timeout
        self._expires_at = time.monotonic() + timeout

    def __call__(self):
        """Seconds left, never below 0; the negative timeout itself if it never expires."""
        if self._timeout >= 0:
            return max(self._expires_at - time.monotonic(), 0)
        else:
            return self._timeout

    def permit(self) -> bool:
        """Whether there is time left."""
        return self() > 0 if self._timeout >= 0 else True


class Client(_mxclient.Client):
    """See the module docstring. `multiplexer=` arguments take ONE (some
    connection, round robin), ALL (every connection), a ConnectionWrapper
    (that connection while it is live, another when it is gone) or a Lane
    from lane() (the lane's connection, which follows a failover, or, for
    a pinned lane, that connection only)."""

    DEFAULT_TIMEOUT = _mxclient.DEFAULT_TIMEOUT
    ONE = 0
    ALL = 1

    def __init__(self, client_type):
        """initialize a client with given client (peer) type"""
        self._client_type = client_type
        self.__instance_id: int | None = None
        self.__on_received: Callable[[int], None] | None = None  # the query under way's, see query()
        super(Client, self).__init__(client_type)

    def __get_instance_id(self) -> int:
        """The instance id, read from the C++ side once."""
        if self.__instance_id is None:
            self.__instance_id = super(Client, self)._get_instance_id()
        return self.__instance_id

    instance_id = property(__get_instance_id)

    @property
    def dropped(self) -> int:
        """How many messages this client gave up on so far, each copy of one
        sent to ALL; the drop observer (`on_drop`) hears of each as it goes."""
        return self._dropped()

    def async_connect(self, endpoint):
        """
        asynchronous connect
        endpoint is e.g. ("localhost", 1980)
        return ConnectionWrapper
        """
        return super(Client, self).async_connect_to(endpoint[0], endpoint[1])

    def connect(self, endpoint, timeout=DEFAULT_TIMEOUT):
        """
        synchronous connect
        endpoint is e.g. ("localhost", 1980)
        return ConnectionWrapper
        """
        return super(Client, self).connect_to(endpoint[0], endpoint[1], timeout)

    def disconnect(self, endpoint: tuple[str, int]) -> bool:
        """Drop the multiplexer given to connect() or async_connect() as
        `endpoint`, the same (host, port), an address in any spelling: no
        reconnect to it any more, unless connect() is called again, and a
        live connection to it closed, what it had not written handed to
        the other connections or held, as a lost connection's is. Returns
        at once, whether the client had it; raises NotConnected after
        shutdown(), as connect() does."""
        return super(Client, self).disconnect_from(endpoint[0], endpoint[1])

    def set_routing(self, routing: Routing) -> None:
        """Which of a multiplexer's routing paths reach this peer, a
        `Routing` from Multiplexer.proto: `any` for rules with whom ANY,
        `all` for whom ALL, both True by default, and `last_resort` for
        getting what nobody else of the type could take; a message with
        `to` always arrives. Told to every multiplexer as the loop runs
        and carried in the welcome of every connection made from now on;
        routing_acknowledged() says when it is in effect everywhere. A
        backend draining sets any and all False; see docs/leaving.md."""
        super(Client, self).set_routing_serialized(routing.SerializeToString())

    def routing_acknowledged(self) -> bool:
        """Whether every connected multiplexer has the routing given to
        set_routing() in effect, so that nothing routed by a path turned
        off is on its way from them, except as a last resort."""
        return super(Client, self).routing_acknowledged()

    def wait_for_connection(self, connection: ConnectionWrapper, timeout: float = DEFAULT_TIMEOUT) -> bool:
        """wait for connection initiated with async_connect"""
        return super(Client, self).wait_for_connection(connection, timeout)

    def __receive_message(self, timeout: float = -1):
        """
        blocking read from all the sockets (or from incoming message queue)
        returns (MultiplexerMessage, ConnectionWrapper)
        """
        next = super(Client, self).read_raw_message(timeout)
        assert isinstance(next[0], bytes)
        mxmsg = parse_message(MultiplexerMessage, next[0])
        return (mxmsg, next[1])

    def bind_to_current_thread(self):
        """Make the calling thread the one this client is used from, for a
        client built on one thread and driven from another;
        BaseMultiplexerServer.serve_forever() calls it on entry. Only the
        debug-build thread checks care: after this they fail on the next call
        from the previous thread."""
        super(Client, self).bind_to_current_thread()

    def receive_message(self, timeout=-1):
        """The next message from any connection as (MultiplexerMessage,
        connection); waits up to `timeout` (-1: forever). Raises
        OperationTimedOut, NotConnected."""
        return self.__receive_message(timeout)

    def handle_drop(self, mxmsg, connwrap=None):
        """A message a synchronous query read while it waited for another,
        the reply: logged and dropped.
        Override in a subclass to keep such messages, a DELIVERY_ERROR for
        an event sent before, say."""
        log(
            WARNING,
            HIGHVERBOSITY,
            text="dropping message %r"
            % dict(
                id=mxmsg.id,
                type=mxmsg.type,
                to=mxmsg.to,
                sender=mxmsg.sender,
                references=mxmsg.references,
                len=len(mxmsg.message),
            ),
        )

    def _check_type(self, message, type, exc=OperationFailed):
        """Raise `exc` unless `message` has type `type`."""
        assert isinstance(message, MultiplexerMessage)
        if message.type != type:
            raise exc()

    def _unpack(self, message, type):
        """Parse a payload as the protocol buffer class `type`."""
        return parse_message(type, message)

    def event(self, *args, **kwargs):
        """send message through all active MX connections (arguments same as
        for send_message)"""
        return self.send_message(*args, multiplexer=Client.ALL, **kwargs)

    def lane(self, pinned: bool = False, connection: "ConnectionWrapper | None" = None) -> Lane:
        """A Lane: one connection for a stream of messages, given as
        `multiplexer=` to send_message() and query(). Empty until first use,
        when it takes the connection the library chose; a lane that is not
        pinned follows a failover and adopts the connection a query's reply
        came through. A pinned lane keeps its first connection for good and
        raises NotConnected once that is gone. `connection`, one a reply
        came through, seeds the lane."""
        return Lane(connection, pinned) if connection is not None else Lane(pinned)

    @overload
    def query(
        self,
        message: Any,
        type: "int | None" = ...,
        timeout: float = ...,
        to: int = ...,
        multiplexer: "int | Lane | ConnectionWrapper" = ...,
        with_connection: Literal[False] = ...,
        *,
        on_received: "Callable[[int], None] | None" = ...,
    ) -> MultiplexerMessage: ...

    @overload
    def query(
        self,
        message: Any,
        type: "int | None" = ...,
        timeout: float = ...,
        to: int = ...,
        multiplexer: "int | Lane | ConnectionWrapper" = ...,
        *,
        with_connection: Literal[True],
        on_received: "Callable[[int], None] | None" = ...,
    ) -> tuple[MultiplexerMessage, ConnectionWrapper]: ...

    def query(
        self,
        message: Any,
        type: "int | None" = None,
        timeout: float = DEFAULT_TIMEOUT,
        to: int = 0,
        multiplexer: "int | Lane | ConnectionWrapper" = ONE,
        with_connection: bool = False,
        *,
        on_received: "Callable[[int], None] | None" = None,
    ) -> "MultiplexerMessage | tuple[MultiplexerMessage, ConnectionWrapper]":
        """Send a request and return its reply, a MultiplexerMessage.

        The request is `message` itself when it is a whole
        MultiplexerMessage, typed by its own `type` and addressed by its own
        `to`, an empty id and sender filled in; `type=` or `to=` beside it is
        a TypeError. Else it is built from the payload, bytes, str or a
        protocol buffer message, and `type`, which it needs, and `to`.

        The request goes through one connection. If it comes back as a
        DELIVERY_ERROR, no reply arrives within `timeout` seconds, or the
        connection is lost under the wait, every connection is asked
        (BACKEND_FOR_PACKET_SEARCH) for a backend that handles `type`; the
        first one to answer gets the request again, addressed directly, with
        a fresh `timeout`, its second and last time out. A query never goes
        back a stage, and a reply to either attempt answers it. Raises
        OperationTimedOut when a stage runs out of time, OperationFailed
        when no backend can be found and nobody took the request (one that
        timed out or went with its connection may still be answered, and is
        waited for), NotConnected when there is no live connection;
        docs/query.md lists every way a stage ends.

        With `to`, the instance id of a peer, the request is addressed: only
        that peer ever gets it. If a multiplexer reports the peer is not
        behind it, or the connection dies under the wait, the peer is located
        with a PING addressed to it on every connection (answered as long as
        the peer lives, by the server classes, ThreadedClient and AsyncClient
        whatever their search policy, never by a SyncClient), and the request
        goes again, its second and last time, through the connection that
        found it. A peer nobody has is OperationFailed, unless a request it
        may have taken, after a lost connection, can still be answered, which
        the query then waits for; one `timeout` covers the three stages.

        `multiplexer` is ONE, a Lane from lane() or a ConnectionWrapper: with
        a lane the request goes through the lane's connection and the lane
        adopts the connection the reply came through; a pinned lane allows
        no other connection and raises NotConnected once its own is gone;
        a connection is preferred while it is live, as with send_message().
        With `with_connection` the result is (reply, connection).

        `on_received`, when given, is called here, on the caller's thread
        while the query waits (so it must not query or receive through this
        client, which could take the reply), with the instance id of each
        backend that acknowledges the request with REQUEST_RECEIVED
        (notify_start()): once, normally, or again when a retry reached a
        backend, the same or another. Nothing about the query changes for
        it; one that raises has its traceback printed.
        """
        request = self.__request(message, type, to)
        _check_size_sent_again(request)
        # Held for the query's duration, where the acknowledgements are met;
        # whatever was there before is back afterwards.
        previous, self.__on_received = self.__on_received, on_received
        try:
            if request.to:
                response, connwrap = self.__query_addressed(request, timeout, multiplexer)
            else:
                response, connwrap = self.__query_typed(request, timeout, multiplexer)
        finally:
            self.__on_received = previous
        return (response, connwrap) if with_connection else response

    def __request(self, message: Any, type: "int | None", to: int) -> MultiplexerMessage:
        """A query's request, a message of its own that the stages may set
        fields of: a copy of `message` when it is a whole MultiplexerMessage,
        an empty sender filled in and an id of its own, as every attempt gets,
        so that one message may be queried again and again; else one built
        from the payload, `type` and `to`. TypeError for `type` or `to`
        beside a whole message, and for a payload without its type."""
        if isinstance(message, MultiplexerMessage):
            if type is not None or to:
                raise TypeError(
                    "a whole MultiplexerMessage is the request itself: set its type and to, not type= or to="
                )
            request = MultiplexerMessage()
            request.CopyFrom(message)
            request.id = self.random()
            if not request.sender:
                request.sender = self.instance_id
            return request
        if type is None:
            raise TypeError("a query of a payload needs its type")
        return self.new_message(type=type, message=message, to=to)

    def __again(self, request: MultiplexerMessage, to: int) -> MultiplexerMessage:
        """The request again, its last time out: a copy with a fresh id, `to`
        the instance found, and a delivery error asked for, which says that
        instance is gone; every other field the request's."""
        again = MultiplexerMessage()
        again.CopyFrom(request)
        again.id = self.random()
        again.to = to
        again.report_delivery_error = True
        return again

    def __query_typed(self, query, timeout, multiplexer):
        """The three stages of a typed query, docs/query.md, `query` the
        request; the same as Client::_query in client.cc, every stage's wait
        reading what comes back by the one table every client follows
        (__await)."""
        lane = multiplexer if isinstance(multiplexer, Lane) else None
        if isinstance(multiplexer, ConnectionWrapper):
            lane = Lane(multiplexer)  # preferred, then any
        ledger = _Ledger()
        # Stage 1: the request through one connection. A reply ends the
        # query here; a delivery error says nobody took it; its time running
        # out, or its connection lost, leaves it with a backend perhaps,
        # routed before the connection went, its reply able to come back
        # another way.
        ticker = TimeoutTicker(timeout)
        try:
            used = self.__send_attempt(query, ticker, None, lane, ledger)
            outcome, answer = self.__await(ledger, ticker, _REQUEST, used, lane)
            if outcome == _ANSWER:
                self.__adopt(lane, answer[1])
                return answer
        except OperationTimedOut:
            pass

        # Stage 2: ask every multiplexer who has a backend for this type, at
        # once. The request is not sent again: it goes out at most twice,
        # and a query never goes back a stage. The search is routed by the
        # request type's own rule with whom forced to ALL, so every live
        # backend answers with a PING. Through a pinned lane the one
        # multiplexer behind it is asked instead.
        search = BackendForPacketSearch()
        search.packet_type = query.type
        mxmsg = self.new_message(message=search, type=types.BACKEND_FOR_PACKET_SEARCH)
        ticker = TimeoutTicker(timeout)
        pinned_connection = self.__send_search(mxmsg, ticker, lane, ledger)
        outcome, answer = self.__await(ledger, ticker, _SEARCH, pinned_connection, lane)
        if outcome not in (_ANSWER, _FOUND):
            # Nobody has a backend of this type: only a backend that may have
            # taken the request can still answer it, and the stage waits for
            # that.
            outcome, answer = _ANSWER, self.__late(ledger, ticker, lane)
        if outcome == _ANSWER:
            self.__adopt(lane, answer[1])
            return answer

        # Stage 3: the request again, to the backend that answered first, by
        # instance id and through the connection its PING came on, asking for
        # a delivery error, which says that backend is gone.
        ping, ping_connection = answer
        direct_query = self.__again(query, ping.sender)
        ticker = TimeoutTicker(timeout)
        used = self.__send_attempt(direct_query, ticker, ping_connection, lane, ledger)
        outcome, answer = self.__await(ledger, ticker, _DIRECT, used, lane)
        if outcome != _ANSWER:
            # The backend found is gone, or the direct request's connection
            # went under it: a reply can come only to an attempt a backend
            # may have, and nothing goes out again.
            answer = self.__late(ledger, ticker, lane)
        self.__adopt(lane, answer[1])
        return answer

    def __query_addressed(self, request, timeout, multiplexer):
        """An addressed query, docs/query.md "An addressed query"; the same
        as Client::_query_addressed in client.cc. One deadline for the
        three stages: the request, the PING that locates the addressee when
        a multiplexer said it is not behind it or the connection died, the
        request again through the connection that found it, its second and
        last time out; `request` its request, addressed by its `to`."""
        lane = multiplexer if isinstance(multiplexer, Lane) else None
        if isinstance(multiplexer, ConnectionWrapper):
            lane = Lane(multiplexer)  # preferred, then any
        ticker = TimeoutTicker(timeout)
        to = request.to
        request.report_delivery_error = True  # "not behind this multiplexer" must come back as a message
        ledger = _Ledger()
        used = self.__send_attempt(request, ticker, None, lane, ledger)
        outcome, answer = self.__await(ledger, ticker, _REQUEST, used, lane)
        if outcome == _ANSWER:
            self.__adopt(lane, answer[1])
            return answer

        # Locate: a PING addressed to the instance, with delivery errors
        # requested, so that a multiplexer without the instance says so. The
        # request is not sent again on a lost connection: it goes out at most
        # twice, and a query never goes back a stage.
        mxmsg = self.new_message(message=b"", type=types.PING, to=to, report_delivery_error=True)
        pinned_connection = self.__send_search(mxmsg, ticker, lane, ledger)
        outcome, answer = self.__await(ledger, ticker, _SEARCH, pinned_connection, lane)
        if outcome not in (_ANSWER, _FOUND):
            # No multiplexer has the instance: a request it may have taken
            # can still be answered, and the query waits for that reply.
            outcome, answer = _ANSWER, self.__late(ledger, ticker, lane)
        if outcome == _ANSWER:
            self.__adopt(lane, answer[1])
            return answer

        # The request again, a fresh id, through the connection the answer
        # came on; the lane adopts it.
        again = self.__again(request, to)
        used = self.__send_attempt(again, ticker, answer[1], lane, ledger)
        outcome, answer = self.__await(ledger, ticker, _DIRECT, used, lane)
        if outcome != _ANSWER:
            # The addressee is gone, or the request's connection went under
            # it: the query waits for a reply to an attempt it may still have.
            answer = self.__late(ledger, ticker, lane)
        self.__adopt(lane, answer[1])
        return answer

    def __send_attempt(self, mxmsg, ticker, preferred, lane, ledger):
        """Sends one attempt of a query, the request or the request sent
        again, through one connection, `preferred` while it is live, waiting
        for a connection if none is; the same as Client::_send_attempt in
        client.cc. The ledger records it before the send, its tracker once
        it is placed: a send that runs out of time leaves the message
        queued, to be written later, and a reply to it still answers.
        Returns the connection that wrote it."""
        attempt = ledger.attempts[mxmsg.id] = _mxclient.Attempt()
        return self.send_one(mxmsg.SerializeToString(), mxmsg.id, mxmsg.type, preferred, lane, ticker(), attempt)

    def __send_search(self, mxmsg, ticker, lane, ledger):
        """Sends the search, or the locating PING: a copy on every live
        connection, waiting for one when none is, or through a pinned lane
        its one connection; the same as Client::_send_search in client.cc.
        The copies wait for room, or for a connection, as long as the stage
        has left and a moment more. The ledger records its id and the
        connections it went through. Returns the pinned lane's connection,
        None without one."""
        ledger.search_id = mxmsg.id
        raw = mxmsg.SerializeToString()
        if lane is not None and lane.pinned:
            used = self.send_one(raw, mxmsg.id, mxmsg.type, None, lane, ticker())
            ledger.searched = [used]
            return used
        # Waits for a connection when none is live.
        ledger.searched = self._schedule_search(raw, mxmsg.id, mxmsg.type, ticker())
        return None

    def __await(
        self,
        ledger: _Ledger,
        ticker: TimeoutTicker,
        stage: str,
        watch: "ConnectionWrapper | None",
        lane: "Lane | None",
    ) -> "tuple[str, tuple[MultiplexerMessage, ConnectionWrapper]]":
        """Waits for what ends a query's stage, reading each message by the
        one table every client follows (docs/query.md), as ThreadedClient
        does; the same as Client::_await in client.cc. A reply to any
        attempt is the answer; a delivery error for an attempt strikes it
        off, and ends the stage only when it is the stage's own, the last
        attempt sent in the request's stage or the direct request's, the
        late wait ending once no attempt is left; in the search's stage a
        PING for it is the instance found, and a delivery error for it is
        nobody behind the connection it came on, counted once per connection
        the search went through, nobody anywhere once every one of them has
        said so. Anything else for the search is nothing, an
        acknowledgement goes to the query's on_received, and what answers
        nothing the query sent to handle_drop(). With `watch`, an attempt's
        connection or a pinned lane's, its loss ends the wait, NotConnected
        through a pinned lane, which is what the pin means. Returns (how it
        ended, (message, connection)), for a loss an empty message and the
        connection lost; OperationTimedOut when `ticker` runs out."""
        accept_ids = [*ledger.attempts, *([ledger.search_id] if stage == _SEARCH else [])]
        ignore_ids = [ledger.search_id] if ledger.search_id else []  # the search's late answers
        while ticker.permit():
            if watch is None:
                mxmsg, connwrap = self.__receive_message(timeout=ticker())
            else:
                got = self.read_raw_message_watching(ticker(), watch)
                if got is None:
                    if lane is not None and lane.pinned:
                        raise NotConnected()
                    log(WARNING, MEDIUMVERBOSITY, text="connection lost while waiting for a reply to %d" % ledger.last)
                    return _LOST, (MultiplexerMessage(), watch)
                mxmsg, connwrap = self.__parse_incoming(got)
            if mxmsg.type == types.REQUEST_RECEIVED:
                self.__acknowledged(mxmsg, accept_ids, ignore_ids)
                continue
            if mxmsg.references in ledger.attempts:
                if mxmsg.type != types.DELIVERY_ERROR:
                    return _ANSWER, (mxmsg, connwrap)
                ledger.struck.add(mxmsg.references)  # nobody has it
                if stage in (_REQUEST, _DIRECT) and mxmsg.references == ledger.last:
                    return _REFUSED, (mxmsg, connwrap)
                if stage == _LATE and not ledger.any_left():
                    return _NOBODY, (mxmsg, connwrap)
                continue  # an earlier attempt's: struck off, and the stage goes on
            if stage == _SEARCH and mxmsg.references == ledger.search_id:
                if mxmsg.type == types.PING:
                    return _FOUND, (mxmsg, connwrap)
                if mxmsg.type == types.DELIVERY_ERROR and ledger.answered(connwrap) and not ledger.searched:
                    return _NOBODY, (mxmsg, connwrap)
                continue
            if mxmsg.references not in ignore_ids:
                self.handle_drop(mxmsg, connwrap)
        raise OperationTimedOut()

    def __late(
        self, ledger: _Ledger, ticker: TimeoutTicker, lane: "Lane | None"
    ) -> "tuple[MultiplexerMessage, ConnectionWrapper]":
        """The query's last wait: a reply to an attempt a backend may still
        have, one placed and neither refused nor given up on, until `ticker`
        runs out; none left, nothing can answer any more, OperationFailed.
        The same as Client::_late in client.cc, which says why the trackers
        are read once, as the wait begins, and why a pinned lane's lost
        connection is NotConnected."""
        ledger.strike_given_up()
        if not ledger.any_left():
            raise OperationFailed
        watch = lane.connection if lane is not None and lane.pinned else None
        outcome, answer = self.__await(ledger, ticker, _LATE, watch, lane)
        if outcome != _ANSWER:
            raise OperationFailed  # the last attempt a backend might have had drew a delivery error
        return answer

    @staticmethod
    def __adopt(lane, connwrap):
        """A lane takes the connection a message went through or a reply came through."""
        if lane is not None:
            lane.adopt(connwrap)

    def _send_and_receive(
        self,
        message,
        accept_ids=(),
        ignore_ids=(),
        timeout=DEFAULT_TIMEOUT,
        handle_delivery_errors=False,
        ignore_types=(),
        timeout_ticker=None,
        lane=None,
        attempts=None,
        **kwargs,
    ):
        """Send `message` once and return the first message that references
        it, a DELIVERY_ERROR included, which a query reads by its stages
        instead (query()): how a test sees a request's own answer.

        Returns (reply, connection). No search and no retry: raises
        OperationTimedOut after `timeout` seconds, or when `timeout_ticker`,
        a deadline shared with other steps, runs out. `accept_ids` are
        further ids a reply may reference, `ignore_ids` and `ignore_types`
        are skipped silently, other unexpected messages go to handle_drop().
        `multiplexer` may be a Lane; `lane` is one to update while
        `multiplexer` names a connection to prefer. With
        `multiplexer=Client.ALL` and `handle_delivery_errors`, one
        DELIVERY_ERROR per copy is taken before the last is returned.

        A message is sent once and a MultiplexerMessage passed in is never
        changed. When the connection that wrote it dies under the wait, it
        is not sent again, having perhaps been routed first: NotConnected.
        `attempts`, a dict, gets the message by its id before it is sent,
        an Attempt that learns its tracker once it is placed: a send that
        runs out of time leaves the message queued, to be written later.
        """
        if timeout_ticker is None:
            timeout_ticker = TimeoutTicker(timeout)
        multiplexer = kwargs.get("multiplexer", Client.ONE)
        if isinstance(multiplexer, Lane):
            lane = kwargs.pop("multiplexer")
        if multiplexer is not Client.ALL:
            return self.__send_and_receive_one(
                message, accept_ids, ignore_ids, ignore_types, timeout_ticker, lane, attempts, **kwargs
            )

        id, tracker = self.__schedule_all(message, timeout_ticker(), **kwargs)
        if tracker == 0:
            # No connection at all: let the reconnect timers fire, then once more.
            if not self.wait_for_any_connection(timeout_ticker()):
                raise NotConnected()
            id, tracker = self.__schedule_all(message, timeout_ticker(), **kwargs)

        accept_ids = [id, *accept_ids]
        while timeout_ticker.permit():
            mxmsg, connwrap = self._receive(
                accept_ids=accept_ids,
                ignore_ids=ignore_ids,
                ignore_types=ignore_types,
                timeout_ticker=timeout_ticker,
            )
            if mxmsg.type == types.DELIVERY_ERROR and handle_delivery_errors:
                # one DELIVERY_ERROR per connection means nobody could take it
                tracker -= 1
                if tracker > 0:
                    continue
            return mxmsg, connwrap

        raise OperationTimedOut

    def __send_and_receive_one(
        self, message, accept_ids, ignore_ids, ignore_types, timeout_ticker, lane, attempts, **kwargs
    ):
        """The single-connection case of _send_and_receive: sent once,
        waiting for a connection if none is live, and a reply waited for. If
        the connection that wrote it dies before the reply arrives, it is
        not sent again, having perhaps been routed first: NotConnected."""
        preferred = kwargs.pop("multiplexer", None)
        if preferred is Client.ONE:
            preferred = None
        kwargs.pop("flush", None)
        if isinstance(message, MultiplexerMessage):
            mxmsg = whole(message, kwargs, self.instance_id, self.random)
        else:
            mxmsg = self.new_message(message=message, **kwargs)
        attempt = None
        if attempts is not None:
            attempt = attempts[mxmsg.id] = _mxclient.Attempt()
        used = self.send_one(
            mxmsg.SerializeToString(), mxmsg.id, mxmsg.type, preferred, lane, timeout_ticker(), attempt
        )
        accept_ids = [mxmsg.id, *accept_ids]
        while timeout_ticker.permit():
            got = self.read_raw_message_watching(timeout_ticker(), used)
            if got is None:
                if lane is None or not lane.pinned:
                    log(WARNING, MEDIUMVERBOSITY, text="connection lost while waiting for a reply to %d" % mxmsg.id)
                raise NotConnected()
            mxmsg_in, connwrap = self.__parse_incoming(got)
            if mxmsg_in.type in ignore_types:
                self.__acknowledged(mxmsg_in, accept_ids, ignore_ids)
                continue
            if mxmsg_in.references in accept_ids:
                return mxmsg_in, connwrap
            if mxmsg_in.references not in ignore_ids:
                self.handle_drop(mxmsg_in, connwrap)
        raise OperationTimedOut()

    def __acknowledged(self, mxmsg, accept_ids, ignore_ids):
        """A skipped message that is a REQUEST_RECEIVED (notify_start())
        acknowledging the request of the query under way, or a retry of it:
        the query's on_received hears from which backend, and the query goes
        on as before, whatever the callback does. ignore_ids holds the id
        of the search or of the locating PING, as nothing acknowledges
        either."""
        on_received = self.__on_received
        if (
            on_received is not None
            and mxmsg.type == types.REQUEST_RECEIVED
            and mxmsg.references in accept_ids
            and mxmsg.references not in ignore_ids
        ):
            try:
                on_received(mxmsg.sender)
            except Exception:
                traceback.print_exc()

    def __parse_incoming(self, got):
        """A (bytes, connection) pair from the C++ side as (MultiplexerMessage, connection)."""
        raw, connwrap = got
        mxmsg = parse_message(MultiplexerMessage, raw)
        return mxmsg, connwrap

    def _receive(
        self,
        accept_ids,
        ignore_ids=(),
        ignore_types=(),
        timeout=DEFAULT_TIMEOUT,
        timeout_ticker=None,
        watch=None,
        kept=None,
    ):
        """Wait for a reply that references one of `accept_ids`, as
        _send_and_receive() does, without sending anything. With `watch`, a
        connection, NotConnected as soon as it is gone. A message of
        `ignore_types` is passed over, appended to `kept` when that is
        given, a list say, for whoever reads such messages."""
        if timeout_ticker is None:
            timeout_ticker = TimeoutTicker(timeout)

        while timeout_ticker.permit():
            if watch is None:
                mxmsg, connwrap = self.__receive_message(timeout=timeout_ticker())
            else:
                got = self.read_raw_message_watching(timeout_ticker(), watch)
                if got is None:
                    raise NotConnected()
                mxmsg, connwrap = self.__parse_incoming(got)
            if mxmsg.type in ignore_types:
                self.__acknowledged(mxmsg, accept_ids, ignore_ids)
                if kept is not None:
                    kept.append(mxmsg)
                continue

            if mxmsg.references in accept_ids:
                return (mxmsg, connwrap)

            if mxmsg.references not in ignore_ids:
                # unexpected message received
                log(
                    WARNING,
                    HIGHVERBOSITY,
                    text="message (id=%d, type=%d, sender=%d, references=%d) "
                    "while waiting for reply for %r"
                    % (mxmsg.id, mxmsg.type, mxmsg.sender, mxmsg.references, accept_ids),
                )
                self.handle_drop(mxmsg, connwrap)

        raise OperationTimedOut()

    def send_message(self, message, **kwargs) -> int:
        """Send a message on one or more connections and return its id.

        `message` is a whole MultiplexerMessage, sent as it is, an empty id
        and sender filled in, or a payload (bytes, str, or a protocol buffer
        message) wrapped into a new one built from the remaining kwargs
        (`type`, `to`, `references`, `workflow`, ...); those beside a whole
        message are a TypeError. Keyword-only options: `multiplexer` is
        Client.ONE (default), Client.ALL, a ConnectionWrapper (that
        connection while it is live; a reply goes back the way the request
        came) or a Lane from lane() (the lane's connection, which the lane
        replaces on a failover, or keeps for good and raises NotConnected
        for when pinned).

        Without `flush` it returns at once: the message is queued, waits
        for room on a full connection, or, with no connection live, is held
        until one comes up, `timeout` seconds at most, and goes out as a
        later call runs the loop; one the client gives up on is reported
        (on_drop). With `flush` it waits until the message reached the
        socket, the first copy for ALL, within `timeout`, a connection that
        dies under it handing it to another or having it held (a copy for
        ALL is held only when no connection is live, and dropped otherwise),
        and raises NotConnected when nothing wrote it with no connection
        live, or when every copy for ALL went with its connection, or
        OperationTimedOut. With a `callback`, flush or not, it returns at
        once and `callback(written)` runs inside a later call that runs the
        loop, 1 once the message is written, the first copy for ALL, 0 once
        it was given up on or shutdown() came first. NotConnected at once
        for a pinned lane whose connection is gone, and after shutdown().
        """
        mxmsg_id, taken = self.__send_message(message, **kwargs)
        if not taken:
            raise NotConnected()
        return mxmsg_id

    def __send_message(self, message, multiplexer=ONE, flush=False, timeout=DEFAULT_TIMEOUT, callback=None, **kwargs):
        """The body of send_message(): wrap, serialize and send as every
        client does (BasicClient::send in C++): placed on a connection, or
        held until one comes up, within `timeout`. With `flush`, wait until
        written, the first copy for ALL, raising as ThreadedClient does
        (Client::_send_one in C++); a `callback` replaces the wait, hearing
        how the message ended instead. Choosing a connection first runs every
        ready handler of the loop, so a connection the multiplexer closed
        while this client was idle is retired, never written into. Returns
        (id, taken), taken False when nothing may take the message: a
        pinned lane whose connection is gone, or the client shut down."""
        if isinstance(message, MultiplexerMessage):
            mxmsg = whole(message, kwargs, self.instance_id, self.random)
        else:
            mxmsg = self.new_message(message=message, **kwargs)

        # Serialized once here; the C++ side wraps the bytes in a frame, with
        # the id and the type a drop is reported by, and queues that same
        # frame on every connection chosen.
        raw = mxmsg.SerializeToString()

        if multiplexer is Client.ALL:
            if flush and callback is None:
                self.send_all_and_wait(raw, mxmsg.id, mxmsg.type, timeout)
                return (mxmsg.id, True)
            return (mxmsg.id, self.send(raw, mxmsg.id, mxmsg.type, True, None, timeout, callback))
        if multiplexer is not Client.ONE and not isinstance(multiplexer, (ConnectionWrapper, Lane)):
            raise NotImplementedError("selecting multiplexer with %r is not supported" % multiplexer)
        lane = multiplexer if isinstance(multiplexer, Lane) else None
        if lane is not None and lane.closed:
            raise NotConnected()
        if flush and callback is None:
            preferred = multiplexer if isinstance(multiplexer, ConnectionWrapper) else None
            self.send_one(raw, mxmsg.id, mxmsg.type, preferred, lane, timeout)
            return (mxmsg.id, True)
        if isinstance(multiplexer, ConnectionWrapper):
            lane = Lane(multiplexer)  # that connection while it lives, another once it is gone
        return (mxmsg.id, self.send(raw, mxmsg.id, mxmsg.type, False, lane, timeout, callback))

    def __schedule_all(self, message, timeout, multiplexer=None, flush=False, **kwargs):
        """_send_and_receive's request to ALL: a copy on every live connection
        now, and (id, how many), 0 with none live, for its delivery error
        count."""
        if isinstance(message, MultiplexerMessage):
            mxmsg = whole(message, kwargs, self.instance_id, self.random)
        else:
            mxmsg = self.new_message(message=message, **kwargs)
        return (mxmsg.id, self._schedule_all(mxmsg.SerializeToString(), mxmsg.id, mxmsg.type, timeout))

    def flush_all(self, timeout=DEFAULT_TIMEOUT):
        """Run the loop until every message sent before the call has left its
        queue, written or dropped, what waits for room included; True when
        every one was written, False when one was dropped, which on_drop
        names, or `timeout` seconds passed first, or, whatever the timeout,
        at once with something held and nothing connected or on its way.
        The program's other threads go on meanwhile."""
        return super(Client, self).flush_all(timeout)

    def read_message(self, *args, **kwargs):
        """shortcut for receiving a message and ignoring connection used"""
        return self.receive_message(*args, **kwargs)[0]

    # helper functions
    def random(self):
        """returns random uint64"""
        return super(Client, self).random()

    message_defaults: ClassVar[dict[str, Any]] = {}

    def new_message(self, **kwargs):
        """creates new MultiplexerMessage with some predefined values"""
        if self.message_defaults:
            kwargs = dict(self.message_defaults, **kwargs)

        # defaults
        kwargs.setdefault("id", self.random())
        renamed_sender(kwargs)
        kwargs.setdefault("sender", self.instance_id)

        # special handling of some values
        if "message" in kwargs:
            if isinstance(kwargs["message"], str):
                kwargs["message"] = bytes(kwargs["message"], "utf-8")
            elif not isinstance(kwargs["message"], bytes):
                kwargs["message"] = kwargs["message"].SerializeToString()

        return make_message(MultiplexerMessage, **kwargs)
