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

from multiplexer._native import *
import multiplexer._native as _mxclient
import atexit
import socket
import time
from functools import wraps
import google.protobuf.message

from multiplexer.protocolbuffers import *
from multiplexer.multiplexer_constants import types
from multiplexer.mxlog import log, WARNING, HIGHVERBOSITY
from multiplexer.Multiplexer_pb2 import BackendForPacketSearch


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


class TimeoutTicker(object):
    """One deadline shared by the steps of a call: calling it gives the seconds
    left, so each step's C++ timeout is what remains of the whole call's.
    A negative timeout never expires."""

    __slots__ = ["_expires_at", "_timeout"]

    def __init__(self, timeout):
        """`timeout` in seconds from now; negative never expires."""
        object.__init__(self)
        self._timeout = timeout
        self._expires_at = time.time() + timeout

    def __call__(self):
        """Seconds left, never below 0; the negative timeout itself if it never expires."""
        if self._timeout >= 0:
            return max(self._expires_at - time.time(), 0)
        else:
            return self._timeout

    def permit(self) -> bool:
        """Whether there is time left."""
        return self() > 0 if self._timeout >= 0 else True


class Client(_mxclient.Client):
    """See the module docstring. `multiplexer=` arguments take ONE (some
    connection, round robin), ALL (every connection) or a ConnectionWrapper."""

    DEFAULT_TIMEOUT = _mxclient.DEFAULT_TIMEOUT
    ONE = 0
    ALL = 1

    def __init__(self, client_type):
        """initialize a client with given client (peer) type"""
        self._client_type = client_type
        self.__instance_id = None
        super(Client, self).__init__(client_type)

    def __get_instance_id(self):
        """The instance id, read from the C++ side once."""
        if self.__instance_id is None:
            self.__instance_id = super(Client, self)._get_instance_id()
        return self.__instance_id

    instance_id = property(__get_instance_id)

    def async_connect(self, endpoint):
        """
        asynchronous connect
        endpoint is e.g. ("localhost", 1980)
        return ConnectionWrapper
        """
        ip4 = socket.gethostbyname(endpoint[0])
        return super(Client, self).async_connect(ip4, endpoint[1])

    def connect(self, endpoint, timeout=DEFAULT_TIMEOUT):
        """
        synchronous connect
        endpoint is e.g. ("localhost", 1980)
        return ConnectionWrapper
        """
        ip4 = socket.gethostbyname(endpoint[0])
        return super(Client, self).connect(ip4, endpoint[1], timeout)

    def wait_for_connection(self, connwrap, timeout=DEFAULT_TIMEOUT):
        """wait for connection initiated with async_connect"""
        return super(Client, self).wait_for_connection(connwrap, timeout)

    def __receive_message(self, timeout=-1):
        """
        blocking read from all the sockets (or from incoming message queue)
        returns (MultiplexerMessage, ConnectionWrapper)
        """
        next = super(Client, self).read_message(timeout)
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
        """overide in subclass if you want to get hold on every message
        dropped"""
        log(
            WARNING,
            HIGHVERBOSITY,
            text="dropping message %r"
            % dict(
                id=mxmsg.id,
                type=mxmsg.type,
                to=mxmsg.to,
                from_=mxmsg.from_,
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
        return self.send_message(multiplexer=Client.ALL, *args, **kwargs)

    def query(self, message, type, timeout=DEFAULT_TIMEOUT):
        """Send a request and return its reply, a MultiplexerMessage.

        The request goes through one connection. If it comes back as a
        DELIVERY_ERROR, or no reply arrives within `timeout` seconds, every
        connection is asked (BACKEND_FOR_PACKET_SEARCH) for a backend that
        handles `type`; the first one to answer gets the request again,
        addressed directly, with a fresh `timeout`. Raises OperationTimedOut
        when a stage runs out of time, OperationFailed when no backend can be
        found, NotConnected when there is no live connection.
        """
        assert not isinstance(message, MultiplexerMessage)
        query = self.new_message(type=type, message=message)
        # Stage 1: the request through one connection. A reply ends it here.
        try:
            response, connwrap = self.send_and_receive(
                query,
                multiplexer=Client.ONE,
                timeout=timeout,
                ignore_types=(types.REQUEST_RECEIVED,),
            )
            if response.type != types.DELIVERY_ERROR:
                return response
        except OperationTimedOut:
            pass

        # Stage 2: ask every multiplexer who has a backend for this type. The
        # search is routed by the request type's own rule with whom forced to
        # ALL, so every live backend answers with a PING. One DELIVERY_ERROR
        # per connection means nobody has one.
        search = BackendForPacketSearch()
        search.packet_type = type
        mxmsg = self.new_message(message=search, type=types.BACKEND_FOR_PACKET_SEARCH)
        response, connwrap = self.send_and_receive(
            mxmsg,
            accept_ids=[query.id],
            multiplexer=Client.ALL,
            timeout=timeout,
            handle_delivery_errors=True,
            ignore_types=(types.REQUEST_RECEIVED,),
        )

        if response.type == types.DELIVERY_ERROR:
            # Every multiplexer reported no backend of this type, so the one
            # that took the request is gone too: nothing can answer any more.
            raise OperationFailed
        if response.references == query.id:
            return response

        self._check_type(response, types.PING)

        # Stage 3: the request again, to the backend that answered first, by
        # instance id and through the connection its PING came on. A late
        # reply to the original request is still accepted; late PINGs from
        # other backends are ignored.
        direct_query = self.new_message(to=response.from_, message=message, type=type)
        assert direct_query.type == type
        assert type
        response, connwrap = self.send_and_receive(
            direct_query,
            accept_ids=[query.id],
            ignore_ids=[mxmsg.id],
            multiplexer=connwrap,
            timeout=timeout,
            ignore_types=(types.REQUEST_RECEIVED,),
        )

        if response.type == types.DELIVERY_ERROR:
            raise OperationFailed

        return response

    def send_and_receive(
        self,
        message,
        accept_ids=[],
        ignore_ids=[],
        timeout=DEFAULT_TIMEOUT,
        handle_delivery_errors=False,
        ignore_types=(),
        **kwargs,
    ):
        """Send `message` once and wait for a reply that references it.

        Returns (reply, connection). No search and no retry: raises
        OperationTimedOut after `timeout` seconds. `accept_ids` are further
        ids a reply may reference, `ignore_ids` and `ignore_types` are
        skipped silently, other unexpected messages go to handle_drop().
        """
        timeout_ticker = TimeoutTicker(timeout)
        multiplexer = kwargs.get("multiplexer", Client.ONE)
        if multiplexer is not Client.ALL:
            return self.__send_and_receive_one(message, accept_ids, ignore_ids, ignore_types, timeout_ticker, **kwargs)

        id, tracker = self.__send_message(message, timeout=timeout_ticker(), **kwargs)
        assert isinstance(tracker, int)
        if tracker == 0:
            # No connection at all: let the reconnect timers fire, then once more.
            if not self.wait_for_any_connection(timeout_ticker()):
                raise NotConnected()
            id, tracker = self.__send_message(message, timeout=timeout_ticker(), **kwargs)

        accept_ids = [id] + accept_ids
        while timeout_ticker.permit():
            mxmsg, connwrap = self.receive(
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

    def __send_and_receive_one(self, message, accept_ids, ignore_ids, ignore_types, timeout_ticker, **kwargs):
        """The single-connection case of send_and_receive, the same as
        Client::_send_and_receive_one in client.h: if the connection used dies
        before the reply arrives, or none is live, keep running the loop so
        the reconnect timers fire, and send again with a fresh id."""
        preferred = kwargs.pop("multiplexer", None)
        if preferred is Client.ONE:
            preferred = None
        kwargs.pop("flush", None)
        mxmsg = message if isinstance(message, MultiplexerMessage) else self.new_message(message=message, **kwargs)
        while True:
            _, used = self.__send_one(mxmsg, timeout_ticker, preferred)
            preferred = None
            accept_ids = [mxmsg.id] + accept_ids
            while timeout_ticker.permit():
                got = self.read_message_watching(timeout_ticker(), used)
                if got is None:
                    break  # the connection died: send again
                mxmsg_in, connwrap = self.__parse_incoming(got)
                if mxmsg_in.type in ignore_types:
                    continue
                if mxmsg_in.references in accept_ids:
                    return mxmsg_in, connwrap
                if mxmsg_in.references not in ignore_ids:
                    self.handle_drop(mxmsg_in, connwrap)
            if not timeout_ticker.permit():
                raise OperationTimedOut()
            log(
                WARNING,
                MEDIUMVERBOSITY,
                text="connection lost while waiting for a reply to %d; sending again" % mxmsg.id,
            )
            mxmsg.id = self.random()

    def __send_one(self, mxmsg, timeout_ticker, preferred=None):
        """Write `mxmsg` to one connection and return (tracker, connection),
        waiting for a connection or for the write to complete up to the
        deadline. A connection that dies under the write is replaced by
        another, or by the same one once reconnected, so a multiplexer
        restart between two calls costs the reconnect delay, not the message.
        Raises NotConnected when no connection exists by the deadline."""
        raw = mxmsg.SerializeToString()
        while True:
            tracker = used = None
            if preferred:
                tracker = self.schedule_one(raw, preferred, 0.0) if preferred else None
                used = preferred
                preferred = None
            if not tracker:
                tracker, used = self.schedule_one_used(raw)
            if tracker:
                self.flush(tracker, timeout=timeout_ticker())
                if tracker.is_sent():
                    return tracker, used
            if not timeout_ticker.permit():
                raise OperationTimedOut()
            if not self.wait_for_any_connection(timeout_ticker()):
                raise NotConnected()

    def __parse_incoming(self, got):
        """A (bytes, connection) pair from the C++ side as (MultiplexerMessage, connection)."""
        raw, connwrap = got
        mxmsg = parse_message(MultiplexerMessage, raw)
        return mxmsg, connwrap

    def receive(
        self,
        accept_ids,
        ignore_ids=[],
        ignore_types=(),
        timeout=DEFAULT_TIMEOUT,
        timeout_ticker=None,
    ):
        """
        like send_and_receive but without sending anything
        """
        if timeout_ticker is None:
            timeout_ticker = TimeoutTicker(timeout)

        while timeout_ticker.permit():
            mxmsg, connwrap = self.__receive_message(timeout=timeout_ticker())
            if mxmsg.type in ignore_types:
                continue

            if mxmsg.references in accept_ids:
                return (mxmsg, connwrap)

            if mxmsg.references not in ignore_ids:
                # unexpected message received
                log(
                    WARNING,
                    HIGHVERBOSITY,
                    text="message (id=%d, type=%d, from=%d, references=%d) "
                    "while waiting for reply for %r"
                    % (mxmsg.id, mxmsg.type, mxmsg.from_, mxmsg.references, accept_ids),
                )
                self.handle_drop(mxmsg, connwrap)

        raise OperationTimedOut()

    def send_message(self, message, **kwargs) -> int:
        """Queue a message on one or more connections and return its id.

        `message` is a MultiplexerMessage, or a payload (bytes, str, or a
        protocol buffer message) wrapped into a new one built from the
        remaining kwargs (`type`, `to`, `references`, `workflow`, ...).
        Keyword-only options: `multiplexer` is Client.ONE (default), Client.ALL,
        or a ConnectionWrapper; `flush` waits until the message reached the
        socket (every socket, for ALL), within `timeout` seconds, resending
        through another connection if the first dies under it; `embed`
        wraps `message` without looking at its type. Raises NotConnected
        when no connection could take the message, OperationTimedOut when a
        flush ran out of time. For the tracker of a queued message use
        schedule_one() or schedule_all() directly.
        """
        mxmsg_id, tracker = self.__send_message(message, **kwargs)
        if (tracker == 0) if isinstance(tracker, int) else not tracker:
            raise NotConnected()
        return mxmsg_id

    def __send_message(self, message, embed=False, multiplexer=ONE, flush=False, timeout=DEFAULT_TIMEOUT, **kwargs):
        """The body of send_message(): wrap, serialize, queue on the chosen
        connection(s), optionally flush. Choosing a connection first runs
        every ready handler of the loop, so a connection the multiplexer
        closed while this client was idle is retired, never written into.
        With `flush` and one connection the message is also sent again if
        that connection dies under it, and the call waits for a reconnect
        within `timeout`; without `flush` the message is only queued."""
        timeout_ticker = TimeoutTicker(timeout)

        if embed or not isinstance(message, MultiplexerMessage):
            mxmsg = self.new_message(message=message, **kwargs)
        else:
            mxmsg = message

        # Serialized once here; the C++ side wraps the bytes in a frame and
        # queues that same frame on every connection chosen.
        raw = mxmsg.SerializeToString()

        if multiplexer is Client.ALL:
            count = self.schedule_all(raw)
            if flush and count and not self.flush_all(timeout_ticker()):
                raise OperationTimedOut()
            return (mxmsg.id, count)
        if multiplexer is not Client.ONE and not isinstance(multiplexer, ConnectionWrapper):
            raise NotImplementedError("selecting multiplexer with %r is not supported" % multiplexer)
        preferred = multiplexer if isinstance(multiplexer, ConnectionWrapper) else None

        if flush:
            tracker, _ = self.__send_one(mxmsg, timeout_ticker, preferred)
            return (mxmsg.id, tracker)
        if preferred is not None:
            return (mxmsg.id, self.schedule_one(raw, preferred, timeout_ticker()))
        return (mxmsg.id, self.schedule_one(raw))

    def flush(self, tracker, timeout=DEFAULT_TIMEOUT):
        """ensure that message tracked by tracker is either sent or dropped"""
        super(Client, self).flush(tracker, timeout)

    def flush_all(self, timeout=DEFAULT_TIMEOUT):
        """try to empty all outgoing messages buffers within timeout seconds"""
        return super(Client, self).flush_all(timeout)

    def read_message(self, *args, **kwargs):
        """shortcut for receiving a message and ignoring connection used"""
        return self.receive_message(*args, **kwargs)[0]

    # helper functions
    def random(self):
        """returns random uint64"""
        return super(Client, self).random()

    message_defaults = {}

    def new_message(self, **kwargs):
        """creates new MultiplexerMessage with some predefined values"""
        if self.message_defaults:
            kwargs = dict(self.message_defaults, **kwargs)

        # defaults
        kwargs.setdefault("id", self.random())
        kwargs.setdefault("from", self.instance_id)

        # special handling of some values
        if "message" in kwargs:
            if isinstance(kwargs["message"], str):
                kwargs["message"] = bytes(kwargs["message"], "utf-8")
            elif not isinstance(kwargs["message"], bytes):
                kwargs["message"] = kwargs["message"].SerializeToString()

        return make_message(MultiplexerMessage, **kwargs)
