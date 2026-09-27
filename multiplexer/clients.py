"""The synchronous Python API: SyncClient, and MxClient to hold one.

A program built on SyncClient, or on an MxClient holding one, imports this
module, whatever its role (docs/api_python.md). SyncClient was named Client
up to 2.3.1, and Client is still the same class. It builds on
multiplexer.mxclient, which wraps the C++ Client: everything about
connections, queues, timeouts and the query algorithm lives there; this
module adds the BACKEND_ERROR check and the exception classes.
BaseMultiplexerServer, a base class for backends, is in multiplexer.servers.

Threading: a SyncClient belongs to one thread. @log_call on most methods logs
entry and exit at DEBUG/CHATTERBOX; it checks should_log() first, so it costs
one C++ call when that level is off.
"""

import pickle
from typing import Any, Callable, Literal, TypeVar, overload

from multiplexer import mxclient
from multiplexer.Multiplexer_pb2 import MultiplexerMessage
from multiplexer.mxclient import CLOSE_FLUSH_SECONDS, ConnectionWrapper, DropReason, Lane

from multiplexer.mxlog import *
from multiplexer.multiplexer_constants import types


# Exceptions raised by this module. The connection-level ones (NotConnected,
# OperationTimedOut, OperationFailed) come from the C++ side via mxclient.
class MultiplexerRelatedException(Exception):
    """Base of every exception this module raises."""

    pass


# server side exceptions


# client side exceptions
class MultiplexerClientException(MultiplexerRelatedException):
    """Raised on the client side."""

    pass


class BackendError(MultiplexerClientException):
    """The backend answered a request with BACKEND_ERROR; the argument is its message."""

    pass


class BasicClient(mxclient.Client):
    """mxclient.Client plus the backend conventions: a reply of type
    BACKEND_ERROR raises BackendError, and REQUEST_RECEIVED notifications are
    ignored while waiting for a reply."""

    @log_call
    def receive_message(self, *args, **kwargs):
        """Like mxclient.Client.receive_message, but a BACKEND_ERROR reply raises BackendError."""
        mxmsg, connwrap = super(BasicClient, self).receive_message(*args, **kwargs)
        self.__check_backend_error(mxmsg)
        return mxmsg, connwrap

    @overload
    def query(
        self,
        message: Any,
        type: int,
        timeout: float = ...,
        to: int = ...,
        probe: int = ...,
        multiplexer: int | Lane | ConnectionWrapper = ...,
        with_connection: Literal[False] = ...,
    ) -> MultiplexerMessage: ...

    @overload
    def query(
        self,
        message: Any,
        type: int,
        timeout: float = ...,
        to: int = ...,
        probe: int = ...,
        multiplexer: int | Lane | ConnectionWrapper = ...,
        *,
        with_connection: Literal[True],
    ) -> tuple[MultiplexerMessage, ConnectionWrapper]: ...

    @log_call
    def query(
        self,
        message: Any,
        type: int,
        timeout: float = mxclient.DEFAULT_TIMEOUT,
        to: int = 0,
        probe: int = types.BACKEND_FOR_PACKET_SEARCH,
        multiplexer: int | Lane | ConnectionWrapper = mxclient.Client.ONE,
        with_connection: bool = False,
    ) -> MultiplexerMessage | tuple[MultiplexerMessage, ConnectionWrapper]:
        """Like mxclient.Client.query, but a BACKEND_ERROR reply raises BackendError."""
        if with_connection:
            reply, connection = super(BasicClient, self).query(
                message, type, timeout, to, probe, multiplexer, with_connection=True
            )
            self.__check_backend_error(reply)
            return reply, connection
        reply = super(BasicClient, self).query(message, type, timeout, to, probe, multiplexer)
        self.__check_backend_error(reply)
        return reply

    def __check_backend_error(self, mxmsg):
        """Raise BackendError if `mxmsg` is a BACKEND_ERROR reply."""
        if mxmsg.type == types.BACKEND_ERROR:
            raise BackendError(mxmsg.message)

    # The pickle convention: a payload that is a Python pickle, answered by a
    # MultiplexerServer (or any backend using send_pickle) with another.
    # Only between Python peers, and only where the network is trusted,
    # since unpickling runs code.
    def query_pickle(self, data: Any, type: int, timeout: float = mxclient.DEFAULT_TIMEOUT, **kwargs: Any) -> Any:
        """query() with `data` pickled as the payload; returns the reply's
        payload unpickled. The kwargs are query()'s: `to`, `probe`,
        `multiplexer`."""
        return pickle.loads(self.query(pickle.dumps(data), type, timeout, **kwargs).message)

    def send_pickle(self, data: Any, **kwargs: Any) -> int:
        """send_message() with `data` pickled as the payload; the kwargs are send_message's."""
        return self.send_message(pickle.dumps(data), **kwargs)

    @log_call
    def send_and_receive(self, *args, **kwargs):
        """Like mxclient.Client.send_and_receive, skipping REQUEST_RECEIVED
        notifications as well as the `ignore_types` given."""
        kwargs["ignore_types"] = tuple(kwargs.get("ignore_types", ())) + (types.REQUEST_RECEIVED,)
        return super(BasicClient, self).send_and_receive(*args, **kwargs)


_SyncClientT = TypeVar("_SyncClientT", bound="SyncClient")


class SyncClient(BasicClient):
    """A peer's connections to every multiplexer in `addresses`, a list of
    (host, port) pairs, used from one thread; named Client up to 2.3.1.

    It is passive: the loop runs only inside calls, so its peer type must
    be marked is_passive in the rules file, which no other class needs.
    See docs/api_python.md.
    """

    @log_call
    def __init__(self, addresses, type=None, *, on_drop: Callable[[int, DropReason], None] | None = None):
        """`addresses`: (host, port) pairs of every multiplexer; `type`: a
        peers.* constant. `on_drop(message_id, reason)` hears of every
        message the client gives up on, each copy of one sent to ALL, with
        a DropReason, inside whichever call runs the loop when it happens;
        `dropped` counts them. Held until shutdown(), so that a callback
        referring to the client keeps it alive until then."""
        if type is None:
            raise ValueError
        super(SyncClient, self).__init__(type)
        if on_drop is not None:
            self._set_drop_observer(on_drop)
        for host, port in addresses:
            self.connect((host, port))

    def __enter__(self: _SyncClientT) -> _SyncClientT:
        """The client, for a `with` block, at whose end it is shut down
        however the block ended."""
        return self

    def __exit__(self, *exc: object) -> None:
        """shutdown(), at the end of a `with` block."""
        self.shutdown()


# The name up to 2.3.1, kept as the same class object: isinstance,
# subclassing and Client.ONE/ALL work under either name, at no cost.
Client = SyncClient


class MxClient:
    """One SyncClient for one peer type, created on first use.

    A place to keep one without a module-level global, and a way to read
    the addresses lazily: `addresses` is a list of (host, port) pairs or a
    zero-argument callable returning one, so settings can be read at first
    use rather than at import. The SyncClient does the rest itself: every
    call notices a connection the multiplexer closed, uses another one or
    waits for the reconnect, so nothing is checked or rebuilt here.

        MX = MxClient(peers.WEB, lambda: settings.MULTIPLEXER_ADDRESSES)
        reply = MX.get().query(payload, type=types.SEARCH_REQUEST)

    The SyncClient belongs to one thread, and so does the holder: give each
    thread its own MxClient, or use
    multiplexer.threaded_client.ThreadedClient, which is made for sharing.
    """

    def __init__(self, peer_type: int, addresses: list[tuple[str, int]] | Callable[[], list[tuple[str, int]]]):
        """`peer_type`: a peers.* constant; `addresses`: (host, port) pairs, or a callable returning them."""
        self.peer_type = peer_type
        self._addresses = addresses
        self._client = None

    def addresses(self) -> list[tuple[str, int]]:
        """The current list of (host, port) pairs."""
        return list(self._addresses() if callable(self._addresses) else self._addresses)

    def get(self) -> SyncClient:
        """The SyncClient, connected to every address on first use, the same one afterwards."""
        if self._client is None:
            self._client = SyncClient(self.addresses(), type=self.peer_type)
        return self._client

    def shutdown(self, timeout: float = CLOSE_FLUSH_SECONDS) -> None:
        """Close the held SyncClient, if any, as its shutdown(timeout) does;
        the next get() makes a new one."""
        if self._client is not None:
            self._client.shutdown(timeout)
            self._client = None
