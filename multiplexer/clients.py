"""The Python client API: Client, and MxClient to hold one.

This is the module clients import (docs/api_python.md). It builds on
multiplexer.mxclient, which wraps the C++ Client: everything about
connections, queues, timeouts and the query algorithm lives there; this
module adds the BACKEND_ERROR check and the exception classes. The backend
side, BaseMultiplexerServer, is in multiplexer.servers and is re-exported
here for the older import path.

Threading: a Client belongs to one thread. @log_call on most methods logs
entry and exit at DEBUG/CHATTERBOX; it checks should_log() first, so it costs
one C++ call when that level is off.
"""

import pickle
from typing import Any, Callable
from multiplexer import mxclient

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

    @log_call
    def query(self, *args, **kwargs):
        """Like mxclient.Client.query, but a BACKEND_ERROR reply raises BackendError."""
        mxmsg = super(BasicClient, self).query(*args, **kwargs)
        self.__check_backend_error(mxmsg)
        return mxmsg

    def __check_backend_error(self, mxmsg):
        """Raise BackendError if `mxmsg` is a BACKEND_ERROR reply."""
        if mxmsg.type == types.BACKEND_ERROR:
            raise BackendError(mxmsg.message)

    # The pickle convention: a payload that is a Python pickle, answered by a
    # MultiplexerServer (or any backend using send_pickle) with another.
    # Only between Python peers, and only where the network is trusted,
    # since unpickling runs code.
    def query_pickle(self, data: Any, type: int, timeout: float = mxclient.DEFAULT_TIMEOUT) -> Any:
        """query() with `data` pickled as the payload; returns the reply's payload unpickled."""
        return pickle.loads(self.query(pickle.dumps(data), type, timeout).message)

    def send_pickle(self, data: Any, **kwargs: Any) -> int:
        """send_message() with `data` pickled as the payload; the kwargs are send_message's."""
        return self.send_message(pickle.dumps(data), **kwargs)

    @log_call
    def send_and_receive(self, *args, **kwargs):
        """Like mxclient.Client.send_and_receive, ignoring REQUEST_RECEIVED notifications."""
        kwargs.setdefault(
            "ignore_function",
            lambda mxmsg, connwrap: mxmsg.type == types.REQUEST_RECEIVED,
        )
        return super(BasicClient, self).send_and_receive(*args, **kwargs)


class Client(BasicClient):
    """A client connected to every multiplexer in `addresses`, a list of (host, port) pairs.

    Clients are passive: the loop runs only inside calls, so the peer type
    must be marked is_passive in the rules file. See docs/api_python.md.
    """

    @log_call
    def __init__(self, addresses, type=None):
        """`addresses`: (host, port) pairs of every multiplexer; `type`: a peers.* constant."""
        if type is None:
            raise ValueError
        super(Client, self).__init__(type)
        for host, port in addresses:
            self.connect((host, port))


class MxClient:
    """One synchronous Client for one peer type, created on first use.

    A place to keep the client without a module-level global, and a way
    to read the addresses lazily: `addresses` is a list of (host, port)
    pairs or a zero-argument callable returning one, so settings can be
    read at first use rather than at import. The Client does the rest
    itself: every call notices a connection the multiplexer closed, uses
    another one or waits for the reconnect, so nothing is checked or
    rebuilt here.

        MX = MxClient(peers.WEBSITE, lambda: settings.MULTIPLEXER_ADDRESSES)
        reply = MX.get().query(payload, type=types.SEARCH_REQUEST)

    The Client belongs to one thread, and so does the holder: give each
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

    def get(self) -> Client:
        """The Client, connected to every address on first use, the same one afterwards."""
        if self._client is None:
            self._client = Client(self.addresses(), type=self.peer_type)
        return self._client

    def shutdown(self) -> None:
        """Close the held Client, if any; the next get() makes a new one."""
        if self._client is not None:
            self._client.shutdown()
            self._client = None
