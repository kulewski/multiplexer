"""The Python backend API: BaseMultiplexerServer, which a backend subclasses.

A backend hands control to serve_forever(): the loop reads one message at a
time, answers the protocol's own messages itself, and calls handle_message()
with the rest; while a message is being handled, send_message() defaults to
replying to it. The C++ BaseMultiplexerServer mirrors this class, including
what happens when a handler raises. docs/api_python.md is the user's view.

Threading: a backend runs on the thread that calls serve_forever().
"""

import faulthandler
import pickle
import sys
import time
import traceback

from multiplexer.clients import BackendError, BasicClient, MultiplexerRelatedException  # noqa: F401
from multiplexer.mxlog import *
from multiplexer.multiplexer_constants import types
from multiplexer.mxclient import OperationTimedOut, parse_message


def format_exception(exc, trace=None):
    """Create new BackendError containing information about `exc`.

    :Parameters:
         - `exc`: exception caught
         - `trace`: traceback; if you don't specify traceback, traceback of
           currently handled exception will be used
    """
    try:
        if trace is None:
            assert exc is sys.exc_info()[1], "Traceback does not match exception passed."
            trace = sys.exc_info()[2]
        return "".join(traceback.format_exception(type(exc), exc, trace))
    finally:
        del trace


class MultiplexerPeer(object):
    """Anything that owns a connection to the multiplexers: the base of
    BaseMultiplexerServer. `self.conn` is the BasicClient."""

    # the peer type, if a subclass wants to fix it instead of passing `type`
    multiplexer_client_type = None

    @log_call
    def __init__(self, addresses, type=None):
        """Connect to every multiplexer in `addresses`, a list of (host, port) pairs.

        `type` is a peers.* constant from the generated multiplexer_constants;
        subclasses may set `multiplexer_client_type` instead of passing it.
        """

        if type is None:
            type = self.multiplexer_client_type
            if type is None:
                raise ValueError("no type provided and " "self.multiplexer_client_type is not set")
        self.type = type
        self.conn = BasicClient(self.type)
        for host, port in addresses:
            self.conn.connect((host, port))


class BaseMultiplexerServer(MultiplexerPeer):
    """Base class for a backend: subclass, implement handle_message(), call
    serve_forever().

    The loop reads one message at a time; the protocol's own messages
    (a client's search for a backend, a PING) are answered here, the rest
    go to handle_message(). While a message is being handled, send_message()
    defaults to replying to it. After every iteration, whether a message
    arrived or the poll timed out, periodic_task() runs: that is the place
    for anything the backend must do on its own schedule, including deciding
    to leave. The C++ BaseMultiplexerServer mirrors this class.

    The library installs no signal handlers. A Python handler runs only
    when the main thread returns from C++, and any C++ library in the same
    process may replace it, so a backend that must notice a shutdown
    request checks for it in periodic_task(): a file, a flag, a queue.
    """

    # time when the instance was initialized
    _start_time = None

    @log_call
    def __init__(self, addresses, type=None):
        """Connect to every multiplexer in `addresses` as a backend of peer type `type`."""
        super(BaseMultiplexerServer, self).__init__(addresses, type)
        self.working = True
        self.last_mxmsg = None
        self._start_time = time.time()
        self._draining_since = None
        self._drain_seconds = 0.0

    start_time = property(lambda self: self._start_time, doc="Time when the instance was instantiated")

    # Draining: a backend about to exit stops answering the search clients
    # use to find a backend, so that no retried request is sent to it, while
    # it keeps serving what the multiplexer still routes to it. Once
    # drained() it exits; the requests it was handling are finished, the
    # ones that reach it later fail over through the search.

    @property
    def draining(self) -> bool:
        """Whether start_draining() was called."""
        return self._draining_since is not None

    def start_draining(self) -> None:
        """Stop answering backend searches; keep serving what arrives until drained()."""
        if self._draining_since is None:
            self._draining_since = time.time()

    def drained(self) -> bool:
        """Whether the drain is over and serve_forever() may return; checked
        after every iteration while draining. Default: the `drain_seconds`
        given to serve_forever() have passed since start_draining(). Override
        to wait for your own condition, for example
        `super().drained() and not self.in_flight`."""
        return self.draining and time.time() - self._draining_since >= self._drain_seconds

    def should_respond_to_backend_for_packet_search(self) -> bool:
        """Whether to answer a client's search for a backend; override for
        your own condition. False while draining."""
        return not self.draining

    def stop(self) -> None:
        """Ask serve_forever() to return, from any thread: it notices within
        one poll, closes the connections and returns. For programs with a
        shutdown sequence of their own; a request to leave that should drain
        first calls start_draining() from periodic_task() instead."""
        self.working = False

    def periodic_task(self) -> None:
        """Called after every iteration of serve_forever(), message or not,
        so at least once per `poll` seconds. Override for work on your own
        schedule and for noticing a request to leave: call start_draining()
        or clear `working`. Does nothing by default."""

    def on_handler_exception(self, exc: Exception) -> bool:
        """Called when handle_message() raised, after BACKEND_ERROR went to
        the requester. Return True to keep serving (the default); return
        False and the exception propagates out of serve_forever()."""
        return True

    def serve_forever(
        self,
        poll: float = 1.0,
        drain_seconds: float = 0.0,
        stall_seconds: float | None = None,
        stall_file=None,
    ) -> None:
        """Run the loop until `working` is cleared or a drain is over, then
        close the connections and return.

        Each iteration waits up to `poll` seconds for a message, handles it
        if one came, and calls periodic_task(). `drain_seconds` is how long
        to keep serving after start_draining(), unless drained() is
        overridden. With `stall_seconds`, an iteration that takes longer
        dumps every thread's stack (faulthandler) to `stall_file` or stderr,
        for finding a handler that hangs. The calling thread becomes the
        backend's thread: a backend may be built on one thread and served
        from another, but from here on only this thread may touch it.
        """
        self.conn.bind_to_current_thread()
        self._drain_seconds = drain_seconds
        stall_file = stall_file or sys.stderr
        try:
            while self.working:
                if self.draining and self.drained():
                    break
                if stall_seconds is not None:
                    faulthandler.dump_traceback_later(stall_seconds, file=stall_file)
                try:
                    self.loop_iter(timeout=poll)
                except OperationTimedOut:
                    pass
                finally:
                    self.periodic_task()
                    if stall_seconds is not None:
                        faulthandler.cancel_dump_traceback_later()
        finally:
            self.close()

    def loop_iter(self, *args, **kwargs):
        """Wait for one message (up to `timeout` seconds, forever by default) and handle it. Raises OperationTimedOut when the time passes."""
        self.last_mxmsg, self.last_connwrap = self.conn.receive_message(*args, **kwargs)
        self.__handle_message()

    @log_call
    def __handle_internal_message(self):
        # A client searching for a backend gets a PING referencing its search:
        # that is how it learns this backend is alive and where to repeat the
        # request. A PING without references is an echo request.
        """Answer the protocol's own messages: a PING referencing a client's search for a backend, an echo of a PING without references."""
        mxmsg = self.last_mxmsg
        if mxmsg.type == types.BACKEND_FOR_PACKET_SEARCH:
            if self.should_respond_to_backend_for_packet_search():
                self.send_message(message="", embed=True, flush=True, type=types.PING)
            else:
                self.no_response()  # draining: let the client find another backend

        elif mxmsg.type == types.PING:
            if not mxmsg.references:
                assert mxmsg.id
                self.send_message(message=mxmsg.message, embed=True, flush=True, type=types.PING)
            else:
                self.no_response()

        else:
            log(
                ERROR,
                LOWVERBOSITY,
                text="received unknown meta-packet",
                data=PickleData(mxmsg.type),
            )

    @log_call
    def __handle_message(self):
        """Dispatch the last received message to __handle_internal_message() or handle_message(), then apply the exception rules."""
        try:
            self._has_sent_response = False
            if self.last_mxmsg.type <= types.MAX_MULTIPLEXER_META_PACKET:
                # internal messages
                self.__handle_internal_message()
                if not self._has_sent_response:
                    log(
                        WARNING,
                        LOWVERBOSITY,
                        text="__handle_internal_message() finished w/o " "exception and w/o any response",
                    )
            else:
                # the rest
                self.handle_message(self.last_mxmsg)
                if not self._has_sent_response:
                    log(
                        WARNING,
                        LOWVERBOSITY,
                        text="handle_message() " "finished w/o exception and w/o any response",
                    )

        except Exception as e:
            # Same as the C++ backend: tell the requester instead of leaving it
            # to time out, then ask on_handler_exception() whether to go on.
            traceback.print_exc()
            log(ERROR, LOWVERBOSITY, text=lambda: "exception in handle_message: %r" % e)
            if not self._has_sent_response:
                self.report_error(message=str(e))
            if not self.on_handler_exception(e):
                raise

    @log_call
    def handle_message(self, mxmsg):
        """Override: called with every non-meta MultiplexerMessage received.

        Reply with send_message(); for an event call no_response() instead, so
        that the missing reply is not logged as a warning.
        """
        raise NotImplementedError()

    def parse_message(self, type, mxmsg=None):
        """parse mxmsg.message with new Protobuf message of type `type'"""
        return parse_message(type, self.last_mxmsg.message if mxmsg is None else mxmsg.message)

    # The pickle convention, see the clients: a payload that is a Python
    # pickle. Only between Python peers on a trusted network, since
    # unpickling runs code.
    def parse_pickle(self, mxmsg=None):
        """The payload of `mxmsg` (the current message by default) unpickled."""
        return pickle.loads(self.last_mxmsg.message if mxmsg is None else mxmsg.message)

    @log_call
    def send_pickle(self, data, type=types.PICKLE_RESPONSE, **kwargs):
        """send_message() with `data` pickled as the payload: by default a
        PICKLE_RESPONSE reply to the current request, flushed."""
        kwargs.setdefault("flush", True)
        return self.send_message(message=pickle.dumps(data), type=type, **kwargs)

    @log_call
    def notify_start(self):
        """Tell the requester at once that its request is being worked on (REQUEST_RECEIVED); call it first in handle_message()."""
        assert not self._has_sent_response, (
            "If you use notify_start(), " "place it as a first function in your handle_message() code"
        )
        self.send_message(
            message="",
            type=types.REQUEST_RECEIVED,
            # references, workflow -- set by send_message
        )
        self._has_sent_response = False

    @log_call
    def send_message(self, **kwargs):
        """Send a message; while handling a request it is a reply to it by default.

        Defaults, each overridable through kwargs: `to` the requester's
        instance id, `references` the request's id, `workflow` the request's
        workflow, `multiplexer` the connection the request arrived on. Other
        kwargs are as for mxclient.Client.send_message.
        """
        if self.last_mxmsg is not None:
            self._has_sent_response = True
            kwargs.setdefault("multiplexer", self.last_connwrap)
            kwargs.setdefault("references", self.last_mxmsg.id)
            kwargs.setdefault("workflow", self.last_mxmsg.workflow)
            kwargs.setdefault("to", self.last_mxmsg.from_)
        return self.conn.send_message(**kwargs)

    @log_call
    def send_backend_error(self, exc, trace=None):
        """Answer the current request with BACKEND_ERROR carrying the formatted exception."""
        assert self.last_mxmsg is not None
        self.report_error(message=format_exception(exc, trace=trace))

    @log_call
    def no_response(self):
        """Declare that the current message needs no reply, so that its absence is not logged."""
        self._has_sent_response = True

    @log_call
    def report_error(self, message="", type=types.BACKEND_ERROR, flush=True, **kwargs):
        """Answer the current request with BACKEND_ERROR (or `type`) carrying `message`."""
        assert self.last_mxmsg is not None
        self.send_message(message=message, type=type, flush=flush, **kwargs)

    @log_call
    def close(self):
        """Close every connection; the server cannot be used afterwards. Safe to call twice."""
        if self.conn is not None:
            self.conn.shutdown()
            self.conn = None


class MultiplexerServer(BaseMultiplexerServer):
    """A backend whose payloads are Python pickles: implement process_pickle()
    and return the answer. The oldest part of the library; only useful when
    both ends are Python, and pickles from the network must be trusted."""

    @log_call
    def __init__(self, addresses, type=None):
        """Connect as a backend of peer type `type`, a peers.* constant."""
        assert isinstance(type, int)
        super(MultiplexerServer, self).__init__(addresses, type)

    @log_call
    def process_pickle(self, data):
        """Override: called with the unpickled payload of every request; the
        return value, pickled, is the reply."""
        raise NotImplementedError

    @log_call
    def handle_message(self, mxmsg):
        """Unpickle the payload, hand it to process_pickle(), reply with its return value pickled."""
        try:
            data = self.parse_pickle(mxmsg)
        except (pickle.UnpicklingError, EOFError):
            print(
                "Failed to pickle.loads(%r) in #%d" % (mxmsg.message, mxmsg.id),
                file=sys.stderr,
            )
            raise
        self.send_pickle(self.process_pickle(data))
