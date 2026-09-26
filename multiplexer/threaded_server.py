"""BaseThreadedMultiplexerServer: a backend whose handlers run on worker
threads behind a heartbeating io thread.

BaseMultiplexerServer (servers.py) runs the loop and the handler on one
thread, so while handle_message() runs nothing heartbeats, and a backend
built on it whose one request takes longer than the multiplexer's drop
interval (docs/semantics.md) is dropped mid-work. This class puts the io on a
ThreadedClient's thread, which heartbeats, reconnects, answers pings and
the search clients use to find a backend, and hands every other message
to a bounded queue that `workers` threads take from. With workers=1
handling is serial, in arrival order, as on BaseMultiplexerServer, and the
peer stays registered under a request of any length.

The handler is handle_message(request): a Request carries the message and
everything needed to answer it, and the reply goes through the threaded
client from whichever thread calls it, so a handler may hand the request
to another thread and answer later. A request that is dropped without a
reply or no_response() is logged. docs/api_python.md has the user's view;
the C++ mirror is multiplexer/backend/base_threaded_multiplexer_server.h.
"""

import collections
import pickle
import sys
import threading
import time
import traceback
from typing import Any, Callable, TypeVar

from multiplexer.Multiplexer_pb2 import DeliveryError, MultiplexerMessage, Routing
from multiplexer.multiplexer_constants import types
from multiplexer.mxclient import ConnectionWrapper, parse_message
from multiplexer.mxlog import DEBUG, ERROR, HIGHVERBOSITY, LOWVERBOSITY, WARNING, log
from multiplexer.servers import format_exception, nothing_more_arrives
from multiplexer.threaded_client import DEFAULT_TIMEOUT, Endpoint, ThreadedClient


class _DropLines:
    """The lines about requests a full queue dropped, at most about two a
    second however many there are, as the C++ library's LogSummary says
    its own: the first of a burst at once, then, while the drops go on,
    one line a second with how many more there were, and the rest once
    the queue takes a request again. The io thread's only."""

    INTERVAL = 1.0  # seconds between two counts

    def __init__(self, clock: Callable[[], float] = time.monotonic) -> None:
        self.clock = clock
        self.burst = False  # a drop was said and the queue has taken nothing since
        self.more = 0  # dropped since the last line
        self.since = 0.0  # when the last line was said

    def dropped(self, mxmsg: MultiplexerMessage) -> None:
        """A request was dropped: said now when it is the first of a
        burst or a second has passed, else counted."""
        now = self.clock()
        if not self.burst:
            self.burst = True
            self.since = now
            log(
                WARNING,
                HIGHVERBOSITY,
                text="request #%d of type %d dropped: queue full" % (mxmsg.id, mxmsg.type),
            )
            return
        self.more += 1
        if now - self.since >= self.INTERVAL:
            self._say(now)

    def accepted(self) -> None:
        """The queue took a request: the burst is over, and its rest is said."""
        if self.more:
            self._say(self.clock())
        self.burst = False

    def _say(self, now: float) -> None:
        """The count since the last line, in LogSummary's words."""
        log(
            WARNING,
            HIGHVERBOSITY,
            text="requests dropped: queue full [%d more in the last %.1f s]" % (self.more, now - self.since),
        )
        self.more = 0
        self.since = now


class Request:
    """One message being handled, and what answers it.

    `mxmsg` is the message, `connection` the connection it came on. reply()
    answers it: `to`, `references`, `workflow` and the connection are filled
    in from the request. no_response() says it needs no answer, as an event
    does; report_error() answers with BACKEND_ERROR; notify_start() tells
    the requester the work began. A request dropped without a reply or
    no_response() logs a warning when it is garbage-collected.
    """

    def __init__(
        self, server: "BaseThreadedMultiplexerServer", mxmsg: MultiplexerMessage, connection: ConnectionWrapper
    ):
        self.server = server
        self.mxmsg = mxmsg
        self.connection = connection
        self.answered = False
        self.dropped = False  # the server said why; no warning from __del__

    @property
    def client(self) -> ThreadedClient:
        """The server's ThreadedClient, for anything beyond a reply."""
        return self.server.client

    def reply(self, message: Any = b"", **kwargs: Any) -> int:
        """Answer the request with `message` (bytes, str or a protocol
        buffer message) and the remaining fields, `type=` above all;
        `to`, `references`, `workflow` and `multiplexer` default to the
        request's. The kwargs are ThreadedClient.send_message()'s, so
        `flush=True` waits for the write. Returns the message id.

        One reply per request: `references` means "this is the reply", and
        a requester built on ThreadedClient or AsyncClient drops what
        references a query it has seen answered. A follow-up that is not
        the reply, a stream of results after the answer for instance, goes
        through self.server.send_message(..., to=request.mxmsg.from_) with
        no `references`, correlated in the payload."""
        self.answered = True
        kwargs.setdefault("to", self.mxmsg.from_)
        kwargs.setdefault("references", self.mxmsg.id)
        kwargs.setdefault("workflow", self.mxmsg.workflow)
        kwargs.setdefault("multiplexer", self.connection)
        return self.client.send_message(message, **kwargs)

    def no_response(self) -> None:
        """Declare that the message needs no reply, as for an event."""
        self.answered = True

    def notify_start(self) -> None:
        """Tell the requester at once that its request is being worked on (REQUEST_RECEIVED)."""
        answered = self.answered
        self.reply(b"", type=types.REQUEST_RECEIVED)
        self.answered = answered

    def report_error(self, message: Any = "", type: int = types.BACKEND_ERROR, **kwargs: Any) -> int:
        """Answer with BACKEND_ERROR (or `type`) carrying `message`; the requester's query() raises BackendError."""
        return self.reply(message, type=type, **kwargs)

    def parse_message(self, type: Any) -> Any:
        """The payload parsed as the protocol buffer class `type`."""
        return parse_message(type, self.mxmsg.message)

    # The pickle convention, see the clients: a payload that is a Python
    # pickle. Only between Python peers on a trusted network, since
    # unpickling runs code.
    def parse_pickle(self) -> Any:
        """The payload unpickled."""
        return pickle.loads(self.mxmsg.message)

    def reply_pickle(self, data: Any, type: int = types.PICKLE_RESPONSE, **kwargs: Any) -> int:
        """reply() with `data` pickled as the payload, a PICKLE_RESPONSE by default."""
        return self.reply(pickle.dumps(data), type=type, **kwargs)

    def __del__(self) -> None:
        """A request nobody answered or declared answered: say so."""
        if not self.answered and not self.dropped:
            log(
                WARNING,
                LOWVERBOSITY,
                text="request #%d of type %d dropped without a reply or no_response()"
                % (self.mxmsg.id, self.mxmsg.type),
            )


# How long close() waits for the last replies to be written before it
# closes the sockets; a peer that stopped reading cannot hold it longer.
CLOSE_FLUSH_SECONDS = 1.0


_ThreadedServerT = TypeVar("_ThreadedServerT", bound="BaseThreadedMultiplexerServer")


class BaseThreadedMultiplexerServer:
    """Base class for a backend whose handlers run on worker threads:
    subclass, implement handle_message(request), call serve_forever(). See
    the module docstring and docs/api_python.md.
    """

    # the peer type, if a subclass wants to fix it instead of passing `type`
    multiplexer_client_type: int | None = None

    def __init__(
        self,
        addresses: list[Endpoint],
        type: int | None = None,
        workers: int = 1,
        queue_size: int = 1024,
        decline_searches_when_full: bool = False,
        timeout: float = DEFAULT_TIMEOUT,
        drain_routing: Routing | None = None,
    ):
        """A backend of peer type `type` for the multiplexers in
        `addresses`. This only makes the instance id: the `workers` handler
        threads start and the connections open in serve_forever(), so
        nothing reaches handle_message() before the subclass's __init__ is
        done, and no multiplexer knows the backend until it serves. `queue_size`
        bounds the requests waiting for a worker; beyond it a request is
        dropped with a warning, as a full queue on the multiplexer drops,
        and the requester retries through the search. A request that
        arrives while the server is leaving (close() under way) is
        refused with DELIVERY_ERROR instead, so the requester retries at
        once; one that answers another is dropped, since refusing a reply
        could start a loop (docs/leaving.md). With `decline_searches_when_full`, a client's search for a
        backend is left unanswered while every worker is busy and
        requests wait, so the retry lands on another instance.
        `drain_routing` is what the backend tells the multiplexers when it
        starts draining: by default `Routing(any=False, all=False)`,
        nothing new by the rules, only what is addressed to it;
        `Routing(any=False)` keeps events coming, `Routing(any=False,
        all=False, last_resort=True)` keeps a lone backend serving through
        its drain."""
        if type is None:
            type = self.multiplexer_client_type
            if type is None:
                raise ValueError("no type provided and self.multiplexer_client_type is not set")
        if workers < 1:
            raise ValueError("workers must be at least 1")
        self.type = type
        self.workers = workers
        self.queue_size = queue_size
        self.decline_searches_when_full = decline_searches_when_full
        self.drain_routing = drain_routing if drain_routing is not None else Routing(any=False, all=False)
        self.working = True
        self._draining_since: float | None = None
        self._drain_seconds = 0.0
        self._start_time = time.time()
        self._queue: collections.deque[Request] = collections.deque()
        self._cond = threading.Condition()
        self._busy = 0  # workers running a handler
        self.dropped = 0  # requests dropped for a full queue, or while leaving
        self._drop_lines = _DropLines()  # what is said about the requests a full queue dropped
        self._accepting = True
        self._wake = threading.Event()
        self._failure: BaseException | None = None
        self._threads: list[threading.Thread] = []  # started by serve_forever()
        self._addresses = addresses  # connected to by connect(), which serve_forever() calls first
        self._timeout = timeout
        self._connected = False
        self._client: ThreadedClient | None = ThreadedClient(
            [],
            type,
            timeout,
            on_message=self._on_message,
            with_connection=True,
            search_policy=self._answers_search,
        )

    start_time = property(lambda self: self._start_time, doc="Time when the instance was instantiated")

    @property
    def client(self) -> ThreadedClient:
        """The ThreadedClient the server is built on, for messages that are
        not replies; gone after close()."""
        client = self._client
        if client is None:
            raise RuntimeError("the server is closed")
        return client

    @property
    def instance_id(self) -> int:
        """This backend's instance id, what a client addresses with `to`;
        known from construction, before connect() or serve_forever() connects."""
        return self.client.instance_id

    # What subclasses implement or override.

    def handle_message(self, request: Request) -> None:
        """Override: called on a worker thread with every message that is
        not one of the protocol's own. Answer with request.reply(), or
        call request.no_response() for an event; either may happen later,
        from any thread, as long as it happens."""
        raise NotImplementedError()

    def periodic_task(self) -> None:
        """Called from serve_forever() after every poll, on its thread:
        the place for work on the backend's own schedule and for noticing
        a request to leave."""

    def on_handler_exception(self, exc: Exception) -> bool:
        """Called on the worker thread when handle_message() raised, after
        BACKEND_ERROR went to the requester. Return True to keep serving
        (the default); return False and the exception propagates out of
        serve_forever()."""
        return True

    def should_respond_to_backend_for_packet_search(self) -> bool:
        """Whether to answer a client's search for a backend; runs on the
        io thread, so keep it quick. False with
        `decline_searches_when_full` while every worker is busy and
        requests wait. Override for a condition of your own. A draining
        backend needs no policy here: the multiplexers stop offering it
        (its drain_routing), which is the better mechanism. A closing one
        answers no search, whatever this returns."""
        if self.decline_searches_when_full:
            with self._cond:
                return self._busy < self.workers or not self._queue
        return True

    def _answers_search(self) -> bool:
        """The search policy the client runs, on the io thread: none
        answered once close() began, since the request that would follow is
        refused; else should_respond_to_backend_for_packet_search()."""
        return self._accepting and self.should_respond_to_backend_for_packet_search()

    def connect(self) -> None:
        """Start the workers and connect to every multiplexer, once;
        serve_forever() calls it first, and a second call does nothing.
        Call it yourself when something waits for a line you print before
        it sends, so that the line means reachable, or in a test that
        wants the backend connected without a thread serving it."""
        self._check_not_inherited()
        if self._connected or self._client is None:
            return
        self._connected = True
        self._start_workers()  # before the first connection, so that nothing waits for a worker
        for endpoint in self._addresses:
            self._client.connect(endpoint, self._timeout)

    # Leaving.

    @property
    def draining(self) -> bool:
        """Whether start_draining() was called."""
        return self._draining_since is not None

    def start_draining(self) -> None:
        """Tell every multiplexer the `drain_routing`, nothing new by the
        rules by default; keep serving what arrives until drained()."""
        if self._draining_since is None:
            self._draining_since = time.time()
            if self._client is not None:
                self._client.set_routing(self.drain_routing)
            self._wake.set()

    def drained(self) -> bool:
        """Whether the drain is over and serve_forever() may return: by
        default once the `drain_seconds` given to serve_forever() have
        passed since start_draining(), or, when the drain routing turns
        every path off and asks for no last resort, once every connected
        multiplexer has confirmed it and no request is queued or being
        handled, since nothing more is on its way; a drain that keeps a
        path open lasts the whole period, since work keeps arriving.
        Override to wait for your own condition."""
        since = self._draining_since
        if since is None:
            return False
        if time.time() - since >= self._drain_seconds:
            return True
        client = self._client
        if client is None:
            return True  # closed: nothing more is coming
        return nothing_more_arrives(self.drain_routing) and self.pending == 0 and client.routing_acknowledged()

    def stop(self) -> None:
        """Ask serve_forever() to return, from any thread: it finishes what
        the workers hold, closes the connections and returns. In a forked
        child, on a server the parent made, it only clears `working`: a
        signal handler the parent installed may call it there."""
        self.working = False
        client = self._client
        if client is not None and client.orphaned():
            return  # nothing serves here, and the event's lock may be the parent's serving thread's
        self._wake.set()

    @property
    def pending(self) -> int:
        """Requests waiting for a worker plus those being handled; `dropped`
        counts the ones a full queue dropped or leaving refused."""
        self._check_not_inherited()
        with self._cond:
            return len(self._queue) + self._busy

    # The loop.

    def serve_forever(self, poll: float = 1.0, drain_seconds: float = 0.0) -> None:
        """connect(), then run
        until stop() or a drain is over: every `poll` seconds, or sooner
        when woken, call periodic_task(); then take no new message, let
        the workers finish the queue, close the connections and return.
        The calling thread only polls: the handlers run on the workers,
        which is the point."""
        self._check_not_inherited()
        self._drain_seconds = drain_seconds
        try:
            self.connect()
            while self.working and not (self.draining and self.drained()) and self._failure is None:
                self._wake.wait(poll)
                self._wake.clear()
                self.periodic_task()
        finally:
            self.close()
        if self._failure is not None:
            raise self._failure

    def close(self) -> None:
        """Take no more messages, let the workers finish what is queued,
        stop them and close the connections. A request that still arrives,
        routed before the multiplexers applied the drain routing or saw the
        connection go, is refused with DELIVERY_ERROR, so that its
        requester retries elsewhere at once, and a reply is dropped. Safe to
        call twice. Joins the
        workers, so from a handler, on a worker, it raises RuntimeError: a
        handler that wants the server gone calls stop()."""
        self._check_not_inherited()
        if threading.current_thread() in self._threads:
            raise RuntimeError("close() called from a worker thread, which it would join; call stop() instead")
        with self._cond:  # before the drain shows: a request seeing `draining` must find the door shut
            self._accepting = False
            self._cond.notify_all()
        self.start_draining()
        for thread in self._threads:
            thread.join()
        self._threads = []
        if self._client is not None:
            self._client.flush_all(CLOSE_FLUSH_SECONDS)  # the last replies go out before the sockets close
            self._client.shutdown()
            self._client = None

    def __enter__(self: _ThreadedServerT) -> _ThreadedServerT:
        """The server, for a `with` block, at whose end it is closed however
        the block ended: `with MyServer(...) as server: server.serve_forever()`."""
        return self

    def __exit__(self, *exc: object) -> None:
        """close(), at the end of a `with` block; after serve_forever()'s own
        close() a second one does nothing."""
        self.close()

    def _check_not_inherited(self) -> None:
        """Raises UsedAfterFork in a forked child, on a server the parent
        made, before the condition, which the parent's io thread and
        workers take for every request and may have held at the fork."""
        client = self._client
        if client is not None:
            client._check_not_orphaned()

    def send_message(self, message: Any, **kwargs: Any) -> int:
        """A message that is not a reply, an event from a handler for
        instance: ThreadedClient.send_message() with no defaults filled in."""
        return self.client.send_message(message, **kwargs)

    # The io thread's side and the workers' side of the queue.

    def _start_workers(self) -> None:
        """The workers, started once, from connect() and before it
        connects: a request must never reach handle_message() before the
        subclass's __init__ is done, and once connected it must not wait
        for a worker."""
        with self._cond:
            if not self._accepting or self._threads:
                return  # closed already, or served before
            self._threads = [
                threading.Thread(target=self._work, name="mx-worker-%d" % index, daemon=True)
                for index in range(self.workers)
            ]
            for thread in self._threads:
                thread.start()

    def _on_message(self, mxmsg: MultiplexerMessage, connection: ConnectionWrapper) -> None:
        """The io thread: queue the message for a worker; drop it when the
        queue is full, as a full queue on the multiplexer drops; refuse it
        when leaving, with the DELIVERY_ERROR a multiplexer sends for a
        peer that is gone, so that a query retries elsewhere at once, and
        drop it then if it answers another message."""
        request = Request(self, mxmsg, connection)
        with self._cond:
            accepting = self._accepting
            accepted = accepting and len(self._queue) < self.queue_size
            if accepted:
                self._queue.append(request)
                self._cond.notify()
            else:
                self.dropped += 1
        if accepted:
            if self._drop_lines.burst:
                self._drop_lines.accepted()
            return
        request.dropped = True  # said below, not by __del__
        if accepting:
            self._drop_lines.dropped(mxmsg)
            return
        if mxmsg.references:
            # A message that answers another is dropped: nobody retries a
            # reply, and refusing one could start a loop, a peer whose
            # handler raised on the refusal answering it with BACKEND_ERROR,
            # refused in turn, until the close ended.
            log(DEBUG, LOWVERBOSITY, text="reply #%d of type %d dropped: leaving" % (mxmsg.id, mxmsg.type))
            return
        log(DEBUG, LOWVERBOSITY, text="request #%d of type %d refused: leaving" % (mxmsg.id, mxmsg.type))
        # A rule reports delivery errors unless told not to, and so does this;
        # a sender that set the message's own flag to false hears nothing.
        wanted = not mxmsg.HasField("report_delivery_error") or mxmsg.report_delivery_error
        if wanted and mxmsg.type > types.MAX_MULTIPLEXER_META_PACKET:
            error = DeliveryError(packet_id=mxmsg.id, failed_type=[self.type])
            request.reply(error, type=types.DELIVERY_ERROR)

    def _work(self) -> None:
        """A worker: take the next request, handle it, report what the handler raised."""
        while True:
            with self._cond:
                while not self._queue and self._accepting:
                    self._cond.wait()
                if not self._queue:
                    return  # leaving, and the queue is empty
                request = self._queue.popleft()
                self._busy += 1
            try:
                self._handle(request)
            finally:
                with self._cond:
                    self._busy -= 1
                del request

    def _handle(self, request: Request) -> None:
        """One request through handle_message(), with the exception rules of BaseMultiplexerServer."""
        try:
            self.handle_message(request)
        except Exception as exc:  # reported to the requester and to on_handler_exception()
            traceback.print_exc()
            log(ERROR, LOWVERBOSITY, text=lambda: "exception in handle_message: %r" % exc)
            if not request.answered:
                request.report_error(message=format_exception(exc, sys.exc_info()[2]))
            if not self.on_handler_exception(exc):
                self._failure = exc
                self.stop()
