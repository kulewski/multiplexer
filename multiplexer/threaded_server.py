"""BaseThreadedMultiplexerServer: a backend whose handlers run on worker
threads behind a heartbeating io thread.

BaseMultiplexerServer (servers.py) runs the loop and the handler on one
thread, so while handle_message() runs nothing heartbeats, and a backend
whose one request takes longer than the multiplexer's drop interval
(docs/semantics.md) is dropped mid-work. This class puts the io on a
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
from typing import Any, Callable

from multiplexer.Multiplexer_pb2 import MultiplexerMessage
from multiplexer.multiplexer_constants import types
from multiplexer.mxclient import ConnectionWrapper, parse_message
from multiplexer.mxlog import ERROR, LOWVERBOSITY, WARNING, log
from multiplexer.servers import format_exception
from multiplexer.threaded_client import DEFAULT_TIMEOUT, Endpoint, ThreadedClient


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
        a threaded or asyncio requester drops what references a query it
        has seen answered. A follow-up that is not the reply, a stream of
        results after the answer for instance, goes through
        self.server.send_message(..., to=request.mxmsg.from_) with no
        `references`, correlated in the payload."""
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


class BaseThreadedMultiplexerServer:
    """Base class for a threaded backend: subclass, implement
    handle_message(request), call serve_forever(). See the module
    docstring and docs/api_python.md.
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
    ):
        """Connect to every multiplexer in `addresses` as a backend of peer
        type `type` and start `workers` handler threads. `queue_size`
        bounds the requests waiting for a worker; beyond it a request is
        dropped with a warning, as a full queue on the multiplexer drops,
        and the requester retries through the search. With
        `decline_searches_when_full`, a client's search for a backend is
        left unanswered while every worker is busy and requests wait, so
        the retry lands on another instance."""
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
        self.working = True
        self._draining_since: float | None = None
        self._drain_seconds = 0.0
        self._start_time = time.time()
        self._queue: collections.deque[Request] = collections.deque()
        self._cond = threading.Condition()
        self._busy = 0  # workers running a handler
        self.dropped = 0  # requests dropped for a full queue, or while leaving
        self._accepting = True
        self._wake = threading.Event()
        self._failure: BaseException | None = None
        self._threads = [
            threading.Thread(target=self._work, name="mx-worker-%d" % index, daemon=True) for index in range(workers)
        ]
        self._client: ThreadedClient | None = ThreadedClient(
            addresses,
            type,
            timeout,
            on_message=self._on_message,
            with_connection=True,
            search_policy=self.should_respond_to_backend_for_packet_search,
        )
        for thread in self._threads:
            thread.start()

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
        """This backend's instance id, what a client addresses with `to`."""
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
        io thread, so keep it quick. False while draining, and with
        `decline_searches_when_full` while every worker is busy and
        requests wait. Override for a condition of your own."""
        if self.draining:
            return False
        if self.decline_searches_when_full:
            with self._cond:
                return self._busy < self.workers or not self._queue
        return True

    # Leaving.

    @property
    def draining(self) -> bool:
        """Whether start_draining() was called."""
        return self._draining_since is not None

    def start_draining(self) -> None:
        """Stop answering backend searches; keep serving what arrives until drained()."""
        if self._draining_since is None:
            self._draining_since = time.time()
            self._wake.set()

    def drained(self) -> bool:
        """Whether the drain is over and serve_forever() may return: by
        default the `drain_seconds` given to serve_forever() have passed
        since start_draining(). Override to wait for your own condition."""
        since = self._draining_since
        return since is not None and time.time() - since >= self._drain_seconds

    def stop(self) -> None:
        """Ask serve_forever() to return, from any thread: it finishes what
        the workers hold, closes the connections and returns."""
        self.working = False
        self._wake.set()

    @property
    def pending(self) -> int:
        """Requests waiting for a worker plus those being handled; `dropped`
        counts the ones a full queue refused."""
        with self._cond:
            return len(self._queue) + self._busy

    # The loop.

    def serve_forever(self, poll: float = 1.0, drain_seconds: float = 0.0) -> None:
        """Run until stop() or a drain is over: every `poll` seconds, or
        sooner when woken, call periodic_task(); then take no new message,
        let the workers finish the queue, close the connections and
        return. The calling thread only polls: the handlers run on the
        workers, which is the point."""
        self._drain_seconds = drain_seconds
        try:
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
        stop them and close the connections. Safe to call twice. Joins the
        workers, so from a handler, on a worker, it raises RuntimeError:
        a handler that wants the server gone calls stop()."""
        if threading.current_thread() in self._threads:
            raise RuntimeError("close() called from a worker thread, which it would join; call stop() instead")
        with self._cond:
            self._accepting = False
            self._cond.notify_all()
        for thread in self._threads:
            thread.join()
        self._threads = []
        if self._client is not None:
            self._client.shutdown()
            self._client = None

    def send_message(self, message: Any, **kwargs: Any) -> int:
        """A message that is not a reply, an event from a handler for
        instance: ThreadedClient.send_message() with no defaults filled in."""
        return self.client.send_message(message, **kwargs)

    # The io thread's side and the workers' side of the queue.

    def _on_message(self, mxmsg: MultiplexerMessage, connection: ConnectionWrapper) -> None:
        """The io thread: queue the message for a worker, or drop it when the queue is full."""
        request = Request(self, mxmsg, connection)
        with self._cond:
            if self._accepting and len(self._queue) < self.queue_size:
                self._queue.append(request)
                self._cond.notify()
                return
        request.dropped = True  # the warning below says it, not __del__
        with self._cond:
            self.dropped += 1
        log(
            WARNING,
            LOWVERBOSITY,
            text="request #%d of type %d dropped: %s"
            % (mxmsg.id, mxmsg.type, "queue full" if self._accepting else "leaving"),
        )

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
