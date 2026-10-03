"""AsyncClient: the multiplexer from asyncio, with `await`.

An asyncio face on ThreadedClient (threaded_client.py). The C++ io thread
keeps the sockets, the heartbeats, the reconnects and the query algorithm;
this module turns its callbacks into futures with
loop.call_soon_threadsafe, so a coroutine awaits a reply or a write and
the event loop is never blocked. What arrives on its own is delivered on
the loop the client was created on, which its subscriptions and
messages() belong to; query() and send_message() may be awaited from any
loop, as ThreadedClient may be called from any thread, which is what code
run through asgiref's async_to_sync away from the server's loop needs.
Once the client's loop has closed, subscribe() and messages() raise and
what arrives is dropped, said once in a warning: a program that must
receive again makes a new client on a running loop.
docs/api_python.md has the user's view and
docs/recipes/async_web_server.md an asyncio web server.

    client = AsyncClient(addresses, type=peers.WEB)
    reply = await client.query(b"pears", types.SEARCH_REQUEST)
    await client.send_message(b"seen", type=types.SEARCH_EVENT)
    unsubscribe = client.subscribe(types.SEARCH_EVENT, handle)   # coroutine or function
    async for mxmsg in client.messages():                        # or pull
        ...

A send is as on every client: the await returns once the io thread has
the message; flush=True awaits its write, a callback hears how it ended,
and `await flush_all()` waits for everything sent before it. Messages
that are not replies, events and requests addressed to this peer, go to
the subscriptions and to messages(). The io thread never waits for the
loop: it hands every one over, and what the loop has not reached yet
waits in memory, as do the tasks of coroutine handlers, with no bound;
keeping up is the program's to do, and `matching` sheds what it does not
need before the loop sees it, though it runs on the io thread, holding the
GIL the loop needs too, for every message of its subscription's type.
messages() alone holds at most
queue_size, dropping its oldest with a warning when nobody reads it; after
close() it ends, once what arrived before is read.

A query with `to` is addressed, and `multiplexer=` takes a Lane from
lane() or a ConnectionWrapper, as on ThreadedClient.
"""

import asyncio
import concurrent.futures
import heapq
import inspect
import itertools
import os
import pickle
import threading
from typing import Any, Awaitable, Callable, Sequence

from multiplexer.Multiplexer_pb2 import MultiplexerMessage, Routing
from multiplexer.mxclient import (
    CLOSE_FLUSH_SECONDS,
    ConnectionWrapper,
    DropReason,
    Lane,
)
from multiplexer.mxlog import WARNING, LOWVERBOSITY, log
from multiplexer.threaded_client import DEFAULT_TIMEOUT, Endpoint, ThreadedClient

# A subscription's handler: called with the message on the loop; a
# coroutine function is scheduled as a task, a plain function is called.
Handler = Callable[[MultiplexerMessage], Awaitable[None] | None]
Matcher = Callable[[MultiplexerMessage], bool]

# What subscribe(), messages() and the io thread say once the client's loop
# has closed.
_LOOP_CLOSED = (
    "the loop the client was created on has closed; to receive, close the client,"
    " or its holder, and make one on a running loop"
)


class _Subscription:
    """What subscribe() keeps: the type, the predicate, the handler, its
    place among the subscriptions, the order its handler runs in, and
    whether the subscription still holds, which a delivery checks on the
    loop, so that once unsubscribe() has returned the handler is never
    called again, for a message already handed to the loop either."""

    __slots__ = ("type", "matching", "handler", "order", "active")

    def __init__(self, type: int | None, matching: Matcher | None, handler: Handler, order: int):
        self.type = type
        self.matching = matching
        self.handler = handler
        self.order = order
        self.active = True


def _in_order(subscription: _Subscription) -> int:
    """Where `subscription` comes among the subscriptions, for the merge of
    a type's with every type's."""
    return subscription.order


class AsyncClient:
    """The client; see the module docstring and docs/api_python.md."""

    ONE = ThreadedClient.ONE
    ALL = ThreadedClient.ALL

    def __init__(
        self,
        addresses: list[Endpoint],
        type: int,
        timeout: float = DEFAULT_TIMEOUT,
        loop: asyncio.AbstractEventLoop | None = None,
        queue_size: int = 1024,
        *,
        on_drop: Callable[[int, DropReason], None] | None = None,
    ):
        """Connect to every (host, port) in `addresses` and bind to `loop`,
        the running one by default. Connecting blocks the loop, like
        ThreadedClient's constructor: one address after another, a
        handshake each, a round trip when its multiplexer is up, up to
        `timeout` for one that hangs; a program that must not block its
        loop at all uses `await AsyncClient.create(...)`. `queue_size` bounds
        what messages() holds for a slow reader, and only that: what waits
        for the loop has no bound. It is 1 at least, ValueError otherwise:
        asyncio.Queue's 0 for no bound is not offered. `on_drop(message_id,
        reason)` runs on the loop for every message the client gives up on,
        each copy of one sent to ALL, with a DropReason; `dropped` counts
        them."""
        if queue_size < 1:
            raise ValueError("queue_size is how many messages messages() holds, 1 at least: %r" % (queue_size,))
        self._loop = loop or asyncio.get_running_loop()
        # The subscriptions by their type, None for every type, each type's in
        # the order they were made: a snapshot the io thread reads as it is,
        # replaced whole under the lock by subscribe() and unsubscribe(), so
        # that a message costs a look at its type's subscriptions only, with
        # no copy and no lock.
        self._by_type: dict[int | None, tuple[_Subscription, ...]] = {}
        self._orders = itertools.count()
        # messages()'s queue, made at its first call. Unbounded to asyncio:
        # _deliver keeps it to queue_size, so that the mark close() puts at
        # its end, None, always fits.
        self._queue: asyncio.Queue[MultiplexerMessage | None] | None = None
        self._messages_ended = False  # on the loop: close() came, the mark is in the queue or will be at its making
        self._queue_size = queue_size
        self._dropped_since_warning = 0
        # The io thread said that the loop had closed under what arrives: once.
        self._said_loop_closed = False
        # close() or aclose() was called: no handler is called again. Set on
        # any thread, read by _deliver on the loop.
        self._closing = False
        # The tasks of coroutine handlers still running, on the loop: the
        # loop holds a task only weakly, and close() ends them.
        self._tasks: set[asyncio.Task] = set()
        self._lock = threading.Lock()  # the writers of the subscriptions' snapshot
        self._on_drop = on_drop
        self._threaded = ThreadedClient(
            addresses,
            type,
            timeout,
            on_message=self._on_message,
            on_drop=self._on_drop_reported if on_drop is not None else None,
        )
        self.type = type

    @classmethod
    async def create(
        cls,
        addresses: list[Endpoint],
        type: int,
        timeout: float = DEFAULT_TIMEOUT,
        queue_size: int = 1024,
        *,
        on_drop: Callable[[int, DropReason], None] | None = None,
    ) -> "AsyncClient":
        """The constructor run in the default executor, so the loop does
        not wait for the connections; bound to the running loop. A caller
        that gives up meanwhile, a timeout around the await say, stops
        waiting, and the client made anyway is closed once it is there,
        rather than left connected with nobody to close it."""
        loop = asyncio.get_running_loop()
        making = loop.run_in_executor(None, lambda: cls(addresses, type, timeout, loop, queue_size, on_drop=on_drop))
        try:
            return await asyncio.shield(making)
        except asyncio.CancelledError:
            making.add_done_callback(cls._close_unwanted)
            raise

    @staticmethod
    def _close_unwanted(making: asyncio.Future) -> None:
        """The client a cancelled create() made, once it is there: closed on
        a thread of its own, since close() waits for the io thread."""
        if not making.cancelled() and making.exception() is None:
            threading.Thread(target=making.result().close, name="mx-create-cancelled", daemon=True).start()

    def _on_drop_reported(self, message_id: int, reason: DropReason) -> None:
        """A drop, as the io thread reports it, handed to `on_drop` on the loop."""
        on_drop = self._on_drop
        assert on_drop is not None
        try:
            self._loop.call_soon_threadsafe(on_drop, message_id, reason)
        except RuntimeError:
            pass  # the loop is closed: nobody left to tell

    # Identity and connections.

    @property
    def instance_id(self) -> int:
        """This peer's instance id, the `sender` of everything it sends."""
        return self._threaded.instance_id

    def new_message(self, **kwargs: Any) -> MultiplexerMessage:
        """A MultiplexerMessage with id and sender filled in, as ThreadedClient.new_message() makes it."""
        return self._threaded.new_message(**kwargs)

    @property
    def dropped(self) -> int:
        """How many messages this client gave up on so far, each copy of one
        sent to ALL; `on_drop` hears of each as it goes."""
        return self._threaded.dropped

    def connections_count(self) -> int:
        """Live connections right now; the io thread keeps it current."""
        return self._threaded.connections_count()

    def disconnect(self, endpoint: Endpoint) -> bool:
        """Drop the multiplexer given to the constructor as `endpoint`, the
        same (host, port); see ThreadedClient.disconnect, whose wait for
        the io thread blocks the loop briefly. Whether the client had it;
        NotConnected after close()."""
        return self._threaded.disconnect(endpoint)

    def set_routing(self, routing: Routing) -> None:
        """Which of a multiplexer's routing paths reach this peer; see
        ThreadedClient.set_routing. Does not block."""
        self._threaded.set_routing(routing)

    def routing_acknowledged(self) -> bool:
        """Whether every connected multiplexer has that routing in effect;
        see ThreadedClient.routing_acknowledged. Blocks briefly."""
        return self._threaded.routing_acknowledged()

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        """The event loop the client was created on, where what arrives on
        its own is delivered."""
        return self._loop

    def _check_loop(self) -> None:
        """messages() from another loop, from no loop, or once the client's
        loop has closed, is a mistake made loud."""
        if self._loop.is_closed():
            raise RuntimeError("AsyncClient.messages(): %s" % _LOOP_CLOSED)
        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            raise RuntimeError("AsyncClient.messages() must be used on the loop the client was created on") from None
        if running is not self._loop:
            raise RuntimeError("AsyncClient.messages() belongs to the loop the client was created on")

    @staticmethod
    def _future() -> asyncio.Future:
        """A future on the loop that awaits it, whichever that is."""
        try:
            return asyncio.get_running_loop().create_future()
        except RuntimeError:
            raise RuntimeError("AsyncClient's calls are awaited from a coroutine on a running loop") from None

    @staticmethod
    def _settle(future: asyncio.Future, result: Any) -> None:
        """From the io thread: resolve `future` on its loop, unless the
        awaiter cancelled it meanwhile."""

        def on_loop() -> None:
            if future.done():
                return
            if isinstance(result, BaseException):
                future.set_exception(result)
            else:
                future.set_result(result)

        try:
            future.get_loop().call_soon_threadsafe(on_loop)
        except RuntimeError:
            pass  # the loop is closed; nobody is waiting

    @staticmethod
    def _before(future: asyncio.Future, callback: Callable[[int], None]) -> Callable[[int], None]:
        """`callback` for the io thread to call: run on the loop that awaits
        `future`, so that whatever the io thread told it before settling the
        future (_settle) runs there before the awaiter resumes."""
        loop = future.get_loop()

        def on_loop(value: int) -> None:
            try:
                loop.call_soon_threadsafe(callback, value)
            except RuntimeError:
                pass  # the loop is closed; nobody is waiting

        return on_loop

    def lane(self, pinned: bool = False, connection: ConnectionWrapper | None = None) -> Lane:
        """A Lane: one connection for a stream of messages, given as
        `multiplexer=` to send_message() and query(); ThreadedClient.lane()
        says the rest."""
        return self._threaded.lane(pinned, connection)

    # Requests.

    async def query(
        self,
        message: Any,
        type: int | None = None,
        timeout: float = DEFAULT_TIMEOUT,
        to: int = 0,
        multiplexer: int | Lane | ConnectionWrapper = ONE,
        with_connection: bool = False,
        on_received: Callable[[int], None] | None = None,
    ) -> Any:
        """Send a request and await its reply: `message` itself when it is
        a whole MultiplexerMessage, typed and addressed by its own fields,
        else one built from the payload, `type` and `to`, as
        ThreadedClient.query() takes them. Raises the same exceptions
        as SyncClient: NotConnected, OperationTimedOut,
        OperationFailed, BackendError. Cancelling the await does not cancel
        the request: a backend may still receive it, its reply is dropped.
        `to`, `multiplexer` and `with_connection` are
        ThreadedClient.query()'s: an addressed query, located with a PING
        when it moved, a lane or a connection to go through, and (reply,
        connection) as the result. `on_received`, when given, is called on
        the loop, before the reply is, with the instance id of each backend
        that acknowledges the request (notify_start()), as in
        ThreadedClient.query()."""
        future = self._future()
        self._threaded.query(
            message,
            type,
            timeout,
            callback=lambda result: self._settle(future, result),
            to=to,
            multiplexer=multiplexer,
            with_connection=with_connection,
            on_received=None if on_received is None else self._before(future, on_received),
        )
        return await future

    async def query_pickle(self, data: Any, type: int, timeout: float = DEFAULT_TIMEOUT, **kwargs: Any) -> Any:
        """query() with `data` pickled as the payload; the reply's payload
        unpickled. The kwargs are query()'s: `to`, `multiplexer`,
        `on_received`; `with_connection` raises TypeError, the result being
        the payload alone."""
        if "with_connection" in kwargs:
            raise TypeError("query_pickle() returns the payload alone: no with_connection")
        reply = await self.query(pickle.dumps(data), type, timeout, **kwargs)
        return pickle.loads(reply.message)

    # Sends.

    async def send_message(
        self,
        message: Any,
        multiplexer: int | Lane | ConnectionWrapper = ONE,
        timeout: float = DEFAULT_TIMEOUT,
        flush: bool = False,
        callback: Callable[[int], None] | None = None,
        **kwargs: Any,
    ) -> int:
        """Send an event and return its message id, as every client sends
        (ThreadedClient.send_message): `message` is a MultiplexerMessage or
        a payload wrapped with the remaining kwargs, such as type= and to=;
        on one connection, on every one with multiplexer=ALL, on a Lane's
        connection, or on a ConnectionWrapper's while it is live and
        another after. It returns once the io thread has the message, which
        it writes right after, or holds until a connection comes up or has
        room, within `timeout`, and drops and reports (on_drop) after that.
        With `flush=True` it returns once the message reached a socket, for
        ALL once one copy did, a connection that dies under it handing it
        to another or having it held (a copy for ALL is held only when no
        connection is live, and dropped otherwise), and raises NotConnected
        when nothing wrote it with no connection live, or when every copy
        for ALL went with its connection, else OperationTimedOut. With a
        `callback`, flush or not, it returns at once and `callback(written)`
        runs on the client's loop once the message's end is known: 1 once
        it was written, the first copy for ALL, 0 once it was given up on
        or close() came first. NotConnected at once for a pinned lane whose
        connection is gone, and after close()."""
        if callback is not None:
            return self._threaded.send_message(
                message,
                multiplexer,
                timeout=timeout,
                callback=lambda written: self._call_on_loop(callback, written),
                **kwargs,
            )
        if not flush:
            return self._threaded.send_message(message, multiplexer, timeout=timeout, **kwargs)
        future = self._future()
        lane = multiplexer if isinstance(multiplexer, Lane) else None
        mxmsg_id = self._threaded._send_and_notify(
            message,
            multiplexer,
            timeout,
            lambda written, not_connected: self._settle(future, (written, not_connected)),
            **kwargs,
        )
        written, not_connected = await future
        if written == 0:
            self._threaded._raise_for_nothing_written(lane, not_connected)
        return mxmsg_id

    async def flush_all(self, timeout: float = DEFAULT_TIMEOUT) -> bool:
        """Await until everything sent before the call was written or given
        up on, what still waits for a connection or for room included, or
        `timeout` seconds; whether every one was written, as
        ThreadedClient.flush_all() says. The
        callbacks of those sends have run by then when awaited on the
        client's loop, where they run."""
        future = self._future()
        self._threaded._flush_all_and_notify(timeout, lambda flushed: self._settle(future, flushed))
        return await future

    def _call_on_loop(self, callback: Callable[[int], None], written: int) -> None:
        """A send's callback, as the io thread calls it, run on the loop."""
        try:
            self._loop.call_soon_threadsafe(callback, written)
        except RuntimeError:
            pass  # the loop is closed: nobody left to tell

    async def send_pickle(self, data: Any, **kwargs: Any) -> int:
        """send_message() with `data` pickled as the payload."""
        return await self.send_message(pickle.dumps(data), **kwargs)

    # What arrives on its own: events, and requests addressed to this peer.

    def subscribe(self, type: int | None, handler: Handler, matching: Matcher | None = None) -> Callable[[], None]:
        """Run `handler(mxmsg)` on the loop for every message of `type`
        (None for every type) for which `matching(mxmsg)` is true; a
        coroutine function runs as a task. Returns the function that ends
        the subscription: once it has returned, on the loop, the handler is
        not called again, for a message already handed to the loop either;
        a coroutine already running goes on. RuntimeError once the client's
        loop has closed, since nothing could be delivered there. In a forked
        child, on a client the parent made, it raises UsedAfterFork, and the
        function it returned before the fork does nothing: nothing is
        delivered there."""
        self._threaded._check_not_orphaned()  # before the lock, which a thread of the parent may have held
        if self._loop.is_closed():
            raise RuntimeError("AsyncClient.subscribe(): %s" % _LOOP_CLOSED)
        with self._lock:
            entry = _Subscription(type, matching, handler, next(self._orders))
            by_type = dict(self._by_type)
            by_type[type] = by_type.get(type, ()) + (entry,)
            self._by_type = by_type

        def unsubscribe() -> None:
            if self._threaded.orphaned():
                return  # quietly, as in cleanup code, and never into a lock a thread of the parent may have held
            entry.active = False  # a delivery already handed to the loop skips it
            with self._lock:
                kept = tuple(subscription for subscription in self._by_type.get(type, ()) if subscription is not entry)
                by_type = dict(self._by_type)
                if kept:
                    by_type[type] = kept
                else:
                    by_type.pop(type, None)
                self._by_type = by_type

        return unsubscribe

    def messages(self) -> "MessageStream":
        """Every message that arrives on its own, as it arrives, as an async
        iterator: the client's inbox, as read_message() is SyncClient's.
        Every call reads the one queue, so tasks reading it share the
        messages, each going to one of them; subscribe() gives a handler
        every message of a type, each handler its own copy. The queue
        exists from the first call on and holds `queue_size` messages: when
        nobody reads, the oldest is dropped and a warning logged. After
        close() every reader gets what arrived before it and then ends, its
        `async for` over; a stream asked for after close() ends at once."""
        self._threaded._check_not_orphaned()  # a stream nothing would ever feed, in a forked child
        self._check_loop()
        if self._queue is None:
            self._queue = asyncio.Queue()
            if self._messages_ended:
                self._queue.put_nowait(None)
        return MessageStream(self._queue)

    def _on_message(self, mxmsg: MultiplexerMessage) -> None:
        """The io thread: hand the message to the loop, cheaply, looking at
        the subscriptions of its type and of every type only, in the order
        they were made. A `matching` that raises takes it for none of its
        subscription's handlers, logged as a handler that raises is. A loop
        that has closed takes nothing, which is said once, unless the client
        is closing."""
        by_type = self._by_type  # the snapshot: replaced whole, never changed
        typed = by_type.get(mxmsg.type, ())
        every = by_type.get(None, ())
        subscriptions = heapq.merge(typed, every, key=_in_order) if typed and every else typed or every
        chosen: list[_Subscription] = []
        for subscription in subscriptions:
            if subscription.matching is not None:
                try:
                    if not subscription.matching(mxmsg):
                        continue
                except Exception as error:  # one predicate's failure is not the others'
                    self._subscription_failed("predicate", subscription.matching, mxmsg, error)
                    continue
            chosen.append(subscription)
        if not chosen and self._queue is None:
            return
        try:
            self._loop.call_soon_threadsafe(self._deliver, mxmsg, chosen)
        except RuntimeError:  # the loop is closed
            if not self._said_loop_closed and not self._closing:
                self._said_loop_closed = True
                log(WARNING, LOWVERBOSITY, text="AsyncClient: what arrives is dropped: %s" % _LOOP_CLOSED)

    def _deliver(self, mxmsg: MultiplexerMessage, subscriptions: list[_Subscription]) -> None:
        """On the loop: run the handlers of the subscriptions that still
        hold, unless the client is closing, and feed the queue. A handler
        that raises, now or later as a coroutine, is logged with the
        message's type and sender, and the other handlers still run."""
        for subscription in subscriptions:
            if self._closing:
                break  # close() came since the message was handed over
            if not subscription.active:
                continue  # unsubscribed since the message was handed over
            handler = subscription.handler
            try:
                result = handler(mxmsg)
            except Exception as error:  # one handler's failure is not the others'
                self._subscription_failed("handler", handler, mxmsg, error)
                continue
            if inspect.isawaitable(result):
                task = asyncio.ensure_future(result, loop=self._loop)
                self._tasks.add(task)
                task.add_done_callback(lambda done, handler=handler, mxmsg=mxmsg: self._task_done(done, handler, mxmsg))
        if self._queue is not None:
            if self._queue.qsize() >= self._queue_size:
                self._queue.get_nowait()  # the oldest goes; the io thread must never wait for the loop
                self._dropped_since_warning += 1
                if self._dropped_since_warning == 1:
                    log(WARNING, LOWVERBOSITY, text="AsyncClient.messages() queue full; dropping the oldest")
            else:
                self._dropped_since_warning = 0
            self._queue.put_nowait(mxmsg)

    def _task_done(self, task: asyncio.Task, handler: Handler, mxmsg: MultiplexerMessage) -> None:
        """A coroutine handler finished: its exception, if any, is ours to report."""
        self._tasks.discard(task)
        if task.cancelled():
            return
        error = task.exception()
        if error is not None:
            self._subscription_failed("handler", handler, mxmsg, error)

    @staticmethod
    def _subscription_failed(
        role: str, function: Callable[..., Any], mxmsg: MultiplexerMessage, error: BaseException
    ) -> None:
        """Log what a subscription's handler or predicate, `role`, raised, where the client's other logging goes."""
        log(
            WARNING,
            LOWVERBOSITY,
            text="subscription %s %s raised %r on a message of type %d from %d"
            % (role, getattr(function, "__qualname__", repr(function)), error, mxmsg.type, mxmsg.sender),
        )

    # Lifetime.

    def close(self, timeout: float = CLOSE_FLUSH_SECONDS) -> None:
        """Write what was sent before the call, `timeout` seconds at most,
        then close the connections and stop the io thread, as
        ThreadedClient.shutdown(timeout) does; the client is done. Blocks
        for that and for a round trip to the multiplexers, which close their
        side too, a second at most; fine at shutdown. Idempotent. From the
        call on no subscription's handler is called, for a message handed
        to the loop before either, and once it has returned the tasks of
        coroutine handlers still running are cancelled on the loop, but for
        the one that called it; aclose() gives them time first. Then
        messages() ends, for every reader, once what arrived before is read."""
        self._close(timeout, self._calling_task())

    def _close(self, timeout: float, sparing: asyncio.Task | None) -> None:
        """close(), the handler task `sparing`, the caller's, left running."""
        self._closing = True
        self._threaded.shutdown(timeout)
        if self._threaded.orphaned():
            return  # a forked child: the loop and its wake-up pipe are the parent's
        try:
            # After the io thread's end: behind every message it handed to the loop.
            self._loop.call_soon_threadsafe(self._ended, sparing)
        except RuntimeError:
            pass  # the loop is closed: nobody reads, nothing runs

    def _calling_task(self) -> asyncio.Task | None:
        """The task this call runs in, when it runs on the client's loop: a
        coroutine handler that closes the client is not cancelled by it."""
        try:
            if asyncio.get_running_loop() is self._loop:
                return asyncio.current_task()
        except RuntimeError:
            pass  # no loop runs on this thread
        return None

    def _ended(self, sparing: asyncio.Task | None) -> None:
        """On the loop, once the client is closed: the handler tasks still
        running are cancelled, but for `sparing`, and the mark that ends
        messages() goes at the end of its queue, once."""
        for task in list(self._tasks):
            if task is not sparing:
                task.cancel()
        if self._messages_ended:
            return
        self._messages_ended = True
        if self._queue is not None:
            self._queue.put_nowait(None)

    async def aclose(self, timeout: float = CLOSE_FLUSH_SECONDS) -> None:
        """close() in the default executor, so the loop does not wait for
        the join, after the tasks of coroutine handlers still running had
        `timeout` seconds to end, when awaited on the client's loop: what
        they send goes out with the rest, and those still running then are
        cancelled. No handler is called from the call on."""
        self._closing = True
        loop = asyncio.get_running_loop()
        this = asyncio.current_task() if loop is self._loop else None  # a handler closing the client goes on
        if loop is self._loop:
            running = [task for task in self._tasks if task is not this]
            if running:
                _, late = await asyncio.wait(running, timeout=timeout)
                for task in late:
                    task.cancel()
        await loop.run_in_executor(None, self._close, timeout, this)

    async def __aenter__(self) -> "AsyncClient":
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    @classmethod
    def holder(cls, type: int, addresses: list[Endpoint] | Callable[[], list[Endpoint]], **kwargs: Any) -> "Holder":
        """One client per process, bound to the running loop at first
        get() or aget(), or to `loop=` among the kwargs, the constructor's,
        forgotten in a forked child: what an ASGI server's worker uses.
        `addresses` may be a callable, read at first use. The first
        caller's loop is the client's for as long as the holder keeps it:
        once that loop has closed, subscribe() raises, and close() makes
        the next use start a client on its own loop, as a test whose loop
        ends with it does in its tear-down. The holder does not replace the
        client by itself: async_to_sync with no server's loop runs every
        call on a new loop, closed after it, and a process that only sends
        keeps one client through them all."""
        return Holder(cls, type, addresses, **kwargs)


class MessageStream:
    """The async iterator messages() returns; `async for mxmsg in stream`,
    which ends once the client is closed and what arrived before is read."""

    def __init__(self, queue: "asyncio.Queue[MultiplexerMessage | None]"):
        self._queue = queue

    def __aiter__(self) -> "MessageStream":
        return self

    async def __anext__(self) -> MultiplexerMessage:
        mxmsg = await self._queue.get()
        if mxmsg is None:  # close()'s mark: put back for the other readers, which end too
            self._queue.put_nowait(None)
            raise StopAsyncIteration
        return mxmsg


class Holder:
    """One AsyncClient per process; see AsyncClient.holder()."""

    def __init__(
        self, cls: type, type: int, addresses: Sequence[Endpoint] | Callable[[], Sequence[Endpoint]], **kwargs: Any
    ):
        self._cls, self._type, self._addresses, self._kwargs = cls, type, addresses, kwargs
        self._client: AsyncClient | None = None
        # The client aget() is making on a thread of its own: every caller
        # awaits it until it is there, it belongs to no loop, and no
        # caller's cancellation ends it.
        self._creating: concurrent.futures.Future | None = None
        self._generation = 0  # moved by close() and a fork: a client made across either is closed, not kept
        self._lock = threading.Lock()
        self._pid = os.getpid()
        os.register_at_fork(after_in_child=self._forget)

    def _forget(self) -> None:
        """A forked child must not touch the parent's client. One it
        inherited is closed, which there closes only the child's copies of
        the parent's connections: dropping it would close nothing, since
        its callback keeps it alive."""
        client = self._client
        self._lock = threading.Lock()
        self._client = None
        self._creating = None
        self._generation += 1
        self._pid = os.getpid()
        if client is not None:
            client.close()

    def _addresses_now(self) -> list[Endpoint]:
        return list(self._addresses() if callable(self._addresses) else self._addresses)

    def get(self) -> AsyncClient:
        """The process's client, made on the first call, on the running loop.
        Making it connects, which blocks the loop for the handshakes; a
        first use that must not do that awaits aget() instead. While another
        call, get() or aget() on any thread, is making it, get() waits for
        that one: there is never more than one. A client that could not be
        made raises for every call that waited for it; the next call tries
        again."""
        if self._pid != os.getpid():
            self._forget()
        client = self._client  # without the lock: once there, it stays until close() or a fork
        if client is not None:
            return client
        creating, generation = self._creation()
        if generation is not None:  # this call makes it
            self._make(creating, generation, lambda: self._cls(self._addresses_now(), self._type, **self._kwargs))
        return creating.result()

    async def aget(self) -> AsyncClient:
        """The process's client, made on a thread on the first call so
        that the loop never waits for the connections, and bound to the
        running loop; callers that arrive while it is being made, get() or
        aget() on any thread, await the same one, so there is never more
        than one. A caller cancelled meanwhile stops waiting; the client is
        still made and kept. One that could not be made raises for every
        caller that waited for it; the next call tries again."""
        if self._pid != os.getpid():
            self._forget()
        client = self._client  # without the lock: once there, it stays until close() or a fork
        if client is not None:
            return client
        creating, generation = self._creation()
        if generation is not None:  # this call makes it, on a thread of its own
            # On the loop the holder was given, as get() makes it, else on this one.
            kwargs = dict(self._kwargs)
            kwargs["loop"] = kwargs.get("loop") or asyncio.get_running_loop()
            try:
                addresses = self._addresses_now()
            except BaseException as error:
                self._failed(creating, error)
                raise
            threading.Thread(
                target=self._make,
                args=(creating, generation, lambda: self._cls(addresses, self._type, **kwargs)),
                name="mx-holder",
                daemon=True,
            ).start()
        return await asyncio.shield(asyncio.wrap_future(creating))

    def _creation(self) -> tuple[concurrent.futures.Future, int | None]:
        """Under the lock: the creation under way and None; or, with none, a
        new one and the generation it is made under, for the caller to make.
        The creation is listed before anything is made, so that a call
        arriving meanwhile waits for it, from any thread, and one that fails
        at once is cleared. A client made since the caller looked is a
        creation already over."""
        with self._lock:
            if self._client is not None:
                made: concurrent.futures.Future = concurrent.futures.Future()
                made.set_result(self._client)
                return made, None
            if self._creating is not None:
                return self._creating, None
            self._creating = concurrent.futures.Future()
            return self._creating, self._generation

    def _make(self, creating: concurrent.futures.Future, generation: int, construct: Callable[[], AsyncClient]) -> None:
        """Runs `construct` and settles `creating` with the client or the
        error: the client kept, unless close() or a fork came meanwhile,
        when it is closed instead."""
        try:
            client = construct()
        except BaseException as error:
            self._failed(creating, error)
            return
        with self._lock:
            kept = generation == self._generation
            if kept:
                self._client = client
            if self._creating is creating:
                self._creating = None
        if kept:
            creating.set_result(client)
        else:
            client.close()
            creating.set_exception(RuntimeError("the holder was closed while its client was being made"))

    def _failed(self, creating: concurrent.futures.Future, error: BaseException) -> None:
        """The creation `creating` failed with `error`: cleared, so that the
        next call tries again, and every caller waiting for it raises."""
        with self._lock:
            if self._creating is creating:
                self._creating = None
        creating.set_exception(error)

    def _detach(self) -> AsyncClient | None:
        """Take the held client out, and disown one being made."""
        with self._lock:
            client, self._client = self._client, None
            self._creating = None
            self._generation += 1
        return client

    def close(self, timeout: float = CLOSE_FLUSH_SECONDS) -> None:
        """Close the held client, if any, as its close(timeout) does; one
        being made is closed when it is there, and its callers get
        RuntimeError. The next get() or aget() makes a new one."""
        client = self._detach()
        if client is not None:
            client.close(timeout)

    async def aclose(self, timeout: float = CLOSE_FLUSH_SECONDS) -> None:
        """close() in the default executor, so the loop does not wait for the join."""
        client = self._detach()
        if client is not None:
            await client.aclose(timeout)
