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
the subscriptions and to messages(); the io thread never waits for the
loop, so a queue nobody drains drops its oldest message with a warning.

A query with `to` is addressed, and `multiplexer=` takes a Lane from
lane() or a ConnectionWrapper, as on ThreadedClient.
"""

import asyncio
import concurrent.futures
import inspect
import os
import pickle
import threading
from typing import Any, Awaitable, Callable, Sequence

from multiplexer.Multiplexer_pb2 import MultiplexerMessage, Routing
from multiplexer.multiplexer_constants import types
from multiplexer.mxclient import (
    CLOSE_FLUSH_SECONDS,
    ConnectionWrapper,
    DropReason,
    Lane,
    NotConnected,
    OperationTimedOut,
)
from multiplexer.mxlog import WARNING, LOWVERBOSITY, log
from multiplexer.threaded_client import DEFAULT_TIMEOUT, Endpoint, ThreadedClient

# A subscription's handler: called with the message on the loop; a
# coroutine function is scheduled as a task, a plain function is called.
Handler = Callable[[MultiplexerMessage], Awaitable[None] | None]
Matcher = Callable[[MultiplexerMessage], bool]


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
        the running one by default. Connecting blocks briefly, like
        ThreadedClient's constructor; a program that must not block its
        loop at all uses `await AsyncClient.create(...)`. `queue_size` bounds
        what messages() holds for a slow reader. `on_drop(message_id,
        reason)` runs on the loop for every message the client gives up on,
        each copy of one sent to ALL, with a DropReason; `dropped` counts
        them."""
        self._loop = loop or asyncio.get_running_loop()
        self._subscriptions: list[tuple[int | None, Matcher | None, Handler]] = []
        self._queue: asyncio.Queue[MultiplexerMessage] | None = None
        self._queue_size = queue_size
        self._dropped_since_warning = 0
        self._lock = threading.Lock()  # the subscriptions, read on the io thread
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
        not wait for the connections; bound to the running loop."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None, lambda: cls(addresses, type, timeout, loop, queue_size, on_drop=on_drop)
        )

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
        """This peer's instance id, the `from` of everything it sends."""
        return self._threaded.instance_id

    @property
    def dropped(self) -> int:
        """How many messages this client gave up on so far, each copy of one
        sent to ALL; `on_drop` hears of each as it goes."""
        return self._threaded.dropped

    def connections_count(self) -> int:
        """Live connections right now; the io thread keeps it current."""
        return self._threaded.connections_count()

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
        """messages() from another loop, or from no loop, is a mistake made loud."""
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

    def lane(self, pinned: bool = False, connection: ConnectionWrapper | None = None) -> Lane:
        """A Lane: one connection for a stream of messages, given as
        `multiplexer=` to send_message() and query(); ThreadedClient.lane()
        says the rest."""
        return self._threaded.lane(pinned, connection)

    # Requests.

    async def query(
        self,
        message: Any,
        type: int,
        timeout: float = DEFAULT_TIMEOUT,
        to: int = 0,
        probe: int = types.BACKEND_FOR_PACKET_SEARCH,
        multiplexer: int | Lane | ConnectionWrapper = ONE,
        with_connection: bool = False,
    ) -> Any:
        """Send a request and await its reply. Raises the same exceptions
        as SyncClient: NotConnected, OperationTimedOut,
        OperationFailed, BackendError. Cancelling the await does not cancel
        the request: a backend may still receive it, its reply is dropped.
        `to`, `probe`, `multiplexer` and `with_connection` are
        ThreadedClient.query()'s: an addressed query, how it locates its
        addressee, a lane or a connection to go through, and (reply,
        connection) as the result."""
        future = self._future()
        self._threaded.query(
            message,
            type,
            timeout,
            callback=lambda result: self._settle(future, result),
            to=to,
            probe=probe,
            multiplexer=multiplexer,
            with_connection=with_connection,
        )
        return await future

    async def query_pickle(self, data: Any, type: int, timeout: float = DEFAULT_TIMEOUT, **kwargs: Any) -> Any:
        """query() with `data` pickled as the payload; the reply's payload
        unpickled. The kwargs are query()'s: `to`, `probe`, `multiplexer`."""
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
        to another or having it held, and raises NotConnected when nothing
        wrote it with no connection live, else OperationTimedOut. With a
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
            message, multiplexer, timeout, lambda written, given_up: self._settle(future, (written, given_up)), **kwargs
        )
        written, given_up = await future
        if written == 0:
            self._threaded._raise_for_nothing_written(lane, given_up)
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
        the subscription. In a forked child, on a client the parent made,
        it raises UsedAfterFork, and the function it returned before the
        fork does nothing: nothing is delivered there."""
        self._threaded._check_not_orphaned()  # before the lock, which the parent's io thread takes for every message
        entry = (type, matching, handler)
        with self._lock:
            self._subscriptions.append(entry)

        def unsubscribe() -> None:
            if self._threaded.orphaned():
                return  # quietly, as in cleanup code, and never into a lock the parent's io thread may have held
            with self._lock:
                if entry in self._subscriptions:
                    self._subscriptions.remove(entry)

        return unsubscribe

    def messages(self, type: int | None = None) -> "MessageStream":
        """Every message that arrives on its own, as it arrives, as an async
        iterator; with `type`, only those. The queue behind it exists from
        this call on and holds `queue_size` messages: when nobody reads,
        the oldest is dropped and a warning logged."""
        self._threaded._check_not_orphaned()  # a stream nothing would ever feed, in a forked child
        self._check_loop()
        if self._queue is None:
            self._queue = asyncio.Queue(self._queue_size)
        return MessageStream(self._queue, type)

    def _on_message(self, mxmsg: MultiplexerMessage) -> None:
        """The io thread: hand the message to the loop, cheaply."""
        with self._lock:
            subscriptions = list(self._subscriptions)
        handlers = [
            handler
            for wanted, matching, handler in subscriptions
            if (wanted is None or wanted == mxmsg.type) and (matching is None or matching(mxmsg))
        ]
        if not handlers and self._queue is None:
            return
        try:
            self._loop.call_soon_threadsafe(self._deliver, mxmsg, handlers)
        except RuntimeError:
            pass  # the loop is closed

    def _deliver(self, mxmsg: MultiplexerMessage, handlers: list[Handler]) -> None:
        """On the loop: run the handlers, feed the queue. A handler that
        raises, now or later as a coroutine, is logged with the message's
        type and sender, and the other handlers still run."""
        for handler in handlers:
            try:
                result = handler(mxmsg)
            except Exception as error:  # one handler's failure is not the others'
                self._handler_failed(handler, mxmsg, error)
                continue
            if inspect.isawaitable(result):
                task = asyncio.ensure_future(result, loop=self._loop)
                task.add_done_callback(lambda done, handler=handler, mxmsg=mxmsg: self._task_done(done, handler, mxmsg))
        if self._queue is not None:
            if self._queue.full():
                self._queue.get_nowait()  # the oldest goes; the io thread must never wait for the loop
                self._dropped_since_warning += 1
                if self._dropped_since_warning == 1:
                    log(WARNING, LOWVERBOSITY, text="AsyncClient.messages() queue full; dropping the oldest")
            else:
                self._dropped_since_warning = 0
            self._queue.put_nowait(mxmsg)

    def _task_done(self, task: asyncio.Task, handler: Handler, mxmsg: MultiplexerMessage) -> None:
        """A coroutine handler finished: its exception, if any, is ours to report."""
        if task.cancelled():
            return
        error = task.exception()
        if error is not None:
            self._handler_failed(handler, mxmsg, error)

    @staticmethod
    def _handler_failed(handler: Handler, mxmsg: MultiplexerMessage, error: BaseException) -> None:
        """Log what a subscription handler raised, where the client's other logging goes."""
        log(
            WARNING,
            LOWVERBOSITY,
            text="subscription handler %s raised %r on a message of type %d from %d"
            % (getattr(handler, "__qualname__", repr(handler)), error, mxmsg.type, getattr(mxmsg, "from")),
        )

    # Lifetime.

    def close(self, timeout: float = CLOSE_FLUSH_SECONDS) -> None:
        """Write what was sent before the call, `timeout` seconds at most,
        then close the connections and stop the io thread, as
        ThreadedClient.shutdown(timeout) does; the client is done. Blocks
        for that and for a round trip to the multiplexers, which close their
        side too, a second at most; fine at shutdown. Idempotent."""
        self._threaded.shutdown(timeout)

    async def aclose(self, timeout: float = CLOSE_FLUSH_SECONDS) -> None:
        """close() in the default executor, so the loop does not wait for the join."""
        await asyncio.get_running_loop().run_in_executor(None, self.close, timeout)

    async def __aenter__(self) -> "AsyncClient":
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    @classmethod
    def holder(cls, type: int, addresses: list[Endpoint] | Callable[[], list[Endpoint]], **kwargs: Any) -> "Holder":
        """One client per process, bound to the running loop at first
        get() or aget(), forgotten in a forked child: what an ASGI server's
        worker uses. `addresses` may be a callable, read at first use."""
        return Holder(cls, type, addresses, **kwargs)


class MessageStream:
    """The async iterator messages() returns; `async for mxmsg in stream`."""

    def __init__(self, queue: "asyncio.Queue[MultiplexerMessage]", type: int | None):
        self._queue, self._type = queue, type

    def __aiter__(self) -> "MessageStream":
        return self

    async def __anext__(self) -> MultiplexerMessage:
        while True:
            mxmsg = await self._queue.get()
            if self._type is None or mxmsg.type == self._type:
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
            loop = asyncio.get_running_loop()
            try:
                addresses = self._addresses_now()
            except BaseException as error:
                self._failed(creating, error)
                raise
            threading.Thread(
                target=self._make,
                args=(creating, generation, lambda: self._cls(addresses, self._type, loop=loop, **self._kwargs)),
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
