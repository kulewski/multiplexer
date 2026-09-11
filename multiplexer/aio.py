"""AsyncClient: the multiplexer from asyncio, with `await`.

An asyncio face on ThreadedClient (threaded_client.py). The C++ io thread
keeps the sockets, the heartbeats, the reconnects and the query algorithm;
this module turns its callbacks into futures with
loop.call_soon_threadsafe, so a coroutine awaits a reply or a write and
the event loop is never blocked. One client belongs to one event loop, the
way a synchronous Client belongs to one thread; a call from another loop
raises RuntimeError. docs/api_python.md has the user's view and
docs/recipes/async_web_server.md an asyncio web server.

    client = AsyncClient(addresses, type=peers.WEBSITE)
    reply = await client.query(b"pears", types.SEARCH_REQUEST)
    await client.send_message(b"seen", type=types.SEARCH_EVENT)
    unsubscribe = client.subscribe(types.SEARCH_EVENT, handle)   # coroutine or function
    async for mxmsg in client.messages():                        # or pull
        ...

Every send is awaited: there is no fire-and-forget form, since an await
costs the loop microseconds and blocks nothing; many at once is
asyncio.gather. Messages that are not replies, events and requests
addressed to this peer, go to the subscriptions and to messages(); the io
thread never waits for the loop, so a queue nobody drains drops its
oldest message with a warning.
"""

import asyncio
import inspect
import os
import pickle
import threading
from typing import Any, Awaitable, Callable

from multiplexer.Multiplexer_pb2 import MultiplexerMessage
from multiplexer.mxclient import NotConnected, OperationTimedOut
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
    ):
        """Connect to every (host, port) in `addresses` and bind to `loop`,
        the running one by default. Connecting blocks briefly, like
        ThreadedClient's constructor; a program that must not block its
        loop at all uses `await AsyncClient.create(...)`. `queue_size` bounds
        what messages() holds for a slow reader."""
        self._loop = loop or asyncio.get_running_loop()
        self._subscriptions: list[tuple[int | None, Matcher | None, Handler]] = []
        self._queue: asyncio.Queue[MultiplexerMessage] | None = None
        self._queue_size = queue_size
        self._dropped_since_warning = 0
        self._lock = threading.Lock()  # the subscriptions, read on the io thread
        self._threaded = ThreadedClient(addresses, type, timeout, on_message=self._on_message)
        self.type = type

    @classmethod
    async def create(
        cls, addresses: list[Endpoint], type: int, timeout: float = DEFAULT_TIMEOUT, queue_size: int = 1024
    ) -> "AsyncClient":
        """The constructor run in the default executor, so the loop does
        not wait for the connections; bound to the running loop."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, lambda: cls(addresses, type, timeout, loop, queue_size))

    # Identity and connections.

    @property
    def instance_id(self) -> int:
        """This peer's instance id, the `from` of everything it sends."""
        return self._threaded.instance_id

    def connections_count(self) -> int:
        """Live connections right now; the io thread keeps it current."""
        return self._threaded.connections_count()

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        """The event loop this client belongs to."""
        return self._loop

    def _check_loop(self) -> None:
        """A call from another loop, or from no loop, is a mistake made loud."""
        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            raise RuntimeError("AsyncClient must be used from a coroutine on the loop it was created on") from None
        if running is not self._loop:
            raise RuntimeError("AsyncClient belongs to another event loop")

    def _future(self) -> asyncio.Future:
        self._check_loop()
        return self._loop.create_future()

    def _settle(self, future: asyncio.Future, result: Any) -> None:
        """From the io thread: resolve `future` on the loop, unless the
        awaiter cancelled it meanwhile."""

        def on_loop() -> None:
            if future.done():
                return
            if isinstance(result, BaseException):
                future.set_exception(result)
            else:
                future.set_result(result)

        try:
            self._loop.call_soon_threadsafe(on_loop)
        except RuntimeError:
            pass  # the loop is closed; nobody is waiting

    # Requests.

    async def query(self, message: Any, type: int, timeout: float = DEFAULT_TIMEOUT) -> MultiplexerMessage:
        """Send a request and await its reply. Raises the same exceptions
        as the synchronous client: NotConnected, OperationTimedOut,
        OperationFailed, BackendError. Cancelling the await does not cancel
        the request: a backend may still receive it, its reply is dropped."""
        future = self._future()
        self._threaded.query(message, type, timeout, callback=lambda result: self._settle(future, result))
        return await future

    async def query_pickle(self, data: Any, type: int, timeout: float = DEFAULT_TIMEOUT) -> Any:
        """query() with `data` pickled as the payload; the reply's payload unpickled."""
        reply = await self.query(pickle.dumps(data), type, timeout)
        return pickle.loads(reply.message)

    # Sends.

    async def send_message(
        self, message: Any, multiplexer: int = ONE, timeout: float = DEFAULT_TIMEOUT, **kwargs: Any
    ) -> int:
        """Send an event and await it reaching a socket; returns the message
        id. `message` is a MultiplexerMessage or a payload wrapped with the
        remaining kwargs, such as type= and to=. On one connection, resent
        through another if the first dies under it, or on every connection
        with multiplexer=ALL. Raises NotConnected when no connection took it
        by `timeout`, OperationTimedOut when the write did not finish."""
        future = self._future()
        mxmsg_id = self._threaded.send_message(
            message,
            multiplexer,
            flush=True,
            timeout=timeout,
            callback=lambda written: self._settle(future, written),
            **kwargs,
        )
        written = await future
        if written == 0:
            raise NotConnected() if self.connections_count() == 0 else OperationTimedOut()
        return mxmsg_id

    async def send_pickle(self, data: Any, **kwargs: Any) -> int:
        """send_message() with `data` pickled as the payload."""
        return await self.send_message(pickle.dumps(data), **kwargs)

    # What arrives on its own: events, and requests addressed to this peer.

    def subscribe(self, type: int | None, handler: Handler, matching: Matcher | None = None) -> Callable[[], None]:
        """Run `handler(mxmsg)` on the loop for every message of `type`
        (None for every type) for which `matching(mxmsg)` is true; a
        coroutine function runs as a task. Returns the function that ends
        the subscription."""
        entry = (type, matching, handler)
        with self._lock:
            self._subscriptions.append(entry)

        def unsubscribe() -> None:
            with self._lock:
                if entry in self._subscriptions:
                    self._subscriptions.remove(entry)

        return unsubscribe

    def messages(self, type: int | None = None) -> "MessageStream":
        """Every message that arrives on its own, as it arrives, as an async
        iterator; with `type`, only those. The queue behind it exists from
        this call on and holds `queue_size` messages: when nobody reads,
        the oldest is dropped and a warning logged."""
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
        """On the loop: run the handlers, feed the queue."""
        for handler in handlers:
            result = handler(mxmsg)
            if inspect.isawaitable(result):
                self._loop.create_task(result)
        if self._queue is not None:
            if self._queue.full():
                self._queue.get_nowait()  # the oldest goes; the io thread must never wait for the loop
                self._dropped_since_warning += 1
                if self._dropped_since_warning == 1:
                    log(WARNING, LOWVERBOSITY, text="AsyncClient.messages() queue full; dropping the oldest")
            else:
                self._dropped_since_warning = 0
            self._queue.put_nowait(mxmsg)

    # Lifetime.

    def close(self) -> None:
        """Close the connections and stop the io thread; the client is done.
        Blocks briefly; fine at shutdown. Idempotent."""
        self._threaded.shutdown()

    async def aclose(self) -> None:
        """close() in the default executor, so the loop does not wait for the join."""
        await self._loop.run_in_executor(None, self.close)

    async def __aenter__(self) -> "AsyncClient":
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    @classmethod
    def holder(cls, type: int, addresses: list[Endpoint] | Callable[[], list[Endpoint]], **kwargs: Any) -> "Holder":
        """One client per process, created on the running loop at first
        get(), forgotten in a forked child: what an ASGI server's worker
        uses. `addresses` may be a callable, read at first use."""
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

    def __init__(self, cls: type, type: int, addresses: Any, **kwargs: Any):
        self._cls, self._type, self._addresses, self._kwargs = cls, type, addresses, kwargs
        self._client: AsyncClient | None = None
        self._pid = os.getpid()
        os.register_at_fork(after_in_child=self._forget)

    def _forget(self) -> None:
        """A forked child must not touch the parent's client."""
        self._client = None
        self._pid = os.getpid()

    def get(self) -> AsyncClient:
        """The process's client, made on the first call, on the running loop."""
        if self._client is None or self._pid != os.getpid():
            addresses = self._addresses() if callable(self._addresses) else self._addresses
            self._client = self._cls(list(addresses), self._type, **self._kwargs)
            self._pid = os.getpid()
        return self._client

    def close(self) -> None:
        """Close the held client, if any."""
        if self._client is not None:
            self._client.close()
            self._client = None
