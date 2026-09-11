# Recipe: use the client from an async web server

Django Channels, Starlette, or any ASGI application: request handlers are
coroutines on one event loop per process, and a blocking call in one of
them stalls every other. `multiplexer.aio.AsyncClient` is made for that
loop; this page is the Channels version of [examples/aio](../../examples/aio),
which does the same with a plain asyncio TCP server.

## One client per process

ASGI servers fork their workers before the event loop runs, so the client
is created at first use on the worker's loop, never at import. The holder
does exactly that and forgets the client in a forked child:

```python
# mx.py
from django.conf import settings
from multiplexer.aio import AsyncClient
from multiplexer.multiplexer_constants import peers

MX = AsyncClient.holder(peers.WEBSITE, lambda: settings.MULTIPLEXER_ADDRESSES)
```

The peer type may be an active one: the io thread heartbeats. A passive
type works too.

## A consumer

```python
from channels.generic.websocket import AsyncWebsocketConsumer
from multiplexer.multiplexer_constants import types
from .mx import MX


class LessonConsumer(AsyncWebsocketConsumer):
    async def connect(self):
        await self.accept()
        self.key = self.scope["session"].session_key.encode()
        self.unsubscribe = MX.get().subscribe(
            types.LESSON_EVENT, self.push, matching=lambda m: m.message.startswith(self.key)
        )

    async def receive(self, text_data=None, bytes_data=None):
        reply = await MX.get().query(self.key + b" " + bytes_data, types.LESSON_REQUEST, timeout=10)
        await self.send(bytes_data=reply.message)

    async def push(self, mxmsg):
        await self.send(bytes_data=mxmsg.message[len(self.key):])

    async def disconnect(self, code):
        self.unsubscribe()
```

`receive` awaits the backend's reply without holding the loop; `push` is a
coroutine the client schedules on the loop for every event the predicate
accepts. The exceptions are the synchronous client's: catch
`OperationTimedOut` and `OperationFailed` where the socket should get an
error frame rather than nothing.

## Pushing to a socket

Every consumer in a worker shares the worker's one client, so a backend
sending an event reaches the worker, not the socket. The payload carries
what identifies the socket, the session key above, and the subscription's
predicate routes it; a socket held by another worker is reached the way
Channels reaches it, through the channel layer, from a handler that
forwards what it matched. Subscribe in `connect`, unsubscribe in
`disconnect`, or the handler keeps a closed consumer alive.

## Backpressure and shutdown

The io thread hands messages to the loop and never waits for it. A
subscription handler that awaits slowly does not slow the client, it
only piles up tasks; `messages()`, the pull form, has a bounded queue that
drops the oldest with a warning. Size the queue for the burst you expect,
and prefer `subscribe` with a predicate to a consumer that reads
everything.

Nothing is needed at shutdown: a worker that exits with the client alive
exits cleanly, and `MX.close()` from an ASGI lifespan shutdown handler is
the tidy option.

## Testing the consumer

`multiplexer.testing` gives a real multiplexer and a scripted backend in
one process; the consumer's own test is an `IsolatedAsyncioTestCase`:

```python
class LessonConsumerTest(unittest.IsolatedAsyncioTestCase):
    async def test_a_message_is_answered(self):
        with Cluster(1) as cluster, FakePeer(cluster, peers.LESSON_SERVER) as backend:
            backend.reply_with(types.LESSON_REQUEST, b"done", types.LESSON_RESPONSE)
            settings.MULTIPLEXER_ADDRESSES = cluster.endpoints
            ...  # drive the consumer with Channels' communicator, assert on backend.received
            MX.close()
```

[Tests that hold up under load](../api_python.md#tests-that-hold-up-under-load)
applies unchanged.
