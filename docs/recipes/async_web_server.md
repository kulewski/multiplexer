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

MX = AsyncClient.holder(peers.WEB, lambda: settings.MULTIPLEXER_ADDRESSES)
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
        mx = await MX.aget()
        self.unsubscribe = mx.subscribe(types.LESSON_EVENT, self.push, matching=lambda m: m.message.startswith(self.key))

    async def receive(self, text_data=None, bytes_data=None):
        mx = await MX.aget()   # made on a thread at the worker's first use, never on the loop
        reply = await mx.query(self.key + b" " + bytes_data, types.LESSON_REQUEST, timeout=10)
        await self.send(bytes_data=reply.message)

    async def push(self, mxmsg):
        await self.send(bytes_data=mxmsg.message[len(self.key):])

    async def disconnect(self, code):
        self.unsubscribe()
```

`receive` awaits the backend's reply without holding the loop; `push` is a
coroutine the client schedules on the loop for every event the predicate
accepts, and if it raises, the client logs it with the message's type and
sender and the other subscriptions still run. The exceptions are those of `SyncClient`: catch
`OperationTimedOut` and `OperationFailed` where the socket should get an
error frame rather than nothing.

## Pushing to a socket

Every consumer in a worker shares the worker's one client, so a backend
sending an event reaches the worker, not the socket. The payload carries
what identifies the socket, the session key above, and the subscription's
predicate routes it. The predicate runs on the client's io thread for
every event of its type that arrives, before anything reaches the loop,
holding the GIL the loop needs too, so it stays a quick test like the one
above, touching nothing of the loop's; an event it refuses never reaches
the loop. A socket held by another worker is
reached the way
Channels reaches it, through the channel layer, from a handler that
forwards what it matched. Subscribe in `connect`, unsubscribe in
`disconnect`, or the handler keeps a closed consumer alive. The channel
layer itself can be the multiplexer: [examples/channels](../../examples/channels)
is a layer whose group sends are events to every process and whose
channel sends are addressed to the owning process, so the same
connections carry the layer and the backends.
[examples/audio](../../examples/audio) streams through those connections:
every 10 ms frame a socket receives is a query to a C++ worker, addressed
to the same worker for as long as it lives, and the answer a group send
to the room.

## Backpressure and shutdown

The io thread hands messages to the loop and never waits for it. A
subscription handler that awaits slowly does not slow the client, it
only piles up tasks, and a loop that falls behind piles up the messages
it has not reached: the library bounds neither, so keeping up is the
application's. `messages()`, the pull form, has a bounded queue that
drops the oldest with a warning when nobody reads it. Size that queue for
the burst you expect, and prefer `subscribe` with a predicate to a
consumer that reads everything: the predicate runs on the io thread, and
a message it refuses never reaches the loop, though every predicate of the
message's type runs, holding the GIL, for every message of that type.

At shutdown, `await MX.aclose()` from an ASGI lifespan shutdown handler
writes what was sent first, a second at most, then closes. A worker that
exits with the client alive exits without a crash, but an `AsyncClient`
is never freed, so what it had not written yet is lost, unreported
([interpreter exit](../api_python.md#threads-exit-and-fork)).

## Testing the consumer

`multiplexer.testing` gives a real multiplexer and a scripted backend in
one process; the consumer's own test is an `IsolatedAsyncioTestCase`:

```python
class LessonConsumerTest(unittest.IsolatedAsyncioTestCase):
    async def asyncTearDown(self):
        await MX.aclose()  # the client is this test's loop's, which ends with the test

    async def test_a_message_is_answered(self):
        with Cluster(1) as cluster, FakePeer(cluster, peers.LESSON_SERVER) as backend:
            backend.reply_with(types.LESSON_REQUEST, b"done", types.LESSON_RESPONSE)
            settings.MULTIPLEXER_ADDRESSES = cluster.endpoints
            ...  # drive the consumer with Channels' communicator, assert on backend.received
```

Every test runs on a loop of its own, and the holder's client delivers on
the loop of its first use, so the tear-down closes it, a failed test's
too: the next test's first use makes a client on that test's loop. One
left over from an earlier test would raise at `subscribe()`.

[Tests that hold up under load](../api_python.md#tests-that-hold-up-under-load)
applies unchanged.
