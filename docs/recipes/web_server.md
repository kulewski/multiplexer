# Use the client from a threaded web server

A web server such as Django under gunicorn or the development server runs
every request on its own thread, forks workers after importing the
application, and reloads or recycles processes while requests are in
flight. The synchronous `Client` fits none of that: it belongs to one
thread and is not fork-aware. `ThreadedClient` fits all of it.

## One client per process, created on first use

```python
# mx.py
import os
import threading

from multiplexer.multiplexer_constants import peers
from multiplexer.threaded_client import ThreadedClient

_client: ThreadedClient | None = None
_lock = threading.Lock()


def _forget_in_child() -> None:
    """The child of a fork must not touch the parent's client."""
    global _client
    _client = None


os.register_at_fork(after_in_child=_forget_in_child)


def mx() -> ThreadedClient:
    """The process's client, made on the first call in this process."""
    global _client
    with _lock:
        if _client is None:
            _client = ThreadedClient(settings.MULTIPLEXER_ADDRESSES, type=peers.WEBSITE)
        return _client
```

Never create the client at import time: the gunicorn master and the test
runner's parent import the application before forking, and a client made
there is exactly what every worker inherits. An inherited client is an
orphan in the child, every call on it raises `UsedAfterFork`, and the hook
above makes sure the child never even sees it. The `pthread_atfork` handler
inside the library covers forks the hook does not, such as a C library
forking.

## Queries from request threads

```python
reply = mx().query(payload, type=types.SEARCH_REQUEST, timeout=5)
```

Any number of requests may query at once; replies are matched by id.
`query()` raises `OperationTimedOut`, `OperationFailed`, `NotConnected` or
`BackendError`, which the view turns into a response. Events go through
`mx().send_message(payload, type=...)`, which returns at once; add
`flush=True` when the request must not complete before the event is on
the wire.

## Events addressed to the server, without a thread

A peer type that events are routed to receives them on the client's own
io thread through `on_message`, so no receiver thread exists to stop at
exit or to be killed on reload:

```python
_client = ThreadedClient(settings.MULTIPLEXER_ADDRESSES, type=peers.WEBSITE, on_message=handle_event)
```

`handle_event(mxmsg)` runs on the io thread with the GIL and must return
quickly: put the message on a queue or start a task, do not block, and do
not call the blocking `query()` from it (pass a `callback` there).

## Exit and reload

The client's io thread is a C++ thread, not a Python one: it never keeps
the interpreter alive, and the client's destructor joins it. A request
thread blocked in `query()` when the interpreter exits gets a shut-down
error, or parks, never a crash. A backend built on `BaseMultiplexerServer`
that must run inside the same process runs its `serve_forever()` on a
daemon thread, or the program calls `stop()` on it before exiting; a
non-daemon thread in `serve_forever()` keeps a gunicorn worker alive at
recycle until the graceful timeout kills it.

## The peer type

`WEBSITE` above may be passive or not. `ThreadedClient` sends heartbeats
either way; a non-passive type additionally lets the multiplexer drop a
hung worker after the heartbeat timeout instead of routing to it forever.
