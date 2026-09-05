# Using the Python library

Three modules matter: `multiplexer.clients` holds `Client` for clients,
`multiplexer.servers` holds `BaseMultiplexerServer` for backends, and
`multiplexer.multiplexer_constants` holds `peers` and `types`, generated
from the [rules file](rules.md) the build was pointed at. Depend on
`@mx//multiplexer:clients` or `@mx//multiplexer:servers`, and on
`@mx//multiplexer:multiplexer_constants`.

```python
from multiplexer.clients import Client
from multiplexer.servers import BaseMultiplexerServer
from multiplexer.multiplexer_constants import peers, types
```

The complete example is [examples/echo](../examples/echo).

## Messages

Every message is a `MultiplexerMessage`, the protocol buffer from
[Multiplexer.proto](../multiplexer/Multiplexer.proto). The fields you will
touch:

| Field | Meaning |
|---|---|
| `type` | the message type, a `types.*` constant |
| `message` | the payload, bytes; the multiplexer never reads it |
| `id` | random 64-bit id, set by the library; each attempt `query()` makes gets a new one, so the same request may reach a backend under different ids |
| `from_` | the sender's instance id, set by the library; `from` is a Python keyword, so the library adds this property |
| `to` | an instance id to deliver to directly, bypassing the rules; 0 means route by the rules |
| `references` | the id of the message this one answers; `query()` matches replies by it |
| `workflow` | opaque bytes copied from request to reply, for tracing |
| `report_delivery_error` | for directly addressed messages: ask for a `DELIVERY_ERROR` when the target is gone |

## Client

```python
client = Client([("10.0.0.1", 1980), ("10.0.0.2", 1980)], type=peers.ECHO_CLIENT)
```

Connects to every address, each with a 10 s timeout, and keeps the
connections. A connection that fails or drops is retried every 3 s, but only
while the library is running its loop, which for a client means inside calls.
The peer type must be marked `is_passive` in the rules file.

- `query(message, type, timeout=10)`: sends a request and returns the reply, a
  `MultiplexerMessage`. `message` is bytes, a `str` (encoded as UTF-8), or a
  protocol buffer message (serialized). The request goes through one
  connection; if it comes back as a delivery error, or nothing comes back
  within `timeout` seconds, the client asks every connection for a backend of
  the right type and repeats the request to the first one that answers. Each
  stage gets its own `timeout`, so a call can take up to three times that.
  Raises `OperationFailed` when no backend can be found, `OperationTimedOut`
  when a stage runs out of time, `NotConnected` when there is no live
  connection, and `BackendError` when the backend answered with
  `BACKEND_ERROR`, which the Python backend does when its handler raised.
  [How a query is answered](query.md) draws it.
- `send_message(message, type=..., to=0, multiplexer=Client.ONE, flush=False, timeout=10)`:
  queues an event and returns its message id. `multiplexer=Client.ONE` uses
  one connection; `Client.ALL` uses every connection, in which case the
  receivers drop the copies; a `ConnectionWrapper` from an earlier reply
  picks that connection. `flush=True` waits until the message reached the
  socket, every socket for `ALL`, and it is what makes an event survive a
  dead connection: the call notices, waits for the reconnect within
  `timeout`, and writes the event again, or uses another connection at
  once; without `flush` the message is only queued. Either way the loop
  runs before a connection is chosen, so a connection the multiplexer
  closed while the client sat idle is retired rather than written into,
  which would succeed and lose the message. Raises `NotConnected` when no connection could take the
  message and `OperationTimedOut` when a flush ran out of time. Extra
  keyword arguments become message fields, such as `to=` or `workflow=`.
  Nothing comes back for an event; a `DELIVERY_ERROR`, if you asked for
  one, arrives on the next call that reads. The lower-level
  `schedule_one()` and `schedule_all()` return the tracker instead, for
  code that wants to look at a queued message's state.
- `event(message, type=...)`: `send_message` through every connection.
- `query_pickle(data, type, timeout=10)` and `send_pickle(data, ...)`: the
  pickle convention, for Python peers talking to Python peers on a trusted
  network, since unpickling runs code. The payload is `pickle.dumps(data)`;
  `query_pickle` returns the reply's payload unpickled, `send_pickle` takes
  `send_message`'s keyword arguments and returns the id. The backend side
  is `MultiplexerServer`, or `parse_pickle()` and `send_pickle()` on any
  `BaseMultiplexerServer`.
- `receive_message(timeout=-1)`: waits for the next message and returns
  `(message, connection)`; `-1` waits forever. Raises `OperationTimedOut`.
- `instance_id`, `connections_count()`, `shutdown()`. After `shutdown()` the
  object is done.

A multiplexer restarting between two calls costs the next call the
reconnect delay, about 3 s, and nothing else: inside the call the client
notices the dead connection, waits for its reconnect timer, and sends
again. With several multiplexers it simply uses another one. Only when no
multiplexer comes back within `timeout` does the call raise
`NotConnected`.

`MxClient(peer_type, addresses)` in the same module holds one such client,
created on first `get()`, for programs that want a single place to keep it
without a module-level global; `addresses` may be a callable, so settings
can be read lazily. The client it returns is a synchronous `Client` and
belongs to one thread, the one that calls `get()` first; it is not for a
threaded server, where every request runs on its own thread, and it is not
fork-aware. Such a program holds one `ThreadedClient` per process instead;
[the web server recipe](recipes/web_server.md) shows how.

`Client.DEFAULT_TIMEOUT` is 10 s. The exception classes live in
`multiplexer.mxclient`: `NotConnected`, `OperationTimedOut` and
`OperationFailed`, all subclasses of `MultiplexerClientError`;
`BackendError` is in `multiplexer.clients`.

## BaseMultiplexerServer

```python
class Echo(BaseMultiplexerServer):
    def handle_message(self, mxmsg):
        self.send_message(message=mxmsg.message.upper(), type=types.ECHO_RESPONSE)


Echo([("10.0.0.1", 1980), ("10.0.0.2", 1980)], type=peers.ECHO_BACKEND).serve_forever()
```

The constructor connects to every address. `serve_forever(poll=1.0,
drain_seconds=0.0, stall_seconds=None)` runs the loop: each iteration waits
up to `poll` seconds for a message, answers the protocol's own messages
itself, calls `handle_message` with every other one, and then calls
`periodic_task()`, whether a message came or the poll timed out. It never
waits without a timeout, so anything checked from `periodic_task()` takes
effect within one poll. It returns, with the connections closed, when
`working` is cleared or a drain is over. `loop_iter(timeout)` is the single
step, for embedding in a loop of your own; it raises `OperationTimedOut`
after `timeout` seconds.

Override `periodic_task()` for work on the backend's own schedule, a
heartbeat to a monitor, a stale-connection check, and for noticing a
request to leave: a file a preStop hook wrote, a flag another thread set.
The default does nothing; gate the frequency inside it if the poll is
shorter than the work's period.

A backend may be built on one thread and served from another: the thread
that calls `serve_forever()` becomes its thread, and only that thread may
touch it from then on. In debug builds the library checks this and fails
an assertion on a call from another thread. A program that drives
`loop_iter()` itself from a thread other than the one that built the
backend calls `backend.conn.bind_to_current_thread()` first; the method is
also on `Client`. Destruction is exempt: a client still alive at interpreter
exit is destroyed on the main thread whichever thread drove it, and that
is fine as long as the driving thread is done with it.

Inside `handle_message`:

- `send_message(message=..., type=...)` sends a reply. By default it goes to
  the requester's instance id, references the request's id, copies its
  workflow, and uses the connection the request arrived on. Any of those can
  be overridden with `to=`, `references=`, `workflow=`, `multiplexer=`.
- `no_response()` says the message needed no reply, as for an event.
  Without a reply and without this call the library logs a warning.
- `notify_start()` sends `REQUEST_RECEIVED` to the requester at once, for
  handlers that take long; `query()` ignores it.
- `report_error(message)` sends `BACKEND_ERROR` instead of a reply; the
  requester's `query()` raises `BackendError`.
- `parse_message(SomeProto)` parses the payload as that protocol buffer type.
  It is the bound form of `mxclient.parse_message(SomeProto, payload_bytes)`,
  which any code may call on any message.
- `parse_pickle()` unpickles the payload and `send_pickle(data)` replies with
  `data` pickled as a `PICKLE_RESPONSE`, flushed: the backend side of the
  pickle convention, for trusted Python peers only.
- `mxmsg.id` identifies this delivery, not the request: a requester that
  sends again after a timeout or a lost connection uses a new id each time.
  Idempotency keys belong in the payload.

If `handle_message` raises, the traceback is printed, `BACKEND_ERROR` is sent
to the requester unless a reply already went out, so the requester fails at
once rather than timing out, and then `on_handler_exception(exc)` is
called. It returns `True` by default and the backend keeps serving; return
`False` and the original exception propagates out of `serve_forever()`, for
backends that would rather be restarted than continue. `close()` drops the
connections; `serve_forever()` calls it on the way out.

**Leaving gracefully.** From `periodic_task()`, call `start_draining()`
when asked to leave: the backend stops answering the search clients use to
find a backend, so no retried request is sent to it, and keeps serving the
requests the multiplexer still routes to it. `serve_forever()` returns once
`drained()`, by default `drain_seconds` after the drain started; override
`drained()` to wait for a condition of your own, keeping the deadline with
`super().drained() and ...` or not. Overriding
`should_respond_to_backend_for_packet_search()` puts any other condition
behind the search. Together with clients that retry through the search,
this makes a rolling restart of backends invisible;
[backend_drains](../tests/scenarios/backend_drains/README.md) and
[backend_drains_until_done](../tests/scenarios/backend_drains_until_done/README.md)
show both forms, and [examples/echo/backend.py](../examples/echo/backend.py)
watches a file for the request to leave.

The library installs no signal handlers, and a backend should not rely on
one either: a Python handler runs only when the main thread returns from
C++, and a C++ library in the same process can replace it. The
[FAQ](faq.md) has the details. A process with non-daemon threads still
needs its own exit after `serve_forever()` returns.

`stall_seconds` arms `faulthandler.dump_traceback_later` around every
iteration: an iteration that takes longer dumps every thread's stack to
`stall_file` or stderr, which finds a handler that hangs.

`MultiplexerServer` is a `BaseMultiplexerServer` that unpickles the payload,
calls `process_pickle(data)`, and replies with its return value through
`send_pickle()`. It is the backend end of the pickle convention that
`query_pickle()` on the clients is the other end of; only useful when both
ends are Python, and pickles from the network must be trusted.

## ThreadedClient

`multiplexer.threaded_client.ThreadedClient` is a client with an io thread
of its own. The thread runs all the time, so heartbeats and reconnects
happen while the program does other things, the peer type need not be
passive, and any thread may use it. Depend on
`@mx//multiplexer:threaded_client_py`.

```python
from multiplexer.threaded_client import ThreadedClient

client = ThreadedClient([("10.0.0.1", 1980), ("10.0.0.2", 1980)], type=peers.MY_SERVICE,
                        on_message=incoming.put)                          # a queue, a handler, or nothing
reply = client.query(b"hello", type=types.ECHO_REQUEST, timeout=10)      # blocking, from any thread
client.query(b"hello", type=types.ECHO_REQUEST, callback=on_reply)        # returns at once
client.send_message(b"payload", type=types.SOME_EVENT, multiplexer=ThreadedClient.ALL)
client.shutdown()
```

- `query(message, type, timeout=10, callback=None)` is the same three-stage
  algorithm as `Client.query()` and raises the same exceptions, plus
  `threaded_client.BackendError`. Any number of threads may call it at
  once; replies are matched to queries by the ids they reference, never by
  arrival order. With `callback` it returns `None` at once and calls
  `callback(result)` on the io thread with the reply or with the exception
  instance, the shape C++'s callback overload has. The blocking form raises
  `RuntimeError` when called from a callback, where it would block the io
  thread.
- `send_message(message, type=..., multiplexer=ONE|ALL, flush=False,
  timeout=10)` queues an event on one connection, or on all of them, and
  returns its message id at once; the io thread writes it right after, and
  a message that finds no live connection waits there for one within
  `timeout`. With `flush=True` it waits until the message reached the
  socket, resending through another connection if the first dies under it,
  and raises `OperationTimedOut` or `NotConnected`; the flushing form must
  not be called from a callback. The same call and the same result as on
  `Client`.
- `query_pickle(data, type, timeout, callback=None)` and
  `send_pickle(data, ...)`: the pickle convention, as on `Client`; with a
  callback, it gets the unpickled reply or the exception.
- `on_message(mxmsg)`, given at construction, runs on the io thread with
  every message that is not a reply to a query or one of the protocol's
  own: events and requests addressed to this peer. Pass `queue.put` to
  collect them for another thread. Without it such messages are logged
  and dropped, never queued for a reader that may never come. A late
  reply to a query that already ended, `REQUEST_RECEIVED` for a query no
  longer tracked and a `PING` (which the client answers itself) never
  reach it.
- Callbacks, `on_message` and `query`'s, hold the GIL on the io thread and
  must return quickly; they may call `query()` with a callback and
  `send_message()` without `flush`, but not the blocking `query()`, a
  flushing `send_message()` or `shutdown()`.
- `shutdown()` fails every query in flight, closes the connections and
  stops the thread. The client is not fork-safe: create it after forking.

The old two-client pattern, one client sending with `from` set to a
receiving client's id and a thread looping on the receiver, is what this
replaces. [examples/echo/workers.py](../examples/echo/workers.py) shows
several worker threads sharing one client, each taking its replies from
its own queue.

## Threads, exit and fork

A synchronous `Client` belongs to one thread; a `ThreadedClient` may be
used from any number of them. A backend built on one thread and served
from another is fine: `serve_forever()` adopts its thread. In debug builds
of the extension the rules are checked and a violation fails an
assertion.

**Interpreter exit.** A thread blocked in one of the library's waits when
the interpreter starts to exit, say a daemon thread in `read_message()`
while the main thread returns, never takes the GIL again: it parks. The
extension does this on every version; without it CPython 3.11 would end
such a thread with `pthread_exit` inside the extension's C++ frames, which
crashes. Two things follow. A daemon thread in a wait costs nothing at
exit. A non-daemon thread in `serve_forever()` keeps the interpreter alive
until something clears `working`, and `threading._shutdown` runs before
`atexit`, so a backend on a thread runs as a daemon or the program calls
`stop()` on it before exiting. The extension registers an `atexit` hook
that marks the exit; do not install another `_native` import path that
skips `multiplexer.mxclient`.

**Fork.** A client inherited by a forked child, from gunicorn's workers,
`multiprocessing`, or Django's parallel test runner, is an orphan there:
its io thread does not exist in the child, its locks may be held by
nobody, and its sockets are shared with the parent. Every call on it raises
`UsedAfterFork`, a `NotConnected`, before touching anything; dropping it
closes only the child's descriptor copies, never the parent's connection.
Create clients after forking, never at import: a module-level client made
by the parent before the fork is exactly what every worker inherits. The
detection is a fork generation counter maintained by a `pthread_atfork`
handler, so it covers forks the library never saw, at no cost when nobody
forks.

## Testing

`multiplexer.testing` (`@mx//multiplexer/testing`) is the infrastructure
this repository's own tests run on, importable from your workspace: real
multiplexers on ephemeral ports, peers in-process or as processes, and the
waits between them. A test needs `Cluster` and one of the peers:

```python
import unittest

from multiplexer.multiplexer_constants import peers, types
from multiplexer.testing import Cluster, FakePeer, TestClient


class SearchTest(unittest.TestCase):
    def test_search_goes_to_the_index(self):
        with Cluster(1) as cluster, FakePeer(cluster, peers.INDEX) as index, TestClient(cluster, peers.WEBSITE) as client:
            index.reply_with(types.SEARCH_REQUEST, b"3 hits", types.SEARCH_RESPONSE)
            reply = client.query(b"pears", types.SEARCH_REQUEST)
            self.assertEqual(b"3 hits", reply.message)
            self.assertEqual(b"pears", index.wait_for(types.SEARCH_REQUEST)[0].message)
```

- `Cluster(count, rules=None, record=False, record_payload_bytes=0)` starts
  `count` multiplexers on entering and stops every peer and multiplexer on
  leaving. `rules` defaults to the file `mx_integration_test` named, else
  the file the `multiplexer_rules` flag names, the one your constants come
  from, so `Cluster()` is right in any test of your workspace.
  `endpoints` is the list of `(host, port)` the clients take;
  `wait_for_peer(type_or_name, count=1, timeout=15)` blocks until every
  multiplexer lists that many peers of the type in its peers file, and
  `wait_for_peer_gone(type_or_name, timeout=15)` until none does; each
  `Mx` in `mx` has `connected_peers()`, `stop()`, `kill()`, `restart()`,
  `pause()`, `resume()` and, with `record=True`, `record_file`.
- `FakePeer(cluster, peer_type, name=None)` is a scripted backend on its own
  thread. `reply_with(request_type, payload, reply_type=None)` answers every
  request of that type with the payload as a reply of `reply_type`, the
  request's type by default; `on(request_type, handler, reply_type=None)`
  answers with what `handler(mxmsg)` returns, or nothing for `None`. It keeps
  every message it got in `received`, `messages(type, matching=None)`
  filters, and `wait_for(type, count=1, timeout=10, matching=None)` blocks
  for them; `matching` is a predicate on the `MultiplexerMessage`, so a
  wait names the message it means, `wait_for(types.SEARCH_REQUEST,
  matching=lambda m: m.message == b"pears")`. A type it has no script for
  is received and dropped. A handler that raises makes the
  requester get `BACKEND_ERROR` and makes `stop()` raise, so the test fails.
- `BackendThread(factory, poll=0.05, name=None)` serves a
  `BaseMultiplexerServer` of yours on its own thread: `factory()` builds it
  there, `start()` returns once it is connected, `stop()` asks it to leave
  and re-raises what serving raised; `backend` is the instance.
- `TestClient(cluster, peer_type)` sends and queries from the test:
  `send(payload, type, to=0, flush=True)` returns the message id, `query(payload,
  type, timeout=10)` returns the reply, `receive(timeout)` the next message
  addressed to it; `client` is the `clients.Client` underneath. Its peer type
  should be `is_passive`, as for every synchronous client.
- `wait_until(predicate, timeout, what)` polls until the predicate returns
  something true and returns it, or raises `TimeoutError` naming `what`.
- `spawn(role, lang, mx, type, **options)` runs a peer as a process and
  `RawPeer` speaks the wire format by hand; both are described in
  [tests/README.md](../tests/README.md), with `mx_integration_test`, the
  macro that runs a scenario against the shipped roles or a binary of yours.

Everything in-process uses the client library as your code does, so what a
test sees is what production sees, including the thread rules: a
`FakePeer` or `BackendThread` is served on its own thread, and a
`TestClient` belongs to the test's thread.

### Tests that hold up under load

A test that passes on a quiet machine and fails on a loaded one has
usually assumed something the protocol does not promise. What holds:

- **A request may reach a backend more than once.** A query whose reply
  does not come within its timeout is sent again, to a backend found by a
  search, and a late reply to the first attempt is dropped by the client.
  So the client sees one answer, but a backend may see two requests, and
  will under load. Count events, which are sent once, or the answers; do
  not assert an exact number of requests received, or give the request a
  key and count keys ([guarantees](guarantees.md)).
- **Wait for the thing itself.** `FakePeer.wait_for(type, matching=...)`,
  `wait_until(predicate, timeout, what)` and `Cluster.wait_for_peer()`
  block until what the test needs has happened; a wait on a total, or a
  `sleep`, is a guess about ordering that load will falsify.
- **Timeouts are for failures.** A wait for something that must happen
  gets the defaults, ten seconds and more, since only a real fault reaches
  them; a short timeout belongs to a step that is expected to time out,
  which load cannot make pass. Never shorten a success wait to make a test
  quick: a passing test costs no waiting at all.
- **Measured tests measure the machine.** The scenarios that check CPU
  time, latency or memory are marked and kept apart; a test of yours that
  asserts on a duration will follow the load of the machine that runs it.

This repository's own suite is run under full load before a change lands,
forty copies of a test at once with every core busy, because that is what
a shared build machine looks like.

## Recording

`multiplexer.recording.read(path)` yields the `Record` messages of a file
written by `run_multiplexer --record`, checking that the recording's rules
match the generated constants (`check_rules=False` to skip);
`describe(record)` renders one with names, `involves_peer(record, id)`
filters by instance id. A `Cluster(record=True)` records every multiplexer;
[operations](operations.md#recording) describes the file.

## Lower level

`multiplexer.mxclient.Client` is what `clients.Client` wraps; it adds nothing
you need but exposes `send_and_receive()` for one request without the search,
`receive()` to wait for replies to given ids, `flush_all(timeout)` to push
every queued message out, and `new_message(**fields)` to build a
`MultiplexerMessage` with id and sender filled in.
