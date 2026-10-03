# Using the Python library

Three modules matter: `multiplexer.clients` holds `SyncClient`, also
importable as `Client`, its name up to 2.3.1; `multiplexer.servers` holds
`BaseMultiplexerServer`, one class a backend can be built on; and
`multiplexer.multiplexer_constants` holds `peers` and `types`, generated
from the [rules file](rules.md) the build was pointed at. Depend on `@mx//multiplexer:clients` or
`@mx//multiplexer:servers`, and on `@mx//multiplexer:multiplexer_constants`.
Outside Bazel, after `pip install mx-multiplexer`, which also installs
the `mxcontrol` command, the constants of your rules file come from
`mxcontrol generate_constants your.rules --python multiplexer_constants.py
--pyi multiplexer_constants.pyi`, a module to
import from wherever it is written
([mxcontrol.md](mxcontrol.md#generate_constants)); the package's own
`multiplexer.multiplexer_constants` holds the system rules' constants, which
the library uses itself.
`multiplexer.threaded_client`, `multiplexer.aio` and
`multiplexer.threaded_server` hold `ThreadedClient`, `AsyncClient` and
`BaseThreadedMultiplexerServer`, each described below;
[which class to build on](README.md#which-class-to-build-on) is the
one-table answer.

```python
from multiplexer.clients import SyncClient
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

## Timeouts

Every timeout is seconds as a float, in every client and server class:
`timeout=` on a connect, a read, a query, a send, `flush_all()`,
`shutdown()` and `close()`, and `serve_forever()`'s `poll`,
`drain_seconds` and `stall_seconds`; the recording calls' `timeout=` too.
A negative one sets no deadline, exactly as `math.inf` does: the call
waits as long as it takes, and a message sent with it waits for a
connection or for room as long as that takes. So `shutdown(timeout=-1)`
and `close(timeout=-1)` write everything sent before them first, however
long that takes; with `poll=-1` a `BaseMultiplexerServer` waits for a
message before it calls `periodic_task()` or sees `working` cleared, and
a `BaseThreadedMultiplexerServer` until a drain, `stop()` or `close()`
wakes it; `drain_seconds=-1` gives a drain no cap, so only the
multiplexers' confirmation ends it, or a stop; and `stall_seconds=-1`
arms no dump.

0 and NaN mean "don't wait", in every class: a call does what it can at
once and gives up, a read taking what has arrived, a connect starting
the attempt. A message sent with one is placed now on a connection with
room, each copy for `ALL` likewise, and otherwise dropped and reported at
once (`NO_ROOM`, or `NO_CONNECTION` with no connection live), never held;
a flushing send's message waits, as every flushing send's does, its
call's deadline and `ROOM_GRACE_SECONDS` more, so that the call ends as a
timeout first. So a flushing send given 0 reports its message not
written, though a connection may write it a moment later: to send
without waiting and learn how the message ended, send without `flush`,
with `callback=`. `shutdown(0)` and `close(0)` drop what is unwritten at
once, `drain_seconds` of 0 or NaN ends a drain as it begins,
`stall_seconds` of 0 or NaN arms no dump, and a `poll` of 0 or NaN is a
loop that never waits.

On a `SyncClient` with nothing connected and nothing on its way, never
connected, made with no address or shut down, a wait for a write gives
up at once, whatever its timeout, as a read raises `NotConnected` then:
`flush_all()` returns `False`, and a flushing send or a query raises
`NotConnected`, the send's message dropped. A
`ThreadedClient` or `AsyncClient` waits on, as another thread may connect
it meanwhile. [Defaults](semantics.md#defaults) says what `math.inf`
does, and the default of each.

## SyncClient

```python
client = SyncClient([("10.0.0.1", 1980), ("10.0.0.2", 1980)], type=peers.ECHO_CLIENT)
```

Named `Client` up to 2.3.1; `Client` is still the same class. Connects to
every address, each with a 10 s timeout, and keeps the
connections. A host is an address or a name; a name is resolved by the
library on every attempt, each address it has tried in turn, so a
multiplexer that moved is found at the next reconnect, and a name that
does not resolve yet is retried like a port that refuses. A connection that
fails or drops is retried every 3 s, but only while the library is running
its loop, which for a client means inside calls. A call that waits, the
connect in the constructor or `connect_to()`, a read, a query, a flush,
`flush_all()`, a send waiting for a connection, `shutdown()`, runs the loop
without the GIL, so the program's other threads go on meanwhile. `on_drop=` and `dropped` are under [Messages the
library gives up on](#messages-the-library-gives-up-on).
The peer type must be marked `is_passive` in the rules file: this is the
one class that needs the mark, since nothing heartbeats between its calls;
`ThreadedClient`, `AsyncClient` and both server classes run the loop all
the time and their peer types are ordinary ones.

- `query(message, type=None, timeout=10, to=0, multiplexer=SyncClient.ONE, with_connection=False, on_received=None)`:
  sends a request and returns the reply, a
  `MultiplexerMessage`. `message` is bytes, a `str` (encoded as UTF-8), or a
  protocol buffer message (serialized), with `type`, which it needs, and
  `to`; or a whole `MultiplexerMessage`, the request itself, typed by its
  own `type` and addressed by its own `to`, its empty `from` filled in and
  each attempt with an id of its own, so that one message may be queried
  again and again, `type=` or `to=` beside it a `TypeError`. The request goes through one
  connection; if it comes back as a delivery error, its connection is
  lost, or nothing comes back within `timeout` seconds, the client asks
  every connection for a backend of the right type and repeats the request
  to the first one that answers, once: the request goes out at most twice,
  and the query never goes back a stage. Each stage gets its own
  `timeout`, so a call can take up to three times that.
  When the search finds nobody, or the backend that answered is gone by
  the repeat, the query fails at once if nobody took the request, and
  otherwise, after a timeout or a lost connection, waits out the stage
  for a late reply from a backend that did, a reply to either attempt
  being accepted at every stage
  ([every way a stage ends](query.md#every-way-a-stage-ends)).
  Raises `OperationFailed` when no backend can be found, `OperationTimedOut`
  when a stage runs out of time, `NotConnected` when there is no live
  connection, and `BackendError` when the backend answered with
  `BACKEND_ERROR`, which the Python server classes send when a handler
  raised. [How a query is answered](query.md) draws it. With `to`, the instance
  id of one peer, the query is addressed: only that peer ever gets it,
  located again with a `PING` when a multiplexer no longer has it, and one
  `timeout` covers the stages; `multiplexer` and `with_connection` are for
  lanes and pinning. All three are described under
  [Lanes, pinning and addressed queries](#lanes-pinning-and-addressed-queries).
  `on_received` hears which backend acknowledged the request, see
  [Knowing a backend took the request](#knowing-a-backend-took-the-request).
- `send_message(message, type=..., to=0, multiplexer=SyncClient.ONE,
  flush=False, timeout=10, callback=None)`: sends an event and returns its
  message id, as every client sends. `message` is a payload, wrapped with
  the keyword arguments, or a whole `MultiplexerMessage`, sent as it is,
  whose empty `id` and `from` are filled in as `new_message()` fills them:
  every receiver drops a message without an id. Message fields beside a
  whole message, `type=`, `to=` and the like, are a `TypeError`.
  `multiplexer=SyncClient.ONE` uses one
  connection; `SyncClient.ALL` uses every connection, in which case the
  receivers drop the copies; a `ConnectionWrapper` from an earlier reply
  prefers that connection, and a `Lane` from `lane()` keeps a stream on one,
  see below. Without `flush` the call returns at once: the message is
  queued, or waits for room on a full connection (1024 queued), or, with no
  connection live, is held until one comes up, in order, `timeout` seconds
  at most each way; it is written inside the call on an idle connection,
  otherwise as a later call runs the loop, the only time a `SyncClient` runs
  it. A connection that dies with the message unwritten hands it, with
  the rest it had not written, in order, to one other, or has it held for
  the next; a copy for `ALL` is held too when no
  connection is live, and dropped while one is, so a flushing send to `ALL`
  whose copies all went with their connections raises `NotConnected`, even
  with a connection that came up since. One the client gives up on is
  reported ([Messages the library gives up
  on](#messages-the-library-gives-up-on)). `flush=True` waits until the
  message reached the socket, the first copy for `ALL`, within `timeout`,
  and raises `NotConnected` when nothing wrote it with no connection live,
  else `OperationTimedOut`. Written means the kernel's buffer
  ([semantics](semantics.md)). With `callback`, flushing or not, the
  call returns at once and `callback(written)` runs once, with the GIL,
  inside a later call that runs the loop: 1 when the message was written,
  the first copy for `ALL`, 0 when it was given up on or `shutdown()` came
  first. `flush_all(timeout)` runs the loop until everything sent before it
  was written or given up on, and returns once the callbacks of those sends
  have run: `True` when every one was written, `False` when one was given up
  on, which `on_drop` names, or `timeout` passed first; [Sending](#sending)
  sets the calls side by side. Either way the loop runs before a connection
  is chosen, so a connection the multiplexer closed while the client sat
  idle is retired rather than written into, which would succeed and lose the
  message. That look reads a queue's worth of frames at most, so that a
  multiplexer sending faster than the client reads cannot hold the call
  there; a closure behind more unread frames is noticed by a later call.
  `NotConnected` at once for a pinned lane whose connection is
  gone, and after `shutdown()`. Extra keyword arguments become message
  fields, such as `to=` or `workflow=`. Nothing comes back for an event; a
  `DELIVERY_ERROR`, if you asked for one, arrives on the next call that
  reads, `read_message()` say. A `query()` that waits meanwhile reads it
  too: every message that is not the reply it waits for goes to
  `handle_drop(mxmsg, connection)`, which logs it and drops it, unless a
  subclass overrides it to keep such messages. To learn how a
  message ended without waiting for it, pass `callback=`: not called yet
  means still on its way.
- `event(message, type=...)`: `send_message` through every connection.
- `query_pickle(data, type, timeout=10)` and `send_pickle(data, ...)`: the
  pickle convention, for Python peers talking to Python peers on a trusted
  network, since unpickling runs code. The payload is `pickle.dumps(data)`;
  `query_pickle` returns the reply's payload unpickled, and takes
  `query()`'s keyword arguments but `with_connection` (`TypeError`: the
  result is the payload alone); `send_pickle` takes `send_message`'s
  keyword arguments and returns the id. The backend side
  is `MultiplexerServer`, or `parse_pickle()` and `send_pickle()` on any
  `BaseMultiplexerServer`.
- `receive_message(timeout=-1)`: waits for the next message, with no
  deadline by default, and returns `(message, connection)`. Raises
  `OperationTimedOut`, or `NotConnected` at once when nothing could
  arrive: no connection and none on its way, on a client never
  connected, made with no address, or shut down.
- `set_routing(routing)` and `routing_acknowledged()`: which of a
  multiplexer's routing paths reach this peer, a `Routing` from
  `multiplexer.Multiplexer_pb2` with `any`, `all` and `last_resort`, told
  to every multiplexer and carried in every welcome from then on, and
  whether every one has it in effect; what a backend's drain uses, see
  [How a backend leaves](leaving.md#what-a-draining-backend-still-takes).
- `refuse_arrivals()` and `refuse_unread()`: how the server classes
  leave. From the first on, a request that arrives is refused at once with
  the `DELIVERY_ERROR` a multiplexer sends for a peer that is gone, so
  that its sender retries elsewhere, and a reply is dropped; what was read
  before stays to be received. The second refuses what was read and not
  received yet.
- `dropped_while_closing()`: the messages the client's connections read
  after they began closing, which they could only drop, each connection's
  logged as a `WARNING` when it ends; the protocol's own answers to what
  the client sent are not counted. Kept after `shutdown()`, so that a
  server's close says what it dropped, as in C++.
- `connect((host, port), timeout=10)` connects to one more multiplexer as
  the constructor does and returns its `ConnectionWrapper`, live or not:
  the library goes on trying. A multiplexer the client has a connection
  to, live or on its way, keeps it: connecting to it again returns that
  connection. `disconnect((host, port))` drops one given
  to the constructor or `connect()`, the same pair, an address in any
  spelling: its reconnect stops, and nothing connects to it again unless
  `connect()` is called again. A live connection to it is closed the
  polite way, as `shutdown()` closes each: what it had not written goes, in
  order, to one other connection, or is held for the next, as a lost connection's
  does ([semantics](semantics.md#failure-modes)), and what it wrote still
  arrives. It returns at once whether the client had that multiplexer, a
  connection to it or a reconnect armed, and raises `NotConnected` after
  `shutdown()` and `UsedAfterFork` in a forked child, as `connect()` does.
  It is for a program that keeps its own list of multiplexers, when one
  leaves the list: the library would retry an address nobody serves any
  more for good, every 3 s, until another deployment's multiplexer gets
  that address, as pod addresses are reused, and the client joins it.
- `instance_id`, `connections_count()`, `orphaned()`, as on
  `ThreadedClient`, `shutdown(timeout=1)`.
  `shutdown()` first writes what was sent before it, running the loop as
  `flush_all()` does, `timeout` seconds at most (`CLOSE_FLUSH_SECONDS`,
  from `multiplexer.mxclient`), and what is still unwritten then is
  dropped and reported, at once with `timeout=0`; every client and server
  class ends this way. It returns once every multiplexer has closed its
  side of the connection too, a round trip, a second at most, so that
  what was written arrives ([semantics](semantics.md#failure-modes));
  after it the object is done: `connect()` and `disconnect()` raise
  `NotConnected`, as on `ThreadedClient`, and nothing is sent.
  `with SyncClient(...) as client:` shuts it down at the end of the block
  ([lifetimes](#lifetimes)). The client, when freed, waits for a name
  lookup in progress, for an address given by name, which runs on a
  thread of the library's and ends with the client: nothing cuts it short,
  so against a slow or unreachable DNS server that is up to the resolver's
  own timeout, on the thread that dropped the client, and without the
  GIL, so that the program's other threads go on. A client given
  addresses never waits so. A server class's client is freed with its
  server.

A multiplexer restarting between two calls costs nothing when the client
is connected to others: the next call uses another one. With a single
multiplexer the next call notices the dead connection before it writes: a
send holds its message until the reconnect, about 3 s later, and a query
waits for that inside the call and sends its request then; it is answered
if the backend is back on the fresh multiplexer by then, and raises `OperationFailed` if
the client reconnected first, since a multiplexer with nobody of the type
reports a delivery error ([semantics](semantics.md#failure-modes)). Only
when no multiplexer comes back within `timeout` does a query or a flushing
send raise `NotConnected`; a send without `flush` has its message dropped
then and reported ([Messages the library gives up
on](#messages-the-library-gives-up-on)).

`MxClient(peer_type, addresses)` in the same module holds one such client,
created on first `get()`, for programs that want a single place to keep it
without a module-level global; `addresses` may be a callable, so settings
can be read lazily. What it returns is a `SyncClient`, which belongs to
one thread, the one that calls `get()` first; it is not for a
threaded server, where every request runs on its own thread, and MxClient
keeps it across a fork, where every call on it raises `UsedAfterFork`.
Such a program holds one `ThreadedClient` per process instead;
[the web server recipe](recipes/web_server.md) shows how.

`SyncClient.DEFAULT_TIMEOUT` is 10 s. The exception classes live in
`multiplexer.mxclient`: `NotConnected`, `OperationTimedOut` and
`OperationFailed`, all subclasses of `MultiplexerClientError`;
`BackendError` is in `multiplexer.clients`, the one class every client
raises for a `BACKEND_ERROR` reply, and `multiplexer.threaded_client`
names it too.

### Lanes, pinning and addressed queries

The same on `SyncClient`, `ThreadedClient` and `AsyncClient`. A message has
two coordinates the caller may fix: the peer it is for and the path it
takes.

**The peer: `to=`.** `query(..., to=instance_id)` is an addressed query:
the request carries `to`, and only that peer ever gets it. When a
multiplexer reports the peer is not behind it, or the connection dies
under the wait, the client locates the peer with a `PING` addressed to it
on every connection and repeats the request through the connection that
found it, once; a peer nobody has is `OperationFailed`, at once unless
the request may still be with the peer, its connection lost under it,
when the query waits for that late reply until its timeout, and never a
detour to another instance of its type; one `timeout` covers the three
stages, and a request that gets no answer at all within it is `OperationTimedOut`
without a `PING`, since a silent peer is one the multiplexer still has.
The `PING` reaches the instance whatever its routing, as every addressed
message does, so a request addressed to a draining backend lands on it
and is served, and the server classes, `ThreadedClient` and `AsyncClient`
all answer it whatever their search policy, so a backend that declines
searches, saturated say, is found too, and so is a peer that serves no
requests at all; `SyncClient` does not answer it.
The instance id comes from a reply, `reply.from_`, or from the peer
itself, `instance_id`. [How a query is answered](query.md#an-addressed-query)
draws the stages.

**The path: `multiplexer=`.** Two pins, a soft one and a hard one, both
a `Lane`, the small object `client.lane()` returns and the caller owns:
`multiplexer=lane` on `send_message()` and `query()` sends through the
lane's connection. A lane is a soft, late pin: empty until its first
message, which pins it to the connection the library chose; every later
one follows, so a stream of events arrives in order, and a query
through the lane leaves it on the connection the reply came through, so
the events after a request follow the request, and a message its full
connection cannot take waits there for room. When the connection dies,
what it had not written moves, in order, to one other connection, and
the lane follows it there: a gap or a reorder at the failover and no
other, one more for each further failover; a sequencer on the receiving
side is for that.
`lane(pinned=True)` is the hard pin: once its connection is gone, every
send and query through it raises `NotConnected`, and `lane.closed` says
so, until the caller makes a new lane; a flushing send through it that
runs out of time while its connection lives raises `OperationTimedOut`,
as through any lane, on every client. `lane.closed` reads true from the
moment the connection stops being live, from any thread. A
`ThreadedClient` or `AsyncClient` send looks at it on the calling thread,
and the io thread places the message a moment later: one whose
connection dies in between is dropped and reported (`CONNECTION_LOST`, a
callback hearing 0) rather than refused. A lane, once it took a
connection, and a `ConnectionWrapper` belong to the client they came
from: given to another client, a send or a query raises `ValueError`. A pinned lane is the
guarantee that everything through it went through one multiplexer, down
to the messages a dying connection had not written yet, which are
reported lost rather than handed to another connection, as a message
that is neither pinned nor a copy sent to `ALL` is. `lane(connection=c)` seeds a
lane with a connection a reply came through, pinned or not. A lane's
`connection` is the `ConnectionWrapper` it holds, empty until its first
message went through, and `pinned`, `holds_connection`, `connected` and
`closed` read its state. A `ConnectionWrapper` is false once its
connection is gone; its `endpoint`, the multiplexer's `(host, port)`,
the address a name resolved to in the one `connect()` returns once
connected, stays, which is how a program or a test tells which multiplexer a
stream took, and `is_same_connection(other)` says whether another
`ConnectionWrapper` names the same connection, or the same multiplexer
once the connection is gone. A lane holds
its connection weakly and the library keeps no registry of lanes, so a
lane lives as long as your reference and keeps nothing alive; a query in
flight holds it until it ends. `multiplexer=connection`, a
`ConnectionWrapper`, prefers that connection for one message and uses
another when it is gone, which is how a backend's reply goes back the way
the request came; `query(..., with_connection=True)` returns `(reply,
connection)` so that a later message can go the same way. A lane never
carries `to`: keep the peer id beside it.

| | the peer | the path |
|---|---|---|
| hard pin, fails loudly | `to=instance_id`, `OperationFailed` when the instance is gone | `lane(pinned=True)`, `NotConnected` when its connection is gone |
| soft pin, follows a move | the locate phase of an addressed query | `lane()`, pinned late to its first connection, taking the next when it dies |
| preferred only | | `multiplexer=connection` |
| where the value comes from | `reply.from_`, `peer.instance_id` | `with_connection=True`, or the lane a query updated |

`multiplexer=` on `query()` takes `ONE`, a lane or a connection, never
`ALL`. The C++ forms are in [the C++ API](api_cpp.md#lanes-pinning-and-addressed-queries).

### Knowing a backend took the request

The same on `SyncClient`, `ThreadedClient` and `AsyncClient`. A backend
whose handler takes long calls `notify_start()` first, which sends the
requester a `REQUEST_RECEIVED` that references the request. A query goes
on waiting for the reply whatever arrives; `on_received=callback` also
calls `callback(backend)` with the instance id of the backend that
acknowledged the request, as soon as the acknowledgement arrives:

```python
def taken(backend: int) -> None:
    print("backend %d is working on it" % backend)

reply = client.query(b"pears", types.SEARCH_REQUEST, timeout=60, on_received=taken)
```

Normally it is called once. A retry, the direct request after a search,
which follows a delivery error, a timeout or a lost connection, may reach a
backend again, the same one or another, which acknowledges it too: the
callback is called again with that backend's id, which tells the caller
the request may be running twice ([what a retry means](semantics.md)). Nothing else changes: the
timeouts, the retries, the search and the result are those of a query
without the callback, and a query without one costs nothing more, as
every client reads the acknowledgement anyway to skip it. Where it runs:
on `SyncClient`, on the calling thread inside `query()`, while the query
waits, so it must not query or receive through that client, which could
take the reply; on `ThreadedClient`, on the io thread, as every callback there,
so it must be quick; on `AsyncClient`, on the loop that awaits the
query, before the await resumes. One that raises has its traceback
printed, and the query goes on. Only a backend that calls
`notify_start()` is heard of: a callback that was never called says
nothing about whether a backend has the request. The C++ form is in
[the C++ API](api_cpp.md#knowing-a-backend-took-the-request).

## BaseMultiplexerServer

```python
class Echo(BaseMultiplexerServer):
    def handle_message(self, mxmsg):
        self.send_message(message=mxmsg.message.upper(), type=types.ECHO_RESPONSE)


Echo([("10.0.0.1", 1980), ("10.0.0.2", 1980)], type=peers.ECHO_BACKEND).serve_forever()
```

The constructor, `BaseMultiplexerServer(addresses, type=None,
drain_routing=None)`, makes the instance id; `type` may instead be the
class attribute `multiplexer_client_type`. `connect()` connects to every
address, once: it starts every connection at once and returns when each
has its handshake done or has failed, 10 s at most in all, so a
multiplexer that drops the connect or never answers holds up the others'
requests that long once, not once for every address after it. It raises
nothing for an address it could not reach, which the library goes on
trying. `serve_forever()` starts the connections itself unless `connect()`
did, waiting for none: its loop finishes the handshakes, so the backend
serves what one multiplexer routes to it while another has not welcomed
it yet, a multiplexer that never answers holds up nothing, and no
multiplexer knows the backend before it can serve. A program that only
constructs and serves never calls `connect()`. Call it yourself when something waits for a
line you print before it sends, a test that reads `ready` from your
stdout or a notebook that greps your log, so that the line means
reachable: the echo backend does. Call it before you start
`serve_forever()` on a thread of your own and send at once, as a test
does: until that thread has connected, the first request finds no
backend and fails with `OperationFailed`. Call it too when you drive
`loop_iter()` yourself instead of `serve_forever()`, and in a test that
wants a backend connected without a thread serving it. A second call
does nothing. `serve_forever(poll=1.0,
drain_seconds=0.0, stall_seconds=None)` runs the loop: each iteration waits
up to `poll` seconds for a message, answers the protocol's own messages
itself, calls `handle_message` with every other one, a `BACKEND_ERROR`
someone sent this backend included, and then calls
`periodic_task()`, whether a message came or the poll timed out. Each wait
ends within `poll`, so anything checked from `periodic_task()` takes
effect within one poll, unless `poll` sets no deadline
([timeouts](#timeouts)). It returns, with the connections closed, when
`working` is cleared or a drain is over. `loop_iter(timeout)` is the single
step, for embedding in a loop of your own; it raises `OperationTimedOut`
after `timeout` seconds.

Override `periodic_task()` for work on the backend's own schedule, a
heartbeat to a monitor, a stale-connection check, and for noticing a
request to leave: a file a preStop hook wrote, a flag another thread set.
The default does nothing; gate the frequency inside it if the poll is
shorter than the work's period.

A `BaseMultiplexerServer` may be made on one thread and served from
another: the thread that calls `serve_forever()` becomes its thread, and
only that thread may touch it from then on. In debug builds the library checks this and fails
an assertion on a call from another thread. A program that drives
`loop_iter()` itself from a thread other than the one that built the
backend calls `backend.conn.bind_to_current_thread()` and then
`backend.connect()` first; the former is also on `SyncClient`. Destruction is exempt: a client still alive at interpreter
exit is destroyed on the main thread whichever thread drove it, and that
is fine as long as the driving thread is done with it.

Inside `handle_message`:

- `send_message(message=..., type=...)` sends a reply. By default it goes to
  the requester's instance id, references the request's id, copies its
  workflow, and uses the connection the request arrived on. Any of those can
  be overridden with `to=`, `references=`, `workflow=`, `multiplexer=`. A
  whole `MultiplexerMessage` as `message` gets those of its fields that are
  empty filled in so, as `request.reply()` does in the threaded server;
  message fields beside it are a `TypeError`.
  Outside `handle_message`, from `periodic_task()` say, there are no such
  defaults: the message is routed by its type like any client's. A reply
  is queued, as every send without `flush` is, and the loop writes it
  right after; every reply this class sends itself is too, the
  `BACKEND_ERROR` of `report_error()`, a pickle reply and the echo of a
  search or a `PING`, as in the C++ class.
- `no_response()` says the message needed no reply, as for an event.
  Without a reply and without this call the library logs a warning.
- `notify_start()` sends `REQUEST_RECEIVED` to the requester at once, for
  handlers that take long; the requester's `query()` goes on waiting for
  the reply, and its `on_received`, when given,
  [hears which backend took the request](#knowing-a-backend-took-the-request).
- `report_error(message)` sends `BACKEND_ERROR` instead of a reply; the
  requester's `query()` raises `BackendError`. A `str` goes as UTF-8, any
  character UTF-8 cannot carry, a lone surrogate echoed from a request
  say, escaped as `\ud800`, so that the report is never lost to its text;
  the threaded server's `request.report_error()` does the same.
- `parse_message(SomeProto)` parses the payload as that protocol buffer type.
  It is the bound form of `mxclient.parse_message(SomeProto, payload_bytes)`,
  which any code may call on any message.
- `parse_pickle()` unpickles the payload and `send_pickle(data)` replies with
  `data` pickled as a `PICKLE_RESPONSE`: the backend side of the
  pickle convention, for trusted Python peers only.
- `mxmsg.id` identifies this delivery, not the request: a requester that
  sends again, to the backend its search found, uses a new id.
  Idempotency keys belong in the payload.

If `handle_message` raises, the traceback is printed, `BACKEND_ERROR` is sent
to the requester unless a reply already went out, so the requester fails at
once rather than timing out, and then `on_handler_exception(exc)` is
called. A message that answers another, one with `references` set, a
reply or someone's `BACKEND_ERROR`, gets no report: nobody waits for an
answer to it, and two backends whose handlers raise on what they do not
expect would answer each other's reports for good. The library never
answers such a message on its own; a handler may. A reply that raised did not go out; a report that fails is logged,
the requester waiting out its timeout, and `on_handler_exception(exc)` is
called all the same. It returns `True` by default and the backend keeps serving; return
`False` and the original exception propagates out of `serve_forever()`, for
backends that would rather be restarted than continue, an
`OperationTimedOut` of the handler's own too: only `poll` running out is
the loop's timeout. So does an exception
`on_handler_exception()` raises, and a `BaseException` that is not an
`Exception`, a handler's `SystemExit` say, which none of this catches. `close(timeout=1)`
ends the connections as `SyncClient.shutdown(timeout)` does, what is still
queued, the last replies, written first; a request that arrives meanwhile,
or was read and will not be handled, is refused with `DELIVERY_ERROR`, so
that its sender retries elsewhere at once. `serve_forever()` calls it on the
way out, and a `with` block at its end ([lifetimes](#lifetimes)).

**Leaving gracefully.** From `periodic_task()`, call `start_draining()`
when asked to leave: the backend tells every multiplexer to route it
nothing new by the rules, its `drain_routing`, so no request and no
search is sent to it, and serves what was already on its way.
`serve_forever()` returns once `drained()`, having served what it had
read by then and refused what arrived later, however fast it comes: by
default once every multiplexer confirmed, so nothing more is coming, or
`drain_seconds` after the drain started at the latest; override `drained()` to wait for
a condition of your own, keeping the deadline with `super().drained()
and ...` or not. `drain_routing`, a constructor argument, is what the
drain asks for: `Routing(any=False)` keeps events coming,
`Routing(any=False, all=False, last_resort=True)` keeps a lone backend
serving through its drain, and either makes the drain last its
`drain_seconds`. [How a backend leaves](leaving.md) draws the phases and
what each costs a caller. Overriding
`should_respond_to_backend_for_packet_search()` puts a condition of your
own behind the search; a drain needs none. Together with clients that
retry through the search, this makes a rolling restart of backends
invisible;
[backend_drains](../tests/scenarios/backend_drains/README.md) and
[backend_drains_until_done](../tests/scenarios/backend_drains_until_done/README.md)
show both forms, and [examples/echo/backend.py](../examples/echo/backend.py)
watches a file for the request to leave.

The library installs no signal handlers. A backend that must leave on
`SIGTERM`, as an orchestrator asks, may install one that only sets a flag,
which `periodic_task()` reads and answers with `start_draining()`: a
Python handler runs only when the main thread returns from C++, which
`serve_forever()` does at least every `poll`, so the flag is seen within
one. The handler must not call the library itself, not even
`start_draining()`, since it can interrupt the main thread inside the
library's own locks, and a C++ library in the same process can replace
it; the drain file needs neither. The [FAQ](faq.md) has the details. A
process with non-daemon threads still needs its own exit after
`serve_forever()` returns.

`stall_seconds` arms `faulthandler.dump_traceback_later` around every
iteration: an iteration that takes longer dumps every thread's stack to
`stall_file` or stderr, which finds a handler that hangs.

`MultiplexerServer` is a `BaseMultiplexerServer`, built the same way, that
unpickles the payload, calls `process_pickle(data)`, and replies with its
return value through `send_pickle()`, except to a message that answers
another, `references` set, which gets none, so that two of them never
answer each other's replies. It is the backend end of the pickle
convention that `query_pickle()` on the clients is the other end of; only
useful when both ends are Python, and pickles from the network must be
trusted.

## BaseThreadedMultiplexerServer

`multiplexer.threaded_server.BaseThreadedMultiplexerServer` is the backend
class whose handlers run on worker threads behind a heartbeating io thread.
Depend on `@mx//multiplexer:threaded_server`.

```python
from multiplexer.threaded_server import BaseThreadedMultiplexerServer, Request


class Echo(BaseThreadedMultiplexerServer):
    def handle_message(self, request: Request):
        request.reply(request.mxmsg.message.upper(), type=types.ECHO_RESPONSE)


Echo(addresses, type=peers.ECHO_BACKEND, workers=4).serve_forever()
```

**Which server class.** `BaseMultiplexerServer` runs the loop and the
handler on one thread, so while a handler runs nothing heartbeats, and a
handler that runs longer than the multiplexer's drop interval, 90 s as
shipped ([semantics](semantics.md#failure-modes)), gets the backend
dropped mid-work. Use it when every handler is quick, requests are to be
handled one at a time, and a handler never blocks on a query of its own;
it is the simplest class and the one most backends are built on. Use
`BaseThreadedMultiplexerServer` when a request may take long, when
several requests should be handled at once (`workers=4`), or when a
handler must block, on a `query()` to another backend for instance,
without the multiplexer taking the backend for dead. With `workers=1`
it handles requests one at a time in arrival order, as
`BaseMultiplexerServer` does; what differs is the heartbeats, which its
io thread keeps up, and one kind of message: a `DELIVERY_ERROR` for a
message the server sent that was not a query, an event whose rule
reports delivery errors say, reaches `handle_message()` here, where
`BaseMultiplexerServer` keeps it to itself. The interface differs in one
place: the handler gets a `Request` and answers through it, rather than
through `self`.

- `BaseThreadedMultiplexerServer(addresses, type=None, workers=1,
  queue_size=1024, decline_searches_when_full=False, timeout=10,
  drain_routing=None)`
  only makes the instance id; `type` may instead be the class attribute
  `multiplexer_client_type`. `connect()` starts the workers and connects,
  to every address at once against one `timeout`, as
  `BaseMultiplexerServer` does, once, and `serve_forever()` calls it first, so nothing reaches
  `handle_message()` before your `__init__` has finished, and no
  multiplexer knows the backend until it can serve; `instance_id` is
  valid from construction. When to call `connect()` yourself is as for
  `BaseMultiplexerServer` above: something waits for a line you print
  before it sends, a test wants the backend connected without serving
  it, or you start `serve_forever()` on a thread of your own and send at
  once. The io thread heartbeats,
  reconnects, answers pings and the search clients use to find a
  backend, and queues every other message for the workers. `queue_size`
  bounds the requests waiting for a worker, the same 1024 the library's
  incoming queue and the multiplexer's per-connection queue hold; beyond
  it a request is dropped with a warning and no answer, so its requester
  waits out its first stage's timeout and searches. Unlike a peer whose
  queue on the multiplexer is full, which round robin skips, a saturated
  backend keeps its share of the requests, and the search may find it
  again unless `decline_searches_when_full=True`.
  A request that arrives while the server is leaving, `close()` under
  way after `serve_forever()` returned, routed before the multiplexer
  applied the drain routing, is refused with `DELIVERY_ERROR`, the
  multiplexer's own answer for a peer that is gone, so the requester's
  retry starts at once rather than after its timeout. A message that
  answers another, one with `references` set, is dropped instead, since
  nobody retries a reply and refusing one could start a loop
  ([how a backend leaves](leaving.md#what-stays)).
- `handle_message(request)` runs on a worker with every message that is
  not the protocol's own, and with the `DELIVERY_ERROR`s for messages the
  server sent that were not queries, which a handler tells by the type
  and answers with `no_response()`. `request.mxmsg` is the message and
  `request.connection` the connection it came on. `request.reply(message,
  type=..., **fields)` answers it, with `to`, `references`, `workflow` and
  the connection filled in from the request, the empty ones of a whole
  `MultiplexerMessage` too, its id and from included, message fields
  beside it a `TypeError`, through its
  `ThreadedClient` from whichever thread calls it: a handler may hand the request to
  another thread and return, and the reply comes later. Such a request
  no longer counts in `pending` once its handler returned, so neither
  `drained()` nor `close()` waits for it: a program that answers later
  overrides `drained()` to wait for its own work too, and a reply sent
  after `close()` raises `RuntimeError`.
  One reply per request: `reply()` sets `references`, and a requester
  built on `ThreadedClient` or `AsyncClient` drops what references a query
  it has seen answered, so a follow-up that is not the reply goes through
  `self.send_message(..., to=request.mxmsg.from_)` with no `references`,
  correlated in the payload. `request.no_response()` says the message needs none, as an
  event; `request.report_error(message)` answers with `BACKEND_ERROR`;
  `request.notify_start()` sends `REQUEST_RECEIVED`;
  `request.parse_message(SomeProto)`, `request.parse_pickle()` and
  `request.reply_pickle(data)` are the parsers and the pickle reply. A
  request dropped without a reply or `no_response()` is logged when it is
  collected. `self.send_message(message, **kwargs)` sends a message that is
  not a reply, with no defaults; `self.client` is the `ThreadedClient`.
- Searches are answered while the backend serves, from `serve_forever()`
  on until `close()` begins: a busy
  `BaseMultiplexerServer` answers a search when it gets to it, and this
  class answers at once from the io thread. `decline_searches_when_full=True`
  leaves a search unanswered while every worker is busy and requests wait,
  so a client's retry lands on another instance;
  `should_respond_to_backend_for_packet_search()` is overridable for any
  other condition, and runs on the io thread, so it must be quick.
- `serve_forever(poll=1.0, drain_seconds=0.0)` runs until `stop()` or a
  drain is over, calling `periodic_task()` every `poll` seconds on its own
  thread; then it takes no new message, lets the workers finish the
  queue, closes the connections and returns. `stop()`, `start_draining()`,
  `draining`, `drained()`, `drain_routing` and `on_handler_exception(exc)`
  are `BaseMultiplexerServer`'s, with the same meanings, `drained()`
  also waiting for the queue to be empty; a handler that raises
  gets the requester `BACKEND_ERROR`, unless its reply went out, a reply
  that raised being none, or the message answers another, `references`
  set, as for `BaseMultiplexerServer`, and the exception goes to
  `on_handler_exception()` on the worker thread, a report that fails
  logged; `False` from there makes `serve_forever()` return and re-raise.
  So does an exception out of `on_handler_exception()`, or a handler's
  `SystemExit` or other `BaseException`: the worker leaves, and what is
  still queued once no worker is left is refused, as during a close.
  `close()` joins the workers, so from a handler it raises `RuntimeError`
  rather than join itself, during another thread's `close()` too; a
  handler that wants the server gone calls `stop()`. From another thread,
  `close()` makes a `serve_forever()` running there return, quietly, even
  while it still connects, and a second call returns once the first is
  done; a `with` block calls it at its end ([lifetimes](#lifetimes)).
  `pending` is the number of requests waiting or being handled, `dropped`
  the number a full queue dropped or leaving refused; `instance_id` what a
  client addresses with `to`.
- A handler may call the blocking `query()` and a flushing `send_message()`
  on `self.client`, since it is not on the io thread, which is the point.
  `BackendThread` from `multiplexer.testing` serves this class too.

## ThreadedClient

`multiplexer.threaded_client.ThreadedClient` is a class with an io thread
of its own, for a client or a backend. The thread runs all the time, so
heartbeats and reconnects happen while the program does other things, the
peer type need not be passive, and any thread may use it. Depend on
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

`on_drop=` and `dropped` are under [Messages the library gives up
on](#messages-the-library-gives-up-on).

- `query(message, type=None, timeout=10, callback=None, to=0, multiplexer=ONE, with_connection=False, on_received=None)`
  is the same three-stage algorithm as `SyncClient.query()`, taking
  `message` as it does, a whole `MultiplexerMessage` as the request itself;
  it raises the
  same exceptions, `BackendError` included, and `ValueError`
  at the call for a message over the 128 MiB limit, as every send of
  every client does (`MAX_MESSAGE_SIZE`), the request measured as the
  query may send it again; `to`, `multiplexer` and
  `with_connection` are
  [the same too](#lanes-pinning-and-addressed-queries), and so is
  [`on_received`](#knowing-a-backend-took-the-request), called on the io
  thread. Any number of
  threads may call it at once; replies are matched to queries by the ids
  they reference, never by arrival order. With `callback` it returns
  `None` at once and calls `callback(result)` on the io thread with the
  reply, `(reply, connection)` with `with_connection`, or with the
  exception instance, the shape C++'s callback overload has; after
  `shutdown()` it calls `callback` at once, on the calling thread, with
  `NotConnected`, so a callback must not query again then, nor take a
  lock its caller holds. The blocking form raises `RuntimeError` when
  called from a callback, where it would block the io thread. A query
  waits for the io thread as a send does (below), and its first stage's
  time starts when the io thread takes it up.
- `send_message(message, type=..., multiplexer=ONE|ALL|lane|connection,
  flush=False, timeout=10, callback=None)` queues an event on one connection, on all of
  them, on a lane's or on a connection's, and returns its message id at
  once; the io thread writes it right after. `message` is as for
  `SyncClient.send_message()`: a payload with its fields, or a whole
  `MultiplexerMessage` sent as it is, fields beside it a `TypeError`. A message that cannot be
  queued yet, no connection being live or the connections' queues full
  (1024 messages each), waits there, behind those sent before it, until a
  connection comes up or has room, within `timeout`, and is dropped and
  reported after that: with `ALL` a full connection gets its copy as soon
  as it has room, and a lane waits for room on its own connection while
  that lives. The io thread takes sends and queries in the order they
  were made, from a queue of its own that has no bound, so a program that
  sends faster than the io thread places what it sends holds the
  difference in memory; `timeout` counts from the call, and a message the
  io thread reaches with its time up is placed only where a connection has
  room for it then, and dropped and reported otherwise.
  Through a pinned lane whose connection is gone, and after `shutdown()`,
  it raises `NotConnected` at once. With `flush=True` it waits until the
  message reached the socket, for `ALL` until one copy did, a connection
  that dies under it handing it to another or having it held, and raises
  `NotConnected` when nothing wrote it with no connection live, else
  `OperationTimedOut`; the flushing form must not be called from a
  callback. A copy for `ALL` is held only when no connection is live and
  dropped otherwise, as on `SyncClient`. With `callback`, flushing or
  not, it returns at once and `callback(written)` runs once on the io
  thread: 1 when the message was written, the first copy for `ALL`, 0
  when it was given up on or `shutdown()` came first; safe from
  callbacks. The same call and the same
  result as on `SyncClient`: both send through one mechanism in the
  library. `lane(pinned=False, connection=None)` makes a lane.
- `flush_all(timeout=10)` waits until everything sent before the call has
  been written or given up on, what still waits for a connection or for
  room included, or `timeout` seconds, and returns, once the callbacks of
  those sends have run, whether every one was written: `False` when one
  was given up on, at its own timeout or with a connection that died,
  which `on_drop` names, or when the time ran out. What is sent meanwhile
  is not waited for, so a flush ends however busy the client is, a
  message a dying connection hands to another included. After
  `shutdown()` it returns `True` at once, nothing being left to wait for.
  What `shutdown()` does first. Not from a callback.
- `query_pickle(data, type, timeout, callback=None, **query_kwargs)` and
  `send_pickle(data, ...)`: the pickle convention, as on `SyncClient`; with a
  callback, it gets the unpickled reply or the exception, what unpickling
  raised included.
- `connect((host, port), timeout=10)` connects to one more multiplexer and
  waits for the handshake: `True` once registered, at once for one
  connected already, which it keeps, `False` as soon as it failed, or at
  `timeout`, the io thread trying again every 3 s on its own.
  `connect_all(endpoints, timeout=10)` connects to every one at once, each
  waited for against the same `timeout`, so a multiplexer that never
  welcomes costs it once, not once for every address after it, and
  returns how many are connected. `disconnect((host, port))` drops one given to the constructor or
  `connect()`, as `SyncClient.disconnect()` does, on the io thread, and
  returns once it is done, whether the client had it:
  `connections_count()` is down by then, a query through the connection
  closed goes on as after a lost connection, and a
  `connect()` still waiting for that multiplexer returns `False`. Neither
  from a callback, where they raise `RuntimeError`; after `shutdown()`
  they raise `NotConnected`.
- `on_message(mxmsg)`, given at construction, runs on the io thread with
  every message that is not a reply to a query: events and requests
  addressed to this peer, and a `DELIVERY_ERROR` for a message that was
  not a query, an event whose rule reports errors, one from each
  multiplexer that could not deliver it. Pass `queue.put` to
  collect them for another thread. Without it such messages are logged
  and dropped, never queued for a reader that may never come. A late
  reply to a query that already ended, `REQUEST_RECEIVED` for a query no
  longer tracked, and a `PING` or a search, which the client answers or
  declines itself, never reach it. The late-reply rule has a consequence for peers that send
  to this client: `references` means "this is the reply", and what
  references a query of this client that has ended (for twice the
  longer of its timeout and 10 s after it ended, 131072 queries at most;
  `forget_finished_ids()` forgets them at once, for a test that measures
  the heap) is dropped whatever its type, so a follow-up that is not the reply must
  not reference the request; it is addressed to this peer with `to` and
  correlated in the payload ([semantics](semantics.md#delivery)). The
  same holds for `AsyncClient`, which is built on this class.
- Callbacks, `on_message`, `query`'s and a send's, hold the GIL on the io
  thread and must return quickly; they may call `query()` with a callback,
  `send_message()` without `flush` or with a callback, and `shutdown()`,
  which does not wait there, but not the blocking `query()`, a flushing
  `send_message()` or `flush_all()`. A callback that raises has its
  traceback printed, and the client goes on.
- `shutdown(timeout=1)` fails every query in flight, writes what was sent
  before it, and what the io thread sends meanwhile, a server's refusal of
  what still arrives say, `timeout` seconds in all, as
  `SyncClient.shutdown()` does; any other send meanwhile raises `NotConnected`,
  then closes the connections and stops the thread, once every
  multiplexer has closed its side too, a round trip, a second at most,
  and a name lookup in progress, for an address given by name, has
  returned, which against a slow or unreachable DNS server is the
  resolver's own timeout;
  what is still unwritten is dropped and reported, at once with
  `timeout=0`. A client whose last reference goes in one
  of its own callbacks, a callback query that outlived the caller's
  reference say, shuts down there without waiting for its thread, which
  ends on its own. `with ThreadedClient(...) as client:` shuts it down at
  the end of the block; what dropping a client does, and when one is
  freed, is under [Lifetimes](#lifetimes). The client is not fork-safe:
  create it after forking. `orphaned()` is whether this one was inherited
  across a fork, where its calls raise `UsedAfterFork`
  ([fork](#threads-exit-and-fork)).

The old two-client pattern, one client sending with `from` set to a
receiving client's id and a thread looping on the receiver, is what this
replaces. [examples/echo/workers.py](../examples/echo/workers.py) shows
several worker threads sharing one client, each taking its replies from
its own queue.

## AsyncClient

`multiplexer.aio.AsyncClient` is the class for asyncio programs, for a
client or a backend: an async face on `ThreadedClient`. The io thread
keeps the sockets, the heartbeats, the reconnects and the query algorithm;
the coroutine awaits a future the io thread settles through the loop, so
nothing ever blocks the event loop. Depend on `@mx//multiplexer:aio`.

```python
from multiplexer.aio import AsyncClient
from multiplexer.multiplexer_constants import peers, types

client = AsyncClient([("127.0.0.1", 1980), ("127.0.0.1", 1981)], type=peers.WEB)
reply = await client.query(b"pears", types.SEARCH_REQUEST, timeout=10)
await client.send_message(b"seen", type=types.SEARCH_EVENT)
unsubscribe = client.subscribe(types.SEARCH_EVENT, handle)   # a coroutine function, or a plain one
```

- `AsyncClient(addresses, type, timeout=10, loop=None, queue_size=1024, *, on_drop=None)`
  connects and binds to the running loop (`on_drop` and `dropped`: [Messages
  the library gives up on](#messages-the-library-gives-up-on)), blocking
  the loop while it connects: one address after another, a round trip
  each when its multiplexer is up, up to `timeout` for one that hangs, so
  `holder.get()` below costs the same;
  `await AsyncClient.create(...)` does the connecting in the default
  executor for a program that must not block its loop even once; a caller
  that gives up meanwhile, a timeout around the await say, leaves nothing
  behind, the client made anyway being closed once it is there. What
  arrives on its own, the subscriptions and `messages()`, runs on that loop;
  `query()` and `send_message()` may be awaited from any loop, as a
  `ThreadedClient` may be called from any thread, which is what code run
  through asgiref's `async_to_sync` away from the server's loop needs.
  `messages()` from another loop raises `RuntimeError`. `loop` is that loop;
  once it is closed, `subscribe()` and `messages()` raise `RuntimeError`
  and what arrives on its own is dropped, said once in a warning, while
  `query()` and `send_message()` still work from other loops, so a
  program that must receive again makes a new client on a running loop.
- `new_message(**fields)` builds a `MultiplexerMessage` with `id` and
  `from` filled in, as on `ThreadedClient`.
- `disconnect((host, port))` drops a multiplexer given to the
  constructor, as `ThreadedClient.disconnect()` does, blocking the loop
  for the round trip to the io thread, as `connections_count()` does;
  `NotConnected` after `close()`.
- `await query(message, type=None, timeout=10, to=0, multiplexer=ONE, with_connection=False, on_received=None)`
  returns the reply and raises the same exceptions as `SyncClient`:
  `NotConnected`, `OperationTimedOut`, `OperationFailed`,
  `BackendError`; `to`, `multiplexer` and `with_connection` are
  [the same too](#lanes-pinning-and-addressed-queries), and `lane()` makes
  a lane; [`on_received`](#knowing-a-backend-took-the-request) is called
  on the loop, before the await resumes. `await query_pickle(data, type)` for the pickle convention.
  Cancelling the await does not cancel the request: a backend may still
  get it, the reply is dropped.
- `await send_message(message, multiplexer=ONE, timeout=10, flush=False, callback=None, **fields)`
  sends an event as every client does (`ThreadedClient.send_message()`)
  and returns its message id once the io thread has the message: queued,
  waiting for room, or held until a connection comes up, within
  `timeout`, and dropped and reported (`on_drop`) after that. With
  `flush=True` it returns once the message reached a socket, for `ALL`
  once one copy did, a connection that dies under it handing it to
  another or having it held (a copy for `ALL` only held, with no
  connection live, as on `SyncClient`), and raises `NotConnected` when
  nothing wrote it with no connection live, else `OperationTimedOut`. With a
  `callback`, `callback(written)` runs on the client's loop once the
  message's end is known: 1 when it was written, the first copy for
  `ALL`, 0 when it was given up on or `close()` came first. `NotConnected`
  at once for a pinned lane whose connection is gone, and after
  `close()`. `await send_pickle(data, ...)` likewise.
- `await flush_all(timeout=10)` returns once everything sent before it was
  written or given up on, what waits for a connection or for room
  included, or after `timeout` seconds, and whether every one was
  written, as `ThreadedClient.flush_all()` says; awaited on the client's
  loop, the callbacks of those sends have run by then.
- `subscribe(type, handler, matching=None)` runs `handler(mxmsg)` on the
  loop for every message of `type` (`None` for all) that `matching`
  accepts; a coroutine function runs as a task. `matching` runs on the io
  thread, with the GIL, for every message that arrives on its own, before
  anything is handed to the loop: it must be quick and must not touch the
  loop's state, and a message it refuses costs the loop nothing. A plain
  handler is called in the order the messages arrived, each call over
  before the next message is handed over; coroutine handlers start in
  that order, as tasks that may interleave at their awaits. A query
  awaited on the client's own loop resumes after everything that arrived
  before its reply was handed over. A handler that raises, at
  once or later as a coroutine, is logged with the message's type and
  sender through the client's logging, and the other handlers still run.
  A `matching` that raises is logged the same way and takes the message
  for none of its subscription's handlers; the other subscriptions and
  `messages()` still get it. The io thread hands every message over and never waits for the loop:
  what the loop has not reached yet waits in memory, as do the tasks of
  coroutine handlers still running, and the library bounds neither. A
  program whose loop may fall behind the traffic sheds load itself, in
  `matching`, which runs before anything is handed over: on the io
  thread, holding the GIL the loop needs too, for every message of its
  subscription's type. A message costs a look at the subscriptions of its
  type and of every type, no others.
  Returns the function that ends the subscription: once it has returned,
  on the loop, the handler is not called again, for a message already
  handed to the loop either; a coroutine already running goes on.
  `RuntimeError` once the client's loop has closed, where nothing could
  be delivered.
  `messages()` is the pull form, the client's inbox, as `read_message()`
  is `SyncClient`'s: an async iterator over one queue of `queue_size`, 1
  at least, `ValueError` otherwise (asyncio's 0 for no bound is not
  offered), every message that arrives on its own; tasks reading it share the
  messages, each going to one of them, and a handler that needs every
  message of a type, each its own copy, is a subscription. When nobody
  reads it, the oldest message is dropped and a warning logged; what
  waits for the loop itself is unbounded, as for the subscriptions. After
  `close()` every reader gets what arrived before it and then its `async
  for` ends, `__anext__()` raising `StopAsyncIteration`; a stream asked for
  after `close()` ends at once.
- `close(timeout=1)` ends the client as `ThreadedClient.shutdown(timeout)`
  does, writing what was sent before it first, and joins the io thread,
  blocking for that and a round trip to the multiplexers, a second at
  most each, and for a name lookup in progress, as there; `await
  aclose(timeout=1)` does it in the executor; `async with` works. From
  the call on no subscription's handler is called, for a message handed
  to the loop before it either. The tasks of coroutine handlers still
  running are cancelled on the loop once `close()` has returned, but for
  a handler that called it, which goes on; `aclose()`, awaited on the
  client's loop, first gives them `timeout` seconds to end, so that what
  they send goes out with the rest, and cancels those still running
  then.
- `AsyncClient.holder(type, addresses, **kwargs)` keeps one client per
  process, made with the constructor's `kwargs` on the running loop at
  first use, or on `loop=` among them, and forgotten in a forked child, with `addresses` read lazily: what a worker of an ASGI server
  uses, since those fork before the loop runs. `await holder.aget()`
  makes it on a thread, so the first request does not hold the loop for
  the handshakes; callers that arrive meanwhile await the same one, and a
  caller cancelled meanwhile stops only its own wait. `holder.get()`
  makes it on the loop when it must be synchronous. Either way there is
  one client, however many threads use the holder first at once, as
  `async_to_sync` calls from a threaded server's requests do: the others
  wait for the one being made. One that could not be made raises for
  every caller waiting for it, and the next use tries again.
  `holder.close()`, or `await holder.aclose()`, closes it; the next use
  makes another. The first use's loop is the client's for as long as the
  holder keeps it, whichever caller came first: a client first asked for
  inside an `async_to_sync` call with no server's loop to run on, or by a
  test on its own loop, delivers nothing once that call or test is over,
  `subscribe()` raising. Closing the holder then makes the next use start
  a client on its own loop, which is what a test does in its tear-down.
  The holder does not replace the client by itself: `async_to_sync` with
  no server's loop runs every call on a new loop, closed after it, and a
  process that only sends, a Celery worker say, keeps one client through
  them all.
  [The async web server recipe](recipes/async_web_server.md) shows it under
  Django Channels; [examples/aio](../examples/aio) is a complete asyncio
  gateway.

## Sending

Every client sends the same way, through one mechanism in the library, and
the server classes' `send_message()` is their client's:

| Call | Returns | No connection live | A loss is learned from |
|---|---|---|---|
| `send_message(...)`, awaited on `AsyncClient` | the message id, at once; on `AsyncClient` once the io thread has it | held until one comes up, `timeout` at most, then dropped | `on_drop(message_id, reason)` and `dropped` |
| `send_message(..., callback=f)` | at once | held, as above | `f(0)`, and `on_drop`; `f(1)` once written |
| `send_message(..., flush=True)` | once written, the first copy for `ALL` | waits for one, `timeout` at most | `NotConnected` when nothing wrote it with no connection live, else `OperationTimedOut` |
| `flush_all(timeout)`, awaited on `AsyncClient` | `True` once everything sent before it was written; `False` once one was given up on, or at the timeout | waits | `False`, and `on_drop` for which |
| `shutdown(timeout)`; `close(timeout)` on `AsyncClient` and the server classes | after writing what was sent before it, `timeout` (1 s) at most | waits, then drops | `on_drop`, `SHUT_DOWN` |

Written means handed to the kernel on a live connection: only a reply says
a message arrived ([semantics](semantics.md#failure-modes)). Order holds
per connection; a lane keeps a stream on one ([Lanes, pinning and
addressed queries](#lanes-pinning-and-addressed-queries)). `flush=True`
makes no send faster: the message goes out as soon either way, and the
call returns later.

## Messages the library gives up on

Every client counts the messages the program sent that it gave up on, and
tells the program of each when given `on_drop`: `SyncClient(addresses,
type=..., on_drop=f)`, `ThreadedClient(..., on_drop=f)` and
`AsyncClient(..., on_drop=f)` call `f(message_id, reason)` for every one,
each copy of a message sent to `ALL` once, and `client.dropped` is the count
so far. `reason` is a `DropReason`, from `multiplexer.mxclient`:

- `NO_ROOM`: it waited for room on a full connection, its multiplexer not
  reading, past its `timeout`, or, sent with no time to wait, found none.
- `NO_CONNECTION`: it waited for a connection to come up past its `timeout`,
  or, sent with no time to wait, found none live.
- `CONNECTION_LOST`: its connection ended and nothing else could take it: a
  pinned lane's, or a copy sent to `ALL`.
- `SHUT_DOWN`: the client shut down before it went: `shutdown()` or
  `close()` could not write it within its timeout.

`f` runs, with the GIL, inside whichever `SyncClient` call runs the loop when
the drop happens, on a `ThreadedClient`'s io thread, where it must return
quickly as `on_message` must, and on an `AsyncClient`'s event loop. A send
given a `callback` hears of its own message as well: `callback(0)` where
`f` hears of the drop, `callback(1)` once the message was written. Each drop
is also logged, the first of a kind at once and the rest counted in a line a
second. A message the library wrote is not dropped, whatever happens to it
next: written means the kernel's buffer, and a multiplexer that dies before
reading it takes it along unreported ([semantics](semantics.md)).

## Lifetimes

Every client and server holds connections, most an io thread too, and
ends when told to: `shutdown()` for `SyncClient` and `ThreadedClient`,
`close()` for `AsyncClient` and the server classes. Each ends the same
way: what was sent before it is written first, a second at most by
default (`timeout=`), then the connections close. A `with` block does
it at its end, however the block ended, `async with` for `AsyncClient`:

```python
with ThreadedClient(addresses, type=peers.WEB) as client:
    reply = client.query(b"pears", type=types.SEARCH_REQUEST)

with SearchBackend(addresses, type=peers.SEARCH) as backend:
    backend.serve_forever()  # closes on its way out; the block's close() then does nothing
```

- **Dropped while running.** A `ThreadedClient` given `on_message`, an
  `AsyncClient` and a threaded server refer to themselves through a
  callback that their running io thread holds, so dropping every reference
  to one ends nothing: it runs, connected, a backend answering requests,
  until it is shut down or closed, as a running `threading.Thread`, a
  `logging.handlers.QueueListener` or a `socketserver` server does. So
  does any client whose `on_drop` refers back to it, a method of its own
  say. Nothing warns about one never shut down, since it is never
  collected while it runs.
- **Dropped and freed.** A `SyncClient`, and a `ThreadedClient` given no
  `on_message`, are freed when dropped, unless their `on_drop`, or the
  callback of a send still on its way, refers back to them: the
  destructor shuts the client down on the thread that dropped it, waiting
  for the multiplexers' side of the close, a second at most, so that what
  was written arrives, and for a name lookup in progress, without the
  GIL ([SyncClient](#syncclient)). A `SyncClient` calls back only inside the calls
  the program makes, so one freed this way calls nothing, neither
  `on_drop` nor a send's callback.
- **Serving.** `serve_forever()` holds its server while it serves and
  closes it on its way out. A server only connected with `connect()`, as a
  test may do, serves on its io thread and workers until `close()`.
- **Shut down.** A client shut down, or a server closed, lets go of its
  callbacks and is freed once nothing else refers to it; its sends and
  queries fail from then on.
- **Forks and interpreter exit** are under [Threads, exit and
  fork](#threads-exit-and-fork).

## Threads, exit and fork

A `SyncClient` belongs to one thread; a `ThreadedClient` may be
used from any number of them; an `AsyncClient` delivers on the event
loop it was made on and may be awaited from any. A
`BaseMultiplexerServer` made on one thread and served from another is
fine: `serve_forever()` adopts its thread. In debug builds of the extension the rules are checked and a
violation fails an assertion.

**Interpreter exit.** A thread blocked in one of the library's waits when
the interpreter starts to exit, say a daemon thread in `read_message()`
while the main thread returns, never takes the GIL again: it parks. The
extension does this on every version; without it CPython 3.11 would end such
a thread with `pthread_exit` inside the extension's C++ frames, which
crashes. Two things follow. A daemon thread in a wait costs nothing at exit.
A non-daemon thread in `serve_forever()` keeps the interpreter alive until
something clears `working`, and `threading._shutdown` runs before `atexit`,
so a backend on a thread runs as a daemon or the program calls `stop()` on
it before exiting. The extension registers an `atexit` hook, when
`multiplexer.mxclient` is first imported, that marks the exit and lets every
callback and wait that was already about to take the GIL finish first, two
seconds at most: a callback still running then, a slow one on the io thread,
may take the process down as the interpreter ends it inside the extension's
frames, so callbacks stay short. From the hook on, a thread coming back from
one of the library's waits parks, so a hook of yours that shuts a client
down or stops a server, waiting for the library's threads, must run first:
`atexit` runs the hook registered last first, so register yours after
importing `multiplexer`. Do not install another `_native` import path that
skips `multiplexer.mxclient`. A program that exits with an `AsyncClient` or
a `ThreadedClient` alive exits without a crash, but only a client that is
freed writes what it still holds, its destructor shutting it down: a
`ThreadedClient` given `on_message`, an `AsyncClient` and a threaded server
are never collected while they run ([Lifetimes](#lifetimes)), so their io
thread ends with the process and what it had not written is lost,
unreported. A program shuts them down or closes them, or leaves their `with`
block, before it exits.

**Fork.** A client inherited by a forked child, from gunicorn's workers,
`multiprocessing`, or Django's parallel test runner, is an orphan there:
its io thread does not exist in the child, its locks may have been held at
the fork by threads that do not exist there either, and its sockets are
shared with the parent. Every call on it that would send, receive,
connect, disconnect, wait or take one of its locks raises `UsedAfterFork`, a
`NotConnected`, first, and so does every use of a lane or a connection
from before the fork, even by a client made in the child, as do the
getters of the connections' state, `connections_count()`,
`has_incoming_messages()` and `routing_acknowledged()`, which would answer
with the parent's; `ThreadedClient.orphaned()` tells without raising. Of a threaded
server, `close()`, `connect()`, `serve_forever()` and `pending` raise, and
`stop()` only clears `working`, since a signal handler may call it; the
function an `AsyncClient`'s `subscribe()` returned does nothing.
`shutdown()`, or an `AsyncClient`'s `close()`, on an inherited client
closes the child's copies of its connections, once, those still closing at
the fork too, never the parent's connection. Dropping one does the same, except a client with a callback,
an `AsyncClient`, a `ThreadedClient` given `on_message` or the one inside
a threaded server, which refers to itself through the callback until it
is shut down: shut it down, a server's as `server.client.shutdown()`, and
it is freed when dropped.
Create clients after forking, never at import: a module-level client made
by the parent before the fork is exactly what every worker inherits. A
holder that makes the client on first use, as in [the web server
recipe](recipes/web_server.md), shuts the inherited one down in a fork
hook and makes its lock anew there, since the fork can come while another
thread holds the old one; `AsyncClient.holder()` does both. The detection
is a fork generation counter maintained by a `pthread_atfork` handler, so
it covers forks the library never saw; a call pays one load for it, and
nothing else runs when nobody forks.

**What a forked child can still wait on.** The child of a process with
threads has only the thread that forked; a lock another thread held at
that instant stays held unless something resets it in the child. glibc
resets its allocator and stdio; it does not reset its name resolver. A
child forked while a thread of the parent was inside `getaddrinfo` can
find a lock of the resolver held for good: every name lookup there waits
forever, and so, in some cases, does a thread's exit. The library looks
names up, on a thread of its own, whenever a client connects to a
multiplexer by name, and again at every reconnect attempt, every 3 s,
while that multiplexer is unreachable. So a program that forks while
clients are alive gives every client addresses, the parent's too,
`("10.0.0.1", 1980)` rather than `("mx1.internal", 1980)`, or resolves the
names once before it makes any; an address in text is never looked up. The
same goes for the program's own threads that resolve names, and the
dynamic loader's locks are the same kind of hazard for a child that
imports an extension module while a parent thread was importing one.
`multiprocessing`'s `spawn` start method starts every child from a fresh
interpreter, and `forkserver` forks it from a server started that way,
which has no threads and no client unless a module preloaded into it makes
them: none of this reaches either.

**A fork inside a callback.** A child forked inside one of the client's
callbacks, `on_message` or a query's, is a copy of the io thread in the
middle of its loop: once the callback returns, the child runs the parent's
loop on the parent's sockets. Such a child calls `exec` or `os._exit()`
before the callback returns, which `subprocess` does.

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
        with Cluster(1, rules="deployment.rules") as cluster, FakePeer(cluster, peers.INDEX) as index, TestClient(cluster, peers.WEB) as client:
            index.reply_with(types.SEARCH_REQUEST, b"3 hits", types.SEARCH_RESPONSE)
            reply = client.query(b"pears", types.SEARCH_REQUEST)
            self.assertEqual(b"3 hits", reply.message)
            self.assertEqual(b"pears", index.wait_for(types.SEARCH_REQUEST)[0].message)
```

- `Cluster(count, rules, record=False, record_payload_bytes=0,
  rules_check_interval=None, drain_seconds=None)` starts `count`
  multiplexers on entering and, on leaving, stops every role process
  still running and then the multiplexers; the in-process peers,
  `FakePeer`, `BackendThread`, `TestClient` and `ThreadedTestClient`,
  are the test's to stop, best with their own `with` blocks inside the
  cluster's, since they would go on reconnecting. Leaving also fails the
  block with an `AssertionError` naming every process that did not end
  cleanly, with the end of its log: a role that `SIGTERM` did not end
  within 10 s with 0, or by the signal for one that does not catch it,
  and a multiplexer that exited on its own, exited other than 0 at any
  stop, the test's, a `restart()`'s or the cluster's, or had to be
  killed when a stop ran out of time, 10 s, or `drain_seconds` + 5 s for
  a cluster that drains longer (a process that `pause()` froze is
  continued after its `SIGTERM`, so that it handles it); so a crash, a
  hang at shutdown and,
  under LeakSanitizer, a leak at exit fail the test. A block that is
  failing already keeps its own error, and the report goes to stderr.
  `rules` is the path of the
  rules file, the one your constants were generated from, which the test
  names: under Bazel `runfile("your/pkg/deployment.rules")` with the file
  in the test's `data`. A scenario under `mx_integration_test` may leave
  it out, since the rule's `rules` attribute names it. A test that edits
  a copy of the file under the running multiplexers gives
  `rules_check_interval`, the seconds between their reads of it (their
  default when `None`, 0 never), and `Mx.reload_rules()` sends one
  `SIGHUP` instead; `drain_seconds` is their `--drain-seconds`, how long a
  stop goes on sending what is queued (their default when `None`). The
  multiplexers run the binary `MXCONTROL` names when
  it is set, else the `mxcontrol` that came with the package, the wheel's
  or, under Bazel, `@mx//mxcontrol` in the runfiles; PATH is never
  searched, so a test runs the multiplexer of the library it imports, and
  one with nothing to run fails before starting anything, naming
  `MXCONTROL`. `multiplexer.mxcontrol.binary_path()` names the package's
  binary for a program that starts one itself.
  `endpoints` is the list of `(host, port)` the clients take;
  `wait_for_peer(type_or_name, count=1, timeout=15)` blocks until every
  multiplexer lists that many peers of the type in its peers file, and
  `wait_for_peer_gone(type_or_name, timeout=15)` until none does; each
  `Mx` in `mx` has `connected_peers()`, `stop()`, `kill()`, `restart()`,
  `pause()`, `resume()`, `log_path` and, with `record=True`, `record_file`:
  `stop()` sends `SIGTERM` and waits for the exit, through the drain, and
  `kill()` `SIGKILL`, an end the cluster does not report, as it does not
  after `expect_exit()`, for a test that ends the process by other means,
  a signal of its own; `start()` starts a
  stopped or killed one again on the same port and returns once it
  listens, and `restart()` is both; `pause()` freezes it with `SIGSTOP`, a
  hung multiplexer whose sockets stay open, and `resume()` thaws it;
  `log_contains(text)`, with `wait_until`, waits for a line of its log
  such as `rules reloaded from`; `endpoint` is its `(host, port)`, and
  `cluster.multiplexer_at(endpoint)` is the `Mx` at the `(host, port)` a
  lane's or a reply's connection names.
  The logs and files go to Bazel's outputs directory under `bazel test`,
  or to `$TEST_TMPDIR` when set, as `make check-py` sets it; else to a
  directory of the process's own under `$MX_TEST_OUTPUT`, kept, when that
  is set, or to one temporary directory per process, removed when the
  process exits. A multiplexer's first `start()` drops an earlier run's
  log and recording; a restart appends to both. A process the test leaves
  running is ended when the test process exits, and, through a pipe each
  is given (`MX_TEST_PARENT_FD`), when it is killed: `mxcontrol`, the
  shipped roles and every program built on `lib/program.h` watch it and
  end as `SIGTERM` ends them.
- `FakePeer(cluster, peer_type, name=None, endpoints=None)` is a scripted
  backend on its own thread, connected to every multiplexer of the
  cluster or to the `endpoints` given, for a peer that is behind one
  only. `reply_with(request_type, payload, reply_type=None)` answers every
  request of that type with the payload as a reply of `reply_type`, the
  request's type by default; `on(request_type, handler, reply_type=None)`
  answers with what `handler(mxmsg)` returns, or nothing for `None`. It keeps
  every message it got in `received`, `messages(type, matching=None)`
  filters, and `wait_for(type, count=1, timeout=10, matching=None)` blocks
  for them; `matching` is a predicate on the `MultiplexerMessage`, so a
  wait names the message it means, `wait_for(types.SEARCH_REQUEST,
  matching=lambda m: m.message == b"pears")`. `via(mxmsg)` is the `Mx`
  a received message came through and `arrivals(type, matching=None)`
  pairs each message with it, so a test of a lane asserts
  `len({peer.via(m) for m in chunks}) == 1` without reading a recording.
  `instance_id` is what a client addresses with `to=`;
  `declining_searches = True` makes the fake decline backend searches, as
  a saturated backend told to does, while it keeps serving. A type it has no
  script for is received and dropped. A handler that raises makes the
  requester get `BACKEND_ERROR` and makes `stop()` raise, so the test fails.
- `BackendThread(factory, poll=0.05, name=None, drain_seconds=0.0)` serves a
  `BaseMultiplexerServer` of yours on its own thread: `factory()` builds it
  there, `start()` returns once it is connected, `stop()` asks it to leave
  and re-raises what `serve_forever()` raised, a handler's exception only
  when the backend's `on_handler_exception()` returns `False`; `backend`
  is the instance. One whose `connect()` raised, or built after `stop()`
  or after a `start()` that ran out of time, is closed rather than served.
- `TestClient(cluster, peer_type)` sends and queries from the test:
  `send(payload, type, to=0, flush=True, multiplexer=ONE)` returns the
  message id, `query(payload, type, timeout=10, to=0, multiplexer=ONE,
  with_connection=False, on_received=None)` returns the reply,
  `receive(timeout)` the next message addressed to it, `lane(pinned=False,
  connection=None)` a lane for `multiplexer=`, `instance_id` its id;
  `client` is the `clients.SyncClient` underneath. Its peer type should be
  `is_passive`, as for every `SyncClient`.
- `ThreadedTestClient(cluster, peer_type, name=None)` is the same on a
  `ThreadedClient`, and so shaped like a production peer built on one: an
  active peer type, replies matched by id, a late reply to a query it has
  seen answered dropped, a `PING` answered. `send()`,
  `query()`, `lane()` and `instance_id` as on `TestClient`; what arrives
  on its own is kept, with `received`, `messages()`, `wait_for()`, `via()`
  and `arrivals()` as on `FakePeer`. A test of a threaded peer that passes
  on `TestClient` may not pass in production, since `SyncClient` keeps a
  late reply that arrives between its calls (`receive_message()` returns
  it); this one shows what production shows.
- `wait_until(predicate, timeout, what)` polls until the predicate returns
  something true and returns it, or raises `TimeoutError` naming `what`.
- `spawn(role, lang, mx, type, **options)` runs a peer as a process and
  `RawPeer` speaks the wire format by hand; both are described in
  [tests/README.md](../tests/README.md), with `mx_integration_test`, the
  macro that runs a scenario against the shipped roles or a binary of yours.

Everything in-process uses the client library as your code does, so what a
test sees is what production sees, including the thread rules: a
`FakePeer` or `BackendThread` is served on its own thread, a
`TestClient` belongs to the test's thread, and a `ThreadedTestClient`
may be used from any.

### Tests that hold up under load

A test that passes on a quiet machine and fails on a loaded one has
usually assumed something the protocol does not promise. What holds:

- **A request may reach a backend more than once.** A query whose reply
  does not come within its timeout is sent again, to a backend found by a
  search, and a late reply to the first attempt is dropped by the client.
  So the client sees one answer, but a backend may see two requests, and
  will under load. Count events, which are sent once, or the answers; do
  not assert an exact number of requests received, or give the request a
  key and count keys ([semantics](semantics.md)).
- **Wait for the thing itself.** `FakePeer.wait_for(type, matching=...)`,
  `wait_until(predicate, timeout, what)` and `Cluster.wait_for_peer()`
  block until what the test needs has happened; a wait on a total, or a
  `sleep`, is a guess about ordering that load will falsify.
- **Timeouts are for failures.** A wait for something that must happen
  gets the defaults, ten seconds and more, since only a real fault reaches
  them; a short timeout belongs to a step that is expected to time out,
  which load cannot make pass. Never shorten a success wait to make a test
  quick: a passing test costs no waiting at all.
- **Fill a connection with a few large frames.** A test that needs
  messages to wait for room, behind a multiplexer frozen with `pause()`
  or a peer that reads nothing, first fills the sockets between them,
  and what they hold depends on the machine: up to the largest send and
  receive buffers its kernel allows, tens of megabytes on some.
  `fill_frames()` from `multiplexer.testing.buffers` is 32 frames that
  together carry twice that, so they fill the sockets wherever the test
  runs and leave a queue of 1024 messages room for the test's own;
  `past_the_queue(payload)` is those, then twice such a queue of
  `payload`. Filled with frames of the test's own size, the same
  connection took hundreds of thousands of them, past the queue and the
  test's time.
- **Measured tests measure the machine.** The scenarios that check CPU
  time, latency or memory are marked and kept apart; a test of yours that
  asserts on a duration will follow the load of the machine that runs it.

This repository's own suite is run under full load before a change lands,
forty copies of a test at once with every core busy, because that is what
a shared build machine looks like.

## Recording

`multiplexer.recording.read(path)` yields the `Record` messages of a file
written by a multiplexer, checking that the recording's rules match the
generated constants (`check_rules=False` to skip); `read_many(paths)`
merges several files by time, each record tagged with its multiplexer;
`describe(record)` renders one with names, `involves_peer(record, id)`
filters by instance id. A file that ends partway through a record, as
one a session is still writing or a multiplexer that died left can,
gives every whole record and then raises `TruncatedRecording`;
`read_many()` merges the other files to their ends first, and
`python -m multiplexer.recording` prints up to the break and exits 1.
A `Cluster(record=True)` records every multiplexer;
[operations](operations.md#recording) describes the file.

The same module drives recording on running multiplexers, through any
client connected to them, for example one of the reserved controller type,
which needs no rules entry and is accepted when the multiplexer was
started with `--recording-dir` or `--allow-tap`:

```python
from multiplexer import recording
from multiplexer.clients import SyncClient

controller = SyncClient(endpoints, type=recording.RECORDING_CONTROLLER)
for status in recording.start(controller, "checkout-bug", max_seconds=600):
    print(status.multiplexer_id, status.error or status.path)
...
recording.stop(controller)
```

- `start(client, label, payload_limit=0, max_bytes=None, max_seconds=0)`,
  `stop(client)` and `status(client)` send one `RecordingControl` on every
  connection and return the `RecordingStatus` of every multiplexer that
  answered within `timeout`: `recording`, `path`, `label`, `bytes`,
  `records`, `stopped` (why the last session ended), `taps`, `tapping`,
  `dropped`, and `error` when the request was refused. `max_bytes=None`
  leaves the multiplexer's default cap, 0 removes it.
- `tap(client, payload_limit=0, timeout=10)` subscribes on every connection
  and returns an iterator over the records as they are routed, each with
  `multiplexer_id`; it raises `OperationTimedOut` after `timeout` seconds
  without one. Records the client does not read in time are dropped and
  counted in the status. `control(client, RecordingControl.UNTAP)` ends it.
- In a test, `Cluster(remote_recording=True)` starts every multiplexer with
  both options and one shared `recording_dir`; `cluster.recording_files()`
  lists the sessions written, and `mxcontrol("recording", "start", ...)`
  runs the command line tool.

## Lower level

`multiplexer.mxclient.Client` is what `clients.SyncClient` wraps; it adds nothing
you need but exposes `flush_all(timeout)` to push out everything sent
before the call, what waits for room included, and
`new_message(**fields)` to build a `MultiplexerMessage` with id and
sender filled in.

A query that sends its request again, to the backend its search found,
gives it a fresh id, as every client does; a `MultiplexerMessage` you
pass is never changed, the second attempt being a copy, and the reply's
`references` names the attempt it answers. Anything else
about the request that must find its way back, a follow-up or a progress
report, is best matched in the payload, since ids belong to attempts (the
follow-up rule in [Delivery](semantics.md#delivery)).
