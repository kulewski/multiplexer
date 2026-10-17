# Using the Rust library

The crate `mx-multiplexer`, in [rust/mx-multiplexer](../rust/mx-multiplexer),
is the Rust client library: a Rust program connects to the multiplexers
as a client or a backend and exchanges messages with the C++ and Python
peers. It has the classes of the other two libraries, with their names
and their behaviour: `SyncClient`, `ThreadedClient`, and the server
classes `BaseMultiplexerServer` and `BaseThreadedMultiplexerServer`, which
run a `serve_forever()` loop that calls your `Handler`. There is no
separate `AsyncClient`: every waiting call of `ThreadedClient` can be
awaited, on any executor, and that is the Rust `AsyncClient`
([below](#threadedclient-awaited-rusts-asyncclient)).

The library is written in Rust, not bound to the C++ one: its core does
the protocol with no socket, thread or clock of its own, the same core
under every class, and [mio](https://crates.io/crates/mio) runs it, where
the C++ library has asio. It depends on five crates: mio, prost for the
protocol buffers, bytes, libc and log. It runs on Linux, as the
multiplexer does, with Rust 1.89 or newer, and starts no runtime: nothing
in it needs tokio, though a tokio program awaits its calls as it awaits
its own.

Add it to `Cargo.toml`, from crates.io, or from this repository:

```toml
[dependencies]
mx-multiplexer = "3.0"
# or: mx-multiplexer = { git = "https://github.com/kulewski/multiplexer" }
```

The constants generated from the [rules file](rules.md) come from
`mxcontrol generate_constants your.rules --rust src/multiplexer_constants.rs`,
a module of your crate (`mod multiplexer_constants;`) with
`multiplexer_constants::types::*` and `multiplexer_constants::peers::*`,
`u32` each; you commit it, and run the command again when the rules file
changes ([mxcontrol](mxcontrol.md#generate_constants)). The system rules'
types, `PING`, `DELIVERY_ERROR`, `BACKEND_ERROR` and the others the
protocol uses, are in the crate as `mx_multiplexer::constants::types`.
The timeouts and limits of [defaults](semantics.md#defaults) are at the
crate's root: `DEFAULT_TIMEOUT`, `MAX_MESSAGE_SIZE`, `MAX_KEY_SIZE`,
`CLOSE_FLUSH_SECONDS` and the rest. The complete example is
[examples/echo-rust](../examples/echo-rust).

## Messages

`Message` is the protocol's `MultiplexerMessage` as a struct of public
fields, the crate's own type rather than the generated one, so that a
change of prost is never a change of this API:

| Field | Type | |
|---|---|---|
| `r#type` | `u32` | the message type, which the rules route by |
| `payload` | `Bytes` | the bytes, the protocol's field `message`; yours to encode |
| `id` | `u64` | 0 to have one drawn where it is sent |
| `sender` | `u64` | 0 for this client's instance id, filled in where it is sent |
| `to` | `Option<u64>` | the instance it is addressed to |
| `references` | `u64` | the id of the message it answers, 0 for none |
| `workflow` | `Option<String>` | a trace or request id, text, which a reply copies from its request |
| `key` | `Option<String>` | what peers' key filters are matched against ([keys](keys.md)) |

and the rest of the protocol's fields, `report_delivery_error`,
`include_original_packet_in_report`, `override_rrules`, `compression`,
`timestamp` and `logging_method`, which a program rarely sets. `Bytes`
is re-exported, `mx_multiplexer::Bytes`; a payload comes from a
`Vec<u8>`, a `String` or a `&'static str` with `.into()`, and a received
payload is a slice of the frame it came in, never copied. `new_message(type,
payload)` on a client makes one with its id and sender filled in. An id is
drawn for every attempt of a query, the first included, and a reply's
`references` names the attempt it answers, so one message may be queried
again and again as it is. `DeliveryError::decode(&payload)` reads a
`DELIVERY_ERROR`'s payload, `Routing` and `RoutingRule` are the routing
types. A message over `MAX_MESSAGE_SIZE`, 128 MiB, is refused where it is
sent or queried, before anything goes out, with `Error::TooLarge`, a
query's request measured as the query may send it again; so is a key
over `MAX_KEY_SIZE`, 256 bytes.

## Timeouts

Every timeout is an `Option<Duration>`, in every class: `None` sets no
deadline, and `Some(Duration::ZERO)` means "don't wait". The builders of
a query and a send take `.timeout(duration)` and `.no_deadline()`, and
default to `DEFAULT_TIMEOUT`, 10 s. No deadline means the call waits as
long as it takes, and a message sent with it waits for a connection or
for room as long as that takes: `shutdown(None)` writes everything sent
before it first, however long that takes, and a drain capped with `None`
ends only at the multiplexers' confirmation or a stop.

Zero means a call does what it can at once and gives up: a receive takes
what has arrived, a connect starts the attempt, a message sent with it is
placed now on a connection with room, each copy for ALL likewise, or
dropped and reported at once (`NoRoom`, or `NoConnection` with no
connection live), never held; `shutdown(Some(Duration::ZERO))` drops what
is unwritten at once. A flushing send's message waits its call's deadline
and `ROOM_GRACE_SECONDS` more, so that the call ends as a timeout first.

On a `SyncClient` with nothing connected and nothing on its way, a wait
with no deadline fails at once with `NotConnected`, as nothing could
arrive; a query whose request waits for a connection, with none live and
none coming, fails with `NotConnected` at once too, and a flushing send
then drops its message. A `ThreadedClient` waits on, as another thread
may connect it meanwhile.

## Errors

One enum, `mx_multiplexer::Error`, for every class, its variants named
after the Python exceptions:

| Variant | When | C++, Python |
|---|---|---|
| `NotConnected` | no connection is live, or none came up in time; a pinned lane's connection is gone; the client was shut down | `NotConnected` |
| `OperationTimedOut` | a query's stage, or a flushing send, ran out of time with a connection live | `OperationTimedOut` |
| `OperationFailed` | no backend takes the request: the search found nobody, the backend it found is gone, an addressed query's instance is gone | `OperationFailed` |
| `Backend(Box<Incoming>)` | a query's reply was a `BACKEND_ERROR`: the backend's handler failed, the payload saying why | C++ returns the reply; Python `BackendError` |
| `TooLarge { what, size, limit }` | a message over `MAX_MESSAGE_SIZE` or a key over `MAX_KEY_SIZE`, refused at the call | `std::length_error`, `ValueError` |
| `InvalidArgument(String)` | another client's lane or connection, a key filter for a protocol type or with a pattern over `MAX_KEY_PATTERN_SIZE`, an address that does not parse | `std::invalid_argument`, `ValueError` |
| `OnIoThread` | a call that waits, made from one of the client's callbacks on its io thread, where it would wait for good | `std::logic_error`, `RuntimeError` |
| `UsedAfterFork` | a call on a client this process inherited across `fork()` | `UsedAfterFork` |
| `Io(String)` | the system refused what a client needs, an epoll instance or a thread | `std::system_error` |

The first four are a query's ways to end other than with its reply, as
[how a query is answered](query.md#every-way-a-stage-ends) tables them; the
rest are a caller's mistake or the system's refusal, at the call, before
anything is queued. The enum is `#[non_exhaustive]`.

## SyncClient

```rust
use mx_multiplexer::SyncClient;

let mut client = SyncClient::builder(peers::ECHO_CLIENT).connect(["10.0.0.1:1980", "10.0.0.2:1980"])?;
let reply = client.query(types::ECHO_REQUEST, "hello").timeout(Duration::from_secs(10)).wait()?;
println!("{}", String::from_utf8_lossy(&reply.message.payload));
```

`SyncClient` has no thread of its own: every call runs the client's loop
inside it until the call's condition holds, and between calls nothing
runs. So its peer type is marked `is_passive` in the rules file; it is
the one class that needs the mark. It is `Send`, not `Sync`: it belongs to
one thread at a time.

- `SyncClient::builder(peer_type)` takes, before the client exists,
  `on_drop(observer)` ([below](#messages-the-library-gives-up-on)),
  `key_filters(filters)` and `routing(routing)`, carried by every welcome
  so that they are in effect from the first message, and
  `on_unexpected(handler)` (below); `build()` makes the client, connected
  to nothing, and `connect(addresses)` connects it to every address at
  once, each handshake waited for against one `DEFAULT_TIMEOUT`, those
  that did not come up tried again as the loop runs.
- `connect(address, timeout)` connects to one more multiplexer, waits for
  its handshake, and returns the `Connection`, live or not: the library
  goes on trying. An address is `host:port`, or `[address]:port` for an
  IPv6 address ([addresses as text](#addresses-as-text)); `host` may be a
  name, resolved inside the library on every attempt, each address it has
  tried in turn, for `CONNECT_ATTEMPT_SECONDS` (5 s) at most, so that a
  multiplexer that moved is found at the next reconnect. Nothing fails
  when the multiplexer is unreachable or the name does not resolve yet;
  the connection is tried again every 3 s while the library runs. A
  multiplexer the client has a connection to, live or on its way, keeps
  it. `connect_all(addresses, timeout)` connects to every address at once,
  each handshake waited for against the one `timeout`, so a multiplexer
  that never answers costs it once, and returns how many are registered;
  every address is parsed first, `InvalidArgument` before anything
  connects.
- `disconnect(address)` drops a multiplexer given to `connect()`: its
  reconnect stops, a connection on its way is abandoned, and a live one is
  closed the polite way, what it had not written going in order to one
  other connection, or held for the next ([semantics](semantics.md#failure-modes)).
  It returns whether the client had that multiplexer. It is for a program
  that keeps its own list of multiplexers, when one leaves the list.
- `query(type, payload)` and `query_message(request)` build a query, which
  `wait()` sends; the reply comes back as an `Incoming`, the `message` and
  the `connection` it came on. The algorithm is the one in [how a query is
  answered](query.md): one connection first, then, after a delivery error,
  a timeout or a lost connection, a search on all of them, then the
  request again to the backend found, each stage with its own timeout; the
  request goes out at most twice. `query_message` takes the request as a
  whole, typed by its own `type` and addressed by its own `to`, its empty
  sender filled in, every attempt with an id of its own. The builder takes
  `timeout(duration)` per stage, `no_deadline()`, `to(instance)` ([below](#lanes-pinning-and-addressed-queries)),
  `key(key)`, which the search for a backend carries too, `lane(&lane)`,
  `connection(&connection)` and `on_received(callback)` ([below](#knowing-a-backend-took-the-request)).
  `wait()` fails with `OperationFailed`, `OperationTimedOut`,
  `NotConnected`, or `Backend` for a `BACKEND_ERROR` reply. While it waits
  it reads whatever arrives; a message that is not its reply, a
  `DELIVERY_ERROR` for an earlier event say, goes to the builder's
  `on_unexpected(handler)`, or is logged and dropped, as the C++ and
  Python clients drop it. A late reply to a query that has ended stays to
  be received.
- `send(type, payload)` and `send_message(message)` build a send of an
  event, the way every client sends. The builder takes `to(instance)`,
  `key(key)`, `references(id)`, `workflow(text)`, `all()`, through every
  connection, a multiplexer whose first connection is still in its
  handshake getting its copy at the welcome, `lane(&lane)`,
  `connection(&connection)`, `timeout(duration)`, `no_deadline()` and
  `on_end(callback)`. `queue()` returns the message's id at once: the
  message is queued on one live connection, round robin or the lane's, or
  on every one, waits for room on a full connection, or, with no
  connection live, is held until one comes up, the timeout at most each
  way, and is written as a later call runs the loop; a connection that
  dies with it unwritten hands it, with the rest it had not written, in
  order, to one other, or has it held for the next. One the client gives
  up on is reported ([below](#messages-the-library-gives-up-on)).
  `on_end(|written| ...)` hears how it ended, once, inside a later call
  that runs the loop: `true` once it was written, the first copy for ALL,
  `false` once it was given up on or the client shut down first. `wait()`
  sends it the same way and waits until it reached a socket, the first
  copy for ALL, and returns its id: the way to send from a client that
  may have lost its connection since the last call. It fails with
  `NotConnected` when nothing wrote it with no connection live, or through
  a pinned lane whose connection is gone, the message lost with it, and
  `OperationTimedOut` otherwise.
- `flush_all(timeout)` runs the loop until everything sent before the call
  was written or given up on, what waits for room included, and returns
  whether every one was written: `false` once one was given up on, which
  the drop observer names, or at the timeout, and at once when something
  waits and no connection is live or coming.
- `receive(timeout)` waits for the next message that arrived on its own
  and returns it as an `Incoming`; `OperationTimedOut` when none came,
  `NotConnected` at once for a wait with no deadline when nothing could
  arrive. It runs the callbacks of the changes acknowledged meanwhile
  ([below](#routing-key-filters-and-acknowledgements)).
  `receive_until_acknowledged(timeout)` also ends, with
  `OperationTimedOut`, as soon as a change of the routing or the key
  filters is acknowledged, or the last message held for a connection goes
  (`held_for_a_connection()`): what a loop of receives acts on at once, the
  server classes' loops among them. A change whose last confirmation came
  while messages routed before it were still to be received is
  acknowledged by the receive that takes the last of them, and the next
  wait ends at it, its callbacks run first. What arrives waits in a queue of
  `DEFAULT_INCOMING_QUEUE_MAX_SIZE`, 1024 messages; past it a message is
  dropped and counted by `incoming_dropped()`, its copy through another
  multiplexer still taken.
- `instance_id()`, `peer_type()`, `connections_count()`, `dropped()`,
  `dropped_while_closing()`, the messages the connections read after they
  began closing, which they could only drop, and `orphaned()`
  ([fork](#fork-and-exec)).
- `shutdown(timeout)`, also run by `Drop` with `CLOSE_FLUSH_SECONDS`, first
  writes what was sent before it, running the loop as `flush_all()` does,
  and what the loop sent meanwhile, `timeout` in all; drops and reports
  what is still unwritten then; and runs the loop until every multiplexer
  has closed its side of the connection too, a round trip,
  `CLOSE_READ_SECONDS` at most, so that what was written arrives. After
  it a send, a query, a connect and a receive with no deadline fail with
  `NotConnected`; a receive with a deadline still hands out a message
  received before, and otherwise waits the deadline out,
  `OperationTimedOut`; `flush_all()` returns `true`, nothing being left;
  the routing and key-filter setters return `Ok` and change nothing, a
  `_then` callback never running; `routing_acknowledged()` and
  `connections_count()`, 0, answer.

### Lanes, pinning and addressed queries

The same on `SyncClient` and `ThreadedClient`, and in the other two
libraries; the reasoning is in [the Python
API](api_python.md#lanes-pinning-and-addressed-queries).

- **An addressed query** is a query with `to(instance)`, or a
  `query_message` whose request has `to` set: only that instance ever gets
  it. When a multiplexer reports the instance is not behind it, or the
  connection dies under the wait, the client locates it with a `PING`
  addressed to it on every connection and repeats the request through the
  connection that found it, once; an instance nobody has is
  `OperationFailed` at once, unless the request may still be with it, its
  connection lost under it, when the query waits for that late reply
  until its timeout; one timeout covers the stages. The server classes
  and `ThreadedClient` answer that `PING` whatever their search policy;
  `SyncClient` does not.
- **A lane**, `Lane::new()`, or `Lane::pinned()`, keeps a stream on one
  connection: a query or a send given `lane(&lane)` goes through the
  lane's connection, the first message taking the connection the library
  chose, a query leaving the lane on the connection the reply came
  through. While the lane's connection lives, a message it has no room
  for waits there for room, so the stream keeps its order. A lane that is
  not pinned takes another connection when its own dies: the one that
  took, in order, what the dead one had not written, so the stream has
  one gap or reorder per failover. A pinned lane keeps its first
  connection for good; once it is `closed()`, a query or a send through
  it fails with `NotConnected`, and a message of its that the dying
  connection had not written is reported lost (`ConnectionLost`), never
  handed to another connection. `Lane::through(&connection, pinned)` seeds
  a lane with a connection, the one a reply came on. `connection()`,
  `connected()`, `closed()` and `is_pinned()` read it. Clones are the same
  lane, usable from any thread; a lane keeps no connection alive and the
  library keeps no registry of lanes. A lane that took a connection, and a
  `Connection`, belong to the client they came from: given to another
  client, the call fails with `InvalidArgument`.
- **A connection**, given to `connection(&connection)` on a query or a
  send, is preferred for that message and replaced when gone, as a
  server's reply goes back the way the request came. `Incoming::connection`
  is where a connection comes from; `target()` is the host and port it was
  asked to connect to, `address()` the address in use, `is_live()` whether
  it is live, from any thread.

### Knowing a backend took the request

The same on both clients and in the other libraries, [where the reasoning
is](api_python.md#knowing-a-backend-took-the-request). A query's
`on_received(|backend| ...)` is called with the instance id of each
backend that acknowledges the request with `REQUEST_RECEIVED`, what a
handler's `request.notify_start()` sends, as soon as the acknowledgement
arrives: normally once; again, with the backend's id, when a retry
reached a backend, which says the request may be running twice. On
`SyncClient` it runs inside `wait()`, on the calling thread, so it must
not query or receive through that client; on `ThreadedClient` on the io
thread, so it must be quick. One that panics is logged, and the query goes
on.

### Routing, key filters and acknowledgements

The same calls on both clients. The server classes reach the key-filter
calls and `routing_acknowledged()` through their handle; their routing
changes by `start_draining()`, and the threaded server's `client()` has
`set_routing()` too.

- `set_routing(routing)`: which of a multiplexer's routing paths reach this
  peer, `any`, `all` and `last_resort` ([how a backend
  leaves](leaving.md#what-a-draining-backend-still-takes)), told to every
  multiplexer and carried in every welcome from then on.
- `set_key_filters(filters)`: which keys of each message type reach this
  peer by the rules, a `KeyFilters`, a `BTreeMap` of message type to its
  patterns, RE2 regular expressions of which one must match a key whole; a
  type left out takes every key, one with no pattern none, and a message
  without a key arrives whatever its type ([keys](keys.md)). It replaces
  the filters set before and leaves the routing as it is; set them on the
  builder to have them in effect from the first message. `InvalidArgument`,
  at the call, for a protocol message type or a pattern over
  `MAX_KEY_PATTERN_SIZE`; a pattern a multiplexer refuses is logged by it,
  and by the client as an error.
- `routing_acknowledged()`: whether every multiplexer has the current
  routing and key filters in effect, with everything they routed before
  received.
- `held_for_a_connection()`: how many messages wait for a connection to
  come up, sent while none was live or left by one that died with none to
  take them; each goes out in order once one is registered, or is given up
  on at its own deadline. The server classes' drains wait for them, since
  the close would drop them.
- `set_routing_then(routing, on_acknowledged)` and
  `set_key_filters_then(filters, on_acknowledged)`: the same, and
  `on_acknowledged` called once this change, or a later one, is
  acknowledged so: on `SyncClient` inside a receive, the one after the last
  message routed before was received, or the one waiting when the
  acknowledgement came, and it must not receive or query through the
  client; on `ThreadedClient` on the io thread, after everything routed
  before went to `on_message`, the streams or its query, and never after
  `shutdown()`. One that panics is logged.

## BaseMultiplexerServer

```rust
use mx_multiplexer::{BaseMultiplexerServer, Handler, HandlerResult, Request};

struct Echo;

impl Handler for Echo {
    fn handle_message(&self, request: Request) -> HandlerResult {
        request.reply(types::ECHO_RESPONSE, request.payload().to_ascii_uppercase())?;
        Ok(())
    }
}

let mut server = BaseMultiplexerServer::builder(peers::ECHO_BACKEND, ["10.0.0.1:1980", "10.0.0.2:1980"]).build(Echo)?;
server.serve_forever(Duration::from_secs(1), None)?;
```

A backend is a `Handler`, a trait whose one required method is
`handle_message(&self, request)`, served by either server class.
`BaseMultiplexerServer` runs its loop and the handler on the thread that
calls `serve_forever()`, one message at a time: the fastest of the two,
for quick handlers. The handler is `Send + Sync + 'static`, so the same
type serves on the threaded class unchanged.

- `BaseMultiplexerServer::builder(peer_type, addresses)` takes
  `drain_routing(routing)`, what `start_draining()` tells the
  multiplexers, `key_filters(filters)`, in effect from the first request,
  and `on_drop(observer)`; `build(handler)` makes the server, connected to
  nothing, `InvalidArgument` for an address that does not parse. The drop
  observer and `set_key_filters_then()`'s callback run on the loop's
  thread between its waits, `serve_forever()`'s, `loop_iter()`'s or
  `close()`'s, not while the loop holds the client: they may call the
  handle and answer kept requests.
- `connect()` connects to every address, once, each handshake waited for
  against one `DEFAULT_TIMEOUT`, and returns how many came up; an address
  it could not reach the library goes on trying. `serve_forever()` starts
  the connections itself unless `connect()` did, waiting for none: its
  loop finishes the handshakes, so the backend serves what one multiplexer
  routes to it while another has not welcomed it yet. Call `connect()`
  when something waits for a line you print before it sends, when you
  drive `loop_iter()` yourself, and before you start `serve_forever()` on
  a thread of your own and send at once.
- `serve_forever(poll, drain)` runs the loop: each iteration waits `poll`
  at most for a message, handles it if one came, then calls the handler's
  `periodic_task(&server_handle)`, message or not, and a change of the
  routing or the key filters that every multiplexer acknowledged, a
  drain's included, ends the wait too, so that `periodic_task()` and
  `drained()` see it at once. It returns, the connections closed, when
  `stop()` was called or a drain is over: `Ok(())`, or
  `Err(ServeError::Handler(error))` for a handler failure that
  `on_handler_error()` said ends the server, or `Err(ServeError::Client(error))`
  when the client failed. `drain` caps a drain, `None` for no cap: while a
  drain with a cap runs, a wait lasts no longer than what is left of it, so
  that the cap ends the drain on time whatever the poll.
  `loop_iter(timeout)` is one step, `OperationTimedOut` when no message
  came, for a loop of your own.
- `handle()` is a `ServerHandle`, `Clone + Send + Sync`: what
  `periodic_task()` is given, and what another thread, a signal's watcher
  say, holds. `stop()` makes `serve_forever()` return, its wait woken;
  `start_draining()` and `draining()`; `working()`; `instance_id()`;
  `send(type, payload)` and `send_message(message)` for a message that is
  no reply, routed by its type, as every client sends; `set_key_filters()`
  and `set_key_filters_then()`; `set_batching()` and `clear_batching()`
  ([batching](#batching)); `connections_count()`,
  `routing_acknowledged()` and `held_for_a_connection()`; `dropped()`, the
  requests dropped, here those the client's full receive queue dropped.
  From another thread than the server's own, a call that needs the client
  wakes the loop's wait and has its turn at once, ahead of the loop's next
  wait.
- `instance_id()`, `handler()`, `dropped_while_closing()`; `dropped()`
  and `incoming_dropped()`, the client's counts of what the server sent
  and gave up on and of what its full receive queue dropped, kept past
  the close; and `close(timeout)`, also run by `Drop`: what arrives
  meanwhile and what was read and not handled is refused with
  `DELIVERY_ERROR`, so that its senders retry elsewhere at once, and the
  client shuts down as `SyncClient::shutdown()` does, the last replies
  written first.

**A request.** `Request` has `message()`, `payload()` and `connection()`;
`reply(type, payload)` and `reply_message(message)`, the reply going to the
requester, referencing the request, the way it came, its empty `to`,
`references` and `workflow` filled in from the request; `no_response()` for
a message that needs no reply, an event; `report_error(text)`, a
`BACKEND_ERROR` carrying the text; `notify_start()`, `REQUEST_RECEIVED` at
once, which a query's `on_received` [hears](#knowing-a-backend-took-the-request);
and `answered()`. One reply per request: `references` means "this is the
reply", so a follow-up that is not the reply goes through the handle with
`to` set. A request dropped without a reply or `no_response()` is logged.
A handler may keep the request, it is `Send`, and answer it later from any
thread; on `BaseMultiplexerServer` that reply wakes the loop's wait and
goes out at once.

**A handler that fails**, returning `Err`, any error boxed as a
`HandlerError`, or panicking, gets its requester a `BACKEND_ERROR`
carrying the error's text, unless a reply went out or the message answers
another, one with `references` set, a reply or someone's
`BACKEND_ERROR`: nobody waits for an answer to it, and two backends whose
handlers fail on what they do not expect would answer each other's
reports for good. A Rust or Python requester's query fails with that
error, `Error::Backend` here; a C++ requester gets the message itself.
Then `on_handler_error(&error)` decides: `true`, the default, keeps
serving; `false` ends `serve_forever()` with the error, and so does a
panic in it, with the panic's text.

**The handler's other methods**, each with a default: `periodic_task()`
above; `should_answer_search()`, whether to answer a client's search for
a backend of this type now, yes by default, which on the threaded class
runs on the io thread and must be quick; and `drained(by_default)`,
whether the drain is over, the library's answer given: the drain's cap
passed, or, with every path turned off and no last resort, every
multiplexer confirmed it, nothing is left to handle and nothing the
server sent waits for a connection to come up: with no connection live
the confirmation holds at once, so a reply made while every multiplexer
was down is held for the next connection and written once one is back,
where the close would drop it. A panic out of
one that runs on the serving thread, `periodic_task()` or `drained()`,
closes the server as `close()` does before it goes on up out of
`serve_forever()`, as C++ and Python close theirs.

**Leaving gracefully.** From `periodic_task()`, call
`server.start_draining()` when asked to leave: the backend tells every
multiplexer to route it nothing new by the rules, its drain routing, while
it serves what was already on its way; `serve_forever()` returns once
`drained()` says so, having served what it had read and refused what
arrived later. The drain routing turns every path off by default,
`direct_only_routing()`; `all` kept keeps events coming, `last_resort`
keeps a lone backend serving through its drain, and either makes the
drain last its cap. [How a backend leaves](leaving.md) draws the phases.
The library installs no signal handlers: a program's own sets a flag that
`periodic_task()` reads, or a thread of its own calls the handle's
`start_draining()`, as [examples/echo-rust](../examples/echo-rust) does.

## BaseThreadedMultiplexerServer

`BaseThreadedMultiplexerServer` runs the handlers on worker threads behind
a `ThreadedClient`'s io thread, which heartbeats, reconnects, answers PINGs
and the search, and hands every other message to a bounded queue the
workers take from. When to use it rather than `BaseMultiplexerServer` is in
[the overview](README.md#which-class-to-build-on): a request that may take
longer than the multiplexer's drop interval, several handled at once, or a
handler that blocks on a query of its own.

```rust
let server = BaseThreadedMultiplexerServer::builder(peers::ECHO_BACKEND, addresses).workers(4).build(Echo)?;
server.serve_forever(Duration::from_secs(1), None)?;
```

- The builder takes `workers(n)` (1), `queue_size(n)` (1024, the requests
  waiting for a worker; beyond it a request is dropped, logged at the drop
  lines' rate, its requester waiting out its first stage before the
  search; `usize::MAX` is no limit), `decline_searches_when_full(bool)` (false: searches are
  answered while the server serves; true leaves them unanswered while
  every worker is busy and requests wait), `connect_timeout(timeout)`,
  `drain_routing(routing)`, `options(ThreadedServerOptions)` for all of
  them at once, `key_filters(filters)` and `on_drop(observer)`. `build(handler)`
  makes the client and its io thread, no worker started and nothing
  connected, so that no multiplexer knows the backend until it can serve;
  `InvalidArgument` for no worker or an address that does not parse.
- `connect()` starts the workers and connects to every address at once
  against one `connect_timeout`, once; `serve_forever(poll, drain)` calls it
  first, then only polls, `periodic_task()` every `poll` or sooner when
  woken, by a stop, a drain, an acknowledgement, a drain's last request or
  the last message held for a connection written, and at a drain's cap,
  and closes the server when it returns. It takes `&self`: a server in an
  `Arc` serves on one thread while others hold it. `handle()`, `handler()`
  and `client()`, the server's `ThreadedClient`, for messages that are not
  replies.
- `handle_message()` runs on a worker, `workers` at a time, with every
  message that is not the protocol's own, the `DELIVERY_ERROR`s for the
  server's own messages included, which a handler tells by the type. A
  request a handler keeps is answered from any thread, through the
  server's client: once `close()` has run, the reply fails with
  `NotConnected`. A kept request no longer counts in `pending()` once its
  handler returned, so neither `drained()` nor `close()` waits for it: a
  program that answers later has `drained()` wait for its own work too.
- A request that arrives while the server leaves, routed before the
  multiplexer applied the drain routing, is refused with `DELIVERY_ERROR`,
  so its requester retries at once; one that answers another is dropped.
  The handle's `pending()` counts the requests waiting for a worker or
  being handled, `dropped()` those dropped for a full queue or refused
  while leaving.
- `close(timeout)`, also run by `Drop`, takes no more messages, tells every
  multiplexer the drain routing, answers no search, lets the workers
  finish what is queued, stops them, refuses what no worker took, and
  shuts the client down, the last replies written first. A second call
  returns once the first is done. From a worker it fails with
  `InvalidArgument` rather than join itself: a handler that wants the
  server gone calls the handle's `stop()`.

## ThreadedClient

```rust
use mx_multiplexer::ThreadedClient;

let client = ThreadedClient::builder(peers::MY_SERVICE)
    .on_message(|incoming| handle(incoming.message))
    .connect(["10.0.0.1:1980"])?;
let reply = client.query(types::ECHO_REQUEST, "hello").wait()?;
client.query(types::ECHO_REQUEST, "hello").then(|result| { /* on the io thread */ })?;
client.send(types::SOME_EVENT, "payload").queue()?;
```

`ThreadedClient` runs the connections on an io thread of its own, so
heartbeats flow and reconnects happen whatever the program does, the peer
type need not be passive, and any thread may use it. Its handles are
`Clone + Send + Sync`, clones of one client.

- `ThreadedClient::builder(peer_type)` takes `on_message(callback)`,
  `on_drop(observer)`, `search_policy(answer)`, `key_filters(filters)` and
  `routing(routing)`; `build()` starts the io thread, connected to nothing,
  and `connect(addresses)` connects it to every address at once, each
  handshake waited for against one `DEFAULT_TIMEOUT`.
- A query is built as on `SyncClient`, with the same options, and ends in
  one of three ways: `wait()` blocks for its end, from any thread but the
  io thread, where it fails with `OnIoThread` rather than wait for good;
  `.await`, on any executor; or `then(|result| ...)`, which returns at once
  and calls back on the io thread, where the callback must be quick, or at
  once, on the calling thread, with `NotConnected` after `shutdown()`; one
  made from a callback while the shutdown writes out is refused too, its
  callback run on the io thread. Any
  number of threads may query at once; replies are matched by the ids
  they reference. A late reply to a query that ended, for twice the longer
  of its timeout and 10 s, 10 s standing for no deadline, is dropped
  whatever its type, the ids of 131072 attempts and searches kept at most,
  one per attempt and per search, so a follow-up that is not the reply must not reference the request
  ([semantics](semantics.md#delivery)).
- A send is built as on `SyncClient`, and ends in `queue()`, which returns
  its id at once, the io thread placing it; `written()`, a `Pending<u64>`,
  the id once a copy reached a socket, `NotConnected` when nothing wrote it
  with no connection live, or it was given up on, `OperationTimedOut`
  otherwise; or `wait()`, `written()` waited for. A message that cannot
  be queued yet, no connection being live or the connections' queues full
  (1024 messages each), waits behind those sent before it, its timeout at
  most, and is dropped and reported after that. The io thread takes sends
  and queries in the order they were made.
- `connect(address, timeout)` and `connect_all(addresses, timeout)` return
  a `Pending`: `true` once the handshake is done, at once for a
  multiplexer connected already, `false` as soon as it failed or at the
  timeout, the io thread going on trying; how many are registered. 
  `disconnect(address)` drops a multiplexer as `SyncClient`'s does and
  returns whether the client had it.
- `flush_all(timeout)`, a `Pending<bool>`: whether everything sent before
  the call was written, `false` once one was given up on or at the
  timeout; `true` at once after `shutdown()`: what is left to write is
  the shutdown's, whose result comes once it is over.
- What arrives on its own, not a reply to a query: a `PING` is answered, a
  search for a backend when the search policy says yes, without a policy
  never; the rest goes to every subscription of its type, `subscribe(type)`,
  to the inbox once `messages()` was asked for, and to `on_message`, each
  getting it; with none of them it is logged and dropped. `messages()` and
  `subscribe()` are `Messages` streams of `DEFAULT_INCOMING_QUEUE_MAX_SIZE`
  that drop their oldest when nobody reads them, `dropped()` counting,
  with a warning at the first drop of each run; `recv(timeout)`, `try_recv()` and `next().await`
  read them, from any number of readers, each message going to one, `None`
  once the client was shut down and the stream is empty. Dropping a
  subscription ends it.
- `on_message`, the drop observer, a query's `then` and `on_received`, a
  send's `on_end`, the search policy and `on_acknowledged` run on the io
  thread: they must return quickly; they may send, query with `then()`,
  and call `shutdown()`, which does not wait there, but not wait. One
  that panics is logged with what it was called for, and the client goes
  on; a search policy that panics answers no.
- `shutdown(timeout)` ends every query in flight with `NotConnected`,
  writes what was sent before it, and what the io thread sends meanwhile,
  `timeout` in all, closes the connections the polite way, and ends the io
  thread; the `Pending<()>` it returns completes once the thread has
  ended. From then on a send, a query, a connect, `connections_count()`
  and `routing_acknowledged()` fail with `NotConnected`; `flush_all()`
  completes with `true`; the routing and key-filter setters return `Ok`
  and change nothing, a `_then` callback never running; and `messages()`
  and `subscribe()` give a stream that ends at once.
  Dropping the last handle does it with `CLOSE_FLUSH_SECONDS` and waits,
  but on the io thread itself, from a callback, where it only starts it.

### ThreadedClient awaited: Rust's AsyncClient

Every waiting call of `ThreadedClient` returns a `Pending<T>`, or a builder
that is `IntoFuture`, a future written on `std` alone that the io thread
completes and wakes: `client.query(...).await`, `send(...).written().await`,
`connect(...).await`, `flush_all(...).await`, `shutdown(...).await`, and
`messages.next().await` for a stream. It needs no runtime of its own and
works on any executor, tokio's included, its waker called from the io
thread; the same `Pending` is `wait()`ed for from a thread that does not
await. Dropping the future cancels nothing, as cancelling the await of an
`AsyncClient` query does not: the query runs its stages on, sending its
request again to the backend a search finds, and its reply is dropped. A
query that must end by a deadline takes `.timeout(d)` rather than an
outer timeout around the await. This is what the Python `AsyncClient` is for asyncio:

```rust
async fn handler(client: ThreadedClient) -> Result<String, mx_multiplexer::Error> {
    let reply = client.query(types::ECHO_REQUEST, "hello").await?;
    Ok(String::from_utf8_lossy(&reply.message.payload).into_owned())
}
```

One client per process, shared by every task, as a tokio web server holds
it: [a web server on tokio](recipes/rust_web_server.md) is the recipe.

## Sending

Every client sends the same way, through one mechanism in the core, and the
server classes send through their client:

| Call | Returns | No connection live | A loss is learned from |
|---|---|---|---|
| `send(...).queue()` on either client | at once, the id | held until one comes up, the timeout at most, then dropped | the drop observer and `dropped()`; `on_end(false)`, `on_end(true)` once written |
| `SyncClient`'s `send(...).wait()` | once written, the id | waits for one, the timeout at most | `NotConnected` when nothing wrote it with no connection live, else `OperationTimedOut` |
| `ThreadedClient`'s `send(...).written()`, `.wait()` | the id once a copy is written | waits | `NotConnected` or `OperationTimedOut`, as above |
| `flush_all(timeout)` | `true` once everything sent before it was written; `false` once one was given up on, or at the timeout | waits | `false`, and the observer for which |
| `shutdown(timeout)`, the servers' `close(timeout)` | after writing what was sent before it, `timeout` at most, `CLOSE_FLUSH_SECONDS` when `Drop` runs it | waits, then drops | the observer, `ShutDown` |

Written means handed to the kernel on a live connection: only a reply says
a message arrived ([semantics](semantics.md#failure-modes)).

## Batching

`set_batching(type, key, batching)` on `SyncClient`, `ThreadedClient` and
the servers' `ServerHandle` sends the events of a message type, or of one
key of it, several in one message, which the receiving library splits
back into one message each, as if each had come alone;
[batching](batching.md) says what goes in a batch, when it goes and what
it changes.

```rust
use mx_multiplexer::Batching;

let batching = Batching { max_delay: Duration::from_millis(5), ..Batching::default() };
client.set_batching(types::METRIC_SAMPLE, None, batching)?;
client.send(types::METRIC_SAMPLE, payload).queue()?;  // in a batch, which goes once full or 5 ms on
```

- `Batching`, at the crate's root, has `max_records`
  (`DEFAULT_BATCH_MAX_RECORDS`, 1000), `max_bytes`
  (`DEFAULT_BATCH_MAX_BYTES`, 1 MiB, measured as `MAX_MESSAGE_SIZE` is)
  and `max_delay`, a `Duration`, zero for no delay, a batch going as soon
  as a connection has nothing left to write; `Batching::default()` is
  those defaults. With `key` `None`, every key of the type batches, each
  apart, and its messages without a key apart again; `Some(key)` sets one
  key, ahead of what is set for its type. `clear_batching(type, key)`
  ends what was set for the type, its keys' settings included, or for
  one key, which then batches as its type says. Each lets every batch go
  first.
- `InvalidArgument`, at the call, for a protocol message type, a key over
  `MAX_KEY_SIZE`, a `max_records` or `max_bytes` of 0, a `max_records`
  over `MAX_BATCH_RECORDS`, a `max_bytes` over `MAX_MESSAGE_SIZE`, or a
  delay over 10⁹ s; on `ThreadedClient` the
  change is made on the io thread right after, ahead of every send made
  after the call.
- What batches is a plain event, no `to`, no `references`, no delivery
  report asked for, no `override_rrules`, sent by a send's `queue()`
  without `lane()`, `connection()` or `all()`, or by a server handle's
  `send()` and `send_message()`. What goes by `wait()` or `written()`, to
  `all()`, through a lane or a connection, with a timeout of zero, a reply
  and a query's request go alone, after every batch.
- `SyncClient` runs its loop only inside its calls: with no delay its
  messages gather only while the socket is full, so give it one. A batch
  then goes when it fills or at the first call after its delay, and
  `flush_all()` and `shutdown()` let it go at once. A `queue()` that only
  joins a batch with a delay runs no loop; the one that fills it, or
  comes after a delay is over, does.
  `BaseMultiplexerServer` is built on a `SyncClient` and wants a delay
  too; its loop runs while `serve_forever()` waits, so the delay ends on
  time between two messages. `ThreadedClient` and
  `BaseThreadedMultiplexerServer` keep the delay on their io thread; with
  no delay their messages gather only while the program sends faster than
  the io thread writes them or the multiplexer is behind, so a delay of a
  few milliseconds batches for sure.
- Each message's `on_end` hears its batch's end; a batch given up on is
  counted in `dropped()` and told to the drop observer once for each
  message in it.

## Messages the library gives up on

Both clients count the messages the program sent that they gave up on,
`dropped()`, and tell an observer of each, the builder's
`on_drop(|id, reason| ...)`, each copy of a message sent to ALL once. The
reasons, `DropReason`:

- `NoRoom`: it waited for room on a full connection, its multiplexer not
  reading, past its timeout, or, sent with no time to wait, found none.
- `NoConnection`: it waited for a connection to come up past its timeout,
  or, sent with no time to wait, found none live.
- `ConnectionLost`: its connection ended and nothing else could take it: a
  pinned lane's, or a copy sent to ALL.
- `ShutDown`: the client shut down before it went.

The drop is counted at once, in `dropped()`, and the observer hears of it
in the pass that runs the sends' callbacks, ahead of them, once the handler
the drop happened in is done: inside whichever `SyncClient` call runs the
loop next, and on a `ThreadedClient`'s io thread, where it must return
quickly. A flush returns once the observer heard of every drop before it. Each drop is also logged, the first of a kind at once,
naming the message's id and type, and the rest counted in a line a
second. A message the library wrote is not dropped, whatever happens to it
next.

## Addresses as text

`parse_endpoint(text, default_port)` reads a multiplexer's address as
[mxcontrol](mxcontrol.md#addresses) and the other libraries take it:
`host:port`, or `[address]:port` for an IPv6 address, the host coming back
without the brackets, empty for `:1980` for the caller to fill in;
`default_port`, when given, lets the port be left out. A malformed text is
`InvalidArgument` saying why: an IPv6 address out of brackets, since
`::1:1980` could be either, brackets around anything else, a port that is
not a number from 0 to 65535. `format_endpoint(host, port)` puts an IPv6
address in brackets. Every call that connects takes the text itself.

## Logging

The library logs through the [log](https://crates.io/crates/log) facade,
the Rust project's own: connections made and lost, drops, refusals and
failures, at the levels the C++ library logs them, under targets named
after the crate's modules. A program sees them once it installs a logger,
any implementation of `log`; without one they go nowhere. On the path a
message takes when all is well only `trace` lines are logged, a query's
request, which a logger leaves off unless asked for them.

## Threads

`SyncClient` is `Send`, not `Sync`: one thread at a time uses it, and
clients for parallel work are one per thread, as the test roles' are in
[tests/roles/rust/src/client.rs](../tests/roles/rust/src/client.rs).
`BaseMultiplexerServer` runs on the thread that calls `serve_forever()`;
its handle reaches it from any thread. `ThreadedClient` and
`BaseThreadedMultiplexerServer` are made of threads: any thread may call
them, their callbacks and handlers run on the io thread and the workers.

## Lifetimes

A client or a server ends when it is dropped, or earlier with `shutdown()`
or `close()`: dropping either client runs its shutdown, writing what was
sent first, `CLOSE_FLUSH_SECONDS` at most, and waiting for the
multiplexers' side of the close, `CLOSE_READ_SECONDS` at most; dropping a
server closes it. A `ThreadedClient` lives as long as a handle of it: one
held by one of its own callbacks, an `on_message` that sends say, keeps it
alive until `shutdown()`. A process that ends without dropping it,
`std::process::exit()` or a fatal signal, loses what the client had not
written.

## Fork and exec

**Fork.** A client inherited by a forked child is an orphan there: its io
thread does not exist in the child, its locks may be held by threads that
do not exist there either, and its sockets are shared with the parent.
Every call on it fails with `UsedAfterFork` first, before it takes any
lock, and so does a send or a query through a `Lane` made before the
fork, whose getters answer as for a lane nothing goes through:
`connection()` None, `connected()` false, `closed()` true. The calls
that cannot fail answer for the child, before any lock too:
`messages()` gives a stream that ends at once, never the parent's
inbox, and a threaded server's `pending()` is 0. `orphaned()` tells
without failing. Dropping it, or `shutdown()`, closes the child's copies
of its sockets with `close(2)` alone, once however many calls do it,
which leaves the parent's connections open, and leaks the rest on
purpose, the parent's threads with their handles; a server class's
`close()` and drop do the same. A `pthread_atfork` child handler,
registered when the first client is made, with no lock a child forked
meanwhile could find held, bumps a fork generation every client
compares with the one it was made in: one load per call. Create clients
after forking. [The C++
page](api_cpp.md#threads) says what a forked child of a process with
threads can still wait on, the resolver's and the dynamic loader's locks;
the same holds for a Rust program, whose library looks names up on a
thread of its own.

**Exec.** Every socket a client opens is closed on exec, as the standard
library and mio open them: a program the process runs holds none of the
connections.

## Testing

The crate exports no test peers. A Rust service is tested as it runs, as
a C++ one is: its binary, started with production's flags against real
multiplexers by `multiplexer.testing` from Python. `Cluster` gives the
multiplexers, `Process` runs the binary, and `FakePeer` and `TestClient`
play its peers, under Bazel with the `rust_binary` as the data of a
`py_test`, or after `pip install mx-multiplexer` beside `cargo build`.
[Test a service binary](recipes/test_a_service.md) has the test.
Two other ways:

- **In a scenario.** A Rust binary that follows the [role
  contract](../tests/README.md#playing-a-role-with-your-own-binary) plays a
  role in `mx_integration_test`, given by its label, against real
  multiplexers and the shipped roles in Python, C++ and Rust; the Rust
  test roles in [tests/roles/rust](../tests/roles/rust) are such
  binaries.
- **Against a multiplexer of the test's own.** `mxcontrol run_multiplexer
  --address 127.0.0.1:0 --port-file FILE --rules RULES` listens on a port
  the system picks and writes it to the file; freezing it with `SIGSTOP`
  stalls it without closing anything. The crate's own tests do this, in
  [rust/mx-multiplexer/tests/support/mod.rs](../rust/mx-multiplexer/tests/support/mod.rs).

A test that holds up under load counts deliveries rather than timing
them, as [the Python page](api_python.md#tests-that-hold-up-under-load)
explains.
