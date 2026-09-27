# Using the C++ library

`multiplexer::SyncClient` in [multiplexer/client.h](../multiplexer/client.h)
is the class a program on one thread connects with. It is an alias of the
class `multiplexer::Client`, its name up to 2.3.1, which compiler messages
show and a forward declaration must name (`class Client;`), since an alias
cannot be forward-declared. `multiplexer::backend::BaseMultiplexerServer` in
[multiplexer/backend/base_multiplexer_server.h](../multiplexer/backend/base_multiplexer_server.h)
is one class a backend can be built on. The constants generated from the
[rules file](rules.md) are in `multiplexer/multiplexer.constants.h`, as
`multiplexer::peers::*` and `multiplexer::types::*`, both `std::uint32_t`.

Bazel targets: `@mx//multiplexer:client`,
`@mx//multiplexer/backend:base_multiplexer_server` and
`@mx//multiplexer:multiplexer_cc_constants`. The library uses standalone Asio and
protocol buffers; a consuming workspace gets both through `mx_dependencies()`
and `mx_setup()`, see [examples/README.md](../examples/README.md). The
complete example is [examples/echo](../examples/echo).

## Messages

`multiplexer::MultiplexerMessage` is the generated protocol buffer class from
[Multiplexer.proto](../multiplexer/Multiplexer.proto). The fields that matter
are the same as in Python: `type`, `message` (a `std::string` of bytes),
`id`, `from`, `to`, `references`, `workflow`. Setters are `set_type()`,
`set_message()` and so on. `id` is drawn by the library per attempt: a query
sent again after a timeout, a search or a lost connection carries a new id,
so a backend that must not do the same work twice keys on the payload, not
on `id()`. A message you pass to a query is never changed, since the library
sends copies: `SyncClient` sends the first attempt under the message's own id,
`ThreadedClient` draws one for every attempt, the first included, and in
both a reply's `references()` names the attempt it answers.

## SyncClient

```cpp
#include "multiplexer/client.h"
#include "multiplexer/multiplexer.constants.h"

multiplexer::SyncClient client(multiplexer::peers::ECHO_CLIENT);
client.connect("10.0.0.1", 1980);
client.connect("10.0.0.2", 1980);
multiplexer::IncomingMessage reply = client.query("hello", multiplexer::types::ECHO_REQUEST, 10);
std::cout << reply.third->message() << "\n";
client.shutdown();
```

- `set_drop_observer()` and `dropped()` are under [Messages the library
  gives up on](#messages-the-library-gives-up-on).
- The constructor takes the peer type and creates its own `io_service`; the
  overloads taking an `asio::io_service` share yours. The client runs
  that service only inside its calls, so the peer type must be `is_passive`;
  this is the one class that needs the mark, every other runs the loop
  all the time
  in the rules file.
- `connect(host, port, timeout = 10)` connects, performs the handshake and
  returns a `ConnectionWrapper`. `host` is an address or a name; a name is
  resolved inside the library, on every attempt, and each address it has
  is tried in turn, so a multiplexer that moved is found at the next
  reconnect. It does not throw when the multiplexer is unreachable or the
  name does not resolve yet; the connection is retried every 3 s while the
  library runs. `async_connect(host, port)` returns at once;
  `wait_for_connection(wrapper, timeout)` waits for it. The wrapper's
  `target()` is the host and port given, `endpoint()` the address in use.
- `query(payload, type, timeout = 10, lane = nullptr)` and `query(mxmsg,
  timeout, lane, probe)` send a request and return an `IncomingMessage`, a
  triple whose `third` is a `shared_ptr<MultiplexerMessage>` with the
  reply and whose `second` is the connection it came on. The algorithm is
  the one in [how a query is answered](query.md): one connection first,
  then a search on all of them, then the request again to the backend
  found, each stage with its own `timeout`. A message with `to` set is an
  addressed query, with one `timeout` for its stages, see
  [below](#lanes-pinning-and-addressed-queries). Throws
  `SyncClient::OperationFailed` when no backend can be found,
  `SyncClient::OperationTimedOut` when a stage runs out of time,
  `SyncClient::NotConnected` when no connection is live. All three derive from
  `SyncClient::MxClientError`, which derives from `std::exception`.
- `queue(mxmsg, timeout, lane = nullptr, done)` and `queue_all(mxmsg,
  timeout, done)` send an event the way every client sends, `ThreadedClient`'s
  `send(msg)` included, and return at once: the event is queued on one
  live connection, round robin or the lane's, or on every one, waits for
  room on a full connection, or, with no connection live, is held until
  one comes up, `timeout` seconds at most each way, and is written as a
  later call runs the loop. A connection that dies with it unwritten
  hands it to another, or has it held for the next. One the client gives
  up on is reported ([Messages the library gives up
  on](#messages-the-library-gives-up-on)). `done`, when given, hears how
  the event ended, once, inside a later call that runs the loop: 1 once
  it was written, the first copy for ALL, 0 once it was given up on or
  `shutdown()` came first, as Python's `send_message(callback=)` does.
  They return the event's tracker, the first copy's for ALL, null, `done`
  never called, only when nothing may take it: a pinned lane whose
  connection is gone, or the client shut down.
- `send(mxmsg, timeout, lane = nullptr)` writes an event through one
  connection and returns the one that wrote it, within `timeout`: sent as
  `queue()` sends, then waited for, so a connection that dies under the
  write, or a client with no live connection, costs the reconnect delay,
  not the event: the way to send an event from a client that may have lost
  its connection since the last call. A connection that dies with the
  event still queued hands it to another, which is the one returned and
  the one a reply comes back through. It throws `NotConnected` when
  nothing wrote it with no connection live, or through a pinned lane
  whose connection is gone, its message lost with it, and
  `OperationTimedOut` otherwise, as the Python clients raise; the C++
  `ThreadedClient`'s flushing `send` returns 0 instead. A negative
  `timeout` waits as long as the write takes, its message with no
  deadline either.
  `send(mxmsg, connection, timeout)` prefers that connection.
- `schedule_one(mxmsg, timeout)` queues an event on one connection and
  returns a `ScheduledMessageTracker`, null when no connection is live,
  since it holds nothing. `schedule_one(mxmsg, wrapper, timeout)` queues
  it on that connection while it is live, waiting there for room, and on
  another once it is gone; with none live it runs the loop up to
  `timeout` for one to come up and throws `NotConnected` if none did, so
  it never returns null; `queue(mxmsg, timeout,
  std::make_shared<Lane>(wrapper))` is the form that holds the message
  instead. `schedule_all(mxmsg, timeout)` queues it on every connection
  and returns how many, 0 when none is live. A message a full connection
  cannot take (1024 queued) waits for its room, in order, `timeout`
  seconds at most, as in `ThreadedClient`; it goes in while a later call
  runs the loop, and is dropped and reported after its timeout. On an
  idle connection a message is written inside the call that queues it,
  and the ones behind it while a later call runs the loop:
  `flush(tracker, timeout)` runs the loop until that message was written
  or dropped, or `timeout` passed, the tracker saying which;
  `flush_all(timeout)` until everything sent before the call was written
  or given up on, what waits for room included, true only when every one
  was written. A
  tracker answers `in_queue()`; `is_sent()`, written to the socket, which
  the kernel then holds and the multiplexer may not have yet; and
  `is_lost()`, dropped: its connection ended before writing it, or it
  waited past its timeout.
- `receive_message(timeout = -1)` waits for the next message and returns a
  pair of the message and its connection; `-1` waits forever. It throws
  `NotConnected` at once when nothing could arrive: no connection and none
  on its way, on a client never connected, made with no address, or shut
  down.
- `set_routing(const Routing&)` and `routing_acknowledged()`: which of a
  multiplexer's routing paths reach this peer, `any`, `all` and
  `last_resort`, told to every multiplexer and carried in every welcome
  from then on, and whether every one has it in effect; what a backend's
  drain uses, see [How a backend
  leaves](leaving.md#what-a-draining-backend-still-takes). On
  `ThreadedClient` too, from any thread.
- `refuse_arrivals()` and `refuse_unread()`: how the server classes leave.
  From the first on, a request that arrives is refused at once with the
  `DELIVERY_ERROR` a multiplexer sends for a peer that is gone, so that its
  sender retries elsewhere, and a reply is dropped; what was read before
  stays to be received. The second refuses what was read and not received
  yet.
- `instance_id()`, `client_type()`, `connections_count()`, `random64()`, and
  `shutdown(timeout = CLOSE_FLUSH_SECONDS)`, also run by the destructor:
  it first writes what was sent before it, running the loop as
  `flush_all(timeout)` does, and what the loop sent meanwhile, a server's
  refusals of what it read, a second in all by default, and drops and
  reports what is still unwritten then, at once with 0, as every client
  and server class ends; then it runs the loop until every multiplexer
  has closed its side of the connection too, a round trip,
  `CLOSE_READ_SECONDS` at most, so that what was written arrives
  ([semantics](semantics.md#failure-modes)).

A message built by hand must carry `set_id(client.random64())` and
`set_from(client.instance_id())`, or the receiving library drops it. The
`query(payload, type)` overload does that for you.

### Lanes, pinning and addressed queries

The same on `SyncClient` and `ThreadedClient`; the reasoning is in
[the Python API](api_python.md#lanes-pinning-and-addressed-queries).

- **An addressed query** is a `query(mxmsg, ...)` whose message has
  `set_to(instance_id)`: only that instance ever gets it. When a
  multiplexer reports the instance is not behind it, or the connection
  dies under the wait, the client probes for it on every connection and
  repeats the request through the connection that found it; an instance
  nobody has is `OperationFailed` (`FAILED`) at once; one `timeout`
  covers the stages. The probe is `PROBE_SEARCH` by default, a
  `BACKEND_FOR_PACKET_SEARCH` addressed to the instance, which reaches it
  whatever its routing, as every addressed message does, or `PROBE_PING`,
  which the server classes and `ThreadedClient` all answer, so it also
  finds a peer that serves no requests. The library sets `report_delivery_error` on the request. The old
  behaviour of `SyncClient::query` with `to`, a search by type and the
  request to whichever backend answered, is gone.
- **A lane**, `multiplexer::Lane` in `multiplexer/basic_client.h`, held as
  `LanePtr` (a `std::shared_ptr<Lane>`), is a soft, late pin, or, made
  pinned, the hard one; it keeps a stream on one connection: `send(msg, lane)`, `send(msg, lane, timeout)` and
  `query(..., lane)` go through the lane's connection, the first message
  taking the connection the library chose, a query leaving the lane on
  the connection the reply came through. While the lane's connection
  lives, a message it has no room for waits there for room, so the stream
  keeps its order. A lane that is not pinned takes another connection when
  its own dies. `Lane(true)` is pinned: once it is `closed()` the lane
  refuses: `SyncClient::send` and `ThreadedClient`'s `send(msg, lane)`
  throw `NotConnected`, `queue()` returns a null tracker, a flushing
  `ThreadedClient` `send` returns 0, and `query` ends `NOT_CONNECTED`
  (`NotConnected` on `SyncClient`); a `ThreadedClient` send whose lane's
  connection goes after the call is dropped and reported
  (`CONNECTION_LOST`). A pinned message a dying connection had not written
  is reported lost, never handed to another connection
  (`RawMessage::pinned`). A pinned lane's send that runs out of time
  while its connection lives is a timeout, `OperationTimedOut` on
  `SyncClient`, as on any lane. `Lane(connection, pinned)`
  seeds a lane. `connection()`, `holds_connection()`, `connected()`,
  `closed()`, `pinned()` read it. A lane holds its connection weakly and
  the library keeps no registry, so it lives as long as your `LanePtr`; a
  query in flight holds it until it ends. Safe to share with the io
  thread.
- **A connection**, `send(msg, connection)`, `send(msg, connection,
  timeout)` and `query(msg, connection, ...)`, is preferred for that
  message and replaced when gone, as `BaseMultiplexerServer::send_message`
  replies the way the request came. `Result::reply.second` and
  `IncomingMessage::second` are where a connection comes from.

## BaseMultiplexerServer

```cpp
#include "multiplexer/backend/base_multiplexer_server.h"
#include "multiplexer/multiplexer.constants.h"

using multiplexer::backend::BaseMultiplexerServer;
using mx::util::kwargs::Kwargs;

class Echo : public BaseMultiplexerServer {
public:
  Echo(const multiplexer::backend::MultiplexerAddresses &addresses)
      : BaseMultiplexerServer(addresses, multiplexer::peers::ECHO_BACKEND) {}

protected:
  void handle_message(multiplexer::MultiplexerMessage &mxmsg) override {
    std::string payload = mxmsg.message();
    // ... work ...
    send_message(Kwargs().set("message", payload).set("type", multiplexer::types::ECHO_RESPONSE));
  }
};
```

The constructor takes a vector of `(host, port)` pairs and makes the
instance id; it must be called from a subclass because it is protected.
`connect()` connects to each address, once, and `serve_forever()` calls it
first, so no multiplexer knows the backend before it can serve, and a
program that only constructs and serves never calls it. Call it yourself
when something waits for a line you print before it sends, a test that
reads `ready` from your stdout or a notebook that greps your log, so that
the line means reachable: the echo backend does. Call it before you
start `serve_forever` on a thread of your own and send at once, as a
test does: until that thread has connected, the first request finds no
backend and fails. Call it too when you
drive `loop_iter` yourself instead of `serve_forever`, and in a test that
wants a backend connected without a thread serving it. A second call does
nothing. The second constructor takes a `SyncClient*` you created and
connected, for backends that also act as clients.
`serve_forever(poll = 1.0f, drain_seconds = 0.0f)` runs the loop:
each iteration waits up to `poll` seconds for a message, handles it if one
came, then calls the virtual `periodic_task()`, message or not, so anything
checked there takes effect within one poll. It returns, with the
connections closed, when the public `working` flag is cleared or a drain is
over. `loop_iter(timeout)` does one step and throws
`SyncClient::OperationTimedOut` after `timeout` seconds. The thread that calls
`serve_forever` becomes the backend's thread, whichever thread built it;
in debug builds a later call from another thread fails an assertion. A
program driving `loop_iter` itself from another thread calls
`SyncClient::bind_to_current_thread()` and then `connect()` first.

`send_message(Kwargs)` takes named arguments, because the message has many
optional fields. Keys and their exact types:

| Key | Type | Default while handling a request |
|---|---|---|
| `message` | `std::string`, `const std::string *`, or `const MultiplexerMessage *` for a message you built yourself | required |
| `type` | `std::uint32_t` | required unless `message` is a whole message |
| `to` | `std::uint64_t` | the requester's instance id |
| `references` | `std::uint64_t` | the request's id |
| `workflow` | `std::string` or `const std::string *` | the request's workflow |
| `multiplexer` | `int` `BaseMultiplexerServer::ONE` or `ALL`, or a `ConnectionWrapper` | the connection the request arrived on |

Outside a handler, from `periodic_task()` say, there are no such
defaults: `to` and `references` are 0 and the message goes through one
connection, routed by its type like any client's. The message is sent as
`SyncClient::queue()` sends: placed, or held while no connection is live,
and reported if given up on, so a reply through a connection that is gone
waits for another rather than holding the loop. The `std::any` it returns
holds the message's `ScheduledMessageTracker`, the first copy's for `ALL`.

`Kwargs` stores each value by its static type, so pass exactly the type in
the table: the generated constants already are `std::uint32_t`, but a
literal or an `int` needs a cast. A wrong type fails an assertion in a debug
build.

**Leaving gracefully.** From `periodic_task()`, call `start_draining()`
when asked to leave: the backend tells every multiplexer to route it
nothing new by the rules, its drain routing, so no request and no search
is sent to it, while it serves what was already on its way.
`serve_forever` returns once the virtual `drained()` says so, having
served what it had read by then and refused what arrived later, however
fast it comes: by default once every multiplexer confirmed, so nothing
more is coming, or `drain_seconds` after the drain started at the latest; override it to
wait for a condition of your own. `set_drain_routing(routing)` before the
drain changes what it asks for: `all` kept keeps events coming,
`last_resort` keeps a lone backend serving through its drain, and either
makes the drain last its `drain_seconds`. [How a backend
leaves](leaving.md) draws the phases and what each costs. Overriding
`should_respond_to_backend_for_packet_search()` puts a condition of your
own behind the search; a drain needs none. The library installs no signal handlers; a handler of
your own must only set a `sig_atomic_t` that `periodic_task()` reads, as
[examples/echo/backend.cc](../examples/echo/backend.cc) does. A C++ process
may rely on a signal like that; a Python process should not, see the
[FAQ](faq.md).

A handler that throws is reported to the requester with `BACKEND_ERROR`,
then the virtual `on_handler_exception(const std::exception &)` decides:
true, the default, keeps serving; false lets the exception propagate out
of `serve_forever`.

`no_response()` marks a message as needing no reply; `notify_start()` sends
`REQUEST_RECEIVED` to the requester at once; `parse_message<SomeProto>(mxmsg)`
parses the payload; `report_error(message)` answers the request with
`BACKEND_ERROR`. An exception escaping `handle_message` is logged and, if no
reply went out yet, reported the same way, so the requester fails at once
rather than timing out; a reply that threw, or that no connection took,
did not go out. A report that fails is logged, the requester waiting out
its timeout, and `on_handler_exception()` decides all the same; the
backend keeps serving. A Python requester sees
`BackendError`; a C++ requester's `query()` returns the `BACKEND_ERROR`
message itself, so check `reply.third->type()` when the backend may fail.
`close(timeout = CLOSE_FLUSH_SECONDS)` ends the connections as
`SyncClient::shutdown(timeout)` does, what is still queued, the last
replies, written first; a request that arrives meanwhile, or was read and
will not be handled, is refused with `DELIVERY_ERROR`, so that its sender
retries elsewhere at once.

## BaseThreadedMultiplexerServer

`multiplexer::backend::BaseThreadedMultiplexerServer` in
[multiplexer/backend/base_threaded_multiplexer_server.h](../multiplexer/backend/base_threaded_multiplexer_server.h)
is the server class whose handlers run on worker threads behind a
heartbeating io thread; target `@mx//multiplexer/backend:base_threaded_multiplexer_server`.
When to use it rather than `BaseMultiplexerServer` is in
[the Python API](api_python.md#basethreadedmultiplexerserver): a request
that may take longer than the multiplexer's drop interval, several handled
at once, or a handler that blocks on a query of its own.

```cpp
#include "multiplexer/backend/base_threaded_multiplexer_server.h"

using multiplexer::backend::BaseThreadedMultiplexerServer;
using multiplexer::backend::RequestPtr;

class Echo : public BaseThreadedMultiplexerServer {
public:
  Echo(const multiplexer::backend::MultiplexerAddresses &addresses, const Options &options)
      : BaseThreadedMultiplexerServer(addresses, multiplexer::peers::ECHO_BACKEND, options) {}

protected:
  void handle_message(const RequestPtr &request) override {
    request->reply(upper(request->mxmsg().message()), multiplexer::types::ECHO_RESPONSE);
  }
};

multiplexer::backend::ThreadedServerOptions options;
options.workers = 4;
Echo(addresses, options).serve_forever();
```

- `ThreadedServerOptions`: `workers` (1), `queue_size` (1024, the requests
  waiting for a worker; beyond it a request is dropped with a warning, as
  the multiplexer's full queue drops; one arriving while the server is
  leaving, routed before the multiplexer applied the drain routing, is
  refused with `DELIVERY_ERROR`, so its requester retries at once, and one
  that answers another is dropped, since refusing a reply could start a
  loop, [how a backend leaves](leaving.md#what-stays)),
  `decline_searches_when_full` (false: searches are answered while the
  backend serves, none once `close()` began; true leaves them unanswered
  while every worker is busy and requests wait), `connect_timeout` and `drain_routing` (what
  `start_draining()` tells the multiplexers; `any` and `all` off by
  default).
- The constructor only makes the instance id; `connect()` starts the
  workers and connects, once, and `serve_forever()` calls it first, so
  nothing reaches `handle_message()` before your constructor has
  finished, and no multiplexer knows the backend until it can serve.
  `instance_id()` is valid from construction. When to call `connect()`
  yourself is as for `BaseMultiplexerServer` above: something waits for
  a line you print before it sends, a test wants the backend connected
  without serving it, or you start `serve_forever()` on a thread of your
  own and send at once.
- `handle_message(const RequestPtr &)` runs on a worker. The `Request`,
  held by `shared_ptr` so a handler may keep it and answer from another
  thread later, has `mxmsg()`, `connection()`, `reply(payload, type)`,
  `reply(MultiplexerMessage)` (id, from, to, references and workflow
  filled in when empty), `no_response()`, `report_error(message)`,
  `notify_start()` and `parse_message<T>()`; a request destroyed without a
  reply or `no_response()` logs a warning. The reply goes the way the
  request came, or another way when that connection is gone. One reply
  per request: `reply()` sets `references`, which a requester built on
  `ThreadedClient` uses to drop late replies, so a follow-up that is not
  the reply goes through `client().send()` with `to` set and no
  `references`. A request kept for later no longer counts in `pending()`
  once its handler returned, so neither `drained()` nor `close()` waits
  for it: a program that answers later overrides `drained()` to wait for
  its own work too.
- `serve_forever(poll, drain_seconds)`, `stop()`, `start_draining()`,
  `draining()`, `drained()`, `periodic_task()`,
  `on_handler_exception()` and `should_respond_to_backend_for_packet_search()`
  are `BaseMultiplexerServer`'s, with the same meanings, `drained()` also
  waiting for the queue to be empty; `close(timeout)` takes
  no more messages, lets the workers finish the queue, and ends the
  connections as `ThreadedClient::shutdown(timeout)` does, the last
  replies written first, and throws `std::logic_error` from a handler, on a
  worker, rather than join itself, during another thread's `close()` too,
  `stop()` being the call for that; from another thread it makes a
  `serve_forever()` running there return, quietly, even while it still
  connects, and a second call returns once the first is done;
  `pending()`, `dropped()`, `instance_id()` and `client()` for messages
  that are not replies. A handler that throws gets the requester
  `BACKEND_ERROR`, unless its reply went out, a reply that threw being
  none, and a report that fails is logged; `false` from
  `on_handler_exception()` makes `serve_forever()` return and rethrow.

## ThreadedClient

`multiplexer::ThreadedClient` in
[multiplexer/threaded_client.h](../multiplexer/threaded_client.h) runs the
connections on a thread of its own, so heartbeats and reconnects happen
without the program calling in, the peer type need not be passive, and any
thread may use it. Target `@mx//multiplexer:threaded_client`.
`set_drop_observer()` and `dropped()` are under [Messages the library gives
up on](#messages-the-library-gives-up-on).

```cpp
multiplexer::ThreadedClient client(multiplexer::peers::MY_SERVICE,
                                   [](const multiplexer::IncomingMessage &m) { handle(*m.third); });
client.connect("10.0.0.1", 1980);
multiplexer::ThreadedClient::Result r = client.query("hello", multiplexer::types::ECHO_REQUEST, 10);
if (r.outcome == multiplexer::ThreadedClient::REPLIED) use(r.reply.third->message());
client.query("hello", multiplexer::types::ECHO_REQUEST, [](const multiplexer::ThreadedClient::Result &r) { ... });
client.send(client.new_message(multiplexer::types::SOME_EVENT, "payload"));
client.shutdown();
```

- `query(payload, type, timeout, lane)` blocks and returns a `Result`: `outcome`
  is `REPLIED`, `TIMED_OUT`, `FAILED` (no backend anywhere, or the
  addressee gone), `NOT_CONNECTED` or `SHUT_DOWN`, and `check()` returns
  the reply or throws the exception `SyncClient::query` would have. `query(msg,
  timeout, lane, probe)` takes the request as a whole, `to` included, and
  sets its id and from per attempt: the addressed form, and
  `query(msg, connection, ...)` prefers a connection, see
  [above](#lanes-pinning-and-addressed-queries). Any number of threads may
  call it at once; replies are matched by the ids they reference. Called
  on the io thread, from a callback, it throws `std::logic_error` rather
  than deadlock. The callback form returns at once and runs the callback
  on the io thread. A request that cannot be queued waits the way a
  message does (below), and the query ends `TIMED_OUT` when a connection
  was live but had no room in time, `NOT_CONNECTED` when none was.
- `send(msg)` and `send_all(msg)` queue a message on one or every
  connection and return at once; the io thread writes it right after. A
  message that cannot be queued yet, no connection being live or the
  connections' queues full (1024 messages each), waits there, behind those
  sent before it, until a connection comes up or has room, for 10 seconds
  at most, and is dropped and reported after that (see [Messages the
  library gives up on](#messages-the-library-gives-up-on)): `send_all`
  gives every live connection its copy, a full one as soon as it has
  room, and a lane waits for room on its own connection while that lives.
  `send(msg, lane)` and `send(msg, connection)` choose the connection.
  `send(msg, lane, done)`, `send(msg, connection, done)` and
  `send_all(msg, done)` call `done(written)` once on the io thread, 1
  when the message was written, the first copy for `send_all`, 0 when it
  was given up on or `shutdown()` came first; `send(msg, nullptr, done)`
  for a message with no lane.
  All are safe from callbacks. `send(msg, timeout)`, `send(msg, lane,
  timeout)`, `send(msg, connection, timeout)` and `send_all(msg,
  timeout)` are the flushing forms: they wait until the message reached
  the socket, for `send_all` until one copy did, the others going out
  from their connections' queues, a connection that dies under it
  handing it to another or having it held, and return 1 once a copy is
  written, 0 when none was by `timeout` or the message was given up on,
  a pinned lane's connection being gone for instance; not from
  callbacks. After `shutdown()` every send throws `NotConnected`. The
  serialized forms, what the Python binding uses, take a callback instead of
  the wait: `send_serialized(serialized, lane, timeout, done)` and
  `send_all_serialized(serialized, timeout, done)` return at once and call
  `done(written)` once on the io thread, 1 when the message was written, the
  first copy for every connection, 0 when it was given up on, and reported,
  or `shutdown()` came first;
  `send_serialized_with_callback(serialized, all, timeout, done, lane)` is
  the flushing send so, `done` hearing 1 once a copy is written, 0 when the
  message was given up on, `timeout` passed or `shutdown()` came first.
  `send_serialized_and_wait(serialized, all, timeout, lane, &given_up)` and
  `send_serialized_and_notify(serialized, all, timeout, done, lane)`, with
  `done(written, given_up)`, also say why a flushing send wrote nothing: its
  message given up on, a pinned lane's connection gone or a shutdown, or
  `timeout` passed, the message then waiting `ROOM_GRACE_SECONDS` more
  before it is dropped; the Python clients raise `NotConnected` for the one
  and `OperationTimedOut` for the other on it, and an asyncio layer awaits
  the second. All are safe from callbacks. A message over `MAX_MESSAGE_SIZE`
  is refused where it is sent or queried, with `std::length_error`.
  `new_message()` fills in id and from.
- `flush_all(timeout)` waits until everything sent before the call has
  been written or given up on, what still waits for a connection or for
  room included, or `timeout` seconds, and returns, once the callbacks of
  those sends have run, whether every one was written: false when one
  was given up on, which the drop observer names, or when the time ran
  out. What is sent meanwhile is not waited for, so a flush ends however
  busy the client is, a message a dying connection hands to another
  included. After `shutdown()` it returns true at once, nothing being
  left to wait for. What `shutdown()` does first. Not from callbacks;
  `flush_all_with_callback(timeout, done)` is the same with
  `done(flushed)` on the io thread instead of the wait, safe from
  callbacks: what an asyncio layer awaits.
- The `MessageSink` given to the constructor runs on the io thread with
  every message that is not a reply to a query: events and requests
  addressed to this peer, and a `DELIVERY_ERROR` for a message that was
  not a query, an event whose rule reports errors, one from each
  multiplexer that could not deliver it. Without one such messages are
  logged and dropped; nothing is queued. A late reply to a
  query that already ended, `REQUEST_RECEIVED` for an untracked query, a
  `PING`, which the client answers itself, and a
  `BACKEND_FOR_PACKET_SEARCH`, answered with a `PING` when addressed to
  this instance, answered by type too when a policy set with
  `set_search_policy()` says yes, and dropped otherwise, never reach it.
  The late-reply rule binds the peers that send to this client: `references`
  means "this is the reply", and what references a query this client has
  seen answered (the last 1024) is dropped whatever its type, so a
  follow-up that is not the reply must not reference the request; it is
  addressed to this peer with `to` and correlated in the payload
  ([semantics](semantics.md#delivery)).
- Callbacks and the sink must return quickly; they may start asynchronous
  queries, send, and call `shutdown()`, which does not wait there, but not
  the blocking `query` or `flush_all`. One that throws, the sink, a query's
  or a send's callback, or the search policy, is logged with what it was
  called for, and the client goes on; a search policy that throws answers
  no.
- `shutdown(timeout = CLOSE_FLUSH_SECONDS)`, also run by the destructor,
  fails every query in flight with `SHUT_DOWN`, writes what was sent
  before it, and what the io thread sends meanwhile, a server's refusal
  of what still arrives say, `timeout` seconds in all, as
  `SyncClient::shutdown()` does; any other send meanwhile throws `NotConnected`,
  then closes the connections and joins the thread, which ends once
  every multiplexer has closed its side too, a round trip,
  `CLOSE_READ_SECONDS` at most, so that what was written arrives
  ([semantics](semantics.md#failure-modes)), and once a name lookup in
  progress, for an address given by name, has returned: nothing cuts a
  lookup short, so against a slow or unreachable DNS server the thread
  ends only at the resolver's own timeout. On the io thread itself, from
  a callback or where the last reference to the client was dropped in
  one, it cannot wait for its own thread and leaves it to end on its own,
  the drain included.

Under the hood the io thread owns a `BasicClient`; other threads reach it
through `io_service::post` only, and the clang thread-safety analysis
checks that (`--config=clang`).

## Sending

Every client sends the same way, through one mechanism in the library
(`BasicClient::send`), and the server classes send through their client:

| Call | Returns | No connection live | A loss is learned from |
|---|---|---|---|
| `SyncClient::queue(msg, timeout, lane, done)`, `queue_all(msg, timeout, done)` | at once, a tracker, null when nothing may take it | held until one comes up, `timeout` at most, then dropped | the drop observer and `dropped()`; `done(0)` when given, `done(1)` once written; the tracker's `is_lost()` |
| `ThreadedClient::send(msg)`, `send(msg, lane, done)`, `send_all(msg, done)` | at once | held, as above | the drop observer and `dropped()`; `done(0)` when given, `done(1)` once written |
| `send_serialized(serialized, lane, timeout, done)`, `send_all_serialized(serialized, timeout, done)` | at once | held | `done(0)`, and the observer; `done(1)` once written |
| `SyncClient::send(msg, timeout, lane)` | once written: the connection that wrote it | waits for one, `timeout` at most | `NotConnected` when nothing wrote it with no connection live, else `OperationTimedOut` |
| `ThreadedClient::send(msg, timeout)`, `send_all(msg, timeout)` | 1 once written, the first copy for `send_all`; 0 at the timeout | waits | 0 |
| `flush_all(timeout)` | `true` once everything sent before it was written; `false` once one was given up on, or at the timeout | waits | `false`, and the observer for which |
| `shutdown(timeout)`, the servers' `close(timeout)` | after writing what was sent before it, `CLOSE_FLUSH_SECONDS` at most | waits, then drops | the observer, `SHUT_DOWN` |

`schedule_one()` and `schedule_all()` stay below `queue()`: a null
tracker, or 0, when no connection is live, nothing held. Written means
handed to the kernel on a live connection: only a reply says a message
arrived ([semantics](semantics.md#failure-modes)).

## Messages the library gives up on

Both clients count the messages the program sent that they gave up on,
`dropped()`, and tell an observer of each, `set_drop_observer(observer)`
with a `BasicClient::DropObserver`, `void(std::uint64_t message_id,
DropReason reason)`, each copy of a message sent to `ALL` once. The
reasons, `multiplexer::DropReason` in `multiplexer/basic_client.h`:

- `NO_ROOM`: it waited for room on a full connection, its multiplexer not
  reading, past its timeout.
- `NO_CONNECTION`: it waited for a connection to come up past its timeout.
- `CONNECTION_LOST`: its connection ended and nothing else could take it: a
  pinned lane's, or a copy sent to `ALL`.
- `SHUT_DOWN`: the client shut down before it went.

The observer runs inside whichever `SyncClient` call runs the loop when the
drop happens, and on a `ThreadedClient`'s io thread, where it must return
quickly; the `ThreadedClient` lets go of it when its io thread ends. Each
drop is also logged, the first of a kind at once and the rest counted in a
line a second. A message the library wrote is not dropped, whatever happens
to it next: written means the kernel's buffer ([semantics](semantics.md)).

## Threads

Neither `SyncClient` nor `BaseMultiplexerServer` is thread-safe. One `SyncClient`
belongs to one thread, and a `BaseMultiplexerServer` runs on the thread that
calls `serve_forever()`. For parallel clients create one per thread, as the
integration test roles do in
[tests/roles/cc/client.cc](../tests/roles/cc/client.cc). In builds without
`NDEBUG` the library asserts this: a call from another thread fails with an
`AssertionError` naming the wrong-thread call.

**Lifetimes.** A client or a server ends when it is destroyed, or earlier
with `shutdown()` or `close()`: the destructors of `Client` and
`ThreadedClient` run `shutdown()`, writing what was sent first,
`CLOSE_FLUSH_SECONDS` at most, and waiting for the multiplexers' side of
the close, `CLOSE_READ_SECONDS` at most, a
`BaseThreadedMultiplexerServer`'s runs `close()`, and a
`BaseMultiplexerServer`'s client goes with it the same way. Nothing the
library holds keeps a C++ client alive: the callbacks it was given are
destroyed when its io thread ends. A process that ends without running
the destructor, by `std::exit()` with the client on the stack, `_exit()`
or a fatal signal, loses what the client had not written.

**Fork.** A client inherited by a forked child is an orphan there: its io
thread does not exist in the child, its locks may have been held at the
fork by threads that do not exist there either, and its sockets are shared
with the parent. Every call that would send, receive, connect, wait or
take one of its locks throws `UsedAfterFork`, a `NotConnected`, first, and
so does every use of a `Lane` made before the fork, or seeded with a
`ConnectionWrapper` from before it, and every send through such a wrapper,
even by a client made in the child, as do the getters of the
connections' state, `connections_count()`, `has_incoming_messages()` and
`routing_acknowledged()`, which would answer with the parent's;
`orphaned()` tells without throwing. `shutdown()` and the destructor close
the child's copies of the descriptors, once, with `close(2)`, and leak the
rest on purpose, so nothing sends a goodbye or a `shutdown(2)` on the
parent's connection and asio never sees those descriptor numbers again. Of
a `BaseThreadedMultiplexerServer` inherited the same way, `close()`,
`connect()`, `serve_forever()` and `pending()` throw, `stop()` only clears
`working`, and the destructor lets go of the parent's workers without
waiting for them. A `pthread_atfork` child handler in `lib/fork.h` bumps a
fork generation that every client and lane compares with the one it was
created under; one load per call, nothing when nobody forks. Create
clients after forking.

**What a forked child can still wait on.** The child of a process with
threads has only the thread that forked; a lock another thread held at
that instant stays held unless something resets it in the child. glibc
resets its allocator and stdio; it does not reset its name resolver. A
child forked while a thread of the parent was inside `getaddrinfo` can
find a lock of the resolver held for good: every name lookup there waits
forever, and so, in some cases, does a thread's exit. The library looks
names up, on asio's resolver thread, whenever a client connects to a
multiplexer by name and again at every reconnect attempt, every 3 s, while
that multiplexer is unreachable; a child's new client connecting by name
would then never connect, and its destructor would wait for the resolver
thread for good. So a program that forks while clients are alive gives
every client addresses, the parent's too, or resolves the names once
before it makes any; an address in text, `connect("10.0.0.1", 1980)`, is
never looked up. The same goes for the program's own threads that resolve
names, and the dynamic loader's locks are the same kind of hazard: for a
child that calls `dlopen` while a parent thread was loading a library,
and, with glibc before 2.35 or GCC's unwinder before 12, for a child that
throws, `UsedAfterFork` included, while a parent thread was throwing.
`posix_spawn`, or `fork` followed at once by `exec`, avoids all of it.

**A fork inside a callback.** A child forked inside one of the client's
callbacks is a copy of the thread running the loop, in the middle of it:
once the callback returns, the child runs the parent's loop on the
parent's sockets. Such a child calls `exec` or `_exit` before the callback
returns.

**Backends on threads.** `BaseMultiplexerServer::stop()` clears `working`
from any thread; `serve_forever()` notices within one poll. A
`BaseThreadedMultiplexerServer` is made of threads: its handlers run on
the workers, its `Request` may be answered from any thread, and
`serve_forever()` only polls.

