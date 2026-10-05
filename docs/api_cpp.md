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
`id`, `sender`, `to`, `references`, `workflow`. Setters are `set_type()`,
`set_message()` and so on. `id` is drawn by the library per attempt: a query's
request sent again, to the backend its search found, carries a new id,
so a backend that must not do the same work twice keys on the payload, not
on `id()`. A message you pass to a query is never changed, since the library
sends copies: both clients draw an id for every attempt, the first
included, and a reply's `references()` names the attempt it answers, so
one message may be queried again and again as it is.

## Timeouts

Every timeout is seconds as a `float`, in every client and server class:
on a connect, a receive, a query, a send, `flush()`, `flush_all()`,
`shutdown()` and `close()`, `serve_forever()`'s `poll` and
`drain_seconds`, and `ThreadedServerOptions::connect_timeout`. A negative
one sets no deadline, exactly as an infinite one does: the call waits as
long as it takes, and a message sent with it waits for a connection or
for room as long as that takes. So `shutdown(-1)` and `close(-1)` write
everything sent before them first, however long that takes; with a
`poll` of -1 a `BaseMultiplexerServer` waits for a message before it
calls `periodic_task()` or sees `working` cleared, and a
`BaseThreadedMultiplexerServer` until a drain, `stop()` or `close()`
wakes it; and a `drain_seconds` of -1 gives a drain no cap, so only the
multiplexers' confirmation ends it, or a stop.

0 and NaN mean "don't wait", in every class: a call does what it can at
once and gives up, a receive taking what has arrived, a connect starting
the attempt. A message sent or scheduled with one, by `queue()`,
`schedule_*()`, `ThreadedClient::send_serialized()` and the like, is
placed now on a connection with room, each copy for `ALL` likewise, and
otherwise dropped and reported at once (`NO_ROOM`, or `NO_CONNECTION`
with no connection live), never held: a `schedule_one()` tracker then
reads `is_lost()`, and `schedule_all()` still counts the connection. A
flushing send's message waits, as every flushing send's does, its call's
deadline and `ROOM_GRACE_SECONDS` more, so that the call ends as a
timeout first. So a flushing send given 0 reports its message not
written, though a connection may write it a moment later: to send
without waiting and learn how the message ended, use
`queue(msg, timeout, lane, done)` or `ThreadedClient`'s
`send(msg, lane, done)`. `shutdown(0)` and `close(0)` drop what is unwritten at
once, a `drain_seconds` of 0 or NaN ends a drain as it begins, and a
`poll` of 0 or NaN is a loop that never waits.

On a `SyncClient` with nothing connected and nothing on its way, never
connected, made with no address or shut down, a wait for a write gives
up at once, whatever its timeout, as a receive throws `NotConnected`
then: `flush_all()` returns false, `flush()` returns with the tracker
reading `in_queue()`, and a flushing `send()` or a query throws
`NotConnected`, the send's message dropped. A `ThreadedClient` waits on,
as another thread may connect it meanwhile.
[Defaults](semantics.md#defaults) says what infinity does, and the
default of each.

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
  that service only inside its calls, so the peer type must be
  `is_passive` in the rules file; this is the one class that needs the
  mark, every other runs the loop all the time. A call runs whatever
  handler of the service is ready, another client's too, so the clients
  that share one service are all driven from one thread, the thread
  that owns them.
- `connect(host, port, timeout = 10)` connects, performs the handshake and
  returns a `ConnectionWrapper`. `host` is an address, IPv4 or IPv6
  without brackets (`::1`), or a name; a name is
  resolved inside the library, on every attempt, and each address it has
  is tried in turn, for 5 s at most (`CONNECT_ATTEMPT_SECONDS`), so that
  one that drops the attempt holds up the others no longer, and a
  multiplexer that moved is found at the next reconnect. It does not throw when the multiplexer is unreachable or the
  name does not resolve yet; the connection is retried every 3 s while the
  library runs. A multiplexer the client has a connection to, live or on
  its way, keeps it: connecting to it again returns that connection.
  `async_connect(host, port)` returns at once;
  `wait_for_connection(wrapper, timeout)` waits for it. The wrapper's
  `target()` is the host and port given, `endpoint()` the address in use;
  for a host name, the wrapper `async_connect()` returns, made before the
  name resolved, has none, and `connect()`'s, once connected, has it.
  `connect(endpoint, timeout)` and `async_connect(endpoint)` take an
  `asio::ip::tcp::endpoint`.
- `disconnect(host, port)`, or `disconnect(endpoint)`, drops a
  multiplexer given to `connect()` or `async_connect()`, the same host and
  port, an address in any spelling: its reconnect stops, and nothing
  connects to it again unless `connect()` is called again. A connection to
  it on its way is abandoned; a live one is closed the polite way, as
  `shutdown()` closes each: what it had not written goes, in order, to one
  other connection, or is held for the next, as a lost connection's does
  ([semantics](semantics.md#failure-modes)), and what it wrote still
  arrives. It returns at once whether the client had that multiplexer, a
  connection to it or a reconnect armed, and throws `NotConnected` after
  `shutdown()` and `UsedAfterFork` in a forked child, as `connect()` does.
  It is for a program that keeps its own list of multiplexers, when one
  leaves the list: the library would retry an address nobody serves any
  more for good, every 3 s, until another deployment's multiplexer gets
  that address, as pod addresses are reused, and the client joins it.
- `query(payload, type, timeout = 10, lane = nullptr, received = nullptr)`
  and `query(mxmsg, timeout, lane, received)` send a request and
  return an `IncomingMessage`, a
  triple whose `third` is a `shared_ptr<MultiplexerMessage>` with the
  reply and whose `second` is the connection it came on. The algorithm is
  the one in [how a query is answered](query.md): one connection first,
  then, after a delivery error, a timeout or a lost connection, a search
  on all of them, then the request again to the backend found, each stage
  with its own `timeout`; the request goes out at most twice, and the
  query never goes back a stage. `mxmsg` is the request itself, its empty
  `sender` filled in, each attempt with an id of its own, ids belonging to
  attempts, so that one message may be queried again and again; the
  request sent again is a copy of it, every field kept. A message with `to` set is an
  addressed query, with one `timeout` for its stages, see
  [below](#lanes-pinning-and-addressed-queries). When the search finds
  nobody, or the backend it found is gone by the direct request, the
  query fails at once if nobody took the request, and otherwise, after a
  timeout or a lost connection, waits out the stage for a late reply from
  a backend that did
  ([every way a stage ends](query.md#every-way-a-stage-ends)). Throws
  `SyncClient::OperationFailed` when no backend can be found,
  `SyncClient::OperationTimedOut` when a stage runs out of time,
  `SyncClient::NotConnected` when no connection is live. All three derive from
  `SyncClient::MxClientError`, which derives from `std::exception`. While
  it waits it reads whatever arrives, and a message that is not its reply,
  a `DELIVERY_ERROR` for an earlier event say, is logged and dropped.
  `received` hears which backend acknowledged the request, see
  [Knowing a backend took the request](#knowing-a-backend-took-the-request).
- `queue(mxmsg, timeout, lane = nullptr, done)` and `queue_all(mxmsg,
  timeout, done)` send an event the way every client sends,
  `ThreadedClient`'s `send(msg)` included, and return at once: the event is
  queued on one live connection, round robin or the lane's, or on every one,
  a multiplexer whose first connection is still in its handshake getting its
  copy at the welcome, waits for room on a full connection, or, with no connection live, is held
  until one comes up, `timeout` seconds at most each way, and is written as
  a later call runs the loop. A connection that dies with it unwritten hands
  it, with the rest it had not written, in order, to one other, or has it
  held for the next; a `queue_all` copy is held too
  when no connection is live, and dropped while one is. One the client gives
  up on is reported ([Messages the library gives up
  on](#messages-the-library-gives-up-on)). `done`, when given, hears how the
  event ended, once, inside a later call that runs the loop: 1 once it was
  written, the first copy for ALL, 0 once it was given up on or `shutdown()`
  came first, as Python's `send_message(callback=)` does. They return the
  event's tracker, the first copy's for ALL, null, `done` never called, only
  when nothing may take it: a pinned lane whose connection is gone, or the
  client shut down.
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
  `ThreadedClient`'s flushing `send` returns 0 instead.
  `send(mxmsg, connection, timeout)` prefers that connection.
- `schedule_one(mxmsg, timeout)` queues an event on one connection and
  returns a `ScheduledMessageTracker`, null when no connection is live,
  since it holds nothing. `schedule_one(mxmsg, wrapper, timeout)` queues
  it on that connection while it is live, waiting there for room, and on
  another once it is gone; with none live it runs the loop up to
  `timeout` for one to come up and throws `NotConnected` if none did, so
  it never returns null; `queue(mxmsg, timeout,
  std::make_shared<Lane>(wrapper))` is the form that holds the message
  instead. `schedule_all(mxmsg, timeout, used = NULL)` queues it on
  every connection and returns how many, 0 when none is live, and puts
  those connections in `*used` when given. A message a full connection
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
- `receive_message(timeout = -1)` waits for the next message, with no
  deadline by default, and returns a pair of the message and its
  connection. It throws `NotConnected` at once when nothing could arrive:
  no connection and none on its way, on a client never connected, made
  with no address, or shut down.
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
- `dropped_while_closing()`: the messages the client's connections read
  after they began closing, which they could only drop, each connection's
  logged as a `WARNING` when it ends. The protocol's own answers to what
  the client sent, a `DELIVERY_ERROR`, `REQUEST_RECEIVED`, `BACKEND_ERROR`,
  a control frame's status or a `PING` that answers, are not counted: only
  the client waited for them.
- `instance_id()`, `client_type()`, `connections_count()`, `random64()`, and
  `shutdown(timeout = CLOSE_FLUSH_SECONDS)`, also run by the destructor:
  it first writes what was sent before it, running the loop as
  `flush_all(timeout)` does, and what the loop sent meanwhile, a server's
  refusals of what it read, a second in all by default, and drops and
  reports what is still unwritten then, at once with 0, as every client
  and server class ends; then it runs the loop until every multiplexer
  has closed its side of the connection too, a round trip,
  `CLOSE_READ_SECONDS` at most, so that what was written arrives
  ([semantics](semantics.md#failure-modes)). After it `connect()`,
  `async_connect()` and `disconnect()` throw `NotConnected`, as
  `ThreadedClient::connect()` does, and nothing is sent or placed. The
  destructor then waits for a name lookup in progress, for an address
  given by name, which runs on asio's resolver thread and ends with the
  client's `io_service`: nothing cuts a lookup short, so against a slow or
  unreachable DNS server it waits up to the resolver's own timeout. A
  client given addresses never waits so.

A message built by hand that has no id or sender gets them where it is
sent: a fresh id, and the client's instance id as `sender`. The receiving
library drops a message without an id. `SyncClient`'s `queue()`,
`queue_all()` and `schedule_*()` also take a frame, a
`std::shared_ptr<const RawMessage>`, sent as it is:
`RawMessage::FromMessage(msg)`, or, for a message serialized already,
`new RawMessage(&serialized, msg.id(), msg.type())`, the id and type a
drop is reported by.

### Lanes, pinning and addressed queries

The same on `SyncClient` and `ThreadedClient`; the reasoning is in
[the Python API](api_python.md#lanes-pinning-and-addressed-queries).

- **An addressed query** is a `query(mxmsg, ...)` whose message has
  `set_to(instance_id)`: only that instance ever gets it. When a
  multiplexer reports the instance is not behind it, or the connection
  dies under the wait, the client locates it with a `PING` addressed to
  it on every connection and repeats the request through the connection
  that found it, once; an instance nobody has is `OperationFailed`
  (`FAILED`) at once, unless the request may still be with it, its
  connection lost under it, when the query waits for that late reply
  until its timeout; one `timeout` covers the stages. The `PING` reaches
  the instance whatever its routing, as every addressed message does, and
  the server classes and `ThreadedClient` all answer it whatever their search
  policy, so it finds a backend that declines searches as well as a peer
  that serves no requests; `SyncClient` does not answer it. The library
  sets `report_delivery_error` on the request. The old behaviour of
  `SyncClient::query` with `to`, a search by type and the request to
  whichever backend answered, is gone.
- **A lane**, `multiplexer::Lane` in `multiplexer/basic_client.h`, held as
  `LanePtr` (a `std::shared_ptr<Lane>`), is a soft, late pin, or, made
  pinned, the hard one; it keeps a stream on one connection: `SyncClient`'s
  `queue(msg, timeout, lane)` and `send(msg, timeout, lane)`,
  `ThreadedClient`'s `send(msg, lane)` and `send(msg, lane, timeout)`, and
  `query(..., lane)` on both go through the lane's connection, the first
  message taking the connection the library chose, a query leaving the lane
  on the connection the reply came through. While the lane's connection
  lives, a message it has no room for waits there for room, so the stream
  keeps its order. A lane that is not pinned takes another connection when
  its own dies: the one that took, in order, what the dead one had not
  written, through every failover since, so the stream has one gap or
  reorder per failover. `Lane(true)` is pinned: once it is `closed()` the lane
  refuses: `SyncClient::send` and `ThreadedClient`'s `send(msg, lane)` throw
  `NotConnected`, `queue()` returns a null tracker, a flushing
  `ThreadedClient` `send` returns 0, and `query` ends `NOT_CONNECTED`
  (`NotConnected` on `SyncClient`); a `ThreadedClient` send whose lane's
  connection goes after the call is dropped and reported
  (`CONNECTION_LOST`). A pinned message a dying connection had not written
  is reported lost, never handed to another connection
  (`RawMessage::pinned`). A pinned lane's send that runs out of time while
  its connection lives is a timeout, `OperationTimedOut` on `SyncClient`, as
  on any lane. `Lane(connection, pinned)` seeds a lane. `connection()`,
  `holds_connection()`, `connected()`, `closed()`, `pinned()` read it. A
  lane holds its connection weakly and the library keeps no registry, so it
  lives as long as your `LanePtr`; a query in flight holds it until it ends.
  A lane, once it took a connection, and a `ConnectionWrapper` belong to the
  client they came from: given to another client, a send, a queue or a
  query throws `std::invalid_argument` on the calling thread.
  Safe to share with the io thread.
- **A connection**, given to `send(msg, connection, timeout)`, which
  flushes on both, to `ThreadedClient`'s queueing `send(msg,
  connection)`, or to `query(msg, connection, ...)`, is preferred for that
  message and replaced when gone, as `BaseMultiplexerServer::send_message`
  replies the way the request came. `Result::reply.second` and
  `IncomingMessage::second` are where a connection comes from.

### Knowing a backend took the request

The same on `SyncClient` and `ThreadedClient`, and on the Python clients,
[where the reasoning is](api_python.md#knowing-a-backend-took-the-request).
Every `query()` overload takes a last, optional `ReceivedCallback
received`, a `std::function<void(std::uint64_t backend)>` from
`multiplexer/basic_client.h`, called with the instance id of each backend
that acknowledges the request with `REQUEST_RECEIVED`, what
`notify_start()` sends, as soon as the acknowledgement arrives:

```cpp
multiplexer::IncomingMessage reply = client.query(
    "hello", multiplexer::types::ECHO_REQUEST, 60, multiplexer::LanePtr(),
    [](std::uint64_t backend) { std::cerr << "backend " << backend << " is working on it\n"; });
```

Normally once; again, with the backend's id, when a retry reached a
backend, the same or another, which says the request may be running
twice. Nothing else
about the query changes, and a query without one costs nothing more. On
`SyncClient` it runs on the calling thread inside `query()`, while the
query waits, so it must not query or receive through that client, which
could take the reply; on `ThreadedClient` on the io thread, so it must be
quick.
One that throws a `std::exception` is logged, and the query goes on.

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
`connect()` connects to every address, once: it starts every connection
at once and returns when each has its handshake done or has failed, 10 s
at most in all, so a multiplexer that drops the connect or never answers
holds up the others' requests that long once, not once for every address
after it. It throws nothing for an address it could not reach, which the
library goes on trying. `serve_forever()` starts the connections itself
unless `connect()` did, waiting for none: its loop finishes the
handshakes, so the backend serves what one multiplexer routes to it while
another has not welcomed it yet, a multiplexer that never answers holds
up nothing, and no multiplexer knows the backend before it can serve. A
program that only constructs and serves never calls `connect()`. Call it yourself
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
checked there takes effect within one poll, unless `poll` sets no deadline
([timeouts](#timeouts)); a liveness probe that must see the loop answers
from a time written down there ([check a backend's
health](recipes/check_backend_health.md)). It returns, with the
connections closed, when the public `working` flag is cleared or a drain
is over. `loop_iter(timeout)`
does one step and throws `SyncClient::OperationTimedOut` after `timeout`
seconds. The thread that calls
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

A whole `MultiplexerMessage` is sent as it is: beside one, only
`multiplexer` may be given, and `type`, `to`, `references` or `workflow`
throws `std::invalid_argument`. While a request is handled, its empty `to`,
`references` and `workflow` are filled in from the request, as
`Request::reply` fills a threaded server's. Outside a handler, from
`periodic_task()` say, there are no such defaults: `to` and `references`
are 0 and the message goes through one connection, routed by its type like
any client's. The message is sent as
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
leaves](leaving.md) draws the phases and what each costs; a request the
multiplexer routed to the backend in the moment it closed goes
unanswered, counted by `dropped_while_closing()`, kept past `close()`, and
logged as a `WARNING`. Overriding
`should_respond_to_backend_for_packet_search()` puts a condition of your
own behind the search; a drain needs none. The library installs no signal handlers; a handler of
your own must only set a `sig_atomic_t` that `periodic_task()` reads, as
[examples/echo/backend.cc](../examples/echo/backend.cc) does. A C++ process
may rely on a signal like that; a Python process should not, see the
[FAQ](faq.md).

A handler that throws is reported to the requester with `BACKEND_ERROR`,
unless the message answers another, one with `references` set, a reply or
someone's `BACKEND_ERROR`: nobody waits for an answer to it, and two
backends whose handlers throw on what they do not expect would answer each
other's reports for good. Then the virtual
`on_handler_exception(const std::exception &)` decides:
true, the default, keeps serving; false lets the exception propagate out
of `serve_forever`, an `OperationTimedOut` of the handler's own too: only
`poll` running out is the loop's timeout.

`no_response()` marks a message as needing no reply; `notify_start()` sends
`REQUEST_RECEIVED` to the requester at once, which a query's `received`
[hears of](#knowing-a-backend-took-the-request); `parse_message<SomeProto>(mxmsg)`
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
retries elsewhere at once. It then destroys its client, which waits for a
name lookup in progress, up to the resolver's own timeout, as
`SyncClient`'s destructor does; so does the server's destructor when
`close()` did not run.

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
  waiting for a worker; beyond it a request is dropped with a warning and no
  answer, its requester waiting out its first stage's timeout before the
  search, and the backend keeps its round-robin share, since the multiplexer
  skips a peer only for its own full queue; one arriving while the server is
  leaving, routed before the multiplexer applied the drain routing, is
  refused with `DELIVERY_ERROR`, so its requester retries at once, and one
  that answers another is dropped, since refusing a reply could start a
  loop, [how a backend leaves](leaving.md#what-stays)),
  `decline_searches_when_full` (false: searches are answered while the
  backend serves, none once `close()` began; true leaves them unanswered
  while every worker is busy and requests wait), `connect_timeout` and
  `drain_routing` (what `start_draining()` tells the multiplexers; `any` and
  `all` off by default).
- The constructor only makes the instance id; `connect()` starts the
  workers and connects, to every address at once against one
  `connect_timeout`, as `BaseMultiplexerServer` does, once, and
  `serve_forever()` calls it first, so
  nothing reaches `handle_message()` before your constructor has
  finished, and no multiplexer knows the backend until it can serve.
  `instance_id()` is valid from construction. When to call `connect()`
  yourself is as for `BaseMultiplexerServer` above: something waits for
  a line you print before it sends, a test wants the backend connected
  without serving it, or you start `serve_forever()` on a thread of your
  own and send at once; such a backend closes in its own destructor, see
  `close()` below.
- `handle_message(const RequestPtr &)` runs on a worker, with every
  message that is not the protocol's own and with the `DELIVERY_ERROR`s
  for messages the server sent that were not queries, which a handler
  tells by the type. The `Request`,
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
  its own work too. A kept request answers through the server's client:
  its `reply()` after `close()` throws `NotConnected`, and after the
  server is destroyed it would use a freed client, so answer every kept
  request, or drop it, before the server goes.
- `serve_forever(poll, drain_seconds)`, `stop()`, `start_draining()`,
  `draining()`, `drained()`, `periodic_task()`, `on_handler_exception()` and
  `should_respond_to_backend_for_packet_search()` are
  `BaseMultiplexerServer`'s, with the same meanings, `drained()` also
  waiting for the queue to be empty; `close(timeout)` takes no more
  messages, lets the workers finish the queue, and ends the connections as
  `ThreadedClient::shutdown(timeout)` does, the last replies written first,
  and throws `std::logic_error` from a handler, on a worker, rather than
  join itself, during another thread's `close()` too, `stop()` being the
  call for that; from another thread it makes a `serve_forever()` running
  there return, quietly, even while it still connects, and a second call
  returns once the first is done. The destructor calls `close()` when
  nothing did, but by then your subclass is destroyed, and a request still
  queued would reach the pure virtual `handle_message()`: a server that may
  still be connected when it is destroyed, `connect()` called without
  `serve_forever()`, or `serve_forever()` running on another thread, calls
  `close()` in its own destructor. `pending()`, `dropped()`, `instance_id()`
  and `client()` for messages that are not replies. A handler that throws
  gets the requester `BACKEND_ERROR`, unless its reply went out, a reply
  that threw being none, or the message answers another, `references`
  set, as for `BaseMultiplexerServer`, and a report that fails is logged; `false` from
  `on_handler_exception()` makes `serve_forever()` return and rethrow.
  So does an exception out of `on_handler_exception()`, or one from a
  handler not derived from `std::exception`: the worker leaves, and what
  is still queued once no worker is left is refused, as during a close.

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

- `query(payload, type, timeout, lane, received)` blocks and returns a
  `Result`: `outcome` is `REPLIED`, `TIMED_OUT`, `FAILED` (no backend
  anywhere, the backend a search found gone, or the addressee gone, with
  nobody holding the request), `NOT_CONNECTED` or `SHUT_DOWN`, and
  `check()` returns the reply or throws the exception `SyncClient::query`
  would have. The stages are `SyncClient`'s. `query(msg, timeout, lane)`
  takes the request as a whole, `to` included, its empty `sender` filled in,
  each attempt with an id of its own:
  the addressed form, and `query(msg, connection, ...)` prefers a
  connection, see [above](#lanes-pinning-and-addressed-queries). `received`,
  the last argument of every overload, is called on the io thread with each
  backend that acknowledged the request, see
  [above](#knowing-a-backend-took-the-request). Any number of threads may
  call it at once; replies are matched by the ids they reference. Called on
  the io thread, from a callback, it throws `std::logic_error` rather than
  deadlock. The callback form returns at once and runs the callback on the
  io thread; an empty callback runs the query all the same, for a caller
  who wants the request delivered and not the reply, its outcome going
  nowhere. After `shutdown()` it runs the callback at once, on the calling
  thread, with `SHUT_DOWN`, so a callback must not query again then, nor
  take a lock its caller holds. A request that cannot be queued waits the
  way a message does (below), and the query ends `TIMED_OUT` when a
  connection was live but had no room in time, `NOT_CONNECTED` when none
  was. A query waits for the io thread as a send does (below), and its
  first stage's timer starts when the io thread takes it up.
- `send(msg)` and `send_all(msg)` queue a message on one or every connection
  and return at once; the io thread writes it right after. A message that
  cannot be queued yet, no connection being live or the connections' queues
  full (1024 messages each), waits there, behind those sent before it, until
  a connection comes up or has room, for 10 seconds at most, and is dropped
  and reported after that (see [Messages the library gives up
  on](#messages-the-library-gives-up-on)): `send_all` gives every live
  connection its copy, a full one as soon as it has room, and a multiplexer
  whose first connection is still in its handshake one at its welcome, so
  that a send just after the start misses none, and a lane waits
  for room on its own connection while that lives. The io thread takes
  sends and queries in the order they were made, from a queue of its own
  that has no bound, so a program that sends faster than the io thread
  places what it sends holds the difference in memory; a message's time
  counts from the call, and one the io thread reaches with its time up is
  placed only where a connection has room for it then, and dropped and
  reported otherwise. `send(msg, lane)` and
  `send(msg, connection)` choose the connection. `send(msg, lane, done)`,
  `send(msg, connection, done)` and `send_all(msg, done)` call
  `done(written)` once on the io thread, 1 when the message was written, the
  first copy for `send_all`, 0 when it was given up on or `shutdown()` came
  first; `send(msg, nullptr, done)` for a message with no lane. All are safe
  from callbacks. `send(msg, timeout)`, `send(msg, lane, timeout)`,
  `send(msg, connection, timeout)` and `send_all(msg, timeout)` are the
  flushing forms: they wait until the message reached the socket, for
  `send_all` until one copy did, the others going out from their
  connections' queues, a connection that dies under it handing it to another
  or having it held, and return 1 once a copy is written, 0 when none was by
  `timeout` or the message was given up on, a pinned lane's connection being
  gone for instance; not from callbacks. A `send_all` copy whose connection
  dies is held only when no connection is live and dropped otherwise, so a
  flushing `send_all` whose copies all went with their connections returns
  0, even with a connection that came up since. After `shutdown()` every
  send throws `NotConnected`. The serialized forms, what the Python binding
  uses, take the message's id and type after its bytes, which a drop is
  reported by, and a callback instead of the wait:
  `send_serialized(serialized, id, type, lane, timeout, done)` and
  `send_all_serialized(serialized, id, type, timeout, done)` return at once
  and call `done(written)` once on the io thread, 1 when the message was
  written, the first copy for every connection, 0 when it was given up on,
  and reported, or `shutdown()` came first;
  `send_serialized_with_callback(serialized, id, type, all, timeout, done,
  lane)` is the flushing send so, `done` hearing 1 once a copy is written,
  0 when the message was given up on, `timeout` passed or `shutdown()` came
  first. `send_serialized_and_wait(serialized, id, type, all, timeout, lane,
  &not_connected)` and `send_serialized_and_notify(serialized, id, type,
  all, timeout, done, lane)`, with
  `done(written, not_connected)`, also say why a flushing send wrote nothing,
  as the synchronous client tells it at its deadline: its message given up
  on, a pinned lane's connection gone or a shutdown, or no connection live
  when `timeout` passed, else `timeout` passed with one live, the message
  then waiting `ROOM_GRACE_SECONDS` more before it is dropped; the Python
  clients raise `NotConnected` for the one and `OperationTimedOut` for the
  other on it, and an asyncio layer awaits
  the second. All are safe from callbacks. A message over `MAX_MESSAGE_SIZE`
  is refused where it is sent or queried, with `std::length_error`, a
  query's request measured as the query may send it again.
  `new_message()` fills in id and sender, and every send fills them in on a
  whole message that left them empty: every receiver drops a message
  without an id.
- `connect(host, port, timeout)` connects as `SyncClient::connect()` does
  and waits for the handshake: true once the connection is registered, at
  once for one connected already, which it keeps, false as soon as it
  failed, or at `timeout`, the io thread trying again every 3 s on its
  own. `connect_all(addresses, timeout)` connects to every `(host, port)`
  at once, each waited for against the same `timeout`, so a multiplexer
  that never welcomes costs it once, not once for every address after
  it, and returns how many are registered. `disconnect(host, port)` drops the multiplexer as
  `SyncClient::disconnect()` does, on the io thread, and returns once it
  is done, whether the client had it: `connections_count()` is down by
  then, a query through the connection closed goes on as after a lost
  connection, and a `connect()` still waiting for
  that multiplexer returns false. Neither from callbacks, where they throw
  `std::logic_error` rather than deadlock; after `shutdown()` they throw
  `NotConnected`.
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
  logged and dropped; nothing is queued. A sink says what it drops
  through `drop_lines()`, on the io thread, at the library's rate for
  such lines, the first of a kind at once and the rest as a count about
  once a second ([multiplexer/log_summary.h](../multiplexer/log_summary.h)),
  its kinds numbered from `BasicClient::OWN_LINES`: so `mxcontrol
  streamlogs` says that no log receiver took its chunks. A late reply to a
  query that already ended, `REQUEST_RECEIVED` for an untracked query, a
  `PING`, which the client answers itself, and a
  `BACKEND_FOR_PACKET_SEARCH`, answered with a `PING` when a policy set
  with `set_search_policy()` says yes and dropped otherwise, never reach
  it.
  The late-reply rule binds the peers that send to this client: `references`
  means "this is the reply", and what references a query of this client
  that has ended (for twice the longer of its timeout and 10 s after it
  ended, 131072 queries at most; `forget_finished_ids()` forgets them at
  once, for a test that measures the heap) is dropped whatever its type, so a
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
| `send_serialized(serialized, id, type, lane, timeout, done)`, `send_all_serialized(serialized, id, type, timeout, done)` | at once | held | `done(0)`, and the observer; `done(1)` once written |
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
  reading, past its timeout, or, sent with no time to wait, found none.
- `NO_CONNECTION`: it waited for a connection to come up past its timeout,
  or, sent with no time to wait, found none live.
- `CONNECTION_LOST`: its connection ended and nothing else could take it: a
  pinned lane's, or a copy sent to `ALL`.
- `SHUT_DOWN`: the client shut down before it went.

The observer runs inside whichever `SyncClient` call runs the loop when the
drop happens, and on a `ThreadedClient`'s io thread, where it must return
quickly; the `ThreadedClient` lets go of it when its io thread ends. Each
drop is also logged, the first of a kind at once, naming the message's id
and type, and the rest counted in a line a second. The id and the type are
those the message's frame was made with, a whole message's own or those
given with its bytes: the library never parses a message for them. A message the library wrote is not dropped, whatever happens
to it next: written means the kernel's buffer ([semantics](semantics.md)).

## Addresses as text

A program that reads multiplexer addresses from its command line or its
configuration reads them with `multiplexer::parse_endpoint()` and writes
them with `multiplexer::format_endpoint()`, in
[multiplexer/endpoint.h](../multiplexer/endpoint.h), Bazel target
`@mx//multiplexer:endpoint`: `host:port`, or `[address]:port` for an IPv6
address, the way [mxcontrol](mxcontrol.md#addresses) and the Python
library take them. `parse_endpoint(text)` returns the `(host, port)` pair
`connect()` takes, an IPv6 address without its brackets, the host empty
for `:1980` for the caller to fill in; `parse_endpoint(text, 1980)` lets
the port be left out. A malformed text throws `std::invalid_argument`
saying why: an IPv6 address out of brackets, since `::1:1980` could be
either, brackets around anything else, a port that is not a number from
0 to 65535. `format_endpoint(host, port)` puts an IPv6 address in
brackets.

## Threads

Neither `SyncClient` nor `BaseMultiplexerServer` is thread-safe. One `SyncClient`
belongs to one thread, and a `BaseMultiplexerServer` runs on the thread that
calls `serve_forever()`; clients that share an `io_service` belong to one
thread together, since each call runs the others' handlers too. For
parallel clients create one per thread, as the integration test roles do in
[tests/roles/cc/client.cc](../tests/roles/cc/client.cc). In builds without
`NDEBUG` the library asserts this: a call from another thread fails with an
`AssertionError` naming the wrong-thread call.

**Lifetimes.** A client or a server ends when it is destroyed, or earlier
with `shutdown()` or `close()`: the destructors of `Client` and
`ThreadedClient` run `shutdown()`, writing what was sent first,
`CLOSE_FLUSH_SECONDS` at most, and waiting for the multiplexers' side of
the close, `CLOSE_READ_SECONDS` at most, and for a name lookup in
progress, on either client and so in either server class, a
`BaseThreadedMultiplexerServer`'s runs `close()`, too late for a
subclass's handlers, so a subclass calls it in its own destructor, and a
`BaseMultiplexerServer`'s client goes with it the same way. Nothing the
library holds keeps a C++ client alive: the callbacks it was given are
destroyed when its io thread ends. A process that ends without running
the destructor, by `std::exit()` with the client on the stack, `_exit()`
or a fatal signal, loses what the client had not written.

**Fork.** A client inherited by a forked child is an orphan there: its io
thread does not exist in the child, its locks may have been held at the
fork by threads that do not exist there either, and its sockets are shared
with the parent. Every call that would send, receive, connect,
disconnect, wait or take one of its locks throws `UsedAfterFork`, a
`NotConnected`, first, and so does every use of a `Lane` made before the
fork, or seeded with a `ConnectionWrapper` from before it, and every send
through such a wrapper, even by a client made in the child, as do the
getters of the connections' state, `connections_count()`,
`has_incoming_messages()` and `routing_acknowledged()`, which would answer
with the parent's;
`orphaned()` tells without throwing. `shutdown()` and the destructor close
the child's copies of the descriptors, once, with `close(2)`: of every
socket the client had open at the fork, those of connections still closing
too, from a table the client keeps for this as its sockets open and close,
never the lists of connections its thread may have been changing at the
fork. They leak the rest on purpose, so nothing sends a goodbye or a
`shutdown(2)` on the parent's connection and asio never sees those
descriptor numbers again. Of
a `BaseThreadedMultiplexerServer` inherited the same way, `close()`,
`connect()`, `serve_forever()` and `pending()` throw, `stop()` only clears
`working`, and the destructor lets go of the parent's workers without
waiting for them. A `pthread_atfork` child handler in `lib/fork.h` bumps a
fork generation that every client and lane compares with the one it was
created under; one load per call, nothing when nobody forks. Create
clients after forking.

**Exec.** Every socket the clients and the multiplexer open is closed on
exec: a program the process runs, by `system()`, `posix_spawn()` or an
exec after a fork, holds none of the connections, which would otherwise
stay open after the client closed them or its process died, the
multiplexer routing to the silent copy until the heartbeats dropped it.
A client's socket is created so; the multiplexer's are marked right after
asio opens or accepts them, which a program embedding the server and
running a program from another thread at that instant could still hand
on.

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

