# Using the C++ library

`multiplexer::Client` in [multiplexer/client.h](../multiplexer/client.h)
is the client; `multiplexer::backend::BaseMultiplexerServer` in
[multiplexer/backend/base_multiplexer_server.h](../multiplexer/backend/base_multiplexer_server.h)
is the base class for backends. The constants generated from the
[rules file](rules.md) are in `multiplexer/multiplexer.constants.h`, as
`multiplexer::peers::*` and `multiplexer::types::*`, both `boost::uint32_t`.

Bazel targets: `@mx//multiplexer:client`,
`@mx//multiplexer/backend:base_multiplexer_server` and
`@mx//multiplexer:multiplexer_cc_constants`. The library uses Boost.Asio and
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
on `id()`.

## Client

```cpp
#include "multiplexer/client.h"
#include "multiplexer/multiplexer.constants.h"

multiplexer::Client client(multiplexer::peers::ECHO_CLIENT);
client.connect("10.0.0.1", 1980);
client.connect("10.0.0.2", 1980);
multiplexer::IncomingMessage reply = client.query("hello", multiplexer::types::ECHO_REQUEST, 10);
std::cout << reply.third->message() << "\n";
client.shutdown();
```

- The constructor takes the peer type and creates its own `io_service`; the
  overloads taking a `boost::asio::io_service` share yours. The client runs
  that service only inside its calls, so the peer type must be `is_passive`
  in the rules file.
- `connect(host, port, timeout = 10)` resolves `host`, connects, performs the
  handshake and returns a `ConnectionWrapper`. It does not throw when the
  multiplexer is unreachable; the connection is retried every 3 s while the
  library runs. `async_connect(host, port)` takes a literal IP address and
  returns at once; `wait_for_connection(wrapper, timeout)` waits for it.
- `query(payload, type, timeout = 10)` and `query(mxmsg, timeout)` send a
  request and return an `IncomingMessage`, a triple whose `third` is a
  `shared_ptr<MultiplexerMessage>` with the reply and whose `second` is the
  connection it came on. The algorithm is the one in
  [how a query is answered](query.md): one connection first, then a search
  on all of them, then the request again to the backend found, each stage
  with its own `timeout`. Throws `Client::OperationFailed` when no backend
  can be found, `Client::OperationTimedOut` when a stage runs out of time,
  `Client::NotConnected` when no connection is live. All three derive from
  `Client::MxClientError`, which derives from `std::exception`.
- `send(mxmsg, timeout)` writes an event through one connection and returns
  it, replacing a connection that dies under the write or waiting for the
  reconnect within `timeout`: the way to send an event from a client that
  may have lost its connection since the last call.
- `schedule_one(mxmsg)` queues an event on one connection and returns a
  `ScheduledMessageTracker`; `schedule_one(mxmsg, wrapper, timeout)` picks
  the connection. `schedule_all(mxmsg)` queues it on every connection and
  returns how many. Queuing does not send: `flush(tracker, timeout)` runs the
  loop until that message reached the socket, `flush_all(timeout)` until
  every queue is empty. A tracker answers `in_queue()`, `is_sent()`,
  `is_lost()`.
- `receive_message(timeout = -1)` waits for the next message and returns a
  pair of the message and its connection; `-1` waits forever.
- `instance_id()`, `client_type()`, `connections_count()`, `random64()`, and
  `shutdown()`.

A message built by hand must carry `set_id(client.random64())` and
`set_from(client.instance_id())`, or the receiving library drops it. The
`query(payload, type)` overload does that for you.

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

The constructor takes a vector of `(host, port)` pairs, connects to each, and
must be called from a subclass because it is protected. The second
constructor takes a `Client*` you created, for backends that also act as
clients. `serve_forever(poll = 1.0f, drain_seconds = 0.0f)` runs the loop:
each iteration waits up to `poll` seconds for a message, handles it if one
came, then calls the virtual `periodic_task()`, message or not, so anything
checked there takes effect within one poll. It returns, with the
connections closed, when the public `working` flag is cleared or a drain is
over. `loop_iter(timeout)` does one step and throws
`Client::OperationTimedOut` after `timeout` seconds. The thread that calls
`serve_forever` becomes the backend's thread, whichever thread built it;
in debug builds a later call from another thread fails an assertion. A
program driving `loop_iter` itself from another thread calls
`Client::bind_to_current_thread()` first.

`send_message(Kwargs)` takes named arguments, because the message has many
optional fields. Keys and their exact types:

| Key | Type | Default while handling a request |
|---|---|---|
| `message` | `std::string`, `const std::string *`, or `const MultiplexerMessage *` for a message you built yourself | required |
| `type` | `boost::uint32_t` | required unless `message` is a whole message |
| `to` | `boost::uint64_t` | the requester's instance id |
| `references` | `boost::uint64_t` | the request's id |
| `workflow` | `std::string` or `const std::string *` | the request's workflow |
| `multiplexer` | `int` `BaseMultiplexerServer::ONE` or `ALL`, or a `ConnectionWrapper` | the connection the request arrived on |

`Kwargs` stores each value by its static type, so pass exactly the type in
the table: the generated constants already are `boost::uint32_t`, but a
literal or an `int` needs a cast. A wrong type fails an assertion in a debug
build.

**Leaving gracefully.** From `periodic_task()`, call `start_draining()`
when asked to leave: the backend stops answering the search clients use to
find a backend, so no retried request is sent to it, while it keeps serving
what the multiplexer still routes to it. `serve_forever` returns once the
virtual `drained()` says so, by default `drain_seconds` after the drain
started; override it to wait for a condition of your own. Overriding
`should_respond_to_backend_for_packet_search()` puts another condition
behind the search. The library installs no signal handlers; a handler of
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
rather than timing out; the backend keeps serving. A Python requester sees
`BackendError`; a C++ requester's `query()` returns the `BACKEND_ERROR`
message itself, so check `reply.third->type()` when the backend may fail.
`close()` drops the connections.

## ThreadedClient

`multiplexer::ThreadedClient` in
[multiplexer/threaded_client.h](../multiplexer/threaded_client.h) runs the
connections on a thread of its own, so heartbeats and reconnects happen
without the program calling in, the peer type need not be passive, and any
thread may use it. Target `@mx//multiplexer:threaded_client`.

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

- `query(payload, type, timeout)` blocks and returns a `Result`: `outcome`
  is `REPLIED`, `TIMED_OUT`, `FAILED` (no backend anywhere), `NOT_CONNECTED`
  or `SHUT_DOWN`, and `check()` returns the reply or throws the exception
  `Client::query` would have. Any number of threads may call it at once;
  replies are matched by the ids they reference. Called on the io thread,
  from a callback, it throws `std::logic_error` rather than deadlock. The
  callback form returns at once and runs the callback on the io thread.
- `send(msg)` and `send_all(msg)` queue a message on one or every
  connection and return at once; the io thread writes it right after, and a
  message that finds no live connection waits there for one. Both are safe
  from callbacks. `send(msg, timeout)` and `send_all(msg, timeout)` are the
  flushing forms: they wait until the message reached the socket, resending
  through another connection if the first dies under it, and return the
  number of connections written to, 0 on timeout; not from callbacks.
  `new_message()` fills in id and from.
- The `MessageSink` given to the constructor runs on the io thread with
  every message that is not a reply to a query or one of the protocol's
  own: events and requests addressed to this peer. Without one such
  messages are logged and dropped; nothing is queued. A late reply to a
  query that already ended, `REQUEST_RECEIVED` for an untracked query and
  a `PING`, which the client answers itself, never reach it.
- Callbacks and the sink must return quickly; they may start asynchronous
  queries and send, but not call the blocking `query` or `shutdown`.
- `shutdown()`, also run by the destructor, fails every query in flight
  with `SHUT_DOWN`, closes the connections and joins the thread.

Under the hood the io thread owns a `BasicClient`; other threads reach it
through `io_service::post` only, and the clang thread-safety analysis
checks that (`--config=clang`).

## Threads

Neither `Client` nor `BaseMultiplexerServer` is thread-safe. One `Client` belongs to one thread, and a
backend runs on the thread that calls `serve_forever()`. For parallel clients
create one per thread, as the integration test roles do in
[tests/roles/cc/client.cc](../tests/roles/cc/client.cc). In builds
without `NDEBUG` the library asserts this: a call from another thread fails
with an `AssertionError` naming the wrong-thread call.

**Fork.** A client inherited by a forked child is an orphan there: its io
thread does not exist in the child, its locks may be held by nobody, and
its sockets are shared with the parent. Every call throws
`UsedAfterFork`, a `NotConnected`, before touching any lock; the
destructor closes only the child's descriptor copies, with `close(2)`, and
leaks the rest on purpose, so nothing sends a goodbye or a `shutdown(2)`
on the parent's connection and asio never sees those descriptor numbers
again. A `pthread_atfork` child handler in `lib/fork.h` bumps a fork
generation that every client compares with the one it was created under;
one load per call, nothing when nobody forks. Create clients after
forking.

**Backends on threads.** `BaseMultiplexerServer::stop()` clears `working`
from any thread; `serve_forever()` notices within one poll.

