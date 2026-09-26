# Semantics: delivery, failures and defaults

What the multiplexer promises, stated so that you can design around it.
It is written for the deployment the multiplexer is designed for and every
other page assumes: several multiplexers, every client and every backend
connected to all of them ([the healthy deployment](README.md#the-healthy-deployment)).
A query's search stage asks every multiplexer the client is connected to,
a request whose connection dies goes out again through another, and a
multiplexer restarting costs nobody anything. With a single multiplexer
there is no other connection to use, and the failure modes below say what
that changes.

## Delivery

- **At most once per multiplexer.** A message is written to the receiving
  connections the multiplexer has at that moment, or it is dropped. Nothing
  is retried, stored or replayed. A backend that connects a second later
  gets nothing that happened before.
- **Copies come from you.** Sending an event through every connection means
  every multiplexer delivers it, so each backend gets one copy per
  multiplexer. The receiving library remembers the ids of the last 2048
  messages it saw and drops repeats, so a backend sees each id once unless
  more than 2048 other messages arrived between the copies.
- **Order holds per connection only.** Two messages from one peer through
  one multiplexer reach a backend in the order they were sent. Through two
  multiplexers there is no order. A lane keeps a stream on one connection
  while that connection lives, so the stream is in order; at a failover
  the lane moves and there is a gap or a reorder, once, unless the lane
  is pinned, in which case the stream ends with `NotConnected` instead.
  A stream sent through every connection keeps its order too, since the
  receiving library passes on the first copy of each message: every
  copy of a message follows the copy of the one before on its own
  connection. It breaks only where a message went out through fewer
  connections than the one after it, one being down at that moment, or
  where a late copy came after the library forgot the first.
- **Full queues drop.** Each connection on the multiplexer holds at most
  `queue_size` unsent messages, 1024 by default per peer type. For `ANY` a
  full peer is skipped in favour of the next one; for `ALL`, and when every
  peer of the type is full, the message is dropped for that peer with a
  warning in the multiplexer's log. The library on the receiving side holds
  at most 1024 unread messages and drops beyond that too. A backend that
  reads slower than clients send loses messages rather than memory. On
  the sending side a client's queue to each multiplexer holds 1024
  messages as well, and every client library holds what does not fit, in
  order, until there is room, within the message's timeout, dropping it
  with a warning after that; a send to `ALL` gives each connection its
  copy that way, and a lane waits for its own connection. A synchronous
  client moves what waits along inside its next call, the only time its
  loop runs.
- **Requests always resolve.** `query()` returns the reply, or raises: a
  delivery error means the search starts, the search finding nobody means
  `OperationFailed`, a stage running out of time means `OperationTimedOut`.
  An addressed request, one with `to`, reaches that instance or nobody:
  the instance gone means `OperationFailed`, and its being behind another
  multiplexer than the one asked is bridged by the locate phase.
  A request that reached a backend which then died is repeated to another
  backend, so a backend must tolerate seeing the same request twice, or make
  its work idempotent. Every attempt is a new message with a new `id`: the
  repeat after a timeout, the direct request after a search, and the resend
  after a lost connection. The client accepts a reply to any attempt, but a
  backend cannot tell the attempts apart by `id`, so deduplicate on
  something in the payload, never on the message id.
- **`references` means "this is the reply".** A client matches replies to
  its queries by the id they reference, and a threaded or asyncio client
  drops what references a query it has seen answered, the last 1024,
  since a retried query's second reply must not reach the program as a
  stray and nothing tells such a reply from a follow-up that merely points
  at its request. So a message that follows a request but is not its
  reply, a stream of results after the answer, a notification about the
  work, must not reference the request: address it to the peer with `to`
  and correlate in the payload. One reply per request.
- **Events give no feedback** unless the rule reports delivery errors, and
  even then only that nobody was there, not that anybody processed it. A
  flushing send (`flush=True`, `SyncClient::send`) does guarantee the event was
  written to a live connection, re-sending across a dead one. The report
  comes from each multiplexer that got a copy and found nobody: an event
  sent through every connection can bring one `DELIVERY_ERROR` per
  multiplexer, each with an id of its own and `references` set to the
  event's id, and one of them says only that its multiplexer had nobody.
  A multiplexer that is down says nothing.

## Failure modes

- **A backend dies mid-request.** The client waits out its timeout, searches,
  and repeats the request elsewhere; the total wait is up to three timeouts.
  [How a query is answered](query.md) shows it.
- **The addressee of an addressed request dies or leaves.** Every
  multiplexer reports it gone and the request fails with `OperationFailed`
  at once; no other instance of its type gets it. An addressee that only
  moved, behind a multiplexer the client's request did not go through, is
  found by the locate phase within the one timeout.
- **A multiplexer dies.** Requests in flight on that connection go out again
  with a fresh id through another connection, at once, so a backend may see
  them twice and the caller sees nothing. A reply that was to go back
  through the dead connection goes through another live one, or the first
  to come up, so it can reach the caller through another multiplexer than
  its request took. Backends and clients reconnect to the restarted
  multiplexer within about 3 s. [Connecting to a
  multiplexer](handshake.md) shows it.
- **A multiplexer moves.** A peer given a host name resolves it inside the
  library on every attempt, at startup and at every reconnect, and tries
  each address the name has in turn; so an instance that comes back under
  another address, a rescheduled pod for example, is found at the next
  reconnect, within about 3 s of the name changing. A name that does not
  resolve yet is not an error: `connect()` returns without a connection,
  as for a port that refuses, and the library keeps trying every 3 s.
- **The only multiplexer dies.** There is no other connection. A threaded
  client sends its in-flight requests again as soon as it is reconnected; a
  synchronous client waits for the reconnect inside its current call and
  sends again. The request is answered if the backend of its type is back
  on the fresh multiplexer by then, and fails at once with `OperationFailed`
  if the client reconnected first: a multiplexer with nobody of the type
  reports a delivery error, and the search that follows asks the same one
  multiplexer. Which reconnect lands first is chance, since both are
  scheduled 3 s after the drop. Run two multiplexers if a restart must be
  invisible; the `threaded_mx_restarts` scenario records both cases.
- **A backend leaves.** A draining backend tells every multiplexer to
  route it nothing new by the rules, so a request goes to another
  backend of the type at once, without a search, and the drain ends when
  the multiplexers confirmed and the work is done, `drain_seconds` at
  the latest; a backend alone of its type drains as the last resort or
  its callers fail at once, its choice. A backend built on
  `BaseThreadedMultiplexerServer` that is closing answers a request routed
  to it before the multiplexer heard with a delivery error, as a
  multiplexer answers for a peer that is gone, so the client searches and
  repeats the request elsewhere at once.
  A plain `BaseMultiplexerServer` loses what arrived after its last read,
  which costs the client a timeout. [How a backend leaves](leaving.md)
  draws it.
- **A backend hangs without dying.** Its connection stays registered as long
  as its library still runs the loop and answers heartbeats, so it keeps
  receiving its share of round-robin requests, which time out. A
  `BaseMultiplexerServer` that blocks in `handle_message` for more than
  90 s is dropped by the multiplexer and reconnects afterwards; a
  `BaseThreadedMultiplexerServer` keeps heartbeating from its io thread
  while a handler runs, for any length of time, and is not.
- **A multiplexer hangs without dying.** A multiplexer stopped with its
  sockets open, a hung host or `SIGSTOP`, is not noticed at once: the
  libraries apply to it the heartbeat intervals it applies to its peers,
  and close the connection after 30 s and 60 s more without a frame, as
  for a dead one. Until then the round robin still gives it its share. A
  typed query sent through it waits out its `timeout`, then its search
  finds a backend through another multiplexer; an addressed query through
  it ends with `OperationTimedOut`; an event sent through it, flushed or
  not, waits inside it, lost if it dies, delivered late if it wakes. A
  flushing send through every connection ends once one copy is written.
- **A client is idle for a long time.** Nothing happens: passive peers are
  never dropped for silence, and a `ThreadedClient` keeps heartbeating.
- **A multiplexer restarts while a synchronous client is idle.** The
  client's next call runs the loop before it picks a connection, so the
  connection that multiplexer closed is retired first and the message goes
  through another, or waits for the reconnect; it is never written into
  the closed socket, which would succeed and lose it.
- **A peer closes one side of its connection.** When the other end of a
  connection stops sending, a half-close such as `shutdown(SHUT_WR)`, the
  multiplexer and the libraries end the connection at once and write
  nothing more to it: a multiplexer drops what it had queued for that
  peer, and a client sends what it had queued through another connection,
  unless it was pinned to that one. No peer of theirs half-closes; a server
  of yours that takes plain TCP clients decides for itself what it still
  owes one that does.
- **A peer closes while a message to it is on its way.** Writing to a
  closed connection fails, but what the peer sent before it closed is
  still in the socket: the multiplexer and the libraries stop writing and
  read that to the end, a few seconds at most, and route or deliver it
  before the connection closes. A client's last events, sent just before
  it exited, are not lost to a delivery error written back to it.
- **A client leaves right after sending.** A socket closed with something
  unread makes the kernel reset the connection and throw away what it had
  not sent yet, which, with the multiplexer behind, is the client's last
  messages. So a client's `shutdown()` closes each connection the polite
  way: nothing more is written, the client's end of the stream follows
  what the kernel still holds, and what the multiplexer still sends is
  read and dropped until the multiplexer closes its side, a round trip,
  `CLOSE_READ_SECONDS` at most. What `flush_all()` reported written
  reaches a multiplexer that reads it within that second; one that does
  not answer holds `shutdown()` that long.
- **Nobody handles a type.** Every multiplexer the client is connected to
  answers the search with a delivery error, and the client learns at once
  rather than by timeout.
- **A message is bigger than 128 MiB.** The sending library refuses it at
  the call, `ValueError` in Python and `std::length_error` in C++, before
  anything is queued; a sender that frames its own bytes and gets past
  that has the receiving side close the connection. The limit is
  `MAX_MESSAGE_SIZE`, below; protocol buffers are built for messages far
  smaller, so anything near it belongs in a store the message points at.
- **Handling a message fails.** When the code handling an arriving message
  throws, a callback of a client's or the multiplexer's routing, the
  exception is logged, that message is dropped, and the connection reads
  on; a client's callback that throws is logged with what it was called
  for.
- **A message is near 128 MiB.** A few frames are built around a peer's
  message and are a little bigger than it: the echo of a `PING` or of a
  search, a delivery error that carries the original, a tap's record. For
  a message near the limit they would be over it, so a `PING` or a search
  whose echo would not fit is answered with `BACKEND_ERROR` saying so, a
  delivery error leaves the original out and sets
  `original_message_omitted`, and a tap's record has its payload cut to
  fit, marked `truncated`. `ThreadedClient`'s `query()` measures a request
  with the id it adds, so one within a few bytes of the limit is refused
  at the call.
- **The rules file changes.** Each multiplexer reads it again every 2 s,
  on `SIGHUP` and on `mxcontrol rules reload`, and puts a changed file in
  use whole, between two messages; the next message is routed by the new
  rules, a peer type added is accepted at the peer's next attempt, and a
  connected peer whose type was removed stays until it reconnects. A file
  that does not parse or names a peer that does not exist leaves the rules
  in use as they were. Different multiplexers pick the change up seconds
  apart, as a rolling restart would ([changing the
  rules](operations.md#changing-the-rules)).

## Defaults

All in [multiplexer/defaults.h](../multiplexer/defaults.h), compiled into the
multiplexer and both libraries, and exported to Python as attributes of
`multiplexer.mxclient`.

| Constant | Value | Where it applies |
|---|---|---|
| `DEFAULT_TIMEOUT` | 10 s | connect, each stage of a query, flush |
| `DEFAULT_READ_TIMEOUT` | none | `receive_message` waits forever by default |
| `AUTO_RECONNECT_TIME` | 3 s | between a connection dropping and the library reconnecting |
| `HEARTBIT_INTERVAL` | 3 s | between heartbeats on an idle connection |
| `CLOSE_READ_SECONDS` | 1 s | a client's `shutdown()`: how long a connection reads on, waiting for its multiplexer to close its side |
| `MX_LOG_VERBOSITY` | `DEBUG:HIGH` | connections logged, traffic not; the environment variable changes it per process ([operations](operations.md#logs)) |
| `NO_HEARTBIT_SO_PREPARE_DROP_INTERVAL` | 30 s | silence on a connection, from a non-passive peer or from a multiplexer, before the other side starts to worry |
| `NO_HEARTBIT_SO_REALLY_DROP_INTERVAL` | 60 s | further silence before that side closes the connection |
| `MAX_MESSAGE_SIZE` | 128 MiB | largest frame body accepted |
| `DEFAULT_INCOMING_QUEUE_MAX_SIZE` | 1024 messages | unread messages a library holds per peer |
| `queue_size` in the rules file | 1024 messages | unsent messages the multiplexer holds per connection, per peer type; also a tap's buffer |
| `DEFAULT_REMOTE_RECORDING_MAX_BYTES` | 1 GiB | a recording session started over the protocol closes itself at this size unless the request says otherwise |
| dedup window | 2048 ids | repeats the library recognizes |

Changing a constant means rebuilding everything that embeds it, and the
heartbeat intervals must agree between the multiplexer and its peers.
