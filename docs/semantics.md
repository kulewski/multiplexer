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
- **Full queues drop.** Each connection on the multiplexer holds at most
  `queue_size` unsent messages, 1024 by default per peer type. For `ANY` a
  full peer is skipped in favour of the next one; for `ALL`, and when every
  peer of the type is full, the message is dropped for that peer with a
  warning in the multiplexer's log. The library on the receiving side holds
  at most 1024 unread messages and drops beyond that too. A backend that
  reads slower than clients send loses messages rather than memory.
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
  flushing send (`flush=True`, `Client::send`) does guarantee the event was
  written to a live connection, re-sending across a dead one.

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
  them twice and the caller sees nothing; backends and clients reconnect to
  the restarted multiplexer within about 3 s. [Connecting to a
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
- **A backend hangs without dying.** Its connection stays registered as long
  as its library still runs the loop and answers heartbeats, so it keeps
  receiving its share of round-robin requests, which time out. A
  `BaseMultiplexerServer` that blocks in `handle_message` for more than
  90 s is dropped by the multiplexer and reconnects afterwards; a
  `BaseThreadedMultiplexerServer` keeps heartbeating from its io thread
  while a handler runs, for any length of time, and is not.
- **A client is idle for a long time.** Nothing happens: passive peers are
  never dropped for silence, and a `ThreadedClient` keeps heartbeating.
- **A multiplexer restarts while a synchronous client is idle.** The
  client's next call runs the loop before it picks a connection, so the
  connection that multiplexer closed is retired first and the message goes
  through another, or waits for the reconnect; it is never written into
  the closed socket, which would succeed and lose it.
- **Nobody handles a type.** Every multiplexer the client is connected to
  answers the search with a delivery error, and the client learns at once
  rather than by timeout.
- **A message is bigger than 128 MiB.** The receiving side closes the
  connection.

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
| `MX_LOG_VERBOSITY` | `DEBUG:HIGH` | connections logged, traffic not; the environment variable changes it per process ([operations](operations.md#logs)) |
| `NO_HEARTBIT_SO_PREPARE_DROP_INTERVAL` | 30 s | silence from a non-passive peer before the multiplexer starts to worry |
| `NO_HEARTBIT_SO_REALLY_DROP_INTERVAL` | 60 s | further silence before it closes the connection |
| `MAX_MESSAGE_SIZE` | 128 MiB | largest frame body accepted |
| `DEFAULT_INCOMING_QUEUE_MAX_SIZE` | 1024 messages | unread messages a library holds per peer |
| `queue_size` in the rules file | 1024 messages | unsent messages the multiplexer holds per connection, per peer type; also a tap's buffer |
| `DEFAULT_REMOTE_RECORDING_MAX_BYTES` | 1 GiB | a recording session started over the protocol closes itself at this size unless the request says otherwise |
| dedup window | 2048 ids | repeats the library recognizes |

Changing a constant means rebuilding everything that embeds it, and the
heartbeat intervals must agree between the multiplexer and its peers.
