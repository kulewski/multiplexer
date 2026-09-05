# Guarantees, failure modes and defaults

What the multiplexer promises, stated so that you can design around it.

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
  multiplexers there is no order.
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
  A request that reached a backend which then died is repeated to another
  backend, so a backend must tolerate seeing the same request twice, or make
  its work idempotent. Every attempt is a new message with a new `id`: the
  repeat after a timeout, the direct request after a search, and the resend
  after a lost connection. The client accepts a reply to any attempt, but a
  backend cannot tell the attempts apart by `id`, so deduplicate on
  something in the payload, never on the message id.
- **Events give no feedback** unless the rule reports delivery errors, and
  even then only that nobody was there, not that anybody processed it. A
  flushing send (`flush=True`, `Client::send`) does guarantee the event was
  written to a live connection, re-sending across a dead one.

## Failure modes

- **A backend dies mid-request.** The client waits out its timeout, searches,
  and repeats the request elsewhere; the total wait is up to three timeouts.
  [How a query is answered](query.md) shows it.
- **A multiplexer dies.** Backends reconnect within about 3 s. A synchronous
  client notices inside its next call, waits there for the reconnect and
  sends again, or uses another connection at once; a threaded client sends
  again as soon as it is reconnected. Requests that were on the wire when
  the connection died are sent again with a fresh id, so a backend may see
  them twice. [Connecting to a multiplexer](handshake.md) shows it.
- **A backend hangs without dying.** Its connection stays registered as long
  as its library still runs the loop and answers heartbeats, so it keeps
  receiving its share of round-robin requests, which time out. A backend
  that blocks in `handle_message` for more than 90 s is dropped by the
  multiplexer and reconnects afterwards.
- **A client is idle for a long time.** Nothing happens: passive peers are
  never dropped for silence, and a `ThreadedClient` keeps heartbeating.
- **A multiplexer restarts while a synchronous client is idle.** The
  client's next call runs the loop before it picks a connection, so the
  connection that multiplexer closed is retired first and the message goes
  through another, or waits for the reconnect; it is never written into
  the closed socket, which would succeed and lose it.
- **Nobody handles a type.** The client learns at once, from a delivery
  error, rather than by timeout.
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
| `NO_HEARTBIT_SO_PREPARE_DROP_INTERVAL` | 30 s | silence from a non-passive peer before the multiplexer starts to worry |
| `NO_HEARTBIT_SO_REALLY_DROP_INTERVAL` | 60 s | further silence before it closes the connection |
| `MAX_MESSAGE_SIZE` | 128 MiB | largest frame body accepted |
| `DEFAULT_INCOMING_QUEUE_MAX_SIZE` | 1024 messages | unread messages a library holds per peer |
| `queue_size` in the rules file | 1024 messages | unsent messages the multiplexer holds per connection, per peer type |
| dedup window | 2048 ids | repeats the library recognizes |

Changing a constant means rebuilding everything that embeds it, and the
heartbeat intervals must agree between the multiplexer and its peers.
