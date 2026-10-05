# Semantics: delivery, failures and defaults

What the multiplexer promises, stated so that you can design around it.
It is written for the deployment the multiplexer is designed for and every
other page assumes: several multiplexers, every client and every backend
connected to all of them ([the healthy deployment](README.md#the-healthy-deployment)).
A query's search stage asks every multiplexer the client is connected to,
a query whose connection dies goes on to that search at once, and a
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
  while that connection lives, so the stream is in order. When that
  connection dies, what it had not written moves, in order, to one other
  connection, and the lane follows it there, so the stream has one gap or
  reorder at the failover and is in order after it; each further failover,
  another multiplexer failing under the stream, makes one more. A pinned
  lane ends the stream with `NotConnected` instead.
  A stream sent through every connection keeps its order too, since the
  receiving library passes on the first copy of each message: every
  copy of a message follows the copy of the one before on its own
  connection. It breaks only where a message went out through fewer
  connections than the one after it, one being down at that moment, or
  where a late copy came after the library forgot the first.
- **Full queues drop.** Each connection on the multiplexer holds at most
  `queue_size` unsent messages, 1024 by default per peer type, and 64
  protocol frames past that, heartbeats and the answers to the peer's
  control requests, which a full queue does not keep out; an answer past
  those 64 is dropped too, logged as any full queue's drop. For `ANY` a
  full peer is skipped in favour of the next one; for `ALL` a full peer's
  copy is dropped with a warning in the multiplexer's log, about one line
  a second for each peer however many it drops. When every peer
  of the type that takes the message is full, the multiplexer logs `queue
  full on every peer of type N (NAME) that takes it` at the rule's level,
  and the rule's `report_delivery_error` answers the sender with a
  `DELIVERY_ERROR`, as for a type nobody is connected as: a query to a
  saturated type fails with `OperationFailed` once its search finds the
  same full queues. The library on the receiving side holds
  at most 1024 unread messages and drops beyond that too. A backend that
  reads slower than clients send loses messages rather than memory. A
  `ThreadedClient` given `on_message` holds nothing: the callback runs on
  the io thread, which reads no more until it returns, so a slow one
  leaves the backlog to the multiplexer's queue. An `AsyncClient` hands
  every message to its event loop and holds what the loop has not
  reached yet in memory, unbounded: keeping up there is the program's to
  do. On the sending side a client's queue to each multiplexer holds 1024
  messages as well, and every client library holds what does not fit, in
  order, until there is room, within the message's timeout, dropping it
  with a warning after that; a send to `ALL` gives each connection its
  copy that way, and a lane waits for its own connection. A synchronous
  client moves what waits along inside its next call, the only time its
  loop runs. A `ThreadedClient` or `AsyncClient` hands every send and
  every query to its io thread first, through a queue of the thread's own
  that has no bound: a program that sends faster than the io thread
  places what it sends holds the difference in memory, and keeping up
  there is the program's to do. A message's timeout counts from the call,
  so one the io thread reaches with its time up is placed only where a
  connection has room for it then, and dropped and reported otherwise;
  a query's first stage's time starts when the io thread takes the query
  up.
- **Requests always resolve.** `query()` returns the reply, or raises: a
  delivery error, a lost connection or a typed query's first timeout
  means the search starts, the search finding nobody means
  `OperationFailed`, and the time running out after that, or an addressed
  query's one deadline, means `OperationTimedOut`. A query never goes
  back a stage, and its request goes out at most twice: first, and to the
  backend the search found. A first attempt that timed out or lost its
  connection may still be with a backend, and a reply to either attempt
  ends the query at any stage, so where the query would fail, the search
  finding nobody or the backend it found gone, it waits out the stage for
  that late reply instead, and raises `OperationTimedOut` without it
  ([every way a stage ends](query.md#every-way-a-stage-ends)).
  An addressed request, one with `to`, reaches that instance or nobody:
  the instance gone means `OperationFailed`, or that wait, and its being
  behind another multiplexer than the one asked is bridged by the locate
  phase.
  A request that reached a backend which then died is repeated to another
  backend, so a backend must tolerate seeing the same request twice, or make
  its work idempotent. The repeat, the direct request after the search, is
  a new message with a new `id`. The client accepts a reply to either
  attempt, but a backend cannot tell the attempts apart by `id`, so
  deduplicate on something in the payload, never on the message id. A
  backend that calls `notify_start()` lets the caller see it happen: a
  query's `on_received` is called with each backend that acknowledged the
  request, so a second call says the request may be running twice
  ([Python](api_python.md#knowing-a-backend-took-the-request),
  [C++](api_cpp.md#knowing-a-backend-took-the-request)).
- **`references` means "this is the reply".** A client matches replies to
  its queries by the id they reference, and a threaded or asyncio client
  drops what references a query that has ended, for twice the longer of
  the query's timeout and 10 s after it ended, 131072 queries at most,
  since a retried query's second reply must not reach the program as a
  stray and nothing tells such a reply from a follow-up that merely points
  at its request. So a message that follows a request but is not its
  reply, a stream of results after the answer, a notification about the
  work, must not reference the request: address it to the peer with `to`
  and correlate in the payload. One reply per request.
  [The stream example](../examples/stream/README.md) is this rule at
  work: an answer's tokens addressed to the requester, each carrying the
  answer's number and its own, and one reply after the last.
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
  and repeats the request elsewhere; the total wait is up to three timeouts,
  whatever happens to the connections meanwhile, since a query never goes
  back a stage.
  [How a query is answered](query.md) shows it.
- **The addressee of an addressed request dies or leaves.** Every
  multiplexer reports it gone and the request fails with `OperationFailed`
  at once, unless a late reply may still come (above), which the query
  waits for until its timeout; no other instance of its type gets it. An
  addressee that only moved, behind a multiplexer the client's request
  did not go through, is found by the locate phase within the one
  timeout.
- **A multiplexer dies.** A query whose request was in flight on that
  connection searches through the others at once, or locates its
  addressee, and sends the request again, with a fresh id, to the backend
  found, so a backend may see it twice and the caller sees only a slower
  call. A reply that was to go back
  through the dead connection goes through another live one, or the first to
  come up, so it can reach the caller through another multiplexer than its
  request took. What a client's connection to it had not written goes, in
  order, through one other live connection, or, with none live, waits for
  the next, each message within its own timeout, the one its send gave
  it, so that a send that gave up on a message never sees it written
  later: one whose time is up when it would wait, for room or for a
  connection, goes with the dead connection, reported; with the default
  10 s, a restart of 3 s loses only what had waited unwritten more than
  7 s; a pinned lane's messages are dropped, and so are the
  copies of messages sent to every connection while another connection
  lives, the other multiplexers having theirs, each drop reported; with none
  live, the message is held once, whole, until one comes up. Backends and
  clients reconnect to the restarted multiplexer within about 3 s.
  [Connecting to a multiplexer](handshake.md) shows it.
- **A multiplexer is stopped.** On `SIGTERM` it accepts nothing new and
  closes each connection once what is queued for it is written, so what it
  held for the peers that read still arrives; a peer that reads nothing by
  `--drain-seconds` loses what was queued for it. The peers do not know it
  is leaving until their connection closes: what one sends in that last
  moment is lost, a query going on to its search as when a multiplexer
  dies. Neither loss, nor what the queue of a peer
  dropped for silence held, is reported to the sender with a
  `DELIVERY_ERROR`.
- **A multiplexer moves.** A peer given a host name resolves it inside the
  library on every attempt, at startup and at every reconnect, and tries
  each address the name has in turn; so an instance that comes back under
  another address, a rescheduled pod for example, is found at the next
  reconnect, within about 3 s of the name changing. A name that does not
  resolve yet is not an error: `connect()` returns without a connection,
  as for a port that refuses, and the library keeps trying every 3 s. A
  program that keeps its own list of multiplexers, addresses it learns
  elsewhere, connects to a new one with `connect()` and lets one that left
  the list go with `disconnect()`, on every client class, which stops its
  reconnect and closes a live connection, what it had not written going
  to another as a lost connection's does: tried for good, an address
  nobody serves any more can one day be another deployment's
  multiplexer's, as pod addresses are reused.
- **The only multiplexer dies.** There is no other connection. A query
  whose request was in flight goes on to its search, which waits for the
  reconnect, inside the current call on a synchronous client; a query
  that starts meanwhile waits for the reconnect and sends its request
  then. An event sent meanwhile, by either client, is held and goes
  out once the client is reconnected, or is dropped and reported at its
  timeout. The query is answered if the backend of its type is back
  on the fresh multiplexer by then. If the client reconnected first, a
  multiplexer with nobody of the type reports a delivery error, and the
  search asks the same one multiplexer: a query that started meanwhile
  fails at once with `OperationFailed`, and one that was in flight waits
  out its search's timeout for a late reply, which comes only if the
  backend had the request before the drop, and raises `OperationTimedOut`
  without it. Which reconnect lands first is chance, since both are
  scheduled 3 s after the drop. Run two multiplexers if a restart must be
  invisible; the `threaded_mx_restarts` scenario records both cases.
- **A backend leaves.** A draining backend tells every multiplexer to
  route it nothing new by the rules, so a request goes to another
  backend of the type at once, without a search, and the drain ends when
  the multiplexers confirmed and the work is done, `drain_seconds` at
  the latest; a backend alone of its type drains as the last resort or
  its callers fail at once, its choice. A backend of either server class
  that is closing, or whose drain is over, answers a request routed to it
  before the multiplexer heard with a delivery error, as a multiplexer
  answers for a peer that is gone, so the client searches and repeats the
  request elsewhere at once; what it had read before is served. Only what
  the multiplexer wrote after the backend's last read is lost, which
  costs the client a timeout. [How a backend leaves](leaving.md) draws it.
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
  for a dead one. Until then the round robin still gives it its share,
  until its queue of 1024 messages is full; from then on new messages and
  requests go to the other connections. A
  typed query sent through it waits out its `timeout`, then its search
  finds a backend through another multiplexer; an addressed query through
  it ends with `OperationTimedOut`. An event sent through it waits in the
  client, queued or for room, and goes out late if the multiplexer wakes;
  if it dies, what waited goes through another connection or waits for
  the next, and what the client had written into its socket is lost
  unreported. One that waits past its timeout is dropped and reported. A
  flushing send through every connection, in every client, ends once one
  copy is written.
- **A client is idle for a long time.** Nothing happens: passive peers are
  never dropped for silence, and a `ThreadedClient` keeps heartbeating.
- **A peer's host or network goes away without closing its connection.**
  The multiplexer closes the connection after 90 s: an active peer's
  silence ends it, and for a passive peer, or one that has not sent its
  welcome yet, which owe no heartbeats, TCP keepalive does, the kernel
  probing a connection silent for 30 s every 10 s and closing it after
  60 s without an answer. A live peer's kernel answers the probes for it,
  however long its program stays away; a connection that stays up and
  never sends its welcome is kept as long, whoever the peer: a
  synchronous client's reconnect sends its welcome only inside the
  client's next call, which may come much later.
- **A multiplexer restarts while a synchronous client is idle.** The
  client's next call runs the loop before it picks a connection, so the
  connection that multiplexer closed is retired first and the message goes
  through another, or waits for the reconnect, rather than into the closed
  socket, which would succeed and lose it. The loop reads a queue's worth
  of frames there at most (the incoming queue's size, 1024 by default), so
  that a multiplexer sending faster than the client reads cannot hold the
  call: a closure behind more unread frames than that is noticed by a later
  call, and a message sent meanwhile may go into the closed socket.
- **A peer closes one side of its connection.** When the other end of a
  connection stops sending, a half-close such as `shutdown(SHUT_WR)`, the
  multiplexer and the libraries end the connection at once and write
  nothing more to it: a multiplexer drops what it had queued for that
  peer, and a client sends what it had queued through another connection,
  or holds it for the next when none is live, unless it was pinned to that
  one or was a copy for every connection. No peer of theirs half-closes; a server
  of yours that takes plain TCP clients decides for itself what it still
  owes one that does.
- **A peer closes while a message to it is on its way.** Writing to a
  closed connection fails, but what the peer sent before it closed is
  still in the socket: the multiplexer and the libraries stop writing and
  read that to the end, a few seconds at most, and route or deliver it
  before the connection closes. A client's last events, sent just before
  it exited, are not lost to a delivery error written back to it.
- **A client leaves right after sending.** Every client and server class
  ends the same way, `shutdown(timeout)` or `close(timeout)`: what was
  sent before it is written first, `CLOSE_FLUSH_SECONDS` at most by
  default, and what a multiplexer that stopped reading, or a connection
  that did not come up, leaves unwritten by then is dropped and reported
  (0 drops it at once). A socket closed with something
  unread makes the kernel reset the connection and throw away what it had
  not sent yet, which, with the multiplexer behind, is the client's last
  messages. So a client's `shutdown()` then closes each connection the polite
  way: nothing more is written, the client's end of the stream follows
  what the kernel still holds, and what the multiplexer still sends is
  read and dropped until the multiplexer closes its side, a round trip,
  `CLOSE_READ_SECONDS` at most. What was written reaches a multiplexer
  that reads it within that second; one that does not answer holds
  `shutdown()` that long, and its drain before that. Past that second
  the socket closes with what the kernel still holds, which the
  multiplexer gets when it reads on, unless it writes to the client
  first, a heartbeat after a stall say: the client's kernel then resets
  the connection, and that tail is lost without a report.
- **The library gives up on a message.** A message the program sent waits
  in its client for room on a full connection, or for a connection to come
  up, within its `timeout`. One that waited longer, one whose connection
  ended with nothing else allowed to take it (a pinned lane's, a copy sent
  to `ALL`), and what a client's shutdown leaves are dropped, and every one
  is counted and told to the program with its id and why: `on_drop` and
  `dropped` in Python, `set_drop_observer()` and `dropped()` in C++
  ([Python](api_python.md#messages-the-library-gives-up-on),
  [C++](api_cpp.md#messages-the-library-gives-up-on)). Each is logged too.
- **A written message is lost.** Written means handed to the kernel on a
  live connection, which is what a flushing send waits for and all a
  library can know: a multiplexer that dies before reading what its socket
  holds takes those messages along, flushed or not, and nobody is told.
  Only an answer says a message arrived: a query's reply, or an
  acknowledgement the receiver sends of its own.
- **Nobody handles a type.** Every multiplexer the client is connected to
  answers the search with a delivery error, and the client learns at once
  rather than by timeout.
- **A message is bigger than 128 MiB.** The sending library refuses it at
  the call, `ValueError` in Python and `std::length_error` in C++, before
  anything is queued, and a query's request too when the copy the query
  may send again, with an id of its own, the `to` of the backend found and
  a delivery error asked for, would be over the limit; a sender that frames
  its own bytes and gets past that has the receiving side close the
  connection. The limit is
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
| `ROOM_GRACE_SECONDS` | 0.01 s | how much longer than its synchronous flushing send a message waits for room or for a connection, so that the send times out before its message is dropped |
| `DEFAULT_READ_TIMEOUT` | none | `receive_message` waits forever by default |
| `AUTO_RECONNECT_TIME` | 3 s | between a connection dropping and the library reconnecting |
| `CONNECT_ATTEMPT_SECONDS` | 5 s | the TCP connect to one address, before the library tries the next the name resolved to, or ends the attempt |
| `HEARTBIT_INTERVAL` | 3 s | between heartbeats on an idle connection |
| `CLOSE_FLUSH_SECONDS` | 1 s | every `shutdown()` and `close()`: how long what was sent before it is written before the connections close; the `timeout=` default |
| `CLOSE_READ_SECONDS` | 1 s | a client's `shutdown()`: how long a connection reads on, waiting for its multiplexer to close its side |
| `MX_LOG_VERBOSITY` | `DEBUG:HIGH` | connections logged, traffic not; the environment variable changes it per process ([operations](operations.md#logs)) |
| `NO_HEARTBIT_SO_PREPARE_DROP_INTERVAL` | 30 s | silence on a connection, from a non-passive peer or from a multiplexer, before the other side starts to worry |
| `NO_HEARTBIT_SO_REALLY_DROP_INTERVAL` | 60 s | further silence before that side closes the connection |
| `KEEPALIVE_PROBE_INTERVAL` | 10 s | between the TCP keepalive probes on the multiplexer's accepted connections, which start after the first interval above and close the connection after the second without an answer |
| `MAX_MESSAGE_SIZE` | 128 MiB | largest frame body accepted |
| `DEFAULT_INCOMING_QUEUE_MAX_SIZE` | 1024 messages | unread messages a synchronous client holds; a `ThreadedClient` with no `on_message` drops what arrives on its own, with a warning |
| `queue_size` in the rules file | 1024 messages | unsent messages the multiplexer holds per connection, per peer type; also a tap's buffer |
| `FORCED_FRAMES_PAST_FULL_QUEUE` | 64 frames | protocol frames, a welcome, a heartbeat, a status reply or a routing request, that a full queue still takes past its limit, in the multiplexer and the libraries |
| `DEFAULT_REMOTE_RECORDING_MAX_BYTES` | 1 GiB | a recording session started over the protocol closes itself at this size unless the request says otherwise |
| dedup window | 2048 ids | repeats the library recognizes |

Every timeout and interval, in the libraries and the multiplexer, is
seconds as a float, and is read as at most 10⁹ s, about 31.7 years: a
larger one, `math.inf` and `sys.maxsize` too, waits as long as it takes.
A negative timeout given to a client or server class sets no deadline
either, exactly as `math.inf`. 0 and NaN are no time at all: a call
given one waits for nothing, a message sent with one goes now or is
dropped at once, and a drain with one is over as it begins
([Python](api_python.md#timeouts), [C++](api_cpp.md#timeouts)).

The silence intervals count whole frames: the clock restarts when a
frame's header has arrived and when its body has, never in between, so a
frame must arrive whole within 90 s of its header. A body that takes
longer, a frame near the 128 MiB limit on a link slower than about
1.5 MB/s, ends the connection mid-frame, and so does every attempt to
send it again.

Changing a constant means rebuilding everything that embeds it, and the
heartbeat intervals must agree between the multiplexer and its peers.
