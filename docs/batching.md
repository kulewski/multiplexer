# Batching: many events in one message

The multiplexer parses, routes and writes every message on its own, on its
one thread, so for small messages what it costs is per message, not per
byte. A client that sends many small events, log lines, metrics,
readings, can send them several in one message instead, a batch. To the
multiplexer a batch is one message of the events' type and key, routed by
the rules as any message is. The receiving library splits it back into
the events and hands each to the program as if it had come alone, with
the id, sender, payload, workflow and key it was sent with.

A client turns it on for a message type, or for one key of a type, with
`set_batching()`. It suits a stream of small events of one type that one
client sends faster than the multiplexer moves them one at a time, or
that should cost the multiplexer less. What it gives up, each below:

- order between two batched types or keys ([order](#order));
- with `whom: ANY`, the round robin's turns go per batch, not per event
  ([on the way](#on-the-way-a-batch-is-one-message));
- a batch nobody took comes back whole in its `DELIVERY_ERROR`
  ([what changes](#what-changes-for-a-program));
- every receiver of the type must run a library newer than 3.0.0
  ([what the receiver sees](#what-the-receiver-sees)).

## Turning it on

`set_batching(type)` batches the events of a message type, each key of it
apart; `set_batching(type, key)` those of one key, ahead of what is set
for its type. `clear_batching(type)` ends both, the type's setting and
every key's of it; `clear_batching(type, key)` one key's, which then
batches as its type says. `SyncClient`, `ThreadedClient` and `AsyncClient`
have them. The server classes batch through their client: in Python
`self.conn` on `BaseMultiplexerServer` and `self.client` on
`BaseThreadedMultiplexerServer`, in C++ the protected `conn` and
`client()`, in Rust their `ServerHandle`. A change lets every open batch
go first, and is in effect for every send made after it.

Three settings say when a batch goes ([below](#when-a-batch-goes)):

| Setting | Default | |
|---|---|---|
| `max_records` | 1000, `DEFAULT_BATCH_MAX_RECORDS` | the most messages in a batch, `MAX_BATCH_RECORDS` (1024) at most |
| `max_bytes` | 1 MiB, `DEFAULT_BATCH_MAX_BYTES` | the most bytes of the batch's message, measured as `MAX_MESSAGE_SIZE` is: the envelope with every message inside, serialized |
| `max_delay` | 0 | how long a batch's first message waits for more; 0, no delay, sends a batch as soon as a connection has nothing left to write |

```python
client = ThreadedClient(addresses, peers.METRICS_AGENT)
client.set_batching(types.METRIC_SAMPLE, max_delay=0.005)  # a sample waits 5 ms at most for others
for sample in samples:
    client.send_message(sample.SerializeToString(), type=types.METRIC_SAMPLE)
```

```cpp
multiplexer::SyncClient::Batching batching;
batching.max_delay = 0.005;  // seconds: a SyncClient writes only inside its calls
client.set_batching(multiplexer::types::METRIC_SAMPLE, batching);
for (const multiplexer::MultiplexerMessage& sample : samples) {
  client.queue(sample);
}
client.flush_all();  // the last batch goes now
```

```rust
let batching = Batching { max_delay: Duration::from_millis(5), ..Batching::default() };
client.set_batching(types::METRIC_SAMPLE, None, batching)?;  // every key of the type, each apart
for sample in samples {
    client.send(types::METRIC_SAMPLE, sample.payload).key(sample.host).queue()?;
}
client.flush_all(Some(Duration::from_secs(10)))?;
```

**Refused at the call**, with `ValueError` in Python,
`std::invalid_argument` in C++ and `InvalidArgument` in Rust:

- a protocol message type, 99 and below;
- a key over `MAX_KEY_SIZE`, 256 bytes;
- a `max_records` below 1 or over `MAX_BATCH_RECORDS`, 1024: the most
  messages one batch holds on the wire, so that what one frame becomes in
  a receiver stays a queue's worth;
- a `max_bytes` below 1 or over `MAX_MESSAGE_SIZE`;
- a delay that is negative, over `MAX_SECONDS` (10⁹ s, about 31.7 years),
  infinite or NaN.

`ThreadedClient` and `AsyncClient` check on the calling thread and make
the change on the io thread right after, ahead of every send made after
the call.

## What goes in a batch

**The sends.** Only a send nobody waits for, through the one connection
the round robin picks:

- C++: `SyncClient::queue()` without a lane; `ThreadedClient`'s
  `send(msg)`, `send(msg, LanePtr(), done)` and `send_serialized()`
  without a lane.
- Python: a `send_message()` that returns at once, without `flush` or with
  a `callback`, which replaces the wait, through `multiplexer=ONE`, the
  default.
- Rust: a send's `queue()`, without `lane()`, `connection()` or `all()`;
  a server handle's `send()` and `send_message()`.

A send with no time to wait, a timeout of 0, goes alone even so: in a
batch it would leave the whole batch no time either.

A server class's `send_message()` of a message that is no reply is its
client's and batches the same way. Everything else goes alone, as before,
after every batch: a flushing send (`flush=True` without a `callback`,
C++ `SyncClient::send()` and `ThreadedClient::send(msg, timeout)`,
Rust's `wait()` and `written()`), a send to every connection (Python's
`event()` and `multiplexer=ALL`, C++ `queue_all()` and `send_all()`,
Rust's `all()`), a send through a lane or a connection, a reply,
`schedule_*()` and a query's request.

**The messages.** Only a plain event: no `to`, no `references`, neither
`report_delivery_error` nor `include_original_packet_in_report`, no
`override_rrules`, the default `logging_method` and `compression`, and no
`batch` of its own. Any other message goes alone, whatever is set for its
type.

**The groups.** A batch holds the messages of one type and one key.
`set_batching(type)` batches each key of the type apart, and its messages
without a key apart again: an empty key is a key, which routes otherwise
than none ([keys](keys.md)). A key's own setting goes ahead of its type's
for that key; the type's other keys batch by the type's setting, or not
at all when the type has none.

## When a batch goes

A batch goes, as one message, at the first of these:

- **It is full.** It goes as soon as it holds `max_records` messages, or
  `max_bytes` of them, and when one more message would take it past
  `max_bytes`, that message beginning the next batch. The batch adds up
  its messages' own sizes as they join, the least its message will hold;
  as it goes, it is measured whole, as the protocol buffer library sizes
  it, and where it would be over `max_bytes` its last messages are cut
  off, at once as many as cover the excess, and go right after it, in a
  batch of their own. So no batch is over `max_bytes` on the wire. A
  message of `max_bytes` or more goes alone.
- **Its delay is over**, `max_delay` after its first message. One timer
  runs, to the earliest end among the open batches.
- **With no delay, a connection has nothing left to write.** A message
  sent while some connection has nothing queued, nothing being written and
  nothing waiting for room goes at once and alone, exactly as without
  batching. Only while every connection is busy do messages gather, and
  their batch goes as soon as one of the connections has written
  everything, on that connection, when a connection comes up, on that
  one, or when none is live any more, to be held then as any message is.
  Such a batch waits for room, and its messages' timeouts run meanwhile:
  it goes when the first of them is up, at the latest
  ([one deadline](#on-the-way-a-batch-is-one-message)).
- **Another batch goes on its own**, full, at its delay or at its
  deadline: the batches that wait for a connection with nothing left to
  write go with it, in the order they began, since they would only be
  written after it. While one group keeps filling batches, another with
  fewer messages waits no longer than the next of them.
- **Something else goes first** ([order](#order)).

A batch of one message goes as that message.

**Which class keeps the time.** `ThreadedClient`, `AsyncClient` and
`BaseThreadedMultiplexerServer` run their loop on an io thread all the
time: a delay ends on time, and with no delay a batch goes the moment a
connection is free. A connection is busy only while a write is on its
way, a few microseconds, or while its socket is full, the multiplexer
behind. So with no delay events gather only while the program sends
faster than its connections write, and a Python program, which spends
microseconds of its own on each event, seldom does: one Python source
sending events as fast as it went put one in forty in a batch, and the
multiplexers' work stayed what it was without batching. No delay costs
an event no time and batches what would have queued anyway; a delay of a
few milliseconds batches for sure, each event waiting that long at most.

`SyncClient` runs its loop only inside its calls. With no delay, each
call first takes in what its connection has finished writing, so the
connection is free again at the next send and every message goes alone;
messages gather only while the socket is full, the kernel holding all it
takes. Give it a delay. A batch then goes when it fills, or at the first
call after its delay, a send, a receive or a flush, which runs the timer:
a program that stops calling holds its last batch until it calls again,
and `flush_all()` lets it go at once. A send that only joins a batch with
a delay runs no loop at all, a system call spared for each message: the
one that fills its batch does, before the batch chooses a connection, and
so does the first after a batch's delay is over; so what arrives in the
meantime is read, and heartbeats answered, at those calls or at the next
receive. `BaseMultiplexerServer` is built on
a `SyncClient`, and its loop runs while `serve_forever()` waits for a
message: a delay ends on time between two messages, and while a handler
runs, at the handler's next send or once the loop waits again.

## Order

**What keeps its order.** The messages of one group go in the order they
were sent, batch after batch. Anything sent that does not batch, a flush
(`flush_all()`, a tracker's `flush()`, a flushing send), a query,
`set_batching()` and `clear_batching()`, and the shutdown first let every
open batch go, in the order the batches began; so does a message that
goes alone because a connection is free or it is too big. So a message
never overtakes a batched one sent before it.

**What does not.** A batch that goes on its own account, full, at its
delay or at a free connection, goes ahead of the older batches of other
groups still open. So the events of two groups may arrive in another
order than they were sent: order is kept within a type and key, and
against what is not batched, not between groups. A program that needs
one order across types batches none of them.

**And per connection only**, as always ([semantics](semantics.md#delivery)):
the round robin picks a connection for each batch, so with several
multiplexers two batches of one group may take two connections and
arrive in either order, as two messages may.

## On the way: a batch is one message

For the rest of the library, and for the multiplexer, a batch is one
message:

- **One peer.** With `whom: ANY` the multiplexer gives the whole batch to
  one peer of the type, so a pool shares out batches, not events; with
  `whom: ALL` every peer gets it. A key filter takes it or passes over it
  whole, by its key.
- **One entry in each queue on the way.** The client's queue to each
  multiplexer, 1024 messages, and the multiplexer's `queue_size` for a
  peer each count a batch as one message. So a queue of batches holds
  that many batches, up to `max_bytes` each: 1024 of the default 1 MiB
  are a GiB. Lower `queue_size` for a peer type that receives large
  batches ([sizing](operations.md#sizing)).
- **One line in the multiplexer's log.** A batch that goes nowhere, to a
  full queue, past every key filter or to a type nobody serves, is
  logged as one message is ([logs](operations.md#logs)): the counts a
  second of a batched type count batches, not events.
- **One deadline.** A batch waits for room, or for a connection, until
  the earliest of its messages' deadlines. With a delay, a message's time
  in its batch is the batching's, and its timeout counts from when the
  batch goes: a batch whose delay outlasts the timeout still waits for a
  connection that is down when it goes. With no delay, the batch itself
  waits for room, and the timeouts count from each message's send, as
  they would without batching: the batch goes at its first deadline at
  the latest, and is given up on then if nothing takes it.
- **Handed over whole.** A connection that dies hands an unwritten batch
  to another with the rest it had not written; with none live, the batch
  is held whole until one comes up.
- **Its end is each message's.** Each message's callback, `done` in C++,
  `callback` in Python, `on_end` in Rust, hears its batch's end, in the
  order the messages were sent: 1, or `true`, once the batch was written,
  0, or `false`, once it was given up on. A C++ tracker reads `in_queue()`
  until then.
- **A loss is told for each message.** A batch given up on is counted in
  `dropped` once for each message in it, and the drop observer, `on_drop`
  in Python and Rust, `set_drop_observer()` in C++, hears each message's
  id, with the batch's reason
  ([semantics](semantics.md#failure-modes)). The log line, one for the
  batch, says how many messages it held and the batch's id.
- **A flush waits for it.** `flush_all()` lets every batch go, then waits
  for them as for everything else sent before it. A batch it lets go that
  is given up on at once, with no time left and no connection to take it,
  makes it return false.

## What the receiver sees

Every library splits a batch, in every class: each message in it reaches
the program on its own, in the order sent, as one call of `on_message`,
one receive or one `handle_message()`, and nothing tells the program it
came in a batch. On the way in:

- a batch whose id the library has seen before is dropped, as any repeat
  is, and so is a message in it seen before, as it would be alone;
- a message in it that is not a plain event of the batch's type and key,
  or has no id, is dropped, and the rest go on;
- a batch of a protocol type, 99 and below, one that does not parse, or
  one that holds more than `MAX_BATCH_RECORDS` messages, is dropped
  whole.

The last two are logged as the library's other drops are, the first of
a kind at once and the rest counted.

**A raw frame is made when it is read.** In C++, `read_raw_message()`,
`receive_raw_message()` and a `ThreadedClient`'s `on_message` hand out a
message's frame with it, and Python reads every message from its frame.
A message from a batch gets a frame of its own only then, the message
serialized again: the same message, its fields in their numbers' order,
which for a message sent with its id and sender are the bytes it went in
the batch as. One the library gave its id or sender as it sent it had
them added after its other fields, and a peer of your own may encode
otherwise than protobuf does; theirs come out in protobuf's order.
`receive_message()` makes none.

**A full incoming queue**, a `SyncClient`'s or a `BaseMultiplexerServer`'s,
1024 unread messages by default, drops a batch whole, `incoming_dropped`
counting each message in it; a queue with room takes the whole batch,
past its limit by one batch at most. A call's look at what arrived counts
each message of a batch toward the queue's worth it reads at most.

**Every receiver must split.** Every receiver of a batched type, whatever
its class, must run a library newer than 3.0.0. One of 3.0.0 or older
hands its program the batch as one message of the type with an empty
payload, and the events in it never reach the program.

## What changes for a program

- **A `DELIVERY_ERROR` is told for each event.** When nobody took a batch
  and the rule reports delivery errors, as it does by default, the
  multiplexer reports the batch as the one message it routed, and sends
  the batch back whole in the report, as its original, whether the rule
  asked for originals or not. The library hands the program one
  `DELIVERY_ERROR` for each event inside instead, as the multiplexer would
  have sent it for the event alone: the event's id as `packet_id` and
  `references`, its workflow, the peer types the batch failed for, and the
  event itself as the original, which it carries even where the rule did
  not ask for originals. Only a failure pays: the batch comes back whole,
  where unbatched events' reports carry no original unless asked. A batch
  too big to carry back, the report's original left out
  (`original_message_omitted`), and one from a multiplexer of 3.0.0 or
  older, which sends no batch back, leave the batch's own report, naming
  the batch's id, which the program never saw. A rule with
  `report_delivery_error: false` sends nothing back, as for any message.
- **A drain waits for its batches.** A server class that drains lets its
  batches go once nothing more arrives and the change is confirmed, and
  waits for them as for anything it holds for a connection.
- **A recording records batches.** The multiplexer's recording has a
  record for each delivery of a batch, as of any message, marked `batch`,
  its payload the batch's bytes, cut by `--record-payload-bytes` as any
  payload is ([recording](operations.md#recording)).

## On the wire

A batch is a `MultiplexerMessage` with:

- `id`, a fresh one the client drew, and `sender`, the client;
- `type` and `key`, the batch's, which every message in it has: no key
  when its messages have none;
- `batch`, field 10, in place of `message`: a serialized `MessageBatch`,
  whose one field, `message` (1), is repeated bytes, each a whole
  serialized `MultiplexerMessage`, exactly as it would have gone alone,
  in the order sent.

`batch` is bytes rather than a repeated field, so that the multiplexer,
which parses every message it routes, copies a batch as one blob and never
parses what is inside. A peer written without the library may send
batches of its own, which the libraries split by the rules above.
[The wire format](wire_format.md#multiplexermessage) has the field among
the others.

## What it costs

- **Nothing batched:** nothing; a send only sees that nothing is set, and
  a frame carries a null pointer for what batching would know of it.
- **Something batched:** a client that batches hands each plain event it
  queues, through no lane, to the batching as its bytes alone, with no
  frame made for it: a message that goes in a batch never gets one, nor
  the checksum every frame's header carries, and only one that goes alone
  is framed, as it goes. Each send looks up its type, and its key for a
  type with keys set. A batch takes each message's bytes as they are and
  serializes them once, straight into the batch's frame (`BatchEnvelope`
  in Multiplexer.proto, the same bytes as the `MultiplexerMessage` holding
  them serialized), in a buffer an earlier batch's frame left once it was
  written, and hands the bytes' room back to the client, which serializes
  its next messages into it. Python hands the library each message
  serialized, and the library reads one of a batched type once more, to
  see whether it is a plain event, of the type it was given, and what its
  key is.
- **Sending from another thread:** a `ThreadedClient`, which batches on
  its io thread, takes a plain event nobody follows from the calling
  thread as its bytes, through one queue the io thread takes a batch at a
  time, waking it once for as many as come meanwhile; and the room of the
  bytes goes back to the calling thread through the same queue. So no
  thread frees what another allocated, which glibc's malloc makes costly:
  it gives a block back to the arena that allocated it, under that
  arena's lock, which the allocating thread takes as it allocates.
- **What it saves:** measured on a 24-core AMD Ryzen Threadripper PRO
  9965WX, one `SyncClient` sending events of 100 bytes through one
  multiplexer to one consumer moved about 440,000 a second unbatched, the
  multiplexer's CPU 2.3 s per million, and about 2,500,000 a second
  with a 5 ms delay, 0.03 s per million.
- **Receiving:** the library reads a batch into strings it keeps from one
  batch to the next, and each message from its string, so that a message
  costs the receiver its own allocations alone, the message and its
  payload, and no frame of its own until something reads one ([what the
  receiver sees](#what-the-receiver-sees)): about three allocations a
  message where it took six. On the same machine, a `SyncClient` taking
  kilobyte events with `receive_message()` used 0.41 µs of CPU a message
  where it used 0.53.
- **Memory for big batches:** a batch of a megabyte is a frame of a
  megabyte at the sender, in each multiplexer and at the receiver. The
  sender serializes its batches into the buffers of those it wrote, four
  of up to 4 MiB at most. Elsewhere, glibc's malloc maps a block that size
  of its own and unmaps it when it is freed until its thresholds settle,
  so a program just started faults the pages of such frames in afresh,
  more so with several threads, each of which glibc gives a heap of its
  own. The multiplexer sets
  glibc's thresholds itself ([sizing](operations.md#sizing)); a library
  cannot for its program, which can in its environment, both or neither,
  since either alone turns glibc's own adjustment off:
  `GLIBC_TUNABLES=glibc.malloc.mmap_threshold=33554432:glibc.malloc.trim_threshold=67108864`.

## Where it is shown

- **[tests/scenarios/batched_events](../tests/scenarios/batched_events/README.md):**
  ten keyed events a client sends in batches, as they went on the wire,
  three messages of the events' type and key, and as a backend gets them,
  one at a time, in order, with the ids, payloads, senders and key they
  were sent with. Every pair of Python, C++ and Rust for the client and
  the backend.
- **[multiplexer/batching_test.cc](../multiplexer/batching_test.cc):** the
  C++ library: queued events in batches of `max_records` and a flush
  sending the rest, and a batch going at its delay; a receiver getting
  each message as it was sent; the keys of a type batched apart, and one
  key's setting ahead of its type's; a send that does not batch, or has
  no time to wait, going alone after the batch; a batch's message within
  `max_bytes` on the wire; with no delay, a message alone while the
  connection has nothing to write, and those sent while it is busy
  together once it is not, on it, or once it dies, or once another
  connection comes up, a multiplexer that reads nothing keeping it busy;
  such a batch going at its first deadline, and with another batch that
  goes on its own; a timeout counted from when a batch with a delay goes;
  a batch given up on told for each message; a batch that went before its
  delay leaving no timer behind; bytes of another type or with too long a
  key going alone; the settings checked; what does not belong in a batch
  that arrives, a repeat and a batch of more than `MAX_BATCH_RECORDS`
  dropped; and a message out of a batch sent on.
- **[multiplexer/batching_test.py](../multiplexer/batching_test.py):** the
  Python classes: `SyncClient`, `ThreadedClient` and `AsyncClient`
  batching until a flush, each message's callback hearing it written; a
  batched key's batch going before a send that does not batch, and a send
  with no time to wait going alone; a `SyncClient`, a `ThreadedClient` and
  an `AsyncClient` each receiving a batch one message at a time; a batch
  given up on told to `on_drop` for each message; no callback running
  while a collected client goes; a batch recorded by the multiplexer as
  one message marked `batch`, with its key, which `recording.describe()`
  and `mxcontrol dump_recording` print; the settings checked in each
  class.
- **[rust/mx-multiplexer/src/core/tests/batching.rs](../rust/mx-multiplexer/src/core/tests/batching.rs):**
  the Rust core, as the C++ test, and besides each message's `done` and
  tracker hearing its batch's end, a shutdown letting the batches go
  first, a leaving server refusing each message of a batch, and a full
  queue dropping an arriving batch whole and taking its copy once there
  is room.
- **The drains**, a server that drains with its multiplexer down waiting
  for what waits in a batch:
  [multiplexer/backend/acknowledged_test.cc](../multiplexer/backend/acknowledged_test.cc),
  [multiplexer/acknowledged_test.py](../multiplexer/acknowledged_test.py)
  and [rust/mx-multiplexer/tests/servers.rs](../rust/mx-multiplexer/tests/servers.rs).
