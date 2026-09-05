# Operations

## Several multiplexers

Run one multiplexer per host you want to survive losing, each with the same
rules file, each on its own address. They do not know about each other. Give
every backend and every client the full list of addresses; the libraries
connect to all of them and keep reconnecting to any that go away.

A request uses one connection and falls back to the others; an event sent
through one connection reaches every backend if every backend is connected
to every multiplexer, which is the point of the full mesh. The
[overview](README.md) draws the healthy deployment.

Two multiplexers on one host, on two ports, protect against a multiplexer
process dying but not against the host going, and they double every
`whom: ALL` fan-out's cost, so prefer one per host.

## Starting one

```
mxcontrol run_multiplexer --address 10.0.0.1:1980 --rules /etc/mx/deployment.rules
```

It has no other dependencies: no state directory, no companion process. Run
it under your process supervisor; it exits with status 0 on `SIGTERM`, and a
restart has no side effects beyond the connections it drops.
[mxcontrol](mxcontrol.md) lists the options.

The rules file is read once. After editing it, restart every multiplexer and
rebuild every peer, since the generated constants come from the same file.
Adding entries is safe to roll out gradually: old peers do not send the new
types, and a multiplexer with the old file drops a new type with a delivery
error rather than misrouting it.

## Restarting one

Restart multiplexers one at a time. During the restart:

- backends lose that connection and get it back within about 3 s of the
  multiplexer being up again;
- a client's next call notices the dead connection, waits inside the call
  for the reconnect, about 3 s, and sends the request again with a fresh
  id; with several multiplexers it uses another connection at once and the
  restart costs nothing;
- a threaded client sends its in-flight requests again as soon as it is
  reconnected;
- messages queued on the multiplexer for delivery are lost.

The `mx_restarts` and `rolling_restart` scenarios in `tests/scenarios/`
record exactly what a backend and a client see across a restart.

## Restarting backends

A backend that exits immediately takes the requests it holds with it; the
clients recover through the search, at the price of one timeout each. A
backend that drains avoids that: asked to leave, it stops answering the
backend search, so no client sends it a retried request, keeps serving what
the multiplexer still routes to it for a few seconds, and then exits. Both
libraries provide this: `start_draining()` from `periodic_task()`, and
`serve_forever(drain_seconds=...)` returning once `drained()`. The echo
example uses it.

How the backend is asked to leave is the deployment's choice, checked from
`periodic_task()` within one poll. The reliable form is a file: a preStop
hook writes it, the backend sees it, and the termination grace period is
longer than the drain. The library handles no signals, because a Python
handler runs only between iterations and a C++ library in the same process
can replace it; a pure C++ backend may set a flag from a handler of its
own. With that, a rolling restart of backends costs nobody a timeout, as
the [backend_drains](../tests/scenarios/backend_drains/README.md) scenario
checks. What the drain cannot avoid: the multiplexer keeps routing
`whom: ANY` requests to a draining backend until it disconnects, so the
drain must be long enough to answer them, and a backend that dies without
draining still costs its clients a timeout.

## Debug symbols

Release builds (`--config=release`) compile with symbols and strip the
binaries that ship: `//mxcontrol:mxcontrol` is the stripped copy of
`//mxcontrol:mxcontrol_with_debug_symbols`, and `_native.so` of
`_native_with_debug_symbols.so`, the way `bazel/maybe_strip.bzl` describes.
Keep the unstripped one next to a release, for cores and profiles; both
targets are public. In other build modes the stripped name is a symlink to
the unstripped binary.

## Logs

The multiplexer and both libraries log to stderr, one entry per line, with
the level, timestamp, pid, context, workflow id, message and source
location. The multiplexer logs every peer that registers and leaves at `INFO`, every
undelivered message at `ERROR` or `WARNING` according to the
rule, and every message dropped for a full queue at `WARNING`.

`--logging-file PATH` on `mxcontrol` writes the same entries as a binary
stream of `LogEntry` protocol buffers, each preceded by its length as a
varint. `mxcontrol streamlogs` can send such a stream on through a
multiplexer, and `mxcontrol receivelogs` prints what arrives; together they
are a minimal log shipper and the smallest example of a backend.

The Python library logs through the same mechanism, so a Python backend's
stderr has the same shape.

## Watching it

There is no status port and no metrics endpoint. What you have:

- the log, above: registrations tell you who is connected, undelivered
  messages tell you when a backend type has nobody behind it;
- `mxcontrol receivelogs`, or any backend, connected as a peer type that
  your rules also route a message type to, as a tap on that type;
- the peers themselves: a client that gets `OperationFailed` has learned that
  no backend of that type is connected anywhere, and a backend's
  `connections_count()` says how many multiplexers it currently reaches;
- the peers file and the recording, below.

A `PING` addressed to a backend's instance id is answered by the backend's
library, which makes a liveness check possible from any client.

### Who is connected

`run_multiplexer --peers-file PATH` keeps a file with one line per connected
peer, `<instance id> <peer type name> <peer type>`, rewritten atomically
(written next to it, then renamed) on every registration and departure. A
shell reads it; so does `Mx.connected_peers()` in the test infrastructure,
and `Cluster.wait_for_peer()` waits on it. It is empty while nobody is
connected, and is left behind as it was when the multiplexer exits.

### Recording

`run_multiplexer --record PATH` writes every routed message to a file as it
passes through: who sent it, who received it or why nobody did, its type,
its ids and its payload, plus every peer arriving and leaving. The file is a
stream of length-prefixed `Record` protocol buffers
([multiplexer/Recording.proto](../multiplexer/Recording.proto)); its header
carries the multiplexer's instance id and the SHA-1 of the rules file, so a
reader knows which numbering the types are in. Messages the protocol
exchanges for itself, heartbeats and welcomes, are not routed and are not
recorded.

Read it with `mxcontrol dump_recording FILE --rules FILE` (names from the
rules file, `--type` and `--peer` to filter), with
`bazel run @mx//multiplexer:dump_recording -- FILE` (names from the
generated constants), or from Python with `multiplexer.recording.read()`,
which yields the records and refuses a file made with other rules than the
constants were generated from. A `RoutedMessage` says whether it was
`DELIVERED` or why not: `NO_RECIPIENT`, `UNKNOWN_TYPE`, `NO_RULE`,
`QUEUE_FULL`.

Recording costs one serialization and one buffered write per message and
grows by the payloads; `--record-payload-bytes N` keeps only the first N
bytes of each (`truncated` is set), which is enough to see what happened
and keeps a long recording small. Use it for a session, not forever: there
is no rotation.

## Sizing

One multiplexer is one thread with one event loop. Every message is forwarded
as the bytes it arrived in, without re-serialization, so the cost per message
is a few map lookups and the socket writes. Memory is the sum of the outgoing
queues: up to `queue_size` messages per connection, each held as long as it
is unsent, so a slow backend can pin up to 1024 messages of whatever size
your peers send. Lower `queue_size` for peer types that receive large
messages.

Backends are where the work is; add more of a type and `whom: ANY` spreads
requests over them. A backend handles one message at a time, so a slow
handler is a slow backend, and a request that takes longer than the client's
timeout is repeated to another backend by the search.
