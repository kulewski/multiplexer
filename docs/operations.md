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

## On Kubernetes

Nothing special is needed: no operator, no leader, no shared state. Each
multiplexer is an independent process that listens on a port, and the
redundancy comes from every peer being connected to all of them. What that
asks of Kubernetes is one thing: a name per instance, because every peer
gets the list and connects to each one, and a Service that load-balances
over the instances would give a peer one connection where it needs all. A
StatefulSet behind a headless Service provides exactly that: `mx-0.mx`,
`mx-1.mx`, `mx-2.mx`, each name one pod, kept across restarts and
reschedules. The libraries resolve a name on every attempt and try each
address it has, so a pod that comes back under a new address is found at
the next reconnect, and a name not published yet, during a rollout, is
retried like a port that refuses.

```yaml
apiVersion: v1
kind: ConfigMap
metadata: {name: mx-rules}
data:
  multiplexer.rules: |
    # your rules file, the one the peers' constants were generated from
---
apiVersion: v1
kind: Service
metadata: {name: mx}
spec:
  clusterIP: None            # headless: a DNS name per pod, no load balancing
  selector: {app: mx}
  ports: [{name: mx, port: 1980}]
---
apiVersion: apps/v1
kind: StatefulSet
metadata: {name: mx}
spec:
  serviceName: mx
  replicas: 3
  selector: {matchLabels: {app: mx}}
  template:
    metadata: {labels: {app: mx}}
    spec:
      affinity:
        podAntiAffinity:     # one per node: a node going away costs one multiplexer
          requiredDuringSchedulingIgnoredDuringExecution:
            - topologyKey: kubernetes.io/hostname
              labelSelector: {matchLabels: {app: mx}}
      containers:
        - name: mx
          image: ghcr.io/kulewski/multiplexer:<version>
          args: [run_multiplexer, --rules, /etc/mx/multiplexer.rules, --address, "0.0.0.0:1980"]
          ports: [{containerPort: 1980}]
          readinessProbe: {tcpSocket: {port: 1980}, periodSeconds: 5}
          livenessProbe: {tcpSocket: {port: 1980}, periodSeconds: 10}
          resources: {requests: {cpu: 100m, memory: 64Mi}}
          volumeMounts: [{name: rules, mountPath: /etc/mx, readOnly: true}]
      volumes:
        - name: rules
          configMap: {name: mx-rules}
---
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata: {name: mx}
spec:
  maxUnavailable: 1          # a drain takes them one at a time, as a restart should
  selector: {matchLabels: {app: mx}}
```

Peers in the same namespace get `mx-0.mx:1980,mx-1.mx:1980,mx-2.mx:1980`;
from another namespace the names carry it, `mx-0.mx.<namespace>:1980`. A
rolling update of the StatefulSet restarts the pods one at a time and
waits for each to be ready, which is the procedure under "Restarting one"
below; with the peers on all three, it costs nothing. Scaling up is one
more replica and its name in the peers' lists. An edit of the ConfigMap
needs no rollout: the pods put the new file in use on their own, see
"Changing the rules" below. A ClusterIP Service per pod,
which older libraries needed for an address that never changes, still
works; it is just no longer required.

The image runs as `nonroot` and holds only the binary, so there is no
shell to `kubectl exec` into; the logs go to stderr, and
`MX_LOG_VERBOSITY` in `env` sets their level. A recording directory or a
peers file, if you use them, want a volume of their own, `emptyDir` for a
recording nobody keeps or a `volumeClaimTemplate` for one somebody does.
[Packaging](packaging.md) describes the image.

## Starting one

```
mxcontrol run_multiplexer --address 10.0.0.1:1980 --rules /etc/mx/deployment.rules
```

It has no other dependencies: no state directory, no companion process. Run
it under your process supervisor; it exits with status 0 on `SIGTERM`, and a
restart has no side effects beyond the connections it drops.
[mxcontrol](mxcontrol.md) lists the options.

## Changing the rules

A running multiplexer puts a changed rules file in use without a restart.
It reads the file again every 2 s (`--rules-check-interval`, 0 turns it
off) and, once two checks in a row have read the same new bytes, parses
the whole file and swaps it in: from then on a message type added to the
file is routed, a peer type added is accepted at its next connection
attempt, a rule edited routes the next message its way, and the peers
already connected take their type's new `queue_size` and `is_passive`,
while an active peer gone silent is still dropped 90 s after its last
frame, reloads or not. The second
check is what keeps a file caught in the middle of being written from
ever being applied; it costs one more interval. A file that is missing,
empty, without a peer type, that does not parse, that repeats a number or
a peer name, or that names a peer that does not exist, changes nothing:
the rules in use stay, the log says why once, and `mxcontrol rules
status` repeats the reason until a later read,
the next check, a `SIGHUP` or a `reload`, finds the file good. At start
such a file is fatal instead.

Two more ways to say "now":

- `SIGHUP`, the Unix convention, what `ExecReload=/bin/kill -HUP $MAINPID`
  in a systemd unit sends. The log says what happened, or that the file is
  the rules in use.
- `mxcontrol rules reload -M host:port -M ...`, over the protocol, from
  anywhere on the network: one line per multiplexer comes back with the
  fingerprint of the rules now in use, `reloaded`, `unchanged`, or the
  error. `mxcontrol rules status` asks without reloading; the fingerprint
  is the one the generated constants carry, so the answers show whether
  every replica runs the same file. [mxcontrol](mxcontrol.md#rules) has
  the options.

What the swap does not do: a peer already connected whose type the file no
longer names stays connected, since dropping it would turn an edit into an
outage; the log counts such peers, and they are gone when they next
reconnect. A recording that spans a reload keeps the fingerprint of its
header. And peers still learn new types only from their generated
constants, so a new type is usable once the programs that send or serve it
are rebuilt with the new file; roll that out at leisure, since old peers
do not send the new types and a multiplexer with the old file drops a new
type with a delivery error rather than misrouting it. Each multiplexer
picks the change up on its own, seconds apart, as a rolling restart would.

The multiplexers read the file at the path they were given, following
symlinks, so any way of changing it works: an editor, `cp`, a
configuration management tool, or a mount that changes underneath. Each
check reads it on the multiplexer's io thread, the one that routes, with
a blocking read, so keep the file on local storage, a local disk or a
ConfigMap volume: a network or FUSE mount that stalls holds up routing,
heartbeats and signals for as long as a read waits. Where the file must
live on such a mount, `--rules-check-interval 0` stops the checks, and a
`SIGHUP` or `mxcontrol rules reload` reads it when asked. The
tidy way is to write the new file next to the old one and rename it over,
which is what the kubelet does and what editors that never leave a torn
file do; a tool that truncates and rewrites in place leaves a moment of
emptiness, which the multiplexer refuses, and a moment of half a file,
which the second check catches. Two things to know:

- On Kubernetes the manifest above mounts the ConfigMap as a directory at
  `/etc/mx`, which is what makes an update arrive: the kubelet writes the
  new file into a fresh directory and swaps one symlink, and the
  multiplexer sees the new file at its next check. A `subPath` mount of the
  single file never updates, and neither does a ConfigMap marked
  `immutable: true`; both need the rollout restart, which still works. The
  kubelet's own delay, its sync period plus its cache, is up to a minute or
  two after `kubectl apply`.
- A file bind-mounted on its own into a container (`docker run -v
  file:file`) keeps the inode it had, and an editor that saves by rename
  replaces the inode, so the container keeps seeing the old file. Mount
  the directory instead.

The `rules_edited_on_disk`, `rules_reload_on_sighup` and
`rules_reload_by_mxcontrol` scenarios in `tests/scenarios/` show each
trigger, the first one against a ConfigMap-style mount;
`rules_reload_peers` and `rules_reload_silent_backend` what a reload does
to the peers connected.

## Restarting one

Restart multiplexers one at a time. With the others up, the restart costs
nothing: a request in flight on the dead connection goes out again through
another at once, and every peer is back on the restarted multiplexer within
about 3 s. Messages the multiplexer held for delivery at that moment are
lost.

With a single multiplexer there is nothing to fall back to. A threaded
client sends its in-flight requests again as soon as it is reconnected, a
synchronous client's current call waits for the reconnect and sends again,
and the request is then answered if its backend is back on the fresh
multiplexer first, or fails with `OperationFailed` if the client got there
first, since a multiplexer with nobody of the type reports a delivery
error. Both reconnects are scheduled 3 s after the drop, so the order is
chance. This is the reason to run at least two.

The `mx_restarts`, `threaded_mx_restarts` and `rolling_restart` scenarios
in `tests/scenarios/` record exactly what a backend and a client see
across a restart.

## Restarting backends

A backend that exits immediately takes the requests it holds with it; the
clients recover through the search, at the price of one timeout each. A
backend that drains avoids that: asked to leave, it tells every multiplexer
to route it nothing new by the rules, serves what was already on its way,
and exits as soon as every multiplexer has confirmed and its work is done.
Both libraries provide this: `start_draining()` from `periodic_task()`, and
`serve_forever(drain_seconds=...)` returning once `drained()`, with
`drain_seconds` the most a drain may take. The echo example uses it.

How the backend is asked to leave is the deployment's choice, checked from
`periodic_task()` within one poll. The reliable form is a file: a preStop
hook writes it, the backend sees it, and the termination grace period is
longer than the drain. The library handles no signals, because a Python
handler runs only between iterations and a C++ library in the same process
can replace it; a pure C++ backend may set a flag from a handler of its
own. With that, a rolling restart of backends costs nobody a timeout or a
retry, as the [backend_drains](../tests/scenarios/backend_drains/README.md)
scenario checks: from the confirmation on, the multiplexer routes the
draining backend nothing by the rules, requests, events and searches
alike, and a peer alone of its type either drains as the last resort,
`Routing(any=False, all=False, last_resort=True)`, or fails its callers at
once. What was routed in the moment before a multiplexer applied the
change is served, and a threaded backend refuses what reaches it once it
is closing, with `DELIVERY_ERROR`, so that costs a retry rather than a
timeout, while a `BaseMultiplexerServer` loses what arrived after its
last read. A backend that dies without draining costs its clients a
timeout per request it held. [How a backend leaves](leaving.md) draws the
three phases and the routing flags; a recording notes each skip as
`NOT_ACCEPTED`.

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

**How much is logged.** Levels above `DEBUG` are always emitted. `DEBUG`
entries have a verbosity, and the default shows a process's connections
coming and going, at `HIGHVERBOSITY`, and not its traffic: the
per-message entries, a request and the connection it took, a message
routed, a connection skipped because it is full, are at `CHATTERBOX` and
off unless asked for. The environment variable `MX_LOG_VERBOSITY`, read
once when the library loads, sets this without a rebuild or a call:
`MX_LOG_VERBOSITY=DEBUG:CHATTERBOX` turns the traffic log on for one
process, `MX_LOG_VERBOSITY=DEBUG:LOW` quiets a chatty one, and a bare
verbosity, `MX_LOG_VERBOSITY=MEDIUM`, applies to every level; names are
those of the constants, with or without the `VERBOSITY` suffix, in any
case. A malformed value is reported at `WARNING` and ignored. In code,
`set_maximal_logging_verbosity(level, verbosity)` does the same in both
languages; `mxcontrol --verbosity` is the multiplexer's own setting and
wins over the variable when given explicitly. A disabled entry costs one
comparison, so leaving the library's debug lines compiled in is free; an
emitted one costs a write to stderr per line, which is why traffic is off
by default.

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
carries the multiplexer's instance id and a fingerprint of the rules file, so a
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
`QUEUE_FULL`, or `NOT_ACCEPTED` for a peer whose routing turned the path
off ([how a backend leaves](leaving.md)). A `PeerEvent` marks a peer
arriving, leaving, or changing its routing. A rules file put in use while
the session is open ([changing the rules](#changing-the-rules)) leaves a
`rules` record with the new fingerprint, from which the numbers are the
new file's; both readers show it, and `recording.read()` refuses to go on
past one that differs from its constants. Read a recording with readers
as new as the multiplexer that made it: an older one shows what it does
not know as something it does, a `NOT_ACCEPTED` route as `DELIVERED` say.

Recording costs one serialization and one buffered write per message and
grows by the payloads; `--record-payload-bytes N` keeps only the first N
bytes of each (`truncated` is set), which is enough to see what happened
and keeps a long recording small. Use it for a session, not forever: there
is no rotation.

### Recording on demand, over the protocol

A running multiplexer can be asked to record by any peer, so a session on a
cluster starts and stops from one command, without restarts or a shell on
the hosts. It is off unless the multiplexer was started with one of:

| Option | Allows |
|---|---|
| `--recording-dir DIR` | file sessions: a peer's START opens a file under `DIR`, its STOP closes it |
| `--allow-tap` | taps: a peer receives every record over its own connection, live, and nothing is written on the host |

The request is a `RECORDING_CONTROL` message (reserved type 6, payload
`RecordingControl`) sent without `to` on a connection to the multiplexer;
the answer is a `RECORDING_STATUS` (7) referencing it. A peer may connect
for this alone as the reserved peer type `RECORDING_CONTROLLER` (3),
accepted only when one of the options is on, so no rules file needs an
entry. Anyone who can reach the port can ask, as with everything else on
this network; the options are the operator's consent.

`mxcontrol recording` is the command
([mxcontrol](mxcontrol.md#recording)); from Python, `multiplexer.recording`
has `start()`, `stop()`, `status()` and `tap()` ([api](api_python.md#recording)).

**File sessions.** A START names a session with a label, letters, digits,
`-` and `_` only; the multiplexer chooses the path:

```
<recording-dir>/<label>.<UTC time>.<multiplexer instance id>.rec
```

so several replicas writing into one shared directory never collide, and
neither do two sessions on one replica. The status reply carries the path.
The file starts with the header, then one `CONNECTED` event for every peer
connected at that moment, so it stands on its own. One session at a time
per multiplexer: a second START is refused until STOP. A session closes by
itself at `max_bytes`, one gibibyte unless the request says otherwise (0 for
no cap), or after `max_seconds`; the status then says why it stopped and
where the file is. A multiplexer started with `--record` has a session too,
without a label, which a STOP closes.

**Taps.** A TAP subscribes the requesting peer: every record goes to it as
a `RECORDING_RECORD` message (8) carrying the `Record`, with
`multiplexer_id` set, until UNTAP or the connection ends. The tap's outgoing
queue is the only buffer: a peer that reads too slowly loses records, which
the multiplexer counts in the status as `dropped`, and routing is never
held up. A tap costs one serialization and one queued frame per record for
each tap.

**Several replicas.** Every multiplexer answers for itself, so a controller
connects to each: `mxcontrol recording` takes `-M host:port` repeatedly and
resolves a host name to every address it has, so a headless Kubernetes
service name reaches every pod. A replica replaced mid-session comes back
not recording, since the state lives in the process: `mxcontrol recording
start --stay` keeps polling and starts the session again on any replica
that has never had one; a tap resubscribes the same way. Read the files of
a session together with `dump_recording FILE...` or
`multiplexer.recording.read_many()`, which merge them by timestamp and tag
each record with its multiplexer. Timestamps are each multiplexer's own
clock, so order within a replica is exact and order between replicas is as
good as their clocks.

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
