# Metrics

`run_multiplexer --monitor-address HOST:PORT`, 4.1.0 or newer, serves `/metrics` beside
`/healthz` and `/readyz` ([health](operations.md#health)): what the
multiplexer has counted since it started, in Prometheus' text format,
version 0.0.4, sent as `text/plain; version=0.0.4; charset=utf-8`.
Nothing is served without the option. Most of all it says what went
nowhere, and why.

## Scraping it

Each multiplexer is a target of its own, its monitor's port and the
path `/metrics`. Next to the StatefulSet of
[operations](operations.md#on-kubernetes), whose pods name that port
`monitor`, the Prometheus Operator's PodMonitor is:

```yaml
apiVersion: monitoring.coreos.com/v1
kind: PodMonitor
metadata: {name: mx}
spec:
  selector: {matchLabels: {app: mx}}
  podMetricsEndpoints: [{port: monitor, path: /metrics, interval: 15s}]
```

Elsewhere, one `static_configs` target per multiplexer address does it.
Prometheus labels each series with the target, `instance`, and with the
pod and its namespace under Kubernetes' discovery; the multiplexer adds
no label saying which it is, since its instance id changes at every
restart and would start new series each time.

Every counter counts from the process's start, so a restart takes it
back to 0: `rate()` and `increase()` take that fall as a reset, and
`process_start_time_seconds` says when each process started. Nothing is
kept across a restart.

What a scrape costs the multiplexer: a task on each event loop, and one
pass over its connections on the main loop, under the routing lock on
several loops. Between scrapes nothing of it runs. The counts are kept
as routing goes: two additions for each frame a peer sends, on that
connection's own loop, and one for each drop, on its failure path. A
scrape is answered 503 when the main loop does not run its pass within
2 s, and Prometheus then marks the target down (`up` 0), as `/healthz`
fails for the same loop.

## What it reports

| Metric | Type | Labels | What it is |
|---|---|---|---|
| `mx_messages_dropped_total` | counter | `reason`, `type`, `peer_type` | messages, or a peer's copy of one, that reached no peer they were routed for: [below](#drops) |
| `mx_messages_received_total` | counter | | frames peers sent the multiplexer to route, a batch one frame; never its own DELIVERY_ERRORs, nor the peers' welcomes and heartbeats |
| `mx_received_bytes_total` | counter | | their bytes, the 8-byte frame headers included |
| `mx_peers` | gauge | `peer_type` | registered connections, for every peer type of the rules a peer may connect as, 0 where nobody is connected |
| `mx_queued_messages` | gauge | `peer_type` | frames waiting in the multiplexer's outgoing queues, the one being written included, by the peer type they wait for: what comes before `queue_full` drops |
| `mx_event_loop_lag_seconds` | gauge | `loop` | how long the check the scrape posted to each event loop, 0 the main one as the log numbers them, waited for the loop to run it; for a loop that has not run it, how long it has waited so far, 2 s or more. A busy loop shows here before `/healthz` fails |
| `mx_accept_failures_total` | counter | | connections the multiplexer could not accept, out of descriptors or memory, each tried again 0.1 s later |
| `mx_rules_info` | gauge | `fingerprint` | always 1, its label the fingerprint of the rules in use, as `mxcontrol rules status` reports it |
| `mx_rules_file_failing` | gauge | | 1 while the rules file on disk is not in use, missing or not parsing, the rules loaded before still routing |
| `mx_stopping` | gauge | | 1 from the moment the multiplexer began to stop, its drain included |
| `mx_build_info` | gauge | `version` | always 1, its label the multiplexer's version |
| `process_start_time_seconds`, `process_cpu_seconds_total`, `process_resident_memory_bytes`, `process_open_fds`, `process_max_fds` | | | the process's own, under the names Prometheus' client libraries give them |

## Drops

Each message that reached no peer it was routed for, or one copy of it,
is counted once in `mx_messages_dropped_total`. `type` is the message's
type and `peer_type` the peer type it was meant for, each by its name in
the rules file; `unknown` stands for a type the rules do not have, so
that a peer naming types of its own adds no series, and the label is
empty where the drop has no such type.

| `reason` | What happened | `type` | `peer_type` |
|---|---|---|---|
| `no_receiver` | no peer of the rule's type was connected to this multiplexer | the message's | the rule's |
| `routing_off` | every peer of the type had turned that routing off, as a draining backend does ([how a backend leaves](leaving.md)) | the message's | the rule's |
| `all_full` | every peer of the type that takes the message had a full queue | the message's | the rule's |
| `key_not_taken` | no peer of the type has a key filter that takes the message's key ([keys](keys.md)) | the message's | the rule's |
| `queue_full` | a peer's copy refused by its full queue: a copy sent to ALL, or a message addressed to it | the message's | the peer's |
| `not_connected` | addressed (`to`) to an instance not connected to this multiplexer | the message's | empty |
| `peer_gone` | queued for a peer whose connection ended before it was written; what the kernel's buffers held by then is not counted | the frame's | the peer's |
| `closing` | sent by a peer whose connection was closing, a stop's drain, a failed write, or a newcomer that took its instance id, and read and dropped | empty | empty |
| `unknown_type` | a type the rules do not have | `unknown` | empty |
| `no_rule` | a type with no routing rule, sent without `to` | the message's | empty |
| `key_too_long` | a key over 256 bytes | the message's | empty |
| `protocol_error` | a protocol message the multiplexer does not know, a backend search it cannot read, or a frame that claims the multiplexer's own instance id | the message's | empty |

What to know reading them:

- The first four are counted for each rule that queued the message
  nowhere, a type two rules route counted twice when both found nobody;
  `queue_full` counts each refused copy. A batch is one frame, one drop.
- A drop here is not always a loss. A query whose request no backend took
  here searches on, and its answer can still come through another
  multiplexer; a backend search that finds nobody is not a drop. A plain
  event dropped here is lost, unless it was a confirmed send, which its
  producer sends again ([delivery](delivery.md)).
- Drops inside the peers' libraries, a client's own queue full or a
  send's timeout, never reach the multiplexer: a program hears of each
  through `on_drop` with its `DropReason` and may export them itself.
- `no_receiver` while a deployment rolls is expected for a moment; for
  long, or on one multiplexer only, it says a peer type is missing there,
  which `mx_peers` shows too.

`multiplexer/metrics_test.py` brings about each reason but `routing_off`
and `closing`, and checks the counts.

## Across replicas and restarts

```
# what went nowhere in the last 5 minutes, and why, every multiplexer together
sum by (reason, type, peer_type) (increase(mx_messages_dropped_total[5m])) > 0
# which multiplexer drops
sum by (instance, reason) (rate(mx_messages_dropped_total[5m]))
# a peer type connected to some multiplexers and not to others
max by (peer_type) (mx_peers) - min by (peer_type) (mx_peers) > 0
# traffic, every multiplexer together
sum(rate(mx_messages_received_total[5m]))
# an event loop falling behind
max by (instance) (mx_event_loop_lag_seconds) > 0.05
# restarts
changes(process_start_time_seconds[1h]) > 0
# replicas on different rules, and a rules file that does not parse
count(count by (fingerprint) (mx_rules_info)) > 1
max(mx_rules_file_failing) > 0
# near the descriptor limit
max by (instance) (process_open_fds / process_max_fds) > 0.8
```

Every peer connects to every multiplexer, so `mx_peers` summed over the
replicas counts each peer once for each multiplexer: take its `max`. The
counters are each multiplexer's own, and summed they are the cluster's.
