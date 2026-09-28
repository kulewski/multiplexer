# Rules reload peers

A rules reload gives the peers already connected their type's new queue size and passive flag, a controller staying passive.

One multiplexer runs a copy of the rules file with `--rules-check-interval
0`, so that only SIGHUP reloads it. The copy gives TEST_TINY_QUEUE a large
queue: a receiver of that type that never reads takes what is addressed to
it past what its socket holds. The reload puts the type's queue of one
back, and the next message addressed to the receiver draws a
DELIVERY_ERROR naming it, as in direct_queue_full, where the large queue
took it. The reload also makes TEST_EVENT_CLIENT, a passive type, active:
a passive peer is sent one heartbeat per frame it sends, so one that sent
only its welcome gets one; once active, it gets them though it sends
nothing more.

A controller is passive whatever the file says, when it registers and at
every reload. The second test reloads a file that names RULES_CONTROLLER,
peer type 4, without is_passive, while a raw controller is connected. A
raw peer of the active type TEST_ACTIVE_CLIENT is the clock: it is sent a
heartbeat every 3 s of silence, so three of them after the reload put 6 s
behind it, by when the controller, had the reload made it active, would
have been sent one 3 s after the reload. It has been sent nothing, and a
frame it sends draws the one heartbeat it is owed. Counted, not timed:
every wait is for a message, and its bound only detects a failure.

## What happens

```mermaid
sequenceDiagram
    participant S as sender (raw peer)
    participant M as multiplexer
    participant R as receiver (TEST_TINY_QUEUE, never reads)
    participant Q as quiet peer (TEST_EVENT_CLIENT)
    Note over M: the file gives TEST_TINY_QUEUE a large queue
    S->>M: 24 x 1 MiB, to = receiver, report_delivery_error
    M->>R: as much as the socket takes, the rest queued
    M->>Q: the one heartbeat its welcome is owed
    Note over M: SIGHUP: TEST_TINY_QUEUE's queue of one back, TEST_EVENT_CLIENT active
    S->>M: 1 MiB more, to = receiver, report_delivery_error
    M-->>S: DELIVERY_ERROR{failed_to = receiver}
    M->>Q: a heartbeat, though it sent nothing more
```

The controller's test:

```mermaid
sequenceDiagram
    participant K as controller (raw peer, RULES_CONTROLLER)
    participant M as multiplexer
    participant T as clock (raw peer, TEST_ACTIVE_CLIENT)
    K->>M: welcome
    M->>K: the one heartbeat its welcome is owed
    T->>M: welcome
    Note over M: SIGHUP: the file names RULES_CONTROLLER, without is_passive
    M->>T: three heartbeats, 3 s of silence apart
    Note over K: nothing waits in its socket
    K->>M: a heartbeat
    M->>K: the one this frame is owed
```

## What is checked

- After the reload, a message addressed to the receiver draws a DELIVERY_ERROR naming it: the connected receiver took its type's new queue size.
- After the reload, the quiet peer is sent a heartbeat it is not owed as a passive peer: it took its type's new passive flag.
- A controller stays passive through a reload of a file that names its type without is_passive: 6 s after the reload, by the clock's heartbeats, it has been sent nothing, and a frame it sends draws the one heartbeat it is owed.

## Run

```
bazel test //tests/scenarios:rules_reload_peers_test
```

This scenario spawns no roles, so it has one configuration.

The scenario is [rules_reload_peers.py](rules_reload_peers.py); the reload is described in
[docs/operations.md](../../../docs/operations.md#changing-the-rules).
