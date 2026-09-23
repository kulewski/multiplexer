# Rules reload silent backend

A silent backend is dropped on time while rules reloads keep coming, and a client a reload made passive is not.

The multiplexer drops an active peer that sent nothing for 30 + 60 s. A
reload applies the file's passive flags to the peers connected: a flag
set again unchanged must not start that count over, or reloads less than
90 s apart would keep a hung backend connected for good, and a flag
turned on ends it. One multiplexer runs a copy of the rules file with
`--rules-check-interval 0`; a raw peer of the client type
TEST_ACTIVE_CLIENT and then one of the backend type TEST_BACKEND_A say
their welcomes and nothing more. Every 10 s the file, with
TEST_ACTIVE_CLIENT made passive, gets a new comment and SIGHUP reloads
it, and the backend is closed before the fifteenth reload. The client,
connected first, would have been closed first; it is still connected,
and sent a heartbeat for one it sends. Slow: it really waits out the
drop.

## What happens

```mermaid
sequenceDiagram
    participant Q as quiet client (raw peer, TEST_ACTIVE_CLIENT)
    participant M as multiplexer
    participant B as silent backend (raw peer, TEST_BACKEND_A)
    Q->>M: welcome, then nothing
    B->>M: welcome, then nothing
    loop every 10 s
        Note over M: the file changes, TEST_ACTIVE_CLIENT passive, and SIGHUP reloads it
        M->>B: heartbeats, read and dropped
    end
    Note over M: 30 s + 60 s after the welcome
    M--xB: connection closed
    Q->>M: a heartbeat
    M->>Q: one back
```

## What is checked

- The silent backend's connection is closed before the fifteenth reload, 10 s apart: reloads do not start its drop over.
- The quiet client, whose type the first reload made passive, is still connected then, though silent longer, and is sent a heartbeat for one it sends.
- Slow: it really waits.

## Run

```
bazel test //tests/scenarios:rules_reload_silent_backend_test
```

This scenario spawns no roles, so it has one configuration.

The scenario is [rules_reload_silent_backend.py](rules_reload_silent_backend.py); the reload is described in
[docs/operations.md](../../../docs/operations.md#changing-the-rules).
