# Mx restarts

The multiplexer restarts under a live backend and a passive client; nobody notices.

The backend, a `BaseMultiplexerServer` that runs its loop all the time,
reconnects after AUTO_RECONNECT_TIME. The client's next call finds its
connection dead before it writes, waits for the reconnect inside the call
and sends then, so every query is answered and none fails. The client is
frozen in its idle time across the restart, until the backend is
registered again: a passive client notices nothing meanwhile, and its
next call comes after both, however long the restart takes.

## What happens

```mermaid
sequenceDiagram
    participant C as client
    participant M as multiplexer
    participant B as backend
    C->>M: request 1
    B->>C: reply 1
    Note over C: idle, frozen (SIGSTOP)
    Note over M: restart
    B-->>M: reconnect after 3 s
    Note over C: resumed once the backend is in the peers file
    Note over C: the next call finds the connection dead before it writes, and waits for its reconnect
    C-->>M: reconnect after 3 s, inside the call
    C->>M: request 2, once
    B->>C: reply 2
    C->>M: request 3
    B->>C: reply 3
```

## What is checked

- No query fails across the restart; the second one takes a few seconds,
  less than its 30 s timeout, which a query that waited for it would take
  whole.
- The backend saw every request, at least once each.

## Run

```
bazel test //tests/scenarios:mx_restarts_py
```

The suffix is the language of the client and the backend: `_py` Python,
`_cc` C++, `_rust` Rust.

The scenario is [mx_restarts.py](mx_restarts.py); the roles it spawns are described in
[tests/README.md](../../README.md).
