# Mx hangs

One of two multiplexers hangs without closing its sockets; queries survive, some slowly.

A frozen multiplexer (SIGSTOP) is worse than a dead one: nothing tells the
peers, so a client keeps using the connection until its heartbeats go
unanswered. A query that picks it waits out its timeout, then finds the
backend through the search on the other multiplexer and is answered. After
the heartbeat drop interval the client's own timers close the connection
and nothing is slow any more. When the multiplexer comes back, the client
reconnects. That the client closed the frozen connection itself, which
the resume would hide, is in the thawed multiplexer's log: the connection
leaves, and the client comes back on one of its own.

## What happens

```mermaid
sequenceDiagram
    participant C as ThreadedClient
    participant M1 as multiplexer 1 (frozen)
    participant M2 as multiplexer 2
    participant B as backend
    Note over M1: SIGSTOP: sockets stay open, nothing answers
    C->>M1: query, into the frozen connection
    Note over C: timeout, 4 s
    C->>M1: search
    C->>M2: search
    B->>C: PING via M2
    C->>M2: query again, to = backend
    B->>C: reply
    Note over C,M1: 30 s + 60 s without a heartbeat: the client closes the connection
    C->>M2: every query goes through M2, none slow
    Note over M1: SIGCONT: the connection the client closed leaves, unregistered
    C-->>M1: reconnects, registered again
```

## What is checked

- Every query is answered, none fails.
- Some queries were slow (they picked the frozen connection) and none of them cost more than one timeout: the search found the backend on the other multiplexer.
- No slow query after the heartbeat drop interval: the client had closed the frozen connection itself.
- The client is connected to both multiplexers again at the end.
- The thawed multiplexer's log has the frozen connection leave and the
  client register again after it: the client had closed the frozen
  connection itself. One that kept it would have used it on once the
  multiplexer was back, never registering again, which the checks above
  could not tell.

## Run

```
bazel test //tests/scenarios:mx_hangs_py
```

The suffix is the client's language: `_py` a Python client, `_cc` a C++
one, `_rust` a Rust one; the backend is the Python one. It is tagged slow: it waits out the
drop interval.
