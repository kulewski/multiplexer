# Idle CPU

An idle deployment uses no CPU: every wait loop sleeps, none spins.

A multiplexer, a backend, a synchronous client and a threaded client sit
idle for a while, heartbeats and all. The CPU time of every process over
that window, read from /proc, stays within a few hundredths of a second: a
loop that spins would burn a whole core into it. This is the check that a
wait somewhere did not turn into a busy loop.

## What happens

```mermaid
sequenceDiagram
    participant S as sync client (passive)
    participant T as threaded client (active)
    participant M as multiplexer
    participant B as backend
    Note over S,B: everyone connected, then 8 s of nothing
    T-->>M: heartbeat every 3 s
    B-->>M: heartbeat every 3 s
    Note over S,B: CPU time of each process read from /proc before and after
    S->>M: one query after idling
    M->>B: request
    B->>S: response
```

## What is checked

- Each of the four processes used under 0.15 s of CPU during the 8 s idle window.
- Both clients still get their answer after idling, so the idleness was real, not a hang.

## Run

```
bazel test //tests/scenarios:idle_cpu_py_py
```

The two suffixes are the languages of the roles, first the backend's and
then the client's.
