# Event all backends

A client's events reach every event backend (whom: ALL).

## What happens

```mermaid
sequenceDiagram
    participant S as event client
    participant M as multiplexer
    participant L1 as event backend 1
    participant L2 as event backend 2
    S->>M: TEST_EVENT "e1"
    M->>L1: TEST_EVENT (rule: whom ALL)
    M->>L2: TEST_EVENT
    S->>M: TEST_EVENT "e2"
    M->>L1: TEST_EVENT
    M->>L2: TEST_EVENT
    Note over S: nothing comes back
```

## What is checked

- Every event backend received every event, in order.
- The sender reported each event as sent; nobody answered.

## Run

```
bazel test //tests/scenarios:event_all_backends_py_py
```

The two suffixes are the languages of the roles, first the event backend's
and then the event client's.

The scenario is [event_all_backends.py](event_all_backends.py); the roles it spawns are described in
[tests/README.md](../../README.md).
