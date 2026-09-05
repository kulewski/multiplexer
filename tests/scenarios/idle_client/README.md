# Idle client

A passive client idles past both drop intervals and still works.

## What happens

```mermaid
sequenceDiagram
    participant C as client (passive)
    participant M as multiplexer
    participant B as backend
    C->>M: welcome
    Note over C,M: 100 s of silence: longer than 30 s + 60 s
    Note over M: passive type: no heartbeat required, connection kept
    C->>M: request after idling
    B->>C: reply
```

## What is checked

- A passive client that idles past both drop intervals is still connected and its query works.
- Slow: it really waits.

## Run

```
bazel test //tests/scenarios:idle_client_py
```

The suffix is the client's language: `_py` a Python client, `_cc` a C++
one; the backend is the Python one.

The scenario is [idle_client.py](idle_client.py); the roles it spawns are described in
[tests/README.md](../../README.md).
