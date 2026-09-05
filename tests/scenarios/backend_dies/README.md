# Backend dies

One of two backends crashes mid-run; the survivor takes over.

## What happens

```mermaid
sequenceDiagram
    participant C as client
    participant M as multiplexer
    participant D as doomed backend
    participant S as survivor
    C->>M: requests 1..5
    M->>D: (round robin with S)
    D-->>D: exits after 5, mid-run
    C->>M: request n (was routed to D)
    Note over C: no reply: timeout, then search
    C->>M: BACKEND_FOR_PACKET_SEARCH
    M->>S: search
    S->>C: PING
    C->>M: request n, to = survivor
    S->>C: reply
```

## What is checked

- All 20 queries are answered, none fails.
- The doomed backend exited with 3 after 5 requests; the survivor answered the rest, including the one found again through the search.

## Run

```
bazel test //tests/scenarios:backend_dies_py
```

The suffix is the client's language: `_py` a Python client, `_cc` a C++
one; the backend is the Python one.

The scenario is [backend_dies.py](backend_dies.py); the roles it spawns are described in
[tests/README.md](../../README.md).
