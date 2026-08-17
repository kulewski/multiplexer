# Two mx backends on each

Two multiplexers with a backend behind each; a client on both.

## What happens

```mermaid
sequenceDiagram
    participant C as client
    participant M1 as multiplexer 1
    participant M2 as multiplexer 2
    participant B1 as backend 1
    participant B2 as backend 2
    Note over B1,B2: both connected to both multiplexers
    C->>M1: request 1
    M1->>B1: ...
    C->>M2: request 2
    M2->>B2: ...
    Note over C: requests alternate between connections and backends
```

## What is checked

- Every query is answered on the full mesh, and both backends and both connections carried work.

## Run

```
bazel test //tests/scenarios:two_mx_backends_on_each_py
```

The suffix is the client's language: `_py` a Python client, `_cc` a C++
one; the backend is the Python one.

The scenario is [two_mx_backends_on_each.py](two_mx_backends_on_each.py); the roles it spawns are described in
[tests/README.md](../../README.md).
