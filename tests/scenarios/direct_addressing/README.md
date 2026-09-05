# Direct addressing

A message with `to` set reaches only that instance, whatever the rules say.

## What happens

```mermaid
sequenceDiagram
    participant S as event client
    participant M as multiplexer
    participant L1 as event backend 1
    participant L2 as event backend 2
    Note over S: learns backend 1's instance id
    S->>M: TEST_EVENT, to = backend 1
    M->>L1: delivered by id
    Note over L2: nothing, although the rule says ALL
```

## What is checked

- Only the addressed backend received the message; the routing rule was not consulted.

## Run

```
bazel test //tests/scenarios:direct_addressing_py
```

The suffix is the client's language: `_py` a Python client, `_cc` a C++
one; the backend is the Python one.

The scenario is [direct_addressing.py](direct_addressing.py); the roles it spawns are described in
[tests/README.md](../../README.md).
