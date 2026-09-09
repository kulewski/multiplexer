# Recording tap

A peer taps two multiplexers and receives every record live, tagged with the multiplexer that routed it; a slow tap loses records but nothing else.

`mxcontrol recording tap` subscribes on every connection and writes the
records it receives as one stream, each carrying the multiplexer's id;
nothing is written on the multiplexers' side. A tap that stops reading fills
its outgoing queue: the multiplexer drops its records, counts them in the
status, and keeps routing.

## What happens

```mermaid
sequenceDiagram
    participant T as mxcontrol recording tap
    participant M1 as multiplexer 1
    participant M2 as multiplexer 2
    participant E as event client
    T->>M1: TAP
    T->>M2: TAP
    E->>M1: event (through every connection)
    E->>M2: event
    M1-->>T: RECORDING_RECORD mx=1
    M2-->>T: RECORDING_RECORD mx=2
    Note over T: SIGINT
    T->>M1: UNTAP
    T->>M2: UNTAP
    Note over M1: a second tap stops reading, 1600 records routed
    Note over M1: queue full, records dropped and counted, routing unaffected
```

## What is checked

- Both multiplexers report one tap; an event sent through both connections arrives in the tap's output twice, once per multiplexer id, along with the backend's arrival; the output has no file header and no file appears on the multiplexers' side.
- SIGINT makes the tap untap and exit 0, reporting how many records it wrote.
- A tap that does not read loses records once its queue is full: the status counts the drops, the queries all succeed, and a file session opened beside it has every one of them.

## Run

```
bazel test //tests/scenarios:recording_tap_py_py
```
