# Remote recording stay

A session kept by `mxcontrol recording start --stay` follows a replica through a restart and ends on SIGINT.

Recording state lives in the multiplexer process, so a replica replaced
mid-session comes back not recording. With `--stay` the command keeps its
connections, asks every multiplexer for its status every couple of seconds,
starts a session on any that has never had one, and stops every session
when it is interrupted.

## What happens

```mermaid
sequenceDiagram
    participant X as mxcontrol recording start --stay
    participant M1 as multiplexer 1
    participant M2 as multiplexer 2
    X->>M1: START label=kept
    X->>M2: START label=kept
    Note over M1: restart: new instance, not recording
    X->>M1: STATUS (every 2 s)
    M1-->>X: not recording, no session yet
    X->>M1: START label=kept
    Note over M2: its session goes on
    Note over X: SIGINT
    X->>M1: STOP
    X->>M2: STOP
```

## What is checked

- Both multiplexers record under the label; after one restarts, the new instance records again within the poll interval plus the reconnect delay, into a new file, while the other's session is untouched: three files in all.
- SIGINT stops every session (each status says stopped by peer) and the command exits 0 having printed that it started the replacement's session.

## Run

```
bazel test //tests/scenarios:remote_recording_stay_test
```
