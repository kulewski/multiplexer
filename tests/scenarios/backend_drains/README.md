# Backend drains

A backend asked to leave drains: it declines searches, serves what arrives, then exits.

Two backends share the requests of a client. One is asked to leave, the
way a preStop hook asks, by a file its periodic_task() notices, and it has a
drain period: from then on it does not answer the search clients use to
find a backend, so no retried request is sent to it, but it keeps serving
the requests the multiplexer still routes to it until the period ends and
it exits. This is the graceful shape of a backend restart.

## What happens

```mermaid
sequenceDiagram
    participant C as client (4 in flight)
    participant M as multiplexer
    participant L as leaving backend
    participant S as staying backend
    C->>M: queries
    M->>L: half of them (round robin)
    M->>S: the other half
    Note over L: drain file appears, periodic_task starts draining
    C->>M: search (a request timed out somewhere)
    M->>L: search
    Note over L: declined, no PING
    M->>S: search
    S->>C: PING
    M->>L: requests keep coming for 3 s, and are served
    Note over L: drain over, close, exit 0
    M->>S: everything from now on
```

## What is checked

- The leaving backend exits with 0 once drained, and served requests after it started draining.
- Every query is answered, none fails, and none took anywhere near a timeout: the restart was invisible to the client.

## Run

```
bazel test //tests/scenarios:backend_drains_py_py
```

The two suffixes are the languages of the roles, first the backend's and
then the client's.
