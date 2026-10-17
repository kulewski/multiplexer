# Backend drains until done

A backend decides for itself when its drain is over, by overriding drained(); alone of its type, it drains as the last resort.

The only backend is asked to leave after a few requests, with a drain period
of half a second but a rule of its own: it refuses to leave before it has
served every request of the run. Being alone of its type it drains as the
last resort, so the multiplexer keeps routing to it what nobody else could
take. A first client's five requests come before it is asked, a second
client's twenty after its drain began, one every 100 ms, the last of them
seconds after the period is over. The drain lasts as long as the rule
says, the backend serves everything, and exits cleanly only then.

## What happens

```mermaid
sequenceDiagram
    participant C1 as first client
    participant C2 as second client
    participant M as multiplexer
    participant B as backend (drained: all 25 served)
    C1->>M: 5 requests
    M->>B: requests, served
    Note over B: drain file appears, start draining as the last resort
    C2->>M: 20 requests, one every 100 ms
    Note over B: 0.5 s later the period is over, drained() still says no
    M->>B: requests keep coming, nobody else could take them, and are served
    Note over B: request 25 served, drained() says yes, close, exit 0
```

## What is checked

- Every query of both clients is answered; none fails.
- The backend served all 25, the second client's twenty after its drain
  began, and exited with 0 only then, well past its half-second drain
  period.

## Run

```
bazel test //tests/scenarios:backend_drains_until_done_py_py
```

The two suffixes are the languages of the roles, first the backend's and
then the client's.
