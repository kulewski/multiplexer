# Integration tests

`bazel test //...` runs everything. `--test_tag_filters=-slow` skips the
scenarios that wait out heartbeat and reconnect intervals; `lang-py` and
`lang-cc` select by the language of the role binaries.

## Layout

- `testing.rules`: the example rules plus a test section (peers and types
  from 201 up). `tests/BUILD` generates `testing_constants.py` from it, so the
  tests never depend on the rules file a deployment builds with.
- The harness is [multiplexer/testing](../multiplexer/testing), a public
  package any workspace that depends on `@mx` can import: `Cluster(n)`
  starts n multiplexers on ephemeral ports through `--address 127.0.0.1:0
  --port-file`, `spawn(role, lang, ...)` launches a role and collects its
  Event events, `Role.wait_for(event, **fields)` waits for one,
  `Cluster.wait_for_peer(type)` and `wait_for_peer_gone(type)` wait on the
  multiplexers' peers files,
  `wait_until(predicate, timeout, what)` on anything else. Every process's
  stderr and events land in the test's undeclared outputs directory.
  `harness/` here re-exports it with the constants of `testing.rules`.
  `FakePeer`, `BackendThread` and `TestClient` are the in-process peers for
  unit tests, described in [docs/api_python.md](../docs/api_python.md#testing).
- `roles/py/` and `roles/cc/`: the same four roles in both languages, with
  the same options and the same Event events, so a scenario can be run with any
  mix. `backend` answers requests and `event_backend` only receives events;
  `client` sends requests and waits for answers, `event_client` sends events
  and does not. The suite assumes every client is passive.
- `multiplexer/testing/raw_peer.py`: a peer that speaks the wire format
  directly, for protocol and robustness scenarios.
- `Cluster(record=True)` makes every multiplexer write a recording of what
  it routed (`Mx.record_file`, read with `multiplexer.recording`), the
  `recording` scenarios check it against what the roles reported.
  `Cluster(remote_recording=True)` lets peers start sessions and tap in
  (one shared `recording_dir`, `recording_files()`), and `mxcontrol(*args)`
  runs the tool to completion; the `remote_recording*` and `recording_tap`
  scenarios use both.
- Memory: roles emit `memory` events every `--memory-every` messages with
  the exact C heap in use (and, in Python, tracemalloc bytes and the object
  count); `Cluster(memory_log_every=N)` makes the multiplexer log its heap
  every N routed messages, read back with `Mx.memory_samples()`. The
  `soak_memory` scenario checks all three plateau; `multiplexer/soak_test.cc`
  does the same in-process for the C++ core, and `multiplexer/leak_test.py`
  watches the Python side and the binding with tracemalloc by source line,
  the object count after a collection, reference counts and weak references. Under `check.sh --leaks` the
  harness turns LeakSanitizer on for the C++ processes, so an allocation
  still held at exit fails the scenario through the exit code.
- `scenarios/`: one folder per scenario with the test and a README that
  draws what happens; [scenarios/README.md](scenarios/README.md) is the
  generated index. One `mx_integration_test` target per
  configuration; the macro is `multiplexer/testing/defs.bzl`, `defs.bzl`
  here fixes its rules file and constants. The language matrix is a list
  comprehension in `scenarios/BUILD`.

## Writing a scenario

A scenario must hold up on a loaded machine: it waits for the events it
needs rather than for totals, gives success waits the defaults, and never
counts requests at a backend as if each were delivered once. The
[Testing section](../docs/api_python.md#tests-that-hold-up-under-load)
of the Python API page says why.

```python
from tests import harness
from tests.harness import Cluster, constants as C, spawn

class Example(unittest.TestCase):
    def test_it(self):
        cfg = harness.CONFIG                      # roles, mx count, params
        with Cluster(cfg.mx) as cluster:
            backend = spawn("backend", cfg.lang("backend"), mx=cluster.addresses,
                            type=C.peers.TEST_BACKEND_A,
                            serves={C.types.TEST_REQUEST_A: C.types.TEST_RESPONSE})
            backend.wait_for("connected", connections=cfg.mx)
            client = spawn("client", cfg.lang("client"), mx=cluster.addresses,
                            type=C.peers.TEST_CLIENT,
                            query=[(C.types.TEST_REQUEST_A, "hello")])
            self.assertEqual(0, client.wait())
            self.assertEqual("HELLO", client.events_of("response")[0]["payload"])

if __name__ == "__main__":
    harness.main()
```

Then in `scenarios/BUILD`:

```
mx_integration_test(
    name = "example_py_cc",
    roles = {"backend": "py", "client": "cc"},
    scenario = "example.py",
)
```

`mx = 2` starts two multiplexers, `params` reaches the scenario through
`cfg.param()`, `deps` and `data` add what it needs. From another workspace,
`load("@mx//multiplexer/testing:defs.bzl", "mx_integration_test")`; there
`rules` defaults to the file the `multiplexer_rules` flag names, the one
your constants come from, and the scenario imports `multiplexer.testing`
directly with its own constants. A `Cluster()` in a plain test of yours,
outside the macro, runs with that same file.

## Playing a role with your own binary

A value in `roles` may be the label of a binary instead of `"py"` or
`"cc"`: it is added to the test's data and the scenario sees `"bin"` for
that role, so `spawn(role, cfg.lang(role), ...)` runs it like a shipped
role. The contract is the command line and the events:

- Options: `--mx host:port` for every multiplexer (repeated), `--type N`
  for the peer type, `--name` for a label the events may carry; then
  whatever `spawn(..., option=value)` adds, one `--option value` each (see
  `_argv` in `multiplexer/testing/__init__.py`). `--drain-file PATH` is
  passed only with `spawn(..., drain_file=True)`.
- Events: one `Event` (`multiplexer/testing/events.proto`) per line of
  stdout in protocol buffer text format, `event: "connected" instance_id: 7
  connections: 1`, and any fields of the message. A line that is not an
  Event is kept as a `stdout` event. The harness waits for `connected`
  with `connections` equal to the number of multiplexers; the other names
  are yours.
- Exit: on `SIGTERM`, cleanly; `Role.stop()` sends it and reports the
  exit code.

[scenarios/label_role/upper_backend.py](scenarios/label_role/upper_backend.py)
is the smallest such program, a `BaseMultiplexerServer` that follows the
contract in forty lines, and `scenarios/label_role` runs it against the
shipped client roles.

## Role options and events

| Role | Options | Events |
|---|---|---|
| all | `--mx host:port` (repeatable), `--type N`, `--name` | `connected {instance_id, connections}` |
| `backend` | `--serves REQ=RESP`, `--behaviour upper\|echo\|drop\|raise\|sleep:MS`, `--crash-after N`, `--memory-every N`, `--drain-seconds S`, `--drain-file PATH` (the harness passes one; `Role.request_drain()` creates it), `--drain-min-handled N`, `--exit-on-exception` | `request {type, id, from_, size}`, `crash`, `draining {drain_seconds}`, `handler_exception {kind, handled}`, `stopped {handled}` |
| `client` | `--query TYPE:payload`, `--count`, `--parallel`, `--timeout`, `--payload-size`, `--sleep-before`, `--sleep-between`, `--threaded`, `--async N`, `--workers N`, `--memory-every N` | `response {round, index, type, from_, payload\|size, ms}`, `error {kind, ms}`, `done` |
| `event_client` | `--send TYPE:payload`, `--to ID`, `--all`, `--no-flush`, `--interval`, `--linger` | `sent {type, id, ...}`, `done` |
| `event_backend` | `--until N`, `--for S` | `received {type, id, from_, to, payload\|size}`, `done` |

Payloads may contain `{worker}`, `{round}` and `{i}`, which the client fills
in. Payloads over 256 bytes are reported by size (and, from the Python roles,
by SHA-256).
