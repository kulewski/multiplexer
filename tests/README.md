# Integration tests

`bazel test //...` runs everything. `--test_tag_filters=-slow` skips the
scenarios that wait out heartbeat and reconnect intervals; `lang-py` and
`lang-cc` select by the language of the role binaries.

## Layout

- `testing.rules`: the system rules plus the tests' types, those of the
  unit tests under `multiplexer/` and a test section (peers and types from
  201 up). The repository's builds generate the package's constants from it
  (`.bazelrc`, and `make check`'s build of its own), and `tests/BUILD`
  generates `testing_constants.py` from it, so the tests never depend on the
  rules file a deployment builds with.
- The harness is [multiplexer/testing](../multiplexer/testing), a public
  package any workspace that depends on `@mx` can import: `Cluster(n)`
  starts n multiplexers on ephemeral ports through `--address 127.0.0.1:0
  --port-file`, `spawn(role, lang, ...)` launches a role and collects its
  Event events, `Role.wait_for(event, **fields)` waits for one, the
  first the role ever printed, so a second wait for the same returns at
  once, `Role.wait_for_count(event, n)` and `wait_for_total(roles, event, n)`
  return the events once n have arrived (a count of another process's
  events, taken right after the client finished, may otherwise miss lines
  still in the pipe), `Cluster.wait_for_peer(type)` and
  `wait_for_peer_gone(type)` wait on the multiplexers' peers files,
  `wait_until(predicate, timeout, what)` on anything else. Every process's
  stderr and events land in the test's undeclared outputs directory.
  Leaving the `Cluster` stops every role still running and then the
  multiplexers, and fails the test with the end of the log of every
  process that did not end cleanly: a role that `SIGTERM` did not end
  within 10 s with 0, or by the signal for one that does not catch it,
  and a multiplexer that exited on its own, exited other than 0 at a stop
  or had to be killed when a stop ran out of time: 10 s, or
  `drain_seconds` + 5 s for a cluster that drains longer. A process that
  `pause()` froze is continued after its `SIGTERM`, so that it handles it.
  A scenario that ends a
  multiplexer itself says so: `Mx.kill()`, or `Mx.expect_exit()` before
  a signal of its own.
  `harness/` here re-exports it with the constants of `testing.rules`.
  `FakePeer`, `BackendThread`, `TestClient` and `ThreadedTestClient` are the in-process peers for
  unit tests, described in [docs/api_python.md](../docs/api_python.md#testing).
- `roles/py/` and `roles/cc/`: the same four roles in both languages, with the
  same options and the same Event events, so a scenario can be run with any
  mix. `backend` answers requests and `event_backend` only receives events;
  `client` sends requests and waits for answers, `event_client` sends events
  and does not. The shipped client roles are built on `SyncClient` unless
  given `--threaded` or `--workers`, so their usual peer types are passive.
- `multiplexer/testing/raw_peer.py`: a peer that speaks the wire format
  directly, for protocol and robustness scenarios.
- `Cluster(record=True)` makes every multiplexer write a recording of what
  it routed (`Mx.record_file`, read with `multiplexer.recording`), the
  `recording` scenarios check it against what the roles reported.
  `Cluster(remote_recording=True)` lets peers start sessions and tap in
  (one shared `recording_dir`, `recording_files()`), and `mxcontrol(*args)`
  runs the tool to completion; the `remote_recording*` and `recording_tap`
  scenarios use both.
- `fake_dns/`: `mxcontrol_fake_dns --hosts FILE ...`, mxcontrol's
  `recording` command with the names under `.test` looked up in FILE,
  which a scenario rewrites to move a name to another address; the binary
  defines `getaddrinfo()` itself, so this needs no root and no change to
  the host. Each such lookup is a line in FILE.lookups: a command that
  looks its names up at every poll counts its polls there, which bounds a
  wait by polls rather than by time. Multiplexers on `127.0.0.2`,
  `127.0.0.3` and so on give a name several addresses on one machine. The
  `remote_recording_new_address` scenario uses both.
- A drain in a unit test: `BackendThread(factory, drain_seconds=...)` is the
  cap of a drain the test starts with the backend's `start_draining()`.
- The rules file under running multiplexers: a scenario gives `Cluster` a
  copy of the file it may edit, `rules=path`, and
  `rules_check_interval=0.1` to have it read again that often (0 never);
  `Mx.reload_rules()` sends SIGHUP, `Mx.log_contains(text)` with
  `wait_until` waits for the log line that says what happened. The
  `rules_edited_on_disk`, `rules_reload_on_sighup` and
  `rules_reload_by_mxcontrol` scenarios cover the three triggers.
- Memory: roles emit `memory` events every `--memory-every` messages with
  the exact C heap in use (and, in Python, tracemalloc bytes and the object
  count); `Cluster(memory_log_every=N)` makes the multiplexer log its heap
  every N routed messages, read back with `Mx.memory_samples()`. The
  `soak_memory` scenario checks all three plateau; `multiplexer/soak_test.cc`
  does the same in-process for the C++ core, and `multiplexer/leak_test.py`
  watches the Python side and the binding with tracemalloc by source line,
  the object count after a collection, reference counts and weak references. Under `check.sh --leaks` the
  harness turns LeakSanitizer on for the C++ processes, so an allocation
  still held at exit fails the scenario through the exit code: leaving
  the `Cluster` checks the multiplexers' and those of the roles still
  running, the scenario those of the roles it waited for.
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
directly with its own constants. A `Cluster` in a plain test of yours,
outside the macro, names its rules file, `Cluster(1,
rules=runfile("your.rules"))`, with the file in the test's `data`; there
is no default, so a test always says which rules it runs with.

## Playing a role with your own binary

A value in `roles` may be the label of a binary instead of `"py"` or
`"cc"`: it is added to the test's data and the scenario sees `"bin"` for
that role, so `spawn(role, cfg.lang(role), ...)` runs it like a shipped
role. The contract is the command line and the events:

- Options: `--mx host:port` for every multiplexer (repeated), `--type N`
  for the peer type, `--name` for a label the events may carry, the
  role's name (`spawn(..., name=...)`, or one the harness makes); then
  whatever `spawn(..., option=value)` adds, one `--option value` each (see
  `_argv` in `multiplexer/testing/__init__.py`). `--drain-file PATH` is
  passed only with `spawn(..., drain_file=True)`.
- Events: one `Event` (`multiplexer/events.proto`) per line of
  stdout in protocol buffer text format, `event: "connected" instance_id: 7
  connections: 1`, and any fields of the message. A line that is not an
  Event is kept as a `stdout` event; one that starts as an Event and does
  not parse, a string field with bytes that are not UTF-8 say, fails the
  next wait on the role. A `match` may name any field, `name` too:
  `wait_for("connected", name="backend-1")`. The harness waits for `connected`
  with `connections` equal to the number of multiplexers; the other names
  are yours.
- Exit: on `SIGTERM`, with 0 within 10 s, or by the signal if the binary
  does not catch it; `Role.stop()` sends it and reports the exit code,
  and leaving the `Cluster` fails the test on any other end of a role
  still running then.

[scenarios/label_role/upper_backend.py](scenarios/label_role/upper_backend.py)
is the smallest such program, a `BaseMultiplexerServer` that follows the
contract in forty lines, and `scenarios/label_role` runs it against the
shipped client roles.

## Role options and events

| Role | Options | Events |
|---|---|---|
| all | `--mx host:port` (repeatable), `--type N`, `--name` | `connected {instance_id, connections}` |
| `backend` | `--serves REQ=RESP`, `--behaviour upper\|echo\|drop\|raise\|sleep:MS`, `--crash-after N`, `--memory-every N`, `--drain-seconds S`, `--drain-file PATH` (the harness passes one; `Role.request_drain()` creates it), `--drain-min-handled N`, `--drain-routing FLAGS` (the `Routing` flags kept on while draining, a comma-separated subset of `any`, `all`, `last_resort`; none by default), `--threaded` (the same on `BaseThreadedMultiplexerServer`, the handler on a worker thread), `--exit-on-exception` | `request {type, id, sender, size}`, `crash`, `draining {drain_seconds}`, `acked {ms}` (every multiplexer has the drain routing in effect), `handler_exception {kind, handled}`, `stopped {handled}` |
| `client` | `--query TYPE:payload`, `--count`, `--parallel`, `--timeout`, `--payload-size`, `--sleep-before`, `--sleep-between`, `--threaded`, `--async N`, `--workers N`, `--memory-every N` | `response {round, index, type, sender, payload\|size, ms}`, `error {kind, ms}`, `done` |
| `event_client` | `--send TYPE:payload`, `--to ID`, `--all`, `--no-flush`, `--interval`, `--linger` | `sent {type, id, ...}`, `done` |
| `event_backend` | `--until N`, `--for S`, `--drain-file PATH`, `--drain-routing FLAGS` (as for `backend`; the drain starts when the file appears and the loop goes on until `--until`, `--for` or SIGTERM) | `received {type, id, sender, to, payload\|size}`, `draining`, `acked`, `done` |

Payloads may contain `{worker}`, `{round}` and `{i}`, which the client fills
in. Payloads over 256 bytes are reported by size (and, from the Python roles,
by SHA-256).
