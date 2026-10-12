# Recipe: test a service binary

A service you deploy is best tested the way it runs. That means its real
binary, started with production's flags, against real multiplexers. It
is checked only through what it does over them, its exit code and its
log. `multiplexer.testing` does this from Python, whatever language the
service is written in: C++, Rust, Python or anything else.
- `Cluster` starts the multiplexers.
- `Process` runs the binary.
- `FakePeer` stands in for the peers the service talks to.
- `TestClient` asks it.

Services in every language share one set of fakes and helpers this way.
[examples/echo/echo_test.py](../../examples/echo/echo_test.py) tests a
C++ and a Python backend so.

## The test

```python
import unittest

from multiplexer.multiplexer_constants import peers, types  # your rules file's
from multiplexer.testing import Cluster, FakePeer, Process, TestClient, runfile

RULES = runfile("deployment.rules")  # the file the constants were generated from
SERVICE = runfile("search_service")  # the binary under test


class SearchServiceTest(unittest.TestCase):
    def start(self, cluster: Cluster) -> Process:
        """The service with production's flags, once it has registered."""
        service = Process([SERVICE, "--mx", ",".join(cluster.addresses)], name="search")
        cluster.wait_for_peer(peers.SEARCH)
        return service

    def test_a_search_asks_the_index(self):
        with (
            Cluster(2, rules=RULES) as cluster,
            FakePeer(cluster, peers.INDEX) as index,
            TestClient(cluster, peers.WEB) as client,
        ):
            index.reply_with(types.INDEX_REQUEST, b"3 hits", types.INDEX_RESPONSE)
            self.start(cluster)
            self.assertEqual(b"3 hits", client.query(b"pears", types.SEARCH_REQUEST).message)
            self.assertEqual(b"pears", index.wait_for(types.INDEX_REQUEST)[0].message)

    def test_it_drains_on_sigterm_and_exits_0(self):
        with (
            Cluster(1, rules=RULES) as cluster,
            FakePeer(cluster, peers.INDEX) as index,
            TestClient(cluster, peers.WEB) as client,
        ):
            index.reply_with(types.INDEX_REQUEST, b"3 hits", types.INDEX_RESPONSE)
            service = self.start(cluster)
            service.proc.terminate()  # what a deployment's stop sends
            # The last of its type, it still answers through its drain.
            self.assertEqual(b"3 hits", client.query(b"pears", types.SEARCH_REQUEST).message)
            self.assertEqual(0, service.wait(30))
```

- **`Process(argv, name=None, env=None, cwd=None)`** starts the binary
  with the harness's environment, `env` over it.
  - Its stdout and stderr, together, go to `lines` and to `<name>.log`
    under the test's outputs. `wait_for_line(pattern)` waits for a line
    that matches, a `ready` say, without polling.
  - `stop()` sends SIGTERM and returns the exit code; `kill()` is a
    crash, and `pause()` and `resume()` freeze and thaw it.
- **`cluster.wait_for_peer(type)`** waits until the service has
  registered on every multiplexer, so nothing is asked of it too early.
  `wait_for_peer_gone(type)` is its other side.
- **`cluster.mx[0].restart()`** restarts a multiplexer under the
  service, which must reconnect: wait for its peer again.
  `cluster.mx[0].pause()` freezes one instead, without closing a socket.
- **`FakePeer`** answers from a script, `reply_with()` or `on()`, and
  keeps what arrived: `messages()` and `wait_for()`. **`TestClient`**
  sends and queries as a peer of the type given. Both go in the
  cluster's `with`, so they stop before it.

## What leaving the cluster checks

Leaving the `with` stops every `Process` still running, as a deployment
stops it, with SIGTERM, then the multiplexers. It fails the test with
`AssertionError` unless each one ended cleanly. For your program, that
means exiting within 10 s with 0, or by the signal itself if it does
not catch SIGTERM. A program that had exited before is the test's to
check, with `returncode` or `wait()`. The report carries the end of each
log.

A program that reads `MX_TEST_PARENT_FD`, a pipe in its environment, as
`mxcontrol` does, ends itself however the test process ends. One that
does not is ended when the test process exits, and under Bazel by the
sandbox if the test is killed.

## Under Bazel

The binary, a `cc_binary` or a `rust_binary`, is data of a `py_test`,
and the constants come from the rules file `.bazelrc` names
(`build --@mx//:multiplexer_rules=//:deployment.rules`), as in
[examples/echo](../../examples/echo):

```python
py_test(
    name = "search_service_test",
    srcs = ["search_service_test.py"],
    data = [
        ":deployment.rules",
        ":search_service",
        "@mx//mxcontrol",
    ],
    deps = [
        "@mx//multiplexer:multiplexer_constants",
        "@mx//multiplexer/testing",
    ],
)
```

`runfile()` finds both files in the test's runfiles. The logs land in
the test's outputs, `bazel-testlogs/.../test.outputs`.

## Without Bazel

`pip install mx-multiplexer` brings `multiplexer.testing` and the
`mxcontrol` that `Cluster` runs. Build the binary as you do, then run the
test with Python:

```
python3 -m venv .venv && .venv/bin/pip install mx-multiplexer
.venv/bin/mxcontrol generate_constants deployment.rules --python multiplexer_constants.py
cargo build --release      # or make, or cmake: the binary under test
.venv/bin/python -m unittest search_service_test
```

The test then imports the constants from `multiplexer_constants`. Outside
Bazel `runfile()` takes a path as given, relative to the working
directory: `runfile("target/release/search_service")`. The logs go to a
temporary directory, removed at exit; with `$MX_TEST_OUTPUT` set they go
to a directory of the test's own in it, and stay.

## Tests that hold up

Count what arrived instead of timing it: wait for the line, the peer or
the message, and never sleep a guess
([tests that hold up under load](../api_python.md#tests-that-hold-up-under-load)).
A program's own unit tests, of code that does not touch the multiplexer,
stay where they are: gtest, `cargo test` or `unittest`.
