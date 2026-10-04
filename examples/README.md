# Examples

Each Bazel example here is a **separate Bazel workspace** that consumes the
multiplexer exactly the way any other project would: as the external
repository `@mx`. Being inside this repository they use
`local_repository(path = "../..")`; your workspace would use
`git_repository` or `http_archive`, as the commented block in
`echo/WORKSPACE` shows. That keeps example-only dependencies out of the
multiplexer's own workspace and makes every example a test of the real
consumption path.

The root `.bazelignore` keeps these directories out of the root `//...`, so
they are built from inside:

```
cd examples/echo
bazel test //...
```

`./test_all.sh` runs every example's tests in turn; use it in CI.

A pip example is installed the way a project on the released package is: a
`requirements.txt` naming `mx-multiplexer`, which brings the multiplexer,
`mxcontrol`, with it, and a `test.sh` that makes a virtual environment and
runs the example's test on that `mxcontrol`, unless `MXCONTROL` names
another. `./test_all.sh` runs those with `EXAMPLES_PIP=1`, as the examples
workflow does against the tree's wheel, mxcontrol inside, and `check.sh` at
the root runs them against the tree itself, its library and its
multiplexer, when `examples/.venv` exists, which `venv.sh` makes.

## Consuming the multiplexer from your own workspace

```
load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")

git_repository(
    name = "mx",
    commit = "<commit sha>",
    remote = "https://github.com/kulewski/multiplexer.git",
)

load("@mx//bazel:deps.bzl", "mx_dependencies")
mx_dependencies()

load("@mx//bazel:setup.bzl", "mx_setup")
mx_setup()
```

Then depend on `@mx//multiplexer:clients` and `@mx//multiplexer:multiplexer_constants`
from Python, or `@mx//multiplexer:client`, `@mx//multiplexer/backend:base_multiplexer_server`
and `@mx//multiplexer:multiplexer_cc_constants` from C++. Import paths and
include paths are the same as inside the multiplexer's own workspace:
`from multiplexer.clients import SyncClient`, `#include "multiplexer/client.h"`.

Your peer and message types live in your own rules file; point the build at
it in your `.bazelrc`:

```
build --@mx//:multiplexer_rules=//:my.rules
```

The multiplexer needs `protoc` and `libprotobuf` from the system, and C++17.

## The examples

- [echo](echo/): a backend and a client in both languages, the threaded
  client shared by worker threads, and the test infrastructure used from
  another workspace.
- [aio](aio/): an asyncio TCP gateway with `AsyncClient` in front of a chat
  backend, replies to the client that asked and broadcasts to every client.
- [cache](cache/): a replicated cache as a Django cache backend, the
  smallest example of both routing modes: a write goes to every replica, a
  read to any one. Its walkthrough builds it line by line and walks through
  its test, the harness's showcase. A pip example.
- [channels](channels/): a Django Channels channel layer on the multiplexer,
  replacing Redis with one setting, and the Channels tutorial's chat on it:
  a group send is an event to every process, a send to a channel is
  addressed to the process that owns it. Its walkthrough builds the layer
  line by line. A pip example too.
- [inference](inference/): a Django web app asking a pool of PyTorch model
  workers, with the walkthrough notebook that builds it up and shows a
  worker die, a rolling restart and a multiplexer die. A pip example too.
- [audio](audio/): an audio room, the first example of what the broker's
  latency makes possible rather than a building block: the browser's
  microphone as 10 ms frames over a WebSocket to a Django Channels gateway,
  each frame one query to a C++ worker that applies an effect and measures
  the spectrum, and the result an event to everyone in the room. A stream
  sticks to one worker and moves when it dies or is restarted. The worker is
  a Bazel workspace and the gateway a pip example, so `./test_all.sh` runs
  the worker's tests always and the whole example with `EXAMPLES_PIP=1`.

## Adding an example

A Bazel example: copy `echo/`, keep its `WORKSPACE` and `.bazelrc`, replace
the rules file and the programs, and add a `py_test` that starts
`@mx//mxcontrol` and exercises the example end to end, so `test_all.sh`
covers it. A pip example: copy `cache/`, with a `requirements.txt`, a
`test.py` on the harness and a `test.sh` that makes a venv and runs it.
Either way, list the example's paths in `pyrightconfig.json`, which types
every example against the tree. A walkthrough that builds the example up,
`walkthrough.md`, shows the files in fenced blocks marked `file=`, and
`check_walkthroughs.py`, which `format.sh --check` runs, keeps those blocks
identical to the files.
