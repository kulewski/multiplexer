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
- [inference](inference/): a Django web app asking a pool of PyTorch model
  workers, with the walkthrough notebook that builds it up and shows a
  worker die, a rolling restart and a multiplexer die. A pip example.

## Adding an example

Copy `echo/`, keep its `WORKSPACE` and `.bazelrc`, replace the rules file and
the programs, and add a `py_test` that starts `@mx//mxcontrol` and exercises
the example end to end, so `test_all.sh` covers it.
