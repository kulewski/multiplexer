# Examples

Each directory here is a **separate Bazel workspace** that consumes the
multiplexer exactly the way any other project would: as the external
repository `@mx`. Being inside this repository they use
`local_repository(path = "../..")`; your workspace would use `git_repository`
or `http_archive`, as the commented block in `echo/WORKSPACE` shows. That keeps
example-only dependencies out of the multiplexer's own workspace and makes
every example a test of the real consumption path.

The root `.bazelignore` keeps these directories out of the root `//...`, so
they are built from inside:

```
cd examples/echo
bazel test //...
```

`./test_all.sh` runs every example's tests in turn; use it in CI.

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
`from multiplexer.clients import Client`, `#include "multiplexer/client.h"`.

Your peer and message types live in your own rules file; point the build at
it in your `.bazelrc`:

```
build --@mx//:multiplexer_rules=//:my.rules
```

The multiplexer needs `protoc` and `libprotobuf` from the system, and C++17.

## Adding an example

Copy `echo/`, keep its `WORKSPACE` and `.bazelrc`, replace the rules file and
the programs, and add a `py_test` that starts `@mx//mxcontrol` and exercises
the example end to end, so `test_all.sh` covers it.
