# Examples

Complete programs on the multiplexer, each in a directory of its own
with a README that runs it and a walkthrough that builds it from
nothing, echo's apart, which runs it step by step. They are of two
kinds. The building blocks are what a service needs and would otherwise
get from Redis, Kafka, an HTTP pool or a task queue: a replicated cache,
a channel layer, a pool of model workers, workers that each hold some of
the models, a clickstream batched by shard, orders that a worker's death
cannot lose, an answer streamed token by token. The others show what a
broker with this latency makes possible, starting with an audio room
where every 10 ms frame is a request. Four small ones come first: the
shape of a backend and a client in both languages, the same in Rust, an
asyncio gateway, and a health check that sees a backend's serve loop.

| example | what it shows | languages | built with | walkthrough |
|---|---|---|---|---|
| [echo](echo/) | the smallest backend and client, in Python and in C++, and threads sharing one client | Python, C++ | Bazel | [walkthrough.md](echo/walkthrough.md) runs it |
| [echo-rust](echo-rust/) | the same backend and client in Rust: a `BaseMultiplexerServer` that drains on `SIGTERM`, a `SyncClient` that asks | Rust | Cargo | [walkthrough.md](echo-rust/walkthrough.md) |
| [aio](aio/) | an asyncio TCP gateway in front of a chat backend: a reply to the one who asked, a broadcast to everyone | Python | Bazel | [walkthrough.md](aio/walkthrough.md) |
| [health](health/) | a backend's health check that sees its serve loop: a Kubernetes liveness probe on an endpoint that answers 503 once `periodic_task()` has not run for a while, beside the drain on a preStop hook | Python | pip | [walkthrough.md](health/walkthrough.md) |
| [cache](cache/) | a replicated cache as a Django cache backend: a write to every replica, a read from any; and a journal added by changing the rules under the running multiplexers | Python | pip | [walkthrough.md](cache/walkthrough.md) |
| [channels](channels/) | a Django Channels channel layer on the multiplexer, Redis replaced by one setting | Python | pip | [walkthrough.md](channels/walkthrough.md) |
| [inference](inference/) | a web app asking a pool of PyTorch workers, with a worker killed, a rolling restart and a multiplexer killed | Python | pip | [walkthrough.ipynb](inference/walkthrough.ipynb) |
| [models](models/) | workers that each hold some of the models: a request's key names its model, a worker's key filter the models it holds, and a model is loaded, and unloaded while its requests keep coming | Python | pip | [walkthrough.md](models/walkthrough.md) |
| [clicks](clicks/) | a clickstream: a click an event keyed with its shard, each collector's key filter taking some shards, and the sources batching the events, a batch one shard's clicks routed whole to its collector; clicks a second and the multiplexers' CPU with batching and without | Python | pip | [walkthrough.md](clicks/walkthrough.md) |
| [orders](orders/) | orders that a worker's death cannot lose: each a confirmed send, kept by the shop until a worker recorded it in a ledger, sent again after the worker that held it died, recorded once though it came twice, and given up on at its deadline with no worker; a confirmation's latency in batches and one by one | Python | pip | [walkthrough.md](orders/walkthrough.md) |
| [stream](stream/) | an answer that arrives token by token, as a language model's does: one request, its pieces addressed and numbered, one reply; a multiplexer killed under it and the lost pieces asked for again; the answer as Server-Sent Events | Python | pip | [walkthrough.md](stream/walkthrough.md) |
| [audio](audio/) | an audio room: a query per 10 ms frame to a C++ worker, the answer heard by everyone in the room | Python, C++ | Bazel and pip | [walkthrough.md](audio/walkthrough.md) |

Read them in that order. Every example is two pages of one shape. The
README is the front door: what the example is, how to run it, a picture
of its peers, and what each file is. The walkthrough is the long page:
it builds the example from nothing, or for echo runs it, draws how its
messages go, then runs the measured steps where there are any, walks
through the test on the library's harness, and says what the example
does not do; inference's is a notebook.

## Running them

The Bazel examples, echo, aio and the audio worker, are separate Bazel
workspaces that consume the multiplexer as the external repository
`@mx`, exactly the way your own project would. Being inside this
repository they use `local_repository(path = "../..")`; yours would use
`git_repository` or `http_archive`, as the commented block in
`echo/WORKSPACE` shows. The root `.bazelignore` keeps these directories
out of the root `//...`, so they are built from inside:

```
cd examples/echo
bazel test //...
```

The pip examples, health, cache, channels, inference, models, clicks, orders, stream and the audio gateway, are
installed the way a Django project is: `mx-multiplexer` from PyPI, which
brings the multiplexer, `mxcontrol`, with it, with a `requirements.txt`, a
`test.py` on the harness and a `test.sh` that makes a virtual environment
and runs it on that `mxcontrol`, unless `MXCONTROL` names another. Each
README's first block is the commands.

A Cargo example is a Cargo workspace of its own that takes the crate
`mx-multiplexer` from this repository by path, as yours would from
crates.io or git ([docs/api_rust.md](../docs/api_rust.md)); its `test.sh`
runs `cargo test` against the `mxcontrol` that `MXCONTROL` names, or this
repository's, built with Bazel.

`./test_all.sh` runs every example: the Bazel tests always, the pip
examples with `EXAMPLES_PIP=1`, as the examples workflow sets against the
tree's wheel, mxcontrol inside, and the Cargo examples with
`EXAMPLES_RUST=1`, which `check.sh` sets; CI's Rust job runs their
`test.sh` too. `check.sh` at the root runs them against
the tree itself, its library and its multiplexer, when `examples/.venv`
exists, which `venv.sh` makes from every example's
requirements, and types them all with pyright through
`pyrightconfig.json`, against Python 3.10, the oldest the library
supports. The tests run on whichever Python made the venv, 3.12 in the
workflow; none runs them on 3.10.

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

Then depend, from Python, on `@mx//multiplexer:multiplexer_constants` and
on what the peer is built on: `@mx//multiplexer:clients` (`SyncClient`),
`@mx//multiplexer:servers` (`BaseMultiplexerServer`),
`@mx//multiplexer:threaded_client_py`, `@mx//multiplexer:threaded_server`
or `@mx//multiplexer:aio`; from C++, on
`@mx//multiplexer:multiplexer_cc_constants` and `@mx//multiplexer:client`,
`@mx//multiplexer:threaded_client`,
`@mx//multiplexer/backend:base_multiplexer_server` or
`@mx//multiplexer/backend:base_threaded_multiplexer_server`. Import paths and
include paths are the same as inside the multiplexer's own workspace:
`from multiplexer.clients import SyncClient`, `#include "multiplexer/client.h"`.

Your peer and message types live in your own rules file; point the build at
it in your `.bazelrc`:

```
build --@mx//:multiplexer_rules=//:my.rules
```

The multiplexer needs `protoc` and `libprotobuf` from the system, and C++17.

## Adding an example

A Bazel example: copy `echo/`, keep its `WORKSPACE` and `.bazelrc`, replace
the rules file and the programs, and add a `py_test` that starts
`@mx//mxcontrol` and exercises the example end to end, so `test_all.sh`
covers it. A pip example: copy `cache/`, with a `requirements.txt`, a
`test.py` on the harness and a `test.sh` that makes a venv and runs it.
An example can be both, as `audio/` is, with a `WORKSPACE` for its C++
side and a `test.sh` for its Python side. Either way, list the example's
paths in `pyrightconfig.json`, which types every example against the
tree. A Cargo example: copy `echo-rust/`, a Cargo workspace of its own
with its tests and a `test.sh` that runs them, so `test_all.sh` covers
it. Every example gets the two pages in the shape above. The walkthrough
shows the files in fenced blocks marked `file=`, a rules file from its
own part on with `from=`, and `check_walkthroughs.py`, which `format.sh
--check` runs, keeps those blocks identical to the files. Every Mermaid
diagram, in a README or a walkthrough, is rendered by
`docs/check_mermaid.py`, which `check.sh` runs; a notebook, which GitHub
shows without Mermaid, links SVG pictures instead, each rendered from
the `.mmd` source beside it, which the same script checks and `--write`
renders again.
