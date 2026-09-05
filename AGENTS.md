# Working on this repository

How the code is laid out, how to build and test it, and the conventions a
change is expected to follow. `docs/README.md` explains what the multiplexer
is; this file is about working on it. `multiplexer/AGENTS.md` and
`tests/AGENTS.md` add what matters inside those directories.

Before opening files, read `docs/code_map.md`: one line per source file,
generated from the file's own header comment, and an index of where each
mechanism lives. `docs/recipes/` has step-by-step pages for the recurring
tasks (a message type, a scenario, an `mxcontrol` subcommand, a default).
`llms.txt` at the root lists every document in reading order.

## What it is

A message broker (`mxcontrol run_multiplexer`) and client libraries in C++
and Python. Peers connect over TCP, register with a type and a random instance
id, and exchange messages routed by type according to a rules file.
`docs/README.md` defines the vocabulary and explains the protocol with
step-by-step diagrams. Use its words: a peer is a client or a backend, and
both talk to multiplexers. Spell out "multiplexer" in prose, "mx" only in code.

## Layout

- `lib/`: the C++ base library, namespace `mx`, macros `MX_*`.
- `multiplexer/`: the broker and the client libraries, C++ and Python side by
  side; `multiplexer/mxlog/` is the Python logging API.
- `mxcontrol/`: the command-line tool and its subcommands.
- `multiplexer/testing/`: the test infrastructure, public and importable by
  other workspaces as `multiplexer.testing`; changes there change every
  consumer's tests, so keep names and signatures.
- `tests/`: integration tests; see `tests/README.md`.
- `examples/`: separate Bazel workspaces consuming this one as `@mx`.
- `bazel/`: dependency macros and patches.
- `docker/`: `check.sh` builds and tests the tree on a clean distribution;
  `docs/building.md` is the page it proves.

## Commands

```
bazel build //...                          # every configuration works without flags
bazel test //...                           # unit and integration tests
bazel test --test_tag_filters=-slow //...  # the fast ones, a few seconds
./examples/test_all.sh                     # the example workspaces
./format.sh                                # black, clang-format-18, buildifier, docs
./format.sh --check                        # what CI runs
bazel build --config=clang //...           # clang thread-safety analysis (lib/thread_annotations.h)
bazel test --config=tsan //lib/...         # ThreadSanitizer; C++ targets only
bazel build --config=asan //...            # AddressSanitizer
./check.sh --leaks                         # LeakSanitizer over the scenarios; soak_memory samples RSS per process
./docker/check.sh [bazel] [debian:12]      # the build and the fast tests on a clean distribution, needs Docker
./tests/bench.sh                           # the throughput and latency numbers the README quotes
bazel run //compdb                         # compile_commands.json for clangd, after a debug build
```

## Conventions

- Think about performance in every change. Nothing new on the per-message
  path; no syscalls there; error handling on the failure path only.
- Network input is never validated with `Assert`. It is a protocol error that
  closes the offending connection.
- A peer is a backend, driven by the library's loop, or a client. A client
  built on the synchronous `Client` is passive and only calls in when it has
  something to send; a `ThreadedClient` runs the loop on its own thread and
  may use an active peer type. Do not build an active client any other way.
- There is no peer authentication and `from` is not checked; the broker runs
  inside a trusted network. Do not add either without discussing it first.
- Warnings are errors for our code (`-Wall -Wextra -Werror`), external code is
  silenced. Do not add `-Wno-*` to make something compile.
- Thread safety is declared, not described. A class owned by one thread has
  an `mx::ThreadChecker` and every method that touches its state starts with
  `MX_DCHECK_RUN_ON(&checker_)`; a member protected by a mutex is declared
  `MX_GUARDED_BY(mu_)` with `mx::Mutex` and `mx::MutexLock`. The runtime
  check is a debug assertion; `bazel build --config=clang` proves the static
  side and must stay clean. The analysis only sees instantiated code, so a
  template method nobody calls is not checked. See `lib/thread_checker.h`.
- Include guards are `MX_<PATH>_H_` derived from the file path; no
  `#pragma once`.
- Names, not numbers: scenarios, documents, decision entries.
- No empty `__init__.py`; Bazel creates them in runfiles.
- Formatting is not optional: run `./format.sh` before finishing. It also
  regenerates the diagram pages, `docs/code_map.md` and the scenario index,
  and rejects Mermaid slips it can spot without a renderer. `./check.sh`
  renders every Mermaid block with mermaid-cli; a `;` inside a
  sequenceDiagram label is the classic mistake, use a comma.
- Every source file opens with a comment: what it is, who uses it, the
  invariants it keeps, the places that surprise a reader. Its first
  paragraph is the file's line in the code map, so keep that paragraph
  true and self-contained. Classes get their role, lifecycle and threading
  assumption; "why" comments go at the tricky places, not on every line.
- Working documents, plans and reviews are not committed; put durable
  knowledge in `docs/` or here.

## Adding things

- A scenario: one file under `tests/scenarios/`, one `mx_integration_test`
  per configuration in its BUILD; see `tests/README.md` for the harness and
  the roles.
- An example: copy `examples/echo/`, keep its `WORKSPACE` and `.bazelrc`,
  replace the rules file and programs, keep a `py_test` that drives it end to
  end.
- A diagram: add a `Page` or a `Section` in `docs/diagrams/generate.py` and
  link the page from `docs/README.md`.
- A recipe: a numbered page under `docs/recipes/`, linked from
  `docs/README.md` and `llms.txt`, for a task that comes up more than once.
- A reference page: hand-written Markdown under `docs/`, linked from the
  "Reference" list in `docs/README.md` and from `llms.txt`. State behaviour the code has, and
  point at the scenario that shows it; the walkthrough's output comes from a
  real run of `examples/echo`.
