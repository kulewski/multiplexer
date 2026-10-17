# Working on this repository

How the code is laid out, how to build and test it, and the conventions a
change is expected to follow. `docs/README.md` explains what the multiplexer
is; this file is about working on it. `multiplexer/AGENTS.md` and
`tests/AGENTS.md` add what matters inside those directories.

Before opening files, read `docs/code_map.md`: one line per source file,
generated from the file's own header comment, and an index of where each
mechanism lives; an example's own README says what its files are
instead. `docs/recipes/` has step-by-step pages for the recurring
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
- `rust/`: the Rust client library, the crate `mx-multiplexer`, in a Cargo
  workspace that Bazel builds too (rules_rust 0.27.0, the crates vendored
  by `rust/vendor.sh`); `rust/README.md` says how.
- `multiplexer/testing/`: the test infrastructure, public and importable by
  other workspaces as `multiplexer.testing`; changes there change every
  consumer's tests, so keep names and signatures.
- `tests/`: integration tests; see `tests/README.md`.
- `examples/`: complete programs, Bazel workspaces consuming this one as
  `@mx`, pip projects on the package, and Cargo workspaces on the crate;
  `examples/README.md` says how to add one.
- `bazel/`: dependency macros, external repositories' BUILD files and
  build rules.
- `docker/`: `check.sh` builds and tests the tree on a clean distribution;
  `docs/building.md` is the page it proves.
- `Makefile` and `make/`: the build without Bazel; `make/sources.mk` is
  generated from the BUILD files by `./format.sh`, so a new source file
  needs no Makefile change.

## Commands

```
bazel build //...                          # every configuration works without flags
bazel test //...                           # unit and integration tests
bazel test --test_tag_filters=-slow //...  # the fast ones, a few seconds
./examples/test_all.sh                     # the example workspaces
./examples/venv.sh                         # examples/.venv, with which check.sh runs the pip examples and their type check
./format.sh                                # black, clang-format, buildifier, rustfmt, docs; --check also parses the YAML
npx pyright                                # the Python package type-checked, after a build (stubs in bazel-bin)
./format.sh --check                        # what CI runs on every push, with the fast tests, also under libc++
bazel build --config=clang //...           # clang thread-safety analysis (lib/thread_annotations.h)
bazel test --config=libcxx --test_tag_filters=-slow //...  # against libc++ instead of libstdc++; needs libc++-18-dev
bazel test --config=tsan //lib/... //multiplexer:several_loops_test //multiplexer:write_through_test  # ThreadSanitizer; C++ targets only
bazel test --config=tsan --test_env=MX_SERVER_THREADS=2 --test_env=GLIBC_TUNABLES=glibc.rtld.optional_static_tls=262144 \
  --build_tests_only --test_tag_filters=lang-cc,-lang-py,-lang-rust,-lang-bin,-slow //tests/scenarios/...  # the scenarios with C++ roles on two loops under ThreadSanitizer; the tunable lets the harness load the instrumented extension
bazel test --test_env=MX_SERVER_THREADS=4 --test_tag_filters=-slow //...  # every multiplexer the tests start on four event loops (run_multiplexer --threads)
bazel build --config=asan //...            # AddressSanitizer
./check.sh --leaks                         # LeakSanitizer over the scenarios; soak_memory samples RSS per process
./check.sh --valgrind                      # memcheck over the C++ tests and the scenarios' multiplexers, and the routing's instruction budget (tests/budgets); needs valgrind
./docker/check.sh [bazel|make] [debian:12] # the build and the fast tests on a clean distribution, needs Docker
./docker/runner_limits.sh                  # the fast tests as a GitHub runner runs them: 4 CPUs, 32 MB TCP buffers
packaging/release_check.sh [COMMIT]        # what a release checks, before the tag; an hour or two, needs Docker
make -j && make check                      # the build without Bazel (Makefile, docs/building.md)
./tests/bench.sh                           # the throughput and latency numbers the README quotes; tests/bench.sh 20000 rust with Rust peers, py with a Python backend
tests/kafka/run.sh                         # docs/kafka.md's comparison with Kafka, every process in Docker; about ten minutes
bazel run //compdb                         # compile_commands.json for clangd, after a debug build
bazel test //rust/...                      # the Rust library; Bazel fetches the Rust toolchain and the crates
bazel test --test_tag_filters=rust,-slow //...  # every Rust target: the library, the roles, the scenarios with Rust roles
(cd rust && cargo clippy --all-targets -- -D warnings)  # cargo from rustup, the release rust-toolchain.toml pins
(cd rust && RUSTDOCFLAGS="-D warnings" cargo doc --no-deps -p mx-multiplexer)  # the crate's docs as docs.rs builds them
rust/vendor.sh                             # after a change to a Cargo.toml's dependencies; --check in check.sh
```

## Conventions

- Think about performance in every change. Nothing new on the per-message
  path; no syscalls there; error handling on the failure path only.
- Network input is never validated with `Assert`. It is a protocol error that
  closes the offending connection.
- A role is what a peer does, not the class it is built on: a client sends
  requests or events, a backend receives them and answers the requests.
  Role words are plain lower case; class names are in code font and in
  full: `SyncClient` (named `Client` before 2.4.0; both names work),
  `ThreadedClient`, `AsyncClient`, and the server classes
  `BaseMultiplexerServer` and `BaseThreadedMultiplexerServer`, which run a
  `serve_forever()` loop that calls `handle_message()`. "Client" in a class
  name is not the role. Never write "client class" or "backend class"; write
  the names, or "the server classes". A peer on `SyncClient` is passive and
  only calls in when it has something to send; an active client is a
  `ThreadedClient` or an `AsyncClient`, never a `SyncClient` on a thread of
  its own. A backend is usually built on a server class: a
  `BaseMultiplexerServer`, one thread, or a `BaseThreadedMultiplexerServer`,
  handlers on workers behind an io thread; `docs/README.md` says which class
  to use when.
- What a client does exists twice: in the C++ library, which the Python
  package binds (`multiplexer/basic_client.cc` and `outbox.cc` with their
  parts, `io/connection.h`, the queries of `client_query.cc` and
  `threaded_client_query.cc`), and in the Rust crate's core,
  `rust/mx-multiplexer/src/core/`, written apart from it. A change to what a
  client does, a send, a stage of a query, a heartbeat, is made in both,
  with their tests, in one change; the scenarios that run in every language
  check that they agree, and `docs/semantics.md` and `docs/query.md` state
  what both do.
- There is no peer authentication and `from` is not checked; the broker runs
  inside a trusted network. Do not add either without discussing it first.
- Warnings are errors for our code (`-Wall -Wextra -Werror`), external code is
  silenced. Do not add `-Wno-*` to make something compile. The same in Rust:
  rustc's warnings are errors in Bazel's builds (`.bazelrc`) and clippy's,
  the vendored crates capped; the crate documents every public item
  (`#![deny(missing_docs)]`), and every `unsafe` block has its SAFETY
  comment (`clippy::undocumented_unsafe_blocks`, in each `Cargo.toml`'s
  `[lints]`). `rust-toolchain.toml` and `rustfmt.toml` sit at the root, so
  that every Cargo workspace and an editor get them.
- Thread safety is declared, not described. A class owned by one thread has
  an `mx::ThreadChecker` and every method that touches its state starts with
  `MX_DCHECK_RUN_ON(&checker_)`; a member protected by a mutex is declared
  `MX_GUARDED_BY(mu_)` with `mx::Mutex` and `mx::MutexLock`. The runtime
  check is a debug assertion; `bazel build --config=clang` proves the static
  side and must stay clean. The analysis only sees instantiated code, so a
  template method nobody calls is not checked. See `lib/thread_checker.h`.
- Include guards are `MX_<PATH>_H_` derived from the file path; no
  `#pragma once`. An external library's header that its BUILD exposes
  without `includes`, RE2's, is included in quotes, `#include "re2/re2.h"`:
  this repository's `.bazelrc` puts external repositories on the
  `-isystem` path (`external_include_paths`), a workspace that builds
  `@mx` may not, and there angle brackets do not find it.
- Names, not numbers: scenarios, documents, decision entries.
- No empty `__init__.py`; Bazel creates them in runfiles.
- Formatting is not optional: run `./format.sh` before finishing. It also
  regenerates the diagram pages, `docs/code_map.md` and the scenario index,
  and rejects Mermaid slips it can spot without a renderer. `./check.sh`
  renders every Mermaid block with mermaid-cli; a `;` inside a
  sequenceDiagram label is the classic mistake, use a comma.
  VS Code set up as docs/building.md's "Editing in VS Code" says formats
  and checks as these do.
- Every source file opens with a comment: what it is, who uses it, the
  invariants it keeps, the places that surprise a reader. Its first
  paragraph is the file's line in the code map, so keep that paragraph
  true and self-contained. Classes get their role, lifecycle and threading
  assumption; "why" comments go at the tricky places, not on every line.
- Working documents, plans and reviews are not committed; put durable
  knowledge in `docs/` or here.

## Adding things

- A scenario: one folder under `tests/scenarios/`, holding the test and its
  README, one `mx_integration_test` per configuration in its BUILD
  (`docs/recipes/add_a_scenario.md`); see `tests/README.md` for the harness
  and the roles.
- An example: as `examples/README.md` says under "Adding an example": a
  Bazel one copies `examples/echo/`, a pip one `examples/cache/`, a Cargo
  one `examples/echo-rust/`; each has
  a README and a walkthrough, whose steps' output comes from a real run.
- A diagram: add a `Page` or a `Section` in `docs/diagrams/pages.py`, which
  `generate.py` writes out, and link the page from `docs/README.md`.
- A recipe: a page named for its task under `docs/recipes/`, linked from
  `docs/README.md` and `llms.txt`, for a task that comes up more than once.
- A reference page: hand-written Markdown under `docs/`, linked from the
  "Reference" list in `docs/README.md` and from `llms.txt`. State behaviour
  the code has, and point at the scenario that shows it.
- A library compiled into a published binary: its license text into
  `THIRD_PARTY_NOTICES`, with the binaries that hold it, and its line in
  docs/packaging.md's "Licenses". The wheels, the release binary and the
  image carry that file, and their build scripts check it.
