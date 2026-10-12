# Rust

The Rust client library of the multiplexer, the crate `mx-multiplexer`,
and the tools around it. The crate is a Cargo workspace member and a Bazel
package at once: Cargo builds it for crates.io and for work on the crate
alone, Bazel builds the same sources with rules_rust 0.27.0, the last
release for this repository's Bazel 6.2.1.

| Path | What |
|---|---|
| [Cargo.toml](Cargo.toml) | the Cargo workspace |
| [mx-multiplexer/](mx-multiplexer/) | the crate |
| [codegen/](codegen/) | the generator of the crate's protocol code, a workspace of its own |
| [generate_defaults.py](generate_defaults.py) | writes the crate's `defaults.rs` from `multiplexer/defaults.h` |
| [vendor.sh](vendor.sh) | vendors the crates the workspace depends on, for Bazel |
| [3rdparty/crates/](3rdparty/crates/) | what `vendor.sh` writes: a BUILD file per crate and `crates.bzl` |
| [../tests/roles/rust/](../tests/roles/rust/) | the test roles in Rust, a workspace of their own |
| [../rust-toolchain.toml](../rust-toolchain.toml) | the Rust release cargo uses anywhere in the repository, the one Bazel registers |
| [../rustfmt.toml](../rustfmt.toml) | rustfmt's settings, for every Cargo workspace here |

## Building and testing

Bazel fetches its own Rust toolchain and the crates the first time a Rust
target is built; nothing else needs to be installed:

```
bazel test //rust/...
bazel test --test_tag_filters=rust,-slow //...   # and the scenarios with Rust roles
```

The Rust targets are tagged `rust`, the scenarios that run the Rust test
roles too, so a build can leave them out with
`--build_tag_filters=-rust --test_tag_filters=-rust`, as CI's C++ jobs do.
The crate's tests against real multiplexers, `mx-multiplexer/tests/`, run
`mxcontrol run_multiplexer` with `tests/testing.rules`: under Bazel the
`mxcontrol` it builds, under cargo the one `$MXCONTROL` names, or
`bazel-bin/mxcontrol/mxcontrol` after a `bazel build //mxcontrol`.

Cargo needs rustup (https://rustup.rs), which installs the release
`rust-toolchain.toml` at the repository's root pins on first use, wherever
cargo runs in the repository. In this directory:

```
cargo test
cargo clippy --all-targets -- -D warnings
cargo clippy --manifest-path ../tests/roles/rust/Cargo.toml --all-targets -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps -p mx-multiplexer
```

Warnings are errors, as the C++ compilers' are: rustc's in Bazel's builds,
which `.bazelrc` gives `-Dwarnings`, the vendored crates built with
`--cap-lints=allow`, and clippy's in the commands above. An `unsafe` block
without its SAFETY comment is an error of clippy's in any `cargo clippy`,
each `Cargo.toml`'s `[lints]` saying so. The crate documents every public
item, `#![deny(missing_docs)]`, and its documentation, what docs.rs shows,
builds without a warning, a broken link included; check.sh and CI run all
three.

The test roles are a Cargo workspace of their own, since the vendoring's
cargo-bazel takes no workspace member outside this directory; Bazel builds
them with the crates vendored here, the roles needing none the library
does not.

## Editing in VS Code

The rust-analyzer extension (`rust-lang.rust-analyzer`) reads what the
command line reads: `rust-toolchain.toml` and `rustfmt.toml` at the
repository's root and each `Cargo.toml`'s `[lints]`. So its formatting is
`./format.sh`'s and its diagnostics are clippy's in `./check.sh`.
[docs/building.md](../docs/building.md#editing-in-vs-code) has its
settings with every other language's. Among them, `linkedProjects` names
the Cargo workspaces it does not find by itself, and a new Cargo example
joins that list.

## Generated files

Nothing here is generated at build time; every generated file is
committed, and `./format.sh --check` says when one is stale:

- `mx-multiplexer/src/proto/`: the protocol's messages, written by
  `cargo run --manifest-path codegen/Cargo.toml` (prost-build on the
  system's protoc). Their headers name the CRC-32 of each `.proto` file,
  which `//rust/mx-multiplexer:generated_test` checks without a Rust
  toolchain.
- `mx-multiplexer/src/defaults.rs`: written by `python3
  rust/generate_defaults.py` from `multiplexer/defaults.h`.
- `mx-multiplexer/src/constants.rs`: the system rules' types, written by
  `mxcontrol generate_constants multiplexer.rules --rust
  rust/mx-multiplexer/src/constants.rs` from the repository's root;
  `//rust/mx-multiplexer:generated_test` checks it.
- `mx-multiplexer/tests/support/testing_constants.rs`: the types of the
  tests' rules, written the same way from `tests/testing.rules`, and
  checked by the same test.
- `mx-multiplexer/testdata/golden_frames.txt`: frames the Python package's
  protobuf writes, which the crate's frames must equal byte for byte;
  `//rust/mx-multiplexer:golden_frames_test` checks it.

## Dependencies

The crate depends on five crates: mio and prost stand where the C++
library has asio and protobuf, libc and log are the Rust project's own, and
bytes is what prost decodes payloads into. Everything else is written in
the crate, as `lib/` does for C++.

After a change to a `Cargo.toml`'s dependencies, run `rust/vendor.sh`: it
resolves `Cargo.lock` again and writes `3rdparty/crates`, with cargo 1.77.2,
which it installs with rustup. The cargo-bazel of rules_rust 0.27.0 reads
lockfiles up to version 3 and the registry layout of cargo before 1.85, so
`Cargo.lock` stays at version 3, which every newer cargo reads and keeps,
and the manifests at edition 2021. `./check.sh` and CI run
`rust/vendor.sh --check`.
