#!/usr/bin/env bash
# Vendors the Rust workspace's crates for Bazel: resolves rust/Cargo.lock
# again and writes rust/3rdparty/crates, the BUILD file of every crate the
# workspace depends on and crates.bzl, whose crate_repositories() the
# WORKSPACE calls. Run it after a change to a Cargo.toml's dependencies;
# check.sh runs it with --check.
#
# It runs what `bazel run //rust:crates_vendor` would, rules_rust 0.27.0's
# cargo-bazel, but with cargo 1.77.2 from rustup and a CARGO_HOME of its
# own in .tools/: that cargo-bazel cannot read the version 4 lockfile a
# cargo newer than 1.82 writes, nor find the crates where a cargo of 1.85
# or newer keeps them. The lockfile stays version 3, which every newer
# cargo reads and keeps.
#
# Usage: rust/vendor.sh          write rust/Cargo.lock and rust/3rdparty/crates
#        rust/vendor.sh --check  exit 1 if either would change
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
root="$PWD"

check=0
[[ "${1:-}" == "--check" ]] && check=1

CARGO_VERSION=1.77.2
rustup="${RUSTUP:-$(command -v rustup || echo "$HOME/.cargo/bin/rustup")}"
[[ -x "$rustup" ]] || { echo "rust/vendor.sh: needs rustup (https://rustup.rs)" >&2 && exit 1; }
"$rustup" toolchain list | grep -q "^$CARGO_VERSION-" ||
  "$rustup" toolchain install "$CARGO_VERSION" --profile minimal >&2
cargo="$("$rustup" which --toolchain "$CARGO_VERSION" cargo)"
rustc="$("$rustup" which --toolchain "$CARGO_VERSION" rustc)"
cargo_home="$root/.tools/cargo-home-vendor"
mkdir -p "$cargo_home"

# The runner's inputs include the lockfile, so a first one is made here.
[[ -f rust/Cargo.lock ]] ||
  env CARGO_HOME="$cargo_home" RUSTC="$rustc" "$cargo" generate-lockfile --manifest-path rust/Cargo.toml >&2
bazel build //rust:crates_vendor 2>/dev/null || bazel build //rust:crates_vendor
runfiles="$(bazel info bazel-bin 2>/dev/null)/rust/crates_vendor.sh.runfiles/mx"

# Under --check the files are vendored in place, compared with a copy of
# them as they were, and put back: cargo-bazel asks Bazel about the
# workspace it writes to, so it cannot write to a scratch copy.
if (( check )); then
  saved="$(mktemp -d)"
  cp -a rust/Cargo.lock rust/3rdparty "$saved/"
  trap 'rm -rf rust/3rdparty && cp -a "$saved/Cargo.lock" "$saved/3rdparty" rust/ && rm -rf "$saved"' EXIT
fi

(
  cd "$runfiles"
  env - PATH="$PATH" HOME="$HOME" BUILD_WORKSPACE_DIRECTORY="$root" CARGO_HOME="$cargo_home" \
    CARGO="$cargo" RUSTC="$rustc" \
    ../cargo_bazel_prebuilt/file/downloaded vendor \
    --config rust/crates_vendor.cargo-bazel-config.json \
    --splicing-manifest rust/crates_vendor.cargo-bazel-splicing-manifest.json \
    --cargo-lockfile "$root/rust/Cargo.lock" \
    --buildifier ../cargo_bazel.buildifier-linux-amd64/file/buildifier.exe \
    --repin
) >&2

if (( check )); then
  if ! diff -r "$saved/3rdparty" rust/3rdparty > /dev/null || ! cmp -s "$saved/Cargo.lock" rust/Cargo.lock; then
    echo "rust/vendor.sh: rust/3rdparty or rust/Cargo.lock is stale: run rust/vendor.sh" >&2
    exit 1
  fi
fi
