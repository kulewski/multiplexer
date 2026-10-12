#!/usr/bin/env bash
# Runs the tests of every example workspace. Each is a separate Bazel
# workspace, so this is a loop rather than one bazel invocation.
# An example with a test.sh is installed with pip, or has a pip side, and
# is checked by that script, which downloads its requirements; those run
# only with EXAMPLES_PIP=1, as the examples workflow sets. An example with
# a Cargo.toml is a Cargo workspace, checked by its test.sh with cargo,
# which downloads its crates; those run only with EXAMPLES_RUST=1, as
# check.sh and CI's Rust job set.
set -uo pipefail
shopt -s nullglob  # a pattern no example matches expands to nothing
cd "$(dirname "${BASH_SOURCE[0]}")"
status=0
for ws in */WORKSPACE; do
  example=${ws%/WORKSPACE}
  echo "==== examples/$example"
  (cd "$example" && bazel test //...) || status=1
done
for script in */test.sh; do
  example=${script%/test.sh}
  [[ -f "$example/Cargo.toml" ]] && continue  # a Cargo example, below
  if [[ "${EXAMPLES_PIP:-}" == 1 ]]; then
    echo "==== examples/$example (pip)"
    "$script" || status=1
  else
    echo "==== examples/$example: skipped, a pip example; EXAMPLES_PIP=1 runs it"
  fi
done
for manifest in */Cargo.toml; do
  example=${manifest%/Cargo.toml}
  if [[ "${EXAMPLES_RUST:-}" == 1 ]]; then
    echo "==== examples/$example (cargo)"
    "$example/test.sh" || status=1
  else
    echo "==== examples/$example: skipped, a Cargo example; EXAMPLES_RUST=1 runs it"
  fi
done
exit $status
