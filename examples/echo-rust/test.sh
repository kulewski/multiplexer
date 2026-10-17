#!/usr/bin/env bash
# Builds the example and runs its tests with cargo, from rustup, against
# the multiplexer $MXCONTROL names: one from the Debian package or the
# wheel, `pip install mx-multiplexer`, or, when it is unset, this
# repository's, built here with Bazel.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
if [[ -z "${MXCONTROL:-}" ]]; then
  if ! command -v bazel > /dev/null; then
    echo "test.sh: set MXCONTROL to an mxcontrol, or install Bazel to build this repository's" >&2
    exit 1
  fi
  # Quiet unless it fails: then what Bazel said, and nothing else.
  if ! log="$(cd ../.. && bazel build //mxcontrol 2>&1)"; then
    echo "$log" >&2
    exit 1
  fi
  MXCONTROL="$(cd ../.. && pwd)/bazel-bin/mxcontrol/mxcontrol"
fi
MXCONTROL="$MXCONTROL" cargo test
