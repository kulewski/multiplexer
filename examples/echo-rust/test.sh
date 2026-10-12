#!/usr/bin/env bash
# Builds the example and runs its tests with cargo, from rustup, against
# the multiplexer $MXCONTROL names: one from the Debian package or the
# wheel, `pip install mx-multiplexer`, or, when it is unset, this
# repository's, built here with Bazel.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
if [[ -z "${MXCONTROL:-}" ]]; then
  (cd ../.. && bazel build //mxcontrol > /dev/null 2>&1)
  MXCONTROL="$(cd ../.. && pwd)/bazel-bin/mxcontrol/mxcontrol"
fi
MXCONTROL="$MXCONTROL" cargo test
