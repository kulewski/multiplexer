#!/usr/bin/env bash
# Runs the tests of every example workspace. Each is a separate Bazel
# workspace, so this is a loop rather than one bazel invocation.
set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
status=0
for ws in */WORKSPACE; do
  example=${ws%/WORKSPACE}
  echo "==== examples/$example"
  (cd "$example" && bazel test //...) || status=1
done
exit $status
