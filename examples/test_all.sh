#!/usr/bin/env bash
# Runs the tests of every example workspace. Each is a separate Bazel
# workspace, so this is a loop rather than one bazel invocation.
# An example with a test.sh is installed with pip, or has a pip side, and
# is checked by that script, which downloads its requirements; those run
# only with EXAMPLES_PIP=1, as the examples workflow sets.
set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
status=0
for ws in */WORKSPACE; do
  example=${ws%/WORKSPACE}
  echo "==== examples/$example"
  (cd "$example" && bazel test //...) || status=1
done
for script in */test.sh; do
  example=${script%/test.sh}
  if [[ "${EXAMPLES_PIP:-}" == 1 ]]; then
    echo "==== examples/$example (pip)"
    "$script" || status=1
  else
    echo "==== examples/$example: skipped, a pip example; EXAMPLES_PIP=1 runs it"
  fi
done
exit $status
