#!/usr/bin/env bash
# The example's check: a virtual environment with the requirements, and
# the test against real multiplexers, run by the mxcontrol the package
# installed unless $MXCONTROL names another.
#
#   examples/cache/test.sh          VENV=path to reuse a venv
#
# With MX_TREE=1 the library is the tree on PYTHONPATH rather than the
# package: the requirements are installed without mx-multiplexer, and the
# multiplexer is the tree's, $MXCONTROL, else this repository's own build
# (bazel build //mxcontrol).
set -euo pipefail
[[ -n "${VENV:-}" ]] && VENV="$(realpath "$VENV")"
[[ -n "${MXCONTROL:-}" ]] && MXCONTROL="$(realpath "$MXCONTROL")"
cd "$(dirname "${BASH_SOURCE[0]}")"
venv="${VENV:-.venv}"
if [[ ! -x "$venv/bin/pip" ]]; then
  [[ -n "${VENV:-}" ]] && { echo "$VENV has no pip; make it with python3 -m venv, or leave VENV unset" >&2; exit 1; }
  python3 -m venv --clear "$venv"  # this script's own, whose ensurepip may have failed: made again
fi
if [[ "${MX_TREE:-}" == 1 ]]; then
  grep -v '^mx-multiplexer' requirements.txt | "$venv/bin/pip" install -q -r /dev/stdin  # the tree is on PYTHONPATH
else
  "$venv/bin/pip" install -q -r requirements.txt
fi
if [[ "${MX_TREE:-}" == 1 && -z "${MXCONTROL:-}" ]]; then  # the tree's library runs the tree's multiplexer
  [[ -x ../../bazel-bin/mxcontrol/mxcontrol ]] \
    || { echo "MX_TREE=1 runs the tree's multiplexer: set MXCONTROL, or run bazel build //mxcontrol" >&2; exit 1; }
  MXCONTROL="$(realpath ../../bazel-bin/mxcontrol/mxcontrol)"
fi
# Unset, the harness runs the mxcontrol the package installed.
if [[ -n "${MXCONTROL:-}" ]]; then export MXCONTROL; fi
export MX_LOG_VERBOSITY="${MX_LOG_VERBOSITY:-DEBUG:LOW}"
"$venv/bin/python" -m unittest -v test
