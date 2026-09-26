#!/usr/bin/env bash
# The example's check: a virtual environment with the requirements (torch
# from its CPU index, a 200 MB download, once), the test, and the
# walkthrough executed in a scratch copy of this directory, so that every
# cell still runs and nothing here is rewritten. The test's multiplexers are
# the mxcontrol the package installed unless $MXCONTROL names another; the
# walkthrough's cells run the package's, as a reader's do. The
# walkthrough's steps use the ports 1980, 1981 and 8000 and curl, as a
# reader's do: with a port taken or curl missing, the check stops and says
# so.
#
#   examples/inference/test.sh [--no-notebook]        VENV=path to reuse a venv
#
# With MX_TREE=1 the library is the tree on PYTHONPATH rather than the
# package: the requirements are installed without mx-multiplexer, the
# multiplexer is the tree's, $MXCONTROL, else this repository's own build
# (bazel build //mxcontrol), and the notebook is not executed, since its
# install cell would put the published package over the tree.
set -euo pipefail
[[ -n "${VENV:-}" ]] && VENV="$(realpath "$VENV")"
[[ -n "${MXCONTROL:-}" ]] && MXCONTROL="$(realpath "$MXCONTROL")"
cd "$(dirname "${BASH_SOURCE[0]}")"
venv="${VENV:-.venv}"
if [[ ! -x "$venv/bin/pip" ]]; then
  [[ -n "${VENV:-}" ]] && { echo "$VENV has no pip; make it with python3 -m venv, or leave VENV unset" >&2; exit 1; }
  python3 -m venv --clear "$venv"  # this script's own, whose ensurepip may have failed: made again
fi
venv="$(realpath "$venv")"
notebook="${1:-}"
if [[ "${MX_TREE:-}" == 1 ]]; then
  cat requirements.txt requirements-notebook.txt | grep -v '^mx-multiplexer\|^-r ' | "$venv/bin/pip" install -q -r /dev/stdin
  notebook="--no-notebook"
else
  "$venv/bin/pip" install -q -r requirements-notebook.txt
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
if [[ "$notebook" != "--no-notebook" ]]; then
  for port in 1980 1981 8000; do
    if (echo > "/dev/tcp/127.0.0.1/$port") 2> /dev/null; then
      echo "port $port is taken; the walkthrough's steps need 1980, 1981 and 8000 free" >&2
      exit 1
    fi
  done
  command -v curl > /dev/null || { echo "the walkthrough's steps call the web app with curl, which is missing" >&2; exit 1; }
  scratch="$(mktemp -d)"
  # Whatever the notebook left running goes with the copy.
  trap 'for pid in "$scratch"/*.pid; do [[ -f "$pid" ]] && kill "$(cat "$pid")" 2> /dev/null || true; done; rm -rf "$scratch"' EXIT
  find . -mindepth 1 -maxdepth 1 ! -name .venv ! -name __pycache__ -exec cp -r {} "$scratch/" \;
  # The notebook's shell cells run `mxcontrol`, `python` and `pip` from
  # PATH, as a reader whose venv is active would; a step that fails fails
  # its cell, and the cell the run.
  PATH="$venv/bin:$PATH" "$venv/bin/jupyter" nbconvert --to notebook --execute \
    --ExecutePreprocessor.timeout=900 --inplace "$scratch/walkthrough.ipynb" > /dev/null
  echo "walkthrough.ipynb: every cell ran"
fi
