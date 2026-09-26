#!/usr/bin/env bash
# The virtual environment check.sh runs the pip examples and their type
# check with: every example's requirements except mx-multiplexer, since
# check.sh puts the tree itself on PYTHONPATH (the package is a namespace
# package: the sources here, the generated modules in bazel-bin), and
# Django's stubs for the type check when an example is on Django; the
# first run downloads them, once. With a build in bazel-bin, mxtree.sh
# then makes the copy of the package the type check resolves from; the
# Bazel examples, echo and aio, need their own workspace built for theirs
# (`bazel build //...` there), and it says so when one is not.
#
#   examples/venv.sh            makes examples/.venv, or updates it
set -euo pipefail
shopt -s nullglob
cd "$(dirname "${BASH_SOURCE[0]}")"
[[ -x .venv/bin/pip ]] || python3 -m venv --clear .venv  # a venv whose ensurepip failed has no pip: made again
cat /dev/null */requirements*.txt | grep -v '^mx-multiplexer\|^-r ' > .venv/requirements.txt || true
grep -qi '^django' .venv/requirements.txt && echo django-stubs >> .venv/requirements.txt
.venv/bin/pip install -q -r .venv/requirements.txt
[[ -d ../bazel-bin/multiplexer ]] && ./mxtree.sh
echo "examples/.venv is ready: $(.venv/bin/python --version), $(.venv/bin/pip list 2> /dev/null | wc -l) packages"
