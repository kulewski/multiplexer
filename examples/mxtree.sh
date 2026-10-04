#!/usr/bin/env bash
# One complete copy of the tree's Python package for the examples' type
# check, under examples/.venv/mxtree: the generated modules from bazel-bin
# (or from make's build/python, whose stub of the extension needs
# pybind11-stubgen), then the sources over them. Pyright cannot merge a
# package split across two roots inside an execution environment, and the
# examples need one environment each, so they resolve the library from
# this copy. A copy per Bazel example carries that example's own
# constants. check.sh refreshes it before the type check; nothing runs
# from it.
#
#   examples/mxtree.sh          after `bazel build //... //multiplexer:_native_pyi`
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
tree=examples/.venv/mxtree
rm -rf "$tree" examples/.venv/mxtree-*
mkdir -p "$tree"
copy_python() {  # copy_python FROM TO: the .py and .pyi files under multiplexer and lib, runfiles trees skipped
  (cd "$1" && find multiplexer lib -name '*.runfiles' -prune -o \( -name '*.py' -o -name '*.pyi' \) -print0 2> /dev/null \
    | xargs -0 --no-run-if-empty cp --parents -t "$2")
}
if [[ -d bazel-bin/multiplexer ]]; then
  copy_python bazel-bin "$PWD/$tree"
elif [[ -d build/python/multiplexer ]]; then
  copy_python build/python "$PWD/$tree"  # a copy of every source too, as of the last make: the sources below win
else
  echo "examples/mxtree.sh: neither bazel-bin nor build/python holds the generated modules; build first" >&2
  exit 1
fi
chmod -R u+w "$tree"  # Bazel's outputs are read-only
copy_python . "$PWD/$tree"
for example in examples/*/; do
  [[ -f "$example/WORKSPACE" && ! -f "$example/requirements.txt" ]] || continue  # a Bazel example, with its own constants
  generated="${example%/}/bazel-bin/external/mx/multiplexer"
  if [[ ! -f "$generated/multiplexer_constants.py" ]]; then
    echo "examples/mxtree.sh: ${example%/} is not built, so its type check cannot resolve the library;" \
      "cd ${example%/} && bazel build //..., then this again" >&2
    continue
  fi
  copy="examples/.venv/mxtree-$(basename "$example")"
  cp -r "$tree" "$copy"
  # The example's constants, and no stub of the library's to shadow them.
  rm -f "$copy"/multiplexer/multiplexer_constants.py "$copy"/multiplexer/multiplexer_constants.pyi
  cp "$generated"/multiplexer_constants.py* "$copy/multiplexer/"
done
echo "examples/.venv/mxtree: $(find "$tree" -name '*.py*' | wc -l) files$(ls -d examples/.venv/mxtree-* 2> /dev/null | sed 's#.*/mxtree-#, with #' | tr -d '\n')"
