#!/usr/bin/env bash
# Formats every source file in the repository:
#   Python  -> black  (settings in pyproject.toml: 120 columns, py311)
#   C++     -> clang-format-18 (settings in .clang-format: LLVM, 120 columns)
#   Bazel   -> buildifier (BUILD, WORKSPACE, *.bzl)
#   docs    -> docs/diagrams/generate.py regenerates the protocol pages,
#              docs/code_map.py regenerates the code map from header comments,
#              docs/check_mermaid.py --fast catches Mermaid syntax slips
#
# Usage: ./format.sh          rewrite files in place
#        ./format.sh --check  exit 1 if anything would change (for CI)
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

check=0
[[ "${1:-}" == "--check" ]] && check=1

# Source files only: skip Bazel's output symlinks and anything generated.
prune=(-path ./bazel-\* -prune -o)
mapfile -t py < <(find . "${prune[@]}" -name '*.py' -print | sort)
mapfile -t cc < <(find . "${prune[@]}" \( -name '*.h' -o -name '*.cc' \) -print | sort)
mapfile -t bzl < <(find . "${prune[@]}" \( -name BUILD -o -name WORKSPACE -o -name '*.bzl' \) -print | sort)

status=0
if (( check )); then
  python3 docs/diagrams/generate.py --check || status=1
  python3 docs/code_map.py --check || status=1
  python3 docs/check_mermaid.py --fast > /dev/null || { python3 docs/check_mermaid.py --fast; status=1; }
  black --check --quiet "${py[@]}" || status=1
  clang-format-18 --dry-run --Werror "${cc[@]}" || status=1
  buildifier -mode=check "${bzl[@]}" || status=1
  (( status == 0 )) && echo "format: all ${#py[@]} Python, ${#cc[@]} C++ and ${#bzl[@]} Bazel files are clean"
else
  python3 docs/diagrams/generate.py
  python3 docs/code_map.py
  black --quiet "${py[@]}"
  clang-format-18 -i "${cc[@]}"
  buildifier "${bzl[@]}"
  echo "format: ${#py[@]} Python, ${#cc[@]} C++ and ${#bzl[@]} Bazel files formatted"
fi
exit $status
