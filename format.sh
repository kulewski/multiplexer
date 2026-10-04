#!/usr/bin/env bash
# Formats every source file in the repository, with the tools at the
# versions requirements-dev.txt and tools/dev_tools.sh pin, from .tools/venv,
# so that a check here and the one CI runs agree:
#   Python  -> black  (settings in pyproject.toml: 120 columns, py311), the
#              code cells of notebooks too; in --check, ruff too (its rules
#              in pyproject.toml)
#   C++     -> clang-format 18 (settings in .clang-format: Google, 120 columns, braces everywhere)
#   Bazel   -> buildifier (BUILD, WORKSPACE, *.bzl), which also sorts the loads
#   YAML    -> parsed with PyYAML in --check (the workflow files), no rewriting
#   docs    -> docs/diagrams/generate.py regenerates the protocol pages,
#              docs/code_map.py regenerates the code map from header comments,
#              docs/check_mermaid.py --fast catches Mermaid syntax slips,
#              examples/check_walkthroughs.py keeps the walkthroughs' code
#              blocks identical to the examples' files
#   make    -> make/generate_sources.py regenerates the Makefile's source lists
#   rules   -> every *.rules file starts with the system rules, multiplexer.rules,
#              as `mxcontrol generate_rules` writes them (--check)
#
# Usage: ./format.sh          rewrite files in place
#        ./format.sh --check  exit 1 if anything would change (for CI)
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

check=0
[[ "${1:-}" == "--check" ]] && check=1

source tools/dev_tools.sh

# Source files only: skip Bazel's output symlinks, make's build/, the tools and anything generated.
prune=(-path ./bazel-\* -prune -o -path ./build -prune -o -path ./.tools -prune -o -name .venv -prune -o)
mapfile -t py < <(find . "${prune[@]}" \( -name '*.py' -o -name '*.ipynb' \) -print | sort)
mapfile -t cc < <(find . "${prune[@]}" \( -name '*.h' -o -name '*.cc' \) -print | sort)
mapfile -t bzl < <(find . "${prune[@]}" \( -name BUILD -o -name WORKSPACE -o -name '*.bzl' \) -print | sort)
mapfile -t yml < <(find . "${prune[@]}" \( -name '*.yml' -o -name '*.yaml' \) -print | sort)
mapfile -t rules < <(find . "${prune[@]}" -name '*.rules' -print | sort)

status=0
if (( check )); then
  python3 docs/diagrams/generate.py --check || status=1
  python3 docs/code_map.py --check || status=1
  python3 make/generate_sources.py --check || status=1
  python3 docs/check_mermaid.py --fast > /dev/null || { python3 docs/check_mermaid.py --fast; status=1; }
  python3 examples/check_walkthroughs.py > /dev/null || { python3 examples/check_walkthroughs.py; status=1; }
  "$DEV_TOOLS/black" --check --quiet "${py[@]}" || status=1
  "$DEV_TOOLS/ruff" check --quiet --force-exclude "${py[@]}" || status=1
  "$DEV_TOOLS/clang-format" --dry-run --Werror "${cc[@]}" || status=1
  "$DEV_TOOLS/buildifier" -mode=check "${bzl[@]}" || status=1
  # Every YAML file parses: a workflow with a syntax slip fails on GitHub before any job starts.
  "$DEV_TOOLS/python" -c 'import sys, yaml
for path in sys.argv[1:]:
    with open(path) as f:
        yaml.safe_load(f)' "${yml[@]}" || status=1
  python3 -c 'import sys
system = open("multiplexer.rules", "rb").read()
for path in sys.argv[1:]:
    if not open(path, "rb").read().startswith(system):
        print(path + ": does not start with the system rules, multiplexer.rules", file=sys.stderr)
        sys.exit(1)' "${rules[@]}" || status=1
  (( status == 0 )) && echo "format: all ${#py[@]} Python, ${#cc[@]} C++, ${#bzl[@]} Bazel and ${#yml[@]} YAML files are clean"
else
  python3 docs/diagrams/generate.py
  python3 docs/code_map.py
  python3 make/generate_sources.py
  "$DEV_TOOLS/black" --quiet "${py[@]}"
  "$DEV_TOOLS/clang-format" -i "${cc[@]}"
  "$DEV_TOOLS/buildifier" "${bzl[@]}"
  echo "format: ${#py[@]} Python, ${#cc[@]} C++ and ${#bzl[@]} Bazel files formatted"
fi
exit $status
