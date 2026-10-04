#!/usr/bin/env bash
# Everything a change is checked with, in the order that fails fastest:
#   formatting and generated docs, every Mermaid block rendered, the fast
#   tests, the clang thread-safety analysis, ThreadSanitizer on the C++
#   unit tests, AddressSanitizer on the threaded client's (which shut
#   connections down in the middle of large frames), the Python package
#   type-checked, the example workspaces, and, when examples/.venv exists
#   (examples/venv.sh), the pip examples against the tree and pyright over
#   every example.
# The slow scenarios (heartbeat intervals, restarts, soak) are `bazel test //...`.
# CI runs the formatting and the fast tests on every push (.github/workflows/checks.yml).
#
# ./check.sh --leaks runs the scenarios and unit tests under AddressSanitizer
# with LeakSanitizer: the harness turns leak detection on for the C++
# processes (the multiplexer, the C++ roles), whose scenarios then fail on
# a leaked allocation at exit, an exit code leaving the Cluster checks, and
# off for the Python ones. The runtime is preloaded because the test's own
# process is the Python interpreter.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
source tools/dev_tools.sh  # the pinned tools: black, ruff, pyright, clang-format and buildifier

if [[ "${1:-}" == "--leaks" ]]; then
  # libstdc++ is preloaded with the sanitizer so that it can intercept
  # __cxa_throw in the Python processes, which load the C++ runtime late.
  preload="$(gcc -print-file-name=libasan.so):$(gcc -print-file-name=libstdc++.so.6)"
  bazel test --config=asan --test_env=LD_PRELOAD="$preload" --test_env=ASAN_OPTIONS=detect_leaks=0 \
    --test_tag_filters=-slow //lib/... //multiplexer/... //tests/...
  bazel test --config=asan --test_env=LD_PRELOAD="$preload" --test_env=ASAN_OPTIONS=detect_leaks=0 \
    //tests/scenarios:soak_memory_cc_cc //tests/scenarios:soak_memory_py_py
  echo "check --leaks: no leaks in the C++ processes"
  exit 0
fi

./format.sh --check
python3 docs/check_mermaid.py          # renders every Mermaid block with mermaid-cli
bazel test --test_tag_filters=-slow --test_env=MX_REQUIRE_PRIVATE_NETWORK=1 //...
bazel build --config=clang //...
bazel test --config=tsan //lib/... //multiplexer:threaded_client_test //multiplexer:soak_test
bazel test --config=asan --test_env=ASAN_OPTIONS=detect_leaks=0 //multiplexer:threaded_client_test //multiplexer:server_release_test
# A workspace of its own that uses @mx from C++ alone, with no Python rules
# declared: the C++ targets build without them.
(cd tests/cc_only_consumer && bazel build //...)
# Type checking after a build in the default configuration: pyright reads
# the stubs from bazel-bin (pyproject.toml), and that symlink follows the
# most recent build, which the clang and tsan steps above move away from
# the configuration that holds them. This leaves it where an editor
# expects it too, and where the examples below find the tree's package.
bazel build //... //multiplexer:_native_pyi
"$DEV_TOOLS/pyright"
if [[ -x examples/.venv/bin/pip ]]; then
  # The pip examples and their type check against the tree itself: the
  # package is a namespace package, its sources here and its generated
  # modules in bazel-bin, and examples/.venv holds the examples'
  # requirements without mx-multiplexer. An installed mx-multiplexer, a
  # regular package, would win over the tree on any path, so it is refused.
  # A notebook is left to the examples workflow, whose venv has the wheel.
  if ! PYTHONPATH="$PWD/bazel-bin:$PWD" examples/.venv/bin/python -c 'import multiplexer, sys; sys.exit(multiplexer.__file__ is not None)'; then
    echo "check: examples/.venv has mx-multiplexer installed, which would be tested instead of the tree;" \
      "examples/.venv/bin/pip uninstall mx-multiplexer" >&2
    exit 1
  fi
  PYTHONPATH="$PWD/bazel-bin:$PWD" VENV="$PWD/examples/.venv" MX_TREE=1 EXAMPLES_PIP=1 \
    MXCONTROL="$PWD/bazel-bin/mxcontrol/mxcontrol" ./examples/test_all.sh
  ./examples/mxtree.sh                  # the copy of the package the examples' type check resolves from
  "$DEV_TOOLS/pyright" -p examples/pyrightconfig.json
else
  ./examples/test_all.sh
  echo "check: without examples/.venv the pip examples and their type check are skipped; examples/venv.sh makes it"
fi
echo "check: everything passed"
