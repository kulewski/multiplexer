#!/usr/bin/env bash
# Everything CI should run, in the order that fails fastest:
#   formatting and generated docs, every Mermaid block rendered, the fast
#   tests, the clang thread-safety analysis, ThreadSanitizer on the C++ unit
#   tests, the example workspaces.
# The slow scenarios (heartbeat intervals, restarts, soak) are `bazel test //...`.
#
# ./check.sh --leaks runs the scenarios and unit tests under AddressSanitizer
# with LeakSanitizer: the harness turns leak detection on for the C++
# processes (the multiplexer, the C++ roles), whose scenarios then fail on
# a leaked allocation at exit, and off for the Python ones. The runtime is
# preloaded because the test's own process is the Python interpreter.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

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
bazel test --test_tag_filters=-slow //...
bazel build --config=clang //...
bazel test --config=tsan //lib/... //multiplexer:threaded_client_test //multiplexer:soak_test
./examples/test_all.sh
echo "check: everything passed"
