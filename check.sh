#!/usr/bin/env bash
# Everything a change is checked with, in the order that fails fastest:
#   formatting and generated docs, every Mermaid block rendered, the fast
#   tests, the same with every multiplexer on two event loops, the clang
#   thread-safety analysis, the fast tests built against libc++ instead of
#   libstdc++, ThreadSanitizer on the C++ unit tests and on the scenarios
#   with C++ roles, their multiplexers on two loops,
#   AddressSanitizer on the threaded client's (which shut connections
#   down in the middle of large frames), clippy over the Rust code and the
#   vendored crates' freshness (rust/vendor.sh), the Python package type-checked,
#   the example workspaces, and, when examples/.venv exists
#   (examples/venv.sh), the pip examples against the tree and pyright over
#   every example.
# The slow scenarios (heartbeat intervals, restarts, soak) are `bazel test //...`.
# CI runs the formatting and the fast tests, also against libc++, on every
# push (.github/workflows/checks.yml).
#
# ./check.sh --leaks runs the scenarios and unit tests under AddressSanitizer
# with LeakSanitizer: the harness turns leak detection on for the C++
# processes (the multiplexer, the C++ roles), whose scenarios then fail on
# a leaked allocation at exit, an exit code leaving the Cluster checks, and
# off for the Python ones. The runtime is preloaded because the test's own
# process is the Python interpreter.
#
# ./check.sh --valgrind runs valgrind's checks, which no sanitizer makes,
# needing valgrind installed: memcheck over the C++ unit tests and over
# every multiplexer the scenarios with C++ roles start, for a value never
# initialized that a branch or a system call reads (MemorySanitizer is not
# run, and frames' buffers are made without zero-fill), RE2's reads of what
# it never wrote, on purpose, suppressed (tools/memcheck.supp); and the
# instructions the multiplexer spends on a query's round trip, counted by
# callgrind against their budget (tests/budgets). Tens of times slower than
# a native run, so local only, a few minutes in all.
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

if [[ "${1:-}" == "--valgrind" ]]; then
  if ! command -v valgrind > /dev/null; then
    echo "check --valgrind: valgrind is not installed (apt install valgrind)" >&2
    exit 1
  fi
  memcheck="valgrind --tool=memcheck --error-exitcode=99 --track-origins=yes --leak-check=no"
  memcheck+=" --suppressions=$PWD/tools/memcheck.supp"
  # The tests that cannot run under valgrind: those that count allocations
  # by an operator new of their own, which valgrind replaces, one that runs
  # a renamed child, which valgrind refuses to start, and one whose sends
  # wait for room within the default deadline, ten seconds, which memcheck's
  # pace outlasts while the test holds a multiplexer frozen.
  cannot="ProcessName.*:ParseTarget.*:ReceiveAllocations.*:SendAllocations.*:RouteAllocations.*"
  cannot+=":ThreadedClient.EveryMultiplexerGetsItsCopyOfASendToAll"
  bazel test --run_under="$memcheck" --test_lang_filters=cc --test_arg=--gtest_filter="-$cannot" \
    --test_timeout=3600 --test_tag_filters=-slow //lib/... //multiplexer/...
  bazel build //mxcontrol
  bazel test --test_env=MXCONTROL="$PWD/tools/memcheck_mxcontrol.sh" \
    --test_env=MX_MEMCHECK_MXCONTROL="$PWD/bazel-bin/mxcontrol/mxcontrol" --test_timeout=3600 --build_tests_only \
    --test_tag_filters=lang-cc,-lang-py,-lang-rust,-lang-bin,-slow //tests/scenarios/...
  bazel test -c opt //tests/budgets:instructions_test
  # Back to the default configuration, where the stubs and an editor expect bazel-bin.
  bazel build //multiplexer:_native_pyi > /dev/null
  echo "check --valgrind: nothing uninitialized read, and the routing within its budget"
  exit 0
fi

./format.sh --check
python3 docs/check_mermaid.py          # renders every Mermaid block with mermaid-cli
bazel test --test_tag_filters=-slow --test_env=MX_REQUIRE_PRIVATE_NETWORK=1 //...
# Every multiplexer the tests start on two event loops (run_multiplexer
# --threads), the in-process ones aside.
bazel test --test_tag_filters=-slow --test_env=MX_REQUIRE_PRIVATE_NETWORK=1 --test_env=MX_SERVER_THREADS=2 //...
bazel build --config=clang //...
# The fast tests again against libc++ (.bazelrc), the standard library a
# build of @mx with clang may use instead of libstdc++.
if ! echo '#include <vector>' | clang-18 -stdlib=libc++ -x c++ -fsyntax-only - 2>/dev/null; then
  echo "check: --config=libcxx needs libc++: sudo apt-get install libc++-18-dev" >&2
  exit 1
fi
bazel test --config=libcxx --test_tag_filters=-slow --test_env=MX_REQUIRE_PRIVATE_NETWORK=1 //...
bazel test --config=tsan //lib/... //multiplexer:threaded_client_test //multiplexer:soak_test \
  //multiplexer:server_loops_test //multiplexer/io:shared_outgoing_test //multiplexer:write_through_test \
  //multiplexer:several_loops_test
# The scenarios whose roles are all C++, every multiplexer on two loops: the
# multiplexer and the roles are built with the sanitizer, as is the Python
# extension the harness loads, which the tunable leaves room to load late,
# its runtime not preloaded (a Python client cannot run so, nor a shell
# with it preloaded).
bazel test --config=tsan --test_env=MX_SERVER_THREADS=2 --test_env=GLIBC_TUNABLES=glibc.rtld.optional_static_tls=262144 \
  --build_tests_only --test_tag_filters=lang-cc,-lang-py,-lang-rust,-lang-bin,-slow //tests/scenarios/...
bazel test --config=asan --test_env=ASAN_OPTIONS=detect_leaks=0 //multiplexer:threaded_client_test //multiplexer:server_release_test
# The Rust code: clippy's warnings are errors, as the compilers' are, and an
# unsafe block without its SAFETY comment is one by every Cargo.toml's
# [lints]; the crate's documentation, what docs.rs shows, builds without a
# warning, a broken link included; the crates Bazel builds are the ones the
# Cargo manifests ask for. cargo runs at the release rust-toolchain.toml
# pins wherever it runs.
(cd rust && cargo clippy --quiet --all-targets -- -D warnings)
(cd rust && cargo clippy --quiet --manifest-path codegen/Cargo.toml -- -D warnings)
for manifest in tests/roles/rust/Cargo.toml examples/*/Cargo.toml; do
  cargo clippy --quiet --manifest-path "$manifest" --all-targets -- -D warnings
done
(cd rust && RUSTDOCFLAGS="-D warnings" cargo doc --quiet --no-deps -p mx-multiplexer)
rust/vendor.sh --check
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
  PYTHONPATH="$PWD/bazel-bin:$PWD" VENV="$PWD/examples/.venv" MX_TREE=1 EXAMPLES_PIP=1 EXAMPLES_RUST=1 \
    MXCONTROL="$PWD/bazel-bin/mxcontrol/mxcontrol" ./examples/test_all.sh
  ./examples/mxtree.sh                  # the copy of the package the examples' type check resolves from
  "$DEV_TOOLS/pyright" -p examples/pyrightconfig.json
else
  EXAMPLES_RUST=1 MXCONTROL="$PWD/bazel-bin/mxcontrol/mxcontrol" ./examples/test_all.sh
  echo "check: without examples/.venv the pip examples and their type check are skipped; examples/venv.sh makes it"
fi
echo "check: everything passed"
