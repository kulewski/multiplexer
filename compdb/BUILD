# compile_commands.json for clangd and other editors.
#
#   bazel build --config=debug //...     # generated headers must exist first
#   bazel run //compdb
#
# Writes compile_commands.json and an `external` symlink to the workspace
# root; both are ignored by git. The targets are the binaries with debug
# symbols (the maybe_strip wrappers have no compile actions) plus the tools
# and tests, so every C++ file in the tree gets a command. Generated headers
# resolve to bazel-out/k8-dbg, so keep the build configuration at debug.
load("@hedron_compile_commands//:refresh_compile_commands.bzl", "refresh_compile_commands")

refresh_compile_commands(
    name = "compdb",
    # Entries for Boost's and protobuf's own headers are not useful in the
    # editor and make the file two hundred times bigger.
    exclude_headers = "external",
    targets = {
        "//lib:kwargs_test": "--config=debug",
        "//multiplexer:_native_with_debug_symbols.so": "--config=debug",
        "//multiplexer:generate_constants": "--config=debug",
        "//mxcontrol:mxcontrol_with_debug_symbols": "--config=debug",
        "//tests/roles/cc:mxtestroles": "--config=debug",
    },
)
