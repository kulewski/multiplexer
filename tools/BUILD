# Development tools for this workspace only (see the dev section of
# WORKSPACE): a consumer of @mx never sees them.
# The stub of the native extension, from the built module's signatures:
#   bazel build //multiplexer:_native_pyi
py_binary(
    name = "native_stub",
    srcs = ["native_stub.py"],
    visibility = ["//multiplexer:__pkg__"],
    deps = ["@pybind11_stubgen_wheel//:pybind11_stubgen"],
)
