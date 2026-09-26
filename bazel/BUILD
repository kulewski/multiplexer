# Build machinery: maybe_strip.bzl, patches/ for external repositories, and
# the default protobuf toolchain behind //:protoc and //:protobuf_runtime.

# protoc from PATH; see //:protoc for pointing the build at another one.
sh_binary(
    name = "system_protoc",
    srcs = ["system_protoc.sh"],
    visibility = ["//visibility:public"],
)

sh_test(
    name = "system_protoc_test",
    size = "small",
    srcs = ["system_protoc_test.sh"],
    data = ["system_protoc.sh"],
)

# The system libprotobuf, headers from the system include path; see
# //:protobuf_runtime for pointing the build at another one.
cc_library(
    name = "system_protobuf",
    linkopts = ["-lprotobuf"],
    visibility = ["//visibility:public"],
)
