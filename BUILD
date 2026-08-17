# Repository root: build-mode settings, the rules-file flag, and aliases.
load("@bazel_skylib//rules:common_settings.bzl", "bool_flag")

# Build-mode switches used by select() in the packages below and in
# bazel/maybe_strip.bzl.
config_setting(
    name = "debug_build",
    values = {"compilation_mode": "dbg"},
)

bool_flag(
    name = "is_asan",
    build_setting_default = False,
)

config_setting(
    name = "asan_build",
    flag_values = {":is_asan": "true"},
)

config_setting(
    name = "release_build",
    values = {"compilation_mode": "opt"},
)

# The routing rules that constants are generated from and that the binaries
# ship with. A deployment keeps its own peer and message types out of this
# repository by pointing the flag at its own file, for example
#   bazel build --@mx//:multiplexer_rules=//your/pkg:multiplexer.rules ...
label_flag(
    name = "multiplexer_rules",
    build_setting_default = ":multiplexer.rules",
    visibility = ["//visibility:public"],
)

exports_files(["multiplexer.rules"])

# The protocol buffer toolchain. By default protoc comes from PATH and the
# runtime is the system libprotobuf; a workspace that bundles its own, for
# example to match a protobuf linked into the same binary by another
# library, points both flags at its targets:
#   bazel build --@mx//:protoc=//third_party/protobuf:protoc \
#               --@mx//:protobuf_runtime=//third_party/protobuf:libprotobuf ...
# The generated .pb.cc then comes from that protoc and links that runtime.
label_flag(
    name = "protoc",
    build_setting_default = "//bazel:system_protoc",
    visibility = ["//visibility:public"],
)

label_flag(
    name = "protobuf_runtime",
    build_setting_default = "//bazel:system_protobuf",
    visibility = ["//visibility:public"],
)

# The command-line tool lives in //mxcontrol; this keeps //:mxcontrol working.
alias(
    name = "mxcontrol",
    actual = "//mxcontrol",
    visibility = ["//visibility:public"],
)
