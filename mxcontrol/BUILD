# The mxcontrol binary and its subcommands; each subcommand is a library so
# that other binaries (the test roles) can reuse the framework.
load("//bazel:maybe_strip.bzl", "maybe_strip_cc_binary")

cc_library(
    name = "task",
    srcs = ["task.cc"],
    hdrs = ["task.h"],
    visibility = ["//tests:__subpackages__"],
    deps = [
        "//lib:options",
        "//lib/logging",
        "//multiplexer:client",
        "@asio",
    ],
)

cc_library(
    name = "tasks_holder",
    srcs = ["tasks_holder.cc"],
    hdrs = ["tasks_holder.h"],
    visibility = ["//tests:__subpackages__"],
    deps = [
        ":task",
    ],
)

# Subcommands register themselves from static initializers, hence alwayslink.
cc_library(
    name = "help",
    srcs = ["help.cc"],
    hdrs = ["help.h"],
    visibility = ["//tests:__subpackages__"],
    deps = [
        ":tasks_holder",
    ],
    alwayslink = 1,
)

cc_library(
    name = "receive_logs",
    srcs = ["receive_logs.cc"],
    deps = [
        ":task",
        ":tasks_holder",
        "//multiplexer/backend:base_multiplexer_server",
    ],
    alwayslink = 1,
)

cc_library(
    name = "start_multiplexer_server",
    srcs = ["start_multiplexer_server.cc"],
    hdrs = ["start_multiplexer_server.h"],
    deps = [
        ":task",
        ":tasks_holder",
        "//lib:fingerprint",
        "//multiplexer:recorder",
        "//multiplexer:server",
        "@asio",
    ],
    alwayslink = 1,
)

cc_library(
    name = "dump_recording",
    srcs = ["dump_recording.cc"],
    deps = [
        ":task",
        ":tasks_holder",
        "//lib/protobuf:stream",
        "//multiplexer:config",
        "//multiplexer:recording_cc_proto",
    ],
    alwayslink = 1,
)

cc_library(
    name = "recording_control",
    srcs = ["recording_control.cc"],
    deps = [
        ":task",
        ":tasks_holder",
        "//lib/protobuf:stream",
        "//multiplexer:client",
        "//multiplexer:multiplexer_cc_proto",
        "//multiplexer:recording_cc_proto",
        "@asio",
    ],
    alwayslink = 1,
)

cc_library(
    name = "stream_logs",
    srcs = ["stream_logs.cc"],
    deps = [
        ":task",
        ":tasks_holder",
        "//multiplexer:multiplexer_cc_proto",
    ],
    alwayslink = 1,
)

cc_library(
    name = "driver",
    srcs = ["driver.cc"],
    hdrs = ["driver.h"],
    visibility = ["//tests:__subpackages__"],
    deps = [
        ":tasks_holder",
        "//lib:assert",
        "//lib:options",
        "//lib/logging",
    ],
)

maybe_strip_cc_binary(
    name = "mxcontrol",
    srcs = ["main.cc"],
    data = ["//:multiplexer_rules"],
    visibility = ["//visibility:public"],
    deps = [
        ":driver",
        ":dump_recording",
        ":help",
        ":receive_logs",
        ":recording_control",
        ":start_multiplexer_server",
        ":stream_logs",
        "//lib:core_dump",
        "//lib:program",
    ],
)

# The same binary linked statically: one file that runs on any Linux and
# in an empty container, for the release asset and the image. The one
# thing static glibc cannot do everywhere is resolve host names, which
# needs the host's own name-service modules: the multiplexer listens on an
# address and never resolves, and the client subcommands take addresses
# too. Built only when named; the release workflow does.
cc_binary(
    name = "mxcontrol_static",
    srcs = ["main.cc"],
    data = ["//:multiplexer_rules"],
    linkopts = [
        "-static",
        "-Wl,--whole-archive,-lpthread,--no-whole-archive",
    ],
    linkstatic = True,
    tags = ["manual"],
    visibility = ["//visibility:public"],
    deps = [
        ":driver",
        ":dump_recording",
        ":help",
        ":receive_logs",
        ":recording_control",
        ":start_multiplexer_server",
        ":stream_logs",
        "//lib:core_dump",
        "//lib:program",
    ],
)
