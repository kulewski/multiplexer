# The mxcontrol binary and its subcommands; each subcommand is a library so
# that other binaries (the test roles) can reuse the framework.
load("//bazel:maybe_strip.bzl", "maybe_strip_cc_binary")

cc_library(
    name = "task",
    srcs = ["task.cc"],
    hdrs = ["task.h"],
    visibility = ["//tests:__subpackages__"],
    deps = [
        "//lib/logging",
        "//multiplexer:client",
        "@boost//:asio",
        "@boost//:program_options",
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
        "@boost//:foreach",
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
        "//lib:sha1",
        "//multiplexer:recorder",
        "//multiplexer:server",
        "@boost//:asio",
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
        "@boost//:asio",
        "@boost//:optional",
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
        "//lib/logging",
        "@boost//:foreach",
        "@boost//:program_options",
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
