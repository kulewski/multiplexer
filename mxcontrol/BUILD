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

# The connections to every replica behind the -M names, for recording and
# rules.
cc_library(
    name = "every_address",
    srcs = ["every_address.cc"],
    hdrs = ["every_address.h"],
    deps = [
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
        "//multiplexer:recorder",
        "//multiplexer:server",
        "@asio",
    ],
    alwayslink = 1,
)

cc_library(
    name = "generate_constants",
    srcs = ["generate_constants.cc"],
    deps = [
        ":task",
        ":tasks_holder",
        "//multiplexer:config",
        "//multiplexer:constants_writer",
    ],
    alwayslink = 1,
)

# The system rules, compiled in for generate_rules: always this
# repository's file, never the one //:multiplexer_rules names.
genrule(
    name = "system_rules_text",
    srcs = ["//:multiplexer.rules"],
    outs = ["system_rules_text.cc"],
    cmd = "sh $(location embed_rules.sh) $(location //:multiplexer.rules) $@",
    tools = ["embed_rules.sh"],
)

cc_library(
    name = "generate_rules",
    srcs = [
        "generate_rules.cc",
        ":system_rules_text",
    ],
    deps = [
        ":task",
        ":tasks_holder",
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

# Visible to the tests: tests/fake_dns builds it with a lookup of its own.
# The class in the header, the one-shot actions in recording_control.cc,
# the ones that keep running in recording_loops.cc.
cc_library(
    name = "recording_control",
    srcs = [
        "recording_control.cc",
        "recording_control.h",
        "recording_loops.cc",
    ],
    visibility = ["//tests:__subpackages__"],
    deps = [
        ":every_address",
        ":task",
        ":tasks_holder",
        "//lib:seconds",
        "//lib/protobuf:stream",
        "//multiplexer:client",
        "//multiplexer:multiplexer_cc_proto",
        "//multiplexer:recording_cc_proto",
        "@asio",
    ],
    alwayslink = 1,
)

cc_library(
    name = "rules_control",
    srcs = ["rules_control.cc"],
    deps = [
        ":every_address",
        ":task",
        ":tasks_holder",
        "//lib:seconds",
        "//multiplexer:client",
        "//multiplexer:multiplexer_cc_proto",
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
        "//multiplexer:threaded_client",
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
    data = [
        "//:multiplexer.rules",
        "//:multiplexer_rules",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":driver",
        ":dump_recording",
        ":generate_constants",
        ":generate_rules",
        ":help",
        ":receive_logs",
        ":recording_control",
        ":rules_control",
        ":start_multiplexer_server",
        ":stream_logs",
        "//lib:core_dump",
        "//lib:program",
    ],
)

py_test(
    name = "verbosity_default_test",
    size = "small",
    srcs = ["verbosity_default_test.py"],
    data = [":mxcontrol"],
    deps = ["//multiplexer/testing"],
)

py_test(
    name = "help_test",
    size = "small",
    srcs = ["help_test.py"],
    data = [":mxcontrol"],
    deps = ["//multiplexer/testing"],
)

py_test(
    name = "generate_rules_test",
    size = "small",
    srcs = ["generate_rules_test.py"],
    data = [
        ":mxcontrol",
        "//:multiplexer.rules",
        "//tests:testing.rules",
    ],
    deps = ["//multiplexer/testing"],
)

py_test(
    name = "rules_check_interval_test",
    size = "small",
    srcs = ["rules_check_interval_test.py"],
    data = [
        ":mxcontrol",
        "//tests:testing.rules",
    ],
    deps = ["//multiplexer/testing"],
)

py_test(
    name = "log_streamer_type_test",
    size = "small",
    srcs = ["log_streamer_type_test.py"],
    data = ["//tests:testing.rules"],
    deps = [
        "//multiplexer:multiplexer_constants",
        "//multiplexer/testing",
    ],
)

py_test(
    name = "answer_deadline_test",
    size = "medium",
    srcs = ["answer_deadline_test.py"],
    data = [":mxcontrol"],
    deps = [
        "//multiplexer:multiplexer_constants",
        "//multiplexer:multiplexer_py_proto",
        "//multiplexer/testing",
    ],
)

py_test(
    name = "streamlogs_test",
    size = "medium",
    srcs = ["streamlogs_test.py"],
    data = [
        ":mxcontrol",
        "//tests:testing.rules",
    ],
    deps = [
        "//multiplexer:multiplexer_constants",
        "//multiplexer:multiplexer_py_proto",
        "//multiplexer/testing",
    ],
)

py_test(
    name = "stream_readers_test",
    size = "small",
    srcs = ["stream_readers_test.py"],
    data = [
        ":mxcontrol",
        "//tests:testing.rules",
    ],
    deps = [
        "//multiplexer:multiplexer_py_proto",
        "//multiplexer:recording_py_proto",
        "//multiplexer/testing",
    ],
)

py_test(
    name = "generate_constants_test",
    size = "small",
    srcs = ["generate_constants_test.py"],
    data = [
        ":mxcontrol",
        "//multiplexer:multiplexer.constants.h",
        "//multiplexer:multiplexer_constants.py",
        "//multiplexer:multiplexer_constants.pyi",
        "//tests:testing.rules",
    ],
    deps = ["//multiplexer/testing"],
)

# The same binary linked statically: one file that runs on any Linux and
# in an empty container, for the release asset and the image. The one
# thing static glibc cannot do everywhere is resolve host names, which
# needs the host's own name-service modules: the multiplexer listens on an
# address and never resolves, and the client subcommands take addresses
# too. Built only when named; the release workflow builds it with
# --config=release, which strips it as it strips //mxcontrol.
maybe_strip_cc_binary(
    name = "mxcontrol_static",
    srcs = ["main.cc"],
    data = [
        "//:multiplexer.rules",
        "//:multiplexer_rules",
    ],
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
        ":generate_constants",
        ":generate_rules",
        ":help",
        ":receive_logs",
        ":recording_control",
        ":rules_control",
        ":start_multiplexer_server",
        ":stream_logs",
        "//lib:core_dump",
        "//lib:program",
    ],
)
