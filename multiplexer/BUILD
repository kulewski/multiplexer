# The multiplexer, the C++ client library and the Python package, with the
# generated constants and protocol buffer code they share.
load("@pybind11_bazel//:build_defs.bzl", "pybind_extension")
load("//bazel:maybe_strip.bzl", "STRIP_IN_RELEASE", "maybe_strip")

# Generates multiplexer.constants.h and multiplexer_constants.py from the
# rules file selected by //:multiplexer_rules.
cc_binary(
    name = "generate_constants",
    srcs = ["generate_constants.cc"],
    # Tools don't get copts/linkopts from .bazelrc, but they do get flags
    copts = select({
        "//:asan_build": ["-fsanitize=address"],
        "//conditions:default": [],
    }),
    linkopts = select({
        "//:asan_build": ["-fsanitize=address"],
        "//conditions:default": [],
    }),
    visibility = ["//visibility:public"],
    deps = [
        ":config",
        "//lib:sha1",
        "//lib:type_utils",
        "@boost//:foreach",
        "@boost//:lexical_cast",
    ],
)

genrule(
    name = "generate_cc_multiplexer_constants",
    srcs = ["//:multiplexer_rules"],
    outs = ["multiplexer.constants.h"],
    cmd = "$(location :generate_constants) $< $@",
    tools = [":generate_constants"],
)

genrule(
    name = "generate_py_multiplexer_constants",
    srcs = ["//:multiplexer_rules"],
    outs = ["multiplexer_constants.py"],
    cmd = "$(location :generate_constants) $< $@",
    tools = [":generate_constants"],
)

cc_library(
    name = "multiplexer_cc_constants",
    hdrs = [":generate_cc_multiplexer_constants"],
    visibility = ["//visibility:public"],
    deps = ["@boost//:cstdint"],
)

py_library(
    name = "multiplexer_constants",
    srcs = [":generate_py_multiplexer_constants"],
    imports = [".."],
    visibility = ["//visibility:public"],
)

genrule(
    name = "multiplexer_proto",
    srcs = [
        "Multiplexer.proto",
        "//lib/logging:Logging.proto",
    ],
    outs = [
        "Multiplexer.pb.cc",
        "Multiplexer.pb.h",
        "Multiplexer_pb2.py",
    ],
    # The include root and the output root are this repository's root, which
    # is "." when we are the main workspace and "external/mx" when we are
    # consumed as @mx; both are derived from where this package sits.
    cmd = "root=$(location Multiplexer.proto); root=$${root%multiplexer/Multiplexer.proto}; root=$${root%/}; " +
          "$(location //:protoc) -I $${root:-.} --cpp_out=$(RULEDIR)/.. --python_out=$(RULEDIR)/.. $(SRCS)",
    tools = ["//:protoc"],
)

genrule(
    name = "recording_proto",
    srcs = ["Recording.proto"],
    outs = [
        "Recording.pb.cc",
        "Recording.pb.h",
        "Recording_pb2.py",
    ],
    cmd = "root=$(location Recording.proto); root=$${root%multiplexer/Recording.proto}; root=$${root%/}; " +
          "$(location //:protoc) -I $${root:-.} --cpp_out=$(RULEDIR)/.. --python_out=$(RULEDIR)/.. $(SRCS)",
    tools = ["//:protoc"],
)

cc_library(
    name = "recording_cc_proto",
    srcs = ["Recording.pb.cc"],
    hdrs = ["Recording.pb.h"],
    visibility = ["//visibility:public"],
    deps = ["//:protobuf_runtime"],
)

py_library(
    name = "recording_py_proto",
    srcs = ["Recording_pb2.py"],
    imports = [".."],
    visibility = ["//visibility:public"],
)

# Reads a --record file from Python; `bazel run //multiplexer:dump_recording -- FILE` prints one.
py_library(
    name = "recording",
    srcs = ["recording.py"],
    imports = [".."],
    visibility = ["//visibility:public"],
    deps = [
        ":multiplexer_constants",
        ":mxclient",
        ":recording_py_proto",
    ],
)

py_binary(
    name = "dump_recording",
    srcs = ["recording.py"],
    main = "recording.py",
    visibility = ["//visibility:public"],
    deps = [":recording"],
)

# Writes the --record file; see Recording.proto.
cc_library(
    name = "recorder",
    srcs = ["recorder.cc"],
    hdrs = ["recorder.h"],
    visibility = ["//mxcontrol:__pkg__"],
    deps = [
        ":multiplexer_cc_proto",
        ":recording_cc_proto",
        "//lib/logging",
        "//lib/protobuf:stream",
    ],
)

cc_library(
    name = "multiplexer_cc_proto",
    srcs = ["Multiplexer.pb.cc"],
    hdrs = ["Multiplexer.pb.h"],
    visibility = ["//visibility:public"],
    deps = [
        "//:protobuf_runtime",
        "//lib/logging:logging_cc_proto",
    ],
)

py_library(
    name = "multiplexer_py_proto",
    srcs = ["Multiplexer_pb2.py"],
    imports = [".."],
    visibility = ["//visibility:public"],
    deps = ["//lib/logging:logging_py_proto"],
)

cc_library(
    name = "defaults",
    hdrs = ["defaults.h"],
    visibility = ["//multiplexer/io:__pkg__"],
)

cc_library(
    name = "config",
    hdrs = ["config.h"],
    visibility = ["//mxcontrol:__pkg__"],
    deps = [
        ":multiplexer_cc_proto",
        "//lib:assert",
        "//lib:exception",
        "//lib/logging",
    ],
)

cc_library(
    name = "connections_manager",
    srcs = ["connections_manager.cc"],
    hdrs = ["connections_manager.h"],
    deps = [
        ":config",
        ":multiplexer_cc_constants",
        "//lib:random",
        "//lib:thread_checker",
        "//multiplexer/io:raw_message",
        "@boost//:asio",
        "@boost//:random",
        "@boost//:shared_ptr",
        "@boost//:smart_ptr",
    ],
)

cc_library(
    name = "threaded_client",
    srcs = ["threaded_client.cc"],
    hdrs = ["threaded_client.h"],
    visibility = ["//visibility:public"],
    deps = [
        ":basic_client",
        ":client",
        ":defaults",
        ":multiplexer_cc_constants",
        ":multiplexer_cc_proto",
        "//lib:mutex",
        "//lib:random",
        "//lib:thread_checker",
        "//lib/logging",
        "@boost//:asio",
        "@boost//:date_time",
    ],
)

cc_library(
    name = "in_process_multiplexer",
    testonly = True,
    hdrs = ["in_process_multiplexer.h"],
    visibility = ["//multiplexer:__subpackages__"],
    deps = [":server"],
)

cc_test(
    name = "fork_test",
    size = "small",
    srcs = ["fork_test.cc"],
    data = ["//:multiplexer.rules"],
    deps = [
        ":client",
        ":in_process_multiplexer",
        ":multiplexer_cc_constants",
        ":threaded_client",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "threaded_client_test",
    size = "small",
    srcs = ["threaded_client_test.cc"],
    data = ["//:multiplexer.rules"],
    deps = [
        ":client",
        ":in_process_multiplexer",
        ":multiplexer_cc_constants",
        ":threaded_client",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "recorder_test",
    size = "small",
    srcs = ["recorder_test.cc"],
    deps = [
        ":recorder",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "soak_test",
    size = "small",
    srcs = ["soak_test.cc"],
    data = ["//:multiplexer.rules"],
    deps = [
        ":multiplexer_cc_constants",
        ":server",
        ":threaded_client",
        "//lib:memory",
        "//multiplexer/backend:base_multiplexer_server",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "basic_client",
    srcs = ["basic_client.cc"],
    hdrs = ["basic_client.h"],
    deps = [
        ":connections_manager",
        ":multiplexer_cc_constants",
        "//lib:fork",
        "//lib:spanset",
        "//lib:timer",
        "//lib:triple",
        "//lib/logging",
        "//multiplexer/io:connection",
        "@boost//:asio",
        "@boost//:date_time",
        "@boost//:foreach",
        "@boost//:tribool",
    ],
)

cc_library(
    name = "client",
    srcs = ["client.cc"],
    hdrs = ["client.h"],
    visibility = ["//visibility:public"],
    deps = [
        ":basic_client",
        ":multiplexer_cc_proto",
        "//lib:vector",
        "@boost//:asio",
        "@boost//:shared_ptr",
        "@boost//:smart_ptr",
        "@boost//:tribool",
    ],
)

cc_library(
    name = "server",
    srcs = ["server.cc"],
    hdrs = ["server.h"],
    visibility = ["//mxcontrol:__pkg__"],
    deps = [
        ":connections_manager",
        ":multiplexer_cc_proto",
        ":recorder",
        "//lib:functors",
        "//lib:memory",
        "//lib/logging",
        "//multiplexer/io:connection",
        "//multiplexer/mxlog:cc_type_id_constants",
        "@boost//:asio",
        "@boost//:foreach",
        "@boost//:random",
    ],
)

# Python side: the pybind11 module and the pure-Python client library.
pybind_extension(
    name = "_native_with_debug_symbols",
    srcs = ["_native.cc"],
    deps = [
        ":client",
        ":threaded_client",
        "//lib:memory",
        "//lib:type_utils",
        "//lib/logging",
    ],
)

maybe_strip(
    name = "_native.so",
    binary = ":_native_with_debug_symbols.so",
    strip = STRIP_IN_RELEASE,
)

py_library(
    name = "_native",
    data = [":_native.so"],
    imports = [".."],
    visibility = ["//multiplexer/mxlog:__pkg__"],
)

py_library(
    name = "release",
    srcs = ["release.py"],
    imports = [".."],
    visibility = ["//multiplexer/mxlog:__pkg__"],
)

py_library(
    name = "protocolbuffers",
    srcs = ["protocolbuffers.py"],
    imports = [".."],
    deps = [":multiplexer_py_proto"],
)

py_library(
    name = "mxclient",
    srcs = ["mxclient.py"],
    imports = [".."],
    deps = [
        ":_native",
        ":multiplexer_constants",
        ":protocolbuffers",
        "//multiplexer/mxlog",
    ],
)

py_test(
    name = "leak_test",
    size = "medium",
    srcs = ["leak_test.py"],
    data = [
        "//:multiplexer.rules",
        "//mxcontrol",
    ],
    deps = [
        ":clients",
        ":multiplexer_constants",
        ":servers",
        ":threaded_client_py",
    ],
)

py_test(
    name = "pickle_test",
    size = "small",
    srcs = ["pickle_test.py"],
    data = [
        "//:multiplexer.rules",
        "//mxcontrol",
    ],
    deps = [
        ":clients",
        ":multiplexer_constants",
        ":servers",
        ":threaded_client_py",
    ],
)

py_test(
    name = "fork_py_test",
    size = "small",
    srcs = ["fork_test.py"],
    data = [
        "//:multiplexer.rules",
        "//mxcontrol",
    ],
    main = "fork_test.py",
    deps = [
        ":clients",
        ":multiplexer_constants",
        ":mxclient",
        ":threaded_client_py",
    ],
)

py_test(
    name = "interpreter_exit_test",
    size = "medium",
    srcs = ["interpreter_exit_test.py"],
    data = [
        "//:multiplexer.rules",
        "//mxcontrol",
    ],
    deps = [
        ":clients",
        ":multiplexer_constants",
        ":threaded_client_py",
    ],
)

py_test(
    name = "threaded_client_py_test",
    size = "small",
    srcs = ["threaded_client_py_test.py"],
    data = [
        "//:multiplexer.rules",
        "//mxcontrol",
    ],
    deps = [
        ":clients",
        ":multiplexer_constants",
        ":threaded_client_py",
    ],
)

py_test(
    name = "serve_thread_test",
    size = "small",
    srcs = ["serve_thread_test.py"],
    data = [
        "//:multiplexer.rules",
        "//mxcontrol",
    ],
    deps = [
        ":multiplexer_constants",
        ":mxclient",
        ":servers",
    ],
)

py_test(
    name = "no_connection_test",
    size = "small",
    srcs = ["no_connection_test.py"],
    deps = [":mxclient"],
)

py_test(
    name = "mxlog_test",
    size = "small",
    srcs = ["mxlog_test.py"],
    deps = ["//multiplexer/mxlog"],
)

py_library(
    name = "threaded_client_py",
    srcs = ["threaded_client.py"],
    imports = [".."],
    visibility = ["//visibility:public"],
    deps = [
        ":multiplexer_constants",
        ":mxclient",
        ":protocolbuffers",
    ],
)

py_library(
    name = "clients",
    srcs = ["clients.py"],
    imports = [".."],
    visibility = ["//visibility:public"],
    deps = [
        ":multiplexer_constants",
        ":mxclient",
    ],
)

# The backend side; depends on clients, so a backend depends on this one.
py_library(
    name = "servers",
    srcs = ["servers.py"],
    imports = [".."],
    visibility = ["//visibility:public"],
    deps = [
        ":clients",
        ":multiplexer_constants",
        ":mxclient",
        "//multiplexer/mxlog",
    ],
)
