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
        "//lib:fingerprint",
        "//lib:type_utils",
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

# The stub of the native extension for type checkers, from the built module;
# manual because it needs the PyPI tool in tools/BUILD:
#   bazel build //multiplexer:_native_pyi
genrule(
    name = "_native_pyi",
    srcs = [":_native.so"],
    outs = ["_native.pyi"],
    cmd = "$(location //tools:native_stub) $(location :_native.so) $@",
    tags = ["manual"],
    tools = ["//tools:native_stub"],
)

# The type ids of log entries, from //multiplexer/mxlog:type_id_constants.txt:
# the text is the Python module verbatim; the stub and the C++ header come
# from the generator. The module lives here rather than in mxlog so that a
# type checker finds its source in bazel-bin: this package has no
# __init__.py and merges across roots, a regular package does not.
genrule(
    name = "generate_py_type_id_constants",
    srcs = ["//multiplexer/mxlog:type_id_constants.txt"],
    outs = ["type_id_constants.py"],
    cmd = "cp $< $@",
)

genrule(
    name = "generate_pyi_type_id_constants",
    srcs = ["//multiplexer/mxlog:type_id_constants.txt"],
    outs = ["type_id_constants.pyi"],
    cmd = "$(location //multiplexer/mxlog:gen_type_id_constants) $< $@",
    tools = ["//multiplexer/mxlog:gen_type_id_constants"],
)

py_library(
    name = "py_type_id_constants",
    srcs = ["type_id_constants.py"],
    imports = [".."],
    visibility = ["//multiplexer/mxlog:__pkg__"],
)

# The Event message the test roles report with, in both languages; here for
# the same reason as the type ids above.
genrule(
    name = "events_proto",
    srcs = ["events.proto"],
    outs = [
        "events.pb.cc",
        "events.pb.h",
        "events_pb2.py",
        "events_pb2.pyi",
    ],
    cmd = "root=$(location events.proto); root=$${root%multiplexer/events.proto}; root=$${root%/}; " +
          "$(location //:protoc) -I $${root:-.} --cpp_out=$(RULEDIR)/.. --python_out=$(RULEDIR)/.. --pyi_out=$(RULEDIR)/.. $(SRCS)",
    tools = ["//:protoc"],
)

cc_library(
    name = "events_cc_proto",
    srcs = ["events.pb.cc"],
    hdrs = ["events.pb.h"],
    visibility = ["//visibility:public"],
    deps = ["//:protobuf_runtime"],
)

py_library(
    name = "events_py_proto",
    srcs = ["events_pb2.py"],
    imports = [".."],
    visibility = ["//visibility:public"],
)

# The stubs type checkers read; bazel-bin is the stubPath in pyproject.toml,
# so they are found wherever the checker runs after a build.
genrule(
    name = "generate_pyi_multiplexer_constants",
    srcs = ["//:multiplexer_rules"],
    outs = ["multiplexer_constants.pyi"],
    cmd = "$(location :generate_constants) $< $@",
    tools = [":generate_constants"],
)

cc_library(
    name = "multiplexer_cc_constants",
    hdrs = [":generate_cc_multiplexer_constants"],
    visibility = ["//visibility:public"],
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
        "Multiplexer_pb2.pyi",
    ],
    # The include root and the output root are this repository's root, which
    # is "." when we are the main workspace and "external/mx" when we are
    # consumed as @mx; both are derived from where this package sits.
    cmd = "root=$(location Multiplexer.proto); root=$${root%multiplexer/Multiplexer.proto}; root=$${root%/}; " +
          "$(location //:protoc) -I $${root:-.} --cpp_out=$(RULEDIR)/.. --python_out=$(RULEDIR)/.. --pyi_out=$(RULEDIR)/.. $(SRCS) && " +
          # multiplexer/protocolbuffers.py adds `from_`, the `from` field under a name Python allows.
          "sed -i 's/^class MultiplexerMessage(.*/&\\n    from_: int/' $(location Multiplexer_pb2.pyi)",
    tools = ["//:protoc"],
)

genrule(
    name = "recording_proto",
    srcs = ["Recording.proto"],
    outs = [
        "Recording.pb.cc",
        "Recording.pb.h",
        "Recording_pb2.py",
        "Recording_pb2.pyi",
    ],
    cmd = "root=$(location Recording.proto); root=$${root%multiplexer/Recording.proto}; root=$${root%/}; " +
          "$(location //:protoc) -I $${root:-.} --cpp_out=$(RULEDIR)/.. --python_out=$(RULEDIR)/.. --pyi_out=$(RULEDIR)/.. $(SRCS)",
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
        "@asio",
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
        "@asio",
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
    name = "connect_by_name_test",
    size = "small",
    srcs = ["connect_by_name_test.cc"],
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
        "//multiplexer/backend:base_threaded_multiplexer_server",
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
        "//lib:mutex",
        "//lib:spanset",
        "//lib:timer",
        "//lib:triple",
        "//lib/logging",
        "//multiplexer/io:connection",
        "@asio",
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
        "@asio",
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
        "@asio",
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
        ":threaded_server",
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

# The asyncio face of the threaded client; see aio.py.
py_library(
    name = "aio",
    srcs = ["aio.py"],
    imports = [".."],
    visibility = ["//visibility:public"],
    deps = [
        ":mxclient",
        ":threaded_client_py",
    ],
)

py_test(
    name = "aio_test",
    size = "medium",
    srcs = ["aio_test.py"],
    deps = [
        ":aio",
        ":multiplexer_constants",
        ":threaded_client_py",
        "//multiplexer/testing",
    ],
)

py_test(
    name = "threaded_lanes_test",
    size = "medium",
    srcs = ["threaded_lanes_test.py"],
    deps = [
        ":aio",
        ":multiplexer_constants",
        ":threaded_client_py",
        "//multiplexer/testing",
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
    name = "threaded_server",
    srcs = ["threaded_server.py"],
    imports = [".."],
    visibility = ["//visibility:public"],
    deps = [
        ":multiplexer_constants",
        ":mxclient",
        ":servers",
        ":threaded_client_py",
        "//multiplexer/mxlog",
    ],
)

py_test(
    name = "threaded_server_test",
    size = "medium",
    srcs = ["threaded_server_test.py"],
    deps = [
        ":clients",
        ":multiplexer_constants",
        ":threaded_client_py",
        ":threaded_server",
        "//multiplexer/testing",
    ],
)

# About two minutes: a request longer than the multiplexer's drop interval.
py_test(
    name = "threaded_server_long_test",
    size = "large",
    srcs = ["threaded_server_long_test.py"],
    tags = ["slow"],
    deps = [
        ":multiplexer_constants",
        ":threaded_client_py",
        ":threaded_server",
        "//multiplexer/testing",
    ],
)

py_test(
    name = "connect_by_name_test_py",
    size = "small",
    srcs = ["connect_by_name_test.py"],
    main = "connect_by_name_test.py",
    deps = [
        ":multiplexer_constants",
        ":threaded_client_py",
        ":threaded_server",
        "//multiplexer/testing",
    ],
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
