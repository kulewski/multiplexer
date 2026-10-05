# The multiplexer, the C++ client library and the Python package, with the
# generated constants and protocol buffer code they share.
load("//bazel:maybe_strip.bzl", "STRIP_IN_RELEASE", "maybe_strip")
load("//bazel:pybind.bzl", "pybind_extension")

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
        ":constants_writer",
        "//lib:type_utils",
    ],
)

# The writer the tool above and `mxcontrol generate_constants` share.
cc_library(
    name = "constants_writer",
    hdrs = ["constants_writer.h"],
    visibility = ["//mxcontrol:__pkg__"],
    deps = [
        "//lib:assert",
        "//lib:exception",
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
    visibility = ["//mxcontrol:__pkg__"],  # the subcommand's test compares with these
)

genrule(
    name = "generate_py_multiplexer_constants",
    srcs = ["//:multiplexer_rules"],
    outs = ["multiplexer_constants.py"],
    cmd = "$(location :generate_constants) $< $@",
    tools = [":generate_constants"],
    visibility = ["//mxcontrol:__pkg__"],  # the subcommand's test compares with these
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
    visibility = ["//mxcontrol:__pkg__"],  # the subcommand's test compares with these
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

# LogEntry, which the C++ logging writes and LOGS_STREAM carries: a module
# of the top-level package in Python, multiplexer.Logging_pb2, where a type
# checker finds its stub as it finds every generated module's.
genrule(
    name = "logging_proto",
    srcs = ["Logging.proto"],
    outs = [
        "Logging.pb.cc",
        "Logging.pb.h",
        "Logging_pb2.py",
        "Logging_pb2.pyi",
    ],
    cmd = "root=$(location Logging.proto); root=$${root%multiplexer/Logging.proto}; root=$${root%/}; " +
          "$(location //:protoc) -I $${root:-.} --cpp_out=$(RULEDIR)/.. --python_out=$(RULEDIR)/.. --pyi_out=$(RULEDIR)/.. $(SRCS)",
    tools = ["//:protoc"],
)

cc_library(
    name = "logging_cc_proto",
    srcs = ["Logging.pb.cc"],
    hdrs = ["Logging.pb.h"],
    visibility = ["//visibility:public"],
    deps = ["//:protobuf_runtime"],
)

py_library(
    name = "logging_py_proto",
    srcs = ["Logging_pb2.py"],
    imports = [".."],
    visibility = ["//visibility:public"],
)

genrule(
    name = "multiplexer_proto",
    srcs = [
        "Multiplexer.proto",
        "Logging.proto",
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
          # multiplexer/protocolbuffers.py keeps `from_`, a read-only alias of `sender`, the field named
          # `from` before 2.4.0.
          "sed -i 's/^class MultiplexerMessage(.*/&\\n    @property\\n    def from_(self) -> int: ...  " +
          "# deprecated: the field is sender/' $(location Multiplexer_pb2.pyi)",
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
        ":logging_cc_proto",
        "//:protobuf_runtime",
    ],
)

py_library(
    name = "multiplexer_py_proto",
    srcs = ["Multiplexer_pb2.py"],
    imports = [".."],
    visibility = ["//visibility:public"],
    deps = [":logging_py_proto"],
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

# Lines about dropped messages, at most about two per kind and second.
cc_library(
    name = "log_summary",
    srcs = ["log_summary.cc"],
    hdrs = ["log_summary.h"],
    deps = [
        "//lib:seconds",
        "//lib/logging",
        "@asio",
    ],
)

cc_test(
    name = "log_summary_test",
    size = "small",
    srcs = ["log_summary_test.cc"],
    deps = [
        ":log_summary",
        "//lib/logging",
        "@asio",
        "@com_google_googletest//:gtest_main",
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
    srcs = [
        "threaded_client.cc",
        "threaded_client_core.h",
    ],
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
        "//lib:seconds",
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
    data = ["//tests:testing.rules"],
    linkopts = ["-ldl"],  # dlsym, for the pthread_mutex_lock wrapper, in libdl before glibc 2.34
    deps = [
        ":client",
        ":in_process_multiplexer",
        ":multiplexer_cc_constants",
        ":threaded_client",
        "//lib:timer",
        "//multiplexer/backend:base_threaded_multiplexer_server",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "routing_index_test",
    size = "small",
    srcs = ["routing_index_test.cc"],
    data = ["//tests:testing.rules"],
    deps = [
        ":client",
        ":in_process_multiplexer",
        ":multiplexer_cc_constants",
        ":multiplexer_cc_proto",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "threaded_client_test",
    size = "medium",
    srcs = ["threaded_client_test.cc"],
    data = ["//tests:testing.rules"],
    deps = [
        ":client",
        ":in_process_multiplexer",
        ":multiplexer_cc_constants",
        ":threaded_client",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "negative_timeout_test",
    size = "small",
    srcs = ["negative_timeout_test.cc"],
    data = ["//tests:testing.rules"],
    deps = [
        ":client",
        ":in_process_multiplexer",
        ":multiplexer_cc_constants",
        ":threaded_client",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "zero_nan_timeout_test",
    size = "small",
    srcs = ["zero_nan_timeout_test.cc"],
    data = ["//tests:testing.rules"],
    deps = [
        ":client",
        ":in_process_multiplexer",
        ":multiplexer_cc_constants",
        ":threaded_client",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "drop_names_test",
    size = "small",
    srcs = ["drop_names_test.cc"],
    deps = [
        ":client",
        ":multiplexer_cc_constants",
        ":threaded_client",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "connect_deadline_test",
    size = "small",
    srcs = ["connect_deadline_test.cc"],
    data = ["//tests:testing.rules"],
    deps = [
        ":client",
        ":in_process_multiplexer",
        ":multiplexer_cc_constants",
        ":threaded_client",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "oversized_message_test",
    size = "small",
    srcs = ["oversized_message_test.cc"],
    deps = [
        ":multiplexer_cc_proto",
        "//multiplexer/io:raw_message",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "server_release_test",
    size = "small",
    srcs = ["server_release_test.cc"],
    data = ["//tests:testing.rules"],
    deps = [
        ":server",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "close_on_exec_test",
    size = "small",
    srcs = ["close_on_exec_test.cc"],
    data = ["//tests:testing.rules"],
    deps = [
        ":client",
        ":in_process_multiplexer",
        ":multiplexer_cc_constants",
        ":threaded_client",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "duplicate_after_drop_test",
    size = "small",
    srcs = ["duplicate_after_drop_test.cc"],
    data = ["//tests:testing.rules"],
    deps = [
        ":client",
        ":in_process_multiplexer",
        ":multiplexer_cc_constants",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "disconnected_record_test",
    size = "small",
    srcs = ["disconnected_record_test.cc"],
    data = ["//tests:testing.rules"],
    deps = [
        ":client",
        ":connections_manager",
        ":in_process_multiplexer",
        ":multiplexer_cc_constants",
        "//lib/protobuf:stream",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "live_id_test",
    size = "small",
    srcs = ["live_id_test.cc"],
    data = ["//tests:testing.rules"],
    deps = [
        ":client",
        ":connections_manager",
        ":in_process_multiplexer",
        ":multiplexer_cc_constants",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "connect_again_test",
    size = "small",
    srcs = ["connect_again_test.cc"],
    data = ["//tests:testing.rules"],
    deps = [
        ":client",
        ":in_process_multiplexer",
        ":multiplexer_cc_constants",
        ":threaded_client",
        "@com_google_googletest//:gtest_main",
    ],
)

py_test(
    name = "connect_again_py_test",
    size = "small",
    srcs = ["connect_again_test.py"],
    main = "connect_again_test.py",
    deps = [
        ":clients",
        ":multiplexer_constants",
        ":threaded_client_py",
        "//multiplexer/testing",
    ],
)

cc_test(
    name = "disconnect_test",
    size = "small",
    srcs = ["disconnect_test.cc"],
    data = ["//tests:testing.rules"],
    deps = [
        ":client",
        ":in_process_multiplexer",
        ":multiplexer_cc_constants",
        ":threaded_client",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "query_stages_test",
    size = "medium",
    srcs = ["query_stages_test.cc"],
    deps = [
        ":client",
        ":in_process_multiplexer",
        ":multiplexer_cc_constants",
        ":threaded_client",
        "@com_google_googletest//:gtest_main",
    ],
)

py_test(
    name = "query_stages_py_test",
    size = "medium",
    srcs = ["query_stages_test.py"],
    main = "query_stages_test.py",
    deps = [
        ":clients",
        ":multiplexer_constants",
        ":multiplexer_py_proto",
        "//multiplexer/testing",
    ],
)

cc_test(
    name = "foreign_lane_test",
    size = "small",
    srcs = ["foreign_lane_test.cc"],
    data = ["//tests:testing.rules"],
    deps = [
        ":client",
        ":in_process_multiplexer",
        ":multiplexer_cc_constants",
        ":threaded_client",
        "@com_google_googletest//:gtest_main",
    ],
)

py_test(
    name = "foreign_lane_py_test",
    size = "small",
    srcs = ["foreign_lane_test.py"],
    main = "foreign_lane_test.py",
    deps = [
        ":clients",
        ":multiplexer_constants",
        ":threaded_client_py",
        "//multiplexer/testing",
    ],
)

cc_test(
    name = "connect_by_name_test",
    size = "small",
    srcs = ["connect_by_name_test.cc"],
    data = ["//tests:testing.rules"],
    deps = [
        ":client",
        ":in_process_multiplexer",
        ":multiplexer_cc_constants",
        ":threaded_client",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "client_names_test",
    size = "small",
    srcs = ["client_names_test.cc"],
    deps = [
        ":client",
        "@com_google_googletest//:gtest_main",
    ],
)

# A global operator new of its own counts the multiplexer's allocations:
# a binary to itself.
cc_test(
    name = "recording_copies_test",
    size = "small",
    srcs = ["recording_copies_test.cc"],
    data = ["//tests:testing.rules"],
    deps = [
        ":client",
        ":in_process_multiplexer",
        ":multiplexer_cc_constants",
        ":recording_cc_proto",
        "//lib/protobuf:stream",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "recorder_test",
    size = "small",
    srcs = ["recorder_test.cc"],
    deps = [
        ":recorder",
        "//lib/protobuf:stream",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "recording_session_test",
    size = "small",
    srcs = ["recording_session_test.cc"],
    data = ["//tests:testing.rules"],
    deps = [
        ":server",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "peers_file_start_test",
    size = "small",
    srcs = ["peers_file_start_test.cc"],
    data = ["//tests:testing.rules"],
    deps = [
        ":server",
        "@asio",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "rules_read_errors_test",
    size = "small",
    srcs = ["rules_read_errors_test.cc"],
    data = ["//tests:testing.rules"],
    deps = [
        ":server",
        "@asio",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "rules_check_interval_test",
    size = "small",
    srcs = ["rules_check_interval_test.cc"],
    data = ["//tests:testing.rules"],
    deps = [
        ":server",
        "@asio",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "soak_test",
    size = "small",
    srcs = ["soak_test.cc"],
    data = ["//tests:testing.rules"],
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
    name = "descriptor_table",
    srcs = ["descriptor_table.cc"],
    hdrs = ["descriptor_table.h"],
)

cc_test(
    name = "descriptor_table_test",
    size = "small",
    srcs = ["descriptor_table_test.cc"],
    deps = [
        ":descriptor_table",
        "@com_google_googletest//:gtest_main",
    ],
)

# A multiplexer's address as text, host:port or [IPv6 address]:port: for
# every command line that takes one, and for the C++ examples.
cc_library(
    name = "endpoint",
    srcs = ["endpoint.cc"],
    hdrs = ["endpoint.h"],
    visibility = ["//visibility:public"],
)

# Two targets that reach one multiplexer are said once, at WARNING.
cc_test(
    name = "duplicate_target_test",
    size = "small",
    srcs = ["duplicate_target_test.cc"],
    data = ["//tests:testing.rules"],
    deps = [
        ":basic_client",
        ":in_process_multiplexer",
        ":multiplexer_cc_constants",
        "@asio",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "endpoint_test",
    size = "small",
    srcs = ["endpoint_test.cc"],
    deps = [
        ":endpoint",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "basic_client",
    srcs = [
        "basic_client.cc",
        "outbox.cc",
        "outbox.h",
    ],
    hdrs = ["basic_client.h"],
    deps = [
        ":connections_manager",
        ":descriptor_table",
        ":endpoint",
        ":log_summary",
        ":multiplexer_cc_constants",
        "//lib:fork",
        "//lib:mutex",
        "//lib:seconds",
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
        ":endpoint",
        ":multiplexer_cc_proto",
        "//lib:seconds",
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
        ":log_summary",
        ":multiplexer_cc_proto",
        ":recorder",
        "//lib:exception",
        "//lib:fingerprint",
        "//lib:functors",
        "//lib:memory",
        "//lib:seconds",
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
        ":endpoint",
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
    deps = [
        ":multiplexer_py_proto",
        ":recording_py_proto",
    ],
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

# What ends a client or a server: with, shutdown()/close(), and dropping.
py_test(
    name = "lifetime_test",
    size = "small",
    srcs = ["lifetime_test.py"],
    deps = [
        ":clients",
        ":multiplexer_constants",
        ":mxclient",
        ":servers",
        ":threaded_client_py",
        ":threaded_server",
        "//multiplexer/testing",
    ],
)

py_test(
    name = "leak_test",
    size = "medium",
    srcs = ["leak_test.py"],
    data = [
        "//mxcontrol",
        "//tests:testing.rules",
    ],
    deps = [
        ":aio",
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
        "//mxcontrol",
        "//tests:testing.rules",
    ],
    deps = [
        ":clients",
        ":multiplexer_constants",
        ":multiplexer_py_proto",
        ":servers",
        ":threaded_client_py",
    ],
)

py_test(
    name = "fork_py_test",
    size = "small",
    srcs = ["fork_test.py"],
    data = [
        "//mxcontrol",
        "//tests:testing.rules",
    ],
    main = "fork_test.py",
    deps = [
        ":aio",
        ":clients",
        ":multiplexer_constants",
        ":mxclient",
        ":threaded_client_py",
        ":threaded_server",
    ],
)

py_test(
    name = "interpreter_exit_test",
    size = "medium",
    srcs = ["interpreter_exit_test.py"],
    data = [
        "//mxcontrol",
        "//tests:testing.rules",
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
        ":clients",
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
        "//mxcontrol",
        "//tests:testing.rules",
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
        "//mxcontrol",
        "//tests:testing.rules",
    ],
    deps = [
        ":multiplexer_constants",
        ":mxclient",
        ":servers",
    ],
)

# About 12 s: a send with no deadline outlasting DEFAULT_TIMEOUT.
py_test(
    name = "no_deadline_send_test",
    size = "medium",
    srcs = ["no_deadline_send_test.py"],
    data = ["//tests:testing.rules"],
    tags = ["slow"],
    deps = [
        ":clients",
        ":multiplexer_constants",
        ":mxclient",
        "//multiplexer/testing",
    ],
)

# A message sent to ALL just after the start reaches the multiplexer whose
# first connection is still in its handshake, at its welcome.
py_test(
    name = "all_at_start_test",
    size = "small",
    srcs = ["all_at_start_test.py"],
    deps = [
        ":multiplexer_constants",
        ":threaded_client_py",
        "//multiplexer/testing",
    ],
)

py_test(
    name = "server_connect_test",
    size = "small",
    srcs = ["server_connect_test.py"],
    deps = [
        ":multiplexer_constants",
        ":mxclient",
        ":servers",
        ":threaded_client_py",
        ":threaded_server",
        "//multiplexer/testing",
    ],
)

# Preloaded into a child Python by sync_client_destroy_test.py; see the
# source.
cc_binary(
    name = "lookup_join_probe.so",
    testonly = True,
    srcs = ["lookup_join_probe.cc"],
    linkopts = ["-ldl"],  # dlsym, in libdl before glibc 2.34
    linkshared = True,
)

py_test(
    name = "sync_client_destroy_test",
    size = "small",
    srcs = ["sync_client_destroy_test.py"],
    data = [":lookup_join_probe.so"],
    deps = [
        ":clients",
        ":multiplexer_constants",
        "//multiplexer/testing",
    ],
)

py_test(
    name = "no_connection_test",
    size = "small",
    srcs = ["no_connection_test.py"],
    data = ["//tests:testing.rules"],
    deps = [
        ":clients",
        ":mxclient",
        ":servers",
        "//multiplexer/testing",
    ],
)

py_test(
    name = "direct_request_test",
    size = "small",
    srcs = ["direct_request_test.py"],
    deps = [
        ":clients",
        ":multiplexer_constants",
        ":multiplexer_py_proto",
        "//multiplexer/testing",
    ],
)

py_test(
    name = "handler_timeout_test",
    size = "small",
    srcs = ["handler_timeout_test.py"],
    deps = [
        ":multiplexer_constants",
        ":servers",
        "//multiplexer/testing",
    ],
)

py_test(
    name = "no_top_level_lib_test",
    size = "small",
    srcs = ["no_top_level_lib_test.py"],
    deps = [":multiplexer_py_proto"],
)

py_test(
    name = "peers_file_test",
    size = "small",
    srcs = ["peers_file_test.py"],
    deps = [
        ":multiplexer_constants",
        ":multiplexer_py_proto",
        "//multiplexer/testing",
    ],
)

py_test(
    name = "whole_message_test",
    size = "medium",
    srcs = ["whole_message_test.py"],
    deps = [
        ":aio",
        ":clients",
        ":multiplexer_constants",
        ":servers",
        ":threaded_client_py",
        "//multiplexer/testing",
    ],
)

py_test(
    name = "pickle_failures_test",
    size = "small",
    srcs = ["pickle_failures_test.py"],
    deps = [
        ":aio",
        ":clients",
        ":multiplexer_constants",
        ":servers",
        ":threaded_client_py",
        "//multiplexer/testing",
    ],
)

py_test(
    name = "recording_tap_test",
    size = "medium",
    srcs = ["recording_tap_test.py"],
    data = ["//tests:testing.rules"],
    deps = [
        ":clients",
        ":multiplexer_constants",
        ":recording",
        "//multiplexer/testing",
    ],
)

py_test(
    name = "recording_read_test",
    size = "small",
    srcs = ["recording_read_test.py"],
    deps = [
        ":multiplexer_constants",
        ":recording",
    ],
)

py_test(
    name = "frames_at_the_limit_test",
    size = "medium",
    srcs = ["frames_at_the_limit_test.py"],
    deps = [
        ":clients",
        ":multiplexer_constants",
        ":mxclient",
        ":recording",
        ":servers",
        "//multiplexer/testing",
    ],
)

py_test(
    name = "forced_frames_test",
    size = "medium",
    srcs = ["forced_frames_test.py"],
    deps = [
        ":multiplexer_constants",
        ":multiplexer_py_proto",
        ":mxclient",
        "//multiplexer/testing",
    ],
)

py_test(
    name = "full_connection_test",
    size = "medium",
    srcs = ["full_connection_test.py"],
    deps = [
        ":clients",
        ":multiplexer_constants",
        ":mxclient",
        "//multiplexer/testing",
    ],
)

py_test(
    name = "all_types_test",
    size = "small",
    srcs = ["all_types_test.py"],
    deps = [
        ":multiplexer_constants",
        ":multiplexer_py_proto",
        "//multiplexer/testing",
    ],
)

py_test(
    name = "zero_timeout_test",
    size = "small",
    srcs = ["zero_timeout_test.py"],
    deps = [
        ":clients",
        ":multiplexer_constants",
        ":mxclient",
        "//multiplexer/testing",
    ],
)

cc_test(
    name = "flooded_client_test",
    size = "small",
    srcs = ["flooded_client_test.cc"],
    deps = [
        ":client",
        ":multiplexer_cc_constants",
        "@com_google_googletest//:gtest_main",
    ],
)

py_test(
    name = "infinite_timeout_test",
    size = "small",
    srcs = ["infinite_timeout_test.py"],
    deps = [
        ":aio",
        ":clients",
        ":multiplexer_constants",
        ":mxclient",
        ":servers",
        ":threaded_client_py",
        ":threaded_server",
        "//multiplexer/testing",
    ],
)

py_test(
    name = "negative_timeout_py_test",
    size = "small",
    srcs = ["negative_timeout_test.py"],
    main = "negative_timeout_test.py",
    deps = [
        ":aio",
        ":clients",
        ":multiplexer_constants",
        ":multiplexer_py_proto",
        ":mxclient",
        ":recording",
        ":servers",
        ":threaded_client_py",
        ":threaded_server",
        "//multiplexer/testing",
    ],
)

py_test(
    name = "monotonic_clock_test",
    size = "small",
    srcs = ["monotonic_clock_test.py"],
    deps = [
        ":multiplexer_constants",
        ":multiplexer_py_proto",
        ":mxclient",
        ":servers",
        ":threaded_server",
        "//multiplexer/testing",
    ],
)

# serve_forever(stall_seconds): the handling and periodic_task() watched,
# never the poll's wait, the watchdog cancelled however they end.
py_test(
    name = "stall_seconds_test",
    size = "small",
    srcs = ["stall_seconds_test.py"],
    deps = [
        ":multiplexer_constants",
        ":multiplexer_py_proto",
        ":mxclient",
        ":servers",
        ":threaded_client_py",
        "//multiplexer/testing",
    ],
)

py_test(
    name = "zero_nan_timeout_py_test",
    size = "small",
    srcs = ["zero_nan_timeout_test.py"],
    main = "zero_nan_timeout_test.py",
    deps = [
        ":aio",
        ":clients",
        ":multiplexer_constants",
        ":multiplexer_py_proto",
        ":mxclient",
        ":recording",
        ":servers",
        ":threaded_client_py",
        ":threaded_server",
        "//multiplexer/testing",
    ],
)

py_test(
    name = "disconnect_py_test",
    size = "small",
    srcs = ["disconnect_test.py"],
    main = "disconnect_test.py",
    deps = [
        ":aio",
        ":clients",
        ":multiplexer_constants",
        ":mxclient",
        ":threaded_client_py",
        "//multiplexer/testing",
    ],
)

py_test(
    name = "drop_lines_test",
    size = "medium",
    srcs = ["drop_lines_test.py"],
    deps = [
        ":clients",
        ":multiplexer_constants",
        ":mxclient",
        ":threaded_client_py",
        "//multiplexer/testing",
    ],
)

py_test(
    name = "keepalive_test",
    size = "small",
    srcs = ["keepalive_test.py"],
    deps = [
        ":multiplexer_constants",
        ":mxclient",
        "//multiplexer/testing",
    ],
)

# A synchronous client keeps its connection through a stall of its
# multiplexer, whose frames since then wait for its next call; about 110 s.
py_test(
    name = "stalled_multiplexer_test",
    size = "large",
    srcs = ["stalled_multiplexer_test.py"],
    tags = ["slow"],
    deps = [
        ":clients",
        ":multiplexer_constants",
        ":mxclient",
        "//multiplexer/testing",
    ],
)

# A welcome long after the connection opened still registers; about 95 s.
py_test(
    name = "late_welcome_test",
    size = "large",
    srcs = ["late_welcome_test.py"],
    tags = ["slow"],
    deps = [
        ":multiplexer_constants",
        ":mxclient",
        "//multiplexer/testing",
    ],
)

py_test(
    name = "graceful_stop_test",
    size = "medium",
    srcs = ["graceful_stop_test.py"],
    deps = [
        ":multiplexer_constants",
        ":multiplexer_py_proto",
        "//multiplexer/testing",
    ],
)

py_test(
    name = "log_reader_gone_test",
    size = "small",
    srcs = ["log_reader_gone_test.py"],
    data = ["//tests:testing.rules"],
    deps = [
        ":clients",
        ":multiplexer_constants",
        "//multiplexer/testing",
    ],
)

py_test(
    name = "unrouted_test",
    size = "medium",
    srcs = ["unrouted_test.py"],
    deps = [
        ":multiplexer_constants",
        ":multiplexer_py_proto",
        ":threaded_client_py",
        "//multiplexer/testing",
    ],
)

py_test(
    name = "mxlog_test",
    size = "small",
    srcs = ["mxlog_test.py"],
    deps = [
        "//multiplexer/mxlog",
        "//multiplexer/mxlog:streaming",
    ],
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
    name = "servers_drain_test",
    size = "medium",
    srcs = ["servers_drain_test.py"],
    data = ["//tests:testing.rules"],
    deps = [
        ":clients",
        ":multiplexer_constants",
        ":multiplexer_py_proto",
        ":servers",
        "//multiplexer/testing",
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
    name = "plain_delivery_error_test",
    size = "small",
    srcs = ["plain_delivery_error_test.py"],
    data = ["//tests:testing.rules"],
    deps = [
        ":multiplexer_constants",
        "//multiplexer/testing",
    ],
)

py_test(
    name = "reserved_types_test",
    size = "small",
    srcs = ["reserved_types_test.py"],
    data = ["//tests:testing.rules"],
    deps = [
        ":multiplexer_constants",
        ":multiplexer_py_proto",
        ":recording",
        ":recording_py_proto",
        "//multiplexer/testing",
    ],
)

py_test(
    name = "unrouted_records_test",
    size = "medium",
    srcs = ["unrouted_records_test.py"],
    data = ["//tests:testing.rules"],
    deps = [
        ":multiplexer_constants",
        ":multiplexer_py_proto",
        ":recording",
        ":recording_py_proto",
        "//multiplexer/testing",
    ],
)

py_test(
    name = "callback_errors_test",
    size = "small",
    srcs = ["callback_errors_test.py"],
    deps = [
        ":multiplexer_constants",
        ":threaded_client_py",
    ],
)

py_test(
    name = "sender_field_test",
    size = "small",
    srcs = ["sender_field_test.py"],
    deps = [
        ":clients",
        ":events_py_proto",
        ":multiplexer_constants",
        ":multiplexer_py_proto",
        ":recording_py_proto",
        ":threaded_client_py",
    ],
)

py_test(
    name = "drop_reports_test",
    size = "medium",
    srcs = ["drop_reports_test.py"],
    data = ["//tests:testing.rules"],
    deps = [
        ":aio",
        ":clients",
        ":multiplexer_constants",
        ":mxclient",
        ":recording",
        ":recording_py_proto",
        ":threaded_client_py",
        "//multiplexer/testing",
    ],
)

py_test(
    name = "end_calls_test",
    size = "medium",
    srcs = ["end_calls_test.py"],
    data = ["//tests:testing.rules"],
    deps = [
        ":aio",
        ":clients",
        ":multiplexer_constants",
        ":mxclient",
        ":threaded_client_py",
        "//multiplexer/testing",
    ],
)

py_test(
    name = "outage_test",
    size = "medium",
    srcs = ["outage_test.py"],
    data = ["//tests:testing.rules"],
    deps = [
        ":clients",
        ":multiplexer_constants",
        ":mxclient",
        ":threaded_client_py",
        "//multiplexer/testing",
    ],
)

py_test(
    name = "query_acknowledged_test",
    size = "medium",
    srcs = ["query_acknowledged_test.py"],
    data = ["//tests:testing.rules"],
    deps = [
        ":aio",
        ":clients",
        ":multiplexer_constants",
        ":multiplexer_py_proto",
        ":threaded_client_py",
        "//multiplexer/testing",
    ],
)

py_test(
    name = "report_loop_test",
    size = "medium",
    srcs = ["report_loop_test.py"],
    data = ["//tests:testing.rules"],
    deps = [
        ":multiplexer_constants",
        ":servers",
        ":threaded_server",
        "//multiplexer/testing",
    ],
)

py_test(
    name = "send_callbacks_test",
    size = "medium",
    srcs = ["send_callbacks_test.py"],
    data = ["//tests:testing.rules"],
    deps = [
        ":clients",
        ":multiplexer_constants",
        ":mxclient",
        ":threaded_client_py",
        "//multiplexer/testing",
    ],
)

py_test(
    name = "server_replies_test",
    size = "medium",
    srcs = ["server_replies_test.py"],
    data = ["//tests:testing.rules"],
    deps = [
        ":aio",
        ":clients",
        ":multiplexer_constants",
        ":mxclient",
        ":servers",
        ":threaded_client_py",
        "//multiplexer/testing",
    ],
)

py_test(
    name = "sync_waits_test",
    size = "medium",
    srcs = ["sync_waits_test.py"],
    deps = [
        ":clients",
        ":multiplexer_constants",
        ":mxclient",
        "//multiplexer/testing",
    ],
)

# A backend whose one multiplexer stops reading: the loop serves on through
# the other, and nothing holds the GIL.
py_test(
    name = "frozen_multiplexer_test",
    size = "medium",
    srcs = ["frozen_multiplexer_test.py"],
    data = ["//tests:testing.rules"],
    deps = [
        ":clients",
        ":multiplexer_constants",
        ":multiplexer_py_proto",
        ":mxclient",
        ":servers",
        "//multiplexer/testing",
    ],
)

py_test(
    name = "send_reports_test",
    size = "medium",
    srcs = ["send_reports_test.py"],
    deps = [
        ":clients",
        ":multiplexer_constants",
        "//multiplexer/testing",
    ],
)

py_test(
    name = "threaded_sends_test",
    size = "medium",
    srcs = ["threaded_sends_test.py"],
    deps = [
        ":aio",
        ":clients",
        ":multiplexer_constants",
        ":threaded_client_py",
        "//multiplexer/testing",
    ],
)

py_test(
    name = "connect_by_name_test_py",
    size = "small",
    srcs = ["connect_by_name_test.py"],
    main = "connect_by_name_test.py",
    deps = [
        ":aio",
        ":multiplexer_constants",
        ":threaded_client_py",
        ":threaded_server",
        "//multiplexer/testing",
    ],
)

py_test(
    name = "client_names_py_test",
    size = "small",
    srcs = ["client_names_test.py"],
    main = "client_names_test.py",
    deps = [
        ":clients",
        "//multiplexer/testing",
    ],
)

# The mxcontrol that came with the package, and the command that runs it:
# the wheel's package data, or //mxcontrol beside the package in runfiles.
py_library(
    name = "mxcontrol_py",
    srcs = ["mxcontrol.py"],
    data = ["//mxcontrol"],
    imports = [".."],
    visibility = ["//visibility:public"],
)

py_test(
    name = "mxcontrol_test",
    size = "small",
    srcs = ["mxcontrol_test.py"],
    data = ["//tests:testing.rules"],
    deps = [
        ":mxcontrol_py",
        "//multiplexer/testing",
    ],
)

# A multiplexer's address, (host, port), and as text: host:port, or
# [IPv6 address]:port, the C++ endpoint library's through the extension.
py_library(
    name = "endpoints",
    srcs = ["endpoints.py"],
    imports = [".."],
    visibility = ["//visibility:public"],
    deps = [":_native"],
)

py_test(
    name = "endpoints_test",
    size = "small",
    srcs = ["endpoints_test.py"],
    deps = [
        ":endpoints",
        ":threaded_client_py",
    ],
)

py_library(
    name = "threaded_client_py",
    srcs = ["threaded_client.py"],
    imports = [".."],
    visibility = ["//visibility:public"],
    deps = [
        ":clients",
        ":endpoints",
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
