# C++ logging and the LogEntry protocol buffer.
genrule(
    name = "logging_proto",
    srcs = ["Logging.proto"],
    outs = [
        "Logging.pb.cc",
        "Logging.pb.h",
        "Logging_pb2.py",
    ],
    # Include and output roots are the repository root, whether we are the
    # main workspace or an external one; see multiplexer/BUILD.
    cmd = "root=$(location Logging.proto); root=$${root%lib/logging/Logging.proto}; root=$${root%/}; " +
          "$(location //:protoc) -I $${root:-.} --cpp_out=$(RULEDIR)/../.. --python_out=$(RULEDIR)/../.. $(SRCS)",
    tools = ["//:protoc"],
)

exports_files(
    ["Logging.proto"],
    visibility = ["//multiplexer:__pkg__"],
)

cc_library(
    name = "logging_cc_proto",
    srcs = ["Logging.pb.cc"],
    hdrs = ["Logging.pb.h"],
    visibility = ["//:__subpackages__"],
    deps = ["//:protobuf_runtime"],
)

py_library(
    name = "logging_py_proto",
    srcs = ["Logging_pb2.py"],
    visibility = ["//multiplexer:__pkg__"],
)

cc_library(
    name = "impl",
    hdrs = ["impl.h"],
    deps = [
        ":logging_cc_proto",
        "//lib:release",
    ],
)

cc_library(
    name = "log_tokens",
    hdrs = ["log_tokens.h"],
    deps = ["//lib/preproc:kwargs"],
)

cc_library(
    name = "logging",
    srcs = ["logging.cc"],
    hdrs = ["logging.h"],
    visibility = ["//:__subpackages__"],
    deps = [
        ":impl",
        ":log_tokens",
        "//lib:assert",
        "//lib:initialization",
        "//lib:random",
        "//lib:repr",
        "//lib/preproc:create_message",
        "//lib/protobuf:stream",
        "@boost//:lexical_cast",
    ],
)
