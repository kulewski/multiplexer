# Test infrastructure for programs built on the multiplexer, public: see
# __init__.py and docs/api_python.md. mx_integration_test is in defs.bzl.

# The Event message the test roles report with, in both languages.
genrule(
    name = "events_proto",
    srcs = ["events.proto"],
    outs = [
        "events.pb.cc",
        "events.pb.h",
        "events_pb2.py",
    ],
    cmd = "root=$(location events.proto); root=$${root%multiplexer/testing/events.proto}; root=$${root%/}; " +
          "$(location //:protoc) -I $${root:-.} --cpp_out=$(RULEDIR)/../.. --python_out=$(RULEDIR)/../.. $(SRCS)",
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
    imports = ["../.."],
    visibility = ["//visibility:public"],
)

# Where the rules file the multiplexer_rules flag names sits in the
# runfiles, so that a Cluster outside mx_integration_test runs with the
# file the generated constants came from, whatever workspace it is in.
genrule(
    name = "rules_path",
    srcs = ["//:multiplexer_rules"],
    outs = ["rules_path.py"],
    cmd = "echo '\"\"\"Generated: the runfiles path of the rules file the build was pointed at.\"\"\"' > $@ && " +
          "echo 'RULES = \"$(rootpath //:multiplexer_rules)\"' >> $@",
)

py_library(
    name = "testing",
    srcs = [
        "__init__.py",
        "fakes.py",
        "raw_peer.py",
        "rules_path.py",
    ],
    data = [
        "//:multiplexer_rules",
        "//mxcontrol",
    ],
    imports = ["../.."],
    visibility = ["//visibility:public"],
    deps = [
        ":events_py_proto",
        "//multiplexer:clients",
        "//multiplexer:multiplexer_constants",
        "//multiplexer:multiplexer_py_proto",
        "//multiplexer:servers",
    ],
)

py_test(
    name = "fakes_test",
    size = "small",
    srcs = ["fakes_test.py"],
    deps = [":testing"],
)

py_test(
    name = "config_test",
    size = "small",
    srcs = ["config_test.py"],
    deps = [":testing"],
)
