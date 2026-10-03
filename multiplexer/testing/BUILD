# Test infrastructure for programs built on the multiplexer, public: see
# __init__.py and docs/api_python.md. mx_integration_test is in defs.bzl.

py_library(
    name = "testing",
    srcs = [
        "__init__.py",
        "buffers.py",
        "fakes.py",
        "raw_peer.py",
    ],
    data = [
        "//:multiplexer_rules",
        "//mxcontrol",
    ],
    imports = ["../.."],
    visibility = ["//visibility:public"],
    deps = [
        "//multiplexer:clients",
        "//multiplexer:events_py_proto",
        "//multiplexer:multiplexer_constants",
        "//multiplexer:multiplexer_py_proto",
        "//multiplexer:mxcontrol_py",
        "//multiplexer:servers",
        "//multiplexer:threaded_client_py",
    ],
)

py_test(
    name = "fakes_test",
    size = "small",
    srcs = ["fakes_test.py"],
    deps = [":testing"],
)

py_test(
    name = "harness_test",
    size = "medium",
    srcs = ["harness_test.py"],
    deps = [
        ":testing",
        "//multiplexer:recording",
    ],
)

py_test(
    name = "lanes_test",
    size = "medium",
    srcs = ["lanes_test.py"],
    deps = [":testing"],
)

py_test(
    name = "config_test",
    size = "small",
    srcs = ["config_test.py"],
    deps = [":testing"],
)

py_test(
    name = "private_network_test",
    size = "small",
    srcs = ["private_network_test.py"],
)

py_test(
    name = "cluster_exit_test",
    size = "medium",
    srcs = ["cluster_exit_test.py"],
    deps = [":testing"],
)
