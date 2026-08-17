# The echo example: a backend and a client, each in both languages.
load("@mx//multiplexer/testing:defs.bzl", "mx_integration_test")

exports_files(["echo.rules"])

py_binary(
    name = "backend_py",
    srcs = ["backend.py"],
    main = "backend.py",
    deps = [
        "@mx//multiplexer:multiplexer_constants",
        "@mx//multiplexer:servers",
    ],
)

cc_binary(
    name = "backend_cc",
    srcs = ["backend.cc"],
    deps = [
        "@mx//multiplexer:multiplexer_cc_constants",
        "@mx//multiplexer/backend:base_multiplexer_server",
    ],
)

py_binary(
    name = "client_py",
    srcs = ["client.py"],
    main = "client.py",
    deps = [
        "@mx//multiplexer:clients",
        "@mx//multiplexer:multiplexer_constants",
    ],
)

cc_binary(
    name = "client_cc",
    srcs = ["client.cc"],
    deps = [
        "@mx//multiplexer:client",
        "@mx//multiplexer:multiplexer_cc_constants",
    ],
)

py_binary(
    name = "workers",
    srcs = ["workers.py"],
    deps = [
        "@mx//multiplexer:multiplexer_constants",
        "@mx//multiplexer:threaded_client_py",
    ],
)

py_test(
    name = "echo_test",
    size = "small",
    srcs = ["echo_test.py"],
    data = [
        ":backend_cc",
        ":backend_py",
        ":client_cc",
        ":client_py",
        ":echo.rules",
        ":workers",
        "@mx//mxcontrol",
    ],
    deps = [
        "@mx//multiplexer:clients",
        "@mx//multiplexer:multiplexer_constants",
    ],
)

# The multiplexer's test infrastructure from this workspace: the macro names
# the rules file (echo.rules, through @mx//:multiplexer_rules) and brings
# the multiplexer binary along.
mx_integration_test(
    name = "harness_test",
    scenario = "harness_test.py",
    deps = [
        "@mx//multiplexer:clients",
        "@mx//multiplexer:recording",
        "@mx//multiplexer:recording_py_proto",
    ],
)

# The same infrastructure from a plain py_test: Cluster() finds echo.rules
# through the flag, without the macro.
py_test(
    name = "default_rules_test",
    size = "small",
    srcs = ["default_rules_test.py"],
    deps = [
        "@mx//multiplexer:multiplexer_constants",
        "@mx//multiplexer/testing",
    ],
)
