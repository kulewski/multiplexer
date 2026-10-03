# Role processes in Python. Each plays one peer and prints its events on
# stdout, one protobuf text-format message a line.
py_library(
    name = "common",
    srcs = ["common.py"],
    deps = [
        "//multiplexer:clients",
        "//multiplexer:endpoints",
        "//multiplexer:events_py_proto",
        "//multiplexer:multiplexer_constants",
        "//multiplexer:servers",
        "//multiplexer:threaded_client_py",
        "//multiplexer:threaded_server",
    ],
)

[
    py_binary(
        name = role,
        srcs = [role + ".py"],
        visibility = ["//visibility:public"],
        deps = [":common"],
    )
    for role in [
        "backend",
        "client",
        "event_client",
        "event_backend",
    ]
]
