# Role processes in Python. Each plays one peer, prints JSON lines on stdout.
py_library(
    name = "common",
    srcs = ["common.py"],
    deps = [
        "//multiplexer:clients",
        "//multiplexer:multiplexer_constants",
        "//multiplexer:servers",
        "//multiplexer:threaded_client_py",
        "//multiplexer/testing:events_py_proto",
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
