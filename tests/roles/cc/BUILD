# The C++ test roles: one binary, mxtestroles <role> <options>, with one
# subcommand per role file. Each role is a library so that the registration
# (REGISTER_MXCONTROL_SUBCOMMAND) links in; alwayslink keeps it.
cc_library(
    name = "common",
    srcs = ["common.cc"],
    hdrs = ["common.h"],
    deps = [
        "//lib:memory",
        "//multiplexer:client",
        "//multiplexer/testing:events_cc_proto",
        "@boost//:program_options",
    ],
)

[
    cc_library(
        name = role,
        srcs = [role + ".cc"],
        deps = [
            ":common",
            "//multiplexer:multiplexer_cc_proto",
            "//multiplexer:threaded_client",
            "//multiplexer/backend:base_multiplexer_server",
            "//mxcontrol:task",
            "//mxcontrol:tasks_holder",
        ],
        alwayslink = 1,
    )
    for role in [
        "backend",
        "client",
        "event_client",
        "event_backend",
    ]
]

cc_binary(
    name = "mxtestroles",
    srcs = ["main.cc"],
    visibility = ["//visibility:public"],
    deps = [
        ":backend",
        ":client",
        ":event_backend",
        ":event_client",
        "//lib:program",
        "//mxcontrol:driver",
        "//mxcontrol:help",
    ],
)
