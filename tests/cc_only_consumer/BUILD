# A C++ client and a C++ backend of the multiplexer, built in a workspace
# that has no Python rules: both only have to compile and link.
cc_binary(
    name = "client",
    srcs = ["client.cc"],
    deps = [
        "@mx//multiplexer:client",
        "@mx//multiplexer:multiplexer_cc_constants",
    ],
)

cc_binary(
    name = "backend",
    srcs = ["backend.cc"],
    deps = [
        "@mx//multiplexer:multiplexer_cc_constants",
        "@mx//multiplexer/backend:base_multiplexer_server",
    ],
)
