# BaseMultiplexerServer, the C++ backend base class.
cc_library(
    name = "base_multiplexer_server",
    srcs = ["base_multiplexer_server.cc"],
    hdrs = ["base_multiplexer_server.h"],
    visibility = ["//visibility:public"],
    deps = [
        "//lib:exception",
        "//lib:kwargs",
        "//multiplexer:client",
        "@boost//:foreach",
    ],
)

# BaseThreadedMultiplexerServer: handlers on worker threads behind a
# heartbeating io thread.
cc_library(
    name = "base_threaded_multiplexer_server",
    srcs = ["base_threaded_multiplexer_server.cc"],
    hdrs = ["base_threaded_multiplexer_server.h"],
    visibility = ["//visibility:public"],
    deps = [
        ":base_multiplexer_server",
        "//lib:mutex",
        "//lib/logging",
        "//multiplexer:multiplexer_cc_constants",
        "//multiplexer:threaded_client",
    ],
)

cc_test(
    name = "threaded_server_test",
    size = "medium",
    srcs = ["threaded_server_test.cc"],
    data = ["//:multiplexer.rules"],
    deps = [
        ":base_threaded_multiplexer_server",
        "//multiplexer:client",
        "//multiplexer:in_process_multiplexer",
        "//multiplexer:multiplexer_cc_constants",
        "//multiplexer:threaded_client",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "serve_thread_test",
    size = "small",
    srcs = ["serve_thread_test.cc"],
    data = ["//:multiplexer.rules"],
    deps = [
        ":base_multiplexer_server",
        "//multiplexer:in_process_multiplexer",
        "//multiplexer:multiplexer_cc_constants",
        "@com_google_googletest//:gtest_main",
    ],
)
