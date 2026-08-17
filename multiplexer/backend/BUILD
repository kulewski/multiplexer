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
