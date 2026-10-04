# The worker: the signal processing, its tests, and the backend around it.
cc_library(
    name = "dsp",
    srcs = ["dsp.cc"],
    hdrs = ["dsp.h"],
)

cc_test(
    name = "dsp_test",
    srcs = ["dsp_test.cc"],
    deps = [
        ":dsp",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_binary(
    name = "worker",
    srcs = ["worker.cc"],
    visibility = ["//visibility:public"],
    deps = [
        ":dsp",
        "//:audio_cc_proto",
        "@mx//multiplexer:endpoint",
        "@mx//multiplexer:multiplexer_cc_constants",
        "@mx//multiplexer/backend:base_threaded_multiplexer_server",
    ],
)
