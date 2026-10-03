# Length-delimited protocol buffer streams, the format of the log files
# mxcontrol writes and reads; public so that other programs can read them.
cc_library(
    name = "stream",
    hdrs = ["stream.h"],
    visibility = ["//visibility:public"],
    deps = [
        "//:protobuf_runtime",
        "//lib:fd",
    ],
)

cc_test(
    name = "stream_test",
    size = "medium",
    srcs = ["stream_test.cc"],
    deps = [
        ":stream",
        "//lib/logging",
        "//multiplexer:logging_cc_proto",
        "@com_google_googletest//:gtest_main",
    ],
)
