# Frames (raw_message) and connections, shared by the multiplexer and the client.
cc_library(
    name = "connection",
    hdrs = ["connection.h"],
    visibility = ["//multiplexer:__pkg__"],
    deps = [
        ":shared_outgoing",
        "//:protobuf_runtime",
        "//lib:functors",
        "//lib:seconds",
        "//lib:thread_checker",
    ],
)

cc_library(
    name = "shared_outgoing",
    hdrs = ["shared_outgoing.h"],
    deps = [
        ":raw_message",
        "//lib:adaptive_mutex",
        "//lib:thread_annotations",
        "//multiplexer:defaults",
    ],
)

cc_test(
    name = "shared_outgoing_test",
    size = "small",
    srcs = ["shared_outgoing_test.cc"],
    deps = [
        ":raw_message",
        ":shared_outgoing",
        "//lib:functors",
        "//multiplexer:multiplexer_cc_proto",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "raw_message",
    srcs = ["raw_message.cc"],
    hdrs = ["raw_message.h"],
    visibility = ["//multiplexer:__pkg__"],
    deps = [
        "//:protobuf_runtime",
        "//lib:assert",
        "//lib:resize_uninitialized",
        "//lib:xxh3",
        "//lib/encoding:decode_from_range",
        "//lib/encoding:encode_to_range",
        "//lib/encoding:little_endian",
        "//multiplexer:defaults",
        "//multiplexer:multiplexer_cc_proto",
        "@asio",
    ],
)
