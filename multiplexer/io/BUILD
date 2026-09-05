# Frames (raw_message) and connections, shared by the multiplexer and the client.
cc_library(
    name = "connection",
    hdrs = ["connection.h"],
    visibility = ["//multiplexer:__pkg__"],
    deps = [
        "//:protobuf_runtime",
        "//lib:functors",
        "//lib:thread_checker",
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
        "//lib/encoding:decode_from_range",
        "//lib/encoding:encode_to_range",
        "//lib/encoding:little_endian",
        "//multiplexer:defaults",
        "@boost//:array",
        "@boost//:asio",
        "@boost//:crc",
    ],
)
