# Byte encoders for the frame header.
cc_library(
    name = "base_decoder",
    hdrs = ["base_decoder.h"],
)

cc_library(
    name = "base_encoder",
    hdrs = ["base_encoder.h"],
)

cc_library(
    name = "decode_from_range",
    hdrs = ["decode_from_range.h"],
    visibility = ["//multiplexer/io:__pkg__"],
    deps = [
        ":base_decoder",
        "//lib:assert",
    ],
)

cc_library(
    name = "encode_to_range",
    hdrs = ["encode_to_range.h"],
    visibility = ["//multiplexer/io:__pkg__"],
    deps = [":base_encoder"],
)

cc_library(
    name = "little_endian",
    hdrs = ["little_endian.h"],
    visibility = ["//multiplexer/io:__pkg__"],
)

cc_test(
    name = "little_endian_test",
    size = "small",
    srcs = ["little_endian_test.cc"],
    deps = [
        ":decode_from_range",
        ":encode_to_range",
        ":little_endian",
        "@com_google_googletest//:gtest_main",
    ],
)
