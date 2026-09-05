# Preprocessor helper headers.
cc_library(
    name = "common",
    hdrs = ["common.h"],
    visibility = ["//lib:__pkg__"],
)

cc_library(
    name = "create_message",
    hdrs = ["create_message.h"],
    visibility = ["//lib/logging:__pkg__"],
    deps = [":create_message_detail"],
)

cc_library(
    name = "create_message_detail",
    hdrs = ["create_message_detail.h"],
)

cc_library(
    name = "kwargs",
    hdrs = ["kwargs.h"],
    visibility = ["//lib:__subpackages__"],
)
