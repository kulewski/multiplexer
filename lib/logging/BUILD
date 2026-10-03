# C++ logging; its LogEntry protocol buffer is multiplexer/Logging.proto.

cc_library(
    name = "impl",
    hdrs = ["impl.h"],
    deps = [
        ":log_tokens",
        "//lib:assert",
        "//lib:release",
        "//lib/preproc:common",
        "//multiplexer:logging_cc_proto",
    ],
)

cc_library(
    name = "log_tokens",
    hdrs = ["log_tokens.h"],
    deps = ["//lib/preproc:kwargs"],
)

cc_library(
    name = "logging",
    srcs = ["logging.cc"],
    hdrs = ["logging.h"],
    visibility = ["//:__subpackages__"],
    deps = [
        ":impl",
        ":log_tokens",
        "//lib:assert",
        "//lib:initialization",
        "//lib:mutex",
        "//lib:random",
        "//lib:repr",
        "//lib/preproc:common",
        "//lib/protobuf:stream",
    ],
)

# The program's name in the log context, from argv[0] whatever it is; the
# test runs itself again with each one, hence its own main().
cc_test(
    name = "process_name_test",
    size = "small",
    srcs = ["process_name_test.cc"],
    deps = [
        ":logging",
        "@com_google_googletest//:gtest",
    ],
)

cc_test(
    name = "verbosity_test",
    size = "small",
    srcs = ["verbosity_test.cc"],
    deps = [
        ":logging",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "concurrent_settings_test",
    size = "small",
    srcs = ["concurrent_settings_test.cc"],
    deps = [
        ":logging",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "stderr_test",
    size = "small",
    srcs = ["stderr_test.cc"],
    deps = [
        ":logging",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "stream_test",
    size = "small",
    srcs = ["stream_test.cc"],
    deps = [
        ":logging",
        "//lib/protobuf:stream",
        "@com_google_googletest//:gtest_main",
    ],
)
