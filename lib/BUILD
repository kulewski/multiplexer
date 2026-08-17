# The C++ base library, one small target per header so that dependencies
# stay explicit and generate_constants can avoid the logging library.
cc_library(
    name = "assert",
    srcs = ["assertion.cc"],
    hdrs = ["assertion.h"],
    visibility = ["//:__subpackages__"],
    deps = [
        ":exception",
        "@boost//:preprocessor",
    ],
)

cc_library(
    name = "thread_annotations",
    hdrs = ["thread_annotations.h"],
    visibility = ["//:__subpackages__"],
)

cc_library(
    name = "mutex",
    hdrs = ["mutex.h"],
    visibility = ["//:__subpackages__"],
    deps = [":thread_annotations"],
)

cc_library(
    name = "thread_checker",
    hdrs = ["thread_checker.h"],
    visibility = ["//:__subpackages__"],
    deps = [
        ":assert",
        ":thread_annotations",
        "//lib/preproc:common",
    ],
)

cc_test(
    name = "thread_checker_test",
    size = "small",
    srcs = ["thread_checker_test.cc"],
    deps = [
        ":mutex",
        ":thread_checker",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "sha1",
    hdrs = ["sha1.h"],
    visibility = ["//visibility:public"],
    deps = ["@boost//:uuid"],
)

cc_library(
    name = "fork",
    srcs = ["fork.cc"],
    hdrs = ["fork.h"],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "memory",
    srcs = ["memory.cc"],
    hdrs = ["memory.h"],
    visibility = ["//:__subpackages__"],
)

cc_library(
    name = "core_dump",
    srcs = ["core_dump.cc"],
    hdrs = ["core_dump.h"],
    visibility = ["//mxcontrol:__pkg__"],
)

cc_library(
    name = "exception",
    srcs = ["exception.cc"],
    hdrs = ["exception.h"],
    visibility = ["//:__subpackages__"],
)

cc_library(
    name = "fd",
    hdrs = ["fd.h"],
    visibility = ["//lib:__subpackages__"],
)

cc_library(
    name = "functors",
    hdrs = ["functors.h"],
    visibility = ["//multiplexer:__subpackages__"],
)

cc_library(
    name = "initialization",
    hdrs = ["initialization.h"],
    visibility = ["//lib:__subpackages__"],
    deps = ["//lib/preproc:common"],
)

cc_library(
    name = "intrusive_value",
    hdrs = ["intrusive_value.h"],
)

cc_library(
    name = "kwargs",
    srcs = ["kwargs.cc"],
    hdrs = ["kwargs.h"],
    visibility = ["//multiplexer/backend:__pkg__"],
    deps = [
        ":exception",
        "@boost//:any",
        "@boost//:foreach",
    ],
)

cc_library(
    name = "program",
    hdrs = ["program.h"],
    visibility = [
        "//mxcontrol:__pkg__",
        "//tests:__subpackages__",
    ],
    deps = [
        ":assert",
        ":type_utils",
        "//lib/logging",
    ],
)

cc_library(
    name = "random",
    srcs = ["random.cc"],
    hdrs = ["random.h"],
    visibility = ["//visibility:public"],
    deps = [
        ":assert",
        "@boost//:random",
    ],
)

cc_library(
    name = "release",
    srcs = ["release.cc"],
    hdrs = ["release.h"],
    visibility = ["//lib/logging:__pkg__"],
)

cc_library(
    name = "repr",
    hdrs = ["repr.h"],
    visibility = ["//lib:__subpackages__"],
)

cc_library(
    name = "spanset",
    hdrs = ["spanset.h"],
    visibility = ["//multiplexer:__pkg__"],
)

cc_library(
    name = "timer",
    hdrs = ["timer.h"],
    visibility = ["//multiplexer:__pkg__"],
    deps = [":intrusive_value"],
)

cc_library(
    name = "triple",
    hdrs = ["triple.h"],
    visibility = ["//multiplexer:__pkg__"],
)

cc_library(
    name = "type_utils",
    hdrs = ["type_utils.h"],
    visibility = ["//:__subpackages__"],
)

cc_library(
    name = "vector",
    hdrs = ["vector.h"],
    visibility = ["//multiplexer:__pkg__"],
)

cc_test(
    name = "kwargs_test",
    size = "small",
    srcs = ["kwargs_test.cc"],
    deps = [
        ":kwargs",
        "@com_google_googletest//:gtest_main",
    ],
)
