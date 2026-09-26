# The tests' rules file, the system rules plus the tests' types, and the
# integration tests' constants generated from it. .bazelrc points
# //:multiplexer_rules at it, and the unit tests run their multiplexers on it.
exports_files(
    ["testing.rules"],
    visibility = ["//visibility:public"],
)

genrule(
    name = "generate_testing_constants",
    srcs = ["testing.rules"],
    outs = ["testing_constants.py"],
    cmd = "$(location //multiplexer:generate_constants) $< $@",
    tools = ["//multiplexer:generate_constants"],
)

py_library(
    name = "testing_constants",
    srcs = ["testing_constants.py"],
    visibility = ["//tests:__subpackages__"],
)
