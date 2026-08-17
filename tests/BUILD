# Integration test support: rules file and constants generated from it.
exports_files(
    ["testing.rules"],
    visibility = ["//tests:__subpackages__"],
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
