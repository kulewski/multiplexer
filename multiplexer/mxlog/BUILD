# type_id_constants.txt is a list of NAME = int lines. It is used verbatim as
# the Python module //multiplexer:type_id_constants.py and turned into a C++
# header, and the module's stub, by gen_type_id_constants.py.
exports_files(
    ["type_id_constants.txt"],
    visibility = ["//multiplexer:__pkg__"],
)

py_binary(
    name = "gen_type_id_constants",
    srcs = ["gen_type_id_constants.py"],
    visibility = ["//multiplexer:__pkg__"],
)

genrule(
    name = "generate_cc_type_id_constants",
    srcs = ["type_id_constants.txt"],
    outs = ["type_id_constants.h"],
    cmd = "$(location :gen_type_id_constants) $< $@",
    tools = [":gen_type_id_constants"],
)

cc_library(
    name = "cc_type_id_constants",
    hdrs = [":generate_cc_type_id_constants"],
    visibility = ["//multiplexer:__pkg__"],
)

py_library(
    name = "streaming",
    srcs = ["streaming.py"],
    imports = ["../.."],
)

py_library(
    name = "mxlog",
    srcs = ["__init__.py"],
    imports = ["../.."],
    visibility = ["//multiplexer:__pkg__"],
    deps = [
        "//multiplexer:_native",
        "//multiplexer:py_type_id_constants",
        "//multiplexer:release",
        "//multiplexer/util:decorators",
    ],
)
