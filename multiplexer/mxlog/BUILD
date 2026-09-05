# type_id_constants.txt is a list of NAME = int lines. It is used verbatim as a
# Python module and turned into a C++ header by gen_type_id_constants.py.
py_binary(
    name = "gen_type_id_constants",
    srcs = ["gen_type_id_constants.py"],
)

genrule(
    name = "generate_cc_type_id_constants",
    srcs = ["type_id_constants.txt"],
    outs = ["type_id_constants.h"],
    cmd = "$(location :gen_type_id_constants) $< $@",
    tools = [":gen_type_id_constants"],
)

genrule(
    name = "generate_py_type_id_constants",
    srcs = ["type_id_constants.txt"],
    outs = ["type_id_constants.py"],
    cmd = "cp $< $@",
)

cc_library(
    name = "cc_type_id_constants",
    hdrs = [":generate_cc_type_id_constants"],
    visibility = ["//multiplexer:__pkg__"],
)

py_library(
    name = "py_type_id_constants",
    srcs = ["type_id_constants.py"],
    imports = ["../.."],
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
        ":py_type_id_constants",
        "//multiplexer:_native",
        "//multiplexer:release",
        "//multiplexer/util:decorators",
    ],
)
