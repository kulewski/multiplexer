"""pybind_extension: a Python extension module built with pybind11, as
pybind11_bazel's macro of that name builds it (build_defs.bzl, BSD-style
license), the same compile and link options and dependencies.

Defined here so that loading //multiplexer, where the extension is
declared, needs no load from @pybind11_bazel: a C++-only workspace, which
declares no Python repositories (mx_dependencies(python = False)), builds
the package's C++ targets, and only building the extension asks for
@pybind11 and @local_config_python.
"""

_DEPS = [
    "@pybind11",
    "@local_config_python//:python_headers",
]

def pybind_extension(name, copts = [], features = [], linkopts = [], tags = [], deps = [], **kwargs):
    """`name`.so, a shared library Python imports as the module `name`."""
    native.cc_binary(
        name = name + ".so",
        copts = copts + select({
            "@pybind11//:msvc_compiler": [],
            "//conditions:default": ["-fexceptions", "-fvisibility=hidden"],
        }),
        features = features + ["-use_header_modules", "-parse_headers"],
        linkopts = linkopts + select({
            "@pybind11//:msvc_compiler": [],
            "@pybind11//:osx": ["-undefined", "dynamic_lookup"],
            "//conditions:default": ["-Wl,-Bsymbolic"],
        }),
        linkshared = 1,
        tags = tags + ["req_dep=%s" % dep for dep in _DEPS],
        deps = deps + _DEPS,
        **kwargs
    )
