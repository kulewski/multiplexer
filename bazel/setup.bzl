"""Second stage of the multiplexer's workspace setup; see deps.bzl."""

load("@com_github_nelhage_rules_boost//:boost/boost.bzl", "boost_deps")
load("@pybind11_bazel//:python_configure.bzl", "python_configure")
load("@rules_python//python:repositories.bzl", "py_repositories")

def mx_setup(python = True, boost = True):
    """Runs the setup calls of the repositories mx_dependencies() declared.

    `python = False` skips rules_python's py_repositories(), for a workspace
    where another ruleset already ran it; `boost = False` skips
    rules_boost's boost_deps() likewise. The Python toolchain for the
    extension (local_config_python) is configured unless it exists already.
    """
    if python:
        py_repositories()
    if boost:
        boost_deps()
    if not native.existing_rule("local_config_python"):
        python_configure(name = "local_config_python", python_version = "3")
