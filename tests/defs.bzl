"""mx_integration_test for this repository's scenarios: the public macro
(multiplexer/testing/defs.bzl) with tests/testing.rules and tests/harness."""

load("//multiplexer/testing:defs.bzl", _mx_integration_test = "mx_integration_test")

def mx_integration_test(name, scenario, extra_deps = [], **kwargs):
    """The public mx_integration_test, with the rules file and the constants
    the scenarios under tests/ use. extra_deps adds py_library targets a
    scenario needs beyond the harness."""
    _mx_integration_test(
        name = name,
        scenario = scenario,
        rules = "//tests:testing.rules",
        deps = ["//tests/harness"] + extra_deps,
        **kwargs
    )
