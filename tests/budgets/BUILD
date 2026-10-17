# Budgets the multiplexer keeps to, counted by valgrind rather than timed:
# ./check.sh --valgrind builds them -c opt and runs them; tagged manual, as
# valgrind is no requirement of the build.
py_test(
    name = "instructions_test",
    size = "large",
    srcs = ["instructions_test.py"],
    data = [
        "//mxcontrol",
        "//tests:testing.rules",
        "//tests/roles/cc:mxtestroles",
    ],
    tags = ["manual"],
    deps = [
        "//multiplexer/testing",
        "//tests:testing_constants",
    ],
)
