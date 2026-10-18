# The crate mx-multiplexer, the Rust client library, for Bazel: the library,
# its unit tests, and the checks that its generated files are fresh. The
# Rust targets are tagged rust: CI's Rust job builds and tests them, the
# other jobs leave them out (.github/workflows/checks.yml), and a local
# `bazel test //...` runs them, the toolchain and the crates fetched once.
load("@rules_rust//rust:defs.bzl", "rust_library", "rust_test")
load("//rust/3rdparty/crates:defs.bzl", "aliases", "all_crate_deps")

# rust/BUILD's crates_vendor reads the manifest.
exports_files(["Cargo.toml"])

rust_library(
    name = "mx_multiplexer",
    srcs = glob(["src/**/*.rs"]),
    aliases = aliases(),
    proc_macro_deps = all_crate_deps(proc_macro = True),
    tags = ["rust"],
    version = "4.1.0",
    visibility = ["//visibility:public"],
    deps = all_crate_deps(normal = True),
)

rust_test(
    name = "mx_multiplexer_test",
    size = "small",
    aliases = aliases(),
    compile_data = ["testdata/golden_frames.txt"],
    crate = ":mx_multiplexer",
    proc_macro_deps = all_crate_deps(proc_macro_dev = True),
    tags = ["rust"],
    deps = all_crate_deps(normal_dev = True),
)

# The crate's tests against real multiplexers, one test crate per file of
# tests/, as cargo builds them, with mxcontrol and the tests' rules file
# from runfiles (tests/support/mod.rs); the soak reads the heap through
# libc, and the servers' test keeps what the library logs through log.
[
    rust_test(
        name = test + "_test",
        size = "medium",
        srcs = ["tests/%s.rs" % test] + glob(["tests/support/*.rs"]),
        crate_root = "tests/%s.rs" % test,
        data = [
            "//mxcontrol",
            "//tests:testing.rules",
        ],
        env = {
            "MXCONTROL": "$(rootpath //mxcontrol)",
            "MX_TEST_RULES": "$(rootpath //tests:testing.rules)",
        },
        tags = ["rust"],
        deps = [
            ":mx_multiplexer",
            "//rust/3rdparty/crates:libc",
            "//rust/3rdparty/crates:log",
        ],
    )
    for test in [
        "allocations",
        "confirmed_sends",
        "fork",
        "servers",
        "soak",
        "sync_client",
        "threaded_client",
    ]
]

# The golden frames the Rust tests compare with are what the Python
# package's protobuf writes for the same messages.
py_test(
    name = "golden_frames_test",
    size = "small",
    srcs = ["testdata/golden.py"],
    args = ["--check"],
    data = ["testdata/golden_frames.txt"],
    main = "testdata/golden.py",
    deps = ["//multiplexer:multiplexer_py_proto"],
)

py_test(
    name = "generated_test",
    size = "small",
    srcs = ["generated_test.py"],
    data = [
        "BUILD",
        "Cargo.toml",
        "src/constants.rs",
        "src/proto/multiplexer.rs",
        "src/proto/mx.logging.rs",
        "tests/support/testing_constants.rs",
        "//:multiplexer.rules",
        "//multiplexer:Logging.proto",
        "//multiplexer:Multiplexer.proto",
        "//mxcontrol",
        "//tests:testing.rules",
    ],
    deps = [
        "//multiplexer:release",
        "//multiplexer/testing",
    ],
)
