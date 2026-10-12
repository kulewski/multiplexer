# The Rust test roles: one binary, mxtestroles <role> <options>, one module
# per role, as tests/roles/cc has one file per role; and the unit tests of
# what they share, the event printer and the parsers. The crates are the
# library's, vendored in rust/3rdparty (tests/roles/rust/Cargo.toml says
# why the roles are no member of its workspace). Tagged rust, as every Rust
# target is: only CI's Rust job builds them, and the scenarios that run
# them are tagged rust too (multiplexer/testing/defs.bzl).
load("@rules_rust//rust:defs.bzl", "rust_binary", "rust_test")

rust_binary(
    name = "mxtestroles",
    srcs = glob(["src/**/*.rs"]),
    tags = ["rust"],
    visibility = ["//visibility:public"],
    deps = [
        "//rust/3rdparty/crates:libc",
        "//rust/3rdparty/crates:log",
        "//rust/mx-multiplexer:mx_multiplexer",
    ],
)

rust_test(
    name = "mxtestroles_test",
    size = "small",
    crate = ":mxtestroles",
    tags = ["rust"],
)
