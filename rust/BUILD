# The Cargo workspace of the Rust library, for Bazel: its manifest and
# lockfile, and the crates_vendor target that rust/vendor.sh runs to write
# rust/3rdparty/crates, the BUILD files of the crates the workspace depends
# on and crates.bzl, whose crate_repositories() the WORKSPACE calls; the
# crates themselves are http_archives, fetched when a target needs them.
load("@rules_rust//crate_universe:defs.bzl", "crates_vendor", "splicing_config")

exports_files([
    "Cargo.toml",
    "Cargo.lock",
])

crates_vendor(
    name = "crates_vendor",
    cargo_bazel = "@cargo_bazel_prebuilt//file:downloaded",
    cargo_lockfile = ":Cargo.lock",
    manifests = [
        ":Cargo.toml",
        "//rust/mx-multiplexer:Cargo.toml",
    ],
    mode = "remote",
    repository_name = "crate_index",
    # The resolver rust/Cargo.toml asks for.
    splicing_config = splicing_config(resolver_version = "2"),
    # The platforms a build of this repository may target; rules_rust
    # 0.27.0's default list names targets that newer compilers lack.
    supported_platform_triples = [
        "aarch64-apple-darwin",
        "aarch64-unknown-linux-gnu",
        "x86_64-apple-darwin",
        "x86_64-unknown-linux-gnu",
    ],
    # Run by rust/vendor.sh alone: never part of //..., whose analysis
    # would fetch the toolchain and cargo-bazel for it.
    tags = ["manual"],
    vendor_path = "3rdparty/crates",
)
