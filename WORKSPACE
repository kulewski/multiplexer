# The Bazel workspace, named mx: our dependencies via bazel/deps.bzl, the
# same way a consumer of @mx declares them, plus development-only tools.
workspace(name = "mx")

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive", "http_file")

# protobuf from source, for --config=libcxx (.bazelrc) alone: the system's
# libprotobuf, which every other configuration links, is built against
# libstdc++. The version every other build of this repository uses
# (packaging/wheels.sh), and the two repositories its BUILD needs that
# nothing else declares. Declared ahead of mx_setup(), whose rules_python
# would otherwise put a protobuf of its own under this name; nothing is
# fetched unless that configuration asks for it.
http_archive(
    name = "com_google_protobuf",
    canonical_id = "v21.12",
    # Its BUILD loads cc_proto_library from rules_cc, which rules_python's
    # rules_cc no longer has: protobuf sees the rules_cc it was released with.
    repo_mapping = {"@rules_cc": "@protobuf_rules_cc"},
    sha256 = "22fdaf641b31655d4b2297f9981fa5203b2866f8332d3c6333f6b0107bb320de",
    strip_prefix = "protobuf-21.12",
    urls = ["https://github.com/protocolbuffers/protobuf/archive/refs/tags/v21.12.tar.gz"],
)

http_archive(
    name = "protobuf_rules_cc",
    sha256 = "0adbd6f567291ad526e82c765e15aed33cea5e256eeba129f1501142c2c56610",
    strip_prefix = "rules_cc-818289e5613731ae410efb54218a4077fb9dbb03",
    urls = ["https://github.com/bazelbuild/rules_cc/archive/818289e5613731ae410efb54218a4077fb9dbb03.zip"],
)

http_archive(
    name = "zlib",
    build_file = "@com_google_protobuf//:third_party/zlib.BUILD",
    sha256 = "d14c38e313afc35a9a8760dadf26042f51ea0f5d154b0630a31da0540107fb98",
    strip_prefix = "zlib-1.2.13",
    urls = ["https://github.com/madler/zlib/releases/download/v1.2.13/zlib-1.2.13.tar.xz"],
)

http_archive(
    name = "rules_pkg",
    sha256 = "8a298e832762eda1830597d64fe7db58178aa84cd5926d76d5b744d6558941c2",
    urls = ["https://github.com/bazelbuild/rules_pkg/releases/download/0.7.0/rules_pkg-0.7.0.tar.gz"],
)

# The same two calls a consumer makes; see bazel/deps.bzl.
load("//bazel:deps.bzl", "mx_dependencies")

mx_dependencies()

load("//bazel:setup.bzl", "mx_setup")

mx_setup()

# compile_commands.json for clangd: bazel run //compdb (see compdb/BUILD).
# Development only; consumers of @mx do not need it. Declared after
# mx_dependencies() so that our pinned dependencies win over its own.
http_archive(
    name = "hedron_compile_commands",
    sha256 = "1b08abffbfbe89f6dbee6a5b33753792e8004f6a36f37c0f72115bec86e68724",
    strip_prefix = "bazel-compile-commands-extractor-abb61a688167623088f8768cc9264798df6a9d10",
    url = "https://github.com/hedronvision/bazel-compile-commands-extractor/archive/abb61a688167623088f8768cc9264798df6a9d10.tar.gz",
)

load("@hedron_compile_commands//:workspace_setup.bzl", "hedron_compile_commands_setup")

hedron_compile_commands_setup()

load("@hedron_compile_commands//:workspace_setup_transitive.bzl", "hedron_compile_commands_setup_transitive")

hedron_compile_commands_setup_transitive()

load("@hedron_compile_commands//:workspace_setup_transitive_transitive.bzl", "hedron_compile_commands_setup_transitive_transitive")

hedron_compile_commands_setup_transitive_transitive()

load("@hedron_compile_commands//:workspace_setup_transitive_transitive_transitive.bzl", "hedron_compile_commands_setup_transitive_transitive_transitive")

hedron_compile_commands_setup_transitive_transitive_transitive()

# pybind11-stubgen, the stub generator for the native extension (tools/BUILD):
# a pure-Python wheel, unpacked as a py_library, for this workspace's own
# checks only, like the rest of this section.
http_archive(
    name = "pybind11_stubgen_wheel",  # not "pybind11_stubgen": a repository named like the package would shadow it
    build_file = "//bazel:pybind11_stubgen.BUILD",
    sha256 = "10824cd2fc5cbbee032b8fb39e6f6c08de232deb309bc66d786a6c6e8a4601bd",
    type = "zip",
    url = "https://files.pythonhosted.org/packages/4f/e4/4f2d41881fae547f06ffa4c9748fa8ed13c4db5c830ff268b84175546b3c/pybind11_stubgen-2.5.5-py3-none-any.whl",
)

# Rust (rust/README.md): rules_rust 0.27.0, the last release for Bazel
# 6.2.x, development only like the rest of this section, so a consumer of
# @mx never fetches it. A C++ build fetches only this archive and the
# toolchains' stubs; the compiler and the crates come when a Rust target is
# built.
#
# platforms is declared here so that its version is ours, the one
# rules_rust asks for: without rules_rust Bazel 6.2.1 used a copy of its own.
http_archive(
    name = "platforms",
    sha256 = "3a561c99e7bdbe9173aa653fd579fe849f1d8d67395780ab4770b1f381431d51",
    urls = ["https://github.com/bazelbuild/platforms/releases/download/0.0.7/platforms-0.0.7.tar.gz"],
)

http_archive(
    name = "rules_rust",
    # 0.27.0 turned experimental_use_coverage_metadata_files on by default,
    # which passes instrumented_files_info(metadata_files = ...), a parameter
    # Bazel 6.2.1 does not have; the patch turns it back off.
    patch_cmds = [
        "sed -i '/name = \"experimental_use_coverage_metadata_files\"/{n;s/True/False/}' rust/settings/BUILD.bazel",
    ],
    sha256 = "db89135f4d1eaa047b9f5518ba4037284b43fc87386d08c1d1fe91708e3730ae",
    urls = ["https://github.com/bazelbuild/rules_rust/releases/download/0.27.0/rules_rust-v0.27.0.tar.gz"],
)

load("@rules_rust//rust:repositories.bzl", "rules_rust_dependencies", "rust_register_toolchains")

rules_rust_dependencies()

# The compiler rust-toolchain.toml pins for cargo, with its digests
# from https://static.rust-lang.org/dist/channel-rust-1.99.0.toml: rules_rust
# 0.27.0 knows them only up to 1.72.0 and would download unverified.
rust_register_toolchains(
    edition = "2021",
    sha256s = {
        "rustc-1.99.0-x86_64-unknown-linux-gnu.tar.gz": "238e72b8617f79bc96f27a5bfeb1a208b5755fe1a48f8bc190fe403ef6d54ab8",
        "cargo-1.99.0-x86_64-unknown-linux-gnu.tar.gz": "c2b8ba1f59e7a230aa5522684f3f7aa620b98e5ad37683a5e5cf9b41f534fa1e",
        "clippy-1.99.0-x86_64-unknown-linux-gnu.tar.gz": "4b3b972738f6b8e0e55065e273a972cc25fccf53629b4308eec67d6a66a8cf4a",
        "rust-std-1.99.0-x86_64-unknown-linux-gnu.tar.gz": "1dcaa01beb6bc78fdb13815b4f15214719167c58e3d79fbdf214591f8c195ee1",
        "llvm-tools-1.99.0-x86_64-unknown-linux-gnu.tar.gz": "128135fdcb3dec851861c952c1d9f97159446387860d5d5a633b7dcc3a1d4997",
        "rust-src-1.99.0.tar.gz": "82b978093b33c71bcbe69005b92181bfb76d3d0e434ef303e637812e21badcd7",
    },
    versions = ["1.99.0"],
)

load("@rules_rust//crate_universe:repositories.bzl", "crate_universe_dependencies")

crate_universe_dependencies()

# The release's own cargo-bazel, which rust/vendor.sh runs; fetched only then.
http_file(
    name = "cargo_bazel_prebuilt",
    executable = True,
    sha256 = "76cd8f797b42ac8297f903d2d0165d0cb0072e08c10fa9c0ac49593ef66d516e",
    urls = ["https://github.com/bazelbuild/rules_rust/releases/download/0.27.0/cargo-bazel-x86_64-unknown-linux-gnu"],
)

# The crates rust/vendor.sh vendored: one http_archive per crate, each
# fetched when a target needs it.
load("//rust/3rdparty/crates:crates.bzl", "crate_repositories")

crate_repositories()
