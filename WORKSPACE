# The Bazel workspace, named mx: our dependencies via bazel/deps.bzl, the
# same way a consumer of @mx declares them, plus development-only tools.
workspace(name = "mx")

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

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

# The container image: bazel run //docker:load (see docker/BUILD). Release
# tooling, development only, and the image targets are tagged manual, so
# neither a consumer of @mx nor a plain `bazel build //...` touches it.
http_archive(
    name = "rules_oci",
    sha256 = "46ce9edcff4d3d7b3a550774b82396c0fa619cc9ce9da00c1b09a08b45ea5a14",
    strip_prefix = "rules_oci-1.8.0",
    url = "https://github.com/bazel-contrib/rules_oci/releases/download/v1.8.0/rules_oci-v1.8.0.tar.gz",
)

load("@rules_oci//oci:dependencies.bzl", "rules_oci_dependencies")

rules_oci_dependencies()

load("@rules_oci//oci:repositories.bzl", "LATEST_CRANE_VERSION", "oci_register_toolchains")

oci_register_toolchains(
    name = "oci",
    crane_version = LATEST_CRANE_VERSION,
)

load("@rules_oci//oci:pull.bzl", "oci_pull")

# gcr.io/distroless/static-debian12:nonroot for linux/amd64, by digest: no
# libc, no shell, a nonroot user, /tmp, time zones and certificates.
oci_pull(
    name = "distroless_static",
    digest = "sha256:52dcfbabb7457ea47c82f6e13af8c8a4a1d9f7b0145142b3ecab20f2b888411d",
    image = "gcr.io/distroless/static-debian12",
)

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
