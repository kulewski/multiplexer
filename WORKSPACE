# The Bazel workspace, named mx: our dependencies via bazel/deps.bzl, the
# same way a consumer of @mx declares them, plus development-only tools.
workspace(name = "mx")

# The same two calls a consumer makes; see bazel/deps.bzl.
load("//bazel:deps.bzl", "mx_dependencies")

mx_dependencies()

load("//bazel:setup.bzl", "mx_setup")

mx_setup()

# compile_commands.json for clangd: bazel run //compdb (see compdb/BUILD).
# Development only; consumers of @mx do not need it. Declared after
# mx_dependencies() so that our pinned dependencies win over its own.
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

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
