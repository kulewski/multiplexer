"""Ship stripped binaries from a build that keeps debug symbols.

Release builds compile with `-g` (see .bazelrc) so that the binary with
symbols exists for debugging cores and profiles. What gets installed and run
should be small, though, so every shipped binary is built twice over:

  <name>_with_debug_symbols   the real cc_binary (or cc_binary-like target)
  <name>                      maybe_strip: a stripped copy in release builds,
                              a symlink to the unstripped one otherwise

Stripping only in release keeps debug and fastbuild cycles free of an extra
action. The toolchain's own `strip` is used, so cross-compilation and custom
toolchains work.
"""

load("@bazel_tools//tools/cpp:toolchain_utils.bzl", "find_cpp_toolchain", "use_cpp_toolchain")
load("@bazel_tools//tools/build_defs/cc:action_names.bzl", "STRIP_ACTION_NAME")

def _maybe_strip_impl(ctx):
    binary = ctx.attr.binary[DefaultInfo]
    unstripped = binary.files_to_run.executable
    output_file = ctx.actions.declare_file(ctx.label.name)
    if ctx.attr.strip:
        cc_toolchain = find_cpp_toolchain(ctx)
        feature_configuration = cc_common.configure_features(
            ctx = ctx,
            cc_toolchain = cc_toolchain,
            requested_features = ctx.features,
            unsupported_features = ctx.disabled_features,
        )
        strip_tool_path = cc_common.get_tool_for_action(
            feature_configuration = feature_configuration,
            action_name = STRIP_ACTION_NAME,
        )
        ctx.actions.run(
            executable = strip_tool_path,
            arguments = ["-o", output_file.path, unstripped.path],
            inputs = depset(direct = [unstripped], transitive = [cc_toolchain.all_files]),
            outputs = [output_file],
            mnemonic = "Strip",
            progress_message = "Stripping %s" % output_file.short_path,
        )
    else:
        ctx.actions.symlink(output = output_file, target_file = unstripped, is_executable = True)

    # The wrapped target's runfiles include the unstripped executable itself;
    # leave it out so a release install does not carry the symbols after all.
    runfiles = ctx.runfiles(
        files = [f for f in binary.default_runfiles.files.to_list() if f != unstripped],
        root_symlinks = binary.default_runfiles.root_symlinks,
        symlinks = binary.default_runfiles.symlinks,
    )
    return [DefaultInfo(executable = output_file, runfiles = runfiles)]

maybe_strip = rule(
    doc = """An executable that is `binary` stripped of symbols when `strip` is
true, and a symlink to `binary` otherwise. Runfiles are the wrapped target's,
minus the unstripped executable. Also usable for shared objects, as
multiplexer/BUILD does for the Python extension.""",
    attrs = {
        "binary": attr.label(
            doc = "The executable target to strip or forward.",
            mandatory = True,
            executable = True,
            cfg = "target",
        ),
        "strip": attr.bool(
            doc = "Strip when true; usually a select() on the compilation mode.",
            mandatory = True,
        ),
        # Bazel 6 resolves the C++ toolchain through this attribute unless
        # --incompatible_enable_cc_toolchain_resolution is set; without it
        # find_cpp_toolchain() fails whenever strip = True (release builds).
        "_cc_toolchain": attr.label(default = Label("@bazel_tools//tools/cpp:current_cc_toolchain")),
    },
    executable = True,
    implementation = _maybe_strip_impl,
    fragments = ["cpp"],
    toolchains = use_cpp_toolchain(),
)

# The select() every shipped binary uses: strip in release builds only.
STRIP_IN_RELEASE = select({
    "//:release_build": True,
    "//conditions:default": False,
})

def maybe_strip_cc_binary(name, visibility = ["//visibility:private"], **kwargs):
    """A cc_binary `name` that is stripped in release builds.

    Declares `name + "_with_debug_symbols"` as the cc_binary with `kwargs`,
    and `name` as its maybe_strip wrapper. Both get `visibility`.
    """
    native.cc_binary(
        name = name + "_with_debug_symbols",
        visibility = visibility,
        **kwargs
    )
    maybe_strip(
        name = name,
        binary = ":" + name + "_with_debug_symbols",
        strip = STRIP_IN_RELEASE,
        visibility = visibility,
    )
