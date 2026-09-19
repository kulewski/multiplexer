# The container image of the multiplexer: the static mxcontrol and the
# example rules file on distroless static, running as nonroot.
#
#   bazel run //docker:load          # into the local docker as multiplexer:dev
#   bazel run //docker:push          # to ghcr.io, tagged with the version
#
# Every target is manual: the release workflow names them, a wildcard
# build does not. rules_oci is declared in WORKSPACE only,
# so consumers of @mx never fetch it.
load("@rules_oci//oci:defs.bzl", "oci_image", "oci_push", "oci_tarball")

# The one layer: /mxcontrol and /etc/mx/multiplexer.rules, owned by root,
# with fixed times and order so that the same inputs give the same layer.
genrule(
    name = "layer",
    srcs = [
        "//mxcontrol:mxcontrol_static",
        "//:multiplexer.rules",
    ],
    outs = ["layer.tar"],
    cmd = "root=$$(mktemp -d) && mkdir -p $$root/etc/mx && " +
          "cp $(location //mxcontrol:mxcontrol_static) $$root/mxcontrol && " +
          "cp $(location //:multiplexer.rules) $$root/etc/mx/multiplexer.rules && " +
          "chmod 755 $$root/mxcontrol && chmod 644 $$root/etc/mx/multiplexer.rules && " +
          "tar --format=ustar --owner=0 --group=0 --numeric-owner --mtime=@0 --sort=name " +
          "-C $$root -cf $@ mxcontrol etc && rm -rf $$root",
    tags = ["manual"],
)

oci_image(
    name = "image",
    base = "@distroless_static",
    cmd = [
        "run_multiplexer",
        "--rules",
        "/etc/mx/multiplexer.rules",
        "--address",
        "0.0.0.0:1980",
    ],
    entrypoint = ["/mxcontrol"],
    exposed_ports = ["1980/tcp"],
    tags = ["manual"],
    tars = [":layer"],
    user = "nonroot",
)

oci_tarball(
    name = "load",
    image = ":image",
    repo_tags = ["multiplexer:dev"],
    tags = ["manual"],
)

oci_push(
    name = "push",
    image = ":image",
    remote_tags = ["latest"],
    repository = "ghcr.io/kulewski/multiplexer",
    tags = ["manual"],
)
