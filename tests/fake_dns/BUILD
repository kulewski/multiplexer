# mxcontrol_fake_dns: mxcontrol's recording command with the names under
# .test looked up in a file a scenario rewrites, for the scenarios in which
# a name moves to another address; see the source's comment.
cc_binary(
    name = "mxcontrol_fake_dns",
    testonly = True,
    srcs = ["mxcontrol_fake_dns.cc"],
    linkopts = ["-ldl"],  # dlsym, in libdl before glibc 2.34
    visibility = ["//tests:__subpackages__"],
    deps = [
        "//lib:program",
        "//mxcontrol:driver",
        "//mxcontrol:help",
        "//mxcontrol:recording_control",
    ],
)
