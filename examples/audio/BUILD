# The audio example's Bazel side: the rules file the constants come from,
# and the protocol buffer the worker and the gateway share, compiled the
# way the library compiles its own, with the protoc @mx points at. The
# worker is under worker/; the gateway is Python, installed with pip.
exports_files(["audio.rules"])

genrule(
    name = "audio_proto",
    srcs = ["audio.proto"],
    outs = [
        "audio.pb.cc",
        "audio.pb.h",
    ],
    cmd = "$(location @mx//:protoc) -I $$(dirname $(location audio.proto)) --cpp_out=$(RULEDIR) $(location audio.proto)",
    tools = ["@mx//:protoc"],
)

cc_library(
    name = "audio_cc_proto",
    srcs = ["audio.pb.cc"],
    hdrs = ["audio.pb.h"],
    visibility = ["//worker:__pkg__"],
    deps = ["@mx//:protobuf_runtime"],
)
