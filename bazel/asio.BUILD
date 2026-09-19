# Standalone Asio, header-only, as @asio//:asio; ASIO_STANDALONE keeps it
# Fetched by mx_dependencies() in deps.bzl.
cc_library(
    name = "asio",
    hdrs = glob([
        "asio/include/asio.hpp",
        "asio/include/asio/**/*.hpp",
        "asio/include/asio/**/*.ipp",
    ]),
    defines = ["ASIO_STANDALONE"],
    includes = ["asio/include"],
    visibility = ["//visibility:public"],
)
