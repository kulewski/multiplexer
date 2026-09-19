"""External repositories the multiplexer needs.

Call from a WORKSPACE, then load and call mx_setup() from //bazel:setup.bzl:

    local_repository(name = "mx", path = "../..")   # or git_repository / http_archive
    load("@mx//bazel:deps.bzl", "mx_dependencies")
    mx_dependencies()
    load("@mx//bazel:setup.bzl", "mx_setup")
    mx_setup()

Two stages are needed because the second one loads from repositories the
first one declares. Every repository is declared with maybe(), so a workspace
that already has its own pin of one of them keeps it: whichever declaration
runs first wins, ours or yours, in either order.

What this repository needs from each: rules_python only for
py_repositories() (the targets use Bazel's native py_* rules), so any
rules_python from 0.1.0 on works, and mx_setup(python = False) skips the
call when another ruleset already made it; standalone Asio for the io layer;
pybind11 and pybind11_bazel for the Python extension; bazel_skylib
for the build flags in the root BUILD. protoc and libprotobuf are not
declared here: they come from the system by default, or from the targets
//:protoc and //:protobuf_runtime are pointed at.
"""

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:utils.bzl", "maybe")

PYBIND11_BAZEL_TAG = "c65db0ac44ff3cd790a803e4a804e2cc806cf641"
PYBIND11_TAG = "b389ae77cb8a872d62e96d5f38ce020480140033"
ASIO_TAG = "asio-1-30-2"

def mx_dependencies(python = True):
    """Declares every repository the multiplexer needs, with maybe(), so a
    workspace's own pin of any of them wins. `python = False` leaves out
    rules_python, pybind11 and pybind11_bazel, which only the Python
    extension needs: a C++-only consumer then fetches Asio and bazel_skylib
    and nothing else. Pair it with mx_setup(python = False).
    """
    maybe(
        http_archive,
        name = "bazel_skylib",
        sha256 = "66ffd9315665bfaafc96b52278f57c7e2dd09f5ede279ea6d39b2be471e7e3aa",
        url = "https://github.com/bazelbuild/bazel-skylib/releases/download/1.4.2/bazel-skylib-1.4.2.tar.gz",
    )
    if python:
        maybe(
            http_archive,
            name = "rules_python",
            sha256 = "2f5c284fbb4e86045c2632d3573fc006facbca5d1fa02976e89dc0cd5488b590",
            strip_prefix = "rules_python-1.6.3",
            url = "https://github.com/bazel-contrib/rules_python/releases/download/1.6.3/rules_python-1.6.3.tar.gz",
        )

    # Standalone Asio: the io layer of the library, header-only. Label()
    # resolves against this file's repository, so the build file is found
    # whether we are the main workspace or @mx.
    maybe(
        http_archive,
        name = "asio",
        build_file = Label("//bazel:asio.BUILD"),
        canonical_id = ASIO_TAG,
        sha256 = "755bd7f85a4b269c67ae0ea254907c078d408cce8e1a352ad2ed664d233780e8",
        strip_prefix = "asio-{tag}".format(tag = ASIO_TAG),
        urls = ["https://github.com/chriskohlhoff/asio/archive/refs/tags/{tag}.tar.gz".format(tag = ASIO_TAG)],
    )
    if python:
        maybe(
            http_archive,
            name = "pybind11_bazel",
            canonical_id = PYBIND11_BAZEL_TAG,
            sha256 = "59d00e3b9ae8a4c141241b0ad73ab905938fbd05fd8ada41bcab5804a73ced7f",
            strip_prefix = "pybind11_bazel-{tag}".format(tag = PYBIND11_BAZEL_TAG),
            urls = ["https://github.com/pybind/pybind11_bazel/archive/{tag}.zip".format(tag = PYBIND11_BAZEL_TAG)],
        )
        maybe(
            http_archive,
            name = "pybind11",
            build_file = "@pybind11_bazel//:pybind11.BUILD",
            canonical_id = PYBIND11_TAG,
            sha256 = "2c4b17d35ecb6255099a7d5ace357c850f8f37f6d3bf45fde4a141ede1fa1122",
            strip_prefix = "pybind11-{tag}".format(tag = PYBIND11_TAG),
            urls = ["https://github.com/pybind/pybind11/archive/{tag}.tar.gz".format(tag = PYBIND11_TAG)],
        )

    # Only our own unit tests use it; http_archive is lazy, so a consumer that
    # never references it never downloads it.
    maybe(
        http_archive,
        name = "com_google_googletest",
        sha256 = "8ad598c73ad796e0d8280b082cebd82a630d73e73cd3c70057938a6501bba5d7",
        strip_prefix = "googletest-1.14.0",
        urls = ["https://github.com/google/googletest/archive/refs/tags/v1.14.0.tar.gz"],
    )
