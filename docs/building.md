# Building

What the build needs, which systems it is tested on, how to point it at a
protobuf of your own, and how to prove all of that on a clean machine with
Docker. The commands themselves are in the [README](../README.md#building-with-bazel).

## With Bazel

### What the system needs

| Need | Why | Debian and Ubuntu packages |
|---|---|---|
| Bazel 6.2.1 | the build; [Bazelisk](https://github.com/bazelbuild/bazelisk) reads `.bazelversion` and fetches exactly that Bazel, which brings its own JDK | a `bazel` on `PATH`: the Bazelisk binary renamed |
| a C++17 compiler | the core, the tools and the Python extension; tested with gcc 11.4 to 14.2 and clang 18 | `g++` |
| protoc and libprotobuf, the same version | the wire format and the recording are protocol buffers; the generated code must match the runtime it links | `protobuf-compiler libprotobuf-dev` |
| the Python protobuf runtime | the generated Python modules | `python3-protobuf` |
| Python 3.10 or newer with headers | the extension (`pybind11`) and the Python library | `python3-dev` |
| network access on the first build | Bazel fetches Boost, pybind11, googletest and the rulesets pinned in `bazel/deps.bzl`; nothing else is downloaded, and every later build is offline | `ca-certificates`, `curl` for Bazelisk itself |

On Debian or Ubuntu, that is:

```
sudo apt-get install g++ protobuf-compiler libprotobuf-dev python3-dev python3-protobuf ca-certificates curl
sudo curl -fsSL -o /usr/local/bin/bazel https://github.com/bazelbuild/bazelisk/releases/download/v1.20.0/bazelisk-linux-amd64
sudo chmod +x /usr/local/bin/bazel
```

Nothing else: no Boost, pybind11 or googletest packages, no JDK, no
`unzip`. glibc 2.33 or newer gives exact heap statistics (`mallinfo2`);
older ones fall back to `mallinfo`, which only affects the memory numbers
in the soak tests.

### Tested systems

Each row is [docker/check.sh](../docker/check.sh) on that distribution with
the packages above and nothing else: `bazel build //...`, the fast tests,
and the echo example workspace.

| System | Compiler | protobuf | Python | glibc | Result |
|---|---|---|---|---|---|
| Debian 12 | gcc 12.2 | 3.21 | 3.11 | 2.36 | everything passes |
| Debian 13 | gcc 14.2 | 3.21 | 3.13 | 2.41 | everything passes |
| Ubuntu 24.04 | gcc 13.3 | 3.21 | 3.12 | 2.39 | everything passes |
| Ubuntu 22.04 | gcc 11.4 | 3.12 | 3.10 | 2.35 | everything passes |

### Python versions

Python 3.10 is the oldest supported; the code uses its syntax for type
unions and needs nothing newer. One thing was arranged for it: no package
under `multiplexer/` may share a name with a standard library module,
because before 3.11 a script's own directory comes first on the module
path and a test or a tool in `multiplexer/` would import that package
instead. The multiplexer's logging API is `multiplexer.mxlog` for that
reason.

### Bringing your own protoc and libprotobuf

By default `protoc` comes from `PATH` and the runtime is the system
`libprotobuf`, through two label flags at the root. A workspace that
carries its own protobuf, for example because another library in the same
binary links a specific version, points both flags at its targets, and the
generated code then comes from that `protoc` and links that runtime:

```
build --@mx//:protoc=//third_party/protobuf:protoc
build --@mx//:protobuf_runtime=//third_party/protobuf:libprotobuf
```

Inside this repository the flags are `--//:protoc` and
`--//:protobuf_runtime`. The two must match each other, as with the
system packages.

### Build configurations

`bazel build //...` works with no flags. `.bazelrc` adds `--config=debug`
(debug info, no optimisation), `--config=release` (optimised, stripped
binaries next to the ones with symbols), `--config=asan` and
`--config=tsan` (the sanitizers), and `--config=clang` (the static
thread-safety analysis). `./check.sh` runs what CI should.

## Development tools

Only for working on the repository, never for building it: `clang-format-18`,
`black` and `buildifier` for `./format.sh`; `clang-18` for the thread-safety
analysis build (`--config=clang`); mermaid-cli, through `npx`, for the
diagram check; Docker for [docker/check.sh](../docker/check.sh).
[AGENTS.md](../AGENTS.md) lists the commands.

## Checking on a clean machine

```
./docker/check.sh                    # debian:12
./docker/check.sh bazel ubuntu:24.04
```

builds an image from the files git knows about, with exactly the packages
in the table, and runs the build, the fast tests and the example workspace
in it. It is how the table above was made; run it before changing what the
build needs.
