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
| network access on the first build | Bazel fetches Asio, pybind11, googletest and the rulesets pinned in `bazel/deps.bzl`; nothing else is downloaded, and every later build is offline | `ca-certificates`, `curl` for Bazelisk itself |

On Debian or Ubuntu, that is:

```
sudo apt-get install g++ protobuf-compiler libprotobuf-dev python3-dev python3-protobuf ca-certificates curl
sudo curl -fsSL -o /usr/local/bin/bazel https://github.com/bazelbuild/bazelisk/releases/download/v1.20.0/bazelisk-linux-amd64
sudo chmod +x /usr/local/bin/bazel
```

Nothing else: no Asio, pybind11 or googletest packages, no JDK, no
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
system packages. A consuming workspace compiles its own `.proto` files
with `@mx//:protoc` and links them against `@mx//:protobuf_runtime`, with
a `genrule` as the multiplexer's own are built, so that its generated
code and the library's come from one `protoc` and link one runtime,
whichever the flags select.

### Build configurations

`bazel build //...` works with no flags. `.bazelrc` adds `--config=debug`
(debug info, no optimisation), `--config=release` (optimised, stripped
binaries next to the ones with symbols), `--config=asan` and
`--config=tsan` (the sanitizers), and `--config=clang` (the static
thread-safety analysis). `./check.sh` runs what CI should.

## Without Bazel

A `Makefile` at the root builds the same things from the distribution's own
packages, for a machine that will not have Bazel: no fetching, no JDK, no
sandbox. It needs the compiler and protobuf from the table above, plus
Asio (standalone, header-only), pybind11 (2.9, Ubuntu 22.04's, is the
oldest tested) and, for the tests, googletest:

```
sudo apt-get install g++ make protobuf-compiler libprotobuf-dev libasio-dev \
    python3-dev python3-protobuf pybind11-dev python3-pybind11 libgtest-dev \
    python3-pip python3-setuptools python3-wheel
make -j                      # build/bin/mxcontrol, build/libmultiplexer.a, build/python/
make check                   # the C++ and Python unit tests, against what was built
make wheel                   # build/dist/mx_multiplexer-<version>-<python>-<platform>.whl, mxcontrol inside, stripped
sudo make install            # mxcontrol, the library and the headers under /usr/local
make RULES=your.rules -j     # the constants from your rules file, as --//:multiplexer_rules does
```

What comes out, and how a program uses it:

| Output | Use |
|---|---|
| `build/bin/mxcontrol` | the multiplexer and its subcommands; `make install` puts it in `PREFIX/bin` |
| `build/libmultiplexer.a` and the headers, under `PREFIX/include/mx` after `make install` | a C++ program compiles and links with `$(pkg-config --cflags --libs multiplexer)`, which `make install` also puts under `PREFIX/lib/pkgconfig`, together with `generate_constants` for your own rules file; the generated `multiplexer/multiplexer.constants.h` for the rules file the build used is among the headers |
| `build/python/` | the `multiplexer` package importable with `PYTHONPATH=build/python`, extension and `mxcontrol` included |
| `build/dist/*.whl` | `pip install` it: the package and the `mxcontrol` command; the package needs only `protobuf`, 3.20 or newer. The wheel also carries `lib.logging`, one generated module the package imports |

`make check` runs the C++ unit tests with googletest and the Python ones
with `unittest`, in a build of its own under `build/check` whose constants
come from `tests/testing.rules`, the system rules plus the tests' types, so
that nothing built for installing or packaging carries those. The tests
that start a multiplexer run the one `multiplexer.testing` finds: the
binary `MXCONTROL` names when it is set, else the package's own
`mxcontrol`, which `build/python` holds, so a test of yours outside Bazel
starts real multiplexers with nothing set. `make/` holds the pieces:
`sources.mk`, the source lists generated from the BUILD files by
`./format.sh` (so the two builds cannot disagree about which files
exist), `setup.py` and `pyproject.toml` for the wheel, and
`wheel_smoke.py`, which `docker/check.sh make` runs with the wheel
installed in a fresh virtual environment, once with the wheel's
`mxcontrol` and once with `MXCONTROL=build/bin/mxcontrol`. The wheel asks for the protobuf runtime its generated modules need, from the `protoc` that made them: `protobuf>=3.20` from protoc 3.20 on, as the released wheels have it, and a range below 4 for an older one, whose code protobuf 4 refuses. `VERSION=1.2.3 make wheel` names the wheel; `CXXFLAGS`,
`PREFIX`, `PYTHON`, `PROTOC` and `STRIP` are variables like `RULES`.

Not built this way: the test roles and scenarios under `tests/`, the
examples, the sanitizer and analysis configurations. Those are Bazel's.

## Development tools

Only for working on the repository, never for building it. The Python
ones, black, ruff, pyright and PyYAML, are pinned in
[requirements-dev.txt](../requirements-dev.txt), and `./format.sh` and
`./check.sh` install them into `.tools/venv` the first time they run and
whenever that file changes, with `python3-venv` and the network. The rest
come from the system: `clang-format-18` and `buildifier` for
`./format.sh`; `clang-18` for the thread-safety analysis build
(`--config=clang`); Node, whose `npx` runs mermaid-cli for the diagram check
and on which pyright runs; Docker for [docker/check.sh](../docker/check.sh).
The type check reads the stubs of the generated modules from `bazel-bin`
(`stubPath` in `pyproject.toml`), where the build writes them next to the
modules, and the stub of the native extension from `bazel build
//multiplexer:_native_pyi`, which Bazel makes with pybind11-stubgen fetched
as a wheel; Pylance in VS Code reads the same configuration, so a fresh
checkout type-checks after one build. `make` writes the same stubs into
`build/`, and `make wheel` ships them with a `py.typed` marker; the
extension's stub needs `pybind11-stubgen` importable by `PYTHON` there and
is left out with no other consequence when it is not, and the stubs of
the protocol buffer modules need a `protoc` of 3.20 or newer, which
Ubuntu 22.04's 3.12 is not, so a package built there has none of those
either. Bazel builds with such a `protoc` too:
[bazel/system_protoc.sh](../bazel/system_protoc.sh), the default
`//:protoc`, then leaves out the flag that asks for those stubs and writes
in their place stubs that make every name of the module `Any`.
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
