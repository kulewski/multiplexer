# Packaging: what a release ships

Every version tag produces the same set of artifacts from one commit,
built and checked by `.github/workflows/release.yml` and attached to the
GitHub release. The scripts under `packaging/` build each of them locally
the same way, in Docker, so a release can be reproduced on a workstation.

| Artifact | For | Needs on the machine |
|---|---|---|
| `mxcontrol-<version>-linux-amd64` with its `.sha256` | running a multiplexer, or its tools, on any Linux | nothing: statically linked |
| `ghcr.io/kulewski/multiplexer:<version>` | running a multiplexer in a container | a container runtime |
| `multiplexer_<version>~<codename>_amd64.deb`, one per Debian and Ubuntu release | mxcontrol as a system package, and building C++ peers | that release; `libprotobuf-dev` and `libasio-dev` to build against the library |
| `multiplexer-<version>-cp3XY-manylinux_2_28_x86_64.whl`, one per CPython from 3.10 | Python peers | `pip`; the `protobuf` package comes with it |
| the source archive GitHub makes for the tag | everything else, through Bazel or `make` | [building.md](building.md) |

Bazel consumers do not use any of these: `git_repository(tag = "v<version>")`
with `mx_dependencies()` and `mx_setup()` builds the library from source
in their workspace, as [examples/README.md](../examples/README.md) shows.

## The static mxcontrol

`bazel build //mxcontrol:mxcontrol_static` links the same binary as
`//mxcontrol` with `-static`: glibc, libstdc++ and protobuf inside, about
8 MB, and it runs on any x86_64 Linux and in an empty container. Static
glibc cannot resolve host names without the running system's name-service
modules, so inside the image, or on a system with a different glibc, give
mxcontrol's client subcommands addresses rather than names. The
multiplexer itself listens on an address and never resolves one.

## The container image

`docker/BUILD` builds it with rules_oci, from the static binary and the
example rules file on `gcr.io/distroless/static-debian12`, which holds no
libc, no shell and no package manager: the only code in the image is
mxcontrol. It runs as `nonroot`, listens on 1980, and its command is

```
/mxcontrol run_multiplexer --rules /etc/mx/multiplexer.rules --address 0.0.0.0:1980
```

A deployment mounts its own rules file over `/etc/mx/multiplexer.rules`
and, for a recording directory or a log file, a writable volume:

```
docker run --rm -p 1980:1980 -v /etc/mx/deployment.rules:/etc/mx/multiplexer.rules:ro \
    ghcr.io/kulewski/multiplexer:<version>
```

Any other subcommand goes after the image name, `... multiplexer:<version>
help` for the list. Locally, `bazel run //docker:load` puts the image into
Docker as `multiplexer:dev`; `bazel run //docker:push` publishes it. The
targets are tagged `manual`, so a wildcard build never fetches the rules or
the base image, and rules_oci is declared in our WORKSPACE only, never for
a consumer of `@mx`.

## The Debian packages

`packaging/build_debs.sh` builds one package per supported release, each
inside that release's container: Debian 12 and 13, Ubuntu 22.04, 24.04 and
26.04. A package holds `mxcontrol` and `generate_constants` in `/usr/bin`,
`libmultiplexer.a` and the headers under `/usr/include/mx`, and
`/usr/lib/pkgconfig/multiplexer.pc`, so a C++ peer builds with

```
g++ -std=c++17 backend.cc $(pkg-config --cflags --libs multiplexer) -o backend
```

There is one package per release because protobuf C++ promises no
compatibility between versions, not even ABI stability between micro
releases: the library is compiled against the release's `libprotobuf-dev`,
and a program using it must be too. `Recommends` names the exact version.
The library itself depends only on the reserved peer and message types,
ids 1 to 99 ([rules.md](rules.md)), which every rules file carries as
shipped; your own types come from `generate_constants your.rules
multiplexer/multiplexer.constants.h`, placed on the include path before
the package's copy. Changing the reserved block is not supported.

## The wheels

`packaging/build_wheels.sh` builds them in the `manylinux_2_28` container:
protobuf 3.21.12 compiled once from source and linked statically into the
extension, then `make wheel` per CPython and `auditwheel`, which verifies
that the wheel needs nothing from the system beyond what manylinux allows.
`pip install multiplexer-<version>-cp312-manylinux_2_28_x86_64.whl` is
then the whole installation; the package depends on `protobuf` from PyPI.
The wheel includes `multiplexer.testing`, the test harness, with the
example rules file it defaults to; a `Cluster` needs `MXCONTROL` in the
environment pointing at a multiplexer binary, the static one for example.

## Cutting a release

1. Bump the version in `Makefile`, `multiplexer/release.py` and
   `lib/release.cc`, in one commit.
2. Tag it: `git tag -a v<version> -m "..."` and `git push origin main v<version>`.
3. The workflow runs the tests, builds every artifact, pushes the image and
   creates the release with the files attached.

To try the workflow before tagging, start it by hand from the Actions tab,
or with `gh workflow run release.yml -f tag=v<version>`: the same build
from the chosen branch, the files named after the tag given, without the
push to ghcr.io and without a release. A failed run is re-run from its
page once the fix is on the branch; the tag never moves.

To rebuild any artifact by hand: `bazel build //mxcontrol:mxcontrol_static`,
`bazel run //docker:load`, `packaging/build_debs.sh`, `packaging/build_wheels.sh`.
