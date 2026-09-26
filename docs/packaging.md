# Packaging: what a release ships

Every version tag produces the same set of artifacts from one commit,
built and checked by `.github/workflows/release.yml` and attached to the
GitHub release. The scripts under `packaging/` build each of them locally
the same way, in Docker, so a release can be reproduced on a workstation.
Every binary in them is a release build, stripped: optimized, without the
debug assertions and without symbols. `packaging/check_binaries.sh` checks
each before it is published, and the binaries with symbols are not
published; [operations](operations.md#debug-symbols) says how to debug.

| Artifact | For | Needs on the machine |
|---|---|---|
| `mxcontrol-<version>-linux-amd64` with its `.sha256` | running a multiplexer, or its tools, on any Linux | nothing: statically linked |
| `ghcr.io/kulewski/multiplexer:<version>` | running a multiplexer in a container | a container runtime |
| `multiplexer_<version>~<codename>_amd64.deb`, one per Debian and Ubuntu release | mxcontrol as a system package, and building C++ peers | that release; `libprotobuf-dev` and `libasio-dev` to build against the library |
| `mx_multiplexer-<version>-cp3XY-cp3XY-manylinux_2_28_x86_64.whl`, one per CPython from 3.10 | Python peers, and `mxcontrol`: a multiplexer, `generate_rules`, `generate_constants`, the test harness's binary | `pip` on x86_64 Linux with glibc 2.28 or newer; the `protobuf` package comes with it |
| the source archive GitHub makes for the tag | everything else, through Bazel or `make` | [building.md](building.md) |

Bazel consumers do not use any of these: `git_repository(tag = "v<version>")`
with `mx_dependencies()` and `mx_setup()` builds the library from source
in their workspace, as [examples/README.md](../examples/README.md) shows.

## The static mxcontrol

`bazel build --config=release //mxcontrol:mxcontrol_static` links the same
binary as `//mxcontrol` with `-static` and strips it: glibc, libstdc++ and
protobuf inside, about 4 MB, and it runs on any x86_64 Linux and in an empty
container. Static
glibc cannot resolve host names without the running system's name-service
modules, so inside the image, or on a system with a different glibc, give
mxcontrol's client subcommands addresses rather than names. The
multiplexer itself listens on an address and never resolves one. The
wheels' `mxcontrol` is another link of the same source, against the
system's glibc, so it resolves names.

## The container image

`docker/BUILD` builds it with rules_oci, from the static binary and the
system rules file on `gcr.io/distroless/static-debian12`, which holds no
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

To edit the file under the running container, which the multiplexer
notices and puts in use ([changing the
rules](operations.md#changing-the-rules)), mount the directory, `-v
/etc/mx:/etc/mx:ro` with the file named `multiplexer.rules` in it: a file
mounted on its own is one inode, and an editor that saves by rename leaves
the container looking at the old one.

Any other subcommand goes after the image name, `... multiplexer:<version>
help` for the list. [Operations](operations.md#on-kubernetes) has the
StatefulSet that runs several of them on Kubernetes. Locally, `bazel run //docker:load` puts the image into
Docker as `multiplexer:dev`; `bazel run //docker:push` publishes it. The
targets are tagged `manual`, so a wildcard build never fetches the rules or
the base image, and rules_oci is declared in our WORKSPACE only, never for
a consumer of `@mx`.

## The Debian packages

`packaging/build_debs.sh` builds one package per supported release, each
inside that release's container: Debian 12 and 13, Ubuntu 22.04, 24.04 and
26.04. A package holds `mxcontrol` and `generate_constants` in `/usr/bin`,
both stripped, `libmultiplexer.a`, without its debug information, and the
headers under `/usr/include/mx`, and `/usr/lib/pkgconfig/multiplexer.pc`, so
a C++ peer builds with

```
g++ -std=c++17 backend.cc $(pkg-config --cflags --libs multiplexer) -o backend
```

There is one package per release because protobuf C++ promises no
compatibility between versions, not even ABI stability between micro
releases: the library is compiled against the release's `libprotobuf-dev`,
and a program using it must be too. `Recommends` names the exact version.
The library's constants are the system rules', which it uses by those
numbers whatever your file says ([rules.md](rules.md)); your rules file
starts from them, as `mxcontrol generate_rules your.rules` writes them, and
your own types' constants come from `mxcontrol generate_constants your.rules
--cxx multiplexer/multiplexer.constants.h`, placed on the include path
before the package's copy (`generate_constants your.rules
multiplexer/multiplexer.constants.h`, the build-time tool the package also
holds, writes the same). Changing a system entry's number or name is not
supported.

## The wheels

`packaging/build_wheels.sh` builds them in the `manylinux_2_28` container:
protobuf 3.21.12 compiled once from source and linked statically into the
extension and into `mxcontrol`, which is linked once and is the same file
in every CPython's wheel, then `make wheel` per CPython, which strips both,
and `auditwheel`, which verifies that the wheel needs nothing from the
system beyond what manylinux allows and tags it `manylinux_2_28`. Each
wheel is then installed into a fresh environment of its CPython and
smoke-tested (`make/wheel_smoke.py`) at that glibc, the oldest it promises.
A wheel is about 2.3 MB.

`pip install mx-multiplexer` installs them from PyPI, where every release
is published under that name, because `multiplexer` on PyPI belongs to an
unrelated package; the import is `multiplexer` all the same. `pip install
mx_multiplexer-<version>-cp312-cp312-manylinux_2_28_x86_64.whl` installs
the file from the release page instead. Either way that is the whole
installation; the package depends on `protobuf` from PyPI. It carries:

- `mxcontrol`, the multiplexer and its tools, which pip puts on the
  environment's PATH as the `mxcontrol` command: the command runs the
  package's binary in its own place, so its pid, signals and exit status
  are the multiplexer's. `python -m multiplexer.mxcontrol` does the same
  without PATH, `multiplexer.mxcontrol.binary_path()` names the binary for
  a program or a supervisor that runs it directly, and `pipx install
  mx-multiplexer` or `uv tool install mx-multiplexer` give a command
  outside any environment. A Debian package's `/usr/bin/mxcontrol` comes
  after an active environment's on PATH.
- `multiplexer.testing`, the test harness: a `Cluster` names its rules
  file and runs the package's `mxcontrol`, or the binary `MXCONTROL` names
  when it is set ([testing](api_python.md#testing)).
- The constants of the system rules; those of your own rules file, the
  `peers` and `types` a Bazel build generates, come from the same
  `mxcontrol`: `mxcontrol generate_rules your.rules` writes the file to
  start from, and `mxcontrol generate_constants your.rules --python
  multiplexer_constants.py --pyi multiplexer_constants.pyi` the constants,
  once, and again when the file changes
  ([mxcontrol.md](mxcontrol.md#generate_constants)).
The package is typed: a `py.typed` marker and a stub next to every
generated module and the extension, so Pylance and pyright check code
against it without any setup.

## Cutting a release

1. Bump the version in `Makefile`, `multiplexer/release.py` and
   `lib/release.cc`, in one commit.
2. Tag it: `git tag -a v<version> -m "..."` and `git push origin main v<version>`.
3. The workflow runs the tests, builds every artifact, pushes the image and
   creates the release with the files attached.
4. Approve the upload to PyPI: the `pypi` job waits for the reviewer of
   the `pypi` environment in the Actions tab, then publishes the wheels
   through trusted publishing, which PyPI grants to this workflow file and
   that environment; no token exists anywhere.

To try the workflow before tagging, start it by hand from the Actions tab,
or with `gh workflow run release.yml -f tag=v<version>rc1`: the same build
from the chosen branch, the files named after the tag given, without the
push to ghcr.io and without a release, and with the wheels uploaded to
test.pypi.org instead of PyPI, installed from there into a fresh
environment and smoke-tested with the `mxcontrol` it carries and then with
the run's own static one, whose rules and constants must be the wheel's.
Every run, a tag's too, smoke-tests the wheels of the oldest and the newest
CPython the same way on a distribution of their own before anything is
published.
TestPyPI never accepts a version it has seen, so the tag given is a
release candidate that will not be tagged, never the version itself. A
failed run is re-run from its page once the fix is on the branch; the tag
never moves.

To rebuild any artifact by hand: `bazel build --config=release
//mxcontrol:mxcontrol_static`, `bazel run --config=release //docker:load`,
`packaging/build_debs.sh`, `packaging/build_wheels.sh`.
