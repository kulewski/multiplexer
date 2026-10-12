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
| `mxcontrol-<version>-linux-amd64` with its `.sha256` | running a multiplexer, or its tools, on any Linux | x86_64 with glibc 2.28 or newer |
| `ghcr.io/kulewski/multiplexer:<version>` | running a multiplexer in a container | a container runtime |
| `multiplexer_<version>~<codename>_amd64.deb`, one per Debian and Ubuntu release | mxcontrol as a system package, and building C++ peers | that release; `libprotobuf-dev` and `libasio-dev` to build against the library |
| `mx_multiplexer-<version>-cp3XY-cp3XY-manylinux_2_28_x86_64.whl`, one per CPython from 3.10 | Python peers, and `mxcontrol`: a multiplexer, `generate_rules`, `generate_constants`, the test harness's binary | `pip` on x86_64 Linux with glibc 2.28 or newer; the `protobuf` package comes with it |
| `mx-multiplexer-<version>.crate`, on crates.io as `mx-multiplexer` | Rust peers | cargo, Rust 1.85 or newer, on Linux |
| the source archive GitHub makes for the tag | everything else, through Bazel or `make` | [building.md](building.md) |

Bazel consumers do not use any of these: `git_repository(tag = "v<version>")`
with `mx_dependencies()` and `mx_setup()` builds the library from source
in their workspace, as [examples/README.md](../examples/README.md) shows.

## Licenses

The multiplexer is MIT. The binaries also hold code of other projects,
compiled in: protobuf in `mxcontrol` and in the wheels' extension, RE2 in
`mxcontrol`, pybind11 in the extension, and Asio's headers in both.
[THIRD_PARTY_NOTICES](../THIRD_PARTY_NOTICES) has their licenses, and every
artifact carries it with LICENSE: the wheels in their `.dist-info`, the
release beside the binary, the image under `/usr/share/doc/mxcontrol`.
The build scripts check that each does. glibc is the system's, or in the
image Debian's, from the distroless base; the Debian packages link
protobuf and RE2 from their distribution.

## The mxcontrol binary

`packaging/binary.sh` builds it inside `quay.io/pypa/manylinux_2_28_x86_64`,
where the wheels are built, and `packaging/build_binary.sh` runs it there
from a workstation. It is the Makefile's `mxcontrol` with the system rules,
with protobuf, RE2 and libstdc++ linked in, about 4 MB. glibc is the
system's, 2.28 or newer: every mainstream x86_64 distribution since 2018
has one, Debian 10, Ubuntu 20.04 and RHEL 8 among the oldest, but a musl
one such as Alpine does not, and there the image or a build from source
serves. The script checks what it built: a stripped release build that
needs nothing but glibc's own libraries and no glibc symbol newer than
2.28. Host names resolve as they do for any program on the system. The
wheels' `mxcontrol` is the same build, with libstdc++ taken from the
system, as manylinux allows.

## The container image

`docker/Dockerfile.image` puts that binary and the system rules file on
`gcr.io/distroless/base-nossl-debian13`, which holds glibc and no shell,
no package manager and no OpenSSL. `packaging/build_image.sh` builds it
reproducibly, every file's time and the image's own the commit's, on a
BuildKit release it pins, in a builder of its own that works on any
Docker, so the same binary and commit give the same image. It runs as `nonroot`, listens
on 1980, and its command is

```
/mxcontrol run_multiplexer --rules /etc/mx/multiplexer.rules --address 0.0.0.0:1980
```

That is every IPv4 address; on an IPv6 network, or a dual-stack one, give
the command with `--address [::]:1980` instead, every IPv6 address and,
as Linux maps it, every IPv4 one ([addresses](mxcontrol.md#addresses)).

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
StatefulSet that runs several of them on Kubernetes. Locally,
`packaging/build_binary.sh` and then `packaging/build_image.sh
build/dist/mxcontrol-<version>-linux-amd64` put the image into Docker as
`multiplexer:dev`; the release workflow starts it once, asks it for its
rules, and pushes it.

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

## The crate

The Rust library, the crate `mx-multiplexer` in `rust/mx-multiplexer`, is
published on crates.io under that name, its version the repository's.
`cargo package` makes it from the crate's directory alone, as a user's
cargo builds it: the sources, the generated protocol code and constants,
which are committed, the README and the license, and the tests with the
golden frames; the Bazel files are left out. It builds with no tool but
cargo, no protoc, and needs `mxcontrol` only for its tests against a real
multiplexer. A program generates its own constants with
`mxcontrol generate_constants your.rules --rust multiplexer_constants.rs`,
from the wheel or the Debian package ([the Rust API](api_rust.md)).

## Cutting a release

1. Bump the version in `Makefile`, `multiplexer/release.py`,
   `lib/release.cc`, `rust/mx-multiplexer/Cargo.toml` and
   `rust/mx-multiplexer/BUILD`, with the lockfiles cargo then writes, in
   one commit; `//rust/mx-multiplexer:generated_test` fails while the
   crate's version is not the repository's.
2. Check it: `packaging/release_check.sh` runs here what the workflows
   will run, at that commit, in a worktree of its own:
   - every test, and the release build;
   - the binary and the image, started once and asked for its rules;
   - the leak run;
   - `docker/check.sh`, both paths, on each of
     [the tested systems](building.md#tested-systems), and
     `docker/runner_limits.sh`;
   - the Debian packages, the wheels and their smoke tests;
   - the crate, as a dry run of its upload;
   - every pip example, against a wheel of the tree.

   Each step's output goes to a file of its own in
   `~/.cache/mx-release-check/logs`, with a line in its `summary.txt`, ok
   or FAIL. It takes an hour or two.
3. Tag it: `git tag -a v<version> -m "..."` and `git push origin main v<version>`.
4. The workflow runs the tests, builds every artifact, pushes the image and
   creates the release with the files attached.
5. Approve the upload to PyPI: the `pypi` job waits for the reviewer of
   the `pypi` environment in the Actions tab, then publishes the wheels
   through trusted publishing, which PyPI grants to this workflow file and
   that environment; no token exists anywhere.
6. Approve the upload to crates.io: the `crates-io` job waits for the
   reviewer of the `crates-io` environment, then publishes the crate
   through crates.io's trusted publishing, the run's token exchanged for
   one that lasts half an hour. crates.io trusts a publisher only for a
   crate that exists, so the crate's first version is published by hand,
   `cargo publish -p mx-multiplexer` in `rust/` with a token of the
   account that will own it, and the workflow and the environment are then
   named as its trusted publisher in the crate's settings on crates.io.
   Reject that release's own `crates-io` job: it would find the version
   taken.

To try the workflow before tagging, start it by hand from the Actions tab,
or with `gh workflow run release.yml -f tag=v<version>rc1`: the same build
from the chosen branch, the files named after the tag given, without the
push to ghcr.io and without a release, and with the wheels uploaded to
test.pypi.org instead of PyPI, installed from there into a fresh
environment and smoke-tested with the `mxcontrol` it carries and then with
the run's own binary, whose rules and constants must be the wheel's.
The crate is packaged and verified, and published nowhere.
Every run, a tag's too, smoke-tests the wheels of the oldest and the newest
CPython the same way on a distribution of their own before anything is
published.
TestPyPI never accepts a version it has seen, so the tag given is a
release candidate that will not be tagged, never the version itself. A
failed run is re-run from its page once the fix is on the branch; the tag
never moves.

To rebuild any artifact by hand: `packaging/build_binary.sh`, then
`packaging/build_image.sh` with the binary it wrote,
`packaging/build_debs.sh`, `packaging/build_wheels.sh`, and in `rust/`,
`cargo package -p mx-multiplexer`.
