#!/usr/bin/env bash
# Builds the Debian package of this checkout on the distribution it runs
# on: make, the tests, `make install` into a staging root, a control file,
# dpkg-deb. Run inside a container of the target release, as
# packaging/build_debs.sh does; needs the packages docs/building.md lists
# for the Makefile, plus dpkg-dev.
#
#   packaging/deb.sh [outdir]      -> outdir/multiplexer_<version>~<codename>_<arch>.deb
#
# The package holds mxcontrol and generate_constants, libmultiplexer.a with
# the headers under /usr/include/mx, and the pkg-config file. It is built
# against the release's own libprotobuf, which is why there is one per
# release: protobuf C++ promises no compatibility between versions, so a
# program using the library must compile against the same libprotobuf-dev.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
out="$(realpath "${1:-build/dist}")"
version="$(sed -n 's/^VERSION ?= //p' Makefile)"
codename="$(. /etc/os-release && echo "${VERSION_CODENAME:-$ID}")"
arch="$(dpkg --print-architecture)"
protobuf="$(dpkg-query -W -f '${Version}' libprotobuf-dev)"
# The runtime package the -dev package depends on: libprotobuf32, or
# libprotobuf32t64 on the releases after the time_t transition.
protobuf_runtime="$(dpkg-query -W -f '${Depends}' libprotobuf-dev | tr ',' '\n' | sed -n 's/^ *\(libprotobuf[0-9][0-9a-z]*\).*/\1/p' | head -1)"
[[ -n "$protobuf_runtime" ]] || { echo "cannot tell the libprotobuf runtime package from libprotobuf-dev's Depends" >&2; exit 1; }
staging="$(mktemp -d)"
trap 'rm -rf "$staging"' EXIT

make -j"$(nproc)" all
make -j"$(nproc)" check-cc
make install DESTDIR="$staging" PREFIX=/usr

mkdir -p "$staging/DEBIAN" "$staging/usr/share/doc/multiplexer"
cp LICENSE "$staging/usr/share/doc/multiplexer/copyright"
size_kb="$(du -sk "$staging/usr" | cut -f1)"
cat > "$staging/DEBIAN/control" <<CONTROL
Package: multiplexer
Version: ${version}~${codename}
Section: net
Priority: optional
Architecture: ${arch}
Maintainer: Krzysztof Kulewski <kulewski@gmail.com>
Installed-Size: ${size_kb}
Depends: ${protobuf_runtime}, libc6, libstdc++6
Recommends: libprotobuf-dev (= ${protobuf}), libasio-dev
Homepage: https://github.com/kulewski/multiplexer
Description: message broker with C++ and Python client libraries
 mxcontrol runs a multiplexer and its tools; libmultiplexer.a, the headers
 under /usr/include/mx and the pkg-config file build a C++ peer against
 this release's protobuf (libprotobuf-dev ${protobuf}); generate_constants
 turns a rules file into the constants header. The Python library is a
 separate wheel.
CONTROL
mkdir -p "$out"
deb="$out/multiplexer_${version}~${codename}_${arch}.deb"
dpkg-deb --root-owner-group --build "$staging" "$deb" > /dev/null
echo "$deb"
