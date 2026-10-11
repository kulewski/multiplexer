#!/usr/bin/env bash
# Builds the release's mxcontrol, mxcontrol-<version>-linux-amd64, with its
# .sha256: run inside quay.io/pypa/manylinux_2_28_x86_64, as
# packaging/build_binary.sh does. The Makefile's mxcontrol, with the system
# rules, protobuf and RE2 linked in from packaging/static_deps.sh and
# libstdc++ and libgcc too, so that it needs nothing from the system but
# glibc, 2.28 or newer: any mainstream x86_64 Linux from 2018 on, not a
# musl one. The image runs the same file (packaging/build_image.sh).
# Checked before it is written out: a stripped release build, nothing
# NEEDED but glibc's own libraries, no glibc symbol newer than 2.28.
# LICENSE and THIRD_PARTY_NOTICES go beside it, for the release.
#
#   packaging/binary.sh VERSION [outdir]    default outdir: build/dist
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
version="$1"
out="$(realpath "${2:-build/dist}")"
source packaging/static_deps.sh
rm -f build/bin/mxcontrol
make -j"$(nproc)" build/bin/mxcontrol LDFLAGS="$LDFLAGS -static-libstdc++ -static-libgcc" > /dev/null
name="mxcontrol-$version-linux-amd64"
mkdir -p "$out"
strip -o "$out/$name" build/bin/mxcontrol
packaging/check_binaries.sh "$out/$name"

# glibc's libraries and nothing else, then the newest glibc symbol it needs.
needed="$(readelf -d "$out/$name" | sed -n 's/.*(NEEDED).*\[\(.*\)\]/\1/p' | sort | tr '\n' ' ')"
for library in $needed; do
  case "$library" in
    libc.so.6 | libm.so.6 | libpthread.so.0 | libdl.so.2 | librt.so.1 | ld-linux-x86-64.so.2) ;;
    *) echo "binary.sh: $name needs $library, which is not glibc's" >&2 && exit 1 ;;
  esac
done
newest="$(objdump -T "$out/$name" | grep -o 'GLIBC_[0-9.]*' | sort -uV | tail -1)"
if [[ "$(printf '%s\n' GLIBC_2.28 "$newest" | sort -V | tail -1)" != GLIBC_2.28 ]]; then
  echo "binary.sh: $name needs $newest, newer than the glibc 2.28 it promises" >&2 && exit 1
fi
(cd "$out" && sha256sum "$name" > "$name.sha256")
# Beside it, what the release attaches with it: ours, and the notices of
# protobuf and RE2, which it holds.
cp LICENSE THIRD_PARTY_NOTICES "$out/"
echo "$out/$name: needs ${needed% }, glibc symbols up to $newest"
