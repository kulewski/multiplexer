#!/usr/bin/env bash
# The release's mxcontrol, built by packaging/binary.sh inside the manylinux
# container, into build/dist/. Needs Docker.
#
#   packaging/build_binary.sh [VERSION]      default: the Makefile's VERSION
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
version="${1:-$(sed -n 's/^VERSION ?= //p' Makefile)}"
docker=(docker)
docker info > /dev/null 2>&1 || docker=(sudo docker)
context="$(mktemp -d)"
trap 'rm -rf "$context"' EXIT
# Tracked and new files that exist; a file deleted but not committed is still listed.
git ls-files -z --cached --others --exclude-standard \
  | while IFS= read -r -d '' path; do [[ -e "$path" ]] && printf '%s\0' "$path"; done \
  | tar --null -T - -c | tar -x -C "$context"
mkdir -p build/dist
image=quay.io/pypa/manylinux_2_28_x86_64
"${docker[@]}" run --rm -v "$context:/work" -v "$(realpath build/dist):/out" -w /work "$image" \
    packaging/binary.sh "$version" /out
# The container ran as root; hand its files back so that the cleanup and the caller can touch them.
"${docker[@]}" run --rm -v "$context:/work" -v "$(realpath build/dist):/out" "$image" chown -R "$(id -u):$(id -g)" /work /out
