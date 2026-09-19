#!/usr/bin/env bash
# The manylinux wheels, built by packaging/wheels.sh inside the manylinux
# container, into build/dist/. Needs Docker.
#
#   packaging/build_wheels.sh                # every CPython from 3.10
#   packaging/build_wheels.sh cp312          # one
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
docker=(docker)
docker info > /dev/null 2>&1 || docker=(sudo docker)
context="$(mktemp -d)"
trap 'rm -rf "$context"' EXIT
git ls-files -z --cached --others --exclude-standard | tar --null -T - -c | tar -x -C "$context"
mkdir -p build/dist
image=quay.io/pypa/manylinux_2_28_x86_64
"${docker[@]}" run --rm -v "$context:/work" -v "$(realpath build/dist):/out" -w /work "$image" \
    packaging/wheels.sh /out "$@"
# The container ran as root; hand its files back so that the cleanup and the caller can touch them.
"${docker[@]}" run --rm -v "$context:/work" -v "$(realpath build/dist):/out" "$image" chown -R "$(id -u):$(id -g)" /work /out
