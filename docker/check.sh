#!/usr/bin/env bash
# Builds and tests this checkout on a clean distribution in Docker, the way
# a new user would, from docker/Dockerfile.bazel. Takes a few minutes;
# needs Docker.
#
#   ./docker/check.sh                 debian:12
#   ./docker/check.sh bazel ubuntu:24.04
#
# The image is built from the tracked and the new files, so the working tree
# is what gets tested, without build outputs.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
path="${1:-all}"
base="${2:-debian:12}"
docker=(docker)
docker info > /dev/null 2>&1 || docker=(sudo docker)

context="$(mktemp -d)"
trap 'rm -rf "$context"' EXIT
git ls-files -z --cached --others --exclude-standard | tar --null -T - -c | tar -x -C "$context"

for which in bazel; do
  [[ "$path" == all || "$path" == "$which" ]] || continue
  tag="mx-check-$which-$(echo "$base" | tr ':.' '__')"
  echo "== $which on $base"
  "${docker[@]}" build -q -f "docker/Dockerfile.$which" --build-arg "BASE=$base" -t "$tag" "$context"
  "${docker[@]}" run --rm "$tag"
  echo "== $which on $base: passed"
done
