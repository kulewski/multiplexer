#!/usr/bin/env bash
# The container image of the multiplexer, from the release's mxcontrol
# (packaging/binary.sh) and the system rules file, built by
# docker/Dockerfile.image with buildx and loaded into the local Docker.
# Reproducible: every file's time and the image's own are the commit's
# (SOURCE_DATE_EPOCH), and the build runs on the BuildKit release pinned
# below, so the same binary and commit give the same image, here and in
# the release workflow.
# The release workflow builds the image it pushes this way. Needs Docker.
#
#   packaging/build_image.sh BINARY [TAG...]     default tag: multiplexer:dev
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
binary="$(realpath "$1")"
shift
tags=("$@")
[[ ${#tags[@]} -gt 0 ]] || tags=(multiplexer:dev)
context="$(mktemp -d)"
# A builder of its own, the pinned BuildKit in a container, removed at the
# end: it writes the image to a file on any Docker, where Docker's own
# builder does that only with the containerd image store, which a GitHub
# runner's Docker does not use.
builder="mx-image-$$"
trap 'docker buildx rm "$builder" > /dev/null 2>&1; rm -rf "$context"' EXIT
docker buildx create --name "$builder" --driver docker-container \
    --driver-opt image=moby/buildkit:v0.33.0 > /dev/null
# The tree as the image holds it, modes and all: the Dockerfile copies it
# as it is, since COPY --chmod would give the directories it creates the
# file's mode too, and nonroot could not enter /etc/mx.
mkdir -p "$context/etc/mx"
chmod 755 "$context/etc" "$context/etc/mx"
install -m 755 "$binary" "$context/mxcontrol"
install -m 644 multiplexer.rules "$context/etc/mx/multiplexer.rules"
args=()
for tag in "${tags[@]}"; do
  args+=(--tag "$tag")
done
# Written to a file and loaded from it: Docker's containerd image store
# unpacks what buildx loads into it directly, which the rewritten times
# preclude, and a tarball loads into either store.
SOURCE_DATE_EPOCH="$(git log -1 --format=%ct)" docker buildx build --builder "$builder" --quiet \
    --file docker/Dockerfile.image --output "type=docker,dest=$context/image.tar,rewrite-timestamp=true" \
    "${args[@]}" "$context"
docker load --quiet --input "$context/image.tar"
