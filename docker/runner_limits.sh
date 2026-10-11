#!/usr/bin/env bash
# The fast tests as a GitHub runner runs them, in Docker: ubuntu:24.04 with
# four CPUs, Bazel running four tests at a time, and the TCP buffer maxima
# the runners allow, 32 MB where a workstation's are a few. A test that
# fills a connection, or counts on how much a socket holds, behaves
# otherwise there, so this reproduces a failure CI saw and checks a change
# before a push. The image is docker/check.sh's for Bazel on ubuntu:24.04,
# built from the tracked and the new files. Needs Docker.
#
#   ./docker/runner_limits.sh                 at 32 MB
#   ./docker/runner_limits.sh 6291456         at another maximum, in bytes
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
bytes="${1:-33554432}"
docker=(docker)
docker info > /dev/null 2>&1 || docker=(sudo docker)
context="$(mktemp -d)"
trap 'rm -rf "$context"' EXIT
# Tracked and new files that exist: a file deleted but not yet committed is
# still listed, and would fail the tar.
git ls-files -z --cached --others --exclude-standard \
  | while IFS= read -r -d '' path; do [[ -e "$path" ]] && printf '%s\0' "$path"; done \
  | tar --null -T - -c | tar -x -C "$context"
tag=mx-check-bazel-ubuntu_24_04
"${docker[@]}" build -q -f docker/Dockerfile.bazel --build-arg BASE=ubuntu:24.04 -t "$tag" "$context" > /dev/null
"${docker[@]}" run --rm --cpus=4 \
    --sysctl net.ipv4.tcp_rmem="4096 131072 $bytes" --sysctl net.ipv4.tcp_wmem="4096 16384 $bytes" \
    "$tag" bash -c 'echo "tcp_rmem: $(cat /proc/sys/net/ipv4/tcp_rmem), tcp_wmem: $(cat /proc/sys/net/ipv4/tcp_wmem)"
        bazel test --test_tag_filters=-slow --test_output=errors --local_cpu_resources=4 //...'
