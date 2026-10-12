#!/usr/bin/env bash
# The release's checks, run here before the tag: at COMMIT, in a worktree
# of its own so that the working tree is left alone, every test, the
# release build, each artifact built and checked as the release workflow
# does, the clean-machine checks and every pip example. Takes an hour or
# two; needs Docker.
#
# The steps, in order:
# - every test, and the release build;
# - the release's binary and image, the image started as a multiplexer and
#   asked for its rules;
# - the leak run;
# - the build and the fast tests on each supported distribution, with
#   Bazel and with make, and at GitHub's runner limits;
# - the Debian packages, the wheels and their smoke tests;
# - every pip example against a wheel of the tree.
# Each step's output goes to a file of its own and a line to the summary,
# ok or FAIL; a failed step does not stop the rest. The worktree stays
# between runs, so that its Bazel cache does too. The steps are COMMIT's
# own: once the worktree is at COMMIT, its copy of this script runs them.
#
#   packaging/release_check.sh [COMMIT] [DIR]     default: HEAD, ~/.cache/mx-release-check
set -uo pipefail
repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
commit="$(git -C "$repo" rev-parse "${1:-HEAD}")"
dir="${2:-$HOME/.cache/mx-release-check}"
wt="$dir/tree"
out="$dir/logs"
mkdir -p "$out"
summary="$out/summary.txt"
: > "$summary"
[[ -d "$wt" ]] || git -C "$repo" worktree add --detach "$wt" "$commit" > /dev/null 2>&1
cd "$wt" || { echo "release_check.sh: no worktree at $wt" >&2 && exit 1; }
[[ "$(git rev-parse --show-toplevel)" == "$(pwd -P)" ]] || { echo "release_check.sh: $wt is not a worktree" >&2 && exit 1; }
git checkout -q --detach "$commit" || exit 1
if [[ "${MX_RELEASE_CHECK_AT:-}" != "$commit" ]]; then
  MX_RELEASE_CHECK_AT="$commit" exec "$wt/packaging/release_check.sh" "$commit" "$dir"
fi
# What an earlier run built, an earlier version's wheels say, would be
# smoke-tested beside this one's.
rm -rf build/dist
version="$(sed -n 's/^VERSION ?= //p' Makefile)"
docker=(docker)
docker info > /dev/null 2>&1 || docker=(sudo docker)
echo "release check of $(git log -1 --format='%h %s' | cut -c1-72), version $version" >> "$summary"

step() {  # NAME COMMAND...: the command, its output to NAME.txt, a summary line
  local name=$1
  shift
  local started
  started="$(date -u +%T)"
  if "$@" > "$out/$name.txt" 2>&1; then
    echo "$name ok $started-$(date -u +%T)" >> "$summary"
  else
    echo "$name FAIL $started-$(date -u +%T)" >> "$summary"
  fi
}
context() {  # the tracked files, as build_debs.sh packs them, into a fresh directory
  local directory
  directory="$(mktemp -d)"
  git ls-files -z --cached --others --exclude-standard \
    | while IFS= read -r -d '' path; do [[ -e "$path" ]] && printf '%s\0' "$path"; done \
    | tar --null -T - -c | tar -x -C "$directory"
  echo "$directory"
}
binary() {  # the release's binary, with the licenses beside it
  packaging/build_binary.sh "$version" || return 1
  for file in "mxcontrol-$version-linux-amd64" "mxcontrol-$version-linux-amd64.sha256" LICENSE THIRD_PARTY_NOTICES; do
    [[ -f "build/dist/$file" ]] || { echo "no build/dist/$file" && return 1; }
  done
}
image() {  # the image from that binary, started once and asked for its rules, as the release job does
  local tag=mx-release-check-image result
  packaging/build_image.sh "build/dist/mxcontrol-$version-linux-amd64" "$tag" || return 1
  "${docker[@]}" rm -f mx-release-check > /dev/null 2>&1  # an interrupted run's
  "${docker[@]}" run -d --name mx-release-check -p 127.0.0.1:19800:1980 "$tag" > /dev/null || return 1
  "build/dist/mxcontrol-$version-linux-amd64" rules status -M 127.0.0.1:19800 --timeout 30
  result=$?
  "${docker[@]}" rm -f mx-release-check > /dev/null
  "${docker[@]}" rmi "$tag" > /dev/null
  return $result
}
smoke() {  # IMAGE PYTHON [PIP EXTRA] [APT] [VENV FLAG], as the release workflow's matrix
  local image=$1 python=$2 extra=${3:-} apt=${4:-} venv=${5:-}
  local tag="cp${python//./}"
  "${docker[@]}" run --rm -v "$wt:/src:ro" -w /src "$image" bash -c "
    set -e
    if [ -n '$apt' ]; then apt-get update -qq && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq $apt > /dev/null; fi
    python3 -m venv $venv /venv
    /venv/bin/pip install -q build/dist/*-$tag-$tag-*.whl $extra
    /venv/bin/python make/wheel_smoke.py
    cp build/dist/mxcontrol-*-linux-amd64 /tmp/mxcontrol && chmod +x /tmp/mxcontrol
    MXCONTROL=/tmp/mxcontrol /venv/bin/python make/wheel_smoke.py"
}
pip_examples() {  # the examples workflow's job, in ubuntu:24.04: a make wheel, then every pip example's test.sh
  local directory result
  directory="$(context)"
  "${docker[@]}" run --rm -v "$directory:/work" -w /work ubuntu:24.04 bash -c '
    set -e
    apt-get update -qq
    DEBIAN_FRONTEND=noninteractive apt-get install -y -qq g++ make protobuf-compiler libprotobuf-dev libasio-dev libre2-dev python3-dev python3-venv python3-pip python3-setuptools python3-wheel python3-protobuf pybind11-dev python3-pybind11 git ca-certificates curl > /dev/null
    # Bazel, as the GitHub runner has it, for the C++ worker of the audio example.
    curl -fsSL -o /usr/local/bin/bazel https://github.com/bazelbuild/bazelisk/releases/download/v1.20.0/bazelisk-linux-amd64
    chmod +x /usr/local/bin/bazel
    make -j"$(nproc)" wheel > /tmp/make.log 2>&1 || { tail -50 /tmp/make.log; exit 1; }
    python3 -m venv examples/.venv && examples/.venv/bin/pip install -q build/dist/*.whl
    status=0
    for script in examples/*/test.sh; do
      echo "==== $script"
      VENV="$PWD/examples/.venv" "$script" || status=1
    done
    exit $status'
  result=$?
  "${docker[@]}" run --rm -v "$directory:/work" ubuntu:24.04 rm -rf /work/build /work/examples
  rm -rf "$directory"
  return $result
}

step all_tests bazel test --test_env=MX_REQUIRE_PRIVATE_NETWORK=1 //...
step release_build bazel build --config=release //...
step binary binary
step image image
step leaks ./check.sh --leaks
for distro in debian:12 debian:13 ubuntu:22.04 ubuntu:24.04; do
  for path in bazel make; do
    step "docker_${path}_${distro/:/_}" ./docker/check.sh "$path" "$distro"
  done
done
step runner_limits ./docker/runner_limits.sh
step debs packaging/build_debs.sh
step wheels packaging/build_wheels.sh
step smoke_3.10 smoke python:3.10-slim 3.10
step smoke_3.14 smoke python:3.14-slim 3.14
step smoke_3.10_protobuf_floor smoke python:3.10-slim 3.10 protobuf==3.20.3
step smoke_ubuntu_22.04 smoke ubuntu:22.04 3.10 "" "python3-venv python3-protobuf" --system-site-packages
step pip_examples pip_examples
echo "done $(date -u +%T)" >> "$summary"
bazel shutdown
cat "$summary"
