#!/usr/bin/env bash
# One Debian package per supported release, each built inside that
# release's container by packaging/deb.sh, into build/dist/. Needs Docker.
#
#   packaging/build_debs.sh                      # every release below
#   packaging/build_debs.sh ubuntu:24.04         # one
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
releases=("$@")
[[ ${#releases[@]} -gt 0 ]] || releases=(debian:12 debian:13 ubuntu:22.04 ubuntu:24.04 ubuntu:26.04)
docker=(docker)
docker info > /dev/null 2>&1 || docker=(sudo docker)

context="$(mktemp -d)"
trap 'rm -rf "$context"' EXIT
# Tracked and new files that exist; a file deleted but not committed is still listed.
git ls-files -z --cached --others --exclude-standard \
  | while IFS= read -r -d '' path; do [[ -e "$path" ]] && printf '%s\0' "$path"; done \
  | tar --null -T - -c | tar -x -C "$context"
mkdir -p build/dist

for base in "${releases[@]}"; do
  tag="mx-deb-$(echo "$base" | tr ':.' '__')"
  echo "== package on $base"
  "${docker[@]}" build -q -f docker/Dockerfile.deb --build-arg "BASE=$base" -t "$tag" "$context" > /dev/null
  "${docker[@]}" run --rm -v "$(realpath build/dist):/out" "$tag"
done
# The containers ran as root; hand the packages to the caller.
"${docker[@]}" run --rm -v "$(realpath build/dist):/out" debian:12 chown -R "$(id -u):$(id -g)" /out
ls -la build/dist/*.deb
