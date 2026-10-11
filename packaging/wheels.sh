#!/usr/bin/env bash
# Builds manylinux wheels of the Python package: run inside
# quay.io/pypa/manylinux_2_28_x86_64, as packaging/build_wheels.sh does.
# protobuf and RE2 come static from packaging/static_deps.sh, once; then,
# per CPython, `make python wheel` with that interpreter, a check that the
# extension and mxcontrol are stripped release builds, auditwheel, which
# checks that the wheel needs nothing from the system beyond what
# manylinux_2_28 allows and tags it so, a check that it carries LICENSE and
# THIRD_PARTY_NOTICES, and the wheel installed into a fresh environment
# of that CPython and smoke-tested with its own mxcontrol, at manylinux's
# glibc.
#
# The tools it installs into each CPython are pinned in
# packaging/wheel_build_requirements.txt.
#
#   packaging/wheels.sh [outdir] [cp310 cp311 ...]   default: every CPython from 3.10
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
out="$(realpath "${1:-build/dist}")"; shift || true
pythons=("$@")
[[ ${#pythons[@]} -gt 0 ]] || pythons=(cp310 cp311 cp312 cp313 cp314)
source packaging/static_deps.sh
mkdir -p "$out"
for tag in "${pythons[@]}"; do
  python="$(ls -d /opt/python/${tag}-${tag}*/bin/python | head -1)"
  echo "== $tag: $python"
  "$python" -m pip install -q -r packaging/wheel_build_requirements.txt
  rm -rf build/python build/obj/multiplexer/_native.o build/wheel
  make -j"$(nproc)" wheel PYTHON="$python" > /dev/null
  packaging/check_binaries.sh build/wheel/multiplexer/_native.so build/wheel/multiplexer/bin/mxcontrol
  rm -rf build/repaired
  "$python" -m auditwheel repair --plat manylinux_2_28_x86_64 --only-plat -w build/repaired \
      build/dist/mx_multiplexer-*-linux_x86_64.whl > /dev/null
  # The licenses travel with the wheel: ours, and the notices of what its binaries hold.
  for license in LICENSE THIRD_PARTY_NOTICES; do
    "$python" -m zipfile -l build/repaired/*.whl | grep -q -E "\.dist-info/(licenses/)?$license " ||
      { echo "wheels.sh: the wheel carries no $license" >&2 && exit 1; }
  done
  rm -rf "/tmp/smoke-$tag" && "$python" -m venv "/tmp/smoke-$tag"
  "/tmp/smoke-$tag/bin/pip" install -q build/repaired/*.whl
  "/tmp/smoke-$tag/bin/python" make/wheel_smoke.py
  mv build/repaired/*.whl "$out"/
  rm -f build/dist/mx_multiplexer-*-linux_x86_64.whl
done
ls -la "$out"/*.whl
