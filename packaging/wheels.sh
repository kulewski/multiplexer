#!/usr/bin/env bash
# Builds manylinux wheels of the Python package: run inside
# quay.io/pypa/manylinux_2_28_x86_64, as packaging/build_wheels.sh does.
# protobuf is built from source once, static, at the version the generated
# code and the extension must match; then, per CPython, `make python wheel`
# with that interpreter and auditwheel, which checks that the wheel needs
# nothing from the system beyond what manylinux_2_28 allows.
#
#   packaging/wheels.sh [outdir] [cp310 cp311 ...]   default: every CPython from 3.10
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
out="$(realpath "${1:-build/dist}")"; shift || true
pythons=("$@")
[[ ${#pythons[@]} -gt 0 ]] || pythons=(cp310 cp311 cp312 cp313 cp314)
PROTOBUF_VERSION="${PROTOBUF_VERSION:-3.21.12}"
prefix=/opt/protobuf

if [[ ! -x "$prefix/bin/protoc" ]]; then
  echo "== protobuf $PROTOBUF_VERSION, static"
  curl -fsSL -o /tmp/protobuf.tar.gz \
      "https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOBUF_VERSION#3.}/protobuf-cpp-${PROTOBUF_VERSION}.tar.gz"
  tar -xzf /tmp/protobuf.tar.gz -C /tmp
  cmake -S "/tmp/protobuf-${PROTOBUF_VERSION}" -B /tmp/protobuf-build -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_INSTALL_PREFIX="$prefix" -Dprotobuf_BUILD_TESTS=OFF -DBUILD_SHARED_LIBS=OFF \
      -DCMAKE_POSITION_INDEPENDENT_CODE=ON > /dev/null
  cmake --build /tmp/protobuf-build -j"$(nproc)" > /dev/null
  cmake --install /tmp/protobuf-build > /dev/null
fi
export PATH="$prefix/bin:$PATH"
export CPPFLAGS="-I$prefix/include -I$prefix/include/asio ${CPPFLAGS:-}"
export LDFLAGS="-L$prefix/lib -L$prefix/lib64 ${LDFLAGS:-}"

if [[ ! -d "$prefix/include/asio" ]]; then
  echo "== asio headers"
  asio_tag="$(sed -n 's/^ASIO_TAG = "\(.*\)"/\1/p' bazel/deps.bzl)"
  curl -fsSL -o /tmp/asio.tar.gz "https://github.com/chriskohlhoff/asio/archive/refs/tags/${asio_tag}.tar.gz"
  tar -xzf /tmp/asio.tar.gz -C /tmp
  cp -r "/tmp/asio-${asio_tag}/asio/include/asio" "/tmp/asio-${asio_tag}/asio/include/asio.hpp" "$prefix/include/"
fi

mkdir -p "$out"
for tag in "${pythons[@]}"; do
  python="$(ls -d /opt/python/${tag}-${tag}*/bin/python | head -1)"
  echo "== $tag: $python"
  "$python" -m pip install -q pybind11 "protobuf>=4.21,<5" setuptools wheel auditwheel
  rm -rf build/python build/obj/multiplexer/_native.o build/wheel
  make -j"$(nproc)" wheel PYTHON="$python" > /dev/null
  "$python" -m auditwheel repair -w "$out" build/dist/multiplexer-*-linux_x86_64.whl > /dev/null
  rm -f build/dist/multiplexer-*-linux_x86_64.whl
done
ls -la "$out"/*.whl
