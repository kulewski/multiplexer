# What packaging/wheels.sh and packaging/binary.sh link into what they build,
# sourced by both inside quay.io/pypa/manylinux_2_28_x86_64: protobuf from
# source, static, at the version the generated code, the extension and
# mxcontrol must match; Asio's headers and RE2, at the releases
# bazel/deps.bzl pins, RE2 static for mxcontrol's key filters. Each is built
# once into /opt/protobuf and found there by a later run in the same
# container. Exports PATH, CPPFLAGS and LDFLAGS for the Makefile.
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

if [[ ! -f "$prefix/lib/libre2.a" && ! -f "$prefix/lib64/libre2.a" ]]; then
  re2_tag="$(sed -n 's/^RE2_TAG = "\(.*\)"/\1/p' bazel/deps.bzl)"
  echo "== RE2 $re2_tag, static"
  curl -fsSL -o /tmp/re2.tar.gz "https://github.com/google/re2/archive/refs/tags/${re2_tag}.tar.gz"
  tar -xzf /tmp/re2.tar.gz -C /tmp
  cmake -S "/tmp/re2-${re2_tag}" -B /tmp/re2-build -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_INSTALL_PREFIX="$prefix" -DRE2_BUILD_TESTING=OFF -DBUILD_SHARED_LIBS=OFF \
      -DCMAKE_POSITION_INDEPENDENT_CODE=ON > /dev/null
  cmake --build /tmp/re2-build -j"$(nproc)" > /dev/null
  cmake --install /tmp/re2-build > /dev/null
fi
