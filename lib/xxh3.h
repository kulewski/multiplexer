// mx::xxh3_64(data, size): XXH3, Yann Collet's 64-bit hash from xxHash 0.8
// (seed 0, its default secret), whose low 32 bits are the checksum in every
// frame's header: every frame's sender computes it, the multiplexer and the
// receiver check it, so its speed is on every message's path. Peers in every
// language must agree on it; XXH3's values have been fixed since xxHash
// 0.8.0, and lib/xxh3_test.cc holds those of xxHash's own sanity check.
// docs/wire_format.md says how a peer in another language computes it.
// lib/crc32.h is the other digest, zlib's CRC-32, for fingerprints.
#ifndef MX_LIB_XXH3_H_
#define MX_LIB_XXH3_H_

#include <cstddef>
#include <cstdint>

namespace mx {

std::uint64_t xxh3_64(const void* data, std::size_t size);

namespace xxh3_detail {

// The ways a body over 240 bytes is hashed, all to the same value: one
// 64-bit lane at a time, or 16, 32 or 64 bytes at a time by the processor's
// vector instructions.
enum class Path { PORTABLE, SSE2, AVX2, AVX512 };

// Whether this processor runs `path`.
bool runs(Path path);

// The path xxh3_64() takes on this processor: the widest it runs.
Path chosen();

// xxh3_64 by `path`, which must be one this processor runs: what the tests
// compare the paths with.
std::uint64_t by(Path path, const void* data, std::size_t size);

}  // namespace xxh3_detail

}  // namespace mx

#endif  // MX_LIB_XXH3_H_
