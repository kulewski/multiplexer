// mx::crc32 eight bytes at a time ("slicing-by-8"): eight tables of 256
// entries, built once from the reflected polynomial when the program starts,
// so that one step folds eight bytes into the CRC; the same CRC as one table
// lookup per byte, about five times faster. See crc32.h for what is computed.
//
// The frames carried this CRC up to 3.0.0 and carry XXH3 now (lib/xxh3.h);
// this one makes the fingerprint of a rules file (lib/fingerprint.h), which
// Python computes alike with zlib.crc32. One lookup per byte made each step
// wait for the one before: measured on 2026-10-10, 230 bytes took about
// 330 ns that way (0.67 GB/s) and take about 65 ns this way (3.3 GB/s).
//
// entries[0] is the usual table, the remainder of each byte value by the
// polynomial; entries[slice] is entries[slice - 1] advanced by one more byte
// of zeros, so the eight lookups of a step are independent of each other and
// their results xor together. Bytes are read as little-endian numbers one by
// one, so neither the machine's byte order nor the buffer's alignment
// matters; a tail of fewer than eight bytes takes the byte-at-a-time step.
// The tables are built by a constructor, not at compile time, because this
// file is also compiled for build tools, whose compiler flags give C++11's
// constexpr rules.
#include "lib/crc32.h"

namespace mx {

namespace {

// The eight tables, built when the program starts; see the file comment.
struct Tables {
  std::uint32_t entries[8][256];

  Tables() {
    for (std::uint32_t index = 0; index < 256; ++index) {
      std::uint32_t value = index;
      for (int bit = 0; bit < 8; ++bit) {
        value = (value & 1) ? (value >> 1) ^ 0xEDB88320u : value >> 1;
      }
      entries[0][index] = value;
    }
    for (std::uint32_t index = 0; index < 256; ++index) {
      for (int slice = 1; slice < 8; ++slice) {
        const std::uint32_t previous = entries[slice - 1][index];
        entries[slice][index] = (previous >> 8) ^ entries[0][previous & 0xFFu];
      }
    }
  }
};
const Tables tables;

// The four bytes at `bytes` as a little-endian number, whatever the machine's
// byte order and the pointer's alignment; compilers make it a single load.
std::uint32_t little_endian32(const unsigned char* bytes) {
  return static_cast<std::uint32_t>(bytes[0]) | static_cast<std::uint32_t>(bytes[1]) << 8 |
         static_cast<std::uint32_t>(bytes[2]) << 16 | static_cast<std::uint32_t>(bytes[3]) << 24;
}

}  // namespace

std::uint32_t crc32(const void* data, std::size_t size) {
  const unsigned char* bytes = static_cast<const unsigned char*>(data);
  std::uint32_t crc = 0xFFFFFFFFu;
  for (; size >= 8; bytes += 8, size -= 8) {
    const std::uint32_t low = little_endian32(bytes) ^ crc;
    const std::uint32_t high = little_endian32(bytes + 4);
    crc = tables.entries[7][low & 0xFFu] ^ tables.entries[6][(low >> 8) & 0xFFu] ^
          tables.entries[5][(low >> 16) & 0xFFu] ^ tables.entries[4][low >> 24] ^ tables.entries[3][high & 0xFFu] ^
          tables.entries[2][(high >> 8) & 0xFFu] ^ tables.entries[1][(high >> 16) & 0xFFu] ^
          tables.entries[0][high >> 24];
  }
  for (; size > 0; ++bytes, --size) {
    crc = tables.entries[0][(crc ^ *bytes) & 0xFFu] ^ (crc >> 8);
  }
  return crc ^ 0xFFFFFFFFu;
}

}  // namespace mx
