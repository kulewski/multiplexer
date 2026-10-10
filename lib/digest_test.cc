// The CRC-32 of this library against published vectors and against the CRC
// computed bit by bit from its definition, at every length up to 512 bytes
// from every start within eight bytes and over a megabyte, since a wrong
// checksum agreed on by both ends of the wire would go unnoticed; and the
// fingerprint built on it. The lengths and starts cover the eight-byte steps
// of mx::crc32, the tail it takes a byte at a time, and every alignment.
#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <string>

#include "lib/crc32.h"
#include "lib/fingerprint.h"

namespace {

// The standard CRC-32 one bit at a time, straight from its definition: the
// reflected polynomial 0xEDB88320, all ones in and out. Slow and obvious.
std::uint32_t crc32_bitwise(const unsigned char* bytes, std::size_t size) {
  std::uint32_t crc = 0xFFFFFFFFu;
  for (std::size_t index = 0; index < size; ++index) {
    crc ^= bytes[index];
    for (int bit = 0; bit < 8; ++bit) {
      crc = (crc >> 1) ^ (0xEDB88320u & (0u - (crc & 1u)));
    }
  }
  return crc ^ 0xFFFFFFFFu;
}

// `size` bytes that are not a short repeating pattern, from a linear
// congruential generator's high bits.
std::string scrambled(std::size_t size) {
  std::string bytes(size, '\0');
  std::uint32_t state = 12345;
  for (char& byte : bytes) {
    state = state * 1103515245u + 12345u;
    byte = static_cast<char>(state >> 24);
  }
  return bytes;
}

}  // namespace

TEST(Digest, Crc32MatchesTheStandardVectors) {
  EXPECT_EQ(0u, mx::crc32("", 0));
  EXPECT_EQ(0xCBF43926u, mx::crc32("123456789", 9));
  std::string text = "The quick brown fox jumps over the lazy dog";
  EXPECT_EQ(0x414FA339u, mx::crc32(text.data(), text.size()));
}

TEST(Digest, Crc32MatchesTheDefinitionAtEveryLengthAndStart) {
  const std::string buffer = scrambled(512 + 8);
  const auto* bytes = reinterpret_cast<const unsigned char*>(buffer.data());
  for (std::size_t start = 0; start < 8; ++start) {
    for (std::size_t size = 0; size <= 512; ++size) {
      ASSERT_EQ(crc32_bitwise(bytes + start, size), mx::crc32(bytes + start, size))
          << "start " << start << ", size " << size;
    }
  }
}

TEST(Digest, Crc32OfAMegabyteMatchesTheDefinition) {
  const std::string buffer = scrambled(std::size_t(1) << 20);
  const auto* bytes = reinterpret_cast<const unsigned char*>(buffer.data());
  EXPECT_EQ(crc32_bitwise(bytes, buffer.size()), mx::crc32(bytes, buffer.size()));
}

TEST(Digest, FingerprintIsTheCrcInHex) {
  EXPECT_EQ("cbf43926", mx::fingerprint("123456789"));
  EXPECT_EQ("00000000", mx::fingerprint(""));
  EXPECT_NE(mx::fingerprint("peer { type: 1 }"), mx::fingerprint("peer { type: 2 }"));
}
