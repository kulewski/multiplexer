// The CRC-32 of this library against published vectors, since a wrong
// checksum agreed on by both ends of the wire would go unnoticed, and the
// fingerprint built on it.
#include <gtest/gtest.h>

#include <string>

#include "lib/crc32.h"
#include "lib/fingerprint.h"

TEST(Digest, Crc32MatchesTheStandardVectors) {
  EXPECT_EQ(0u, mx::crc32("", 0));
  EXPECT_EQ(0xCBF43926u, mx::crc32("123456789", 9));
  std::string text = "The quick brown fox jumps over the lazy dog";
  EXPECT_EQ(0x414FA339u, mx::crc32(text.data(), text.size()));
}

TEST(Digest, FingerprintIsTheCrcInHex) {
  EXPECT_EQ("cbf43926", mx::fingerprint("123456789"));
  EXPECT_EQ("00000000", mx::fingerprint(""));
  EXPECT_NE(mx::fingerprint("peer { type: 1 }"), mx::fingerprint("peer { type: 2 }"));
}
