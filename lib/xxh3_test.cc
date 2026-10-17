// mx::xxh3_64 against the values of xxHash's own sanity check, since a wrong
// checksum agreed on by both ends of the wire would go unnoticed: at the
// lengths it takes, which reach every way XXH3 hashes a body, and at the
// edges between those ways, from xxHash 0.8.1 and LLVM 18's implementation
// of it, which agree. And every path this processor runs (the vector
// instructions' and one lane at a time) against the others: at every
// length up to five blocks from every start within eight bytes, and over a
// megabyte.
#include "lib/xxh3.h"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <vector>

namespace {

using mx::xxh3_detail::Path;

const Path PATHS[] = {Path::PORTABLE, Path::SSE2, Path::AVX2, Path::AVX512};

// The bytes xxHash's sanity check hashes: the top byte of a number
// multiplied by a 64-bit prime at each step.
std::vector<unsigned char> sanity_buffer(std::size_t size) {
  std::vector<unsigned char> bytes(size);
  std::uint64_t state = 2654435761u;
  for (unsigned char& byte : bytes) {
    byte = static_cast<unsigned char>(state >> 56);
    state *= 11400714785074694797ull;
  }
  return bytes;
}

struct Known {
  std::size_t size;
  std::uint64_t hash;
};

const Known KNOWN[] = {
    // xxHash's sanity check, seed 0.
    {0, 0x2D06800538D394C2ull},
    {1, 0xC44BDFF4074EECDBull},
    {6, 0x27B56A84CD2D7325ull},
    {12, 0xA713DAF0DFBB77E7ull},
    {24, 0xA3FE70BF9D3510EBull},
    {48, 0x397DA259ECBA1F11ull},
    {80, 0xBCDEFBBB2C47C90Aull},
    {195, 0xCD94217EE362EC3Aull},
    {403, 0xCDEB804D65C6DEA4ull},
    {512, 0x617E49599013CB6Bull},
    {2048, 0xDD59E2C3A5F038E0ull},
    {2099, 0xC6B9D9B3FC9AC765ull},
    {2240, 0x6E73A90539CF2948ull},
    {2367, 0xCB37AEB9E5D361EDull},
    // Either side of where XXH3 changes its way, and a megabyte.
    {3, 0x54247382A8D6B94Dull},
    {4, 0xE5DC74BC51848A51ull},
    {8, 0x24CCC9ACAA9F65E4ull},
    {9, 0x14D5001C15DD3F2Bull},
    {16, 0x981B17D36C7498C9ull},
    {17, 0x796F5ACD3A60F862ull},
    {128, 0xFCFF24126754D861ull},
    {129, 0x98F1B0A679A2CA29ull},
    {240, 0x81C3C2B67F568CCFull},
    {241, 0xC5A639ECD2030E5Eull},
    {1024, 0xDD85C9B5C1109C5Cull},
    {1025, 0xD870C0FA13211C6Aull},
    {1 << 20, 0x7ADE52BBFBD697BAull},
};

TEST(Xxh3, MatchesXxhashOnItsOwnSanityCheck) {
  const std::vector<unsigned char> buffer = sanity_buffer(1 << 20);
  for (const Known& known : KNOWN) {
    EXPECT_EQ(known.hash, mx::xxh3_64(buffer.data(), known.size)) << "size " << known.size;
    for (Path path : PATHS) {
      if (mx::xxh3_detail::runs(path)) {
        EXPECT_EQ(known.hash, mx::xxh3_detail::by(path, buffer.data(), known.size))
            << "size " << known.size << ", path " << static_cast<int>(path);
      }
    }
  }
}

TEST(Xxh3, EveryPathAgreesAtEveryLengthAndStart) {
  // Five blocks of 1024 and a part: the stripes of a block, the scramble
  // after it and the last stripe that overlaps the ones before.
  const std::vector<unsigned char> buffer = sanity_buffer(5 * 1024 + 200);
  for (std::size_t start = 0; start < 8; ++start) {
    for (std::size_t size = 0; size + start <= buffer.size(); ++size) {
      const std::uint64_t expected = mx::xxh3_detail::by(Path::PORTABLE, buffer.data() + start, size);
      ASSERT_EQ(expected, mx::xxh3_64(buffer.data() + start, size)) << "start " << start << ", size " << size;
      for (Path path : PATHS) {
        if (mx::xxh3_detail::runs(path)) {
          ASSERT_EQ(expected, mx::xxh3_detail::by(path, buffer.data() + start, size))
              << "start " << start << ", size " << size << ", path " << static_cast<int>(path);
        }
      }
    }
  }
}

TEST(Xxh3, TakesTheWidestPathThisProcessorRuns) {
  EXPECT_TRUE(mx::xxh3_detail::runs(Path::PORTABLE));
#if defined(__x86_64__)
  EXPECT_TRUE(mx::xxh3_detail::runs(Path::SSE2));  // in every x86-64
#endif
  const Path chosen = mx::xxh3_detail::chosen();
  EXPECT_TRUE(mx::xxh3_detail::runs(chosen));
  for (Path path : PATHS) {
    if (static_cast<int>(path) > static_cast<int>(chosen)) {
      EXPECT_FALSE(mx::xxh3_detail::runs(path)) << static_cast<int>(path);
    }
  }
  std::printf("this processor hashes long bodies by path %d of 0 (portable) to 3 (AVX-512)\n",
              static_cast<int>(chosen));
}

}  // namespace
