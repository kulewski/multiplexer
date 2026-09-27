// LittleEndian<uint32_t>, the frame header's length and CRC on the wire:
// the least significant byte first, written and read back. The decode
// reads its four bytes one statement each; read as the operands of one
// expression, their order was unspecified, right only because gcc and
// clang read them left to right, so no test here can tell the two apart.
#include "lib/encoding/little_endian.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <string>

#include "lib/encoding/decode_from_range.h"
#include "lib/encoding/encode_to_range.h"

namespace {

typedef mx::encoders::DecodeFromRange<std::string::const_iterator, mx::encodings::LittleEndian> Decoder;
typedef mx::encoders::EncodeToRange<std::string::iterator, mx::encodings::LittleEndian> Encoder;

TEST(LittleEndian, TheLeastSignificantByteFirst) {
  const std::string wire("\x01\x02\x03\x04", 4);
  Decoder decoder(wire.begin(), wire.end());
  std::uint32_t value = 0;
  decoder(value);
  EXPECT_EQ(0x04030201u, value);
}

TEST(LittleEndian, WrittenAndReadBack) {
  for (const std::uint32_t value : {0u, 1u, 0x80u, 0x1234u, 0xdeadbeefu, 0xffffffffu}) {
    std::string wire(8, '\0');
    Encoder encoder(wire.begin(), wire.end());
    encoder(value);
    encoder(~value);
    Decoder decoder(wire.cbegin(), wire.cend());
    std::uint32_t first = 0;
    std::uint32_t second = 0;
    decoder(first);
    decoder(second);
    EXPECT_EQ(value, first);
    EXPECT_EQ(~value, second);
  }
}

}  // namespace
