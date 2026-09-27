// Little-endian uint32, the encoding of the frame header (length, CRC) on
// the wire; see multiplexer/io/raw_message.h.
#ifndef MX_LIB_ENCODING_LITTLE_ENDIAN_H_
#define MX_LIB_ENCODING_LITTLE_ENDIAN_H_

#include <cstdint>

namespace mx {
namespace encodings {

template <typename T>
struct LittleEndian;

template <>
struct LittleEndian<std::uint32_t> {
  template <typename Encoder>
  static void encode(Encoder& enc, std::uint32_t n) {
    enc.write_byte(n & 0xff);
    enc.write_byte((n >> 8) & 0xff);
    enc.write_byte((n >> 16) & 0xff);
    enc.write_byte((n >> 24) & 0xff);
  }

  // One statement a byte: each read advances the decoder, and the
  // operands of one expression are read in an order C++ leaves unspecified.
  template <typename Decoder>
  static void decode(Decoder& dec, std::uint32_t& n) {
    const std::uint32_t byte0 = dec.read_byte();
    const std::uint32_t byte1 = dec.read_byte();
    const std::uint32_t byte2 = dec.read_byte();
    const std::uint32_t byte3 = dec.read_byte();
    n = byte0 | (byte1 << 8) | (byte2 << 16) | (byte3 << 24);
  }
};

};  // namespace encodings
};  // namespace mx

#endif  // MX_LIB_ENCODING_LITTLE_ENDIAN_H_
