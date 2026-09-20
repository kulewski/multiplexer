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

  template <typename Decoder>
  static void decode(Decoder& dec, std::uint32_t& n) {
    n = dec.read_byte() + ((std::uint32_t)dec.read_byte() << 8) + ((std::uint32_t)dec.read_byte() << 16) +
        ((std::uint32_t)dec.read_byte() << 24);
  }
};

};  // namespace encodings
};  // namespace mx

#endif  // MX_LIB_ENCODING_LITTLE_ENDIAN_H_
