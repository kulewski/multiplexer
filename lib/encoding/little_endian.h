// Little-endian uint32, the encoding of the frame header (length, CRC) on
// the wire; see multiplexer/io/raw_message.h.
#ifndef MX_LIB_ENCODING_LITTLE_ENDIAN_H_
#define MX_LIB_ENCODING_LITTLE_ENDIAN_H_

#include <boost/cstdint.hpp>

namespace mx {
namespace encodings {

template <typename T> struct LittleEndian;

template <> struct LittleEndian<boost::uint32_t> {
  template <typename Encoder> static void encode(Encoder &enc, boost::uint32_t n) {
    enc.write_byte(n & 0xff);
    enc.write_byte((n >> 8) & 0xff);
    enc.write_byte((n >> 16) & 0xff);
    enc.write_byte((n >> 24) & 0xff);
  }

  template <typename Decoder> static void decode(Decoder &dec, boost::uint32_t &n) {
    n = dec.read_byte() + ((boost::uint32_t)dec.read_byte() << 8) + ((boost::uint32_t)dec.read_byte() << 16) +
        ((boost::uint32_t)dec.read_byte() << 24);
  }
};

}; // namespace encodings
}; // namespace mx

#endif // MX_LIB_ENCODING_LITTLE_ENDIAN_H_
