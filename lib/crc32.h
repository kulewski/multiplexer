// mx::crc32(data, size): the standard CRC-32 (IEEE 802.3, the one zlib and
// Python's zlib.crc32 compute), table-driven, for the frame header. Peers
// in any language must agree on it, so the polynomial, the initial value
// and the final inversion are the conventional ones and nothing else.
#ifndef MX_LIB_CRC32_H_
#define MX_LIB_CRC32_H_

#include <cstddef>
#include <cstdint>

namespace mx {

std::uint32_t crc32(const void* data, std::size_t size);

}  // namespace mx

#endif  // MX_LIB_CRC32_H_
