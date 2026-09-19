// See crc32.h. The table is built once from the reflected polynomial.
#include "lib/crc32.h"

namespace mx {

namespace {
struct Table {
  std::uint32_t entries[256];
  Table() {
    for (std::uint32_t index = 0; index < 256; ++index) {
      std::uint32_t value = index;
      for (int bit = 0; bit < 8; ++bit)
        value = (value & 1) ? (value >> 1) ^ 0xEDB88320u : value >> 1;
      entries[index] = value;
    }
  }
};
const Table table;
} // namespace

std::uint32_t crc32(const void *data, std::size_t size) {
  const unsigned char *bytes = static_cast<const unsigned char *>(data);
  std::uint32_t crc = 0xFFFFFFFFu;
  for (std::size_t index = 0; index < size; ++index)
    crc = table.entries[(crc ^ bytes[index]) & 0xFFu] ^ (crc >> 8);
  return crc ^ 0xFFFFFFFFu;
}

} // namespace mx
