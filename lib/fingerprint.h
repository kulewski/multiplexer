// mx::fingerprint(text): a short, stable name for a file's contents, the
// CRC-32 of the bytes as eight lowercase hex digits. Used to tell whether
// a recording was made with the rules file the generated constants came
// from: the constants carry it, the recording's header carries it, and a
// reader compares the two. Not a checksum against tampering, a name.
#ifndef MX_LIB_FINGERPRINT_H_
#define MX_LIB_FINGERPRINT_H_

#include <cstdio>
#include <string>

#include "lib/crc32.h"

namespace mx {

inline std::string fingerprint(const std::string &text) {
  char hex[9];
  std::snprintf(hex, sizeof hex, "%08x", crc32(text.data(), text.size()));
  return std::string(hex, 8);
}

} // namespace mx

#endif // MX_LIB_FINGERPRINT_H_
