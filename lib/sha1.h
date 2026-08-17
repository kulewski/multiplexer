// mx::sha1_hex(text): the SHA-1 of a string as 40 lowercase hex digits,
// from Boost's implementation. Used to fingerprint the rules file: the
// generated constants carry it, a recording's header carries it, and a
// reader can tell whether the two agree.
#ifndef MX_LIB_SHA1_H_
#define MX_LIB_SHA1_H_

#include <cstdio>
#include <string>

#include <boost/uuid/detail/sha1.hpp>

namespace mx {

inline std::string sha1_hex(const std::string &text) {
  boost::uuids::detail::sha1 sha1;
  sha1.process_bytes(text.data(), text.size());
  unsigned int digest[5];
  sha1.get_digest(digest);
  char hex[41];
  for (int word = 0; word < 5; ++word)
    std::snprintf(hex + word * 8, 9, "%08x", digest[word]);
  return std::string(hex, 40);
}

} // namespace mx

#endif // MX_LIB_SHA1_H_
