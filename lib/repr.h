// repr(x): a string form of x for log messages, through operator<<, with
// strings quoted and vectors bracketed; from_string<T>(text), the way
// back for numbers. Not fast; keep repr inside MX_LOG TEXT() so it only
// runs when the entry is emitted.
#ifndef MX_LIB_REPR_H_
#define MX_LIB_REPR_H_

#include <sstream>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <vector>

namespace mx {

/*
 * repr(value) -> the value streamed into a string
 * with string quoted
 */
template <typename T>
inline std::string repr(const T& value) {
  std::ostringstream out;
  out << value;
  return out.str();
}

// The number `text` holds, as T; std::invalid_argument when it is not one
// or has anything after it, or for an unsigned T, is negative: a stream
// reads "-1" as an unsigned number's largest, which made --max-bytes -1
// no cap and a port of -1 65535.
template <typename T>
inline T from_string(const std::string& text) {
  if (std::is_unsigned<T>::value) {
    const std::string::size_type first = text.find_first_not_of(" \t\n\v\f\r");
    if (first != std::string::npos && text[first] == '-') {
      throw std::invalid_argument("not a number of 0 or more: '" + text + "'");
    }
  }
  std::istringstream in(text);
  T value;
  if (!(in >> value) || !in.eof()) {
    throw std::invalid_argument("not a number: '" + text + "'");
  }
  return value;
}

static inline std::string repr(const std::string& value) { return "'" + value + "'"; }

template <typename T, typename Alloc>
static inline std::string repr(const std::vector<T, Alloc>& vec) {
  std::string out = "[";
  for (unsigned int index = 0; index < vec.size(); ++index) {
    if (index) {
      out += ", ";
    }
    out += repr(vec[index]);
  }
  return out;
}

};  // namespace mx

#endif  // MX_LIB_REPR_H_
