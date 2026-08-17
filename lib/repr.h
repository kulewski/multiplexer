// repr(x): a string form of x for log messages, via lexical_cast, with
// strings quoted and vectors bracketed. Not fast; keep it inside MX_LOG
// TEXT() so it only runs when the entry is emitted.
#ifndef MX_LIB_REPR_H_
#define MX_LIB_REPR_H_

#include <boost/lexical_cast.hpp>
#include <string>
#include <vector>

namespace mx {

/*
 * repr(value) -> lexical_cast<std::string>(value)
 * with string quoted
 */
template <typename T> inline std::string repr(const T &value) { return boost::lexical_cast<std::string>(value); }

static inline std::string repr(const std::string &value) { return "'" + value + "'"; }

template <typename T, typename Alloc> static inline std::string repr(const std::vector<T, Alloc> &vec) {
  std::string out = "[";
  for (unsigned int index = 0; index < vec.size(); ++index) {
    if (index)
      out += ", ";
    out += repr(vec[index]);
  }
  return out;
}

}; // namespace mx

#endif // MX_LIB_REPR_H_
