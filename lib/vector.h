// contains(vector, value): linear search, for the short id lists the
// client's receive loop matches against.
#ifndef MX_LIB_VECTOR_H_
#define MX_LIB_VECTOR_H_

#include <vector>

namespace mx {

template <typename T, typename Alloc, typename U>
static inline bool contains(const std::vector<T, Alloc> &vec, const U &val) {
  for (unsigned int index = 0; index < vec.size(); ++index)
    if (vec[index] == val)
      return true;
  return false;
}

}; // namespace mx

#endif // MX_LIB_VECTOR_H_
