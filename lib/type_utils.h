// type_name(x): the demangled dynamic type of x, for error messages.
#ifndef MX_LIB_TYPE_UTILS_H_
#define MX_LIB_TYPE_UTILS_H_

#include <cxxabi.h>

namespace mx {
namespace type_utils {

template <typename T> std::string type_name(const T &t) {
  int status;
  char *name = abi::__cxa_demangle(typeid(t).name(), 0, 0, &status);

  if (status != 0)
    return typeid(t).name();

  std::string sname = name;
  free(name);
  return sname;
}

}; // namespace type_utils
}; // namespace mx

#endif // MX_LIB_TYPE_UTILS_H_
