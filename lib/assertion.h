// Assert, AssertMsg, DbgAssert; see the comment at the macros.
#ifndef MX_LIB_ASSERTION_H_
#define MX_LIB_ASSERTION_H_

#include "lib/exception.h"
#include <boost/preprocessor/stringize.hpp>

// Assert and AssertMsg are always compiled in, and they throw
// mx::AssertionError rather than abort, after printing the site to stderr.
// Use them for invariants of our own code only: input from the network is
// checked with plain ifs and answered by closing the connection, never
// asserted. DbgAssert is the same but compiled out with NDEBUG.
#define Assert(w)                                                                                                      \
  do {                                                                                                                 \
    if (!(w))                                                                                                          \
      ::mx::_AssertionFailed(__FILE__, __LINE__, __PRETTY_FUNCTION__, BOOST_PP_STRINGIZE(w));                          \
  } while (0)

#define AssertMsg(w, args...)                                                                                          \
  do {                                                                                                                 \
    if (!(w))                                                                                                          \
      ::mx::_AssertionFailed(__FILE__, __LINE__, __PRETTY_FUNCTION__, BOOST_PP_STRINGIZE(w), args);                    \
  } while (0)

#ifndef NDEBUG
#define DbgAssert Assert
#define DbgAssertMsg AssertMsg
#else
// TODO(kk) we may want to disable DbgAssert without disabling all assert()'s in
// the productional environment
#define DbgAssert(...)
#define DbgAssertMsg(...)
#endif

namespace mx {

struct AssertionError : public Exception {
public:
  AssertionError(const std::string &file, unsigned int line, const std::string &function, const std::string &question,
                 const std::string &explanation) throw();
};

void _AssertionFailed(const char *file, unsigned int line, const char *function, const char *question,
                      const std::string &explanation = "");

}; // namespace mx

#endif // MX_LIB_ASSERTION_H_
