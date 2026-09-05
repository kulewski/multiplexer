// mx::Exception and MXTHROW; see the struct comment.
#ifndef MX_LIB_EXCEPTION_H_
#define MX_LIB_EXCEPTION_H_

#include <exception>
#include <string>

namespace mx {

// Base of every exception thrown by this code: an explanation plus the
// throw site, which MXTHROW fills in. With ABORT_ON_EXCEPTION set in the
// environment, constructing one aborts instead, for a core dump at the
// origin.
struct Exception : public std::exception {

  static bool abort_on_exception();
  static void abort_on_exception(bool);

  Exception() throw();
  explicit Exception(const std::string explanation, const std::string &file = "<unknown file>", int line = 0,
                     const std::string &function = "<unknown function>") throw();

  virtual ~Exception() throw();
  virtual const char *what() const throw();
  // virtual std::string what() throw(); // hides std::exception::what() const

  const std::string &file() const { return file_; }
  const std::string &function() const { return function_; }
  int line() const { return line_; }

  void set_file(const std::string &file) { file_ = file; }
  void set_line(const int line) { line_ = line; }
  void set_function(const std::string &function) { function_ = function; }

protected:
  std::string explanation_;
  std::string file_, function_;
  int line_;
};

struct NotImplementedError : Exception {};

namespace impl {
template <typename T> T raise(T e, const char *file, int line, const char *function) {
  e.set_file(file);
  e.set_line(line);
  e.set_function(function);
  throw e;
}
}; // namespace impl

// Throw an mx::Exception with the file, line and function recorded.
#define MXTHROW(e...) (throw ::mx::impl::raise((e), __FILE__, __LINE__, __PRETTY_FUNCTION__))

}; // namespace mx

#ifdef BOOST_NO_EXCEPTIONS
#include <iostream>
namespace boost {
template <typename T> void throw_exception(const T &t) {
  std::cerr << "gotta throw: " << t.what() << "\n";
  abort();
}

inline void throw_exception(const std::exception &t) {
  std::cerr << "gotta throw: " << t.what() << "\n";
  abort();
}
}; // namespace boost
#endif

#endif // MX_LIB_EXCEPTION_H_
