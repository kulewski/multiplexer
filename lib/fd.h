// Fd: a file descriptor that is closed on destruction when owned.
#ifndef MX_LIB_FD_H_
#define MX_LIB_FD_H_

#include <boost/noncopyable.hpp>
#include <unistd.h>

namespace mx {
namespace util {

struct Fd : boost::noncopyable {
  explicit Fd(int fd, bool own_fd = false) : fd_(fd), own_fd_(own_fd) {}

  inline int fd() const { return fd_; }
  ~Fd() {
    if (own_fd_ && fd_ >= 0) {
      close(fd_);
    }
  }

private:
  int fd_;
  bool own_fd_;
}; // struct Fd

}; // namespace util
}; // namespace mx

#endif // MX_LIB_FD_H_
