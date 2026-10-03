// Provides main() for programs that define MxMain(); see the note below.
#ifndef MX_LIB_PROGRAM_H_
#define MX_LIB_PROGRAM_H_

// Defines main() for a program that provides int MxMain(int, char**): runs
// it and returns its result; an uncaught mx::Exception is printed with its
// throw site, any other std::exception with its type, and the program exits
// with 1. Include it from exactly one file of a binary.

#include <fcntl.h>
#include <signal.h>
#include <unistd.h>

#include <cerrno>
#include <cstdlib>
#include <iostream>
#include <thread>

#include "lib/exception.h"
#include "lib/type_utils.h"

int MxMain(int argc, char** argv);

namespace mx {
namespace program_internal {
// Under the test harness (multiplexer/testing), which hands every process
// it starts the read end of a pipe only it writes to, named by
// MX_TEST_PARENT_FD: the program ends as SIGTERM ends it once the pipe
// reaches its end, which is when the test process is gone, however it
// died, so that nothing it started runs on. The variable is cleared and
// the descriptor kept from the program's own children, which could take
// an unrelated descriptor of that number for it. Nothing without it.
inline void end_with_the_test_harness() {
  const char* named = std::getenv("MX_TEST_PARENT_FD");
  if (!named) {
    return;
  }
  const int descriptor = std::atoi(named);
  unsetenv("MX_TEST_PARENT_FD");
  if (descriptor < 0 || fcntl(descriptor, F_SETFD, FD_CLOEXEC) == -1) {
    return;  // not open: nothing to watch
  }
  std::thread([descriptor] {
    char byte;
    for (;;) {
      const ssize_t got = ::read(descriptor, &byte, 1);
      if (got == 0) {
        break;  // the test process is gone
      }
      if (got < 0 && errno != EINTR) {
        return;
      }
    }
    ::kill(::getpid(), SIGTERM);
  }).detach();
}
}  // namespace program_internal
}  // namespace mx

int main(int argc, char** argv) {
  using std::cerr;
  using std::endl;

  mx::program_internal::end_with_the_test_harness();
  try {
    return MxMain(argc, argv);

  } catch (mx::Exception& error) {
    cerr << mx::type_utils::type_name(error) << " in " << error.file() << ":" << error.line() << " ("
         << error.function() << ")\n"
         << "    " << error.what() << endl;
    return 1;

  } catch (std::exception& error) {
    cerr << mx::type_utils::type_name(error) << ": " << error.what() << "\n";
    return 1;
  }
}

#endif  // MX_LIB_PROGRAM_H_
