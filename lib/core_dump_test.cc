// enable_core_dump() raises the soft core limit to the hard one under a
// finite hard limit, and says nothing: it asked for unlimited outright,
// which an unprivileged process is refused, kept the soft limit where it
// was, and warned on every run. Unlimited where the process may raise its
// hard limit, which an unprivileged test process may not. In a forked
// child, whose limits and stderr nothing else of the test's shares.
#include "lib/core_dump.h"

#include <fcntl.h>
#include <gtest/gtest.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cstdlib>
#include <string>

TEST(CoreDump, TheSoftLimitReachesAFiniteHardOneUnsaid) {
  const char* directory = std::getenv("TEST_TMPDIR");
  const std::string said = std::string(directory ? directory : "/tmp") + "/core_dump_stderr";
  const pid_t pid = fork();
  ASSERT_NE(-1, pid);
  if (pid == 0) {
    const struct rlimit lowered = {0, 1 << 20};  // what an unprivileged shell often sets: none, and a cap
    if (setrlimit(RLIMIT_CORE, &lowered) != 0) {
      _exit(10);
    }
    const int out = open(said.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0600);
    if (out < 0 || dup2(out, STDERR_FILENO) != STDERR_FILENO) {
      _exit(11);
    }
    mx::enable_core_dump();
    struct rlimit now;
    if (getrlimit(RLIMIT_CORE, &now) != 0) {
      _exit(12);
    }
    _exit(now.rlim_cur == now.rlim_max ? 0 : 1);
  }
  int status = 0;
  ASSERT_EQ(pid, waitpid(pid, &status, 0));
  ASSERT_TRUE(WIFEXITED(status));
  EXPECT_EQ(0, WEXITSTATUS(status)) << "1: the soft limit stayed below the hard one";
  struct stat written;
  ASSERT_EQ(0, stat(said.c_str(), &written));
  EXPECT_EQ(0, written.st_size) << "nothing said on stderr";
}
