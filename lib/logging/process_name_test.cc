// The program's name in the log context is the basename of argv[0], spaces
// and all, and "unknown" for an empty argv[0] or one ending in '/': loading
// the library ended such a process with an assertion in a static
// initializer, and cut a name at its first space. The test runs itself
// again, /proc/self/exe, with each argv[0], and the child, told so by
// MX_PROCESS_NAME_PROBE, prints its process context, <host>.<name>.
#include <gtest/gtest.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cstdlib>
#include <iostream>
#include <string>
#include <utility>

#include "lib/logging/logging.h"

namespace {

// What a run with `argv0` printed, and its wait status.
std::pair<std::string, int> run_with(const std::string& argv0) {
  int out[2];
  if (pipe(out) != 0) {
    ADD_FAILURE() << "pipe";
    return {"", -1};
  }
  const pid_t child = fork();
  if (child == 0) {
    dup2(out[1], STDOUT_FILENO);
    close(out[0]);
    close(out[1]);
    setenv("MX_PROCESS_NAME_PROBE", "1", 1);
    char* const argv[] = {const_cast<char*>(argv0.c_str()), nullptr};
    execv("/proc/self/exe", argv);
    _exit(127);
  }
  close(out[1]);
  std::string printed;
  char buffer[256];
  ssize_t count;
  while ((count = read(out[0], buffer, sizeof buffer)) > 0) {
    printed.append(buffer, static_cast<std::size_t>(count));
  }
  close(out[0]);
  int status = -1;
  waitpid(child, &status, 0);
  return {printed, status};
}

// The name part of the context a run with `argv0` printed, after it ended with 0.
std::string name_with(const std::string& argv0) {
  const std::pair<std::string, int> run = run_with(argv0);
  EXPECT_TRUE(WIFEXITED(run.second) && WEXITSTATUS(run.second) == 0)
      << "argv[0] '" << argv0 << "' ended with status " << run.second;
  const std::string context = run.first.substr(0, run.first.find('\n'));
  const std::string::size_type dot = context.find('.');
  return dot == std::string::npos ? "" : context.substr(dot + 1);
}

TEST(ProcessName, TheBasenameOfArgv0) {
  EXPECT_EQ("mxprobe", name_with("mxprobe"));
  EXPECT_EQ("mxprobe", name_with("/usr/local/bin/mxprobe"));
}

TEST(ProcessName, SpacesAndAll) { EXPECT_EQ("my probe", name_with("./my probe")); }

TEST(ProcessName, UnknownForAnEmptyArgv0OrADirectory) {
  EXPECT_EQ("unknown", name_with(""));
  EXPECT_EQ("unknown", name_with("bin/"));
}

}  // namespace

int main(int argc, char** argv) {
  if (std::getenv("MX_PROCESS_NAME_PROBE")) {
    std::cout << mx::logging::process_context() << std::endl;
    return 0;
  }
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
