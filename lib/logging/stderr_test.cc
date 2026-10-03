// A log line stderr does not take is dropped and counted, and the next
// that gets through says how many were, ahead of it: stderr here is a
// pipe that does not block, filled so that every write fails with EAGAIN,
// then emptied. The lines went through std::cerr, which the first failed
// write left failing for good: every later line was dropped unsaid, the
// one after the pipe was emptied too. Counted, not timed: what the pipe
// holds is read once the line was written.
#include <fcntl.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <string>

#include "lib/logging/logging.h"

namespace {

// Everything the pipe holds, read without waiting.
std::string drain(int fd) {
  std::string text;
  char buffer[4096];
  for (;;) {
    const ssize_t got = ::read(fd, buffer, sizeof buffer);
    if (got <= 0) {
      return text;
    }
    text.append(buffer, static_cast<std::size_t>(got));
  }
}

}  // namespace

TEST(Stderr, LinesItDoesNotTakeAreCountedAndSaidByTheNextThatGetsThrough) {
  int ends[2];
  ASSERT_EQ(0, ::pipe2(ends, O_NONBLOCK));
  const int saved = ::dup(STDERR_FILENO);
  ASSERT_EQ(STDERR_FILENO, ::dup2(ends[1], STDERR_FILENO));
  const std::string block(4096, 'b');
  while (::write(ends[1], block.data(), block.size()) > 0) {
  }
  while (::write(ends[1], "b", 1) > 0) {  // full to the last byte
  }
  for (int index = 0; index < 3; ++index) {
    MX_LOG(ERROR, LOWVERBOSITY, TEXT("dropped " + std::to_string(index)));
  }
  drain(ends[0]);  // the filling
  MX_LOG(ERROR, LOWVERBOSITY, TEXT("gets through"));
  const std::string text = drain(ends[0]);
  ::dup2(saved, STDERR_FILENO);
  ::close(saved);
  ::close(ends[0]);
  ::close(ends[1]);
  const std::string::size_type notice = text.find("txt=\"3 log entries lost: stderr did not take them\"");
  const std::string::size_type line = text.find("txt=\"gets through\"");
  ASSERT_NE(std::string::npos, line) << "the line after the pipe was emptied: " << text;
  ASSERT_NE(std::string::npos, notice) << text;
  EXPECT_LT(notice, line) << "the count first";
  EXPECT_EQ(std::string::npos, text.find("dropped")) << text;
}
