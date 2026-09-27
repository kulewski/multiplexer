// The binary log stream with threads: two threads logging at once, the
// stream replaced while another thread logs, and forks while another thread
// logs. Every entry is read back whole from the file it went to, in the
// order its thread wrote it, and counted, where the threads wrote into one
// buffer, or into one being freed, and crashed; a forked child logs and
// exits, where one forked while the other thread held the stream's lock
// would wait for it until its alarm.
#include "lib/protobuf/stream.h"

#include <fcntl.h>
#include <gtest/gtest.h>
#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <string>
#include <thread>
#include <vector>

#include "lib/logging/logging.h"

namespace {

using mx::logging::LogEntry;

// How many entries a thread writes; the ids of thread `n` start at n * kEntries.
const std::uint64_t kEntries = 20000;

// Opens a new empty file in the test's scratch directory for writing; its
// path goes to `path`.
int scratch_file(const std::string& name, std::string* path) {
  const char* directory = std::getenv("TEST_TMPDIR");
  *path = std::string(directory ? directory : "/tmp") + "/stream_test_" + name + "_XXXXXX";
  return mkstemp(&(*path)[0]);
}

// Writes an entry with `id` to the binary stream only, not to stderr.
void log_entry(std::uint64_t id) {
  LogEntry entry;
  entry.set_id(id);
  entry.set_text(std::string(200, 'x'));
  mx::logging::impl::_emit_log(entry);
}

// The ids of the entries in the file at `path`, in the order written, up
// to the first that does not read whole.
std::vector<std::uint64_t> ids_in(const std::string& path) {
  mx::protobuf::FileMessageInputStream input(open(path.c_str(), O_RDONLY), /*own_fd=*/true);
  std::vector<std::uint64_t> ids;
  LogEntry entry;
  while (input.read(entry)) {
    ids.push_back(entry.id());
  }
  return ids;
}

// The exit code of the child `pid`, or -1 when a signal ended it: the
// alarm, when it waited for good.
int exit_code_of(pid_t pid) {
  int status = 0;
  if (waitpid(pid, &status, 0) != pid || !WIFEXITED(status)) {
    return -1;
  }
  return WEXITSTATUS(status);
}

}  // namespace

TEST(LogStream, TwoThreadsLoggingAtOnceWriteEveryEntryWhole) {
  std::string path;
  const int fd = scratch_file("two_threads", &path);
  ASSERT_LE(0, fd);
  mx::logging::set_logging_fd(fd, /*close_on_delete=*/true, /*log_the_fact=*/false);
  std::vector<std::thread> writers;
  for (std::uint64_t writer = 0; writer < 2; ++writer) {
    writers.emplace_back([writer] {
      for (std::uint64_t index = 0; index < kEntries; ++index) {
        log_entry(writer * kEntries + index);
      }
    });
  }
  for (auto& writer : writers) {
    writer.join();
  }
  std::uint64_t next[2] = {0, kEntries};
  const std::vector<std::uint64_t> ids = ids_in(path);
  for (const std::uint64_t id : ids) {
    const std::uint64_t writer = id / kEntries;
    ASSERT_LT(writer, 2u) << "an entry no thread wrote";
    ASSERT_EQ(next[writer], id) << "an entry out of its thread's order";
    ++next[writer];
  }
  EXPECT_EQ(2 * kEntries, ids.size());
}

TEST(LogStream, AStreamReplacedWhileAnotherThreadLogsLosesNoEntry) {
  std::string paths[2];
  int fds[2];
  for (int index = 0; index < 2; ++index) {
    fds[index] = scratch_file("replaced_" + std::to_string(index), &paths[index]);
    ASSERT_LE(0, fds[index]);
  }
  mx::logging::set_logging_fd(fds[0], /*close_on_delete=*/false, /*log_the_fact=*/false);
  std::atomic<bool> replacing{true};
  std::uint64_t written = 0;
  std::thread writer([&] {
    while (replacing.load()) {
      log_entry(written++);
    }
  });
  for (int replacement = 1; replacement <= 2000; ++replacement) {
    mx::logging::set_logging_fd(fds[replacement % 2], /*close_on_delete=*/false, /*log_the_fact=*/false);
  }
  replacing = false;
  writer.join();
  // Every entry whole in one of the two files, each file in the order written.
  std::uint64_t read = 0;
  for (const std::string& path : paths) {
    const std::vector<std::uint64_t> ids = ids_in(path);
    for (size_t index = 1; index < ids.size(); ++index) {
      ASSERT_LT(ids[index - 1], ids[index]) << path;
    }
    read += ids.size();
  }
  EXPECT_EQ(written, read);
}

TEST(LogStream, AChildForkedWhileAnotherThreadLogsCanLog) {
  std::string path;
  const int fd = scratch_file("forks", &path);
  ASSERT_LE(0, fd);
  mx::logging::set_logging_fd(fd, /*close_on_delete=*/true, /*log_the_fact=*/false);
  std::atomic<bool> forking{true};
  std::thread writer([&] {
    for (std::uint64_t id = 0; forking.load(); ++id) {
      log_entry(id);
    }
  });
  // Twenty children, each logging and re-arming the stream, up to the
  // first that does not exit on its own.
  int stuck = -1;
  for (int child = 0; child < 20 && stuck < 0; ++child) {
    const pid_t pid = fork();
    if (pid == 0) {
      alarm(10);
      log_entry(0);
      mx::logging::set_logging_fd(open("/dev/null", O_WRONLY), /*close_on_delete=*/true, /*log_the_fact=*/false);
      _exit(0);
    }
    if (pid < 0 || exit_code_of(pid) != 0) {
      stuck = child;
    }
  }
  forking = false;
  writer.join();
  EXPECT_EQ(-1, stuck) << "a child waited for the stream's lock until its alarm";
}
