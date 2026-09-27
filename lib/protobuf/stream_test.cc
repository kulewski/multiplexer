// The length-delimited reader: a stream of any length read whole, past the
// 2 GiB a CodedInputStream reads at most, where one for the whole stream
// stopped there as if the stream ended; and the end of a stream told from
// an entry cut short, at a field's end too, or garbled. Counted: the
// entries read.
#include "lib/protobuf/stream.h"

#include <fcntl.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <gtest/gtest.h>
#include <signal.h>
#include <unistd.h>

#include <cstdint>
#include <cstdlib>
#include <string>
#include <thread>

#include "lib/logging/Logging.pb.h"

namespace {

// An entry with `id` and `size` bytes of data.
mx::logging::LogEntry entry_of(std::uint64_t id, std::size_t size) {
  mx::logging::LogEntry entry;
  entry.set_id(id);
  entry.set_data(std::string(size, 'x'));
  return entry;
}

// `entry` as the stream holds it: its length as a varint, then the entry.
std::string framed(const mx::logging::LogEntry& entry) {
  const std::string body = entry.SerializeAsString();
  std::string frame;
  {
    google::protobuf::io::StringOutputStream output(&frame);
    google::protobuf::io::CodedOutputStream coded(&output);
    coded.WriteVarint64(body.size());
    coded.WriteRaw(body.data(), static_cast<int>(body.size()));
  }
  return frame;
}

// Writes all of `bytes` to `fd`: false when a write failed.
bool write_all(int fd, const std::string& bytes) {
  std::size_t written = 0;
  while (written < bytes.size()) {
    const ssize_t now = ::write(fd, bytes.data() + written, bytes.size() - written);
    if (now <= 0) {
      return false;
    }
    written += static_cast<std::size_t>(now);
  }
  return true;
}

// A scratch file holding `bytes`, open for reading from its start.
int file_holding(const std::string& bytes) {
  const char* directory = std::getenv("TEST_TMPDIR");
  std::string path = std::string(directory ? directory : "/tmp") + "/stream_test_XXXXXX";
  const int fd = ::mkstemp(&path[0]);
  if (fd < 0 || !write_all(fd, bytes) || ::lseek(fd, 0, SEEK_SET) != 0) {
    ADD_FAILURE() << "no scratch file";
  }
  return fd;
}

// How many entries `input` reads before it stops.
int read_all(mx::protobuf::FileMessageInputStream& input) {
  mx::logging::LogEntry entry;
  int count = 0;
  while (input.read(entry)) {
    ++count;
  }
  return count;
}

}  // namespace

TEST(FileMessageInputStream, AStreamPastTwoGibibytesReadsWhole) {
  ::signal(SIGPIPE, SIG_IGN);  // a reader that stops early leaves the writer an EPIPE, not a signal
  int ends[2];
  ASSERT_EQ(0, ::pipe(ends));
  const std::string frame = framed(entry_of(7, 1 << 20));
  const int count = static_cast<int>((std::int64_t(1) << 31) / static_cast<std::int64_t>(frame.size())) + 64;
  std::thread writer([&ends, &frame, count] {
    for (int index = 0; index < count && write_all(ends[1], frame); ++index) {
    }
    ::close(ends[1]);
  });
  int read = 0;
  bool failed = true;
  {
    mx::protobuf::FileMessageInputStream input(ends[0], /*own_fd=*/true);
    read = read_all(input);
    failed = input.failed();
  }  // the read end closed: a writer still writing stops
  writer.join();
  EXPECT_EQ(count, read) << "entries read of the " << count << " written, 2 GiB passed";
  EXPECT_FALSE(failed) << "the stream ended whole";
}

TEST(FileMessageInputStream, TheEndOfAStreamIsNoFailure) {
  mx::protobuf::FileMessageInputStream input(file_holding(framed(entry_of(1, 100)) + framed(entry_of(2, 100))),
                                             /*own_fd=*/true);
  EXPECT_EQ(2, read_all(input));
  EXPECT_FALSE(input.failed());
}

TEST(FileMessageInputStream, AnEntryCutShortIsAFailureNotTheEnd) {
  const std::string whole = framed(entry_of(1, 100));
  const std::string second = framed(entry_of(2, 100));
  // In the middle of the data; right after the id, the end of a field, where
  // what is there parses as an entry of its own; and in the length.
  const std::size_t length_bytes = second.size() - entry_of(2, 100).ByteSizeLong();
  for (const std::size_t cut : {second.size() / 2, length_bytes + 2, std::size_t(0)}) {
    const std::string stream = whole + second.substr(0, cut) + (cut ? "" : std::string("\x80", 1));
    mx::protobuf::FileMessageInputStream input(file_holding(stream), /*own_fd=*/true);
    EXPECT_EQ(1, read_all(input)) << "cut at " << cut;
    EXPECT_TRUE(input.failed()) << "cut at " << cut;
  }
}

TEST(FileMessageInputStream, ALengthNoEntryHasIsAFailure) {
  const std::string garbled("\x80\x80\x80\x80\x80\x20", 6);  // 2^40
  mx::protobuf::FileMessageInputStream input(file_holding(framed(entry_of(1, 100)) + garbled), /*own_fd=*/true);
  EXPECT_EQ(1, read_all(input));
  EXPECT_TRUE(input.failed());
}
