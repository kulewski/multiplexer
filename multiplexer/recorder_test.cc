// The session label rules and the session file name, the parts of remote
// recording that keep a request from choosing a path. And a recording file
// appended to: a record a multiplexer that died left half written at its
// end is cut first, so that every record after it reads, where the reader
// stopped at the torn one; a file that is not a recording, or a recording
// garbled before its end, is left as it is and not recorded into, where
// it was cut at its first record that did not parse; and a recorder counts
// the bytes it wrote, not the file's, where an appended --record file's
// earlier runs were counted as this session's.
#include "multiplexer/recorder.h"

#include <fcntl.h>
#include <gtest/gtest.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstdlib>
#include <fstream>
#include <sstream>
#include <string>

#include "lib/protobuf/stream.h"

using multiplexer::Record;
using multiplexer::Recorder;
using multiplexer::recording::session_path;
using multiplexer::recording::valid_label;

namespace {

// A file of the test's own, removed with it.
struct ScratchFile {
  ScratchFile() {
    const char* directory = std::getenv("TEST_TMPDIR");
    path = std::string(directory ? directory : "/tmp") + "/recorder_test.XXXXXX";
    const int fd = ::mkstemp(&path[0]);
    if (fd >= 0) {
      ::close(fd);
    }
  }
  ~ScratchFile() { ::unlink(path.c_str()); }
  std::string path;
};

// The size of the file at `path`.
std::uint64_t size_of(const std::string& path) {
  struct stat file;
  return ::stat(path.c_str(), &file) == 0 ? static_cast<std::uint64_t>(file.st_size) : 0;
}

// A routed record stamped `when`.
Record routed(std::uint64_t when) {
  Record record;
  record.set_timestamp_us(when);
  record.mutable_routed()->set_payload(std::string(300, 'x'));
  return record;
}

// The records in the file at `path`, read as dump_recording reads them,
// and whether the reading stopped at a broken one rather than the end.
std::pair<int, bool> whole_records(const std::string& path) {
  mx::protobuf::FileMessageInputStream stream(::open(path.c_str(), O_RDONLY), true);
  Record record;
  int records = 0;
  while (stream.read(record)) {
    ++records;
  }
  return {records, stream.failed()};
}

}  // namespace

TEST(RecordingLabel, AcceptsSafeNames) {
  EXPECT_TRUE(valid_label("session"));
  EXPECT_TRUE(valid_label("checkout-bug_2"));
  EXPECT_TRUE(valid_label("A"));
  EXPECT_TRUE(valid_label(std::string(64, 'x')));
}

TEST(RecordingLabel, RefusesAnythingThatCouldBeAPath) {
  EXPECT_FALSE(valid_label(""));
  EXPECT_FALSE(valid_label("../etc"));
  EXPECT_FALSE(valid_label("a/b"));
  EXPECT_FALSE(valid_label("a b"));
  EXPECT_FALSE(valid_label("a.rec"));
  EXPECT_FALSE(valid_label(std::string(65, 'x')));
}

TEST(SessionPath, NamesTheMultiplexerAndTheTime) {
  // 2026-09-10T18:30:12.123456Z
  const std::uint64_t started_us = 1789065012123456ULL;
  EXPECT_EQ("/var/recordings/session.20260910T183012.123456Z.42.rec",
            session_path("/var/recordings", "session", 42, started_us));
  EXPECT_EQ("/var/recordings/session.20260910T183012.123456Z.42.rec",
            session_path("/var/recordings/", "session", 42, started_us));
}

TEST(SessionPath, DiffersPerMultiplexerAndPerSession) {
  const std::uint64_t started_us = 1789065012123456ULL;
  EXPECT_NE(session_path("d", "s", 1, started_us), session_path("d", "s", 2, started_us));
  EXPECT_NE(session_path("d", "s", 1, started_us), session_path("d", "s", 1, started_us + 1));
}

// A file ending in half a record, as a multiplexer that died while writing
// leaves it: the next recorder cuts it and appends after the last whole
// record, so that the reader reads every record to the end.
TEST(RecorderAppend, CutsARecordLeftHalfWritten) {
  ScratchFile file;
  {
    Recorder first(file.path, 0);
    ASSERT_TRUE(first.ok());
    first.header(1, "rules", "");
    first.write(routed(10));
    first.flush();
  }
  std::ostringstream framed;  // a record as the stream writes it, of which half goes to the file
  mx::protobuf::OstreamMessageOutputStream(&framed, false).write(routed(20));
  {
    std::ofstream out(file.path, std::ios::app | std::ios::binary);
    out << framed.str().substr(0, framed.str().size() / 2);
  }
  EXPECT_TRUE(whole_records(file.path).second) << "the torn record";
  {
    Recorder second(file.path, 0);
    ASSERT_TRUE(second.ok());
    second.header(2, "rules", "");
    second.write(routed(30));
    second.flush();
  }
  EXPECT_EQ(std::make_pair(4, false), whole_records(file.path)) << "two headers, two records, to the end";
}

// A file ending inside a record's length, its first byte of two written:
// cut as any torn record is.
TEST(RecorderAppend, CutsARecordWhoseLengthWasLeftHalfWritten) {
  ScratchFile file;
  {
    Recorder first(file.path, 0);
    first.header(1, "rules", "");
    first.flush();
  }
  std::ostringstream framed;
  mx::protobuf::OstreamMessageOutputStream(&framed, false).write(routed(20));
  ASSERT_NE(0, framed.str()[0] & 0x80) << "a length of two bytes";
  {
    std::ofstream out(file.path, std::ios::app | std::ios::binary);
    out << framed.str().substr(0, 1);
  }
  {
    Recorder second(file.path, 0);
    ASSERT_TRUE(second.ok());
    second.header(2, "rules", "");
    second.flush();
  }
  EXPECT_EQ(std::make_pair(2, false), whole_records(file.path)) << "two headers, to the end";
}

// The bytes of the file at `path`.
std::string contents_of(const std::string& path) {
  std::ifstream in(path, std::ios::binary);
  std::ostringstream all;
  all << in.rdbuf();
  return all.str();
}

// A file that is not a recording, a rules file named by mistake, long or
// short, is left as it is and nothing is recorded into it, where the first
// record that did not parse was taken for a torn one and the file was cut
// there, at its first byte: emptied, then appended to.
TEST(RecorderAppend, LeavesAFileThatIsNotARecordingAsItIs) {
  const std::string rules =
      "# The rules of this deployment.\n"
      "peer {\n    type: 102\n    name: \"WEBSITE\"\n    is_passive: true\n}\n";
  for (const std::string& text : {rules, std::string("peer {\n")}) {
    ScratchFile file;
    {
      std::ofstream out(file.path, std::ios::binary | std::ios::trunc);
      out << text;
    }
    {
      Recorder recorder(file.path, 0);
      EXPECT_FALSE(recorder.ok()) << "recording into " << text;
      recorder.header(1, "rules", "");
      recorder.write(routed(10));
      recorder.flush();
    }
    EXPECT_EQ(text, contents_of(file.path));
  }
}

// A recording with a record garbled in its middle, its length whole but
// its bytes not a record, is left as it is too: what follows the damage
// is someone's to recover by hand, where it was cut away with the damage.
TEST(RecorderAppend, LeavesARecordingGarbledInItsMiddleAsItIs) {
  ScratchFile file;
  {
    Recorder first(file.path, 0);
    first.header(1, "rules", "");
    first.write(routed(10));
    first.flush();
  }
  std::string damaged = contents_of(file.path);
  {
    std::ostringstream framed;
    mx::protobuf::OstreamMessageOutputStream(&framed, false).write(routed(20));
    std::string garbled = framed.str();
    for (std::size_t at = 2; at < garbled.size(); ++at) {
      garbled[at] = '\xff';  // the length whole, the rest no record
    }
    damaged += garbled + framed.str();
    std::ofstream out(file.path, std::ios::binary | std::ios::trunc);
    out << damaged;
  }
  {
    Recorder second(file.path, 0);
    EXPECT_FALSE(second.ok());
    second.header(2, "rules", "");
    second.flush();
  }
  EXPECT_EQ(damaged, contents_of(file.path));
}

// A recorder on a file that exists counts what it wrote, the file growing
// by as much, not the earlier runs' bytes.
TEST(RecorderAppend, CountsThisSessionsBytes) {
  ScratchFile file;
  {
    Recorder first(file.path, 0);
    first.header(1, "rules", "");
    first.write(routed(10));
    first.flush();
  }
  const std::uint64_t before = size_of(file.path);
  ASSERT_GT(before, 0u);
  Recorder second(file.path, 0);
  second.header(2, "rules", "");
  second.write(routed(20));
  second.flush();
  EXPECT_EQ(size_of(file.path) - before, second.bytes());
  EXPECT_EQ(2u, second.records());
}
