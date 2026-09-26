// LogSummary on its own, with an io_service of the test's: the first line
// of a kind is the caller's, the rest come out as one count a second, a
// kind gone quiet is forgotten and the timer stops, kinds beyond the cap
// share one count, and a level the settings drop costs nothing. Every check
// is on counts in what was logged; the io_service runs until it has no
// work left, which is also the proof that nothing keeps waking it.
#include "multiplexer/log_summary.h"

#include <gtest/gtest.h>

#include <asio/io_service.hpp>
#include <regex>
#include <string>

#include "lib/logging/logging.h"

namespace {

using multiplexer::LogSummary;

// The sum of N over every "[N more ...]" in `log` after `text`, and how
// many such lines there were.
std::pair<std::uint64_t, int> said_more(const std::string& log, const std::string& text) {
  const std::regex line(std::regex_replace(text, std::regex(R"([.^$|()\[\]{}*+?\\])"), R"(\$&)") +
                        R"( \[(\d+) more in the last \d+\.\d s\])");
  std::uint64_t sum = 0;
  int lines = 0;
  for (std::sregex_iterator match(log.begin(), log.end(), line), end; match != end; ++match) {
    sum += std::stoull((*match)[1].str());
    ++lines;
  }
  return {sum, lines};
}

class LogSummaryTest : public ::testing::Test {
 protected:
  void SetUp() override {
    ASSERT_TRUE(mx::logging::apply_verbosity_spec("HIGH"));
    ASSERT_TRUE(mx::logging::apply_verbosity_spec("DEBUG:ZERO"));
  }
  asio::io_service io_service;
};

TEST_F(LogSummaryTest, TheFirstOfAKindIsTheCallersAndTheRestOneCount) {
  LogSummary summary(io_service, "test");
  const LogSummary::Kind kind{1, WARNING, 207, 0};
  int first = 0;
  for (int line = 0; line < 1000; ++line) {
    const std::string* text = summary.first(kind, [] { return std::string("routing nowhere (TEST)"); });
    if (text) {
      ++first;
      EXPECT_EQ("routing nowhere (TEST)", *text);
    }
  }
  EXPECT_EQ(1, first) << "the first only; the caller logs it";
  ::testing::internal::CaptureStderr();
  const std::size_t handlers = io_service.run();  // one tick says the count, the next forgets the kind
  const std::string log = ::testing::internal::GetCapturedStderr();
  EXPECT_EQ(2u, handlers) << "the timer stops once the kind is forgotten";
  const std::pair<std::uint64_t, int> said = said_more(log, "routing nowhere (TEST)");
  EXPECT_EQ(999u, said.first) << log;
  EXPECT_EQ(1, said.second) << log;
  EXPECT_NE(nullptr, summary.first(kind, [] { return std::string("routing nowhere (TEST)"); }))
      << "a kind that went quiet for a whole interval is said at once again";
}

TEST_F(LogSummaryTest, KindsAreCountedApart) {
  LogSummary summary(io_service, "test");
  int first = 0;
  for (int line = 0; line < 100; ++line) {
    first += summary.first({2, WARNING, 0, 11}, [] { return std::string("to 11"); }) != nullptr;
    first += summary.first({2, WARNING, 0, 12}, [] { return std::string("to 12"); }) != nullptr;
    first += summary.first({2, ERROR, 0, 12}, [] { return std::string("to 12 at ERROR"); }) != nullptr;
  }
  EXPECT_EQ(3, first);
  ::testing::internal::CaptureStderr();
  io_service.run();
  const std::string log = ::testing::internal::GetCapturedStderr();
  EXPECT_EQ(99u, said_more(log, "to 11").first) << log;
  EXPECT_EQ(99u, said_more(log, "to 12").first) << log;
  EXPECT_EQ(99u, said_more(log, "to 12 at ERROR").first) << log;
}

TEST_F(LogSummaryTest, KindsBeyondTheCapShareOneCount) {
  LogSummary summary(io_service, "test");
  const unsigned int beyond = 10;
  int first = 0;
  for (unsigned int peer = 1; peer <= LogSummary::MAX_KINDS + beyond; ++peer) {
    first += summary.first({3, WARNING, 0, peer}, [&] { return "to " + std::to_string(peer); }) != nullptr;
  }
  EXPECT_EQ(static_cast<int>(LogSummary::MAX_KINDS), first);
  ::testing::internal::CaptureStderr();
  io_service.run();
  const std::string log = ::testing::internal::GetCapturedStderr();
  EXPECT_NE(std::string::npos, log.find("[" + std::to_string(beyond) + " more lines of other kinds in the last"))
      << log;
}

TEST_F(LogSummaryTest, ALevelTheSettingsDropCountsNothingAndArmsNothing) {
  LogSummary summary(io_service, "test");
  for (int line = 0; line < 100; ++line) {
    EXPECT_EQ(nullptr, summary.first({4, DEBUG, 0, 0}, [] { return std::string("debug"); }));
  }
  EXPECT_EQ(0u, io_service.run()) << "no timer was armed";
}

TEST_F(LogSummaryTest, FlushSaysTheCountsAtOnceAndStopsTheTimer) {
  LogSummary summary(io_service, "test");
  for (int line = 0; line < 5; ++line) {
    summary.first({5, WARNING, 0, 0}, [] { return std::string("leaving"); });
  }
  ::testing::internal::CaptureStderr();
  summary.flush();
  const std::string flushed = ::testing::internal::GetCapturedStderr();
  EXPECT_EQ(4u, said_more(flushed, "leaving").first) << flushed;
  ::testing::internal::CaptureStderr();
  io_service.run();  // only the cancelled wait's handler
  const std::string after = ::testing::internal::GetCapturedStderr();
  EXPECT_EQ(0, said_more(after, "leaving").second) << after;
}

}  // namespace
