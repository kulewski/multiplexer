// The logging settings may change while other threads log: the process
// context and the verbosity table, set on one thread, are read on the
// others, by every line logged and every check made, without a data race,
// and every line carries a whole context, one of those set, never one
// half written. Under ThreadSanitizer, which check.sh runs over //lib/...,
// a race on either fails it, as the plain string and array did; without
// it the test checks the contexts the lines carry. Counted, not timed: a
// fixed number of lines and of settings.
#include <gtest/gtest.h>

#include <regex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "lib/logging/logging.h"

namespace {
const int THREADS = 4;
const int LINES = 500;     // logged by each thread
const int SETTINGS = 500;  // changes of the context and the verbosity meanwhile
}  // namespace

TEST(ConcurrentSettings, ThreadsLogWhileTheContextAndTheVerbosityChange) {
  mx::logging::set_maximal_logging_verbosity(WARNING, LOWVERBOSITY);
  mx::logging::set_maximal_logging_verbosity(DEBUG, LOWVERBOSITY);  // below what the threads check
  mx::logging::set_process_context_program_name("third");
  const std::set<std::string> contexts = {"first", "second", mx::logging::process_context()};
  mx::logging::set_process_context("first");
  ::testing::internal::CaptureStderr();
  std::vector<std::thread> loggers;
  for (int thread = 0; thread < THREADS; ++thread) {
    loggers.emplace_back([] {
      for (int line = 0; line < LINES; ++line) {
        MX_LOG(WARNING, LOWVERBOSITY, TEXT("logged"));
        MX_LOG(DEBUG, HIGHVERBOSITY, TEXT("never logged"));  // reads the entry the other thread writes
      }
    });
  }
  for (int setting = 0; setting < SETTINGS; ++setting) {
    if (setting % 100 == 50) {
      mx::logging::set_process_context_program_name("third");
    } else {
      mx::logging::set_process_context(setting % 2 ? "first" : "second");
    }
    mx::logging::set_maximal_logging_verbosity(DEBUG, setting % 2 ? ZEROVERBOSITY : LOWVERBOSITY);
  }
  for (std::thread& logger : loggers) {
    logger.join();
  }
  const std::string log = ::testing::internal::GetCapturedStderr();
  const std::regex logged(R"(ctx=(\S*)  flw="[^"]*"  txt="logged")");
  int lines = 0;
  for (std::sregex_iterator match(log.begin(), log.end(), logged), end; match != end; ++match) {
    ++lines;
    EXPECT_EQ(1u, contexts.count((*match)[1].str())) << "a context not set: " << (*match)[1].str();
  }
  EXPECT_EQ(THREADS * LINES, lines);
  EXPECT_EQ(std::string::npos, log.find("never logged"));
}
