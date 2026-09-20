// apply_verbosity_spec, the parser behind MX_LOG_VERBOSITY: what it
// accepts, what it refuses, and that a refused spec changes nothing.
#include <gtest/gtest.h>

#include "lib/logging/logging.h"

using namespace mx::logging::consts;

namespace {
bool logs(unsigned int level, unsigned int verbosity) { return mx::logging::impl::should_log(level, verbosity); }
}  // namespace

TEST(VerbositySpec, OneLevelOrEveryLevel) {
  mx::logging::apply_verbosity_spec("HIGH");
  EXPECT_TRUE(logs(DEBUG, HIGHVERBOSITY));
  EXPECT_FALSE(logs(DEBUG, CHATTERBOX));
  EXPECT_TRUE(mx::logging::apply_verbosity_spec("DEBUG:CHATTERBOX"));
  EXPECT_TRUE(logs(DEBUG, CHATTERBOX));
  EXPECT_TRUE(logs(INFO, HIGHVERBOSITY)) << "the other levels untouched";
  EXPECT_FALSE(logs(INFO, CHATTERBOX));
  EXPECT_TRUE(mx::logging::apply_verbosity_spec(" debug : low , info:ZEROVERBOSITY"));
  EXPECT_TRUE(logs(DEBUG, LOWVERBOSITY));
  EXPECT_FALSE(logs(DEBUG, MEDIUMVERBOSITY));
  EXPECT_FALSE(logs(INFO, LOWVERBOSITY));
  EXPECT_TRUE(logs(INFO, ZEROVERBOSITY));
  EXPECT_TRUE(mx::logging::apply_verbosity_spec("MediumVerbosity"));
  EXPECT_TRUE(logs(INFO, MEDIUMVERBOSITY));
  EXPECT_FALSE(logs(WARNING, HIGHVERBOSITY));
}

TEST(VerbositySpec, ARefusedSpecChangesNothing) {
  ASSERT_TRUE(mx::logging::apply_verbosity_spec("DEBUG:HIGH,INFO:LOW"));
  std::string error;
  EXPECT_FALSE(mx::logging::apply_verbosity_spec("DEBUG:LOUD", &error));
  EXPECT_NE(std::string::npos, error.find("unknown verbosity"));
  EXPECT_FALSE(mx::logging::apply_verbosity_spec("TRACE:HIGH", &error));
  EXPECT_NE(std::string::npos, error.find("unknown level"));
  EXPECT_FALSE(mx::logging::apply_verbosity_spec("DEBUG:LOW,INFO:WHAT", &error)) << "the second item fails the whole";
  EXPECT_FALSE(mx::logging::apply_verbosity_spec("", &error));
  EXPECT_FALSE(mx::logging::apply_verbosity_spec(" , ", &error));
  EXPECT_TRUE(logs(DEBUG, HIGHVERBOSITY)) << "still as set before";
  EXPECT_FALSE(logs(DEBUG, CHATTERBOX));
  EXPECT_TRUE(logs(INFO, LOWVERBOSITY));
  EXPECT_FALSE(logs(INFO, MEDIUMVERBOSITY));
}
