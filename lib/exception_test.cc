// mx::Exception made without an explanation: the same defaults as the
// explaining constructor's, where its line was whatever the memory held
// and its file and function were empty; MXTHROW sets the throw site on it;
// and ABORT_ON_EXCEPTION aborts at an explained one only (exception.h
// says why).
#include "lib/exception.h"

#include <gtest/gtest.h>

#include <cstdlib>
#include <string>

namespace {

TEST(Exception, WithoutAnExplanationTheThrowSiteIsUnknown) {
  const mx::NotImplementedError error;
  EXPECT_EQ("", std::string(error.what()));
  EXPECT_EQ("<unknown file>", error.file());
  EXPECT_EQ("<unknown function>", error.function());
  EXPECT_EQ(0, error.line());
}

TEST(Exception, MxthrowSetsTheThrowSite) {
  try {
    MXTHROW(mx::NotImplementedError());
  } catch (const mx::Exception& error) {
    EXPECT_NE(std::string::npos, error.file().find("lib/exception_test.cc")) << error.file();
    EXPECT_GT(error.line(), 0);
    EXPECT_NE("<unknown function>", error.function());
  }
}

TEST(Exception, AbortOnExceptionAbortsAtAnExplainedOneOnly) {
  EXPECT_EXIT(
      {
        mx::Exception::abort_on_exception(true);
        const mx::NotImplementedError unexplained;
        std::exit(0);
      },
      ::testing::ExitedWithCode(0), "");
  EXPECT_DEATH(
      {
        mx::Exception::abort_on_exception(true);
        const mx::Exception explained("why");
      },
      "");
}

}  // namespace
