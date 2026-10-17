// mx::resize_uninitialized: a string grown keeps what it held and, where the
// standard library allows it, leaves the new bytes as the memory held them,
// which resize() would have zeroed; shrunk, it is cut. Both standard
// libraries this repository builds with, libstdc++ from GCC 14 and libc++,
// allow it.
#include "lib/resize_uninitialized.h"

#include <gtest/gtest.h>

#include <string>

namespace {

TEST(ResizeUninitialized, GrowsWithoutFillingWhereTheLibraryAllows) {
  std::string body(1000, 'x');
  body.resize(2);  // the buffer keeps its room, and its 'x' beyond the terminator
  mx::resize_uninitialized(&body, 1000);
  ASSERT_EQ(1000u, body.size());
  EXPECT_EQ("xx", body.substr(0, 2)) << "what it held, kept";
  EXPECT_EQ(mx::RESIZE_SKIPS_FILL ? 'x' : '\0', body[999]) << "the new bytes left as the memory held them";
  mx::resize_uninitialized(&body, 3000);
  ASSERT_EQ(3000u, body.size());
  EXPECT_EQ("xx", body.substr(0, 2)) << "kept when the buffer grows";
}

TEST(ResizeUninitialized, ShrinksAsResizeDoes) {
  std::string body("abcdef");
  mx::resize_uninitialized(&body, 3);
  EXPECT_EQ("abc", body);
  mx::resize_uninitialized(&body, 0);
  EXPECT_TRUE(body.empty());
}

TEST(ResizeUninitialized, ThisRepositorysLibrariesSkipTheFill) {
#if defined(_LIBCPP_VERSION) || (defined(_GLIBCXX_RELEASE) && _GLIBCXX_RELEASE >= 14)
  EXPECT_TRUE(mx::RESIZE_SKIPS_FILL);
#else
  GTEST_SKIP() << "a standard library this repository does not build with";
#endif
}

}  // namespace
