// Unit tests for lib/kwargs.h (googletest): bazel test //lib:kwargs_test.
#include "lib/kwargs.h"

#include <gtest/gtest.h>
#include <string>

using mx::util::kwargs::KeyError;
using mx::util::kwargs::Kwargs;
using mx::util::kwargs::KwargsKeys;

TEST(Kwargs, SetAndGetKeepTheStoredType) {
  Kwargs kw;
  kw.set("type", 7u).set("name", std::string("x"));
  EXPECT_EQ(7u, kw.get<unsigned int>("type"));
  EXPECT_EQ("x", kw.get<std::string>("name"));
}

TEST(Kwargs, MissingKeyThrowsUnlessDefaultGiven) {
  Kwargs kw;
  EXPECT_THROW(kw.get<int>("missing"), KeyError);
  EXPECT_EQ(5, kw.get<int>("missing", 5));
  EXPECT_FALSE(kw.has_key("missing"));
}

TEST(Kwargs, SetDefaultDoesNotOverwrite) {
  Kwargs kw;
  kw.set("a", 1);
  kw.set_default("a", 2);
  kw.set_default("b", 3);
  EXPECT_EQ(1, kw.get<int>("a"));
  EXPECT_EQ(3, kw.get<int>("b"));
}

TEST(Kwargs, CheckKeysAcceptsOnlyListedKeys) {
  Kwargs kw;
  kw.set("a", 1).set("b", 2);
  EXPECT_TRUE(kw.check_keys(KwargsKeys("a")("b")("c")));
  EXPECT_FALSE(kw.check_keys(KwargsKeys("a")));
}

TEST(Kwargs, CopiesAreIndependent) {
  Kwargs original;
  original.set("a", 1);
  Kwargs copy = original;
  copy.set_default("b", 2).set("a", 3);
  EXPECT_EQ(1, original.get<int>("a"));
  EXPECT_FALSE(original.has_key("b"));
  EXPECT_EQ(3, copy.get<int>("a"));
}

TEST(Kwargs, TypeQueries) {
  Kwargs kw;
  kw.set("n", 1);
  EXPECT_TRUE(kw.unsafe_is<int>("n"));
  EXPECT_TRUE(kw.empty_or<int>("n"));
  EXPECT_TRUE(kw.empty_or<int>("absent"));
  EXPECT_FALSE(kw.empty_or<std::string>("n"));
}
