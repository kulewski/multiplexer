// SpentBodies: a body is kept while it is small enough and there is room,
// taken back as the same room, and what keep_all() cannot keep stays with
// its owner, to be freed outside the lock the call was made under.
#include "multiplexer/spent_bodies.h"

#include <gtest/gtest.h>

#include <string>

namespace {

using multiplexer::SpentBodies;

TEST(SpentBodies, KeepsABodyAndGivesTheSameRoomBack) {
  SpentBodies spent;
  std::string body(500, 'b');
  const char* room = body.data();
  ASSERT_TRUE(spent.keep(&body));
  EXPECT_TRUE(body.empty());
  EXPECT_EQ(1u, spent.count());
  std::string next;
  ASSERT_TRUE(spent.take(&next));
  EXPECT_EQ(room, next.data());
  EXPECT_EQ(0u, spent.count());
  EXPECT_EQ(0u, spent.bytes());
  EXPECT_FALSE(spent.take(&next));
}

TEST(SpentBodies, ABodyWithNoRoomOfItsOwnIsNotKept) {
  SpentBodies spent;
  std::string inline_only("short");
  EXPECT_FALSE(spent.keep(&inline_only));
  std::string emptied(500, 'b');
  std::string elsewhere = std::move(emptied);
  emptied.clear();
  EXPECT_FALSE(spent.keep(&emptied));
  EXPECT_EQ(0u, spent.count());
}

TEST(SpentBodies, ABodyTooBigIsNotKept) {
  SpentBodies spent;
  std::string body(SpentBodies::BODY_BYTES_KEPT + 1, 'b');
  EXPECT_FALSE(spent.keep(&body));
  EXPECT_EQ(SpentBodies::BODY_BYTES_KEPT + 1, body.size());  // left for the caller
  EXPECT_EQ(0u, spent.count());
}

TEST(SpentBodies, KeepsNoMoreThanBytesKeptTogether) {
  SpentBodies spent;
  std::string room;
  room.reserve(SpentBodies::BODY_BYTES_KEPT / 2);  // within the limit, whatever the library rounds to
  const std::size_t each = room.capacity();
  const std::size_t held = SpentBodies::BYTES_KEPT / each;
  for (std::size_t index = 0; index < held; ++index) {
    std::string body;
    body.reserve(each);
    ASSERT_TRUE(spent.keep(&body));
  }
  std::string one_more;
  one_more.reserve(each);
  EXPECT_FALSE(spent.keep(&one_more));
  EXPECT_EQ(held, spent.count());
  EXPECT_EQ(held * each, spent.bytes());
}

TEST(SpentBodies, WhatKeepAllCannotKeepStaysWithItsOwner) {
  SpentBodies kept;
  SpentBodies other;
  std::string room;
  room.reserve(SpentBodies::BODY_BYTES_KEPT / 2);  // within the limit, whatever the library rounds to
  const std::size_t each = room.capacity();
  const std::size_t held = SpentBodies::BYTES_KEPT / each;
  for (std::size_t index = 0; index < held + 2; ++index) {
    std::string body;
    body.reserve(each);
    if (index < held) {
      ASSERT_TRUE(kept.keep(&body));
    } else {
      ASSERT_TRUE(other.keep(&body));
    }
  }
  kept.keep_all(&other);
  EXPECT_EQ(held, kept.count());
  EXPECT_EQ(2 * each, other.bytes());
  other.clear();
  EXPECT_EQ(0u, other.bytes());
  EXPECT_EQ(0u, other.count());
}

}  // namespace
