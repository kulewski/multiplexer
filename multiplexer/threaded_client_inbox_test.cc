// ThreadedClientInbox on its own: only the hand-over that finds no wake-up
// pending asks for one, what was handed over comes out in its order, calls
// and plain sends alike, confirmations beside them, and a plain send's body
// is swapped out for one handed back. SpentBodies' own limits are spent_bodies_test.cc's.
#include "multiplexer/threaded_client_inbox.h"

#include <gtest/gtest.h>

#include <string>
#include <vector>

namespace {

using multiplexer::HandedConfirmation;
using multiplexer::PlainSend;
using multiplexer::SpentBodies;
using multiplexer::ThreadedClientInbox;

// A plain send of `type` whose body, `body`, is swapped into the inbox.
bool push_plain(ThreadedClientInbox* inbox, std::uint32_t type, std::string* body) {
  PlainSend plain;
  plain.type = type;
  plain.id = type;
  return inbox->push_plain(&plain, body);
}

TEST(ThreadedClientInbox, OnlyTheFirstHandOverAfterATakeWakesTheIoThread) {
  ThreadedClientInbox inbox;
  std::string body = "a";
  EXPECT_TRUE(inbox.push([] {}));
  EXPECT_FALSE(inbox.push([] {}));
  EXPECT_FALSE(push_plain(&inbox, 7, &body));
  std::vector<ThreadedClientInbox::Entry> taken;
  std::vector<HandedConfirmation> confirmations;
  SpentBodies spent;
  inbox.take(&taken, &confirmations, &spent);
  EXPECT_EQ(3u, taken.size());
  taken.clear();
  body = "b";
  EXPECT_TRUE(push_plain(&inbox, 8, &body));
  EXPECT_FALSE(inbox.push([] {}));
}

TEST(ThreadedClientInbox, WhatWasHandedOverComesOutInItsOrder) {
  ThreadedClientInbox inbox;
  std::vector<int> ran;
  std::string body;
  inbox.push([&ran] { ran.push_back(1); });
  body = "second";
  push_plain(&inbox, 2, &body);
  inbox.push([&ran] { ran.push_back(3); });
  body = "fourth";
  push_plain(&inbox, 4, &body);
  std::vector<ThreadedClientInbox::Entry> taken;
  std::vector<HandedConfirmation> confirmations;
  SpentBodies spent;
  inbox.take(&taken, &confirmations, &spent);
  ASSERT_EQ(4u, taken.size());
  taken[0].call();
  EXPECT_FALSE(taken[1].call);
  EXPECT_EQ(2u, taken[1].plain.type);
  EXPECT_EQ("second", taken[1].plain.body);
  taken[2].call();
  EXPECT_EQ(4u, taken[3].plain.type);
  EXPECT_EQ("fourth", taken[3].plain.body);
  EXPECT_EQ((std::vector<int>{1, 3}), ran);
}

TEST(ThreadedClientInbox, ConfirmationsComeOutBesideTheCallsAndWakeAsTheyDo) {
  ThreadedClientInbox inbox;
  EXPECT_TRUE(inbox.push_confirmation(HandedConfirmation{7, 1, 0}));
  EXPECT_FALSE(inbox.push([] {}));
  EXPECT_FALSE(inbox.push_confirmation(HandedConfirmation{7, 2, 2}));
  std::vector<ThreadedClientInbox::Entry> taken;
  std::vector<HandedConfirmation> confirmations;
  SpentBodies spent;
  inbox.take(&taken, &confirmations, &spent);
  EXPECT_EQ(1u, taken.size());
  ASSERT_EQ(2u, confirmations.size());
  EXPECT_EQ(1u, confirmations[0].number);
  EXPECT_EQ(2u, confirmations[1].number);
  EXPECT_EQ(2u, confirmations[1].outcome);
  confirmations.clear();
  EXPECT_TRUE(inbox.push_confirmation(HandedConfirmation{7, 3, 0})) << "a take lets the next hand-over wake";
}

TEST(ThreadedClientInbox, APlainSendTakesABodyHandedBack) {
  ThreadedClientInbox inbox;
  std::vector<ThreadedClientInbox::Entry> taken;
  std::vector<HandedConfirmation> confirmations;
  SpentBodies spent;
  std::string spent_body(1000, 's');
  const char* handed_back = spent_body.data();
  ASSERT_TRUE(spent.keep(&spent_body));
  inbox.take(&taken, &confirmations, &spent);
  EXPECT_EQ(1u, inbox.returned_count());
  std::string body(200, 'm');
  push_plain(&inbox, 5, &body);
  EXPECT_EQ(handed_back, body.data());  // the same room, not a copy
  EXPECT_EQ(0u, inbox.returned_count());
  EXPECT_EQ(0u, inbox.returned_bytes());
  inbox.take(&taken, &confirmations, &spent);
  ASSERT_EQ(1u, taken.size());
  EXPECT_EQ(std::string(200, 'm'), taken[0].plain.body);
}

TEST(ThreadedClientInbox, BodiesPastTheLimitAreLeftForTheIoThreadToFree) {
  ThreadedClientInbox inbox;
  std::vector<ThreadedClientInbox::Entry> taken;
  std::vector<HandedConfirmation> confirmations;
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
  inbox.take(&taken, &confirmations, &spent);
  spent.clear();  // as the io thread does after each take
  EXPECT_EQ(held, inbox.returned_count());
  for (std::size_t index = 0; index < 3; ++index) {
    std::string body;
    body.reserve(each);
    ASSERT_TRUE(spent.keep(&body));
  }
  inbox.take(&taken, &confirmations, &spent);
  EXPECT_EQ(held, inbox.returned_count());
  EXPECT_EQ(3 * each, spent.bytes());  // left for the caller to free
  spent.clear();
  EXPECT_EQ(0u, spent.count());
}

// The wake-up handler's block serves one handler at a time and is taken
// again once given back; a second at once, or a bigger one, takes the heap.
TEST(WakeMemory, OneBlockServesOneHandlerAtATime) {
  multiplexer::WakeMemory memory;
  void* first = memory.allocate(64);
  void* second = memory.allocate(64);
  EXPECT_NE(first, second);
  memory.deallocate(second);
  memory.deallocate(first);
  EXPECT_EQ(first, memory.allocate(64)) << "the block, given back, taken again";
  void* big = memory.allocate(multiplexer::WakeMemory::BYTES + 1);
  EXPECT_NE(first, big);
  memory.deallocate(big);
  memory.deallocate(first);
}

}  // namespace
