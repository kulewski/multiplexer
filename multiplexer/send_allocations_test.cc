// What sending allocates, and where it is freed. A ThreadedClient's plain
// sends, events nobody follows while something is batched, go to its io
// thread as their bytes, and the bodies come back to be serialized into
// again (threaded_client_inbox.h): once they do, the sending thread
// allocates nothing per message, and no thread frees what another
// allocated, which glibc's malloc makes costly. A SyncClient's queue() of
// a plain event that joins a batch makes no frame for it, and its body is
// one the batching handed back: its tracker, nothing more. Counted by this binary's operator new, which
// tags every block with the thread that allocated it, so that a free on
// another thread is seen.
#include <gtest/gtest.h>

#include <asio/io_service.hpp>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <future>
#include <new>
#include <string>
#include <vector>

#include "multiplexer/client.h"
#include "multiplexer/in_process_multiplexer.h"
#include "multiplexer/multiplexer.constants.h"
#include "multiplexer/threaded_client.h"

namespace {

// Every block starts HEADER bytes before what operator new returns: the
// tag of the thread that allocated it, then its size; HEADER keeps the
// alignment malloc gives.
const std::size_t HEADER = 16;
std::atomic<std::uint32_t> next_tag{0};
thread_local std::uint32_t thread_tag = 0;
// Where this thread counts its allocations, when it does.
thread_local std::atomic<std::size_t>* counter = nullptr;
// Frees of a block another thread allocated, while `watching`; and the
// biggest block a thread freed while it counts (`counter`).
std::atomic<bool> watching{false};
std::atomic<std::size_t> foreign_frees{0};
thread_local std::size_t biggest_freed = 0;

std::uint32_t this_thread_tag() {
  if (!thread_tag) {
    thread_tag = ++next_tag;
  }
  return thread_tag;
}

void* tagged(std::size_t size) noexcept {
  if (counter) {
    counter->fetch_add(1, std::memory_order_relaxed);
  }
  char* block = static_cast<char*>(std::malloc(size + HEADER));
  if (!block) {
    return nullptr;
  }
  *reinterpret_cast<std::uint32_t*>(block) = this_thread_tag();
  *reinterpret_cast<std::size_t*>(block + 8) = size;
  return block + HEADER;
}

void untagged(void* memory) noexcept {
  if (!memory) {
    return;
  }
  char* block = static_cast<char*>(memory) - HEADER;
  if (watching.load(std::memory_order_relaxed) && *reinterpret_cast<std::uint32_t*>(block) != this_thread_tag()) {
    foreign_frees.fetch_add(1, std::memory_order_relaxed);
  }
  if (counter && *reinterpret_cast<std::size_t*>(block + 8) > biggest_freed) {
    biggest_freed = *reinterpret_cast<std::size_t*>(block + 8);
  }
  std::free(block);
}

}  // namespace

// Every form but the aligned ones, which keep their own pair; never
// inlined, as receive_allocations_test.cc explains (-Wmismatched-new-delete).
__attribute__((noinline)) void* operator new(std::size_t size) {
  if (void* memory = tagged(size)) {
    return memory;
  }
  throw std::bad_alloc();
}
__attribute__((noinline)) void* operator new[](std::size_t size) {
  if (void* memory = tagged(size)) {
    return memory;
  }
  throw std::bad_alloc();
}
__attribute__((noinline)) void* operator new(std::size_t size, const std::nothrow_t&) noexcept { return tagged(size); }
__attribute__((noinline)) void* operator new[](std::size_t size, const std::nothrow_t&) noexcept {
  return tagged(size);
}
__attribute__((noinline)) void operator delete(void* memory) noexcept { untagged(memory); }
__attribute__((noinline)) void operator delete[](void* memory) noexcept { untagged(memory); }
__attribute__((noinline)) void operator delete(void* memory, std::size_t) noexcept { untagged(memory); }
__attribute__((noinline)) void operator delete[](void* memory, std::size_t) noexcept { untagged(memory); }
__attribute__((noinline)) void operator delete(void* memory, const std::nothrow_t&) noexcept { untagged(memory); }
__attribute__((noinline)) void operator delete[](void* memory, const std::nothrow_t&) noexcept { untagged(memory); }

namespace {

using multiplexer::MultiplexerMessage;
using multiplexer::testing::InProcessMultiplexer;
namespace peers = multiplexer::peers;
namespace types = multiplexer::types;

const int RECORDS = 2000;  // a round of sends
const int BATCH = 100;     // the messages of a batch
const int PAYLOAD = 100;   // a message's payload, in bytes

// Batching by count alone: BATCH messages a batch, never by time.
multiplexer::BasicClient::Batching by_count() {
  multiplexer::BasicClient::Batching batching;
  batching.max_records = BATCH;
  batching.max_bytes = 16 * 1024 * 1024;
  batching.max_delay = 3600;
  return batching;
}

// A key longer than a string holds inline, so that one in a string of its
// own would be a block to free.
const char* const LONG_KEY = "a key longer than a string holds inline, 45 b";

// An event the library gives its id and sender, keyed with LONG_KEY when
// `keyed`.
MultiplexerMessage event(int index, bool keyed = false) {
  MultiplexerMessage msg;
  msg.set_type(types::TEST_EVENT);
  msg.set_message(std::string(PAYLOAD - 8, 'p') + std::to_string(10000000 + index));
  if (keyed) {
    msg.set_key(LONG_KEY);
  }
  return msg;
}

// A round's events, made before anything is counted.
std::vector<MultiplexerMessage> events(bool keyed = false) {
  std::vector<MultiplexerMessage> made;
  for (int index = 0; index < RECORDS; ++index) {
    made.push_back(event(index, keyed));
  }
  return made;
}

// The messages `receiver` reads, `expected` at most: each read waits 5
// seconds at most, and the first that finds none ends it.
int receive_all(multiplexer::Client* receiver, int expected) {
  int received = 0;
  while (received < expected && receiver->receive_message(5).first) {
    ++received;
  }
  return received;
}

TEST(SendAllocations, AThreadedClientsPlainSendsAllocateNothingAnotherThreadFrees) {
  InProcessMultiplexer mx;
  multiplexer::SyncClient receiver(peers::TEST_EVENT_BACKEND);
  ASSERT_TRUE(receiver.connect("127.0.0.1", mx.port, 5));
  multiplexer::ThreadedClient sender(peers::TEST_EVENT_CLIENT);
  ASSERT_TRUE(sender.connect("127.0.0.1", mx.port, 5));
  sender.set_batching(types::TEST_EVENT, by_count());
  const std::vector<MultiplexerMessage> round = events(/*keyed=*/true);  // a key goes in its message's body
  // A round first, whose bodies come back to be serialized into again.
  for (const MultiplexerMessage& msg : round) {
    sender.send(msg);
  }
  ASSERT_TRUE(sender.flush_all(5));
  // The io thread counts its own allocations from here.
  std::atomic<std::size_t> io_allocations{0};
  std::promise<void> counting;
  sender.flush_all_with_callback(5, [&](bool) {
    counter = &io_allocations;
    counting.set_value();
  });
  counting.get_future().wait();
  std::atomic<std::size_t> sender_allocations{0};
  watching = true;
  counter = &sender_allocations;
  for (const MultiplexerMessage& msg : round) {
    sender.send(msg);
  }
  counter = nullptr;
  ASSERT_TRUE(sender.flush_all(5));  // the io thread's part of the round done
  watching = false;
  // A body at most per message, and nothing else: a fresh one only for a
  // send that finds none handed back yet, which how far this round gets
  // ahead of the io thread decides; where a message cost five. That the
  // bodies come back, rather than being freed by the io thread, is the
  // count of foreign frees below.
  EXPECT_LE(sender_allocations.load(), static_cast<std::size_t>(RECORDS + 4));
  // A few from flush_all(), whose call the io thread runs and lets go of.
  EXPECT_LE(foreign_frees.load(), 8u);
  // Per batch, not per message: the batch, its frame, their lists, the
  // records' list as it grows, its write; a message's own would be RECORDS.
  EXPECT_LE(io_allocations.load(), static_cast<std::size_t>(RECORDS / BATCH * 40));
  sender.flush_all_with_callback(5, [](bool) { counter = nullptr; });
  EXPECT_EQ(2 * RECORDS, receive_all(&receiver, 2 * RECORDS));
}

// A backlog grows the inbox's room, which the io thread lets go once it is
// over: held for good, a backlog of 400,000 sends kept some 60 MB. The io
// thread is held in a callback while the sender hands it BACKLOG sends.
TEST(SendAllocations, TheRoomABacklogGrewGoesOnceItIsOver) {
  const int BACKLOG = 20000;
  InProcessMultiplexer mx;
  multiplexer::SyncClient receiver(peers::TEST_EVENT_BACKEND);
  ASSERT_TRUE(receiver.connect("127.0.0.1", mx.port, 5));
  multiplexer::ThreadedClient sender(peers::TEST_EVENT_CLIENT);
  ASSERT_TRUE(sender.connect("127.0.0.1", mx.port, 5));
  sender.set_batching(types::TEST_EVENT, by_count());
  std::promise<void> held;
  std::promise<void> release;
  std::shared_future<void> released = release.get_future().share();
  sender.flush_all_with_callback(5, [&held, released](bool) {
    held.set_value();
    released.wait();
  });
  held.get_future().wait();
  const MultiplexerMessage msg = event(0);
  for (int index = 0; index < BACKLOG; ++index) {
    sender.send(msg);
  }
  std::atomic<std::size_t> io_allocations{0};
  std::promise<void> counting;
  sender.flush_all_with_callback(5, [&](bool) {  // queued behind the backlog
    counter = &io_allocations;
    biggest_freed = 0;
    counting.set_value();
  });
  release.set_value();
  counting.get_future().wait();
  // Two turns at the inbox with little in it: the backlog's two lists of
  // room, which take turns, go at them.
  for (int turn = 0; turn < 2; ++turn) {
    sender.send(msg);
    ASSERT_TRUE(sender.flush_all(5));
  }
  std::promise<std::size_t> freed;
  sender.flush_all_with_callback(5, [&freed](bool) {
    freed.set_value(biggest_freed);
    counter = nullptr;
  });
  EXPECT_GE(freed.get_future().get(), static_cast<std::size_t>(BACKLOG) * sizeof(MultiplexerMessage*) * 8)
      << "the backlog's room, a list of BACKLOG entries";
  EXPECT_EQ(BACKLOG + 2, receive_all(&receiver, BACKLOG + 2));
}

TEST(SendAllocations, ASyncClientsQueuedEventInABatchCostsItsBodyAndItsTracker) {
  InProcessMultiplexer mx;
  asio::io_service io_service;
  multiplexer::Client receiver(io_service, peers::TEST_EVENT_BACKEND);
  ASSERT_TRUE(receiver.connect("127.0.0.1", mx.port, 5));
  multiplexer::SyncClient sender(peers::TEST_EVENT_CLIENT);
  ASSERT_TRUE(sender.connect("127.0.0.1", mx.port, 5));
  sender.set_batching(types::TEST_EVENT, by_count());
  const std::vector<MultiplexerMessage> round = events();
  for (const MultiplexerMessage& msg : round) {
    ASSERT_TRUE(sender.queue(msg));
  }
  ASSERT_TRUE(sender.flush_all(5));
  std::atomic<std::size_t> allocations{0};
  counter = &allocations;
  for (const MultiplexerMessage& msg : round) {
    sender.queue(msg);
  }
  counter = nullptr;
  ASSERT_TRUE(sender.flush_all(5));
  // Each its tracker, its body one handed back, and the batches' own
  // allocations spread over their messages, where a frame per message, its
  // marks, its body and a list of trackers made it more than three.
  EXPECT_LE(allocations.load(), static_cast<std::size_t>(RECORDS * 3 / 2));
  EXPECT_EQ(2 * RECORDS, receive_all(&receiver, 2 * RECORDS));
}

}  // namespace
