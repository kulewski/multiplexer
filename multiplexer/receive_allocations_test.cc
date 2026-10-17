// What receiving allocates. A receive or a read that finds a message
// queued takes it without making a timer: none is allocated, armed,
// cancelled and freed, where every such call made one first, three
// allocations and, in a busy serve loop, a timerfd_settime per message,
// only to find the message there. And a message taken from a batch costs
// its own allocations alone, the message and its payload, where it took a
// string of its own and a raw frame too; after a batch over
// BasicClient::KEPT_SPLIT_BYTES the strings kept for the next go. Counted
// by this binary's operator new, on the test's thread, around the one call
// or the loop that reads a batch.
#include <gtest/gtest.h>

#include <asio/io_service.hpp>
#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <new>
#include <string>

#include "multiplexer/client.h"
#include "multiplexer/in_process_multiplexer.h"
#include "multiplexer/multiplexer.constants.h"

namespace {

thread_local bool counting = false;
thread_local std::size_t allocations = 0;

}  // namespace

// Every allocation of the binary goes through these, every form but the
// aligned ones, which keep their own pair, so that what one form allocates
// any other may free, as a sanitizer checks; only the allocations of the
// thread that counts, while it counts, are counted. Never inlined, any of
// them: GCC at -O2, inlining a new or a delete into its caller, sees
// malloc() or free() behind it and calls the pair a mismatch
// (-Wmismatched-new-delete), not knowing that every form here is malloc's.
namespace {
void* counted(std::size_t size) noexcept {
  if (counting) {
    ++allocations;
  }
  return std::malloc(size ? size : 1);
}
}  // namespace
__attribute__((noinline)) void* operator new(std::size_t size) {
  if (void* memory = counted(size)) {
    return memory;
  }
  throw std::bad_alloc();
}
__attribute__((noinline)) void* operator new[](std::size_t size) {
  if (void* memory = counted(size)) {
    return memory;
  }
  throw std::bad_alloc();
}
__attribute__((noinline)) void* operator new(std::size_t size, const std::nothrow_t&) noexcept { return counted(size); }
__attribute__((noinline)) void* operator new[](std::size_t size, const std::nothrow_t&) noexcept {
  return counted(size);
}
__attribute__((noinline)) void operator delete(void* memory) noexcept { std::free(memory); }
__attribute__((noinline)) void operator delete[](void* memory) noexcept { std::free(memory); }
__attribute__((noinline)) void operator delete(void* memory, std::size_t) noexcept { std::free(memory); }
__attribute__((noinline)) void operator delete[](void* memory, std::size_t) noexcept { std::free(memory); }
__attribute__((noinline)) void operator delete(void* memory, const std::nothrow_t&) noexcept { std::free(memory); }
__attribute__((noinline)) void operator delete[](void* memory, const std::nothrow_t&) noexcept { std::free(memory); }

namespace {

using multiplexer::testing::InProcessMultiplexer;

const int EVENTS = 3;

// The allocations `call` makes on this thread.
template <typename Call>
std::size_t allocations_of(Call call) {
  allocations = 0;
  counting = true;
  call();
  counting = false;
  return allocations;
}

// A receiver with a message queued: EVENTS events sent to it, and its loop
// run until the first of them is read off its socket and queued, none
// taken, as a serve loop finds one waiting after another's turn.
struct Queued {
  Queued()
      : receiver(io_service, multiplexer::peers::TEST_EVENT_BACKEND), sender(multiplexer::peers::TEST_EVENT_CLIENT) {
    EXPECT_TRUE(receiver.connect("127.0.0.1", mx.port, 5));
    EXPECT_TRUE(sender.connect("127.0.0.1", mx.port, 5));
    for (int index = 0; index < EVENTS; ++index) {
      multiplexer::MultiplexerMessage event;
      event.set_id(sender.random64());
      event.set_sender(sender.instance_id());
      event.set_type(multiplexer::types::TEST_EVENT);
      event.set_message(std::to_string(index));
      sender.flush(sender.schedule_one(event));
    }
    for (int pass = 0; pass < 3000 && !receiver.has_incoming_messages(); ++pass) {
      io_service.run_one_for(std::chrono::milliseconds(10));
    }
    EXPECT_TRUE(receiver.has_incoming_messages());
  }
  InProcessMultiplexer mx;
  asio::io_service io_service;
  multiplexer::Client receiver;
  multiplexer::Client sender;
};

const int BATCH = 100;  // the messages of a smaller batch; a larger has twice as many

// A receiver on this thread's loop, and a sender that batches TEST_EVENT by
// count, up to 2 * BATCH messages a batch, a batch closed by a flush.
struct Batches {
  Batches()
      : receiver(io_service, multiplexer::peers::TEST_EVENT_BACKEND), sender(multiplexer::peers::TEST_EVENT_CLIENT) {
    EXPECT_TRUE(receiver.connect("127.0.0.1", mx.port, 5));
    EXPECT_TRUE(sender.connect("127.0.0.1", mx.port, 5));
    multiplexer::SyncClient::Delivery batching;
    batching.batch = true;
    batching.max_records = 2 * BATCH;
    batching.max_bytes = 2 * multiplexer::BasicClient::KEPT_SPLIT_BYTES;
    batching.max_delay = 3600;  // never, here
    batching.timeout = -1;
    sender.set_delivery(multiplexer::types::TEST_EVENT, batching);
  }
  // The allocations of the loop that reads a batch of `count` events of
  // `size` bytes, but the one at `big_at`, of `big_size`, sent before; its
  // messages taken after.
  std::size_t receive(int count, std::size_t size = 100, int big_at = -1, std::size_t big_size = 0) {
    for (int index = 0; index < count; ++index) {
      multiplexer::MultiplexerMessage event;
      event.set_id(sender.random64());
      event.set_sender(sender.instance_id());
      event.set_type(multiplexer::types::TEST_EVENT);
      event.set_message(std::string(index == big_at ? big_size : size, 'b'));
      EXPECT_TRUE(sender.queue(event));
    }
    EXPECT_TRUE(sender.flush_all(5));
    const std::size_t counted = allocations_of([this] {
      for (int pass = 0; pass < 3000 && !receiver.has_incoming_messages(); ++pass) {
        io_service.run_one_for(std::chrono::milliseconds(10));
      }
    });
    for (int index = 0; index < count; ++index) {
      receiver.receive_message(5);
    }
    return counted;
  }
  InProcessMultiplexer mx;
  asio::io_service io_service;
  multiplexer::Client receiver;
  multiplexer::Client sender;
};

TEST(ReceiveAllocations, AReceiveTakesAQueuedMessageWithNoTimer) {
  Queued queued;
  EXPECT_EQ(0u, allocations_of([&queued] { queued.receiver.receive_message(30); }));
}

TEST(ReceiveAllocations, AServeLoopsReceiveTakesAQueuedMessageWithNoTimer) {
  Queued queued;
  EXPECT_EQ(0u, allocations_of([&queued] { queued.receiver.receive_message(30, /*end_on_acknowledgement=*/true); }));
}

TEST(ReceiveAllocations, AReadTakesAQueuedMessageWithNoTimer) {
  Queued queued;
  EXPECT_EQ(0u, allocations_of([&queued] { queued.receiver.read_raw_message(30); }));
}

// What a larger batch allocates more than a smaller is its messages' share,
// the kept strings grown before by a batch as large: three a message, the
// message and its payload, a string protobuf allocates and the string's
// bytes, and about a third more, the queue's chunks, of four messages, and
// the ids' table; where a message took six and a third, a string of its
// own and its bytes and a raw frame too.
TEST(ReceiveAllocations, AMessageFromABatchCostsItsMessageAndItsPayload) {
  Batches batches;
  batches.receive(2 * BATCH);
  const std::size_t smaller = batches.receive(BATCH);
  const std::size_t larger = batches.receive(2 * BATCH);
  ASSERT_GE(larger, smaller);
  EXPECT_GE(larger - smaller, 3u * BATCH) << "the message, its payload's string and the string's bytes";
  EXPECT_LE(larger - smaller, 4u * BATCH) << "nothing more but a share of the queue's chunks and of the ids' table";
}

TEST(ReceiveAllocations, AHugeBatchLetsTheKeptStringsGo) {
  Batches batches;
  batches.receive(BATCH);
  const std::size_t kept = batches.receive(BATCH);  // read into the strings the batch before left
  batches.receive(BATCH, multiplexer::BasicClient::KEPT_SPLIT_BYTES / BATCH + 1024);
  EXPECT_GE(batches.receive(BATCH), kept + BATCH) << "a string again for each message";
}

// Batches each well under KEPT_SPLIT_BYTES, a big message at another place
// in each, leave the kept strings holding more than that together, each
// place the room of the biggest message it held: once they do, the strings
// go, as after one huge batch.
TEST(ReceiveAllocations, BigMessagesAtNewPlacesLetTheKeptStringsGo) {
  Batches batches;
  batches.receive(BATCH);
  const std::size_t kept = batches.receive(BATCH);
  const std::size_t big = multiplexer::BasicClient::KEPT_SPLIT_BYTES / 3;
  for (int place = 0; place < 3; ++place) {
    batches.receive(BATCH, 100, place, big);
  }
  EXPECT_GE(batches.receive(BATCH), kept + BATCH) << "a string again for each message";
}

}  // namespace
