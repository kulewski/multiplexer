// A receive or a read that finds a message queued takes it without making a
// timer: none is allocated, armed, cancelled and freed, where every such
// call made one first, three allocations and, in a busy serve loop, a
// timerfd_settime per message, only to find the message there. Counted by
// this binary's operator new, on the test's thread, around the one call.
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
// thread that counts, while it counts, are counted.
namespace {
void* counted(std::size_t size) noexcept {
  if (counting) {
    ++allocations;
  }
  return std::malloc(size ? size : 1);
}
}  // namespace
void* operator new(std::size_t size) {
  if (void* memory = counted(size)) {
    return memory;
  }
  throw std::bad_alloc();
}
void* operator new[](std::size_t size) {
  if (void* memory = counted(size)) {
    return memory;
  }
  throw std::bad_alloc();
}
void* operator new(std::size_t size, const std::nothrow_t&) noexcept { return counted(size); }
void* operator new[](std::size_t size, const std::nothrow_t&) noexcept { return counted(size); }
void operator delete(void* memory) noexcept { std::free(memory); }
void operator delete[](void* memory) noexcept { std::free(memory); }
void operator delete(void* memory, std::size_t) noexcept { std::free(memory); }
void operator delete[](void* memory, std::size_t) noexcept { std::free(memory); }
void operator delete(void* memory, const std::nothrow_t&) noexcept { std::free(memory); }
void operator delete[](void* memory, const std::nothrow_t&) noexcept { std::free(memory); }

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

}  // namespace
