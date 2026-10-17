// A message costs no timer work: a client of one connection, its loop's
// earliest timer the heartbeat it is to send HEARTBIT_INTERVAL after its
// last write, sends message after message, each written before the next,
// and the timer is not moved for each, where it was cancelled and re-armed,
// asio's timerfd_settime(2) for every message; it waits, and moves itself
// on to the last write's moment when it comes due. Counted by this
// binary's own timerfd_settime, per thread, around the sends. The frames
// read move the drop's wait the same way, which the loop's handlers count.
#include <gtest/gtest.h>
#include <sys/syscall.h>
#include <sys/timerfd.h>
#include <unistd.h>

#include <asio/io_service.hpp>
#include <chrono>
#include <cstddef>
#include <string>

#include "multiplexer/client.h"
#include "multiplexer/in_process_multiplexer.h"
#include "multiplexer/multiplexer.constants.h"

namespace {

thread_local std::size_t settimes = 0;

}  // namespace

// Every timerfd_settime of the binary, asio's in the libraries included,
// counted on the thread that makes it, then made; a sanitizer's own
// interception of the call gives way to this one.
extern "C" int timerfd_settime(int fd, int flags, const struct itimerspec* value, struct itimerspec* old) noexcept {
  ++settimes;
  return static_cast<int>(::syscall(SYS_timerfd_settime, fd, flags, value, old));
}

namespace {

using multiplexer::testing::InProcessMultiplexer;

const int MESSAGES = 100;
const std::size_t FEW = 5;  // what may move the timer for reasons of its own: none per message

// What the timers may move themselves besides FEW over `elapsed`: the
// heartbeat's and the drop's, each when it comes due, about once a
// HEARTBIT_INTERVAL. The clock read as data, so that a run stretched by
// load or valgrind allows for them, where a fixed FEW failed it.
std::size_t due_over(std::chrono::steady_clock::duration elapsed) {
  return 2 * static_cast<std::size_t>(std::chrono::duration<double>(elapsed).count() / multiplexer::HEARTBIT_INTERVAL);
}

// An event nobody receives, said to nobody: the multiplexer drops it.
multiplexer::MultiplexerMessage event(std::uint64_t id, std::uint64_t sender) {
  multiplexer::MultiplexerMessage msg;
  msg.set_id(id);
  msg.set_sender(sender);
  msg.set_type(multiplexer::types::TEST_UNROUTED);
  msg.set_report_delivery_error(false);
  msg.set_message("event");
  return msg;
}

TEST(TimerSyscalls, ASyncClientsMessagesMoveNoTimer) {
  InProcessMultiplexer mx;
  multiplexer::Client client(multiplexer::peers::TEST_EVENT_CLIENT);
  ASSERT_TRUE(client.connect("127.0.0.1", mx.port, 5));
  const std::chrono::steady_clock::time_point started = std::chrono::steady_clock::now();
  const std::size_t before = settimes;
  for (int index = 0; index < MESSAGES; ++index) {
    // Written before the next: the queue empties at every message.
    client.flush(client.schedule_one(event(client.random64(), client.instance_id())), 30);
  }
  const std::size_t moved = settimes - before;
  EXPECT_LT(moved, FEW + due_over(std::chrono::steady_clock::now() - started)) << "the timer moved for messages";
}

// Frames read move no timer either: each frame read runs the handlers of
// its reads and nothing else, where the drop's wait was re-armed at each,
// and every re-arm cancelled the wait before it, whose handler the loop
// then ran too, twice the handlers per frame.
// Counted by the receiver's loop, run here: its handlers, until every
// event is queued, each taken from the queue with no wait.
TEST(TimerSyscalls, AFrameReadRunsItsReadsAlone) {
  InProcessMultiplexer mx;
  asio::io_service io_service;
  multiplexer::Client receiver(io_service, multiplexer::peers::TEST_EVENT_BACKEND);
  ASSERT_TRUE(receiver.connect("127.0.0.1", mx.port, 5));
  multiplexer::Client sender(multiplexer::peers::TEST_EVENT_CLIENT);
  ASSERT_TRUE(sender.connect("127.0.0.1", mx.port, 5));
  while (io_service.poll() > 0) {
  }
  for (int index = 0; index < MESSAGES; ++index) {
    multiplexer::MultiplexerMessage msg = event(sender.random64(), sender.instance_id());
    msg.set_type(multiplexer::types::TEST_EVENT);  // routed to every event backend there is
    sender.flush(sender.schedule_one(msg), 30);
  }
  std::size_t handlers = 0;
  int received = 0;
  for (int pass = 0; pass < 3000 && received < MESSAGES; ++pass) {
    handlers += io_service.run_one_for(std::chrono::milliseconds(10));
    while (receiver.has_incoming_messages()) {
      receiver.receive_message(0);  // queued: taken with no wait, and so no handler
      ++received;
    }
  }
  ASSERT_EQ(MESSAGES, received);
  EXPECT_LT(handlers, std::size_t(3 * MESSAGES)) << "the loop ran more than the reads per frame";
}

}  // namespace
