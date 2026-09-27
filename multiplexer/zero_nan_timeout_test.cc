// A timeout of 0 or NaN is no time at all, "don't wait", in both C++
// clients. A message sent or scheduled with one goes now on a connection
// with room, each copy for ALL likewise, or is dropped and reported at
// once, NO_ROOM or NO_CONNECTION: it waited DEFAULT_TIMEOUT in the
// SyncClient and a millisecond in ThreadedClient. A connect given NaN
// starts the attempt and returns, where an assertion threw. And a
// SyncClient's flush of a message held when nothing is connected or on its
// way returns at once, where with no deadline its loop had no work and an
// assertion threw. Counted, not timed: the drops are told and counted
// inside the call, or by a round trip through the io thread, and a
// multiplexer frozen with the client's connection full keeps that
// connection without room.
#include <gtest/gtest.h>

#include <cmath>
#include <cstdint>
#include <fstream>
#include <future>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "multiplexer/client.h"
#include "multiplexer/in_process_multiplexer.h"
#include "multiplexer/multiplexer.constants.h"
#include "multiplexer/threaded_client.h"

using multiplexer::DropReason;
using multiplexer::MultiplexerMessage;
using multiplexer::ThreadedClient;
using multiplexer::testing::InProcessMultiplexer;

namespace {

// Holds a multiplexer's io thread, so that it reads nothing while the
// sockets stay open: a multiplexer frozen, until the end of the scope.
struct Freeze {
  explicit Freeze(InProcessMultiplexer& mx) {
    std::shared_future<void> gate = released.get_future().share();
    std::promise<void> frozen;
    mx.io_service.post([gate, &frozen] {
      frozen.set_value();
      gate.wait();
    });
    frozen.get_future().wait();
  }
  ~Freeze() { released.set_value(); }
  std::promise<void> released;
};

// What a client's drop observer was told, in order: the message id and why.
struct Drops {
  // The observer that keeps what it is told; on the client's loop thread.
  multiplexer::BasicClient::DropObserver observer() {
    return [this](std::uint64_t id, DropReason reason) {
      std::lock_guard<std::mutex> lock(mutex);
      told.emplace_back(id, reason);
    };
  }
  // A copy of what was told so far.
  std::vector<std::pair<std::uint64_t, DropReason>> seen() {
    std::lock_guard<std::mutex> lock(mutex);
    return told;
  }
  std::mutex mutex;
  std::vector<std::pair<std::uint64_t, DropReason>> told;
};

// A message nobody takes, asking for no delivery error, with its id and
// sender: what fills a connection.
MultiplexerMessage unrouted(multiplexer::Client& client, const std::string& payload) {
  MultiplexerMessage msg;
  msg.set_id(client.random64());
  msg.set_from(client.instance_id());
  msg.set_type(multiplexer::types::TEST_UNROUTED);
  msg.set_report_delivery_error(false);
  msg.set_message(payload);
  return msg;
}

// How many messages of `size` bytes a frozen multiplexer's connection
// cannot take: twice what the two sockets may buffer, the largest the
// kernel allows each, and twice the queue. Past that, messages wait.
int frames_to_fill(std::size_t size) {
  std::size_t buffers = 0;
  for (const char* path : {"/proc/sys/net/ipv4/tcp_wmem", "/proc/sys/net/ipv4/tcp_rmem"}) {
    std::ifstream limits(path);
    std::size_t least = 0, initial = 0, largest = 8 << 20;  // a guess where /proc does not say
    limits >> least >> initial >> largest;
    buffers += largest;
  }
  return static_cast<int>(2 * buffers / size) + 2 * 1024;
}

}  // namespace

// With no connection live, a message sent with 0 or NaN is dropped and
// told at once, NO_CONNECTION, an ALL send once, its tracker reading
// lost, and `done` hears 0 in the next call that runs the loop: it was held
// for DEFAULT_TIMEOUT.
TEST(ZeroOrNanTimeout, ASyncSendWithNoConnectionIsDroppedAtOnce) {
  Drops drops;  // before the client, whose observer writes into it
  std::vector<unsigned int> heard;
  multiplexer::Client client(multiplexer::peers::WEBSITE);  // never connected
  client.set_drop_observer(drops.observer());
  const MultiplexerMessage zero = unrouted(client, "zero");
  multiplexer::Client::ScheduledMessageTracker tracker =
      client.queue(zero, 0, multiplexer::LanePtr(), [&heard](unsigned int written) { heard.push_back(written); });
  ASSERT_TRUE(tracker);
  EXPECT_TRUE(tracker.is_lost()) << "held for a connection";
  const MultiplexerMessage nan = unrouted(client, "nan");
  client.queue_all(nan, std::nanf(""));
  EXPECT_EQ(2u, client.dropped());
  EXPECT_EQ((std::vector<std::pair<std::uint64_t, DropReason>>{{zero.id(), DropReason::NO_CONNECTION},
                                                               {nan.id(), DropReason::NO_CONNECTION}}),
            drops.seen());
  EXPECT_THROW(client.receive_message(0), multiplexer::Client::OperationTimedOut);  // runs the loop once
  EXPECT_EQ(std::vector<unsigned int>{0}, heard);
}

// With the connection full, its multiplexer frozen, a message sent or
// scheduled with 0 or NaN is dropped and told at once, NO_ROOM, where it
// waited for room DEFAULT_TIMEOUT; schedule_all() still counts the
// connection it gave a copy to.
TEST(ZeroOrNanTimeout, ASyncSendToAFullConnectionIsDroppedAtOnce) {
  InProcessMultiplexer mx;
  Drops drops;
  multiplexer::Client client(multiplexer::peers::WEBSITE);
  ASSERT_TRUE(client.wait_for_connection(client.connect("127.0.0.1", mx.port, 5), 5));
  client.set_drop_observer(drops.observer());
  const std::string chunk(16 * 1024, 'x');
  {
    Freeze frozen(mx);
    for (int index = 0; index < frames_to_fill(chunk.size()); ++index) {
      client.schedule_one(unrouted(client, chunk));  // DEFAULT_TIMEOUT: past the queue, these wait for room
    }
    ASSERT_EQ(0u, client.dropped());
    const MultiplexerMessage zero = unrouted(client, "zero");
    multiplexer::Client::ScheduledMessageTracker queued = client.queue(zero, 0);
    ASSERT_TRUE(queued);
    EXPECT_TRUE(queued.is_lost()) << "waiting for room";
    const MultiplexerMessage nan = unrouted(client, "nan");
    multiplexer::Client::ScheduledMessageTracker scheduled = client.schedule_one(nan, std::nanf(""));
    ASSERT_TRUE(scheduled);
    EXPECT_TRUE(scheduled.is_lost()) << "waiting for room";
    const MultiplexerMessage all = unrouted(client, "all");
    EXPECT_EQ(1u, client.schedule_all(all, 0)) << "the connection given a copy";
    EXPECT_EQ(3u, client.dropped());
    EXPECT_EQ((std::vector<std::pair<std::uint64_t, DropReason>>{
                  {zero.id(), DropReason::NO_ROOM}, {nan.id(), DropReason::NO_ROOM}, {all.id(), DropReason::NO_ROOM}}),
              drops.seen());
  }
  client.shutdown(0);  // what still waits is dropped: not this test's count
}

// ThreadedClient's sends with 0 or NaN, with no connection live, are
// dropped and told by the time the io thread has handled them: a round
// trip through it finds nothing waiting, where each was held a
// millisecond.
TEST(ZeroOrNanTimeout, AThreadedSendWithNoConnectionIsDroppedAtOnce) {
  Drops drops;
  std::promise<unsigned int> zero_ended;
  ThreadedClient client(multiplexer::peers::WEBSITE);  // never connected
  client.set_drop_observer(drops.observer());
  const MultiplexerMessage zero = client.new_message(multiplexer::types::TEST_UNROUTED, "zero");
  const MultiplexerMessage nan = client.new_message(multiplexer::types::TEST_UNROUTED, "nan");
  client.send_serialized(zero.SerializeAsString(), multiplexer::LanePtr(), 0,
                         [&zero_ended](unsigned int written) { zero_ended.set_value(written); });
  client.send_all_serialized(nan.SerializeAsString(), std::nanf(""));
  EXPECT_EQ(0u, client.waiting_messages()) << "held for a connection";
  EXPECT_EQ(2u, client.dropped());
  EXPECT_EQ((std::vector<std::pair<std::uint64_t, DropReason>>{{zero.id(), DropReason::NO_CONNECTION},
                                                               {nan.id(), DropReason::NO_CONNECTION}}),
            drops.seen());
  std::future<unsigned int> ended = zero_ended.get_future();
  ASSERT_EQ(std::future_status::ready, ended.wait_for(std::chrono::seconds(30)));
  EXPECT_EQ(0u, ended.get());
}

// A connect given NaN starts the attempt and returns, as with 0, where
// wait_for_connection() took NaN for a timer that had expired too soon and
// threw.
TEST(ZeroOrNanTimeout, ASyncConnectWithNanStartsTheAttemptAndReturns) {
  InProcessMultiplexer mx;
  multiplexer::Client client(multiplexer::peers::WEBSITE);
  multiplexer::ConnectionWrapper connection;
  ASSERT_NO_THROW(connection = client.connect("127.0.0.1", mx.port, std::nanf("")));
  EXPECT_TRUE(client.wait_for_connection(connection, 5)) << "the attempt went on";
  EXPECT_NO_THROW(client.wait_for_connection(connection, std::nanf("")));
}

// A flush of a message held with no deadline, on a client with nothing
// connected or on its way, returns at once, the tracker reading
// in_queue(): with no deadline the loop had no work, and an assertion
// threw. (flush_all(), which spun there, is no_connection_test.py's, on a
// thread of its own.)
TEST(ZeroOrNanTimeout, AFlushWithNothingComingReturnsAtOnce) {
  multiplexer::Client client(multiplexer::peers::WEBSITE);  // never connected
  multiplexer::Client::ScheduledMessageTracker tracker = client.queue(unrouted(client, "held"), -1);
  ASSERT_TRUE(tracker);
  EXPECT_NO_THROW(client.flush(tracker, -1));
  EXPECT_TRUE(tracker.in_queue()) << "held still";
  EXPECT_EQ(0u, client.dropped());
}
