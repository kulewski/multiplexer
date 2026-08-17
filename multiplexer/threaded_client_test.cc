// Unit tests for ThreadedClient against a Server running in this process:
// the outcomes a query can have, and the lifecycle rules, without the
// integration harness.
#include <atomic>
#include <chrono>
#include <future>
#include <thread>

#include <boost/asio/io_service.hpp>
#include <gtest/gtest.h>

#include "multiplexer/client.h"
#include "multiplexer/in_process_multiplexer.h"
#include "multiplexer/multiplexer.constants.h"
#include "multiplexer/threaded_client.h"

using multiplexer::ThreadedClient;

namespace {

using multiplexer::testing::InProcessMultiplexer;

} // namespace

TEST(ThreadedClient, QueryWithNoBackendFailsAtOnce) {
  InProcessMultiplexer mx;
  ThreadedClient client(multiplexer::peers::WEBSITE);
  ASSERT_TRUE(client.connect("127.0.0.1", mx.port, 5));
  ThreadedClient::Result result = client.query("hello", multiplexer::types::PYTHON_TEST_REQUEST, 5);
  EXPECT_EQ(ThreadedClient::FAILED, result.outcome);
  EXPECT_THROW(result.check(), ThreadedClient::OperationFailed);
}

TEST(ThreadedClient, QueryWithNoMultiplexerIsNotConnected) {
  ThreadedClient client(multiplexer::peers::WEBSITE);
  EXPECT_FALSE(client.connect("127.0.0.1", 1, 0.2f)); // nothing listens on port 1
  ThreadedClient::Result result = client.query("hello", multiplexer::types::PYTHON_TEST_REQUEST, 0.3f);
  EXPECT_EQ(ThreadedClient::NOT_CONNECTED, result.outcome);
}

// A synchronous Client next to the threaded one, to send it messages by
// instance id and read what comes back.
struct Peer {
  explicit Peer(unsigned short port, boost::uint32_t type) : client(type) { client.connect("127.0.0.1", port, 5); }
  multiplexer::MultiplexerMessage message(boost::uint32_t type, const std::string &payload, boost::uint64_t to,
                                          boost::uint64_t references = 0) {
    multiplexer::MultiplexerMessage msg;
    msg.set_id(client.random64());
    msg.set_from(client.instance_id());
    msg.set_type(type);
    msg.set_message(payload);
    msg.set_to(to);
    if (references)
      msg.set_references(references);
    return msg;
  }
  void send(const multiplexer::MultiplexerMessage &msg) { client.flush(client.schedule_one(msg), 5); }
  multiplexer::Client client;
};

TEST(ThreadedClient, OnMessageGetsWhatIsAddressedToIt) {
  InProcessMultiplexer mx;
  std::promise<multiplexer::MultiplexerMessage> got;
  ThreadedClient client(multiplexer::peers::WEBSITE,
                        [&got](const multiplexer::IncomingMessage &incoming) { got.set_value(*incoming.third); });
  ASSERT_TRUE(client.connect("127.0.0.1", mx.port, 5));
  Peer peer(mx.port, multiplexer::peers::WEBSITE);
  peer.send(peer.message(multiplexer::types::PYTHON_TEST_REQUEST, "for you", client.instance_id()));
  std::future<multiplexer::MultiplexerMessage> future = got.get_future();
  ASSERT_EQ(std::future_status::ready, future.wait_for(std::chrono::seconds(5)));
  EXPECT_EQ("for you", future.get().message());
}

TEST(ThreadedClient, PingIsAnsweredAndNotHandedOn) {
  InProcessMultiplexer mx;
  std::atomic<int> handed_on(0);
  ThreadedClient client(multiplexer::peers::WEBSITE,
                        [&handed_on](const multiplexer::IncomingMessage &) { ++handed_on; });
  ASSERT_TRUE(client.connect("127.0.0.1", mx.port, 5));
  Peer peer(mx.port, multiplexer::peers::WEBSITE);
  multiplexer::MultiplexerMessage ping = peer.message(multiplexer::types::PING, "echo me", client.instance_id());
  peer.send(ping);
  multiplexer::IncomingMessage pong = peer.client.read_raw_message(5);
  EXPECT_EQ(multiplexer::types::PING, pong.third->type());
  EXPECT_EQ(ping.id(), pong.third->references());
  EXPECT_EQ("echo me", pong.third->message());
  EXPECT_EQ(0, handed_on);
}

TEST(ThreadedClient, LateReplyIsDroppedButAnEventIsNot) {
  InProcessMultiplexer mx;
  std::atomic<int> handed_on(0);
  std::promise<multiplexer::MultiplexerMessage> event;
  ThreadedClient client(multiplexer::peers::WEBSITE, [&](const multiplexer::IncomingMessage &incoming) {
    ++handed_on;
    if (!incoming.third->references())
      event.set_value(*incoming.third);
  });
  ASSERT_TRUE(client.connect("127.0.0.1", mx.port, 5));
  Peer backend(mx.port, multiplexer::peers::PYTHON_TEST_SERVER);
  // The backend reads the request, lets the query time out (its search
  // stage too), then answers: a late reply.
  ThreadedClient::Result result = [&] {
    std::promise<ThreadedClient::Result> done;
    client.query(
        "slow", multiplexer::types::PYTHON_TEST_REQUEST,
        [&done](const ThreadedClient::Result &r) { done.set_value(r); }, 0.3f);
    multiplexer::IncomingMessage request = backend.client.read_raw_message(5);
    EXPECT_EQ("slow", request.third->message());
    ThreadedClient::Result r = done.get_future().get();
    backend.send(backend.message(multiplexer::types::PYTHON_TEST_RESPONSE, "too late", client.instance_id(),
                                 request.third->id()));
    return r;
  }();
  EXPECT_NE(ThreadedClient::REPLIED, result.outcome);
  // A genuine event to the client still arrives, after the late reply.
  backend.send(backend.message(multiplexer::types::PYTHON_TEST_REQUEST, "event", client.instance_id()));
  std::future<multiplexer::MultiplexerMessage> future = event.get_future();
  ASSERT_EQ(std::future_status::ready, future.wait_for(std::chrono::seconds(5)));
  EXPECT_EQ("event", future.get().message());
  EXPECT_EQ(1, handed_on) << "the late reply was handed on";
}

TEST(ThreadedClient, BlockingQueryOnTheIoThreadThrows) {
  InProcessMultiplexer mx;
  ThreadedClient client(multiplexer::peers::WEBSITE);
  ASSERT_TRUE(client.connect("127.0.0.1", mx.port, 5));
  std::promise<bool> threw;
  client.query("hello", multiplexer::types::PYTHON_TEST_REQUEST, [&](const ThreadedClient::Result &) {
    try {
      client.query("nested", multiplexer::types::PYTHON_TEST_REQUEST, 1);
      threw.set_value(false);
    } catch (std::logic_error &) {
      threw.set_value(true);
    }
  });
  EXPECT_TRUE(threw.get_future().get());
}

TEST(ThreadedClient, CallsAfterShutdownDoNotHang) {
  InProcessMultiplexer mx;
  ThreadedClient client(multiplexer::peers::WEBSITE);
  ASSERT_TRUE(client.connect("127.0.0.1", mx.port, 5));
  client.shutdown();
  client.shutdown(); // idempotent
  EXPECT_THROW(client.connections_count(), ThreadedClient::NotConnected);
  EXPECT_THROW(client.send(client.new_message(multiplexer::types::PYTHON_TEST_REQUEST, "x")),
               ThreadedClient::NotConnected);
  ThreadedClient::Result result = client.query("hello", multiplexer::types::PYTHON_TEST_REQUEST, 1);
  EXPECT_EQ(ThreadedClient::SHUT_DOWN, result.outcome);
}

TEST(ThreadedClient, ConcurrentShutdownIsSafe) {
  InProcessMultiplexer mx;
  ThreadedClient client(multiplexer::peers::WEBSITE);
  ASSERT_TRUE(client.connect("127.0.0.1", mx.port, 5));
  std::thread first([&] { client.shutdown(); });
  std::thread second([&] { client.shutdown(); });
  first.join();
  second.join();
}

TEST(ThreadedClient, ShutdownFailsQueriesInFlight) {
  InProcessMultiplexer mx;
  ThreadedClient client(multiplexer::peers::WEBSITE);
  ASSERT_TRUE(client.connect("127.0.0.1", mx.port, 5));
  ThreadedClient::Outcome seen = ThreadedClient::REPLIED;
  // PING to nobody in particular is never answered: the query waits for its
  // deadline, or for shutdown().
  client.query(
      "x", multiplexer::types::PYTHON_TEST_RESPONSE, [&](const ThreadedClient::Result &r) { seen = r.outcome; }, 30);
  client.shutdown();
  EXPECT_EQ(ThreadedClient::SHUT_DOWN, seen);
}
