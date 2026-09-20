// Unit tests for ThreadedClient against a Server running in this process:
// the outcomes a query can have, and the lifecycle rules, without the
// integration harness.
#include "multiplexer/threaded_client.h"

#include <gtest/gtest.h>

#include <asio/io_service.hpp>
#include <atomic>
#include <chrono>
#include <future>
#include <thread>

#include "multiplexer/client.h"
#include "multiplexer/in_process_multiplexer.h"
#include "multiplexer/multiplexer.constants.h"

using multiplexer::ThreadedClient;

namespace {

using multiplexer::testing::InProcessMultiplexer;

}  // namespace

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
  EXPECT_FALSE(client.connect("127.0.0.1", 1, 0.2f));  // nothing listens on port 1
  ThreadedClient::Result result = client.query("hello", multiplexer::types::PYTHON_TEST_REQUEST, 0.3f);
  EXPECT_EQ(ThreadedClient::NOT_CONNECTED, result.outcome);
}

// A synchronous Client next to the threaded one, to send it messages by
// instance id and read what comes back.
struct Peer {
  explicit Peer(unsigned short port, std::uint32_t type) : client(type) { client.connect("127.0.0.1", port, 5); }
  multiplexer::MultiplexerMessage message(std::uint32_t type, const std::string& payload, std::uint64_t to,
                                          std::uint64_t references = 0) {
    multiplexer::MultiplexerMessage msg;
    msg.set_id(client.random64());
    msg.set_from(client.instance_id());
    msg.set_type(type);
    msg.set_message(payload);
    msg.set_to(to);
    if (references) {
      msg.set_references(references);
    }
    return msg;
  }
  void send(const multiplexer::MultiplexerMessage& msg) { client.flush(client.schedule_one(msg), 5); }
  multiplexer::Client client;
};

TEST(ThreadedClient, OnMessageGetsWhatIsAddressedToIt) {
  InProcessMultiplexer mx;
  std::promise<multiplexer::MultiplexerMessage> got;
  ThreadedClient client(multiplexer::peers::WEBSITE,
                        [&got](const multiplexer::IncomingMessage& incoming) { got.set_value(*incoming.third); });
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
                        [&handed_on](const multiplexer::IncomingMessage&) { ++handed_on; });
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
  ThreadedClient client(multiplexer::peers::WEBSITE, [&](const multiplexer::IncomingMessage& incoming) {
    ++handed_on;
    if (!incoming.third->references()) {
      event.set_value(*incoming.third);
    }
  });
  ASSERT_TRUE(client.connect("127.0.0.1", mx.port, 5));
  Peer backend(mx.port, multiplexer::peers::PYTHON_TEST_SERVER);
  // The backend reads the request, lets the query time out (its search
  // stage too), then answers: a late reply.
  ThreadedClient::Result result = [&] {
    std::promise<ThreadedClient::Result> done;
    client.query(
        "slow", multiplexer::types::PYTHON_TEST_REQUEST,
        [&done](const ThreadedClient::Result& r) { done.set_value(r); }, 0.3f);
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
  client.query("hello", multiplexer::types::PYTHON_TEST_REQUEST, [&](const ThreadedClient::Result&) {
    try {
      client.query("nested", multiplexer::types::PYTHON_TEST_REQUEST, 1);
      threw.set_value(false);
    } catch (std::logic_error&) {
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
  client.shutdown();  // idempotent
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
      "x", multiplexer::types::PYTHON_TEST_RESPONSE, [&](const ThreadedClient::Result& r) { seen = r.outcome; }, 30);
  client.shutdown();
  EXPECT_EQ(ThreadedClient::SHUT_DOWN, seen);
}

// --- Addressed queries, lanes and pinning -----------------------------------

namespace {

// Reads the next message addressed to `peer` and answers it as a backend
// would: a PING for a search or a ping, the payload upper-cased for a
// request.
void serve_one(Peer& peer, float timeout = 5) {
  multiplexer::IncomingMessage incoming = peer.client.read_raw_message(timeout);
  const multiplexer::MultiplexerMessage& msg = *incoming.third;
  if (msg.type() == multiplexer::types::BACKEND_FOR_PACKET_SEARCH || msg.type() == multiplexer::types::PING) {
    peer.send(peer.message(multiplexer::types::PING, msg.message(), msg.from(), msg.id()));
    return;
  }
  std::string payload = msg.message();
  for (char& character : payload) {
    character = std::toupper(static_cast<unsigned char>(character));
  }
  peer.client.flush(
      peer.client.schedule_one(peer.message(multiplexer::types::PYTHON_TEST_RESPONSE, payload, msg.from(), msg.id()),
                               incoming.second),
      5);
}

}  // namespace

TEST(ThreadedClient, AddressedQueryReachesTheInstanceNamedOnly) {
  InProcessMultiplexer mx;
  ThreadedClient client(multiplexer::peers::WEBSITE);
  ASSERT_TRUE(client.connect("127.0.0.1", mx.port, 5));
  Peer first(mx.port, multiplexer::peers::PYTHON_TEST_SERVER);
  Peer second(mx.port, multiplexer::peers::PYTHON_TEST_SERVER);
  multiplexer::MultiplexerMessage request = client.new_message(multiplexer::types::PYTHON_TEST_REQUEST, "for two");
  request.set_to(second.client.instance_id());
  // The peers are synchronous clients of this thread, so the query goes
  // out in its callback form and the peer is served here meanwhile.
  std::promise<ThreadedClient::Result> done;
  client.query(request, [&done](const ThreadedClient::Result& r) { done.set_value(r); }, 5);
  serve_one(second);
  ThreadedClient::Result result = done.get_future().get();
  ASSERT_EQ(ThreadedClient::REPLIED, result.outcome);
  EXPECT_EQ("FOR TWO", result.reply.third->message());
  EXPECT_EQ(second.client.instance_id(), result.reply.third->from());
  EXPECT_THROW(first.client.read_raw_message(0.3f), multiplexer::Client::OperationTimedOut) << "the other saw it";
  // The PING probe, as an option, on a query that needs no locating.
  std::promise<ThreadedClient::Result> again;
  client.query(
      request, [&again](const ThreadedClient::Result& r) { again.set_value(r); }, 5, multiplexer::LanePtr(),
      multiplexer::PROBE_PING);
  serve_one(second);
  EXPECT_EQ(ThreadedClient::REPLIED, again.get_future().get().outcome);
}

TEST(ThreadedClient, AddressedQueryToAGoneInstanceFailsAtOnce) {
  InProcessMultiplexer mx;
  ThreadedClient client(multiplexer::peers::WEBSITE);
  ASSERT_TRUE(client.connect("127.0.0.1", mx.port, 5));
  Peer backend(mx.port, multiplexer::peers::PYTHON_TEST_SERVER);
  multiplexer::MultiplexerMessage request = client.new_message(multiplexer::types::PYTHON_TEST_REQUEST, "lost");
  request.set_to(0x1234567890ull);
  std::chrono::steady_clock::time_point started = std::chrono::steady_clock::now();
  ThreadedClient::Result result = client.query(request, 10);
  EXPECT_EQ(ThreadedClient::FAILED, result.outcome);
  EXPECT_LT(std::chrono::steady_clock::now() - started, std::chrono::seconds(3)) << "a delivery error, not a timeout";
  EXPECT_THROW(backend.client.read_raw_message(0.3f), multiplexer::Client::OperationTimedOut)
      << "an instance of the type got it";
}

TEST(ThreadedClient, ASearchAddressedToTheClientIsAnsweredWithAPing) {
  InProcessMultiplexer mx;
  std::atomic<int> handed_on(0);
  ThreadedClient client(multiplexer::peers::WEBSITE,
                        [&handed_on](const multiplexer::IncomingMessage&) { ++handed_on; });
  ASSERT_TRUE(client.connect("127.0.0.1", mx.port, 5));
  Peer peer(mx.port, multiplexer::peers::WEBSITE);
  multiplexer::BackendForPacketSearch search;
  search.set_packet_type(multiplexer::types::PYTHON_TEST_REQUEST);
  multiplexer::MultiplexerMessage probe =
      peer.message(multiplexer::types::BACKEND_FOR_PACKET_SEARCH, search.SerializeAsString(), client.instance_id());
  peer.send(probe);
  multiplexer::IncomingMessage pong = peer.client.read_raw_message(5);
  EXPECT_EQ(multiplexer::types::PING, pong.third->type());
  EXPECT_EQ(probe.id(), pong.third->references());
  EXPECT_EQ(0, handed_on);
}

TEST(ThreadedClient, ALaneKeepsAStreamOnOneConnectionAndAPinnedOneFails) {
  std::unique_ptr<InProcessMultiplexer> first(new InProcessMultiplexer());
  std::unique_ptr<InProcessMultiplexer> second(new InProcessMultiplexer());
  ThreadedClient client(multiplexer::peers::WEBSITE);
  ASSERT_TRUE(client.connect("127.0.0.1", first->port, 5));
  ASSERT_TRUE(client.connect("127.0.0.1", second->port, 5));
  Peer backend(first->port, multiplexer::peers::PYTHON_TEST_SERVER);
  backend.client.connect("127.0.0.1", second->port, 5);

  multiplexer::LanePtr lane(new multiplexer::Lane());
  for (int index = 0; index < 100; ++index) {
    ASSERT_EQ(1, client.send(client.new_message(multiplexer::types::PYTHON_TEST_REQUEST, "n" + std::to_string(index)),
                             lane, 5));
  }
  unsigned short way = 0;
  for (int index = 0; index < 100; ++index) {
    multiplexer::IncomingMessage incoming = backend.client.read_raw_message(5);
    EXPECT_EQ("n" + std::to_string(index), incoming.third->message()) << "in order";
    if (!way) {
      way = incoming.second.endpoint().port();
    }
    EXPECT_EQ(way, incoming.second.endpoint().port()) << "all the same way";
  }
  EXPECT_EQ(way, lane->connection().endpoint().port());
  EXPECT_TRUE(lane->connected());

  multiplexer::LanePtr pinned(new multiplexer::Lane(true));
  ASSERT_EQ(1, client.send(client.new_message(multiplexer::types::PYTHON_TEST_REQUEST, "pinned"), pinned, 5));
  multiplexer::IncomingMessage incoming = backend.client.read_raw_message(5);
  unsigned short pinned_way = incoming.second.endpoint().port();
  multiplexer::ConnectionWrapper connection = pinned->connection();

  // The multiplexer behind the pinned lane goes away.
  (pinned_way == first->port ? first : second).reset();
  std::chrono::steady_clock::time_point deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (pinned->connected() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  ASSERT_TRUE(pinned->closed());
  EXPECT_EQ(0, client.send(client.new_message(multiplexer::types::PYTHON_TEST_REQUEST, "refused"), pinned, 5));
  multiplexer::MultiplexerMessage request = client.new_message(multiplexer::types::PYTHON_TEST_REQUEST, "refused");
  EXPECT_EQ(ThreadedClient::NOT_CONNECTED, client.query(request, 5, pinned).outcome);
  // The bare connection is only preferred: the message goes the other way.
  EXPECT_EQ(1, client.send(client.new_message(multiplexer::types::PYTHON_TEST_REQUEST, "fell back"), connection, 10));
  // The lane that is not pinned follows.
  EXPECT_EQ(1, client.send(client.new_message(multiplexer::types::PYTHON_TEST_REQUEST, "followed"), lane, 10));
  EXPECT_NE(pinned_way, lane->connection().endpoint().port());
  for (int index = 0; index < 2; ++index) {
    multiplexer::IncomingMessage got = backend.client.read_raw_message(10);
    EXPECT_NE(pinned_way, got.second.endpoint().port());
  }
}

TEST(ThreadedClient, AQueryReleasesItsLaneWhenItEnds) {
  InProcessMultiplexer mx;
  ThreadedClient client(multiplexer::peers::WEBSITE);
  ASSERT_TRUE(client.connect("127.0.0.1", mx.port, 5));
  Peer backend(mx.port, multiplexer::peers::PYTHON_TEST_SERVER);
  multiplexer::LanePtr lane(new multiplexer::Lane());
  std::weak_ptr<multiplexer::Lane> weak = lane;
  std::promise<ThreadedClient::Result> done;
  client.query(
      client.new_message(multiplexer::types::PYTHON_TEST_REQUEST, "held"),
      [&done](const ThreadedClient::Result& r) { done.set_value(r); }, 5, lane);
  lane.reset();  // the caller lets go mid-query
  EXPECT_FALSE(weak.expired()) << "the query holds it while it runs";
  serve_one(backend);
  EXPECT_EQ(ThreadedClient::REPLIED, done.get_future().get().outcome);
  std::chrono::steady_clock::time_point deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!weak.expired() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  EXPECT_TRUE(weak.expired()) << "the query released it";
}
