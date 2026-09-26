// Unit tests for ThreadedClient against a Server running in this process:
// the outcomes a query can have, and the lifecycle rules, without the
// integration harness.
#include "multiplexer/threaded_client.h"

#include <gtest/gtest.h>

#include <asio/io_service.hpp>
#include <asio/ip/tcp.hpp>
#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <stdexcept>
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

// A backend built on the threaded client: answers every request handed to on_message.
struct Answering {
  explicit Answering(unsigned short port)
      : client(multiplexer::peers::PYTHON_TEST_SERVER, [this](const multiplexer::IncomingMessage& incoming) {
          multiplexer::MultiplexerMessage reply =
              client.new_message(multiplexer::types::PYTHON_TEST_RESPONSE, "answered");
          reply.set_to(incoming.third->from());
          reply.set_references(incoming.third->id());
          client.send(reply, incoming.second);
        }) {
    client.set_search_policy([] { return true; });
    EXPECT_TRUE(client.connect("127.0.0.1", port, 5));
  }
  void wait_acknowledged() {
    for (int tries = 0; tries < 500 && !client.routing_acknowledged(); ++tries) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_TRUE(client.routing_acknowledged());
  }
  ThreadedClient client;
};

TEST(ThreadedClient, RoutingOffTakesThePeerOutOfRuleRoutingAndBackIn) {
  InProcessMultiplexer mx;
  Answering backend(mx.port);
  ThreadedClient client(multiplexer::peers::WEBSITE);
  ASSERT_TRUE(client.connect("127.0.0.1", mx.port, 5));
  EXPECT_EQ(ThreadedClient::REPLIED, client.query("hi", multiplexer::types::PYTHON_TEST_REQUEST, 5).outcome);

  multiplexer::Routing off;
  off.set_any(false);
  off.set_all(false);
  backend.client.set_routing(off);
  backend.wait_acknowledged();
  ThreadedClient::Result refused = client.query("hi", multiplexer::types::PYTHON_TEST_REQUEST, 5);
  EXPECT_EQ(ThreadedClient::FAILED, refused.outcome) << "nobody takes the request by the rules, at once";

  backend.client.set_routing(multiplexer::Routing());
  backend.wait_acknowledged();
  EXPECT_EQ(ThreadedClient::REPLIED, client.query("hi", multiplexer::types::PYTHON_TEST_REQUEST, 5).outcome);

  // A last resort gets what nobody else could take.
  off.set_last_resort(true);
  backend.client.set_routing(off);
  backend.wait_acknowledged();
  EXPECT_EQ(ThreadedClient::REPLIED, client.query("hi", multiplexer::types::PYTHON_TEST_REQUEST, 5).outcome);
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

// A callback that throws when shutdown() fails its query is logged, and
// the shutdown goes on. It skipped the teardown: the io thread ran on and
// shutdown() never returned.
TEST(ThreadedClient, ShutdownGoesOnPastACallbackThatThrows) {
  InProcessMultiplexer mx;
  // On the heap, and left there should the shutdown hang: a destructor
  // would wait for it too, and the test would never report.
  ThreadedClient* client = new ThreadedClient(multiplexer::peers::WEBSITE);
  ASSERT_TRUE(client->connect("127.0.0.1", mx.port, 5));
  client->query(
      "x", multiplexer::types::PYTHON_TEST_RESPONSE,
      [](const ThreadedClient::Result&) { throw std::runtime_error("a bug in the caller's callback"); }, 30);
  std::shared_ptr<std::promise<void>> returned = std::make_shared<std::promise<void>>();
  std::future<void> shut = returned->get_future();
  std::thread([client, returned] {
    client->shutdown();
    returned->set_value();
  }).detach();
  ASSERT_EQ(std::future_status::ready, shut.wait_for(std::chrono::seconds(10))) << "shutdown() never returned";
  delete client;
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

// A port nothing listens on: bound and released at once.
unsigned short unused_port() {
  asio::io_service io_service;
  asio::ip::tcp::acceptor acceptor(io_service, asio::ip::tcp::endpoint(asio::ip::address_v4::loopback(), 0));
  const unsigned short port = acceptor.local_endpoint().port();
  acceptor.close();
  return port;
}

// A search counts the connections it went through. Another connection
// ending, here a connect that fails, neither ends the search nor turns the
// backend's PING to it into the query's answer.
TEST(ThreadedClient, ASearchIsNotEndedByAConnectionItDidNotGoThrough) {
  std::promise<ThreadedClient::Result> done;
  InProcessMultiplexer mx;
  ThreadedClient client(multiplexer::peers::WEBSITE);
  ASSERT_TRUE(client.connect("127.0.0.1", mx.port, 5));
  Peer backend(mx.port, multiplexer::peers::PYTHON_TEST_SERVER);
  client.query(
      client.new_message(multiplexer::types::PYTHON_TEST_REQUEST, "hello"),
      [&done](const ThreadedClient::Result& r) { done.set_value(r); }, 1.0f);
  multiplexer::IncomingMessage request = backend.client.read_raw_message(5);  // left unanswered
  ASSERT_EQ(multiplexer::types::PYTHON_TEST_REQUEST, request.third->type());
  multiplexer::IncomingMessage search = backend.client.read_raw_message(5);  // a second later
  ASSERT_EQ(multiplexer::types::BACKEND_FOR_PACKET_SEARCH, search.third->type());
  EXPECT_FALSE(client.connect("127.0.0.1", unused_port(), 0.5f));
  backend.send(backend.message(multiplexer::types::PING, "", search.third->from(), search.third->id()));
  serve_one(backend);  // the direct request, answered
  std::future<ThreadedClient::Result> future = done.get_future();
  ASSERT_EQ(std::future_status::ready, future.wait_for(std::chrono::seconds(5)));
  ThreadedClient::Result result = future.get();
  ASSERT_EQ(ThreadedClient::REPLIED, result.outcome);
  EXPECT_EQ(multiplexer::types::PYTHON_TEST_RESPONSE, result.reply.third->type()) << "the reply, not the PING";
  EXPECT_EQ("HELLO", result.reply.third->message());
}

TEST(ThreadedClient, AddressedQueryReachesTheInstanceNamedOnly) {
  // Before the client: on an early exit, its shutdown still runs the
  // queries' callbacks, which must find the promises alive.
  std::promise<ThreadedClient::Result> done;
  std::promise<ThreadedClient::Result> again;
  InProcessMultiplexer mx;
  ThreadedClient client(multiplexer::peers::WEBSITE);
  ASSERT_TRUE(client.connect("127.0.0.1", mx.port, 5));
  Peer first(mx.port, multiplexer::peers::PYTHON_TEST_SERVER);
  Peer second(mx.port, multiplexer::peers::PYTHON_TEST_SERVER);
  multiplexer::MultiplexerMessage request = client.new_message(multiplexer::types::PYTHON_TEST_REQUEST, "for two");
  request.set_to(second.client.instance_id());
  // The peers are synchronous clients of this thread, so the query goes
  // out in its callback form and the peer is served here meanwhile.
  client.query(request, [&done](const ThreadedClient::Result& r) { done.set_value(r); }, 5);
  serve_one(second);
  std::future<ThreadedClient::Result> answered = done.get_future();
  ASSERT_EQ(std::future_status::ready, answered.wait_for(std::chrono::seconds(10)));
  ThreadedClient::Result result = answered.get();
  ASSERT_EQ(ThreadedClient::REPLIED, result.outcome);
  EXPECT_EQ("FOR TWO", result.reply.third->message());
  EXPECT_EQ(second.client.instance_id(), result.reply.third->from());
  EXPECT_THROW(first.client.read_raw_message(0.3f), multiplexer::Client::OperationTimedOut) << "the other saw it";
  // The PING probe, as an option, on a query that needs no locating.
  client.query(
      request, [&again](const ThreadedClient::Result& r) { again.set_value(r); }, 5, multiplexer::LanePtr(),
      multiplexer::PROBE_PING);
  serve_one(second);
  std::future<ThreadedClient::Result> pinged = again.get_future();
  ASSERT_EQ(std::future_status::ready, pinged.wait_for(std::chrono::seconds(10)));
  EXPECT_EQ(ThreadedClient::REPLIED, pinged.get().outcome);
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

TEST(ThreadedClient, ATypedQueryThroughALaneIsSentAgainWhenItsMultiplexerGoes) {
  // Before the client: on an early exit, its shutdown still runs the
  // queries' callbacks, which must find the promises alive.
  std::promise<ThreadedClient::Result> warm;
  std::promise<ThreadedClient::Result> done;
  std::unique_ptr<InProcessMultiplexer> first(new InProcessMultiplexer());
  std::unique_ptr<InProcessMultiplexer> second(new InProcessMultiplexer());
  ThreadedClient client(multiplexer::peers::WEBSITE);
  ASSERT_TRUE(client.connect("127.0.0.1", first->port, 5));
  ASSERT_TRUE(client.connect("127.0.0.1", second->port, 5));
  Peer backend(first->port, multiplexer::peers::PYTHON_TEST_SERVER);
  backend.client.connect("127.0.0.1", second->port, 5);

  // A first query sets the lane's connection to the one it went through.
  multiplexer::LanePtr lane(new multiplexer::Lane());
  client.query(
      client.new_message(multiplexer::types::PYTHON_TEST_REQUEST, "warm-up"),
      [&warm](const ThreadedClient::Result& r) { warm.set_value(r); }, 5, lane);
  serve_one(backend);
  std::future<ThreadedClient::Result> warmed = warm.get_future();
  ASSERT_EQ(std::future_status::ready, warmed.wait_for(std::chrono::seconds(10)));
  ASSERT_EQ(ThreadedClient::REPLIED, warmed.get().outcome);
  const unsigned short way = lane->connection().endpoint().port();
  const bool first_way = way == first->port;

  // The backend holds the next request, and the multiplexer that carried it
  // goes away under the wait: the client sends the request again through
  // the other multiplexer, whose answer ends the query, and the lane moved.
  client.query(
      client.new_message(multiplexer::types::PYTHON_TEST_REQUEST, "slow"),
      [&done](const ThreadedClient::Result& r) { done.set_value(r); }, 10, lane);
  multiplexer::IncomingMessage held = backend.client.read_raw_message(5);
  EXPECT_EQ(way, held.second.endpoint().port());
  (first_way ? first : second).reset();
  const std::chrono::steady_clock::time_point killed = std::chrono::steady_clock::now();
  serve_one(backend, 10);  // the resend; without it, OperationTimedOut fails the test here
  std::future<ThreadedClient::Result> answered = done.get_future();
  ASSERT_EQ(std::future_status::ready, answered.wait_for(std::chrono::seconds(10)));
  EXPECT_LT(std::chrono::steady_clock::now() - killed, std::chrono::milliseconds(1500))
      << "sent again at once, not after the next reconnect attempt";
  ThreadedClient::Result result = answered.get();
  ASSERT_EQ(ThreadedClient::REPLIED, result.outcome);
  EXPECT_EQ("SLOW", result.reply.third->message());
  EXPECT_NE(way, lane->connection().endpoint().port()) << "the lane followed";
  EXPECT_EQ(0u, client.watched_ids()) << "the first attempt's id too, which was kept for good";

  // The multiplexer comes back where it was; the client's reconnect finds it.
  (first_way ? first : second).reset(new InProcessMultiplexer(way));
  std::chrono::steady_clock::time_point deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (client.connections_count() < 2 && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  EXPECT_EQ(2u, client.connections_count()) << "the reconnect the failed resend used to leave unscheduled";
}

// Through a pinned lane the search goes through the lane's connection
// only. That connection dying under the search ends the query with
// NOT_CONNECTED at once, which is what the pin means, and the client
// reconnects as for any lost connection.
TEST(ThreadedClient, APinnedLaneLosingItsMultiplexerDuringTheSearchIsNotConnected) {
  std::promise<ThreadedClient::Result> done;
  std::unique_ptr<InProcessMultiplexer> first(new InProcessMultiplexer());
  std::unique_ptr<InProcessMultiplexer> second(new InProcessMultiplexer());
  ThreadedClient client(multiplexer::peers::WEBSITE);
  ASSERT_TRUE(client.connect("127.0.0.1", first->port, 5));
  ASSERT_TRUE(client.connect("127.0.0.1", second->port, 5));
  Peer backend(first->port, multiplexer::peers::PYTHON_TEST_SERVER);
  backend.client.connect("127.0.0.1", second->port, 5);
  multiplexer::LanePtr lane(new multiplexer::Lane(true));
  ASSERT_EQ(1, client.send(client.new_message(multiplexer::types::PYTHON_TEST_REQUEST, "pins"), lane, 5));
  backend.client.read_raw_message(5);
  const unsigned short way = lane->connection().endpoint().port();
  const bool first_way = way == first->port;

  client.query(
      client.new_message(multiplexer::types::PYTHON_TEST_REQUEST, "held"),
      [&done](const ThreadedClient::Result& r) { done.set_value(r); }, 1.0f, lane);
  EXPECT_EQ(multiplexer::types::PYTHON_TEST_REQUEST, backend.client.read_raw_message(5).third->type());
  multiplexer::IncomingMessage search = backend.client.read_raw_message(5);  // a second later, left unanswered
  ASSERT_EQ(multiplexer::types::BACKEND_FOR_PACKET_SEARCH, search.third->type());
  EXPECT_EQ(way, search.second.endpoint().port()) << "the search went through the lane only";
  (first_way ? first : second).reset();
  std::future<ThreadedClient::Result> ended = done.get_future();
  ASSERT_EQ(std::future_status::ready, ended.wait_for(std::chrono::seconds(5)));
  EXPECT_EQ(ThreadedClient::NOT_CONNECTED, ended.get().outcome) << "not a timeout";

  (first_way ? first : second).reset(new InProcessMultiplexer(way));
  std::chrono::steady_clock::time_point deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (client.connections_count() < 2 && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  EXPECT_EQ(2u, client.connections_count()) << "reconnected";
}

TEST(ThreadedClient, AQueryReleasesItsLaneWhenItEnds) {
  std::promise<ThreadedClient::Result> done;  // before the client, as above
  InProcessMultiplexer mx;
  ThreadedClient client(multiplexer::peers::WEBSITE);
  ASSERT_TRUE(client.connect("127.0.0.1", mx.port, 5));
  Peer backend(mx.port, multiplexer::peers::PYTHON_TEST_SERVER);
  multiplexer::LanePtr lane(new multiplexer::Lane());
  std::weak_ptr<multiplexer::Lane> weak = lane;
  client.query(
      client.new_message(multiplexer::types::PYTHON_TEST_REQUEST, "held"),
      [&done](const ThreadedClient::Result& r) { done.set_value(r); }, 5, lane);
  lane.reset();  // the caller lets go mid-query
  EXPECT_FALSE(weak.expired()) << "the query holds it while it runs";
  serve_one(backend);
  std::future<ThreadedClient::Result> answered = done.get_future();
  ASSERT_EQ(std::future_status::ready, answered.wait_for(std::chrono::seconds(10)));
  EXPECT_EQ(ThreadedClient::REPLIED, answered.get().outcome);
  std::chrono::steady_clock::time_point deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!weak.expired() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  EXPECT_TRUE(weak.expired()) << "the query released it";
}

// A connection lost seconds before shutdown() arms a reconnect timer; the
// timer firing after the shutdown must not open a connection that nobody
// closes, which kept the io thread alive and the peer registered.
TEST(ThreadedClient, ShutdownRightAfterALostConnectionReturns) {
  InProcessMultiplexer first;
  std::unique_ptr<InProcessMultiplexer> second(new InProcessMultiplexer());
  ThreadedClient client(multiplexer::peers::WEBSITE);
  ASSERT_TRUE(client.connect("127.0.0.1", first.port, 5));
  ASSERT_TRUE(client.connect("127.0.0.1", second->port, 5));
  unsigned short lost_port = second->port;
  second.reset();  // gone; the client's reconnect timer is now pending
  std::chrono::steady_clock::time_point deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (client.connections_count() != 1 && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  ASSERT_EQ(1u, client.connections_count());
  second.reset(new InProcessMultiplexer(lost_port));  // back on the same port, so the reconnect would succeed
  std::chrono::steady_clock::time_point started = std::chrono::steady_clock::now();
  client.shutdown();
  EXPECT_LT(std::chrono::steady_clock::now() - started, std::chrono::seconds(2)) << "shutdown waited for the io thread";
  // No connection arrives at the multiplexer that came back: the timer was
  // cancelled by the shutdown. The count is read on the server's own thread.
  std::this_thread::sleep_for(std::chrono::seconds(4));
  std::promise<unsigned int> peers;
  second->io_service.post([&] { peers.set_value(second->server->connections_count(true)); });
  EXPECT_EQ(0u, peers.get_future().get()) << "a reconnect after shutdown";
}

// --- A connection shut down in the middle of a frame ------------------------
//
// asio reads and writes a frame over 64 KiB in steps, and a step already
// queued runs after shutdown(); the handlers hold the frame to the end. What
// these guard against is a read into, or a write from, freed memory, which
// only AddressSanitizer sees: ./check.sh runs this target under it.

TEST(ThreadedClient, AMultiplexerStoppingInTheMiddleOfLargeFramesFreesNothingInUse) {
  const std::string big(512 * 1024, 'x');
  for (int round = 0; round < 20; ++round) {
    std::unique_ptr<InProcessMultiplexer> mx(new InProcessMultiplexer());
    ThreadedClient client(multiplexer::peers::WEBSITE);
    ASSERT_TRUE(client.connect("127.0.0.1", mx->port, 5));
    for (int index = 0; index < 8; ++index) {
      client.send(client.new_message(multiplexer::types::PYTHON_TEST_REQUEST, big));
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(round % 5));
    mx.reset();  // its connection is reading one of the frames, the client's is writing one
    client.shutdown();
  }
}

TEST(ThreadedClient, AClientShutDownInTheMiddleOfALargeFrameFreesNothingInUse) {
  InProcessMultiplexer mx;
  Peer sender(mx.port, multiplexer::peers::WEBSITE);
  const std::string big(512 * 1024, 'y');
  for (int round = 0; round < 20; ++round) {
    std::unique_ptr<ThreadedClient> client(new ThreadedClient(multiplexer::peers::WEBSITE));
    ASSERT_TRUE(client->connect("127.0.0.1", mx.port, 5));
    for (int index = 0; index < 6; ++index) {
      sender.client.schedule_one(sender.message(multiplexer::types::PYTHON_TEST_REQUEST, big, client->instance_id()));
    }
    sender.client.flush_all(0.001f * (1 + round % 4));
    client->shutdown();  // its connection is reading one of the frames
    sender.client.flush_all(2);
    client.reset();
  }
}

// --- The synchronous Client: a reply to any attempt answers -----------------

namespace {

// A synchronous backend on a thread of its own, connected to two
// multiplexers: the first request it sees gets no answer, as if lost with
// its multiplexer; every later one is answered after `delay`, upper-cased,
// and a search at once. `requests` counts the requests that arrived.
struct LosingBackend {
  LosingBackend(unsigned short first, unsigned short second, std::chrono::milliseconds delay)
      : thread([this, first, second, delay] {
          multiplexer::Client client(multiplexer::peers::PYTHON_TEST_SERVER);
          client.connect("127.0.0.1", first, 5);
          client.connect("127.0.0.1", second, 5);
          ready = true;
          while (!stop) {
            multiplexer::IncomingMessage incoming;
            try {
              incoming = client.read_raw_message(0.1f);
            } catch (const std::exception&) {
              continue;
            }
            const multiplexer::MultiplexerMessage& request = *incoming.third;
            if (request.type() != multiplexer::types::PYTHON_TEST_REQUEST &&
                request.type() != multiplexer::types::BACKEND_FOR_PACKET_SEARCH) {
              continue;
            }
            if (request.type() == multiplexer::types::PYTHON_TEST_REQUEST && ++requests == 1) {
              continue;  // the first one is lost
            }
            if (request.type() == multiplexer::types::PYTHON_TEST_REQUEST) {
              std::this_thread::sleep_for(delay);
            }
            multiplexer::MultiplexerMessage reply;
            reply.set_id(client.random64());
            reply.set_from(client.instance_id());
            reply.set_to(request.from());
            reply.set_references(request.id());
            const bool search = request.type() == multiplexer::types::BACKEND_FOR_PACKET_SEARCH;
            reply.set_type(search ? multiplexer::types::PING : multiplexer::types::PYTHON_TEST_RESPONSE);
            std::string payload = search ? std::string() : request.message();
            for (char& character : payload) {
              character = std::toupper(static_cast<unsigned char>(character));
            }
            reply.set_message(payload);
            try {
              client.flush(client.schedule_one(reply), 5);
            } catch (const std::exception&) {
            }
          }
        }) {
    while (!ready) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
  }
  ~LosingBackend() {
    stop = true;
    thread.join();
  }
  std::atomic<bool> ready{false};
  std::atomic<bool> stop{false};
  std::atomic<int> requests{0};
  std::thread thread;
};

}  // namespace

// The lane's multiplexer dies under the wait and the request goes out again
// with a new id; its reply arrives only after the first stage ran out. It
// answers the query then and there, as a reply to any attempt does, rather
// than being dropped for a search and a third copy of the request.
TEST(Client, AReplyToTheResendAnswersDuringTheSearch) {
  std::unique_ptr<InProcessMultiplexer> first(new InProcessMultiplexer());
  std::unique_ptr<InProcessMultiplexer> second(new InProcessMultiplexer());
  LosingBackend backend(first->port, second->port, std::chrono::milliseconds(1300));
  multiplexer::Client client(multiplexer::peers::WEBSITE);
  multiplexer::ConnectionWrapper to_first = client.connect("127.0.0.1", first->port, 5);
  client.connect("127.0.0.1", second->port, 5);
  multiplexer::LanePtr lane(new multiplexer::Lane(to_first));  // the request goes through the first
  std::thread killer([&first] {
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    first.reset();
  });
  std::string answer;
  try {
    answer = client.query("resent", multiplexer::types::PYTHON_TEST_REQUEST, 1.0f, lane).third->message();
  } catch (const std::exception& error) {
    ADD_FAILURE() << "the query raised " << error.what();
  }
  killer.join();
  EXPECT_EQ("RESENT", answer);
  EXPECT_EQ(2, backend.requests.load()) << "the first and the resend; no direct copy after the search";
}
