// Unit tests for ThreadedClient against a Server running in this process:
// the outcomes a query can have, the lifecycle rules, what waits for a full
// connection's room, counted rather than timed, what a connection whose peer
// is gone still reads, and what a client that leaves still sends, without the
// integration harness.
#include "multiplexer/threaded_client.h"

#include <arpa/inet.h>
#include <dirent.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include <asio/io_service.hpp>
#include <asio/ip/tcp.hpp>
#include <asio/read.hpp>
#include <asio/write.hpp>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <fstream>
#include <future>
#include <memory>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <string>
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

// A flushing send ends when its frame is written: a hundred in a row each
// report their copy written. That nothing polls meanwhile is
// NothingPollsWhileAMessageWaits below.
TEST(ThreadedClient, AFlushingSendEndsWhenWritten) {
  InProcessMultiplexer mx;
  ThreadedClient client(multiplexer::peers::WEBSITE);
  ASSERT_TRUE(client.connect("127.0.0.1", mx.port, 5));
  for (int index = 0; index < 100; ++index) {
    ASSERT_EQ(1u, client.send(client.new_message(multiplexer::types::PYTHON_TEST_REQUEST, "n"), 5));
  }
}

// --- More than a connection's queue holds -----------------------------------
//
// What a full connection cannot take waits on the io thread, in order, and
// goes out when the connection says it has room: nothing polls, and an
// event costs what it moves. A multiplexer frozen with its socket open
// fills the queue whatever the speed of the machine, so what these check
// is counted, never timed.

// A backend that takes whatever comes, so that nothing sent comes back as a
// delivery error, and counts it. The multiplexer holds a whole burst for its
// type, PYTHON_TEST_SERVER, whose queue the rules make that large, so a sink
// that falls behind, on a loaded machine or under a sanitizer, loses
// nothing, and what a test counts is what the client did.
struct Sink {
  explicit Sink(unsigned short port)
      : client(multiplexer::peers::PYTHON_TEST_SERVER, [this](const multiplexer::IncomingMessage&) { ++received; }) {
    client.connect("127.0.0.1", port, 5);
  }
  // Whether `count` messages in all arrived within `seconds`.
  bool wait_for(std::size_t count, int seconds) {
    std::chrono::steady_clock::time_point deadline = std::chrono::steady_clock::now() + std::chrono::seconds(seconds);
    while (received.load() < count && std::chrono::steady_clock::now() < deadline) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return received.load() == count;
  }
  std::atomic<std::size_t> received{0};  // before the client, which counts into it until it is gone
  ThreadedClient client;
};

// A backend that answers every request with its payload.
struct Answerer {
  explicit Answerer(unsigned short port)
      : client(multiplexer::peers::PYTHON_TEST_SERVER, [this](const multiplexer::IncomingMessage& incoming) {
          multiplexer::MultiplexerMessage reply =
              client.new_message(multiplexer::types::PYTHON_TEST_RESPONSE, incoming.third->message());
          reply.set_to(incoming.third->from());
          reply.set_references(incoming.third->id());
          client.send(reply, incoming.second);
        }) {
    client.connect("127.0.0.1", port, 5);
  }
  ThreadedClient client;
};

// Holds a multiplexer's io thread, so that it reads nothing while the
// sockets stay open: a multiplexer frozen, until release() or the end of
// the scope.
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
  ~Freeze() { release(); }
  void release() {
    if (!done) {
      done = true;
      released.set_value();
    }
  }
  std::promise<void> released;
  bool done = false;
};

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

// Sends `count` messages of `size` bytes, fire and forget; `to`, when
// given, the instance that takes them.
void burst(ThreadedClient& client, int count, std::size_t size, std::uint64_t to = 0) {
  const std::string payload(size, 'x');
  for (int index = 0; index < count; ++index) {
    multiplexer::MultiplexerMessage msg = client.new_message(multiplexer::types::PYTHON_TEST_REQUEST, payload);
    if (to) {
      msg.set_to(to);
    }
    client.send(msg);
  }
}

// How many times this process's threads went to sleep and were woken, in
// all, as the kernel counts it: a thread that polls wakes every few
// milliseconds, one that waits for events only when one comes. What strace
// would show as the io thread's waits, without tracing it. False where
// there is no /proc to count in.
bool voluntary_switches(std::uint64_t* total) {
  *total = 0;
  DIR* tasks = opendir("/proc/self/task");
  if (!tasks) {
    return false;
  }
  while (dirent* task = readdir(tasks)) {
    if (task->d_name[0] == '.') {
      continue;
    }
    std::ifstream status(std::string("/proc/self/task/") + task->d_name + "/status");
    for (std::string line; std::getline(status, line);) {
      if (line.rfind("voluntary_ctxt_switches:", 0) == 0) {
        *total += std::stoull(line.substr(line.find(':') + 1));
      }
    }
  }
  closedir(tasks);
  return true;
}

// A burst the connection cannot take while its multiplexer is frozen: the
// rest waits, and everything arrives once it reads again, where most of it
// used to be dropped at its timeout while every send looked at every one
// still waiting. The io thread tries each waiting message about once, not
// once per message sent after it.
TEST(ThreadedClient, ABurstBeyondTheQueueArrivesWholeAtACostInProportion) {
  InProcessMultiplexer mx;
  Sink backend(mx.port);
  ThreadedClient client(multiplexer::peers::WEBSITE);
  ASSERT_TRUE(client.connect("127.0.0.1", mx.port, 5));
  const int count = frames_to_fill(1024);
  {
    Freeze freeze(mx);
    burst(client, count, 1024);
    EXPECT_FALSE(client.flush_all(0.2f)) << "frozen: something waits";
  }
  EXPECT_TRUE(client.flush_all(120));
  EXPECT_TRUE(backend.wait_for(count, 120)) << backend.received.load() << " of " << count << " arrived";
  const std::uint64_t retries = client.retries();
  EXPECT_GT(retries, 0u) << "the burst waited for room";
  EXPECT_LT(retries, 3u * count);
}

// A send to every connection gives each its copy: one whose connection is
// full waits for room there, where it used to be skipped, the copy going
// to the other multiplexers only.
TEST(ThreadedClient, EveryMultiplexerGetsItsCopyOfASendToAll) {
  InProcessMultiplexer first, second;
  Sink behind_first(first.port), behind_second(second.port);
  ThreadedClient client(multiplexer::peers::WEBSITE);
  ASSERT_TRUE(client.connect("127.0.0.1", first.port, 5));
  ASSERT_TRUE(client.connect("127.0.0.1", second.port, 5));
  const std::string chunk(16 * 1024, 'x');
  const int count = frames_to_fill(chunk.size());
  {
    Freeze freeze(second);
    for (int index = 0; index < count; ++index) {
      client.send_all(client.new_message(multiplexer::types::PYTHON_TEST_REQUEST, chunk));
    }
    EXPECT_TRUE(behind_first.wait_for(count, 60)) << behind_first.received.load() << " of " << count;
  }
  EXPECT_TRUE(client.flush_all(120));
  EXPECT_TRUE(behind_second.wait_for(count, 120)) << behind_second.received.load() << " of " << count;
}

// A query whose request finds the queue full waits for room, where it
// used to wait for a connection to come up and end NOT_CONNECTED with its
// connection live all along.
TEST(ThreadedClient, QueriesThatFindTheQueueFullAreAnswered) {
  InProcessMultiplexer mx;
  Sink sink(mx.port);
  Answerer answerer(mx.port);
  ThreadedClient client(multiplexer::peers::WEBSITE);
  ASSERT_TRUE(client.connect("127.0.0.1", mx.port, 5));
  const int count = 500;
  std::atomic<int> replied(0), ended(0);
  std::promise<void> all_ended;
  {
    Freeze freeze(mx);
    burst(client, frames_to_fill(16 * 1024), 16 * 1024, sink.client.instance_id());  // for the sink: the queue fills
    for (int index = 0; index < count; ++index) {
      multiplexer::MultiplexerMessage request =
          client.new_message(multiplexer::types::PYTHON_TEST_REQUEST, "q" + std::to_string(index));
      request.set_to(answerer.client.instance_id());
      client.query(
          request,
          [&](const ThreadedClient::Result& result) {
            if (result.outcome == ThreadedClient::REPLIED) {
              ++replied;
            }
            if (++ended == count) {
              all_ended.set_value();
            }
          },
          30);
    }
  }
  ASSERT_EQ(std::future_status::ready, all_ended.get_future().wait_for(std::chrono::seconds(60)));
  EXPECT_EQ(count, replied.load());
}

// While messages wait for room and a flush_all() waits for them, the
// threads sleep until something happens: over half a second the process
// wakes a handful of times, where the two 5 ms polls that ran then woke it
// some two hundred.
TEST(ThreadedClient, NothingPollsWhileAMessageWaits) {
  std::uint64_t before = 0, after = 0;
  if (!voluntary_switches(&before)) {
    GTEST_SKIP() << "no /proc/self/task to count wakeups in";
  }
  InProcessMultiplexer mx;
  Sink backend(mx.port);
  ThreadedClient client(multiplexer::peers::WEBSITE);
  ASSERT_TRUE(client.connect("127.0.0.1", mx.port, 5));
  std::future<bool> flushed;
  {
    Freeze freeze(mx);
    burst(client, frames_to_fill(16 * 1024), 16 * 1024);  // the queue fills, the rest waits
    flushed = std::async(std::launch::async, [&client] { return client.flush_all(60); });
    // A round trip through the io thread: every send before it is placed,
    // so what follows counts waiting, not work.
    ASSERT_EQ(1u, client.connections_count());
    voluntary_switches(&before);
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    voluntary_switches(&after);
    EXPECT_LT(after - before, 20u);
  }
  EXPECT_TRUE(flushed.get());
}

// flush_all() counts what waits for a connection: with none up it is not
// done, and it is once the client reconnected and wrote the message.
TEST(ThreadedClient, FlushAllWaitsForAMessageThatFoundNoConnection) {
  std::unique_ptr<InProcessMultiplexer> mx(new InProcessMultiplexer());
  const unsigned short port = mx->port;
  ThreadedClient client(multiplexer::peers::WEBSITE);
  ASSERT_TRUE(client.connect("127.0.0.1", port, 5));
  mx.reset();
  std::chrono::steady_clock::time_point deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (client.connections_count() != 0 && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  ASSERT_EQ(0u, client.connections_count());
  client.send(client.new_message(multiplexer::types::PYTHON_TEST_REQUEST, "waits"));
  EXPECT_FALSE(client.flush_all(0.5f)) << "the message waits for a connection";
  mx.reset(new InProcessMultiplexer(port));
  EXPECT_TRUE(client.flush_all(15)) << "written once the client reconnected";
}

// flush_all() waits for what was sent before it, not for what is sent
// meanwhile: with another thread sending all along, it still ends.
TEST(ThreadedClient, FlushAllEndsWhileAnotherThreadKeepsSending) {
  InProcessMultiplexer mx;
  Sink backend(mx.port);
  ThreadedClient client(multiplexer::peers::WEBSITE);
  ASSERT_TRUE(client.connect("127.0.0.1", mx.port, 5));
  std::atomic<bool> stop(false);
  std::thread sender([&] {
    while (!stop.load()) {
      burst(client, 10, 100);
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  });
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  for (int round = 0; round < 5; ++round) {
    EXPECT_TRUE(client.flush_all(30));
  }
  stop = true;
  sender.join();
}

// A pinned lane whose connection's queue is full waits for room there,
// within its timeout, instead of dropping the message as if the connection
// were gone; once the multiplexer reads again, everything goes out.
TEST(ThreadedClient, APinnedLaneWaitsForRoomOnItsConnection) {
  InProcessMultiplexer mx;
  Sink backend(mx.port);
  ThreadedClient client(multiplexer::peers::WEBSITE);
  ASSERT_TRUE(client.connect("127.0.0.1", mx.port, 5));
  multiplexer::LanePtr pinned(new multiplexer::Lane(true));
  ASSERT_EQ(1u, client.send(client.new_message(multiplexer::types::PYTHON_TEST_REQUEST, "first"), pinned, 5));
  const std::string chunk(16 * 1024, 'x');
  const int count = frames_to_fill(chunk.size());
  std::future<unsigned int> last;
  {
    Freeze freeze(mx);
    for (int index = 0; index < count; ++index) {  // the queue fills, the rest waits
      client.send(client.new_message(multiplexer::types::PYTHON_TEST_REQUEST, chunk), pinned);
    }
    last = std::async(std::launch::async, [&client, pinned] {
      return client.send(client.new_message(multiplexer::types::PYTHON_TEST_REQUEST, "last"), pinned, 30);
    });
    EXPECT_EQ(std::future_status::timeout, last.wait_for(std::chrono::milliseconds(300))) << "waits, not refused";
  }
  EXPECT_EQ(1u, last.get());
  EXPECT_TRUE(client.flush_all(30));
  EXPECT_FALSE(pinned->closed());
  EXPECT_TRUE(backend.wait_for(count + 2, 60)) << backend.received.load() << " of " << count + 2 << " arrived";
}

// A lane that is not pinned keeps its connection while that lives: what its
// full connection cannot take waits for room there, where it went through
// the other multiplexer, the lane moving there, the stream split in two.
TEST(ThreadedClient, ALaneWaitsForRoomOnItsConnectionWhileItLives) {
  InProcessMultiplexer first, second;
  Sink behind_first(first.port), behind_second(second.port);
  ThreadedClient client(multiplexer::peers::WEBSITE);
  ASSERT_TRUE(client.connect("127.0.0.1", first.port, 5));
  ASSERT_TRUE(client.connect("127.0.0.1", second.port, 5));
  multiplexer::LanePtr lane(new multiplexer::Lane());
  ASSERT_EQ(1u, client.send(client.new_message(multiplexer::types::PYTHON_TEST_REQUEST, "first"), lane, 5));
  const unsigned short held = lane->connection().endpoint().port();
  InProcessMultiplexer& frozen = held == first.port ? first : second;
  Sink& its = held == first.port ? behind_first : behind_second;
  Sink& other = held == first.port ? behind_second : behind_first;
  const std::string chunk(16 * 1024, 'x');
  const int count = frames_to_fill(chunk.size());
  {
    Freeze freeze(frozen);
    for (int index = 0; index < count; ++index) {
      client.send(client.new_message(multiplexer::types::PYTHON_TEST_REQUEST, chunk), lane);
    }
    ASSERT_EQ(2u, client.connections_count());  // a round trip through the io thread: every send placed
    EXPECT_EQ(held, lane->connection().endpoint().port()) << "the lane moved";
  }
  EXPECT_TRUE(client.flush_all(60));
  EXPECT_TRUE(its.wait_for(count + 1, 60)) << its.received.load() << " of " << count + 1;
  EXPECT_EQ(0u, other.received.load()) << "went the other way";
}

// connect() ends when the connection does: when it fails, at once rather
// than at its timeout.
TEST(ThreadedClient, ConnectEndsWhenTheConnectionFails) {
  ThreadedClient client(multiplexer::peers::WEBSITE);
  const std::chrono::steady_clock::time_point started = std::chrono::steady_clock::now();
  EXPECT_FALSE(client.connect("127.0.0.1", 1, 30));  // nothing listens on port 1
  EXPECT_LT(std::chrono::steady_clock::now() - started, std::chrono::seconds(15)) << "not at its timeout";
}

// --- The synchronous Client and full connections -----------------------------
//
// The Client shares BasicClient's outbox with ThreadedClient: what a full
// connection cannot take waits for its room, and goes in as the Client's
// calls run the loop.

// CPU time this thread has used: a wait that spins uses as much as it
// lasts, one that sleeps next to none, whatever the machine's speed.
std::chrono::nanoseconds thread_cpu() {
  timespec now;
  clock_gettime(CLOCK_THREAD_CPUTIME_ID, &now);
  return std::chrono::seconds(now.tv_sec) + std::chrono::nanoseconds(now.tv_nsec);
}

// Nothing is refused while the connection lives, where a message the full
// queue could not take got a null tracker, and all of it arrives.
TEST(Client, WhatAFullConnectionCannotTakeWaitsForItsRoom) {
  InProcessMultiplexer mx;
  Sink backend(mx.port);
  Peer sender(mx.port, multiplexer::peers::WEBSITE);
  const std::string chunk(16 * 1024, 'x');
  const int count = frames_to_fill(chunk.size());
  int refused = 0;
  {
    Freeze freeze(mx);
    for (int index = 0; index < count; ++index) {
      if (!sender.client.schedule_one(sender.message(multiplexer::types::PYTHON_TEST_REQUEST, chunk, 0))) {
        ++refused;
      }
    }
  }
  EXPECT_EQ(0, refused);
  EXPECT_TRUE(sender.client.flush_all(60));
  EXPECT_TRUE(backend.wait_for(count, 60)) << backend.received.load() << " of " << count;
}

// A flushing send that finds its connection full waits for room with the
// loop asleep, where it went round a loop at full speed until its timeout.
TEST(Client, AFlushingSendWaitsForRoomAsleep) {
  InProcessMultiplexer mx;
  Sink backend(mx.port);
  Peer sender(mx.port, multiplexer::peers::WEBSITE);
  const std::string chunk(16 * 1024, 'x');
  Freeze freeze(mx);
  for (int index = 0; index < frames_to_fill(chunk.size()); ++index) {
    sender.client.schedule_one(sender.message(multiplexer::types::PYTHON_TEST_REQUEST, chunk, 0));
  }
  const std::chrono::nanoseconds before = thread_cpu();
  EXPECT_THROW(sender.client.send(sender.message(multiplexer::types::PYTHON_TEST_REQUEST, "last", 0), 0.5f),
               multiplexer::Client::OperationTimedOut);
  EXPECT_LT(thread_cpu() - before, std::chrono::milliseconds(100)) << "of the 0.5 s it waited";
}

// A send to every connection gives a full one its copy once it has room,
// where the copy was skipped.
TEST(Client, EveryMultiplexerGetsItsCopyOfASendToAll) {
  InProcessMultiplexer first, second;
  Sink behind_first(first.port), behind_second(second.port);
  Peer sender(first.port, multiplexer::peers::WEBSITE);
  sender.client.connect("127.0.0.1", second.port, 5);
  const std::string chunk(16 * 1024, 'x');
  const int count = frames_to_fill(chunk.size());
  {
    Freeze freeze(second);
    for (int index = 0; index < count; ++index) {
      EXPECT_EQ(2u, sender.client.schedule_all(sender.message(multiplexer::types::PYTHON_TEST_REQUEST, chunk, 0)));
    }
    EXPECT_FALSE(sender.client.flush_all(0.2f)) << "frozen: the copies for it wait";
  }
  EXPECT_TRUE(sender.client.flush_all(60));
  EXPECT_TRUE(behind_first.wait_for(count, 60)) << behind_first.received.load() << " of " << count;
  EXPECT_TRUE(behind_second.wait_for(count, 60)) << behind_second.received.load() << " of " << count;
}

// A lane that is not pinned keeps its connection while that lives, as in
// ThreadedClient: a flushing send its full connection cannot take waits
// there, where it went through the other multiplexer and the lane moved.
TEST(Client, ALaneWaitsForRoomOnItsConnectionWhileItLives) {
  InProcessMultiplexer first, second;
  Sink behind_first(first.port), behind_second(second.port);
  Peer sender(first.port, multiplexer::peers::WEBSITE);
  sender.client.connect("127.0.0.1", second.port, 5);
  multiplexer::LanePtr lane(new multiplexer::Lane());
  sender.client.send(sender.message(multiplexer::types::PYTHON_TEST_REQUEST, "first", 0), 5, lane);
  const unsigned short held = lane->connection().endpoint().port();
  InProcessMultiplexer& frozen = held == first.port ? first : second;
  Sink& its = held == first.port ? behind_first : behind_second;
  Sink& other = held == first.port ? behind_second : behind_first;
  const std::string chunk(16 * 1024, 'x');
  const int count = frames_to_fill(chunk.size());
  {
    Freeze freeze(frozen);
    for (int index = 0; index < count; ++index) {
      sender.client.schedule_one(sender.message(multiplexer::types::PYTHON_TEST_REQUEST, chunk, 0), lane->connection());
    }
    EXPECT_THROW(sender.client.send(sender.message(multiplexer::types::PYTHON_TEST_REQUEST, "last", 0), 0.2f, lane),
                 multiplexer::Client::OperationTimedOut);
    EXPECT_EQ(held, lane->connection().endpoint().port()) << "the lane moved";
  }
  EXPECT_TRUE(sender.client.flush_all(60));
  EXPECT_TRUE(its.wait_for(count + 2, 60)) << its.received.load() << " of " << count + 2;
  EXPECT_EQ(0u, other.received.load()) << "went the other way";
}

// --- A peer gone with messages unread ----------------------------------------

// A peer that sends and closes at once, with a reset, while the multiplexer
// is frozen: when the multiplexer reads again, a write to the peer fails
// while what the peer sent still waits in the socket. The multiplexer reads
// it on and routes it, where it used to close the connection with all of
// it unread. The first message asks for a delivery error from nobody, so
// that the multiplexer writes to the peer before it has read the rest.
TEST(Connection, WhatAPeerSentBeforeItClosedIsRoutedWhenAWriteToItFails) {
  InProcessMultiplexer mx;
  Sink sink(mx.port);
  asio::io_service io_service;
  asio::ip::tcp::socket socket(io_service);
  socket.connect(asio::ip::tcp::endpoint(asio::ip::make_address("127.0.0.1"), mx.port));
  socket.set_option(asio::ip::tcp::no_delay(true));  // as a client's: every frame goes out as written
  const std::uint64_t instance = 0x5eed;
  asio::write(socket,
              multiplexer::impl::create_welcome_message(multiplexer::peers::WEBSITE, instance)->get_message_buffer());
  multiplexer::RawMessage welcome;  // the multiplexer's: the peer is registered
  asio::read(socket, welcome.get_header_buffer());
  ASSERT_TRUE(welcome.unpack_header());
  asio::read(socket, welcome.get_body_buffer());
  ASSERT_TRUE(welcome.verify());
  const int count = 200;
  {
    Freeze freeze(mx);
    for (int index = 0; index <= count; ++index) {
      multiplexer::MultiplexerMessage msg;
      msg.set_id(1000 + index);
      msg.set_from(instance);
      msg.set_type(multiplexer::types::PYTHON_TEST_REQUEST);
      msg.set_message("m" + std::to_string(index));
      if (index == 0) {
        msg.set_to(0xdead);  // nobody: its delivery error goes back to the peer
        msg.set_report_delivery_error(true);
      }
      asio::write(
          socket,
          std::shared_ptr<multiplexer::RawMessage>(multiplexer::RawMessage::FromMessage(msg))->get_message_buffer());
    }
    // All of it in the multiplexer's socket, acknowledged, before the
    // reset: what is lost from here on, the multiplexer lost.
    int unacknowledged = 0;
    const std::chrono::steady_clock::time_point deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (ioctl(socket.native_handle(), TIOCOUTQ, &unacknowledged) == 0 && unacknowledged != 0 &&
           std::chrono::steady_clock::now() < deadline) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_EQ(0, unacknowledged);
    socket.set_option(asio::socket_base::linger(true, 0));
    socket.close();  // a reset, before the multiplexer read any of it
  }
  EXPECT_TRUE(sink.wait_for(count, 30)) << sink.received.load() << " of " << count << " routed";
}

// --- A client leaving with messages on their way ----------------------------
//
// A client that shut down right after flush_all(), its multiplexer behind
// and a frame from it not read yet, closed its sockets with that frame
// unread: the kernel then reset the connection and threw away what it had
// not sent yet, messages flush_all() had called written. It now sends its
// end of the stream after them and reads on until the multiplexer's.

namespace {

// Reads `size` bytes into `data`: 0, -1 at the end of the stream, or the
// errno.
int read_exactly(int fd, void* data, std::size_t size) {
  char* at = static_cast<char*>(data);
  while (size) {
    const ssize_t got = ::recv(fd, at, size, 0);
    if (got == 0) {
      return -1;
    }
    if (got < 0) {
      if (errno == EINTR) {
        continue;
      }
      return errno;
    }
    at += got;
    size -= static_cast<std::size_t>(got);
  }
  return 0;
}

// Reads one frame: 0, -1 at the end of the stream, or the errno.
int read_frame(int fd, multiplexer::RawMessage* frame) {
  if (int error = read_exactly(fd, frame->get_header_buffer().data(), frame->get_header_buffer().size())) {
    return error;
  }
  if (!frame->unpack_header()) {
    return EPROTO;
  }
  if (int error = read_exactly(fd, frame->get_body_buffer().data(), frame->get_body_buffer().size())) {
    return error;
  }
  return frame->verify() ? 0 : EPROTO;
}

// Writes one frame whole: 0 or the errno.
int write_frame(int fd, const multiplexer::RawMessage& frame) {
  for (const asio::const_buffer& part : frame.get_message_buffer()) {
    const char* at = static_cast<const char*>(part.data());
    std::size_t size = part.size();
    while (size) {
      const ssize_t sent = ::send(fd, at, size, MSG_NOSIGNAL);
      if (sent < 0) {
        if (errno == EINTR) {
          continue;
        }
        return errno;
      }
      at += sent;
      size -= static_cast<std::size_t>(sent);
    }
  }
  return 0;
}

// What /proc/net/tcp says of the socket from port `local` to port `remote`
// on 127.0.0.1: its state, two hex digits ("01" established, "04" once its
// end of the stream is queued), and how much of what it received nobody
// read yet. The state is "" once the socket is gone, "?" when /proc does
// not say.
struct TcpEntry {
  std::string state;
  unsigned long unread = 0;
};
TcpEntry loopback_tcp_entry(unsigned short local, unsigned short remote) {
  TcpEntry entry;
  std::ifstream table("/proc/net/tcp");
  if (!table) {
    entry.state = "?";
    return entry;
  }
  // An address as the kernel prints it: the number its bytes make in memory.
  char from[32], to[32];
  std::snprintf(from, sizeof from, "%08X:%04X", htonl(INADDR_LOOPBACK), local);
  std::snprintf(to, sizeof to, "%08X:%04X", htonl(INADDR_LOOPBACK), remote);
  std::string line;
  std::getline(table, line);  // the heading
  while (std::getline(table, line)) {
    std::istringstream fields(line);
    std::string slot, local_address, remote_address, state, queues;  // queues: tx_queue:rx_queue
    fields >> slot >> local_address >> remote_address >> state >> queues;
    if (local_address == from && remote_address == to && queues.find(':') != std::string::npos) {
      entry.state = state;
      entry.unread = std::stoul(queues.substr(queues.find(':') + 1), nullptr, 16);
      return entry;
    }
  }
  return entry;
}

// A multiplexer of the test's own, which reads nothing while the client is
// there. It takes the client's welcome and answers with its own, on a
// socket that buffers next to nothing, so that most of what the client then
// sends waits in the client's kernel. heartbeat() sends the client a frame
// and returns once the client's kernel has it: unread until the client
// reads again. Once the client has left, its socket closed, or is leaving,
// its end of the stream queued and that frame read, it counts the client's
// messages of `type` to the end of the stream, and whether the stream ended
// with the client's end or with a reset.
class LateReader {
 public:
  struct Result {
    int counted = 0;
    bool reset = false;
    bool seen = true;  // false when /proc/net/tcp was not there to say when the client left
  };

  explicit LateReader(std::uint32_t type) : type_(type) {
    listener_ = ::socket(AF_INET, SOCK_STREAM, 0);
    const int least = 1;  // the kernel's least instead; an accepted socket inherits it
    ::setsockopt(listener_, SOL_SOCKET, SO_RCVBUF, &least, sizeof least);
    sockaddr_in address = {};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    socklen_t length = sizeof address;
    if (::bind(listener_, reinterpret_cast<sockaddr*>(&address), length) != 0 || ::listen(listener_, 1) != 0 ||
        ::getsockname(listener_, reinterpret_cast<sockaddr*>(&address), &length) != 0) {
      ::close(listener_);
      throw std::runtime_error("no socket to listen on");
    }
    port = ntohs(address.sin_port);
    thread_ = std::thread([this] { _serve(); });
  }
  ~LateReader() {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      stopping_ = true;
    }
    ::shutdown(listener_, SHUT_RDWR);  // an accept still waiting returns
    thread_.join();
    ::close(listener_);
  }

  // Sends the client a heartbeat and returns once the client's kernel has
  // acknowledged it: false when there was no connection to send it on.
  bool heartbeat() {
    int fd;
    {
      std::unique_lock<std::mutex> lock(mutex_);
      changed_.wait_for(lock, std::chrono::seconds(10), [this] { return fd_ >= 0 || stopping_; });
      fd = fd_;
    }
    if (fd < 0) {
      return false;
    }
    multiplexer::MultiplexerMessage beat;
    beat.set_id(1);
    beat.set_from(kId);
    beat.set_type(multiplexer::types::HEARTBIT);
    if (write_frame(fd, *std::unique_ptr<multiplexer::RawMessage>(multiplexer::RawMessage::FromMessage(beat)))) {
      return false;
    }
    int unacknowledged = 0;
    const std::chrono::steady_clock::time_point deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (ioctl(fd, TIOCOUTQ, &unacknowledged) == 0 && unacknowledged != 0 &&
           std::chrono::steady_clock::now() < deadline) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return unacknowledged == 0;
  }

  // What was read once the client left, when it has.
  Result result() {
    std::unique_lock<std::mutex> lock(mutex_);
    changed_.wait(lock, [this] { return done_; });
    return result_;
  }

  unsigned short port = 0;

 private:
  static const std::uint64_t kId = 0x6d78;

  void _serve() {
    Result result;
    const int fd = ::accept(listener_, nullptr, nullptr);
    if (fd >= 0) {
      _talk(fd, &result);
      ::close(fd);
    }
    std::lock_guard<std::mutex> lock(mutex_);
    fd_ = -1;
    result_ = result;
    done_ = true;
    changed_.notify_all();
  }

  void _talk(int fd, Result* result) {
    timeval patience = {30, 0};  // a stream that never ends fails the test rather than hangs it
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &patience, sizeof patience);
    sockaddr_in client = {};
    socklen_t length = sizeof client;
    ::getpeername(fd, reinterpret_cast<sockaddr*>(&client), &length);
    multiplexer::RawMessage welcome;  // the client's
    if (read_frame(fd, &welcome)) {
      return;
    }
    {
      std::lock_guard<std::mutex> lock(mutex_);
      fd_ = fd;
    }
    changed_.notify_all();
    if (write_frame(fd, *multiplexer::impl::create_welcome_message(multiplexer::peers::MULTIPLEXER, kId))) {
      return;
    }
    // The client's socket, as the kernel sees it: gone once it closed, or
    // with its end of the stream queued and nothing left unread once it
    // reads on after that. Queued alone is not enough: a shutdown() that
    // closes at once queues the end too, first.
    for (;;) {
      const TcpEntry entry = loopback_tcp_entry(ntohs(client.sin_port), port);
      if (entry.state == "?") {
        result->seen = false;
        return;
      }
      if (entry.state.empty() || (entry.state != "01" && entry.unread == 0)) {
        break;
      }
      std::unique_lock<std::mutex> lock(mutex_);
      if (changed_.wait_for(lock, std::chrono::milliseconds(1), [this] { return stopping_; })) {
        return;
      }
    }
    for (;;) {
      multiplexer::RawMessage frame;
      if (const int error = read_frame(fd, &frame)) {
        result->reset = error == ECONNRESET;
        return;
      }
      multiplexer::MultiplexerMessage msg;
      if (msg.ParseFromString(frame.get_message()) && msg.type() == type_) {
        ++result->counted;
      }
    }
  }

  const std::uint32_t type_;
  int listener_ = -1;
  std::thread thread_;
  std::mutex mutex_;
  std::condition_variable changed_;
  int fd_ = -1;  // the connection, once the client's welcome is read
  bool stopping_ = false;
  bool done_ = false;
  Result result_;
};

// Messages of which the late reader's socket takes a few: the rest waits in
// the client's kernel, which takes them all before the reader reads.
const int kLeftBehind = 12;
const std::string kKilobyte(1024, 'k');

}  // namespace

TEST(ThreadedClient, WhatFlushAllCalledWrittenArrivesThoughTheClientLeavesWithAFrameUnread) {
  LateReader multiplexer(multiplexer::types::PYTHON_TEST_REQUEST);
  std::atomic<bool> unread(false);
  {
    ThreadedClient client(multiplexer::peers::WEBSITE);
    ASSERT_TRUE(client.connect("127.0.0.1", multiplexer.port, 5));
    for (int index = 0; index < kLeftBehind; ++index) {
      client.send(client.new_message(multiplexer::types::PYTHON_TEST_REQUEST, kKilobyte));
    }
    if (!client.flush_all(5)) {
      GTEST_SKIP() << "this kernel does not hold " << kLeftBehind << " KiB for a peer that reads nothing";
    }
    // A query nobody answers: its callback runs on the io thread as
    // shutdown() begins, before the connection closes, and has the
    // heartbeat arrive while nothing reads.
    client.query(
        "never answered", multiplexer::types::PYTHON_TEST_RESPONSE,
        [&](const ThreadedClient::Result& result) {
          if (result.outcome == ThreadedClient::SHUT_DOWN) {
            unread = multiplexer.heartbeat();
          }
        },
        60);
    client.shutdown();
  }
  ASSERT_TRUE(unread.load()) << "no frame was left unread";
  const LateReader::Result result = multiplexer.result();
  if (!result.seen) {
    GTEST_SKIP() << "no /proc/net/tcp to say when the client left";
  }
  EXPECT_EQ(kLeftBehind, result.counted);
  EXPECT_FALSE(result.reset) << "the client reset the connection";
}

TEST(Client, WhatFlushAllCalledWrittenArrivesThoughTheClientLeavesWithAFrameUnread) {
  LateReader multiplexer(multiplexer::types::PYTHON_TEST_REQUEST);
  bool unread = false;
  {
    multiplexer::Client client(multiplexer::peers::WEBSITE);
    ASSERT_TRUE(client.wait_for_connection(client.connect("127.0.0.1", multiplexer.port, 5), 0));
    for (int index = 0; index < kLeftBehind; ++index) {
      multiplexer::MultiplexerMessage msg;
      msg.set_id(client.random64());
      msg.set_from(client.instance_id());
      msg.set_type(multiplexer::types::PYTHON_TEST_REQUEST);
      msg.set_message(kKilobyte);
      client.schedule_one(msg);
    }
    if (!client.flush_all(5)) {
      GTEST_SKIP() << "this kernel does not hold " << kLeftBehind << " KiB for a peer that reads nothing";
    }
    unread = multiplexer.heartbeat();  // no loop runs until shutdown()
    client.shutdown();
  }
  ASSERT_TRUE(unread) << "no frame was left unread";
  const LateReader::Result result = multiplexer.result();
  if (!result.seen) {
    GTEST_SKIP() << "no /proc/net/tcp to say when the client left";
  }
  EXPECT_EQ(kLeftBehind, result.counted);
  EXPECT_FALSE(result.reset) << "the client reset the connection";
}

// --- A client destroyed on its own io thread --------------------------------

namespace {

// The threads of this process, from /proc; -1 where /proc does not say.
int thread_count() {
  DIR* tasks = opendir("/proc/self/task");
  if (!tasks) {
    return -1;
  }
  int count = 0;
  while (dirent* entry = readdir(tasks)) {
    if (entry->d_name[0] != '.') {
      ++count;
    }
  }
  closedir(tasks);
  return count;
}

}  // namespace

// The last reference to a client dropped in one of its own callbacks runs
// its destructor on the io thread, which cannot wait for itself: that
// aborted the process ("Resource deadlock avoided", or the debug
// assertion). Now the thread ends on its own once its handlers are done.
TEST(ThreadedClient, DestroyedInItsOwnCallbackItsThreadEndsOnItsOwn) {
  InProcessMultiplexer mx;
  const int before = thread_count();
  auto client = std::make_shared<ThreadedClient>(multiplexer::peers::WEBSITE);
  ASSERT_TRUE(client->connect("127.0.0.1", mx.port, 5));
  std::weak_ptr<ThreadedClient> watch(client);
  std::promise<bool> destroyed_inside;
  client->query(
      "no backend answers this", multiplexer::types::PYTHON_TEST_REQUEST,
      [held = client, watch, &destroyed_inside](const ThreadedClient::Result&) mutable {
        held.reset();  // the last reference: the destructor runs here, on the io thread
        destroyed_inside.set_value(watch.expired());
      },
      5);
  client.reset();
  std::future<bool> inside = destroyed_inside.get_future();
  ASSERT_EQ(std::future_status::ready, inside.wait_for(std::chrono::seconds(10)));
  ASSERT_TRUE(inside.get()) << "something else held the client, so its destructor ran elsewhere";
  if (before < 0) {
    GTEST_SKIP() << "no /proc/self/task to count the threads";
  }
  const std::chrono::steady_clock::time_point deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (thread_count() > before && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  EXPECT_EQ(before, thread_count()) << "the client's io thread did not end";
}

// --- A frame whose handling throws ------------------------------------------
//
// An exception out of the code that handles a frame used to unwind through
// the handler that read it, and the connection was never read again,
// though still registered and writing. The client now logs what threw and
// the connection reads on.

TEST(ThreadedClient, AnOnMessageThatThrowsIsLoggedAndTheConnectionReadsOn) {
  InProcessMultiplexer mx;
  std::atomic<int> seen(0);
  std::promise<void> second;
  ThreadedClient client(multiplexer::peers::WEBSITE, [&seen, &second](const multiplexer::IncomingMessage&) {
    const int count = ++seen;
    if (count == 1) {
      throw std::runtime_error("the first message upsets on_message");
    }
    if (count == 2) {
      second.set_value();
    }
  });
  ASSERT_TRUE(client.connect("127.0.0.1", mx.port, 5));
  Peer peer(mx.port, multiplexer::peers::WEBSITE);
  peer.send(peer.message(multiplexer::types::PYTHON_TEST_REQUEST, "one", client.instance_id()));
  peer.send(peer.message(multiplexer::types::PYTHON_TEST_REQUEST, "two", client.instance_id()));
  EXPECT_EQ(std::future_status::ready, second.get_future().wait_for(std::chrono::seconds(10)))
      << seen.load() << " of 2 reached on_message";
}

// A query's callback that throws: the delivery error that ends the next
// query still arrives, so that query fails at once rather than timing out.
TEST(ThreadedClient, AQueryCallbackThatThrowsLeavesTheConnectionReading) {
  InProcessMultiplexer mx;
  ThreadedClient client(multiplexer::peers::WEBSITE);
  ASSERT_TRUE(client.connect("127.0.0.1", mx.port, 5));
  std::promise<void> thrown;
  client.query(
      "no backend answers this", multiplexer::types::PYTHON_TEST_REQUEST,
      [&thrown](const ThreadedClient::Result&) {
        thrown.set_value();
        throw std::runtime_error("a callback that throws");
      },
      5);
  ASSERT_EQ(std::future_status::ready, thrown.get_future().wait_for(std::chrono::seconds(10)));
  EXPECT_EQ(ThreadedClient::FAILED, client.query("nor this", multiplexer::types::PYTHON_TEST_REQUEST, 5).outcome);
}

// The net under both: whatever the manager's handling of a frame throws,
// here a BasicClient's own sink, the connection logs it, drops that frame
// and reads on.
TEST(Connection, AFrameWhoseHandlingThrowsIsDroppedAndTheConnectionReadsOn) {
  InProcessMultiplexer mx;
  asio::io_service io_service;
  std::shared_ptr<multiplexer::BasicClient> client =
      multiplexer::BasicClient::Create(io_service, multiplexer::peers::WEBSITE);
  int seen = 0;
  client->set_incoming_sink([&seen](const multiplexer::BasicClient::IncomingMessagesBuffer::value_type&) {
    if (++seen == 1) {
      throw std::runtime_error("the first frame upsets the sink");
    }
  });
  ASSERT_TRUE(client->wait_for_connection(client->connect("127.0.0.1", mx.port, 5), 5));
  Peer peer(mx.port, multiplexer::peers::WEBSITE);
  peer.send(peer.message(multiplexer::types::PYTHON_TEST_REQUEST, "one", client->instance_id()));
  peer.send(peer.message(multiplexer::types::PYTHON_TEST_REQUEST, "two", client->instance_id()));
  std::unique_ptr<mx::SimpleTimer> timer = client->create_timer(10);
  while (seen < 2 && !timer->expired()) {
    try {
      client->run_one();
    } catch (const std::exception&) {
      // what the connection let through, before it caught it itself
    }
  }
  EXPECT_EQ(2, seen);
  client->shutdown();
}

// --- Answers that would be over MAX_MESSAGE_SIZE -----------------------------
//
// Frames the libraries and the multiplexer build around a peer's message,
// a PING's echo, a delivery error carrying the original, used to go over
// the limit when that message was near it, and the size check threw inside
// the handler that read the message: that connection went deaf.

namespace {

// A message whose own frame body is exactly at the size limit.
void fill_to_the_limit(multiplexer::MultiplexerMessage* msg) {
  msg->set_message(std::string(multiplexer::MAX_MESSAGE_SIZE - msg->ByteSizeLong() - 16, 'p'));
  while (msg->ByteSizeLong() < multiplexer::MAX_MESSAGE_SIZE) {
    msg->mutable_message()->push_back('p');
  }
}

// The next message `peer` receives that references `id`, or null.
std::shared_ptr<multiplexer::MultiplexerMessage> reply_to(Peer& peer, std::uint64_t id) {
  const std::chrono::steady_clock::time_point deadline = std::chrono::steady_clock::now() + std::chrono::seconds(20);
  while (std::chrono::steady_clock::now() < deadline) {
    try {
      std::shared_ptr<multiplexer::MultiplexerMessage> got = peer.client.receive_message(1).first;
      if (got->references() == id) {
        return got;
      }
    } catch (const multiplexer::Client::OperationTimedOut&) {
    }
  }
  return nullptr;
}

}  // namespace

// A PING is answered with its payload echoed back. One whose echo, a
// `references` field longer, would be over the limit is answered with
// BACKEND_ERROR saying why; the connection goes on, and the next PING has
// its echo.
TEST(ThreadedClient, APingWhoseEchoWouldBeTooBigIsAnsweredWithBackendError) {
  InProcessMultiplexer mx;
  ThreadedClient client(multiplexer::peers::WEBSITE);
  ASSERT_TRUE(client.connect("127.0.0.1", mx.port, 5));
  Peer peer(mx.port, multiplexer::peers::WEBSITE);
  multiplexer::MultiplexerMessage ping = peer.message(multiplexer::types::PING, "", client.instance_id());
  fill_to_the_limit(&ping);
  peer.send(ping);
  std::shared_ptr<multiplexer::MultiplexerMessage> answer = reply_to(peer, ping.id());
  ASSERT_TRUE(answer) << "no answer to the PING";
  EXPECT_EQ(multiplexer::types::BACKEND_ERROR, answer->type());
  EXPECT_NE(std::string::npos, answer->message().find("MAX_MESSAGE_SIZE")) << answer->message();
  multiplexer::MultiplexerMessage small = peer.message(multiplexer::types::PING, "bounce", client.instance_id());
  peer.send(small);
  answer = reply_to(peer, small.id());
  ASSERT_TRUE(answer) << "no echo after the big PING";
  EXPECT_EQ(multiplexer::types::PING, answer->type());
  EXPECT_EQ("bounce", answer->message());
}

// A search addressed to a ThreadedClient, an addressed query locating it,
// is answered with a PING carrying the search back, as a PING is; one
// whose echo would be over the limit gets BACKEND_ERROR saying so.
TEST(ThreadedClient, ASearchIsAnsweredWithItsPayloadEchoed) {
  InProcessMultiplexer mx;
  ThreadedClient client(multiplexer::peers::WEBSITE);
  ASSERT_TRUE(client.connect("127.0.0.1", mx.port, 5));
  Peer peer(mx.port, multiplexer::peers::WEBSITE);
  multiplexer::MultiplexerMessage search =
      peer.message(multiplexer::types::BACKEND_FOR_PACKET_SEARCH, "what the searcher sent", client.instance_id());
  peer.send(search);
  std::shared_ptr<multiplexer::MultiplexerMessage> answer = reply_to(peer, search.id());
  ASSERT_TRUE(answer) << "no answer to the search";
  EXPECT_EQ(multiplexer::types::PING, answer->type());
  EXPECT_EQ("what the searcher sent", answer->message());
  multiplexer::MultiplexerMessage big =
      peer.message(multiplexer::types::BACKEND_FOR_PACKET_SEARCH, "", client.instance_id());
  fill_to_the_limit(&big);
  peer.send(big);
  answer = reply_to(peer, big.id());
  ASSERT_TRUE(answer) << "no answer to the big search";
  EXPECT_EQ(multiplexer::types::BACKEND_ERROR, answer->type());
  EXPECT_NE(std::string::npos, answer->message().find("echo of a search")) << answer->message();
}

// A delivery error that carries the original, for a message near the limit,
// would be over it: the multiplexer leaves the original out and says so,
// and the sender's connection reads on, so the next report comes whole.
TEST(Server, ADeliveryErrorLeavesOutAnOriginalTooBigToCarry) {
  InProcessMultiplexer mx;
  Peer peer(mx.port, multiplexer::peers::WEBSITE);
  const std::uint64_t nobody = 0xdeadbeef;
  multiplexer::MultiplexerMessage big = peer.message(multiplexer::types::PYTHON_TEST_REQUEST, "", nobody);
  big.set_report_delivery_error(true);
  big.set_include_original_packet_in_report(true);
  fill_to_the_limit(&big);
  peer.send(big);
  std::shared_ptr<multiplexer::MultiplexerMessage> report = reply_to(peer, big.id());
  ASSERT_TRUE(report) << "no delivery error for the big message";
  ASSERT_EQ(multiplexer::types::DELIVERY_ERROR, report->type());
  multiplexer::DeliveryError error;
  ASSERT_TRUE(error.ParseFromString(report->message()));
  EXPECT_EQ(big.id(), error.packet_id());
  EXPECT_FALSE(error.has_original_message());
  EXPECT_TRUE(error.original_message_omitted());
  multiplexer::MultiplexerMessage small = peer.message(multiplexer::types::PYTHON_TEST_REQUEST, "small", nobody);
  small.set_report_delivery_error(true);
  small.set_include_original_packet_in_report(true);
  peer.send(small);
  report = reply_to(peer, small.id());
  ASSERT_TRUE(report) << "no delivery error for the small message after the big one";
  ASSERT_TRUE(error.ParseFromString(report->message()));
  EXPECT_EQ("small", error.original_message().message());
  EXPECT_FALSE(error.original_message_omitted());
}

// A request the caller's check let through and the io thread's framing
// then took over the limit used to throw on the io thread, and the query
// never ended. The check at the call measures the request as it will be
// framed, so the caller gets the error.
TEST(ThreadedClient, AQueryTooBigOnceFramedIsRefusedAtTheCall) {
  ThreadedClient client(multiplexer::peers::WEBSITE);
  multiplexer::MultiplexerMessage request;
  request.set_type(multiplexer::types::PYTHON_TEST_REQUEST);
  fill_to_the_limit(&request);  // at the limit without its id and `from`
  std::promise<void> ended;
  EXPECT_THROW(
      client.query(request, [&ended](const ThreadedClient::Result&) { ended.set_value(); }, 1), std::length_error);
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
