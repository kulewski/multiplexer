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

#include <algorithm>
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
#include <limits>
#include <memory>
#include <mutex>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

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

// With no multiplexer up, the queries that ended at their deadlines are
// not kept: the client holds those still waiting, and 64 more at most,
// where every one stayed, its whole request with it, until a connection
// came up, and a long outage grew without bound. One query waits a second,
// so that the list is cleared out beside it, and then none waits. Counted
// once each has ended, as its callback says.
TEST(ThreadedClient, AnOutageKeepsNoQueryThatEnded) {
  const int count = 200;
  std::promise<void> all_ended;  // before the client, whose callbacks set them
  std::promise<void> last_ended;
  std::atomic<int> ended(0);
  ThreadedClient client(multiplexer::peers::WEBSITE);
  EXPECT_FALSE(client.connect("127.0.0.1", 1, 0.2f));  // nothing listens on port 1
  client.query(
      "waits", multiplexer::types::PYTHON_TEST_REQUEST,
      [&last_ended](const ThreadedClient::Result&) { last_ended.set_value(); }, 1.0f);
  for (int index = 0; index < count; ++index) {
    client.query(
        "hello", multiplexer::types::PYTHON_TEST_REQUEST,
        [&](const ThreadedClient::Result&) {
          if (++ended == count) {
            all_ended.set_value();
          }
        },
        0.05f);
  }
  ASSERT_EQ(std::future_status::ready, all_ended.get_future().wait_for(std::chrono::seconds(30)));
  EXPECT_LE(client.waiting_queries(), 1u + 64u) << "queries that ended beside one that waits";
  ASSERT_EQ(std::future_status::ready, last_ended.get_future().wait_for(std::chrono::seconds(30)));
  EXPECT_EQ(0u, client.waiting_queries()) << "queries that ended";
}

// With no multiplexer up, the messages that ended at their deadlines are
// not kept: the outbox holds those still waiting, and 64 more at most,
// where every one stayed, its whole frame with it, until a connection came
// up. One message waits a second, a flushing send on a thread of its own,
// so that the queue is cleared out beside it, and then none waits. Counted
// once the drops are reported.
TEST(ThreadedClient, AnOutageKeepsNoMessageThatEnded) {
  const int count = 200;
  std::promise<void> all_dropped;  // before the client, whose observer sets them
  std::promise<void> last_dropped;
  std::atomic<int> dropped(0);
  ThreadedClient client(multiplexer::peers::WEBSITE);
  client.set_drop_observer([&](std::uint64_t, multiplexer::DropReason) {
    const int now = ++dropped;
    if (now == count) {
      all_dropped.set_value();
    } else if (now == count + 1) {
      last_dropped.set_value();
    }
  });
  EXPECT_FALSE(client.connect("127.0.0.1", 1, 0.2f));  // nothing listens on port 1
  std::thread waiting(
      [&client] { client.send(client.new_message(multiplexer::types::PYTHON_TEST_REQUEST, "waits"), 1.0f); });
  for (int tries = 0; tries < 1000 && client.waiting_messages() == 0; ++tries) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  ASSERT_EQ(1u, client.waiting_messages()) << "the message that waits";
  const std::string payload(1024, 'x');
  for (int index = 0; index < count; ++index) {
    client.send(client.new_message(multiplexer::types::PYTHON_TEST_REQUEST, payload), 0.001f);
  }
  ASSERT_EQ(std::future_status::ready, all_dropped.get_future().wait_for(std::chrono::seconds(30)));
  EXPECT_LE(client.waiting_messages(), 1u + 64u) << "messages that ended beside one that waits";
  waiting.join();
  ASSERT_EQ(std::future_status::ready, last_dropped.get_future().wait_for(std::chrono::seconds(30)));
  EXPECT_EQ(0u, client.waiting_messages()) << "messages that ended";
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

// A whole message sent without an id or a sender gets both, through every
// send that takes one, ThreadedClient's and SyncClient's, as new_message()
// and a reply fill them: every receiver dropped it for its id 0. Ordered,
// not timed: each sender's marker comes behind what it sent before.
TEST(ThreadedClient, AWholeMessageSentWithoutIdOrSenderGetsBoth) {
  InProcessMultiplexer mx;
  std::mutex mutex;
  std::condition_variable arrived;
  std::vector<multiplexer::MultiplexerMessage> got;
  ThreadedClient receiver(multiplexer::peers::PYTHON_TEST_SERVER, [&](const multiplexer::IncomingMessage& incoming) {
    std::lock_guard<std::mutex> lock(mutex);
    got.push_back(*incoming.third);
    arrived.notify_all();
  });
  ASSERT_TRUE(receiver.connect("127.0.0.1", mx.port, 5));
  ThreadedClient threaded(multiplexer::peers::WEBSITE);
  ASSERT_TRUE(threaded.connect("127.0.0.1", mx.port, 5));
  Peer sync(mx.port, multiplexer::peers::WEBSITE);
  const auto bare = [&receiver](const std::string& payload) {
    multiplexer::MultiplexerMessage msg;
    msg.set_type(multiplexer::types::PYTHON_TEST_REQUEST);
    msg.set_message(payload);
    msg.set_to(receiver.instance_id());
    return msg;
  };
  multiplexer::MultiplexerMessage marker = threaded.new_message(multiplexer::types::PYTHON_TEST_REQUEST, "marker");
  marker.set_to(receiver.instance_id());
  threaded.send(bare("queued"));
  ASSERT_EQ(1u, threaded.send(bare("flushed"), 5.0f));
  threaded.send(marker);
  sync.send(bare("sync"));
  sync.send(sync.message(multiplexer::types::PYTHON_TEST_REQUEST, "marker", receiver.instance_id()));
  std::unique_lock<std::mutex> lock(mutex);
  ASSERT_TRUE(arrived.wait_for(lock, std::chrono::seconds(10), [&] {
    return std::count_if(got.begin(), got.end(),
                         [](const multiplexer::MultiplexerMessage& msg) { return msg.message() == "marker"; }) == 2;
  }));
  for (const std::string payload : {"queued", "flushed", "sync"}) {
    const auto found = std::find_if(
        got.begin(), got.end(), [&](const multiplexer::MultiplexerMessage& msg) { return msg.message() == payload; });
    ASSERT_NE(got.end(), found) << payload << " never arrived";
    EXPECT_NE(0u, found->id()) << payload;
    EXPECT_EQ(payload == "sync" ? sync.client.instance_id() : threaded.instance_id(), found->from()) << payload;
  }
}

// One message queried twice is answered twice: every attempt goes out
// under an id of its own, ids belonging to attempts, so that the
// backend's library, which drops a repeated id, reads both requests, where
// the synchronous client sent the request under the message's own id and
// the second was dropped as a repeat. Each client asks from a thread of
// its own, the backend answers from this one.
TEST(SyncClient, OneMessageQueriedTwiceIsAnsweredTwice) {
  InProcessMultiplexer mx;
  Peer backend(mx.port, multiplexer::peers::PYTHON_TEST_SERVER);
  const unsigned short port = mx.port;
  std::future<std::string> asked = std::async(std::launch::async, [port] {
    std::string answers;
    multiplexer::Client sync(multiplexer::peers::WEBSITE);
    sync.connect("127.0.0.1", port, 5);
    ThreadedClient threaded(multiplexer::peers::WEBSITE);
    threaded.connect("127.0.0.1", port, 5);
    multiplexer::MultiplexerMessage question;
    question.set_id(sync.random64());
    question.set_type(multiplexer::types::PYTHON_TEST_REQUEST);
    question.set_message("question");
    try {
      answers += sync.query(question, 30).third->message();
      answers += sync.query(question, 30).third->message();
    } catch (const std::exception& error) {
      answers += std::string(" the synchronous query raised ") + error.what();
    }
    try {
      answers += threaded.query(question, 30).check().third->message();
      answers += threaded.query(question, 30).check().third->message();
    } catch (const std::exception& error) {
      answers += std::string(" the threaded query raised ") + error.what();
    }
    return answers;
  });
  std::string answers;
  for (int index = 0; index < 4; ++index) {
    multiplexer::IncomingMessage request = backend.client.read_raw_message(60);
    backend.send(backend.message(multiplexer::types::PYTHON_TEST_RESPONSE, std::to_string(index), request.third->from(),
                                 request.third->id()));
  }
  EXPECT_EQ("0123", asked.get());
}

// A typed query of a whole message with neither id nor sender: the request
// goes out with both filled in, as every send fills them, and the reply to
// it is the answer, where the query recorded the id 0, so that the reply to
// the id the frame was given went unread. The client asks from a thread of
// its own, the backend answers from this one.
TEST(SyncClient, AQueryOfAMessageWithoutAnIdIsAnswered) {
  InProcessMultiplexer mx;
  Peer backend(mx.port, multiplexer::peers::PYTHON_TEST_SERVER);
  const unsigned short port = mx.port;
  std::future<std::string> asked = std::async(std::launch::async, [port] {
    multiplexer::Client client(multiplexer::peers::WEBSITE);
    client.connect("127.0.0.1", port, 5);
    multiplexer::MultiplexerMessage bare;
    bare.set_type(multiplexer::types::PYTHON_TEST_REQUEST);
    bare.set_message("question");
    try {
      return client.query(bare, 30).third->message();
    } catch (const std::exception& error) {
      return std::string("the query raised ") + error.what();
    }
  });
  multiplexer::IncomingMessage request = backend.client.read_raw_message(30);
  backend.send(
      backend.message(multiplexer::types::PYTHON_TEST_RESPONSE, "answer", request.third->from(), request.third->id()));
  EXPECT_EQ("answer", asked.get());
}

// After shutdown() a synchronous client connects to nothing and places
// nothing, as ThreadedClient refuses: connect() opened and registered a
// connection every later send was refused on, schedule_one() and
// schedule_all() placed messages on it, and it stayed registered until
// the client was destroyed.
TEST(SyncClient, NothingConnectsOrIsPlacedAfterShutdown) {
  InProcessMultiplexer mx;
  multiplexer::Client client(multiplexer::peers::WEBSITE);
  ASSERT_TRUE(client.connect("127.0.0.1", mx.port, 5));
  client.shutdown();
  EXPECT_THROW(client.connect("127.0.0.1", mx.port, 5), multiplexer::Client::NotConnected);
  EXPECT_THROW(client.async_connect("127.0.0.1", mx.port), multiplexer::Client::NotConnected);
  multiplexer::MultiplexerMessage msg;
  msg.set_id(client.random64());
  msg.set_from(client.instance_id());
  msg.set_type(multiplexer::types::PYTHON_TEST_REQUEST);
  msg.set_message("after");
  EXPECT_FALSE(client.schedule_one(msg));
  EXPECT_EQ(0u, client.schedule_all(msg));
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

// A late reply is recognised by the age of its query, not by how many
// queries ended since: after more than a thousand ids of failed queries, a
// second reply to the first query, answered long before, is still
// dropped, where the ring of the last 1024 had forgotten it and handed it
// on, which a threaded server queues as a request. Ordered by events, no
// timer: an event after it says it was read.
TEST(ThreadedClient, ALateReplyIsDroppedAfterAThousandMoreQueries) {
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
  // The backend answers the first query.
  std::promise<ThreadedClient::Result> done;
  client.query(
      "first", multiplexer::types::PYTHON_TEST_REQUEST, [&done](const ThreadedClient::Result& r) { done.set_value(r); },
      30);
  multiplexer::IncomingMessage request = backend.client.read_raw_message(30);
  ASSERT_EQ("first", request.third->message());
  backend.send(
      backend.message(multiplexer::types::PYTHON_TEST_RESPONSE, "FIRST", client.instance_id(), request.third->id()));
  EXPECT_EQ(ThreadedClient::REPLIED, done.get_future().get().outcome);
  // Six hundred queries nobody serves fail, each leaving two finished ids,
  // the request's and the search's: more than the 1024 the ring held.
  for (int index = 0; index < 600; ++index) {
    EXPECT_EQ(ThreadedClient::FAILED, client.query("filler", multiplexer::types::TEST_REQUEST_A, 5).outcome);
  }
  backend.send(
      backend.message(multiplexer::types::PYTHON_TEST_RESPONSE, "again", client.instance_id(), request.third->id()));
  backend.send(backend.message(multiplexer::types::PYTHON_TEST_REQUEST, "event", client.instance_id()));
  std::future<multiplexer::MultiplexerMessage> future = event.get_future();
  ASSERT_EQ(std::future_status::ready, future.wait_for(std::chrono::seconds(30)));
  EXPECT_EQ("event", future.get().message());
  EXPECT_EQ(1, handed_on) << "the second reply was handed on";
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
  // A backend that reads nothing and answers nothing: the query waits for
  // its deadline, or for shutdown(). With no backend, the multiplexer's
  // delivery error could end it first.
  Peer backend(mx.port, multiplexer::peers::PYTHON_TEST_SERVER);
  ThreadedClient::Outcome seen = ThreadedClient::REPLIED;
  client.query(
      "x", multiplexer::types::PYTHON_TEST_REQUEST, [&](const ThreadedClient::Result& r) { seen = r.outcome; }, 30);
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

namespace {

// What a call racing shutdown() did with its callback, and where it stands:
// a Gated callback stops the caller's thread at its `at`-th copy made
// there, until `released`. Copies on other threads pass.
struct Gate {
  std::mutex mutex;
  std::condition_variable changed;
  std::thread::id caller;
  int at = 0;
  int copies = 0;
  bool standing = false;
  bool released = false;
  bool returned = false;  // the call returned, or threw
  int ends = 0;           // callbacks called, and NotConnected thrown
};

struct Gated {
  std::shared_ptr<Gate> gate;
  explicit Gated(std::shared_ptr<Gate> shared) : gate(std::move(shared)) {}
  Gated(Gated&&) = default;
  Gated(const Gated& other) : gate(other.gate) {
    std::unique_lock<std::mutex> lock(gate->mutex);
    if (std::this_thread::get_id() != gate->caller || ++gate->copies != gate->at) {
      return;
    }
    gate->standing = true;
    gate->changed.notify_all();
    gate->changed.wait(lock, [this] { return gate->released; });
  }
  template <typename... Args>
  void operator()(Args&&...) const {
    std::lock_guard<std::mutex> lock(gate->mutex);
    ++gate->ends;
  }
};

// Runs `call(client, callback)` on a thread of its own once for each copy
// of the callback it makes: stopped at that copy while shutdown() runs to
// its end, and let go after. The call must end once, its callback called
// or NotConnected thrown, whichever copy it stood at. A call that posted
// after its check of stopped_, the lock let go, never ended: the loop it
// posted to had ended, and its callback, or its future, waited for good.
template <typename Callback, typename Call>
void CheckEveryCopyRacingShutdown(Call call) {
  for (int at = 1;; ++at) {
    ThreadedClient client(multiplexer::peers::WEBSITE);
    std::shared_ptr<Gate> gate = std::make_shared<Gate>();
    gate->at = at;
    Callback callback = Gated(gate);
    std::thread caller([&] {
      {
        std::lock_guard<std::mutex> lock(gate->mutex);
        gate->caller = std::this_thread::get_id();
      }
      bool refused = false;
      try {
        call(client, callback);
      } catch (const ThreadedClient::NotConnected&) {
        refused = true;
      }
      std::lock_guard<std::mutex> lock(gate->mutex);
      gate->ends += refused ? 1 : 0;
      gate->returned = true;
      gate->changed.notify_all();
    });
    bool stood;
    {
      std::unique_lock<std::mutex> lock(gate->mutex);
      gate->changed.wait(lock, [&] { return gate->standing || gate->returned; });
      stood = gate->standing;
    }
    client.shutdown(0);  // to its end: the io thread is gone
    {
      std::lock_guard<std::mutex> lock(gate->mutex);
      gate->released = true;
      gate->changed.notify_all();
    }
    caller.join();
    std::lock_guard<std::mutex> lock(gate->mutex);
    EXPECT_EQ(1, gate->ends) << "standing at copy " << at << " of the callback";
    if (!stood) {
      return;  // every copy the call makes has had its turn
    }
  }
}

}  // namespace

// A call that races shutdown() is answered or refused, never left waiting
// on a loop that ended, wherever it stands while the shutdown runs.
TEST(ThreadedClient, ACallRacingShutdownEndsOnce) {
  CheckEveryCopyRacingShutdown<ThreadedClient::Callback>([](ThreadedClient& client, ThreadedClient::Callback& done) {
    client.query("x", multiplexer::types::PYTHON_TEST_REQUEST, done, 30);
  });
  CheckEveryCopyRacingShutdown<ThreadedClient::FlushCallback>(
      [](ThreadedClient& client, ThreadedClient::FlushCallback& done) { client.flush_all_with_callback(30, done); });
  CheckEveryCopyRacingShutdown<ThreadedClient::SendCallback>(
      [](ThreadedClient& client, ThreadedClient::SendCallback& done) {
        client.send_all(client.new_message(multiplexer::types::PYTHON_TEST_REQUEST, "x"), done);
      });
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
  // query's callback, which must find the promise alive.
  std::promise<ThreadedClient::Result> done;
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
  // The send that does not wait refuses at the call, as in Python, where it
  // returned and dropped the message with a warning.
  EXPECT_THROW(client.send(client.new_message(multiplexer::types::PYTHON_TEST_REQUEST, "refused"), pinned),
               ThreadedClient::NotConnected);
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

// The multiplexer that carried a typed query's request goes away under
// the wait: the request may have been routed first, so it is not sent
// again. The query moves on to the search at once, through the other
// multiplexer, and the request goes out a second and last time, to the
// backend that answered, whose reply ends the query; the lane follows.
// Where the request went out again at once, the backend's next message
// was the request, not the search. Counted: a query timeout far longer
// than the backend's reads, so a loss noticed only at the timeout fails.
TEST(ThreadedClient, ATypedQueryWhoseMultiplexerGoesSearchesInsteadOfSendingAgain) {
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
  // goes away under the wait.
  client.query(
      client.new_message(multiplexer::types::PYTHON_TEST_REQUEST, "slow"),
      [&done](const ThreadedClient::Result& r) { done.set_value(r); }, 60, lane);
  multiplexer::IncomingMessage held = backend.client.read_raw_message(5);
  EXPECT_EQ(way, held.second.endpoint().port());
  (first_way ? first : second).reset();
  multiplexer::IncomingMessage next = backend.client.read_raw_message(30);
  ASSERT_EQ(multiplexer::types::BACKEND_FOR_PACKET_SEARCH, next.third->type()) << "the request went out again";
  backend.send(backend.message(multiplexer::types::PING, next.third->message(), next.third->from(), next.third->id()));
  multiplexer::IncomingMessage direct = backend.client.read_raw_message(30);
  ASSERT_EQ(multiplexer::types::PYTHON_TEST_REQUEST, direct.third->type());
  EXPECT_EQ(backend.client.instance_id(), direct.third->to()) << "the second time out, to the backend found";
  backend.client.flush(backend.client.schedule_one(backend.message(multiplexer::types::PYTHON_TEST_RESPONSE, "SLOW",
                                                                   direct.third->from(), direct.third->id()),
                                                   direct.second),
                       5);
  std::future<ThreadedClient::Result> answered = done.get_future();
  ASSERT_EQ(std::future_status::ready, answered.wait_for(std::chrono::seconds(10)));
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
// closes, which kept the io thread alive, the peer registered and
// shutdown() waiting for good: it returns, a hang failing at the test's
// own timeout rather than at a bound on how long it took. That shutdown()
// takes every reconnect out is ShutdownTakesOutTheReconnectArmed below,
// on the synchronous client, whose loop runs only inside its calls; both
// clients' BasicClient does it.
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
  client.shutdown();
}

// A target whose connection failed has a reconnect armed, its timer 3 s
// away; shutdown() cancels it and takes it out, so that nothing is left to
// open a connection after the shutdown, or to keep a threaded client's io
// thread waiting for it to fire. Counted, not timed: the synchronous
// client's loop runs only inside its calls, so the reconnect cannot fire
// before the count.
TEST(SyncClient, ShutdownTakesOutTheReconnectArmed) {
  unsigned short gone_port = 0;
  {
    InProcessMultiplexer gone;
    gone_port = gone.port;
  }
  multiplexer::Client client(multiplexer::peers::WEBSITE);
  EXPECT_FALSE(client.wait_for_connection(client.connect("127.0.0.1", gone_port, 5), 0)) << "refused";
  ASSERT_EQ(1u, client.reconnects_pending()) << "no reconnect armed";
  client.shutdown();
  EXPECT_EQ(0u, client.reconnects_pending()) << "shutdown() left a reconnect armed";
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

// A backend that counts what it is handed and keeps the short payloads,
// for a test that asks whether one message in particular arrived, after a
// later one that must come behind it.
struct PayloadSink {
  explicit PayloadSink(unsigned short port)
      : client(multiplexer::peers::PYTHON_TEST_SERVER, [this](const multiplexer::IncomingMessage& incoming) {
          std::lock_guard<std::mutex> lock(mutex);
          ++count;
          if (incoming.third->message().size() < 256) {
            payloads.push_back(incoming.third->message());
          }
          arrived.notify_all();
        }) {
    client.connect("127.0.0.1", port, 5);
  }
  // Whether a message with `payload` arrived within `seconds`.
  bool wait_for(const std::string& payload, int seconds) {
    std::unique_lock<std::mutex> lock(mutex);
    return arrived.wait_for(lock, std::chrono::seconds(seconds), [&] { return _has(payload); });
  }
  bool has(const std::string& payload) {
    std::lock_guard<std::mutex> lock(mutex);
    return _has(payload);
  }
  std::size_t received() {
    std::lock_guard<std::mutex> lock(mutex);
    return count;
  }
  bool _has(const std::string& payload) const {
    return std::find(payloads.begin(), payloads.end(), payload) != payloads.end();
  }
  // Before the client, which writes into them until it is gone.
  std::mutex mutex;
  std::condition_variable arrived;
  std::size_t count = 0;
  std::vector<std::string> payloads;
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

// What the two sockets of a connection may buffer at most: the largest the
// kernel allows each.
std::size_t socket_bytes() {
  std::size_t buffers = 0;
  for (const char* path : {"/proc/sys/net/ipv4/tcp_wmem", "/proc/sys/net/ipv4/tcp_rmem"}) {
    std::ifstream limits(path);
    std::size_t least = 0, initial = 0, largest = 8 << 20;  // a guess where /proc does not say
    limits >> least >> initial >> largest;
    buffers += largest;
  }
  return buffers;
}

// How many messages of `size` bytes a frozen multiplexer's connection
// cannot take: twice what the two sockets may buffer and twice the queue.
// Past that, messages wait.
int frames_to_fill(std::size_t size) { return static_cast<int>(2 * socket_bytes() / size) + 2 * 1024; }

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

// A send made on the io thread while shutdown(timeout) writes out what was
// sent before, as a server's refusal of what still arrives is, goes out:
// it threw NotConnected, so the refusal was lost and its sender waited out
// its timeout. The first multiplexer, frozen with copies of a send to ALL
// queued for it, holds the write-out open; a request arrives through the
// second meanwhile and is answered.
TEST(ThreadedClient, ASendFromTheIoThreadDuringTheShutdownsWriteOutGoesOut) {
  InProcessMultiplexer first;
  InProcessMultiplexer second;
  ThreadedClient* answering_client = nullptr;
  ThreadedClient answering(multiplexer::peers::PYTHON_TEST_SERVER,
                           [&answering_client](const multiplexer::IncomingMessage& incoming) {
                             if (incoming.third->type() != multiplexer::types::PYTHON_TEST_REQUEST) {
                               return;  // the multiplexers' word on the filler
                             }
                             multiplexer::MultiplexerMessage reply =
                                 answering_client->new_message(multiplexer::types::PYTHON_TEST_RESPONSE, "late");
                             reply.set_to(incoming.third->from());
                             reply.set_references(incoming.third->id());
                             answering_client->send(reply, incoming.second);
                           });
  answering_client = &answering;
  ASSERT_TRUE(answering.connect("127.0.0.1", first.port, 5));
  ASSERT_TRUE(answering.connect("127.0.0.1", second.port, 5));
  Peer requester(second.port, multiplexer::peers::WEBSITE);
  std::thread closing;
  {
    Freeze frozen(first);
    const std::string filler(1024, 'f');
    for (int index = 0; index < frames_to_fill(filler.size()); ++index) {
      multiplexer::MultiplexerMessage msg = answering.new_message(9999, filler);  // a type nobody takes
      msg.set_report_delivery_error(false);
      answering.send_all(msg);
    }
    closing = std::thread([&answering] { answering.shutdown(10); });
    for (bool stopped = false; !stopped;) {  // until the shutdown has begun
      try {
        answering.connections_count();
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
      } catch (const ThreadedClient::NotConnected&) {
        stopped = true;
      }
    }
    multiplexer::MultiplexerMessage request =
        requester.message(multiplexer::types::PYTHON_TEST_REQUEST, "still there?", answering.instance_id());
    requester.send(request);
    try {
      const multiplexer::IncomingMessage got = requester.client.read_raw_message(5);
      EXPECT_EQ(request.id(), got.third->references());
      EXPECT_EQ("late", got.third->message());
    } catch (const multiplexer::Client::OperationTimedOut&) {
      ADD_FAILURE() << "no answer: the send threw NotConnected on the io thread";
    }
  }  // the first multiplexer thawed: the write-out ends
  closing.join();
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
// done, and it is once the client reconnected and wrote the message, which
// a backend on the restarted multiplexer receives. The client connects by
// a name that resolves only while the multiplexer is to be found, so that
// it reconnects once that backend is there to take the message.
TEST(ThreadedClient, FlushAllWaitsForAMessageThatFoundNoConnection) {
  std::unique_ptr<InProcessMultiplexer> mx(new InProcessMultiplexer());
  const unsigned short port = mx->port;
  std::atomic<bool> found(true);
  ThreadedClient client(multiplexer::peers::WEBSITE);
  client.set_resolver([&found, port](const std::string&, std::uint16_t, asio::error_code& error) {
    std::vector<asio::ip::tcp::endpoint> addresses;
    if (found.load()) {
      addresses.emplace_back(asio::ip::make_address("127.0.0.1"), port);
    } else {
      error = asio::error::host_not_found;
    }
    return addresses;
  });
  ASSERT_TRUE(client.connect("mx", 1980, 5));
  found.store(false);
  mx.reset();
  std::chrono::steady_clock::time_point deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (client.connections_count() != 0 && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  ASSERT_EQ(0u, client.connections_count());
  client.send(client.new_message(multiplexer::types::PYTHON_TEST_REQUEST, "waits"));
  EXPECT_FALSE(client.flush_all(0.5f)) << "the message waits for a connection";
  mx.reset(new InProcessMultiplexer(port));
  PayloadSink backend(port);
  found.store(true);
  EXPECT_TRUE(client.flush_all(15)) << "written once the client reconnected";
  EXPECT_TRUE(backend.wait_for("waits", 30)) << "and it arrived";
  EXPECT_EQ(1u, backend.received());
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

// A read with no deadline on a client with nothing it could receive from,
// no connection and none on its way, throws NotConnected at once, where it
// spun at full CPU forever: one never connected, and one shut down. Each
// reads on a thread of its own, let go of if it never ends, so that a spin
// fails the test instead of hanging it.
TEST(Client, AReadWithNothingToReceiveFromThrowsNotConnected) {
  InProcessMultiplexer mx;
  for (const bool shut_down : {false, true}) {
    std::promise<std::string> ended;
    std::future<std::string> outcome = ended.get_future();
    std::thread reader([&ended, shut_down, port = mx.port] {
      multiplexer::Client client(multiplexer::peers::WEBSITE);
      if (shut_down) {
        client.connect("127.0.0.1", port, 5);
        client.shutdown();
      }
      try {
        client.read_raw_message(-1);
        ended.set_value("a message");
      } catch (const multiplexer::Client::NotConnected&) {
        ended.set_value("NotConnected");
      } catch (const std::exception& error) {
        ended.set_value(error.what());
      }
    });
    const char* what = shut_down ? "shut down" : "never connected";
    if (outcome.wait_for(std::chrono::seconds(10)) != std::future_status::ready) {
      reader.detach();  // spinning for good
      ADD_FAILURE() << what << ": the read never ended";
      continue;
    }
    reader.join();
    EXPECT_EQ("NotConnected", outcome.get()) << what;
  }
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
// The message waits for room as long as the call and a moment more, so
// that a call that timed out has its message dropped soon after, where it
// waited 10 s whatever the call's timeout and went out after the call had
// thrown.
TEST(Client, ALaneWaitsForRoomOnItsConnectionWhileItLives) {
  InProcessMultiplexer first, second;
  PayloadSink behind_first(first.port), behind_second(second.port);
  Peer sender(first.port, multiplexer::peers::WEBSITE);
  sender.client.connect("127.0.0.1", second.port, 5);
  multiplexer::LanePtr lane(new multiplexer::Lane());
  sender.client.send(sender.message(multiplexer::types::PYTHON_TEST_REQUEST, "first", 0), 5, lane);
  const unsigned short held = lane->connection().endpoint().port();
  InProcessMultiplexer& frozen = held == first.port ? first : second;
  PayloadSink& its = held == first.port ? behind_first : behind_second;
  PayloadSink& other = held == first.port ? behind_second : behind_first;
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
    // Past the message's own deadline, the loop sees it while the
    // multiplexer is still frozen, before any room could take it.
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    sender.client.flush_all(0.01f);
  }
  EXPECT_TRUE(sender.client.flush_all(60));
  sender.client.send(sender.message(multiplexer::types::PYTHON_TEST_REQUEST, "after", 0), 30, lane);
  ASSERT_TRUE(its.wait_for("after", 60)) << its.received() << " arrived";
  EXPECT_FALSE(its.has("last")) << "it went out after its call had timed out";
  EXPECT_EQ(static_cast<std::size_t>(count) + 2, its.received()) << "the first, the chunks and the one after";
  EXPECT_EQ(0u, other.received()) << "went the other way";
}

// The same without a lane: a synchronous send that timed out has its
// message dropped soon after, where it waited 10 s and went out once the
// multiplexer read again.
TEST(Client, ASendThatTimedOutHasItsMessageDropped) {
  InProcessMultiplexer mx;
  PayloadSink backend(mx.port);
  Peer sender(mx.port, multiplexer::peers::WEBSITE);
  const std::string chunk(16 * 1024, 'x');
  {
    Freeze freeze(mx);
    for (int index = 0; index < frames_to_fill(chunk.size()); ++index) {
      sender.client.schedule_one(sender.message(multiplexer::types::TEST_UNROUTED, chunk, 0));
    }
    EXPECT_THROW(sender.client.send(sender.message(multiplexer::types::PYTHON_TEST_REQUEST, "late", 0), 0.2f),
                 multiplexer::Client::OperationTimedOut);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    sender.client.flush_all(0.01f);  // the loop sees its deadline while the multiplexer is frozen
  }
  sender.client.send(sender.message(multiplexer::types::PYTHON_TEST_REQUEST, "after", 0), 30);
  ASSERT_TRUE(backend.wait_for("after", 60));
  EXPECT_FALSE(backend.has("late")) << "it went out after its call had timed out";
}

// A pinned lane whose send runs out of time while its connection lives
// times out, as through any lane and as on ThreadedClient, where it threw
// NotConnected, the lane not closed.
TEST(Client, APinnedLaneThatRunsOutOfTimeTimesOut) {
  InProcessMultiplexer mx;
  Sink backend(mx.port);
  Peer sender(mx.port, multiplexer::peers::WEBSITE);
  multiplexer::LanePtr lane(new multiplexer::Lane(true));
  sender.client.send(sender.message(multiplexer::types::PYTHON_TEST_REQUEST, "first", 0), 5, lane);
  const std::string chunk(16 * 1024, 'x');
  Freeze freeze(mx);
  for (int index = 0; index < frames_to_fill(chunk.size()); ++index) {
    sender.client.schedule_one(sender.message(multiplexer::types::TEST_UNROUTED, chunk, 0), lane->connection());
  }
  EXPECT_THROW(sender.client.send(sender.message(multiplexer::types::PYTHON_TEST_REQUEST, "late", 0), 0.2f, lane),
               multiplexer::Client::OperationTimedOut);
  EXPECT_FALSE(lane->closed());
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

// A multiplexer of the test's own for one connection, where a real one
// accepts only the peer types in its rules: it takes the client's welcome,
// answers with its own, so that the client's connect returns, and says
// which peer type the client announced.
class WelcomeReader {
 public:
  WelcomeReader() {
    listener_ = ::socket(AF_INET, SOCK_STREAM, 0);
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
    reading_ = std::async(std::launch::async, [this] { return _read(); });
  }
  ~WelcomeReader() {
    ::shutdown(listener_, SHUT_RDWR);  // an accept still waiting returns
    if (reading_.valid()) {
      reading_.wait();
    }
    ::close(listener_);
  }

  // The peer type the client's welcome announced, once read; 0 when none
  // was.
  std::uint32_t announced() { return reading_.get(); }

  unsigned short port = 0;

 private:
  std::uint32_t _read() {
    const int fd = ::accept(listener_, nullptr, nullptr);
    if (fd < 0) {
      return 0;
    }
    timeval patience = {30, 0};  // a welcome that never comes fails the test rather than hangs it
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &patience, sizeof patience);
    std::uint32_t type = 0;
    multiplexer::RawMessage frame;
    multiplexer::MultiplexerMessage envelope;
    multiplexer::WelcomeMessage welcome;
    if (!read_frame(fd, &frame) && envelope.ParseFromString(frame.get_message()) &&
        welcome.ParseFromString(envelope.message()) &&
        !write_frame(fd, *multiplexer::impl::create_welcome_message(multiplexer::peers::MULTIPLEXER, 0x6d78))) {
      type = welcome.type();
    }
    ::close(fd);
    return type;
  }

  int listener_;
  std::future<std::uint32_t> reading_;
};

// A multiplexer of the test's own that takes the client's welcome, answers
// with its own, and reads nothing after, on a socket that buffers next to
// nothing, so that what the client sends waits in the client; reset() ends
// the connection with all that unread.
class DeafAfterWelcome {
 public:
  DeafAfterWelcome() {
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
    welcomed_ = std::async(std::launch::async, [this] { return _welcome(); });
  }
  ~DeafAfterWelcome() {
    ::shutdown(listener_, SHUT_RDWR);  // an accept still waiting returns
    if (welcomed_.valid()) {
      welcomed_.wait();
    }
    reset();
    ::close(listener_);
  }

  // Waits for the welcomes to be exchanged: false when they were not.
  bool welcomed() { return welcomed_.get(); }

  // Ends the connection with RST, what the client sent since unread.
  void reset() {
    if (fd_ >= 0) {
      const linger now = {1, 0};
      ::setsockopt(fd_, SOL_SOCKET, SO_LINGER, &now, sizeof now);
      ::close(fd_);
      fd_ = -1;
    }
  }

  unsigned short port = 0;

 private:
  bool _welcome() {
    fd_ = ::accept(listener_, nullptr, nullptr);
    if (fd_ < 0) {
      return false;
    }
    timeval patience = {30, 0};  // a welcome that never comes fails the test rather than hangs it
    ::setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &patience, sizeof patience);
    multiplexer::RawMessage welcome;  // the client's
    return !read_frame(fd_, &welcome) &&
           !write_frame(fd_, *multiplexer::impl::create_welcome_message(multiplexer::peers::MULTIPLEXER, 0x6d78));
  }

  int listener_;
  int fd_ = -1;
  std::future<bool> welcomed_;
};

// A message as a multiplexer routes it to a peer: of `type`, answering
// `references` unless that is 0.
multiplexer::MultiplexerMessage routed(std::uint32_t type, std::uint64_t references = 0) {
  static std::uint64_t ids = 0;
  multiplexer::MultiplexerMessage message;
  message.set_id(++ids);
  message.set_from(0x6d78);
  message.set_type(type);
  message.set_references(references);
  message.set_message("late");
  return message;
}

// A multiplexer of the test's own that, once the client has closed its
// side, sends `late` and then closes its own: what a multiplexer routes to
// a peer in the moment the peer leaves.
class SendsAfterTheClose {
 public:
  explicit SendsAfterTheClose(std::vector<multiplexer::MultiplexerMessage> late) : late_(std::move(late)) {
    listener_ = ::socket(AF_INET, SOCK_STREAM, 0);
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
    sent_ = std::async(std::launch::async, [this] { return _serve(); });
  }
  ~SendsAfterTheClose() {
    ::shutdown(listener_, SHUT_RDWR);  // an accept still waiting returns
    if (sent_.valid()) {
      sent_.wait();
    }
    if (fd_ >= 0) {
      ::close(fd_);
    }
    ::close(listener_);
  }

  // Waits for the late messages: whether they were written.
  bool sent() { return sent_.get(); }

  unsigned short port = 0;

 private:
  bool _serve() {
    fd_ = ::accept(listener_, nullptr, nullptr);
    if (fd_ < 0) {
      return false;
    }
    timeval patience = {30, 0};  // a client that never closes fails the test rather than hangs it
    ::setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &patience, sizeof patience);
    multiplexer::RawMessage welcome;  // the client's
    if (read_frame(fd_, &welcome) ||
        write_frame(fd_, *multiplexer::impl::create_welcome_message(multiplexer::peers::MULTIPLEXER, 0x6d78))) {
      return false;
    }
    for (;;) {  // whatever the client sends, to its end
      multiplexer::RawMessage frame;
      if (read_frame(fd_, &frame)) {
        break;
      }
    }
    bool written = true;
    for (const multiplexer::MultiplexerMessage& message : late_) {
      std::unique_ptr<multiplexer::RawMessage> raw(multiplexer::RawMessage::FromMessage(message));
      written = !write_frame(fd_, *raw) && written;
    }
    ::shutdown(fd_, SHUT_WR);  // the multiplexer's end, which the client reads on to
    return written;
  }

  const std::vector<multiplexer::MultiplexerMessage> late_;
  int listener_;
  int fd_ = -1;
  std::future<bool> sent_;
};

// A peer that closes, its side first, reads on to its multiplexer's end:
// a message arriving meanwhile it can only drop, as a request routed to a
// backend in the moment it leaves. The drop is counted, and logged, where
// it went unsaid and its sender waited out its timeout.
TEST(SyncClient, AMessageArrivingWhileItClosesIsCountedAsDropped) {
  SendsAfterTheClose multiplexer({routed(multiplexer::types::PYTHON_TEST_REQUEST)});
  multiplexer::Client client(multiplexer::peers::PYTHON_TEST_SERVER);
  ASSERT_TRUE(client.connect("127.0.0.1", multiplexer.port, 5));
  client.shutdown();
  EXPECT_TRUE(multiplexer.sent());
  EXPECT_EQ(1u, client.dropped_while_closing());
}

// The protocol's own answers to what the client sent are no loss when
// they arrive while it closes, only the client having waited for them: not
// counted, where every one was. A PING or a search that asks still is,
// its sender waiting for an answer.
TEST(SyncClient, TheProtocolsAnswersArrivingWhileItClosesAreNotCounted) {
  using namespace multiplexer::types;
  SendsAfterTheClose multiplexer({routed(DELIVERY_ERROR, 7), routed(REQUEST_RECEIVED, 7), routed(BACKEND_ERROR, 7),
                                  routed(RECORDING_STATUS, 7), routed(RULES_STATUS, 7), routed(PEER_STATUS, 7),
                                  routed(PING, 7), routed(PING), routed(BACKEND_FOR_PACKET_SEARCH)});
  multiplexer::Client client(multiplexer::peers::PYTHON_TEST_SERVER);
  ASSERT_TRUE(client.connect("127.0.0.1", multiplexer.port, 5));
  client.shutdown();
  EXPECT_TRUE(multiplexer.sent());
  EXPECT_EQ(2u, client.dropped_while_closing());  // the PING and the search that ask
}

// A multiplexer of the test's own behind a pinned lane, for a query's
// search stage: it answers the query's request, of `type`, with a delivery
// error, which has the client search through the lane, and reads nothing
// after the search, on a socket that buffers next to nothing, so that what
// the client sends next waits in the client. answer() then has a backend
// answer the search, and a note to the client follow it, which the
// client's on_message hears once it has acted on the answer; close() ends
// the connection with all that unread.
class SearchStage {
 public:
  explicit SearchStage(std::uint32_t type) : type_(type) {
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
    searched_ = std::async(std::launch::async, [this] { return _until_the_search(); });
  }
  ~SearchStage() {
    ::shutdown(listener_, SHUT_RDWR);  // an accept still waiting returns
    if (searched_.valid()) {
      searched_.wait();
    }
    close();
    ::close(listener_);
  }

  // Waits for the client's search: false when it did not come.
  bool searched() { return searched_.get(); }

  // A PING answering the search from `backend`, then `note` to the client;
  // false when they could not be written.
  bool answer(std::uint64_t backend, const std::string& note) {
    multiplexer::MultiplexerMessage ping;
    ping.set_id(kId + 1);
    ping.set_from(backend);
    ping.set_to(client_);
    ping.set_type(multiplexer::types::PING);
    ping.set_references(search_);
    multiplexer::MultiplexerMessage told;
    told.set_id(kId + 2);
    told.set_from(kId);
    told.set_to(client_);
    told.set_type(multiplexer::types::PYTHON_TEST_REQUEST);
    told.set_message(note);
    return !_write(ping) && !_write(told);
  }

  // Ends the connection, what the client sent since its search unread.
  void close() {
    if (fd_ >= 0) {
      ::close(fd_);
      fd_ = -1;
    }
  }

  unsigned short port = 0;

 private:
  static const std::uint64_t kId = 0x6d78;

  int _write(const multiplexer::MultiplexerMessage& msg) {
    return write_frame(fd_, *std::unique_ptr<multiplexer::RawMessage>(multiplexer::RawMessage::FromMessage(msg)));
  }

  bool _until_the_search() {
    fd_ = ::accept(listener_, nullptr, nullptr);
    if (fd_ < 0) {
      return false;
    }
    timeval patience = {30, 0};  // a search that never comes fails the test rather than hangs it
    ::setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &patience, sizeof patience);
    multiplexer::RawMessage welcome;  // the client's
    if (read_frame(fd_, &welcome) ||
        write_frame(fd_, *multiplexer::impl::create_welcome_message(multiplexer::peers::MULTIPLEXER, kId))) {
      return false;
    }
    for (;;) {
      multiplexer::RawMessage frame;
      multiplexer::MultiplexerMessage msg;
      if (read_frame(fd_, &frame) || !msg.ParseFromString(frame.get_message())) {
        return false;
      }
      if (msg.type() == type_) {
        client_ = msg.from();
        multiplexer::MultiplexerMessage refused;
        refused.set_id(kId + 3);
        refused.set_from(kId);
        refused.set_to(client_);
        refused.set_type(multiplexer::types::DELIVERY_ERROR);
        refused.set_references(msg.id());
        if (_write(refused)) {
          return false;
        }
      } else if (msg.type() == multiplexer::types::BACKEND_FOR_PACKET_SEARCH) {
        search_ = msg.id();
        return true;
      }
    }
  }

  std::uint32_t type_;
  int listener_;
  int fd_ = -1;
  std::uint64_t client_ = 0;
  std::uint64_t search_ = 0;
  std::future<bool> searched_;
};

// A multiplexer of the test's own for a typed query's last stage: it
// answers the request with a delivery error, nobody taking it, or, when
// `taken`, keeps it, as a backend busy with it would; answers the search
// with a PING from a backend it no longer has; answers the direct request
// to that backend with a delivery error when the request asks for one, as
// a multiplexer that lost the backend does, and drops it otherwise; and,
// `taken`, then sends the reply to the first request, late.
class DirectStage {
 public:
  explicit DirectStage(bool taken) : taken_(taken) {
    listener_ = ::socket(AF_INET, SOCK_STREAM, 0);
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
    scripted_ = std::async(std::launch::async, [this] { return _script(); });
  }
  ~DirectStage() {
    ::shutdown(listener_, SHUT_RDWR);  // an accept still waiting returns
    if (scripted_.valid()) {
      scripted_.wait();
    }
    if (fd_ >= 0) {
      ::close(fd_);
    }
    ::close(listener_);
  }

  // Waits for the script to end: whether the direct request asked for a
  // delivery error.
  bool asked() { return scripted_.get(); }

  unsigned short port = 0;

 private:
  static const std::uint64_t kId = 0x6d78;      // this multiplexer
  static const std::uint64_t kGone = 0x60e;     // the backend that answered the search, gone since
  static const std::uint64_t kBackend = 0xbac;  // the backend that took the request

  int _write(const multiplexer::MultiplexerMessage& msg) {
    return write_frame(fd_, *std::unique_ptr<multiplexer::RawMessage>(multiplexer::RawMessage::FromMessage(msg)));
  }

  // The client's next message of `type` addressed `to` (0 for none) into
  // `msg`, the rest skipped: false when none came.
  bool _next(std::uint32_t type, std::uint64_t to, multiplexer::MultiplexerMessage* msg) {
    for (;;) {
      multiplexer::RawMessage frame;
      if (read_frame(fd_, &frame) || !msg->ParseFromString(frame.get_message())) {
        return false;
      }
      if (msg->type() == type && msg->to() == to) {
        return true;
      }
    }
  }

  // A message of `type` from `from` to the client, answering `references`.
  multiplexer::MultiplexerMessage _answer(std::uint32_t type, std::uint64_t from, std::uint64_t references) {
    multiplexer::MultiplexerMessage msg;
    msg.set_id(++ids_);
    msg.set_from(from);
    msg.set_to(client_);
    msg.set_type(type);
    msg.set_references(references);
    msg.set_message("late");
    return msg;
  }

  bool _script() {
    fd_ = ::accept(listener_, nullptr, nullptr);
    if (fd_ < 0) {
      return false;
    }
    timeval patience = {30, 0};  // a step that never comes fails the test rather than hangs it
    ::setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &patience, sizeof patience);
    multiplexer::RawMessage welcome;  // the client's
    if (read_frame(fd_, &welcome) ||
        write_frame(fd_, *multiplexer::impl::create_welcome_message(multiplexer::peers::MULTIPLEXER, kId))) {
      return false;
    }
    multiplexer::MultiplexerMessage request, search, direct;
    if (!_next(multiplexer::types::PYTHON_TEST_REQUEST, 0, &request)) {
      return false;
    }
    client_ = request.from();
    if (!taken_ && _write(_answer(multiplexer::types::DELIVERY_ERROR, kId, request.id()))) {
      return false;
    }
    if (!_next(multiplexer::types::BACKEND_FOR_PACKET_SEARCH, 0, &search) ||
        _write(_answer(multiplexer::types::PING, kGone, search.id())) ||
        !_next(multiplexer::types::PYTHON_TEST_REQUEST, kGone, &direct)) {
      return false;
    }
    if (direct.report_delivery_error() && _write(_answer(multiplexer::types::DELIVERY_ERROR, kId, direct.id()))) {
      return false;
    }
    if (taken_ && _write(_answer(multiplexer::types::PYTHON_TEST_RESPONSE, kBackend, request.id()))) {
      return false;
    }
    return direct.report_delivery_error();
  }

  const bool taken_;
  int listener_;
  int fd_ = -1;
  std::uint64_t client_ = 0;
  std::uint64_t ids_ = kId;
  std::future<bool> scripted_;
};

// A typed query's request, for the synchronous client.
multiplexer::MultiplexerMessage request_of(multiplexer::Client& client) {
  multiplexer::MultiplexerMessage msg;
  msg.set_id(client.random64());
  msg.set_from(client.instance_id());
  msg.set_type(multiplexer::types::PYTHON_TEST_REQUEST);
  msg.set_message("request");
  return msg;
}

}  // namespace

// A typed query's direct request asks for a delivery error, so that the
// backend that answered the search, gone since, is noticed: with nobody
// having taken the request, the query fails at once, where the request,
// asking for none, was dropped unsaid and the stage ran out its timeout.
TEST(Client, ADirectRequestToAGoneBackendFailsWhenNobodyTookTheRequest) {
  DirectStage multiplexer(/*taken=*/false);
  multiplexer::Client client(multiplexer::peers::WEBSITE);
  ASSERT_TRUE(client.wait_for_connection(client.connect("127.0.0.1", multiplexer.port, 5), 0));
  EXPECT_THROW(client.query(request_of(client), 5), multiplexer::Client::OperationFailed);
  EXPECT_TRUE(multiplexer.asked()) << "the direct request asked for no delivery error";
}

// With a backend holding the request, a gone backend's delivery error
// ends nothing: the stage waits on, and the late reply to the first
// request answers the query.
TEST(Client, ADirectRequestToAGoneBackendWaitsForTheRequestABackendTook) {
  DirectStage multiplexer(/*taken=*/true);
  multiplexer::Client client(multiplexer::peers::WEBSITE);
  ASSERT_TRUE(client.wait_for_connection(client.connect("127.0.0.1", multiplexer.port, 5), 0));
  multiplexer::IncomingMessage reply = client.query(request_of(client), 1);
  EXPECT_EQ(multiplexer::types::PYTHON_TEST_RESPONSE, reply.third->type());
  EXPECT_EQ("late", reply.third->message());
  EXPECT_TRUE(multiplexer.asked()) << "the direct request asked for no delivery error";
}

// The same in the threaded client, whose stages are its own.
TEST(ThreadedClient, ADirectRequestToAGoneBackendFailsWhenNobodyTookTheRequest) {
  DirectStage multiplexer(/*taken=*/false);
  ThreadedClient client(multiplexer::peers::WEBSITE);
  ASSERT_TRUE(client.connect("127.0.0.1", multiplexer.port, 5));
  EXPECT_EQ(ThreadedClient::FAILED, client.query("request", multiplexer::types::PYTHON_TEST_REQUEST, 5).outcome);
  EXPECT_TRUE(multiplexer.asked()) << "the direct request asked for no delivery error";
}

TEST(ThreadedClient, ADirectRequestToAGoneBackendWaitsForTheRequestABackendTook) {
  DirectStage multiplexer(/*taken=*/true);
  ThreadedClient client(multiplexer::peers::WEBSITE);
  ASSERT_TRUE(client.connect("127.0.0.1", multiplexer.port, 5));
  ThreadedClient::Result result = client.query("request", multiplexer::types::PYTHON_TEST_REQUEST, 1);
  ASSERT_EQ(ThreadedClient::REPLIED, result.outcome);
  EXPECT_EQ("late", result.reply.third->message());
  EXPECT_TRUE(multiplexer.asked()) << "the direct request asked for no delivery error";
}

// A peer type above 16 bits was cut to its low 16 by the one function
// every client is made through, BasicClient::Create: this one was
// announced as WEBSITE.
TEST(Client, APeerTypeAboveSixteenBitsIsAnnouncedWhole) {
  const std::uint32_t type = 65536 + multiplexer::peers::WEBSITE;
  {
    WelcomeReader multiplexer;
    multiplexer::Client client(type);
    EXPECT_TRUE(client.wait_for_connection(client.connect("127.0.0.1", multiplexer.port, 30), 0));
    EXPECT_EQ(type, multiplexer.announced()) << "the synchronous client";
  }
  {
    WelcomeReader multiplexer;
    ThreadedClient client(type);
    EXPECT_TRUE(client.connect("127.0.0.1", multiplexer.port, 30));
    EXPECT_EQ(type, multiplexer.announced()) << "the threaded client";
  }
}

// Through a pinned lane, the request a search ends in, sent again to the
// backend that answered the search, is pinned too: when the lane's
// connection dies before writing it, it goes nowhere, as the NOT_CONNECTED
// the query ends with says. It went out unpinned, and the client handed it
// to another connection: the backend ran a request whose caller heard it
// did not go, and a caller who sent it again on a new lane, as the pin
// asks, had it run twice. Counted at the backend before a last request
// that the same connection carries after the hand-over.
// A connection that dies with frames of its own unwritten, here the
// PEER_CONTROL a routing change sent behind messages its multiplexer never
// read: those go with it, unreported. Each was reported dropped, under an
// id the program never sent; a welcome was handed to another connection,
// whose multiplexer then closed that one on a second welcome. Every id the
// drop observer hears is one the test sent.
TEST(ThreadedClient, AConnectionsOwnFramesGoWithItUnreported) {
  std::mutex mutex;  // before the client, whose observer uses them
  std::vector<std::uint64_t> reported;
  std::set<std::uint64_t> sent;
  DeafAfterWelcome multiplexer;
  {
    ThreadedClient client(multiplexer::peers::WEBSITE);
    client.set_drop_observer([&](std::uint64_t id, multiplexer::DropReason) {
      std::lock_guard<std::mutex> lock(mutex);
      reported.push_back(id);
    });
    ASSERT_TRUE(client.connect("127.0.0.1", multiplexer.port, 5));
    ASSERT_TRUE(multiplexer.welcomed());
    // More than the sockets hold: the rest, and the routing after it, wait
    // in the client.
    const std::string filler(1 << 20, 'f');
    for (std::size_t index = 0; index <= 2 * socket_bytes() / filler.size(); ++index) {
      multiplexer::MultiplexerMessage msg = client.new_message(multiplexer::types::PYTHON_TEST_REQUEST, filler);
      {
        std::lock_guard<std::mutex> lock(mutex);
        sent.insert(msg.id());
      }
      client.send(msg);
    }
    multiplexer::Routing off;
    off.set_any(false);
    client.set_routing(off);
    EXPECT_EQ(1u, client.connections_count());  // a round trip through the io thread: the routing is queued
    multiplexer.reset();
    for (int waited = 0; waited < 1000 && client.connections_count() != 0; ++waited) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_EQ(0u, client.connections_count()) << "the connection outlived the reset";
    std::lock_guard<std::mutex> lock(mutex);
    for (const std::uint64_t id : reported) {
      EXPECT_EQ(1u, sent.count(id)) << "reported dropped, never sent: " << id;
    }
  }
}

TEST(ThreadedClient, TheRequestASearchThroughAPinnedLaneEndsInIsPinnedToo) {
  std::promise<ThreadedClient::Result> done;  // before the clients, whose callbacks set them
  std::promise<void> noted;
  std::promise<int> ran_before_the_last;
  std::atomic<bool> noted_once(false);
  std::atomic<int> ran(0);
  SearchStage stalled(multiplexer::types::PYTHON_TEST_REQUEST);
  InProcessMultiplexer other;
  ThreadedClient backend(multiplexer::peers::PYTHON_TEST_SERVER, [&](const multiplexer::IncomingMessage& incoming) {
    if (incoming.third->message() == "once") {
      ++ran;
    } else if (incoming.third->message() == "last") {
      ran_before_the_last.set_value(ran.load());
    }
    multiplexer::MultiplexerMessage reply = backend.new_message(multiplexer::types::PYTHON_TEST_RESPONSE, "ran");
    reply.set_to(incoming.third->from());
    reply.set_references(incoming.third->id());
    backend.send(reply, incoming.second);
  });
  ASSERT_TRUE(backend.connect("127.0.0.1", other.port, 5));
  ThreadedClient client(multiplexer::peers::WEBSITE, [&](const multiplexer::IncomingMessage&) {
    if (!noted_once.exchange(true)) {
      noted.set_value();
    }
  });
  ASSERT_TRUE(client.connect("127.0.0.1", stalled.port, 5));
  multiplexer::LanePtr lane = std::make_shared<multiplexer::Lane>(true);
  client.query(
      "once", multiplexer::types::PYTHON_TEST_REQUEST,
      [&done](const ThreadedClient::Result& result) { done.set_value(result); }, 30, lane);
  ASSERT_TRUE(stalled.searched());
  ASSERT_TRUE(client.connect("127.0.0.1", other.port, 5));
  // More than the sockets hold, through the lane: the rest, and the
  // request the search ends in after it, wait in the client.
  const std::string filler(1 << 20, 'f');
  for (std::size_t index = 0; index <= 2 * socket_bytes() / filler.size(); ++index) {
    client.send(client.new_message(multiplexer::types::PYTHON_TEST_REQUEST, filler), lane);
  }
  ASSERT_TRUE(stalled.answer(backend.instance_id(), "noted"));
  std::future<void> acted = noted.get_future();
  ASSERT_EQ(std::future_status::ready, acted.wait_for(std::chrono::seconds(30)));
  stalled.close();
  std::future<ThreadedClient::Result> ended = done.get_future();
  ASSERT_EQ(std::future_status::ready, ended.wait_for(std::chrono::seconds(30)));
  EXPECT_EQ(ThreadedClient::NOT_CONNECTED, ended.get().outcome);
  EXPECT_EQ(ThreadedClient::REPLIED, client.query("last", multiplexer::types::PYTHON_TEST_REQUEST, 10).outcome);
  std::future<int> counted = ran_before_the_last.get_future();
  ASSERT_EQ(std::future_status::ready, counted.wait_for(std::chrono::seconds(10)));
  EXPECT_EQ(0, counted.get()) << "the request ran through another multiplexer";
}

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
  {
    // The multiplexer reads nothing until the test has let go of the
    // client: its answer, a delivery error, could otherwise bring the
    // callback first, whose reference was then not the last.
    Freeze freeze(mx);
    client->query(
        "no backend answers this", multiplexer::types::PYTHON_TEST_REQUEST,
        [held = client, watch, &destroyed_inside](const ThreadedClient::Result&) mutable {
          held.reset();  // the last reference: the destructor runs here, on the io thread
          destroyed_inside.set_value(watch.expired());
        },
        5);
    client.reset();
  }
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
// A connection that stays full, its multiplexer frozen: what ended waiting
// for its room is cleared out beside a message that still waits, 64 more
// than what waits kept at most, where every one stayed, its whole frame
// with it, until room came. Counted once the drops are reported.
TEST(BasicClient, AFullConnectionKeepsABoundedRestOfWhatEndedWaitingForRoom) {
  InProcessMultiplexer mx;
  asio::io_service io_service;
  std::shared_ptr<multiplexer::BasicClient> client =
      multiplexer::BasicClient::Create(io_service, multiplexer::peers::WEBSITE);
  ASSERT_TRUE(client->wait_for_connection(client->connect("127.0.0.1", mx.port, 5), 5));
  const std::string payload(1024, 'x');
  auto frame = [&] {
    multiplexer::MultiplexerMessage msg;
    msg.set_id(client->random64());
    msg.set_from(client->instance_id());
    msg.set_type(multiplexer::types::PYTHON_TEST_REQUEST);
    msg.set_message(payload);
    return std::shared_ptr<const multiplexer::RawMessage>(multiplexer::RawMessage::FromMessage(msg));
  };
  // Runs the loop until `done`, 30 s at most.
  auto run_until = [&](const std::function<bool()>& done) {
    std::unique_ptr<mx::SimpleTimer> timer = client->create_timer(30);
    while (!done() && !timer->expired()) {
      client->run_one();
    }
    return done();
  };
  Freeze frozen(mx);
  // The sockets and the queue filled; what does not fit waits half a second.
  const int filler = frames_to_fill(payload.size());
  for (int index = 0; index < filler; ++index) {
    client->send(frame(), false, multiplexer::LanePtr(), 0.5f);
  }
  ASSERT_TRUE(run_until([&] { return client->outbox_entries() == 0; })) << "the filler that waited ended";
  const std::uint64_t before = client->dropped();
  client->send(frame(), false, multiplexer::LanePtr(), 60);  // waits for room all along
  for (int index = 0; index < 200; ++index) {
    client->send(frame(), false, multiplexer::LanePtr(), 0.001f);
  }
  ASSERT_TRUE(run_until([&] { return client->dropped() == before + 200; }));
  EXPECT_LE(client->outbox_entries(), 1u + 64u) << "messages that ended beside one that waits";
  frozen.release();
  client->shutdown();
}

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

// A search a ThreadedClient's search policy says yes to is answered with
// a PING carrying the search back, as a PING is; one whose echo would be
// over the limit gets BACKEND_ERROR saying so.
TEST(ThreadedClient, ASearchIsAnsweredWithItsPayloadEchoed) {
  InProcessMultiplexer mx;
  ThreadedClient client(multiplexer::peers::WEBSITE);
  client.set_search_policy([] { return true; });
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

// A ThreadedClient without a search policy is no backend and answers no
// search, not even one addressed to it, which it answered for the
// addressed query's old probe, nor hands one to on_message; a PING it
// answers. The search goes first, so that the PING's answer, sent after
// whatever the search drew, ends the wait.
TEST(ThreadedClient, WithoutASearchPolicyNoSearchIsAnswered) {
  InProcessMultiplexer mx;
  std::atomic<int> handed_on(0);
  ThreadedClient client(multiplexer::peers::WEBSITE,
                        [&handed_on](const multiplexer::IncomingMessage&) { ++handed_on; });
  ASSERT_TRUE(client.connect("127.0.0.1", mx.port, 5));
  Peer peer(mx.port, multiplexer::peers::WEBSITE);
  multiplexer::MultiplexerMessage search =
      peer.message(multiplexer::types::BACKEND_FOR_PACKET_SEARCH, "", client.instance_id());
  multiplexer::MultiplexerMessage ping = peer.message(multiplexer::types::PING, "after", client.instance_id());
  peer.send(search);
  peer.send(ping);
  std::vector<std::uint64_t> answered;  // what each answer references, up to the PING's
  const std::chrono::steady_clock::time_point deadline = std::chrono::steady_clock::now() + std::chrono::seconds(20);
  while (std::chrono::steady_clock::now() < deadline && (answered.empty() || answered.back() != ping.id())) {
    try {
      answered.push_back(peer.client.receive_message(1).first->references());
    } catch (const multiplexer::Client::OperationTimedOut&) {
    }
  }
  ASSERT_FALSE(answered.empty()) << "no answer to the PING";
  EXPECT_EQ(ping.id(), answered.back()) << "no answer to the PING";
  EXPECT_EQ(0, std::count(answered.begin(), answered.end(), search.id())) << "the search was answered";
  EXPECT_EQ(0, handed_on) << "the search handed to on_message";
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

// A typed query goes to the backend its search found addressed with `to`
// (_direct), up to 11 bytes the check at the call did not count: a request
// within 11 bytes of the limit passed it, threw on the io thread, and the
// query ended TIMED_OUT at its search timer. The check counts that `to` at
// its largest, so the caller gets the error.
TEST(ThreadedClient, ATypedQueryTooBigOnceAddressedIsRefusedAtTheCall) {
  ThreadedClient client(multiplexer::peers::WEBSITE);
  multiplexer::MultiplexerMessage request;
  request.set_type(multiplexer::types::PYTHON_TEST_REQUEST);
  request.set_id(std::numeric_limits<std::uint64_t>::max());
  request.set_from(client.instance_id());
  fill_to_the_limit(&request);  // at the limit as the check measured it: the largest id, `from`, no `to`
  request.clear_id();
  request.clear_from();
  EXPECT_THROW(client.query(request, [](const ThreadedClient::Result&) {}, 1), std::length_error);
}

namespace {

// A typed request at the limit once sent again: an id and a `to` of the
// most bytes and a delivery error asked for, as the request sent again
// carries them, put it one byte over; without the `to` and the flag it is
// under. `from` as the query fills it in.
multiplexer::MultiplexerMessage too_big_to_send_again(std::uint64_t from) {
  multiplexer::MultiplexerMessage request;
  request.set_type(multiplexer::types::PYTHON_TEST_REQUEST);
  request.set_from(from);
  request.set_id(std::numeric_limits<std::uint64_t>::max());
  request.set_to(std::numeric_limits<std::uint64_t>::max());
  request.set_report_delivery_error(true);
  request.set_message(std::string(multiplexer::MAX_MESSAGE_SIZE - request.ByteSizeLong() - 16, 'p'));
  request.mutable_message()->append(multiplexer::MAX_MESSAGE_SIZE + 1 - request.ByteSizeLong(), 'p');
  request.clear_to();
  request.clear_report_delivery_error();
  return request;
}

}  // namespace

// A typed query's request sent again asks for a delivery error too, three
// bytes the check at the call did not count: a request within them of the
// limit passed it, threw on the io thread, and the query ended TIMED_OUT.
// The check counts the flag, so the caller gets the error.
TEST(ThreadedClient, ATypedQueryTooBigOnceItAsksForDeliveryErrorsIsRefusedAtTheCall) {
  ThreadedClient client(multiplexer::peers::WEBSITE);
  multiplexer::MultiplexerMessage request = too_big_to_send_again(client.instance_id());
  EXPECT_THROW(client.query(request, [](const ThreadedClient::Result&) {}, 1), std::length_error);
}

// The synchronous client measures the request sent again at the call too:
// a request whose copy sent again would be over the limit is refused
// there, before anything goes out, where it went out and the query failed
// at its third stage, or, with no connection, waited for one and raised
// NotConnected.
TEST(SyncClient, ATypedQueryTooBigToSendAgainIsRefusedAtTheCall) {
  multiplexer::Client client(multiplexer::peers::WEBSITE);
  multiplexer::MultiplexerMessage request = too_big_to_send_again(client.instance_id());
  EXPECT_THROW(client.query(request, 0.5), std::length_error);
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
// and a search at once. `requests` counts the requests that arrived,
// `searched` says a search came, `second_to` is where the second request
// was addressed and `id` is the backend's instance id.
struct LosingBackend {
  LosingBackend(unsigned short first, unsigned short second, std::chrono::milliseconds delay)
      : thread([this, first, second, delay] {
          multiplexer::Client client(multiplexer::peers::PYTHON_TEST_SERVER);
          client.connect("127.0.0.1", first, 5);
          client.connect("127.0.0.1", second, 5);
          id = client.instance_id();
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
            if (request.type() == multiplexer::types::BACKEND_FOR_PACKET_SEARCH) {
              searched = true;
            }
            if (request.type() == multiplexer::types::PYTHON_TEST_REQUEST && ++requests == 1) {
              continue;  // the first one is lost
            }
            if (request.type() == multiplexer::types::PYTHON_TEST_REQUEST && requests == 2) {
              second_to = request.to();
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
  std::atomic<bool> searched{false};
  std::atomic<std::uint64_t> second_to{0};
  std::atomic<std::uint64_t> id{0};
  std::thread thread;
};

}  // namespace

// The lane's multiplexer dies under the wait: the request may have been
// routed first, so it is not sent again. The query searches at once, and
// the request goes out a second and last time, addressed to the backend
// that answered, whose reply ends the query. Where the request went out
// again at once, routed by its type, no search came and the second request
// was addressed to nobody.
TEST(Client, ALostConnectionSearchesInsteadOfSendingAgain) {
  std::unique_ptr<InProcessMultiplexer> first(new InProcessMultiplexer());
  std::unique_ptr<InProcessMultiplexer> second(new InProcessMultiplexer());
  LosingBackend backend(first->port, second->port, std::chrono::milliseconds(0));
  multiplexer::Client client(multiplexer::peers::WEBSITE);
  multiplexer::ConnectionWrapper to_first = client.connect("127.0.0.1", first->port, 5);
  client.connect("127.0.0.1", second->port, 5);
  multiplexer::LanePtr lane(new multiplexer::Lane(to_first));  // the request goes through the first
  // The first multiplexer goes once the backend has the request: ordered by
  // the event, the bound only detecting a failure.
  std::thread killer([&first, &backend] {
    for (int checks = 0; checks < 3000 && backend.requests.load() < 1; ++checks) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    first.reset();
  });
  std::string answer;
  try {
    answer = client.query("searched", multiplexer::types::PYTHON_TEST_REQUEST, 30.0f, lane).third->message();
  } catch (const std::exception& error) {
    ADD_FAILURE() << "the query raised " << error.what();
  }
  killer.join();
  EXPECT_EQ("SEARCHED", answer);
  EXPECT_TRUE(backend.searched.load()) << "the query moved on to the search";
  EXPECT_EQ(2, backend.requests.load()) << "the request and the direct request, no more";
  EXPECT_EQ(backend.id.load(), backend.second_to.load()) << "the second time out, to the backend found";
}

namespace {

// A threaded backend connected to two multiplexers that answers every
// request at once, through the connection it came on, with its payload,
// and counts the requests.
struct CountingAnswerer {
  CountingAnswerer(unsigned short first, unsigned short second)
      : client(multiplexer::peers::PYTHON_TEST_SERVER, [this](const multiplexer::IncomingMessage& incoming) {
          if (incoming.third->type() != multiplexer::types::PYTHON_TEST_REQUEST) {
            return;
          }
          ++requests;
          multiplexer::MultiplexerMessage reply =
              client.new_message(multiplexer::types::PYTHON_TEST_RESPONSE, incoming.third->message());
          reply.set_to(incoming.third->from());
          reply.set_references(incoming.third->id());
          client.send(reply, incoming.second);
        }) {
    EXPECT_TRUE(client.connect("127.0.0.1", first, 5));
    EXPECT_TRUE(client.connect("127.0.0.1", second, 5));
  }
  std::atomic<int> requests{0};  // before the client, which counts into it until it is gone
  ThreadedClient client;
};

}  // namespace

// A query through a lane whose multiplexer is frozen with the connection
// full: the request waits there, the multiplexer goes, and the request is
// handed to the other connection and written there. The flushing send
// under the query reports that connection, and the reply is awaited
// there; it reported the dead one, so the request went out again under a
// new id and the backend counted two.
TEST(Client, AHandedOverRequestIsAnsweredWithoutAResend) {
  InProcessMultiplexer kept;
  std::unique_ptr<InProcessMultiplexer> going(new InProcessMultiplexer());
  CountingAnswerer backend(kept.port, going->port);
  multiplexer::Client client(multiplexer::peers::WEBSITE);
  client.connect("127.0.0.1", kept.port, 5);
  multiplexer::ConnectionWrapper to_going = client.connect("127.0.0.1", going->port, 5);
  multiplexer::LanePtr lane(new multiplexer::Lane(to_going));  // the request goes through the one that goes
  Freeze freeze(*going);
  const std::string chunk(16 * 1024, 'x');
  multiplexer::MultiplexerMessage fill;  // routed nowhere: fills the connection, answered by nobody
  fill.set_type(multiplexer::types::TEST_UNROUTED);
  fill.set_from(client.instance_id());
  fill.set_message(chunk);
  for (int index = 0; index < frames_to_fill(chunk.size()); ++index) {
    fill.set_id(client.random64());
    client.schedule_one(fill, to_going, 30);
  }
  std::thread killer([&freeze, &going] {
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    freeze.release();
    going.reset();
  });
  std::string answer;
  try {
    answer = client.query("question", multiplexer::types::PYTHON_TEST_REQUEST, 30.0f, lane).third->message();
  } catch (const std::exception& error) {
    ADD_FAILURE() << "the query raised " << error.what();
  }
  killer.join();
  EXPECT_EQ("question", answer);
  EXPECT_EQ(1, backend.requests.load()) << "sent once, not again under a new id";
}

namespace {

// A backend on the threaded client that keeps the id of every message it
// is handed.
struct IdSink {
  explicit IdSink(unsigned short port)
      : client(multiplexer::peers::PYTHON_TEST_SERVER, [this](const multiplexer::IncomingMessage& incoming) {
          std::lock_guard<std::mutex> lock(mutex);
          ids.insert(incoming.third->id());
        }) {
    client.connect("127.0.0.1", port, 5);
  }
  // The ids that arrived, once `count` did or `seconds` passed.
  std::set<std::uint64_t> wait_for(std::size_t count, int seconds) {
    const std::chrono::steady_clock::time_point deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(seconds);
    for (;;) {
      {
        std::lock_guard<std::mutex> lock(mutex);
        if (ids.size() >= count || std::chrono::steady_clock::now() >= deadline) {
          return ids;
        }
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
  }
  std::mutex mutex;
  std::set<std::uint64_t> ids;  // before the client, which writes into it until it is gone
  ThreadedClient client;
};

}  // namespace

// A client that sends to a frozen multiplexer and shuts down with 0, which
// drops what is not written at once: what was written arrives and
// everything else was told to the drop observer, so that every message is
// delivered or reported, and none of those reported arrives. Most went
// without a word. The multiplexer reads again at the first drop reported,
// while the client's close still reads on for the multiplexer's end, so
// that it takes what was written within the close's bound, however long
// the freeze took: one still stalled past CLOSE_READ_SECONDS may lose what
// the client's kernel held (semantics.md).
TEST(ThreadedClient, EveryMessageIsDeliveredOrReportedAtShutdown) {
  InProcessMultiplexer mx;
  IdSink sink(mx.port);
  std::mutex mutex;
  std::set<std::uint64_t> reported;
  std::set<multiplexer::DropReason> reasons;
  std::set<std::uint64_t> sent;
  Freeze* frozen = nullptr;  // set while the multiplexer is frozen; released by the shutdown's first drop
  ThreadedClient client(multiplexer::peers::WEBSITE);
  client.set_drop_observer([&](std::uint64_t id, multiplexer::DropReason reason) {
    std::lock_guard<std::mutex> lock(mutex);
    reported.insert(id);
    reasons.insert(reason);
    if (frozen && reason == multiplexer::DropReason::SHUT_DOWN) {
      frozen->release();
    }
  });
  ASSERT_TRUE(client.connect("127.0.0.1", mx.port, 5));
  const std::string chunk(16 * 1024, 'x');
  {
    Freeze freeze(mx);
    frozen = &freeze;
    for (int index = 0; index < frames_to_fill(chunk.size()); ++index) {
      multiplexer::MultiplexerMessage msg = client.new_message(multiplexer::types::PYTHON_TEST_REQUEST, chunk);
      sent.insert(msg.id());
      client.send(msg);
    }
    client.shutdown(0);  // returns once the io thread is done, the drop observer with it
    frozen = nullptr;
  }
  std::lock_guard<std::mutex> lock(mutex);
  ASSERT_FALSE(reported.empty()) << "a frozen multiplexer left something to report";
  EXPECT_EQ(std::set<multiplexer::DropReason>({multiplexer::DropReason::SHUT_DOWN}), reasons);
  EXPECT_EQ(reported.size(), client.dropped());
  const std::set<std::uint64_t> arrived = sink.wait_for(sent.size() - reported.size(), 60);
  std::size_t both = 0, either = 0;
  for (std::uint64_t id : sent) {
    both += arrived.count(id) && reported.count(id);
    either += arrived.count(id) || reported.count(id);
  }
  EXPECT_EQ(0u, both) << "reported dropped, yet it arrived";
  EXPECT_EQ(sent.size(), either) << "neither delivered nor reported";
}

// flush_all() is false when a message it waited for was given up on, on
// both clients, where it counted the drop as done and said true: written
// messages flush true; with the only multiplexer gone, a message held past
// its timeout is dropped while the next flush waits for it.
TEST(Client, FlushAllIsFalseWhenAMessageItWaitedForWasDropped) {
  std::unique_ptr<InProcessMultiplexer> mx(new InProcessMultiplexer());
  Peer sender(mx->port, multiplexer::peers::WEBSITE);
  ASSERT_TRUE(sender.client.queue(sender.message(multiplexer::types::PYTHON_TEST_REQUEST, "written", 0), 5));
  EXPECT_TRUE(sender.client.flush_all(5));
  mx.reset();  // gone: the client notices in its next call, and holds what follows
  ASSERT_TRUE(sender.client.queue(sender.message(multiplexer::types::PYTHON_TEST_REQUEST, "held", 0), 0.2f));
  EXPECT_FALSE(sender.client.flush_all(30));
  EXPECT_EQ(1u, sender.client.dropped());
}

TEST(ThreadedClient, FlushAllIsFalseWhenAMessageItWaitedForWasDropped) {
  std::unique_ptr<InProcessMultiplexer> mx(new InProcessMultiplexer());
  ThreadedClient client(multiplexer::peers::WEBSITE);
  ASSERT_TRUE(client.connect("127.0.0.1", mx->port, 5));
  client.send(client.new_message(multiplexer::types::PYTHON_TEST_REQUEST, "written"));
  EXPECT_TRUE(client.flush_all(5));
  mx.reset();
  for (int tries = 0; tries < 1000 && client.connections_count() > 0; ++tries) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));  // the io thread notices the end
  }
  ASSERT_EQ(0u, client.connections_count());
  const multiplexer::MultiplexerMessage held = client.new_message(multiplexer::types::PYTHON_TEST_REQUEST, "held");
  client.send_serialized(held.SerializeAsString(), held.id(), held.type(), multiplexer::LanePtr(), 0.2f);
  EXPECT_FALSE(client.flush_all(30));
  EXPECT_EQ(1u, client.dropped());
}

// A send's callback, in C++ as in Python, on both clients: queue() and
// queue_all() on the synchronous client call it inside a later call that
// runs the loop, ThreadedClient's send() and send_all() on the io thread;
// once, with 1 when the message was written, the first copy for ALL, 0 when
// it was given up on or shutdown() came first. Only the serialized sends
// took one.
TEST(Client, AQueuedMessagesCallbackHearsHowItEnded) {
  std::unique_ptr<InProcessMultiplexer> mx(new InProcessMultiplexer());
  std::vector<unsigned int> heard;  // before the client, which calls into it until its shutdown
  Peer sender(mx->port, multiplexer::peers::WEBSITE);
  auto record = [&heard](unsigned int written) { heard.push_back(written); };
  ASSERT_TRUE(sender.client.queue(sender.message(multiplexer::types::PYTHON_TEST_REQUEST, "one", 0), 5,
                                  multiplexer::LanePtr(), record));
  ASSERT_TRUE(sender.client.queue_all(sender.message(multiplexer::types::PYTHON_TEST_REQUEST, "all", 0), 5, record));
  EXPECT_TRUE(sender.client.flush_all(5));
  EXPECT_EQ(std::vector<unsigned int>({1, 1}), heard) << "each once, by the time flush_all() returned";
  mx.reset();  // gone: the client notices in its next call, and holds what follows
  ASSERT_TRUE(sender.client.queue(sender.message(multiplexer::types::PYTHON_TEST_REQUEST, "held", 0), 0.2f,
                                  multiplexer::LanePtr(), record));
  ASSERT_TRUE(sender.client.queue(sender.message(multiplexer::types::PYTHON_TEST_REQUEST, "left", 0), 30,
                                  multiplexer::LanePtr(), record));
  sender.client.flush_all(0.5f);  // past the first's timeout
  EXPECT_EQ(std::vector<unsigned int>({1, 1, 0}), heard) << "given up on at its timeout";
  sender.client.shutdown(0);
  EXPECT_EQ(std::vector<unsigned int>({1, 1, 0, 0}), heard) << "given up on at the shutdown";
}

TEST(ThreadedClient, AMessagesCallbackHearsHowItEnded) {
  InProcessMultiplexer mx;
  std::mutex mutex;
  std::vector<unsigned int> heard;  // before the client, whose io thread calls into it
  ThreadedClient client(multiplexer::peers::WEBSITE);
  ASSERT_TRUE(client.connect("127.0.0.1", mx.port, 5));
  auto record = [&mutex, &heard](unsigned int written) {
    std::lock_guard<std::mutex> lock(mutex);
    heard.push_back(written);
  };
  client.send(client.new_message(multiplexer::types::PYTHON_TEST_REQUEST, "one"), multiplexer::LanePtr(), record);
  client.send_all(client.new_message(multiplexer::types::PYTHON_TEST_REQUEST, "all"), record);
  EXPECT_TRUE(client.flush_all(5));
  {
    std::lock_guard<std::mutex> lock(mutex);
    EXPECT_EQ(std::vector<unsigned int>({1, 1}), heard) << "each once, by the time flush_all() returned";
  }
  const std::string chunk(16 * 1024, 'x');
  Freeze freeze(mx);
  for (int index = 0; index < frames_to_fill(chunk.size()); ++index) {
    client.send(client.new_message(multiplexer::types::PYTHON_TEST_REQUEST, chunk));
  }
  client.send(client.new_message(multiplexer::types::PYTHON_TEST_REQUEST, "left"), multiplexer::LanePtr(), record);
  client.shutdown(0);
  std::lock_guard<std::mutex> lock(mutex);
  EXPECT_EQ(std::vector<unsigned int>({1, 1, 0}), heard) << "given up on at the shutdown";
}

// A flushing send that wrote nothing says why: out of time, its message
// waiting for room on a frozen multiplexer's full connection, or given up
// on, the client shutting down under it. The Python clients raise
// OperationTimedOut for the one and NotConnected for the other on it, as
// the synchronous client does, where they guessed from the lane and the
// connections afterwards.
TEST(ThreadedClient, AFlushingSendThatWroteNothingSaysWhy) {
  InProcessMultiplexer mx;
  std::promise<std::pair<unsigned int, bool>> ended;  // before the client, which settles it as it shuts down
  ThreadedClient client(multiplexer::peers::WEBSITE);
  ASSERT_TRUE(client.connect("127.0.0.1", mx.port, 5));
  const std::string chunk(16 * 1024, 'x');
  Freeze freeze(mx);
  for (int index = 0; index < frames_to_fill(chunk.size()); ++index) {
    client.send(client.new_message(multiplexer::types::PYTHON_TEST_REQUEST, chunk));
  }
  bool not_connected = true;
  const multiplexer::MultiplexerMessage late = client.new_message(multiplexer::types::PYTHON_TEST_REQUEST, "late");
  EXPECT_EQ(0u, client.send_serialized_and_wait(late.SerializeAsString(), late.id(), late.type(), false, 0.3f,
                                                multiplexer::LanePtr(), &not_connected));
  EXPECT_FALSE(not_connected) << "out of time with a connection live";
  const multiplexer::MultiplexerMessage left = client.new_message(multiplexer::types::PYTHON_TEST_REQUEST, "left");
  client.send_serialized_and_notify(
      left.SerializeAsString(), left.id(), left.type(), false, 30,
      [&ended](unsigned int written, bool lost) { ended.set_value(std::make_pair(written, lost)); });
  client.shutdown(0);
  const std::pair<unsigned int, bool> end = ended.get_future().get();
  EXPECT_EQ(0u, end.first);
  EXPECT_TRUE(end.second) << "given up on, at the shutdown";
}

// Out of time with no connection live is NotConnected, as the synchronous
// client tells it at its deadline: decided on the io thread then, where it
// was told as a timeout and the Python clients asked the io thread for the
// connections afterwards, on an asyncio loop too, a connection up by then
// making it a timeout.
TEST(ThreadedClient, AFlushingSendOutOfTimeWithNoConnectionSaysNotConnected) {
  std::promise<std::pair<unsigned int, bool>> ended;   // before the client, which settles it as it shuts down
  ThreadedClient client(multiplexer::peers::WEBSITE);  // connected nowhere
  bool not_connected = false;
  const multiplexer::MultiplexerMessage nowhere =
      client.new_message(multiplexer::types::PYTHON_TEST_REQUEST, "nowhere");
  EXPECT_EQ(0u, client.send_serialized_and_wait(nowhere.SerializeAsString(), nowhere.id(), nowhere.type(), false, 0.2f,
                                                multiplexer::LanePtr(), &not_connected));
  EXPECT_TRUE(not_connected) << "no connection live at the deadline";
  const multiplexer::MultiplexerMessage either =
      client.new_message(multiplexer::types::PYTHON_TEST_REQUEST, "nowhere either");
  client.send_serialized_and_notify(
      either.SerializeAsString(), either.id(), either.type(), false, 0.2f,
      [&ended](unsigned int written, bool none) { ended.set_value(std::make_pair(written, none)); });
  const std::pair<unsigned int, bool> end = ended.get_future().get();
  EXPECT_EQ(0u, end.first);
  EXPECT_TRUE(end.second) << "the callback form, as the asyncio client awaits it";
}

namespace {

// A synchronous client that holds on to its connections' objects, as a
// connection does itself for a while after a failed write, reading to the
// end.
struct HoldingClient : multiplexer::Client {
  explicit HoldingClient(std::uint32_t type) : multiplexer::Client(type) {}
  std::vector<std::shared_ptr<void>> hold_connections() {
    std::vector<std::shared_ptr<void>> held;
    for (auto entry = basic_client_->begin(); entry != basic_client_->end(); ++entry) {
      if (std::shared_ptr<void> conn = entry->second.lock()) {
        held.push_back(conn);
      }
    }
    return held;
  }
  multiplexer::MultiplexerMessage message(const std::string& payload) {
    multiplexer::MultiplexerMessage msg;
    msg.set_id(random64());
    msg.set_from(instance_id());
    msg.set_type(multiplexer::types::PYTHON_TEST_REQUEST);
    msg.set_message(payload);
    return msg;
  }
};

}  // namespace

// A pinned lane reads closed from the moment its connection stops being
// live, though the connection's object lives on, where it read closed only
// once that object was gone, and a ThreadedClient send through it
// meanwhile was dropped rather than refused. So does a lane seeded with
// the connection, once a message went through it.
TEST(Lane, APinnedLaneIsClosedOnceItsConnectionDiesThoughTheConnectionLingers) {
  std::unique_ptr<InProcessMultiplexer> mx(new InProcessMultiplexer());
  HoldingClient client(multiplexer::peers::WEBSITE);
  multiplexer::ConnectionWrapper connection = client.connect("127.0.0.1", mx->port, 5);
  ASSERT_TRUE(connection);
  multiplexer::LanePtr pinned = std::make_shared<multiplexer::Lane>(true);
  multiplexer::LanePtr seeded = std::make_shared<multiplexer::Lane>(connection, true);
  ASSERT_TRUE(client.queue(client.message("through the pinned lane"), 5, pinned));
  ASSERT_TRUE(client.queue(client.message("through the seeded lane"), 5, seeded));
  ASSERT_TRUE(client.flush_all(5));
  ASSERT_TRUE(pinned->connected());
  ASSERT_TRUE(seeded->connected());
  std::vector<std::shared_ptr<void>> held = client.hold_connections();
  ASSERT_EQ(1u, held.size());
  mx.reset();  // the multiplexer is gone; the client notices in a call that runs the loop
  for (int tries = 0; tries < 200 && client.connections_count() > 0; ++tries) {
    try {
      client.receive_message(0.05f);
    } catch (const std::exception&) {
    }
  }
  ASSERT_EQ(0u, client.connections_count());
  EXPECT_FALSE(pinned->connected());
  EXPECT_TRUE(pinned->closed()) << "its connection's object still exists";
  EXPECT_TRUE(seeded->closed());
}

// shutdown() writes what was sent before it, in both clients: sent to a
// frozen multiplexer, whose connection fills and leaves messages waiting,
// and which reads again while the shutdown waits, every message arrives
// and none is reported, where shutdown() dropped what it had not written.
// The multiplexer is let go from another thread once the shutdown began.
static void expect_shutdown_writes_what_was_sent(bool threaded) {
  InProcessMultiplexer mx;
  IdSink sink(mx.port);
  std::atomic<int> reported(0);  // before the clients, which report into it until they are gone
  std::set<std::uint64_t> sent;
  ThreadedClient threaded_client(multiplexer::peers::WEBSITE);
  Peer sync_client(mx.port, multiplexer::peers::WEBSITE);
  threaded_client.set_drop_observer([&reported](std::uint64_t, multiplexer::DropReason) { ++reported; });
  sync_client.client.set_drop_observer([&reported](std::uint64_t, multiplexer::DropReason) { ++reported; });
  ASSERT_TRUE(threaded_client.connect("127.0.0.1", mx.port, 5));
  const std::string chunk(16 * 1024, 'x');
  Freeze freeze(mx);
  for (int index = 0; index < frames_to_fill(chunk.size()); ++index) {
    const multiplexer::MultiplexerMessage msg = sync_client.message(multiplexer::types::PYTHON_TEST_REQUEST, chunk, 0);
    sent.insert(msg.id());
    if (threaded) {
      threaded_client.send(msg);
    } else {
      sync_client.client.queue(msg, 60);
    }
  }
  std::thread release([&freeze] {
    std::this_thread::sleep_for(std::chrono::milliseconds(500));  // the shutdown below began by then
    freeze.release();
  });
  if (threaded) {
    threaded_client.shutdown(60);
  } else {
    sync_client.client.shutdown(60);
  }
  release.join();
  EXPECT_EQ(0, reported.load());
  EXPECT_EQ(0u, threaded ? threaded_client.dropped() : sync_client.client.dropped());
  EXPECT_EQ(sent, sink.wait_for(sent.size() - reported.load(), 60));  // what was not reported, at once
}

TEST(ThreadedClient, ShutdownWritesWhatWasSentBeforeIt) { expect_shutdown_writes_what_was_sent(true); }
TEST(Client, ShutdownWritesWhatWasSentBeforeIt) { expect_shutdown_writes_what_was_sent(false); }

// A message that waited for room past its timeout is told to the
// synchronous client's drop observer, in the call that ran the loop then.
TEST(Client, AMessageThatWaitedForRoomPastItsTimeoutIsReported) {
  InProcessMultiplexer mx;
  Sink backend(mx.port);
  // Before the client, which reports what its shutdown drops as it is destroyed.
  std::vector<std::pair<std::uint64_t, multiplexer::DropReason>> heard;
  Peer sender(mx.port, multiplexer::peers::WEBSITE);
  sender.client.set_drop_observer(
      [&heard](std::uint64_t id, multiplexer::DropReason reason) { heard.emplace_back(id, reason); });
  const std::string chunk(16 * 1024, 'x');
  Freeze freeze(mx);
  for (int index = 0; index < frames_to_fill(chunk.size()); ++index) {
    sender.client.schedule_one(sender.message(multiplexer::types::PYTHON_TEST_REQUEST, chunk, 0));
  }
  const multiplexer::MultiplexerMessage late = sender.message(multiplexer::types::PYTHON_TEST_REQUEST, "late", 0);
  sender.client.schedule_one(late, 0.3f);
  EXPECT_FALSE(sender.client.flush_all(0.6f));  // the loop runs past its deadline
  ASSERT_EQ(1u, heard.size());
  EXPECT_EQ(late.id(), heard[0].first);
  EXPECT_EQ(multiplexer::DropReason::NO_ROOM, heard[0].second);
  EXPECT_EQ(1u, sender.client.dropped());
}

// The synchronous client's queue() holds a message while no multiplexer is
// reachable and writes it once one is back, as ThreadedClient's send()
// does, where schedule_one() has nothing to put it on: its tracker reads
// written then, and the message arrives.
TEST(Client, QueueHoldsAMessageUntilAConnectionComesUp) {
  std::unique_ptr<InProcessMultiplexer> mx(new InProcessMultiplexer());
  const unsigned short port = mx->port;
  Peer sender(port, multiplexer::peers::WEBSITE);
  mx.reset();  // gone; the client notices in its next call
  const multiplexer::MultiplexerMessage held = sender.message(multiplexer::types::PYTHON_TEST_REQUEST, "held", 0);
  multiplexer::Client::ScheduledMessageTracker tracker = sender.client.queue(held, 30);
  ASSERT_TRUE(tracker) << "held, not refused";
  EXPECT_TRUE(tracker.in_queue());
  mx.reset(new InProcessMultiplexer(port));
  PayloadSink backend(port);  // registered before the client, which reconnects only inside its calls
  EXPECT_TRUE(sender.client.flush_all(15));
  EXPECT_TRUE(tracker.is_sent());
  EXPECT_TRUE(backend.wait_for("held", 10));
  EXPECT_EQ(0u, sender.client.dropped());
}
