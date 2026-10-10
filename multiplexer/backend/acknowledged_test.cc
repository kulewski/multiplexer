// A change of the routing or the key filters, once acknowledged, as an event
// in every C++ class: the callback runs once, after every message routed to
// the peer before the change was handed out (received from SyncClient,
// passed to ThreadedClient's on_message, handled by the server classes, on
// their loop's thread), and the server classes' loops wake at the
// acknowledgement rather than at their next poll, an hour away here, for a
// drain as for a change of filters. The Python mirror is
// multiplexer/acknowledged_test.py. Counted, not timed: the sender's own
// marker coming back says the multiplexer routed everything it sent
// before; a held handler says what a threaded server still has; a bound
// on a wait only detects a failure.
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <future>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "multiplexer/backend/base_multiplexer_server.h"
#include "multiplexer/backend/base_threaded_multiplexer_server.h"
#include "multiplexer/in_process_multiplexer.h"
#include "multiplexer/multiplexer.constants.h"
#include "multiplexer/threaded_client.h"

using multiplexer::Client;
using multiplexer::MultiplexerMessage;
using multiplexer::Routing;
using multiplexer::ThreadedClient;
using multiplexer::backend::BaseMultiplexerServer;
using multiplexer::backend::BaseThreadedMultiplexerServer;
using multiplexer::backend::MultiplexerAddresses;
using multiplexer::backend::RequestPtr;
using multiplexer::testing::InProcessMultiplexer;

namespace {

const int EVENTS = 50;
const float HOUR = 3600.0f;            // a poll no test waits out: the acknowledgement has to end the wait
const std::chrono::seconds BOUND(30);  // what only a failure reaches

Routing all_off() {
  Routing routing;
  routing.set_all(false);
  return routing;
}

// An event from `sender`, addressed to `to` when given.
MultiplexerMessage event(Client& sender, const std::string& payload, std::uint64_t to = 0) {
  MultiplexerMessage msg;
  msg.set_id(sender.random64());
  msg.set_sender(sender.instance_id());
  msg.set_type(multiplexer::types::TEST_EVENT);
  msg.set_message(payload);
  if (to) {
    msg.set_to(to);
  }
  return msg;
}

// EVENTS events, numbered, routed to every event backend there is: a marker
// the sender addresses to itself comes back after them, since frames from
// one connection are routed in their order.
void send_routed(Client& sender) {
  for (int index = 0; index < EVENTS; ++index) {
    sender.flush(sender.schedule_one(event(sender, std::to_string(index))));
  }
  const MultiplexerMessage marker = event(sender, "marker", sender.instance_id());
  sender.flush(sender.schedule_one(marker));
  while (sender.receive_message(30).first->id() != marker.id()) {
  }
}

// A count other threads wait for, and the threads that counted.
class Counter {
 public:
  void add() {
    std::lock_guard<std::mutex> lock(mutex_);
    ++value_;
    threads_.push_back(std::this_thread::get_id());
    changed_.notify_all();
  }
  // Waits until the count is past `value`: the count, or -1 at the bound.
  int wait_past(int value) {
    std::unique_lock<std::mutex> lock(mutex_);
    if (!changed_.wait_for(lock, BOUND, [&] { return value_ > value; })) {
      return -1;
    }
    return value_;
  }
  int value() {
    std::lock_guard<std::mutex> lock(mutex_);
    return value_;
  }
  std::thread::id thread(int index) {
    std::lock_guard<std::mutex> lock(mutex_);
    return threads_.at(index);
  }

 private:
  std::mutex mutex_;
  std::condition_variable changed_;
  int value_ = 0;
  std::vector<std::thread::id> threads_;
};

TEST(Acknowledged, SyncClientHearsItInTheReceiveAfterEverythingReceived) {
  InProcessMultiplexer mx;
  Client receiver(multiplexer::peers::TEST_EVENT_BACKEND);
  ASSERT_TRUE(receiver.connect("127.0.0.1", mx.port, 5));
  Client sender(multiplexer::peers::TEST_EVENT_CLIENT);
  ASSERT_TRUE(sender.connect("127.0.0.1", mx.port, 5));
  send_routed(sender);
  std::vector<std::string> received;
  std::vector<std::size_t> heard;
  receiver.set_routing(all_off(), [&] { heard.push_back(received.size()); });
  // What the receiver sends after the change reaches the sender after the
  // multiplexer took the change, whose confirmation is on its way to the
  // receiver by then; the next send polls the receiver's sockets first, as
  // a server's reply does, and reads it with the events, before any of
  // them is received.
  receiver.flush(receiver.schedule_one(event(receiver, "after", sender.instance_id())));
  while (sender.receive_message(30).first->message() != "after") {
  }
  receiver.flush(receiver.schedule_one(event(receiver, "poll", sender.instance_id())));
  while (received.size() < std::size_t(EVENTS)) {  // every one routed before the change, then nothing
    received.push_back(receiver.receive_message(30).first->message());
    ASSERT_TRUE(heard.empty()) << "not while a message routed before was still to be received";
  }
  for (int tries = 0; tries < 600 && heard.empty(); ++tries) {  // the caller back for more, as a loop is
    try {
      received.push_back(receiver.receive_message(0.05f).first->message());
    } catch (const Client::OperationTimedOut&) {
    }
  }
  ASSERT_EQ(std::vector<std::size_t>{std::size_t(EVENTS)}, heard);
  for (int index = 0; index < EVENTS; ++index) {
    EXPECT_EQ(std::to_string(index), received[index]);
  }
  EXPECT_TRUE(receiver.routing_acknowledged());
}

TEST(Acknowledged, ThreadedClientHearsItAfterOnMessageHadEverything) {
  InProcessMultiplexer mx;
  std::atomic<int> got{0};
  ThreadedClient receiver(multiplexer::peers::TEST_EVENT_BACKEND,
                          [&](const multiplexer::IncomingMessage&) { got.fetch_add(1); });
  ASSERT_TRUE(receiver.connect("127.0.0.1", mx.port, 5));  // registered once it returns
  Client sender(multiplexer::peers::TEST_EVENT_CLIENT);
  ASSERT_TRUE(sender.connect("127.0.0.1", mx.port, 5));
  send_routed(sender);
  Counter heard;
  std::atomic<int> counted{-1};
  receiver.set_routing(all_off(), [&] {
    counted = got.load();
    heard.add();
  });
  ASSERT_EQ(1, heard.wait_past(0));
  EXPECT_EQ(EVENTS, counted.load()) << "every one went to on_message first, on the same io thread";
  receiver.set_key_filters({{multiplexer::types::TEST_EVENT, {}}}, [&] { heard.add(); });  // and the next change
  EXPECT_EQ(2, heard.wait_past(1));
}

// Answers, and is asked to leave by its first request.
class Leaving : public BaseMultiplexerServer {
 public:
  explicit Leaving(const MultiplexerAddresses& addresses)
      : BaseMultiplexerServer(addresses, multiplexer::peers::PYTHON_TEST_SERVER) {}

 protected:
  void handle_message(MultiplexerMessage&) override {
    send_message(mx::util::kwargs::Kwargs()
                     .set("message", std::string("ok"))
                     .set("type", static_cast<std::uint32_t>(multiplexer::types::PYTHON_TEST_RESPONSE)));
    start_draining();
  }
};

TEST(Acknowledged, AServerDrainsAtTheAcknowledgementNotAtItsPoll) {
  InProcessMultiplexer mx;
  Leaving backend({{"127.0.0.1", mx.port}});
  backend.connect();  // registered once it returns; served from the thread below
  std::promise<void> left;
  std::thread serving([&] {
    backend.serve_forever(HOUR, HOUR);
    left.set_value();
  });
  ThreadedClient asking(multiplexer::peers::WEBSITE);
  ASSERT_TRUE(asking.connect("127.0.0.1", mx.port, 5));
  EXPECT_EQ(ThreadedClient::REPLIED, asking.query("leave", multiplexer::types::PYTHON_TEST_REQUEST, 30).outcome);
  const bool drained = left.get_future().wait_for(BOUND) == std::future_status::ready;
  if (!drained) {
    backend.stop();  // the test fails; the thread is left to the process's end
    serving.detach();
  } else {
    serving.join();
  }
  EXPECT_TRUE(drained) << "the drain over at the multiplexer's confirmation";
}

// Changes its filters when asked, its callback noting where it runs, and
// stops itself when asked, its loop's wait being an hour.
class Filtering : public BaseMultiplexerServer {
 public:
  explicit Filtering(const MultiplexerAddresses& addresses, Counter& heard)
      : BaseMultiplexerServer(addresses, multiplexer::peers::PYTHON_TEST_SERVER), heard_(heard) {}
  std::atomic<bool> in_callback_while_handling{false};

 protected:
  void handle_message(MultiplexerMessage& mxmsg) override {
    handling_ = true;
    if (mxmsg.message() == "stop") {
      working = false;
    } else {
      set_key_filters({{multiplexer::types::PYTHON_TEST_REQUEST, {"one.*"}}}, [this] {
        in_callback_while_handling = in_callback_while_handling || handling_;
        heard_.add();
      });
    }
    send_message(mx::util::kwargs::Kwargs()
                     .set("message", std::string("ok"))
                     .set("type", static_cast<std::uint32_t>(multiplexer::types::PYTHON_TEST_RESPONSE)));
    handling_ = false;
  }

 private:
  Counter& heard_;
  bool handling_ = false;
};

TEST(Acknowledged, AServersCallbackRunsOnItsLoopBetweenMessages) {
  InProcessMultiplexer mx;
  Counter heard;
  Filtering backend({{"127.0.0.1", mx.port}}, heard);
  backend.connect();
  std::thread serving([&] { backend.serve_forever(HOUR); });
  const std::thread::id loop = serving.get_id();
  ThreadedClient asking(multiplexer::peers::WEBSITE);
  ASSERT_TRUE(asking.connect("127.0.0.1", mx.port, 5));
  MultiplexerMessage change = asking.new_message(multiplexer::types::PYTHON_TEST_REQUEST, "change");
  change.set_key("one");
  EXPECT_EQ(ThreadedClient::REPLIED, asking.query(change, 30).outcome);
  EXPECT_EQ(1, heard.wait_past(0));
  EXPECT_EQ(loop, heard.thread(0)) << "on the loop's thread";
  EXPECT_FALSE(backend.in_callback_while_handling.load()) << "between messages, not in a handler";
  MultiplexerMessage stop = asking.new_message(multiplexer::types::PYTHON_TEST_REQUEST, "stop");
  stop.set_key("one");
  EXPECT_EQ(ThreadedClient::REPLIED, asking.query(stop, 30).outcome);
  serving.join();
}

// Holds the request it is told to until the test releases it; counts the
// loop's turns.
class Held : public BaseThreadedMultiplexerServer {
 public:
  explicit Held(const MultiplexerAddresses& addresses)
      : BaseThreadedMultiplexerServer(addresses, multiplexer::peers::PYTHON_TEST_SERVER, options()) {}
  static Options options() {
    Options options;
    options.workers = 2;
    return options;
  }
  std::promise<void> holding;
  std::promise<void> release;
  std::shared_future<void> released{release.get_future().share()};
  Counter turns;

 protected:
  void handle_message(const RequestPtr& request) override {
    if (request->mxmsg().message() == "hold") {
      holding.set_value();
      released.wait_for(BOUND);
    }
    request->reply("ok", multiplexer::types::PYTHON_TEST_RESPONSE);
  }
  void periodic_task() override { turns.add(); }
};

TEST(Acknowledged, AThreadedServerHearsItOnceItsWorkersFinishedWhatCameBefore) {
  InProcessMultiplexer mx;
  Held server({{"127.0.0.1", mx.port}});
  server.connect();
  std::thread serving([&] { server.serve_forever(HOUR); });
  const std::thread::id loop = serving.get_id();
  ThreadedClient asking(multiplexer::peers::WEBSITE);
  ASSERT_TRUE(asking.connect("127.0.0.1", mx.port, 5));
  std::promise<ThreadedClient::Outcome> answered;
  asking.query(
      "hold", multiplexer::types::PYTHON_TEST_REQUEST,
      [&](const ThreadedClient::Result& result) { answered.set_value(result.outcome); }, 30);
  ASSERT_EQ(std::future_status::ready, server.holding.get_future().wait_for(BOUND))
      << "the request reached a worker, which holds it";
  Counter heard;
  const int before = server.turns.value();
  server.set_key_filters({{multiplexer::types::PYTHON_TEST_REQUEST, {}}}, [&] { heard.add(); });
  ASSERT_LT(before, server.turns.wait_past(before)) << "woken by the acknowledgement, which it has looked at";
  EXPECT_EQ(0, heard.value()) << "not while a request from before is in a worker";
  server.release.set_value();
  EXPECT_EQ(1, heard.wait_past(0));
  EXPECT_EQ(loop, heard.thread(0)) << "on serve_forever()'s thread";
  EXPECT_EQ(ThreadedClient::REPLIED, answered.get_future().get());
  server.stop();
  serving.join();
}

}  // namespace
