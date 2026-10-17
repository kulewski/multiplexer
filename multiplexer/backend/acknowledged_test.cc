// A change of the routing or the key filters, once acknowledged, as an event
// in every C++ class: the callback runs once, after every message routed to
// the peer before the change was handed out (received from SyncClient,
// passed to ThreadedClient's on_message, handled by the server classes, on
// their loop's thread), and the server classes' loops wake at the
// acknowledgement rather than at their next poll, an hour away here, for a
// drain as for a change of filters. A connection's welcome confirms the
// routing it carries, so a client is acknowledged once connected, a change
// made before connecting is heard after it, and a drain survives its
// multiplexer's restart. A drain also waits for what its server holds for
// a connection, every multiplexer down, and ends on its cap whatever the
// poll. The Python mirror is
// multiplexer/acknowledged_test.py. Counted, not timed: the sender's own
// marker coming back says the multiplexer routed everything it sent
// before; a held handler says what a threaded server still has; a bound
// on a wait only detects a failure.
#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <future>
#include <memory>
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
                     .set("type", multiplexer::types::PYTHON_TEST_RESPONSE));
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
                     .set("type", multiplexer::types::PYTHON_TEST_RESPONSE));
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

// Waits until `condition` holds, BOUND at most; whether it does. For a state
// no event tells the test of: how many connections a client has.
template <typename Condition>
bool eventually(Condition condition) {
  const std::chrono::steady_clock::time_point until = std::chrono::steady_clock::now() + BOUND;
  while (!condition()) {
    if (std::chrono::steady_clock::now() > until) {
      return false;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  return true;
}

// A connection's welcome carries the client's routing, which the
// multiplexer takes with it: once connected, with no change made and
// nothing received, the routing reads acknowledged.
TEST(Acknowledged, AConnectionsWelcomeConfirmsTheRouting) {
  InProcessMultiplexer mx;
  Client sync(multiplexer::peers::TEST_EVENT_BACKEND);
  ASSERT_TRUE(sync.connect("127.0.0.1", mx.port, 5));
  EXPECT_TRUE(sync.routing_acknowledged());
  ThreadedClient threaded(multiplexer::peers::TEST_EVENT_BACKEND);
  ASSERT_TRUE(threaded.connect("127.0.0.1", mx.port, 5));  // registered once it returns
  EXPECT_TRUE(threaded.routing_acknowledged());
}

// A change made with no connection is in effect at once, and its callback
// runs at the next receive; a connection made before that receive carries
// the change in its welcome and takes nothing back.
TEST(Acknowledged, AChangeMadeBeforeConnectingIsHeardAfter) {
  InProcessMultiplexer mx;
  Client receiver(multiplexer::peers::TEST_EVENT_BACKEND);
  int heard = 0;
  receiver.set_key_filters({{multiplexer::types::TEST_EVENT, {}}}, [&] { ++heard; });
  ASSERT_TRUE(receiver.connect("127.0.0.1", mx.port, 5));
  for (int tries = 0; tries < 600 && !heard; ++tries) {  // the caller back for more, as a loop is
    try {
      receiver.receive_message(0.05f);
    } catch (const Client::OperationTimedOut&) {
    }
  }
  EXPECT_EQ(1, heard) << "at a receive after the connection";
  EXPECT_TRUE(receiver.routing_acknowledged());
}

// A threaded server drains with a request in a worker, the drain
// confirmed; its multiplexer restarts, and the connection that comes back
// carries the drain routing in its welcome, which confirms it again: the
// drain ends once the worker is done, not at its cap, an hour away. The
// worker is let go only once the connection is back, so that the drain
// cannot end while no multiplexer is connected.
TEST(Acknowledged, ADrainAcrossARestartEndsOnceItsWorkIsDone) {
  std::unique_ptr<InProcessMultiplexer> mx(new InProcessMultiplexer());
  const unsigned short port = mx->port;
  Held server({{"127.0.0.1", port}});
  server.connect();
  std::promise<void> left;
  std::thread serving([&] {
    server.serve_forever(HOUR, HOUR);
    left.set_value();
  });
  Client asking(multiplexer::peers::WEBSITE);
  ASSERT_TRUE(asking.connect("127.0.0.1", port, 5));
  MultiplexerMessage hold;
  hold.set_id(asking.random64());
  hold.set_sender(asking.instance_id());
  hold.set_type(multiplexer::types::PYTHON_TEST_REQUEST);
  hold.set_message("hold");
  asking.flush(asking.schedule_one(hold));
  ASSERT_EQ(std::future_status::ready, server.holding.get_future().wait_for(BOUND))
      << "the request reached a worker, which holds it";
  server.start_draining();
  EXPECT_TRUE(eventually([&] { return server.client().routing_acknowledged(); })) << "the drain confirmed";
  mx.reset();  // stopped: every connection to it ends
  EXPECT_TRUE(eventually([&] { return server.client().connections_count() == 0; }));
  mx.reset(new InProcessMultiplexer(port));  // back on the same address
  EXPECT_TRUE(eventually([&] { return server.client().connections_count() == 1; })) << "reconnected";
  server.release.set_value();
  std::future<void> leaving = left.get_future();
  const bool drained = leaving.wait_for(BOUND) == std::future_status::ready;
  EXPECT_TRUE(drained) << "the drain over once the worker is done";
  if (!drained) {
    server.stop();  // the test fails; the stop ends the loop at its next look
  }
  serving.join();
}

// The request a test holds: what Held and HeldPlain hold until released.
MultiplexerMessage hold_request(Client& asking) {
  MultiplexerMessage hold;
  hold.set_id(asking.random64());
  hold.set_sender(asking.instance_id());
  hold.set_type(multiplexer::types::PYTHON_TEST_REQUEST);
  hold.set_message("hold");
  return hold;
}

// A threaded server drains while its only multiplexer is down, a request
// in a worker: the worker's reply is held for the next connection, and the
// drain waits for it, the multiplexer back, where the confirmation, with
// no connection, held at once and the close dropped the reply. Counted:
// what the server's client gave up on.
TEST(Acknowledged, ADrainWaitsForWhatItHoldsForAConnection) {
  std::unique_ptr<InProcessMultiplexer> mx(new InProcessMultiplexer());
  const unsigned short port = mx->port;
  Held server({{"127.0.0.1", port}});
  server.connect();
  std::promise<void> left;
  std::thread serving([&] {
    server.serve_forever(HOUR, HOUR);
    left.set_value();
  });
  Client asking(multiplexer::peers::WEBSITE);
  ASSERT_TRUE(asking.connect("127.0.0.1", port, 5));
  asking.flush(asking.schedule_one(hold_request(asking)));
  ASSERT_EQ(std::future_status::ready, server.holding.get_future().wait_for(BOUND))
      << "the request reached a worker, which holds it";
  mx.reset();  // stopped: every connection to it ends
  EXPECT_TRUE(eventually([&] { return server.client().connections_count() == 0; }));
  server.start_draining();
  server.release.set_value();
  EXPECT_TRUE(eventually([&] {
    try {
      return server.client().held_for_a_connection() == 1;
    } catch (const ThreadedClient::NotConnected&) {
      return true;  // closed, the reply not waited for: dropped() below says so
    }
  })) << "the reply held";
  mx.reset(new InProcessMultiplexer(port));  // back on the same address
  std::future<void> leaving = left.get_future();
  const bool drained = leaving.wait_for(BOUND) == std::future_status::ready;
  EXPECT_TRUE(drained) << "the drain over once the reply went";
  if (!drained) {
    server.stop();  // the test fails; the stop ends the loop at its next look
  }
  serving.join();
  EXPECT_EQ(0u, server.client().dropped()) << "the reply written once a connection was back, not dropped";
}

// Starts draining at the request it is told to hold, holds it until the
// test releases it, then answers it; notes, on its loop's thread, the most
// messages its client held for a connection. Built on the test's client,
// which the close shuts down and leaves to the test.
class HeldPlain : public BaseMultiplexerServer {
 public:
  explicit HeldPlain(Client* client) : BaseMultiplexerServer(client, multiplexer::peers::PYTHON_TEST_SERVER) {}
  std::promise<void> holding;
  std::promise<void> release;
  std::atomic<std::size_t> held{0};

 protected:
  void handle_message(MultiplexerMessage& mxmsg) override {
    if (mxmsg.message() == "hold") {
      start_draining();
      holding.set_value();
      release.get_future().wait_for(BOUND);
    }
    send_message(mx::util::kwargs::Kwargs()
                     .set("message", std::string("ok"))
                     .set("type", multiplexer::types::PYTHON_TEST_RESPONSE));
  }
  void periodic_task() override { held = std::max(held.load(), conn->held_for_a_connection()); }
};

// The same for the plain server: its reply, made as its only multiplexer
// went down, held for the next connection, which the drain waits for.
TEST(Acknowledged, APlainDrainWaitsForWhatItHoldsForAConnection) {
  std::unique_ptr<InProcessMultiplexer> mx(new InProcessMultiplexer());
  const unsigned short port = mx->port;
  Client client(multiplexer::peers::PYTHON_TEST_SERVER);
  ASSERT_TRUE(client.connect("127.0.0.1", port, 5));
  const std::uint64_t server_id = client.instance_id();
  HeldPlain server(&client);
  std::promise<void> left;
  std::thread serving([&] {
    server.serve_forever(HOUR, HOUR);
    left.set_value();
  });
  Client asking(multiplexer::peers::WEBSITE);
  ASSERT_TRUE(asking.connect("127.0.0.1", port, 5));
  asking.flush(asking.schedule_one(hold_request(asking)));
  ASSERT_EQ(std::future_status::ready, server.holding.get_future().wait_for(BOUND))
      << "the request reached the server, which holds it, draining";
  mx.reset();  // stopped: every connection to it ends
  server.release.set_value();
  EXPECT_TRUE(eventually([&] { return server.held.load() == 1; })) << "the reply held";
  mx.reset(new InProcessMultiplexer(port));  // back on the same address
  std::future<void> leaving = left.get_future();
  const bool drained = leaving.wait_for(BOUND) == std::future_status::ready;
  EXPECT_TRUE(drained) << "the drain over once the reply went";
  if (!drained) {
    server.stop();  // the test fails; a request addressed to the server wakes its loop to see the stop
    Client waking(multiplexer::peers::WEBSITE);  // `asking` has not reconnected: nothing drove it
    if (waking.connect("127.0.0.1", port, 5)) {
      MultiplexerMessage wake = hold_request(waking);
      wake.set_message("wake");
      wake.set_to(server_id);
      waking.flush(waking.schedule_one(wake));
    }
  }
  serving.join();
  EXPECT_EQ(0u, client.dropped()) << "the reply written once a connection was back, not dropped";
}

// As HeldPlain, but what it sends once released is an event that waits in
// a batch, its delay an hour.
class HeldBatch : public BaseMultiplexerServer {
 public:
  explicit HeldBatch(Client* client) : BaseMultiplexerServer(client, multiplexer::peers::PYTHON_TEST_SERVER) {
    Client::Batching batching;
    batching.max_delay = 3600;
    client->set_batching(multiplexer::types::TEST_EVENT, batching);
  }
  std::promise<void> holding;
  std::promise<void> release;
  std::atomic<std::size_t> held{0};

 protected:
  void handle_message(MultiplexerMessage& mxmsg) override {
    if (mxmsg.message() == "hold") {
      start_draining();
      holding.set_value();
      release.get_future().wait_for(BOUND);
    }
    MultiplexerMessage event;
    event.set_type(multiplexer::types::TEST_EVENT);
    event.set_message("batched");
    conn->queue(event);  // its id and sender filled in
  }
  void periodic_task() override { held = std::max(held.load(), conn->held_for_a_connection()); }
};

// A plain server's drain waits for what waits in a batch, its only
// multiplexer down: the drain lets the batch go, to be held for the next
// connection and written once it is back, where the drain ended at once,
// nothing being held, and the close dropped the batch.
TEST(Acknowledged, APlainDrainWaitsForWhatWaitsInABatch) {
  std::unique_ptr<InProcessMultiplexer> mx(new InProcessMultiplexer());
  const unsigned short port = mx->port;
  Client client(multiplexer::peers::PYTHON_TEST_SERVER);
  ASSERT_TRUE(client.connect("127.0.0.1", port, 5));
  const std::uint64_t server_id = client.instance_id();
  HeldBatch server(&client);
  std::promise<void> left;
  std::thread serving([&] {
    server.serve_forever(HOUR, HOUR);
    left.set_value();
  });
  Client asking(multiplexer::peers::WEBSITE);
  ASSERT_TRUE(asking.connect("127.0.0.1", port, 5));
  asking.flush(asking.schedule_one(hold_request(asking)));
  ASSERT_EQ(std::future_status::ready, server.holding.get_future().wait_for(BOUND))
      << "the request reached the server, which holds it, draining";
  mx.reset();  // stopped: every connection to it ends
  server.release.set_value();
  EXPECT_TRUE(eventually([&] { return server.held.load() == 1; })) << "the batch let go, and held";
  mx.reset(new InProcessMultiplexer(port));  // back on the same address
  std::future<void> leaving = left.get_future();
  const bool drained = leaving.wait_for(BOUND) == std::future_status::ready;
  EXPECT_TRUE(drained) << "the drain over once the batch went";
  if (!drained) {
    server.stop();  // the test fails; a request addressed to the server wakes its loop to see the stop
    Client waking(multiplexer::peers::WEBSITE);
    if (waking.connect("127.0.0.1", port, 5)) {
      MultiplexerMessage wake = hold_request(waking);
      wake.set_message("wake");
      wake.set_to(server_id);
      waking.flush(waking.schedule_one(wake));
    }
  }
  serving.join();
  EXPECT_EQ(0u, client.dropped()) << "the batch written once a connection was back, not dropped";
}

// Leaving, its drain keeping events coming: only the cap can end it.
class OpenLeaving : public Leaving {
 public:
  explicit OpenLeaving(const MultiplexerAddresses& addresses) : Leaving(addresses), id(conn->instance_id()) {
    Routing routing = drain_routing();
    routing.set_all(true);
    set_drain_routing(routing);
  }
  const std::uint64_t id;  // what a message addressed to it carries
};

// A drain that keeps a path open ends on its cap alone: with a poll of an
// hour the loop's wait ends at the cap, where nothing woke the loop after
// the confirmation and the drain went on for good.
TEST(Acknowledged, APlainDrainEndsOnItsCapWhateverThePoll) {
  InProcessMultiplexer mx;
  OpenLeaving backend({{"127.0.0.1", mx.port}});
  backend.connect();
  std::promise<void> left;
  std::thread serving([&] {
    backend.serve_forever(HOUR, 1.0f);
    left.set_value();
  });
  ThreadedClient asking(multiplexer::peers::WEBSITE);
  ASSERT_TRUE(asking.connect("127.0.0.1", mx.port, 5));
  EXPECT_EQ(ThreadedClient::REPLIED, asking.query("leave", multiplexer::types::PYTHON_TEST_REQUEST, 30).outcome);
  const bool drained = left.get_future().wait_for(BOUND) == std::future_status::ready;
  EXPECT_TRUE(drained) << "the drain over at its cap";
  if (!drained) {
    backend.stop();  // the test fails; a request addressed to it wakes the loop to see the stop
    MultiplexerMessage wake = asking.new_message(multiplexer::types::PYTHON_TEST_REQUEST, "wake");
    wake.set_to(backend.id);
    asking.query(wake, 30);
  }
  serving.join();
}

// A threaded server whose drain keeps events coming: only the cap can end
// it.
class OpenThreaded : public BaseThreadedMultiplexerServer {
 public:
  explicit OpenThreaded(const MultiplexerAddresses& addresses)
      : BaseThreadedMultiplexerServer(addresses, multiplexer::peers::PYTHON_TEST_SERVER, options()) {}
  static Options options() {
    Options options;
    options.drain_routing.set_all(true);
    return options;
  }

 protected:
  void handle_message(const RequestPtr& request) override { request->no_response(); }
};

TEST(Acknowledged, AThreadedDrainEndsOnItsCapWhateverThePoll) {
  InProcessMultiplexer mx;
  OpenThreaded server({{"127.0.0.1", mx.port}});
  server.connect();
  std::promise<void> left;
  std::thread serving([&] {
    server.serve_forever(HOUR, 1.0f);
    left.set_value();
  });
  server.start_draining();
  const bool drained = left.get_future().wait_for(BOUND) == std::future_status::ready;
  EXPECT_TRUE(drained) << "the drain over at its cap";
  if (!drained) {
    server.stop();  // the test fails; the stop ends the loop at its next look
  }
  serving.join();
}

}  // namespace
