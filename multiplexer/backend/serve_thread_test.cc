// A backend built on one thread, connected and served from another: serve_forever()
// adopts the serving thread, so the debug-build thread checks, which bind a
// client and its connections to the thread that made them, do not fire. And
// a PING whose echo would be over MAX_MESSAGE_SIZE, answered with
// BACKEND_ERROR by a backend that goes on serving.
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <set>
#include <string>
#include <thread>

#include "lib/kwargs.h"
#include "multiplexer/backend/base_multiplexer_server.h"
#include "multiplexer/in_process_multiplexer.h"
#include "multiplexer/multiplexer.constants.h"

using multiplexer::backend::BaseMultiplexerServer;
using multiplexer::backend::MultiplexerAddresses;
using multiplexer::testing::InProcessMultiplexer;

namespace {

// Serves three iterations, then stops.
class CountingBackend : public BaseMultiplexerServer {
 public:
  explicit CountingBackend(const MultiplexerAddresses& addresses)
      : BaseMultiplexerServer(addresses, multiplexer::peers::PYTHON_TEST_SERVER) {}
  int iterations = 0;
  multiplexer::Client* conn_for_test() { return conn; }
  void close_for_test() { close(); }

 protected:
  void handle_message(multiplexer::MultiplexerMessage&) override { no_response(); }
  void periodic_task() override {
    if (++iterations >= 3) {
      working = false;
    }
  }
};

// Answers every request after a pause, so that the rest queue up; drains
// when `leave` is set.
class SlowBackend : public BaseMultiplexerServer {
 public:
  explicit SlowBackend(const MultiplexerAddresses& addresses)
      : BaseMultiplexerServer(addresses, multiplexer::peers::PYTHON_TEST_SERVER) {}
  std::atomic<int> handled{0};
  std::atomic<bool> leave{false};
  std::atomic<bool> serving{false};  // connected and looping: the first periodic_task() ran

 protected:
  void handle_message(multiplexer::MultiplexerMessage& mxmsg) override {
    ++handled;
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    send_message(mx::util::kwargs::Kwargs()
                     .set("message", mxmsg.message())
                     .set("type", static_cast<std::uint32_t>(multiplexer::types::PYTHON_TEST_RESPONSE)));
  }
  void periodic_task() override {
    serving = true;
    if (leave.load() && !draining()) {
      start_draining();
    }
  }
};

// Treats every handler exception as fatal, and counts them: serve_forever()
// ends at the first.
class StrictBackend : public BaseMultiplexerServer {
 public:
  explicit StrictBackend(const MultiplexerAddresses& addresses)
      : BaseMultiplexerServer(addresses, multiplexer::peers::PYTHON_TEST_SERVER) {}
  multiplexer::Client* conn_for_test() { return conn; }
  std::atomic<int> exceptions{0};
  std::atomic<bool> serving{false};
  std::atomic<bool> leave{false};

 protected:
  void handle_message(multiplexer::MultiplexerMessage&) override { no_response(); }
  bool on_handler_exception(const std::exception&) override {
    ++exceptions;
    return false;
  }
  void periodic_task() override {
    serving = true;
    if (leave.load()) {
      working = false;
    }
  }
};

// The next message `client` receives that references `id`, or null.
std::shared_ptr<multiplexer::MultiplexerMessage> reply_to(multiplexer::Client& client, std::uint64_t id) {
  const std::chrono::steady_clock::time_point deadline = std::chrono::steady_clock::now() + std::chrono::seconds(20);
  while (std::chrono::steady_clock::now() < deadline) {
    try {
      std::shared_ptr<multiplexer::MultiplexerMessage> got = client.receive_message(1).first;
      if (got->references() == id) {
        return got;
      }
    } catch (const multiplexer::Client::OperationTimedOut&) {
    }
  }
  return nullptr;
}

}  // namespace

// A PING is answered with its payload echoed. One whose echo, a
// `references` field longer, would be over MAX_MESSAGE_SIZE threw where the
// echo was built: no answer, and on_handler_exception(), which could end
// serve_forever(). It is now answered with BACKEND_ERROR saying why, and
// the backend serves on: the next PING has its echo.
TEST(ServeThread, APingWhoseEchoWouldBeTooBigIsAnsweredWithBackendError) {
  InProcessMultiplexer mx;
  MultiplexerAddresses addresses;
  addresses.push_back(std::make_pair("127.0.0.1", mx.port));
  StrictBackend backend(addresses);
  std::atomic<bool> ended_by_exception(false);
  std::thread server([&backend, &ended_by_exception] {
    try {
      backend.serve_forever(0.05f, 1.0f);
    } catch (const std::exception&) {
      ended_by_exception = true;
    }
  });
  for (int waited = 0; waited < 500 && !backend.serving.load(); ++waited) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  ASSERT_TRUE(backend.serving.load());
  multiplexer::Client client(multiplexer::peers::WEBSITE);
  ASSERT_TRUE(client.connect("127.0.0.1", mx.port, 5));
  multiplexer::MultiplexerMessage ping;
  ping.set_id(client.random64());
  ping.set_from(client.instance_id());
  ping.set_to(backend.conn_for_test()->instance_id());
  ping.set_type(multiplexer::types::PING);
  ping.set_message(std::string(multiplexer::MAX_MESSAGE_SIZE - ping.ByteSizeLong() - 16, 'p'));
  while (ping.ByteSizeLong() < multiplexer::MAX_MESSAGE_SIZE) {
    ping.mutable_message()->push_back('p');
  }
  client.flush(client.schedule_one(ping), 10);
  std::shared_ptr<multiplexer::MultiplexerMessage> answer = reply_to(client, ping.id());
  ASSERT_TRUE(answer) << "no answer to the PING";
  EXPECT_EQ(multiplexer::types::BACKEND_ERROR, answer->type());
  EXPECT_NE(std::string::npos, answer->message().find("echo")) << answer->message();
  ping.set_id(client.random64());
  ping.set_message("bounce");
  client.flush(client.schedule_one(ping), 10);
  answer = reply_to(client, ping.id());
  ASSERT_TRUE(answer) << "no echo after the big PING";
  EXPECT_EQ("bounce", answer->message());
  backend.leave = true;
  server.join();
  EXPECT_EQ(0, backend.exceptions.load());
  EXPECT_FALSE(ended_by_exception.load());
}

// A draining BaseMultiplexerServer serves what it had already read: the
// requests the synchronous client pulled off the socket while a reply was
// being sent are handled before the connections close. Every request sent is
// therefore either answered or, routed after the multiplexer applied the
// drain routing, refused by the multiplexer with a delivery error: none
// vanishes into a timeout.
TEST(ServeThread, ADrainServesWhatWasAlreadyRead) {
  InProcessMultiplexer mx;
  MultiplexerAddresses addresses;
  addresses.push_back(std::make_pair("127.0.0.1", mx.port));
  SlowBackend backend(addresses);
  std::thread server([&backend] { backend.serve_forever(0.05f, 10.0f); });
  for (int waited = 0; waited < 500 && !backend.serving.load(); ++waited) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));  // registered before anything is sent
  }
  ASSERT_TRUE(backend.serving.load());
  multiplexer::Client client(multiplexer::peers::WEBSITE);
  ASSERT_TRUE(client.connect("127.0.0.1", mx.port, 5));
  std::set<std::uint64_t> sent;
  for (int index = 0; index < 12; ++index) {
    multiplexer::MultiplexerMessage msg;
    msg.set_id(client.random64());
    msg.set_from(client.instance_id());
    msg.set_type(multiplexer::types::PYTHON_TEST_REQUEST);
    msg.set_message("r" + std::to_string(index));
    sent.insert(msg.id());
    client.flush(client.schedule_one(msg), 5);
  }
  for (int waited = 0; waited < 500 && backend.handled.load() < 2; ++waited) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  const std::chrono::steady_clock::time_point started = std::chrono::steady_clock::now();
  backend.leave = true;
  server.join();
  EXPECT_LT(std::chrono::steady_clock::now() - started, std::chrono::seconds(5)) << "on the confirmation, not the cap";
  // Every request has an outcome: a response from the backend, or a
  // delivery error from the multiplexer for one routed after the drain.
  int responses = 0;
  int refused = 0;
  const std::chrono::steady_clock::time_point deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (responses + refused < 12 && std::chrono::steady_clock::now() < deadline) {
    try {
      std::pair<std::shared_ptr<multiplexer::MultiplexerMessage>, multiplexer::ConnectionWrapper> got =
          client.receive_message(0.5f);
      if (!sent.count(got.first->references())) {
        continue;
      }
      sent.erase(got.first->references());
      if (got.first->type() == multiplexer::types::DELIVERY_ERROR) {
        ++refused;
      } else {
        ++responses;
      }
    } catch (const multiplexer::Client::OperationTimedOut&) {
    }
  }
  EXPECT_EQ(12, responses + refused) << "none vanished into a timeout";
  EXPECT_EQ(backend.handled.load(), responses) << "what the backend served was answered";
  EXPECT_GE(backend.handled.load(), 2);
}

TEST(ServeThread, BuiltOnOneThreadServedFromAnother) {
  InProcessMultiplexer mx;
  MultiplexerAddresses addresses;
  addresses.push_back(std::make_pair("127.0.0.1", mx.port));
  CountingBackend backend(addresses);  // built on this thread; serve_forever() connects on the other
  std::thread server([&backend] { backend.serve_forever(0.1f); });
  server.join();
  EXPECT_EQ(3, backend.iterations);
}

TEST(ServeThread, ExplicitRebindForOwnLoop) {
  InProcessMultiplexer mx;
  MultiplexerAddresses addresses;
  addresses.push_back(std::make_pair("127.0.0.1", mx.port));
  CountingBackend backend(addresses);
  std::thread driver([&backend] {
    backend.conn_for_test()->bind_to_current_thread();
    backend.connect();  // what serve_forever() would have done first
    for (int i = 0; i < 3; ++i) {
      try {
        backend.loop_iter(0.1f);
      } catch (multiplexer::Client::OperationTimedOut&) {
      }
    }
    backend.close_for_test();  // the owning thread closes; destroying on another would fail the check
  });
  driver.join();
}

TEST(ServeThread, NothingIsConnectedBeforeServeForever) {
  InProcessMultiplexer mx;
  MultiplexerAddresses addresses;
  addresses.push_back(std::make_pair("127.0.0.1", mx.port));
  CountingBackend backend(addresses);
  EXPECT_NE(0u, backend.conn_for_test()->instance_id()) << "the id is known before serving";
  EXPECT_EQ(0u, backend.conn_for_test()->connections_count()) << "and nothing is connected";
  backend.connect();  // a program that announces itself before serving
  EXPECT_EQ(1u, backend.conn_for_test()->connections_count());
  std::thread server([&backend] { backend.serve_forever(0.1f); });  // connects nothing more
  server.join();
  EXPECT_EQ(3, backend.iterations);
}
