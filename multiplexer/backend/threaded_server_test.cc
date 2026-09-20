// BaseThreadedMultiplexerServer against a Server in this process: serial
// order with one worker, four requests at once with four, a reply from
// another thread later, the search answered while busy unless told to
// decline, a full queue dropping, a handler that throws, draining.
#include <gtest/gtest.h>

#include <atomic>
#include <condition_variable>
#include <future>
#include <mutex>
#include <thread>

#include "multiplexer/backend/base_threaded_multiplexer_server.h"
#include "multiplexer/client.h"
#include "multiplexer/in_process_multiplexer.h"
#include "multiplexer/multiplexer.constants.h"
#include "multiplexer/threaded_client.h"

using multiplexer::ThreadedClient;
using multiplexer::backend::BaseThreadedMultiplexerServer;
using multiplexer::backend::RequestPtr;
using multiplexer::backend::ThreadedServerOptions;
using multiplexer::testing::InProcessMultiplexer;

namespace {

// A backend the tests steer by payload: "block" waits for release(),
// "wait" joins a barrier of four, "later" is kept for the test to answer,
// "throw" throws, "event..." gets no reply, anything else is upper-cased.
struct Scripted : BaseThreadedMultiplexerServer {
  Scripted(unsigned short port, const ThreadedServerOptions& options = ThreadedServerOptions())
      : BaseThreadedMultiplexerServer({{"127.0.0.1", port}}, multiplexer::peers::PYTHON_TEST_SERVER, options) {}

  void handle_message(const RequestPtr& request) override {
    const std::string payload = request->mxmsg().message();
    {
      std::lock_guard<std::mutex> lock(mutex);
      handled.push_back(payload);
    }
    if (payload == "block") {
      std::unique_lock<std::mutex> lock(mutex);
      released_cv.wait_for(lock, std::chrono::seconds(10), [this] { return released; });
      request->no_response();
    } else if (payload == "wait") {
      std::unique_lock<std::mutex> lock(mutex);
      if (++waiting == 4) {
        released_cv.notify_all();
      } else {
        released_cv.wait_for(lock, std::chrono::seconds(10), [this] { return waiting >= 4; });
      }
      request->reply("passed", multiplexer::types::PYTHON_TEST_RESPONSE);
    } else if (payload == "later") {
      std::lock_guard<std::mutex> lock(mutex);
      kept = request;
    } else if (payload == "throw") {
      throw std::runtime_error("as asked");
    } else if (payload == "close") {
      close();  // from a worker: an error, not a deadlock
    } else if (payload.rfind("event", 0) == 0) {
      request->no_response();
    } else {
      std::string upper = payload;
      for (char& character : upper) {
        character = std::toupper(static_cast<unsigned char>(character));
      }
      request->reply(upper, multiplexer::types::PYTHON_TEST_RESPONSE);
    }
  }
  bool on_handler_exception(const std::exception&) override { return keep_serving; }

  void release() {
    std::lock_guard<std::mutex> lock(mutex);
    released = true;
    released_cv.notify_all();
  }
  std::vector<std::string> snapshot() {
    std::lock_guard<std::mutex> lock(mutex);
    return handled;
  }

  std::mutex mutex;
  std::condition_variable released_cv;
  bool released = false;
  int waiting = 0;
  std::vector<std::string> handled;
  RequestPtr kept;
  std::atomic<bool> keep_serving{true};
};

// A Scripted served on its own thread, as a program would.
struct Served {
  explicit Served(unsigned short port, const ThreadedServerOptions& options = ThreadedServerOptions())
      : server(port, options), thread([this] {
          try {
            server.serve_forever(0.05f);
          } catch (...) {
            failure = std::current_exception();
          }
        }) {}
  ~Served() {
    server.stop();
    thread.join();
  }
  Scripted server;
  std::exception_ptr failure;
  std::thread thread;
};

// A synchronous client: sends events and requests, searches by hand.
struct Requester {
  explicit Requester(unsigned short port) : client(multiplexer::peers::WEBSITE) {
    client.connect("127.0.0.1", port, 5);
  }
  multiplexer::MultiplexerMessage message(const std::string& payload, std::uint32_t type) {
    multiplexer::MultiplexerMessage msg;
    msg.set_id(client.random64());
    msg.set_from(client.instance_id());
    msg.set_type(type);
    msg.set_message(payload);
    return msg;
  }
  void send(const std::string& payload, multiplexer::LanePtr lane = multiplexer::LanePtr()) {
    client.send(message(payload, multiplexer::types::PYTHON_TEST_REQUEST), 5, lane);
  }
  std::string query(const std::string& payload, multiplexer::LanePtr lane = multiplexer::LanePtr()) {
    return client.query(message(payload, multiplexer::types::PYTHON_TEST_REQUEST), 10, lane).third->message();
  }
  // The search clients use to find a backend: PING back, or a timeout.
  std::uint32_t search(float timeout) {
    multiplexer::BackendForPacketSearch search;
    search.set_packet_type(multiplexer::types::PYTHON_TEST_REQUEST);
    multiplexer::MultiplexerMessage msg =
        message(search.SerializeAsString(), multiplexer::types::BACKEND_FOR_PACKET_SEARCH);
    client.flush(client.schedule_one(msg), 5);
    for (;;) {
      multiplexer::IncomingMessage got = client.read_raw_message(timeout);
      if (got.third->references() == msg.id()) {
        return got.third->type();
      }
    }
  }
  multiplexer::Client client;
};

template <typename Predicate>
bool eventually(Predicate predicate, float seconds = 10) {
  std::chrono::steady_clock::time_point deadline =
      std::chrono::steady_clock::now() + std::chrono::milliseconds(static_cast<long>(seconds * 1000));
  while (!predicate()) {
    if (std::chrono::steady_clock::now() > deadline) {
      return false;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  return true;
}

}  // namespace

TEST(ThreadedServer, OneWorkerHandlesInArrivalOrderAndAnswers) {
  InProcessMultiplexer mx;
  Served served(mx.port);
  Requester requester(mx.port);
  multiplexer::LanePtr lane(new multiplexer::Lane());
  std::vector<std::string> expected;
  for (int index = 0; index < 100; ++index) {
    requester.send("event-" + std::to_string(index), lane);
    expected.push_back("event-" + std::to_string(index));
  }
  EXPECT_EQ("HELLO", requester.query("hello", lane));
  expected.push_back("hello");
  EXPECT_EQ(expected, served.server.snapshot());
  EXPECT_TRUE(eventually([&] { return served.server.pending() == 0; })) << "the worker done with the query";
}

TEST(ThreadedServer, FourWorkersHandleFourRequestsAtOnce) {
  InProcessMultiplexer mx;
  ThreadedServerOptions options;
  options.workers = 4;
  Served served(mx.port, options);
  ThreadedClient client(multiplexer::peers::PYTHON_TEST_CLIENT);
  ASSERT_TRUE(client.connect("127.0.0.1", mx.port, 5));
  std::vector<std::future<ThreadedClient::Result>> results;
  for (int index = 0; index < 4; ++index) {
    results.push_back(std::async(std::launch::async,
                                 [&] { return client.query("wait", multiplexer::types::PYTHON_TEST_REQUEST, 10); }));
  }
  for (auto& result : results) {
    ThreadedClient::Result r = result.get();
    ASSERT_EQ(ThreadedClient::REPLIED, r.outcome) << "the four were not handled at once";
    EXPECT_EQ("passed", r.reply.third->message());
  }
}

TEST(ThreadedServer, ARequestMayBeAnsweredLaterFromAnotherThread) {
  InProcessMultiplexer mx;
  Served served(mx.port);
  std::thread answerer([&] {
    ASSERT_TRUE(eventually([&] {
      std::lock_guard<std::mutex> lock(served.server.mutex);
      return static_cast<bool>(served.server.kept);
    }));
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    served.server.kept->reply("finally", multiplexer::types::PYTHON_TEST_RESPONSE);
  });
  Requester requester(mx.port);
  EXPECT_EQ("finally", requester.query("later"));
  answerer.join();
}

TEST(ThreadedServer, TheSearchIsAnsweredWhileBusyUnlessToldToDecline) {
  InProcessMultiplexer mx;
  {
    Served served(mx.port);
    Requester requester(mx.port);
    requester.send("block");
    requester.send("event-queued");
    ASSERT_TRUE(eventually([&] { return served.server.pending() == 2; }));
    EXPECT_EQ(multiplexer::types::PING, requester.search(5)) << "answered at once, from the io thread";
    served.server.release();
    EXPECT_EQ("AFTER", requester.query("after"));
  }
  ThreadedServerOptions options;
  options.decline_searches_when_full = true;
  Served served(mx.port, options);
  Requester requester(mx.port);
  requester.send("block");
  ASSERT_TRUE(eventually([&] { return served.server.pending() == 1; }));
  EXPECT_EQ(multiplexer::types::PING, requester.search(5)) << "busy but nothing waiting: answered";
  requester.send("event-queued");
  ASSERT_TRUE(eventually([&] { return served.server.pending() == 2; }));
  EXPECT_THROW(requester.search(1), multiplexer::Client::OperationTimedOut) << "saturated: declined";
  served.server.release();
  EXPECT_EQ("AFTER", requester.query("after"));
}

TEST(ThreadedServer, AFullQueueDropsAndTheRestIsHandledInOrder) {
  InProcessMultiplexer mx;
  ThreadedServerOptions options;
  options.queue_size = 2;
  Served served(mx.port, options);
  Requester requester(mx.port);
  multiplexer::LanePtr lane(new multiplexer::Lane());
  requester.send("block", lane);
  ASSERT_TRUE(eventually([&] { return served.server.pending() == 1; }));
  for (int index = 0; index < 5; ++index) {
    requester.send("event-" + std::to_string(index), lane);
  }
  // A flushing send only says the bytes left; the drops say the rest arrived.
  ASSERT_TRUE(eventually([&] { return served.server.pending() == 3 && served.server.dropped() == 3; }));
  served.server.release();
  EXPECT_EQ("MARKER", requester.query("marker", lane));
  EXPECT_EQ((std::vector<std::string>{"block", "event-0", "event-1", "marker"}), served.server.snapshot());
}

TEST(ThreadedServer, AThrowingHandlerReportsBackendErrorAndServesOn) {
  InProcessMultiplexer mx;
  Served served(mx.port);
  Requester requester(mx.port);
  multiplexer::IncomingMessage reply =
      requester.client.query(requester.message("throw", multiplexer::types::PYTHON_TEST_REQUEST), 10);
  EXPECT_EQ(multiplexer::types::BACKEND_ERROR, reply.third->type());
  EXPECT_EQ("as asked", reply.third->message());
  EXPECT_EQ("STILL", requester.query("still"));
  served.server.keep_serving = false;
  reply = requester.client.query(requester.message("throw", multiplexer::types::PYTHON_TEST_REQUEST), 10);
  EXPECT_EQ(multiplexer::types::BACKEND_ERROR, reply.third->type());
  served.thread.join();                // serve_forever() returned on its own, rethrowing
  served.thread = std::thread([] {});  // the destructor joins again
  EXPECT_TRUE(served.failure) << "serve_forever() should have rethrown";
}

TEST(ThreadedServer, CloseFromAHandlerIsAnErrorNotADeadlock) {
  InProcessMultiplexer mx;
  Served served(mx.port);
  Requester requester(mx.port);
  multiplexer::IncomingMessage reply =
      requester.client.query(requester.message("close", multiplexer::types::PYTHON_TEST_REQUEST), 10);
  EXPECT_EQ(multiplexer::types::BACKEND_ERROR, reply.third->type());
  EXPECT_NE(std::string::npos, reply.third->message().find("worker thread"));
  EXPECT_EQ("STILL", requester.query("still")) << "still serving";
}

TEST(ThreadedServer, DrainingDeclinesSearchesAndFinishesTheQueue) {
  InProcessMultiplexer mx;
  Scripted server(mx.port);
  Requester requester(mx.port);
  requester.send("block");
  for (int index = 0; index < 3; ++index) {
    requester.send("event-" + std::to_string(index));
  }
  ASSERT_TRUE(eventually([&] { return server.pending() == 4; }));
  server.start_draining();
  EXPECT_THROW(requester.search(1), multiplexer::Client::OperationTimedOut);
  server.release();
  server.serve_forever(0.05f);  // drained at once: finishes the queue and returns
  EXPECT_EQ((std::vector<std::string>{"block", "event-0", "event-1", "event-2"}), server.snapshot());
}
