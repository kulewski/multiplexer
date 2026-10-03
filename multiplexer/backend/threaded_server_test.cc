// BaseThreadedMultiplexerServer against a Server in this process: serial
// order with one worker, four requests at once with four, a reply from
// another thread later, the search answered while busy unless told to
// decline, a full queue dropping, a queue of none still giving every
// worker a request, a handler that throws, one whose
// exception not derived from std::exception, or an exception out of
// on_handler_exception(), ends serve_forever(), draining, a
// request refused while leaving, nothing connected before serve_forever,
// a poll and a drain with no deadline.
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <future>
#include <limits>
#include <mutex>
#include <regex>
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
using multiplexer::testing::FILL_FRAMES;
using multiplexer::testing::fill_size;
using multiplexer::testing::InProcessMultiplexer;

namespace {

// A backend the tests steer by payload: "block" waits for release(),
// "wait" joins a barrier of four, "later" is kept for the test to answer,
// "throw" throws, "block, then throw an int" waits for release() and
// throws 3, "too big" replies over MAX_MESSAGE_SIZE, which throws where
// the reply is built, "shut down and throw" shuts the client down and
// throws, "event..." gets no reply, anything else is upper-cased.
// on_handler_exception() throws when asked to.
struct Scripted : BaseThreadedMultiplexerServer {
  Scripted(unsigned short port, const ThreadedServerOptions& options = ThreadedServerOptions())
      : Scripted(multiplexer::backend::MultiplexerAddresses{{"127.0.0.1", port}}, options) {}
  Scripted(const multiplexer::backend::MultiplexerAddresses& addresses, const ThreadedServerOptions& options)
      : BaseThreadedMultiplexerServer(addresses, multiplexer::peers::PYTHON_TEST_SERVER, options) {}

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
    } else if (payload == "block, then throw an int") {
      {
        std::unique_lock<std::mutex> lock(mutex);
        released_cv.wait_for(lock, std::chrono::seconds(10), [this] { return released; });
      }
      throw 3;
    } else if (payload == "too big") {
      request->reply(std::string(multiplexer::MAX_MESSAGE_SIZE, 'x'), multiplexer::types::PYTHON_TEST_RESPONSE);
    } else if (payload == "shut down and throw") {
      client().shutdown(0);
      throw std::runtime_error("after the client");
    } else if (payload == "close") {
      close();  // from a worker: an error, not a deadlock
    } else if (payload == "block, then close") {
      {
        std::unique_lock<std::mutex> lock(mutex);
        released_cv.wait_for(lock, std::chrono::seconds(10), [this] { return released; });
      }
      close();  // from a worker while another thread closes: an error too
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
  bool on_handler_exception(const std::exception&) override {
    {
      std::lock_guard<std::mutex> lock(exceptions_mutex);
      ++exceptions;
      exceptions_counted.notify_all();
    }
    if (throw_from_on_handler_exception) {
      throw std::runtime_error("from on_handler_exception");
    }
    return keep_serving;
  }
  // How many handler exceptions were counted, once `count` were, 30 s at
  // most, a failure detector: the report a requester gets goes out before
  // the server hands the exception to on_handler_exception().
  int exceptions_once(int count) {
    std::unique_lock<std::mutex> lock(exceptions_mutex);
    exceptions_counted.wait_for(lock, std::chrono::seconds(30), [&] { return exceptions.load() >= count; });
    return exceptions.load();
  }

  void release() {
    std::lock_guard<std::mutex> lock(mutex);
    released = true;
    released_cv.notify_all();
  }
  std::vector<std::string> snapshot() {
    std::lock_guard<std::mutex> lock(mutex);
    return handled;
  }
  RequestPtr kept_request() {  // the worker sets it under the lock
    std::lock_guard<std::mutex> lock(mutex);
    return kept;
  }

  std::mutex mutex;
  std::condition_variable released_cv;
  bool released = false;
  int waiting = 0;
  std::vector<std::string> handled;
  RequestPtr kept;
  std::atomic<bool> keep_serving{true};
  std::atomic<bool> throw_from_on_handler_exception{false};
  std::atomic<int> exceptions{0};
  std::mutex exceptions_mutex;
  std::condition_variable exceptions_counted;
};

// A Scripted served on its own thread, as a program would; built once it
// is connected, since serve_forever() is what connects.
struct Served {
  explicit Served(unsigned short port, const ThreadedServerOptions& options = ThreadedServerOptions(),
                  float drain_seconds = 0.0f)
      : server(port, options), thread([this, drain_seconds] {
          try {
            server.serve_forever(0.05f, drain_seconds);
          } catch (...) {
            failure = std::current_exception();
          }
          returned = true;
        }) {
    for (int waited = 0; waited < 1000 && server.client().connections_count() == 0; ++waited) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
  }
  ~Served() {
    server.stop();
    if (thread.joinable()) {  // a test may have joined it, to see serve_forever() return
      thread.join();
    }
  }
  Scripted server;
  std::exception_ptr failure;
  std::atomic<bool> returned{false};  // serve_forever() is over
  std::thread thread;
};

// A Scripted that counts its periodic_task() calls and, once asked to
// leave, starts a drain there and says whether drained() held right
// after, on the serving thread.
struct Periodic : Scripted {
  using Scripted::Scripted;
  void periodic_task() override {
    ++calls;
    if (leave.load() && !draining()) {
      start_draining();
      drained_at_once = drained();
      checked = true;
    }
  }
  std::atomic<int> calls{0};
  std::atomic<bool> leave{false};
  std::atomic<bool> drained_at_once{false};
  std::atomic<bool> checked{false};
};

// A Scripted that stops itself from periodic_task(), on the serving thread.
struct SelfStopping : Scripted {
  using Scripted::Scripted;
  void periodic_task() override { stop(); }
};

// serve_forever() of a server the test made, on a thread of its own: the
// server stopped and the thread joined at the end of the scope, however
// the test ends. A failure, from a handler, is the test's to look for,
// once `returned` says serve_forever() is over.
struct ServingThread {
  ServingThread(BaseThreadedMultiplexerServer& server, float poll, float drain_seconds = 0.0f)
      : server(server), thread([this, poll, drain_seconds] {
          try {
            this->server.serve_forever(poll, drain_seconds);
          } catch (...) {
            failure = std::current_exception();
          }
          returned = true;
        }) {}
  ~ServingThread() {
    server.stop();
    thread.join();
  }
  BaseThreadedMultiplexerServer& server;
  std::exception_ptr failure;
  std::atomic<bool> returned{false};
  std::thread thread;
};

// Throws on every message but the marker, on its one worker: sends `peer`
// an event at its first poll, and the marker once its handler threw on a
// BACKEND_ERROR, behind any report of that.
struct RaisingThreaded : BaseThreadedMultiplexerServer {
  explicit RaisingThreaded(unsigned short port)
      : BaseThreadedMultiplexerServer(multiplexer::backend::MultiplexerAddresses{{"127.0.0.1", port}},
                                      multiplexer::peers::PYTHON_TEST_SERVER) {}
  std::atomic<std::uint64_t> peer{0};
  std::atomic<int> reports{0};  // the BACKEND_ERRORs its handler got
  std::atomic<bool> marked{false};
  std::atomic<bool> handled_report{false};

 protected:
  void handle_message(const RequestPtr& request) override {
    if (request->mxmsg().type() == multiplexer::types::PYTHON_TEST_RESPONSE) {
      marked = true;
      request->no_response();
      return;
    }
    raised_on_report_ = request->mxmsg().type() == multiplexer::types::BACKEND_ERROR;
    if (raised_on_report_) {
      ++reports;
    }
    throw std::runtime_error("unexpected type " + std::to_string(request->mxmsg().type()));
  }
  bool on_handler_exception(const std::exception&) override {
    if (raised_on_report_) {
      handled_report = true;  // after the report, if any, went to the client
    }
    return true;
  }
  void periodic_task() override {
    const std::uint64_t to = peer.load();
    if (to && !sent_event_) {
      sent_event_ = true;
      multiplexer::MultiplexerMessage event =
          client().new_message(multiplexer::types::PYTHON_TEST_REQUEST, "unexpected");
      event.set_to(to);
      client().send(event);
    }
    if (handled_report.load() && !sent_marker_) {
      sent_marker_ = true;
      multiplexer::MultiplexerMessage marker = client().new_message(multiplexer::types::PYTHON_TEST_RESPONSE, "marker");
      marker.set_to(to);
      client().send(marker);
    }
  }

 private:
  bool raised_on_report_ = false;                  // the one worker's
  bool sent_event_ = false, sent_marker_ = false;  // serve_forever()'s thread's
};

// A synchronous client: sends events and requests, searches by hand.
struct Requester {
  explicit Requester(unsigned short port) : client(multiplexer::peers::WEBSITE) {
    client.connect("127.0.0.1", port, 5);
  }
  multiplexer::MultiplexerMessage message(const std::string& payload, std::uint32_t type) {
    multiplexer::MultiplexerMessage msg;
    msg.set_id(client.random64());
    msg.set_sender(client.instance_id());
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
    return answer_to(message(search.SerializeAsString(), multiplexer::types::BACKEND_FOR_PACKET_SEARCH), timeout);
  }
  // `msg` sent as it is, and the type of the first message referencing it.
  std::uint32_t answer_to(const multiplexer::MultiplexerMessage& msg, float timeout) {
    client.flush(client.schedule_one(msg), 5);
    return answer(msg.id(), timeout);
  }
  // The type of the first message referencing `id`, waiting up to
  // `timeout` for each message; throws OperationTimedOut when none comes.
  std::uint32_t answer(std::uint64_t id, float timeout) {
    for (;;) {
      multiplexer::IncomingMessage got = client.read_raw_message(timeout);
      if (got.third->references() == id) {
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

}  // namespace

// X sends Y an event, Y's handler throws and Y reports it to X with
// BACKEND_ERROR, and X's handler throws on that report: X sends no report
// back, where the two answered each other's reports for good. X's marker,
// sent after it handled the report, reaches Y behind anything X sent.
TEST(ThreadedServer, TwoBackendsAnswerNoReportWithAReport) {
  InProcessMultiplexer mx;
  RaisingThreaded x(mx.port), y(mx.port);
  std::thread y_thread([&y] { y.serve_forever(0.05f); });
  std::thread x_thread([&x] { x.serve_forever(0.05f); });
  for (int waited = 0; waited < 500 && (x.client().connections_count() == 0 || y.client().connections_count() == 0);
       ++waited) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  x.peer = y.instance_id();
  for (int waited = 0; waited < 1000 && !y.marked.load(); ++waited) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  x.stop();
  y.stop();
  x_thread.join();
  y_thread.join();
  EXPECT_TRUE(y.marked.load()) << "the marker never came";
  EXPECT_EQ(1, x.reports.load()) << "X got Y's report, once";
  EXPECT_EQ(0, y.reports.load()) << "X did not report on Y's report";
}

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
    ASSERT_TRUE(eventually([&] { return static_cast<bool>(served.server.kept_request()); }));
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    served.server.kept_request()->reply("finally", multiplexer::types::PYTHON_TEST_RESPONSE);
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
  // In the handler, not merely queued: a queued request with the worker
  // still on its way to it is "nothing waiting" too, and answered.
  ASSERT_TRUE(eventually([&] { return served.server.snapshot().size() == 1; })) << "the worker in the handler";
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
  // In the handler, not merely queued, so that the queue has its two
  // places for the events: pending() counts a request still queued too.
  ASSERT_TRUE(eventually([&] { return served.server.snapshot().size() == 1; })) << "the worker in the handler";
  for (int index = 0; index < 5; ++index) {
    requester.send("event-" + std::to_string(index), lane);
  }
  // A flushing send only says the bytes left; the drops say the rest arrived.
  ASSERT_TRUE(eventually([&] { return served.server.pending() == 3 && served.server.dropped() == 3; }));
  served.server.release();
  // The queue drained first, so that the marker finds a place in it.
  ASSERT_TRUE(eventually([&] { return served.server.pending() == 0; })) << "the events handled";
  EXPECT_EQ("MARKER", requester.query("marker", lane));
  EXPECT_EQ((std::vector<std::string>{"block", "event-0", "event-1", "marker"}), served.server.snapshot());
}

// A queue of none still lets every idle worker take a request: queue_size
// counts what waits for a worker, and a request an idle worker is about
// to take waits for none, where it was counted and queue_size 0 dropped
// every request. Two workers, no queue: two "block" requests are taken,
// and a third, sent once both workers are in their handlers, is dropped.
// Counted: the requests in the handlers, and the drops.
TEST(ThreadedServer, AQueueOfNoneLetsEveryWorkerTakeARequest) {
  InProcessMultiplexer mx;
  ThreadedServerOptions options;
  options.workers = 2;
  options.queue_size = 0;
  Served served(mx.port, options);
  Requester requester(mx.port);
  requester.send("block");
  requester.send("block");
  ASSERT_TRUE(eventually([&] { return served.server.snapshot().size() == 2; })) << "both workers in their handlers";
  requester.send("event-dropped");
  ASSERT_TRUE(eventually([&] { return served.server.dropped() == 1; })) << "the third dropped";
  served.server.release();
  EXPECT_EQ((std::vector<std::string>{"block", "block"}), served.server.snapshot());
}

// The drops of a full queue said as one line and a count, not a line each:
// fifty dropped while the worker is held, said by the time the server is
// gone, since its client's shutdown says what is still counted.
TEST(ThreadedServer, AFullQueueSaysItsDropsInOneLineAndACount) {
  InProcessMultiplexer mx;
  ::testing::internal::CaptureStderr();
  {
    ThreadedServerOptions options;
    options.queue_size = 1;
    Served served(mx.port, options);
    Requester requester(mx.port);
    multiplexer::LanePtr lane(new multiplexer::Lane());
    requester.send("block", lane);
    // In the handler, not merely queued: the queue's one place is the
    // first event's.
    ASSERT_TRUE(eventually([&] { return served.server.snapshot().size() == 1; })) << "the worker in the handler";
    for (int index = 0; index < 51; ++index) {
      requester.send("event-" + std::to_string(index), lane);
    }
    ASSERT_TRUE(eventually([&] { return served.server.dropped() == 50; }));
    served.server.release();
  }
  const std::string log = ::testing::internal::GetCapturedStderr();
  const std::regex first(R"(request #\d+ of type \d+ dropped: queue full)");
  const std::regex counted(R"(requests dropped: queue full \[(\d+) more in the last \d+\.\d s\])");
  EXPECT_EQ(1, std::distance(std::sregex_iterator(log.begin(), log.end(), first), std::sregex_iterator())) << log;
  std::uint64_t more = 0;
  for (std::sregex_iterator match(log.begin(), log.end(), counted), end; match != end; ++match) {
    more += std::stoull((*match)[1].str());
  }
  EXPECT_EQ(49u, more) << log;
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

// An exception not derived from std::exception out of a handler is not
// the worker's to swallow: serve_forever() rethrows it, as
// BaseMultiplexerServer's does, and the request queued behind it, which no
// worker is left to take, is refused at once. It escaped the worker and
// terminated the process.
TEST(ThreadedServer, AHandlersNonStdExceptionEndsServeForeverAndTheQueueIsRefused) {
  InProcessMultiplexer mx;
  Served served(mx.port);
  Requester requester(mx.port);  // one multiplexer: one connection, in order
  requester.send("block, then throw an int");
  ASSERT_TRUE(eventually([&] { return served.server.pending() == 1; })) << "the worker busy";
  std::thread releasing([&] {
    eventually([&] { return served.server.pending() == 2; });  // the request queued behind it
    served.server.release();
  });
  std::uint32_t answer = 0;
  try {
    answer = requester.answer_to(requester.message("after", multiplexer::types::PYTHON_TEST_REQUEST), 20);
  } catch (const multiplexer::Client::OperationTimedOut&) {
  }
  releasing.join();
  EXPECT_EQ(multiplexer::types::DELIVERY_ERROR, answer) << "refused, not left waiting";
  served.thread.join();                // serve_forever() returned on its own, rethrowing
  served.thread = std::thread([] {});  // the destructor joins again
  ASSERT_TRUE(served.failure) << "serve_forever() should have rethrown";
  try {
    std::rethrow_exception(served.failure);
  } catch (int code) {
    EXPECT_EQ(3, code);
  } catch (...) {
    ADD_FAILURE() << "not what the handler threw";
  }
  EXPECT_EQ(std::vector<std::string>{"block, then throw an int"}, served.server.snapshot());
}

// on_handler_exception() throwing, after the requester heard
// BACKEND_ERROR: serve_forever() rethrows it, where it escaped the worker
// and terminated the process.
TEST(ThreadedServer, AnExceptionOutOfOnHandlerExceptionEndsServeForever) {
  InProcessMultiplexer mx;
  Served served(mx.port);
  served.server.throw_from_on_handler_exception = true;
  Requester requester(mx.port);
  multiplexer::IncomingMessage reply =
      requester.client.query(requester.message("throw", multiplexer::types::PYTHON_TEST_REQUEST), 10);
  EXPECT_EQ(multiplexer::types::BACKEND_ERROR, reply.third->type());
  served.thread.join();                // serve_forever() returned on its own, rethrowing
  served.thread = std::thread([] {});  // the destructor joins again
  ASSERT_TRUE(served.failure) << "serve_forever() should have rethrown";
  try {
    std::rethrow_exception(served.failure);
  } catch (const std::runtime_error& error) {
    EXPECT_STREQ("from on_handler_exception", error.what());
  } catch (...) {
    ADD_FAILURE() << "not what on_handler_exception() threw";
  }
}

// A reply that threw where it was built, one over MAX_MESSAGE_SIZE, did
// not go out: the requester gets BACKEND_ERROR, where the request counted
// as answered and the requester waited out its timeout.
TEST(ThreadedServer, AReplyThatThrowsIsAnsweredWithBackendError) {
  InProcessMultiplexer mx;
  Served served(mx.port);
  Requester requester(mx.port);
  multiplexer::IncomingMessage reply =
      requester.client.query(requester.message("too big", multiplexer::types::PYTHON_TEST_REQUEST), 5);
  EXPECT_EQ(multiplexer::types::BACKEND_ERROR, reply.third->type());
  EXPECT_EQ(1, served.server.exceptions_once(1));
}

// A report of a handler's exception that fails, the client shut down
// under the worker, is logged and on_handler_exception() is still told,
// where the report's exception left the worker's thread and ended the
// process.
TEST(ThreadedServer, AReportThatFailsStillTellsOnHandlerException) {
  InProcessMultiplexer mx;
  Served served(mx.port);
  Requester requester(mx.port);
  requester.send("shut down and throw");
  EXPECT_EQ(1, served.server.exceptions_once(1));
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

// close() with the worker still busy, on a server whose drain routing
// keeps every path open, as a request routed before the multiplexer
// applied the usual one would arrive: refused with DELIVERY_ERROR.
TEST(ThreadedServer, ARequestArrivingWhileLeavingIsRefusedAtOnce) {
  InProcessMultiplexer mx;
  ThreadedServerOptions options;
  options.drain_routing = multiplexer::Routing();  // the multiplexer keeps routing; the server refuses
  Served served(mx.port, options);                 // serving, so that "block" is in the handler when close() starts
  Scripted& server = served.server;
  Requester requester(mx.port);
  requester.send("block");
  ASSERT_TRUE(eventually([&] { return server.pending() == 1; }));
  std::thread closing([&] { server.close(); });
  ASSERT_TRUE(eventually([&] { return server.draining(); }));
  EXPECT_EQ(multiplexer::types::DELIVERY_ERROR,
            requester.answer_to(requester.message("late", multiplexer::types::PYTHON_TEST_REQUEST), 5));
  server.release();
  closing.join();
  EXPECT_EQ((std::vector<std::string>{"block"}), server.snapshot());
  EXPECT_EQ(1u, server.dropped());
}

// close() without a drain tells the multiplexer the drain routing, as
// start_draining() does: once it applied it, a search and a request come
// back as the multiplexer's own DELIVERY_ERROR instead of reaching the
// closing server, whose refusal would cost an addressed query its retry.
TEST(ThreadedServer, CloseTakesTheServerOutOfRouting) {
  InProcessMultiplexer mx;
  Served served(mx.port);
  Scripted& server = served.server;
  Requester requester(mx.port);
  requester.send("block");
  ASSERT_TRUE(eventually([&] { return server.pending() == 1; }));
  std::thread closing([&] { server.close(); });
  // Unanswered while the multiplexer still offers it (a closing server
  // answers no search), then the multiplexer's DELIVERY_ERROR, well
  // before "block" gives up waiting at 10 s and the server is gone.
  EXPECT_TRUE(eventually(
      [&] {
        try {
          return requester.search(0.5) == multiplexer::types::DELIVERY_ERROR;
        } catch (const multiplexer::Client::OperationTimedOut&) {
          return false;
        }
      },
      5));
  EXPECT_EQ(multiplexer::types::DELIVERY_ERROR,
            requester.answer_to(requester.message("late", multiplexer::types::PYTHON_TEST_REQUEST), 5));
  EXPECT_EQ(1u, server.pending()) << "still closing: the worker holds \"block\"";
  server.release();
  closing.join();
  EXPECT_EQ(0u, server.dropped()) << "nothing reached the closing server to be refused";
}

// A closing server answers no search, even one the multiplexer still
// offers it: the request that would follow is refused.
TEST(ThreadedServer, AClosingServerAnswersNoSearch) {
  InProcessMultiplexer mx;
  ThreadedServerOptions options;
  options.drain_routing = multiplexer::Routing();  // the multiplexer keeps offering it
  Served served(mx.port, options);
  Scripted& server = served.server;
  Requester requester(mx.port);
  EXPECT_EQ(multiplexer::types::PING, requester.search(5)) << "serving: answered";
  requester.send("block");
  ASSERT_TRUE(eventually([&] { return server.pending() == 1; }));
  std::thread closing([&] { server.close(); });
  ASSERT_TRUE(eventually([&] { return server.draining(); }));
  EXPECT_THROW(requester.search(1), multiplexer::Client::OperationTimedOut) << "closing: not answered";
  server.release();
  closing.join();
}

// A worker that calls close() while another thread's close() joins it
// throws, as from a handler at any time, rather than wait for the close()
// that waits for it. The server is leaked when the two wait for each other.
TEST(ThreadedServer, CloseFromAHandlerDuringAnotherCloseIsAnErrorNotADeadlock) {
  InProcessMultiplexer mx;
  std::unique_ptr<Served> served(new Served(mx.port));
  Scripted& server = served->server;
  Requester requester(mx.port);
  requester.send("block, then close");
  ASSERT_TRUE(eventually([&] { return server.pending() == 1; }));
  std::promise<void> closed;
  std::future<void> close_returned = closed.get_future();
  std::thread closing([&server, &closed] {
    server.close();
    closed.set_value();
  });
  ASSERT_TRUE(eventually([&] { return server.draining(); }));
  server.release();
  if (close_returned.wait_for(std::chrono::seconds(10)) != std::future_status::ready) {
    closing.detach();  // waiting for good, with the server
    served.release();
    FAIL() << "close() waited for the worker, which waited for it";
  }
  closing.join();
  EXPECT_EQ(1, server.exceptions.load()) << "the handler's close() threw";
}

// close() on another thread while serve_forever() still connects, to a
// multiplexer that never answers: the connect under way ends, the next
// address is not tried, and serve_forever() returns, where the next connect
// threw NotConnected out of it.
TEST(ThreadedServer, ACloseWhileServeForeverConnectsEndsItQuietly) {
  InProcessMultiplexer mx;
  const int silent = ::socket(AF_INET, SOCK_STREAM, 0);
  sockaddr_in address = {};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  socklen_t length = sizeof address;
  ASSERT_EQ(0, ::bind(silent, reinterpret_cast<sockaddr*>(&address), length));
  ASSERT_EQ(0, ::listen(silent, 1));
  ASSERT_EQ(0, ::getsockname(silent, reinterpret_cast<sockaddr*>(&address), &length));
  ThreadedServerOptions options;
  options.connect_timeout = 30;
  Scripted server({{"127.0.0.1", ntohs(address.sin_port)}, {"127.0.0.1", mx.port}}, options);
  std::exception_ptr failure;
  std::thread serving([&server, &failure] {
    try {
      server.serve_forever(0.05f);
    } catch (...) {
      failure = std::current_exception();
    }
  });
  pollfd waiting = {silent, POLLIN, 0};
  ASSERT_EQ(1, ::poll(&waiting, 1, 10000)) << "the first connect never came";
  const int accepted = ::accept(silent, nullptr, nullptr);  // the first connect is under way, waiting for a welcome
  server.close();
  serving.join();
  ::close(accepted);
  ::close(silent);
  EXPECT_FALSE(failure) << "serve_forever() threw";
}

// close() from another thread while a worker is busy: serve_forever()'s
// own close() waits for that one to finish, so serve_forever() returns
// only once the workers are done and the client shut down, where it
// returned at once with the server half closed.
TEST(ThreadedServer, ASecondCloseWaitsForTheFirst) {
  InProcessMultiplexer mx;
  Served served(mx.port);
  Scripted& server = served.server;
  Requester requester(mx.port);
  requester.send("block");
  ASSERT_TRUE(eventually([&] { return server.pending() == 1; }));
  std::thread closing([&] { server.close(); });
  ASSERT_TRUE(eventually([&] { return server.draining(); }));
  // stop() in close() wakes serve_forever(), which calls close() too.
  EXPECT_FALSE(eventually([&] { return served.returned.load(); }, 1)) << "returned with a worker busy";
  server.release();
  closing.join();
  EXPECT_TRUE(eventually([&] { return served.returned.load(); }));
}

// A message that answers another, one with `references`, arriving while
// the server leaves is dropped, not refused: nobody retries a reply, and
// refusing one could start a loop with a peer that answers the refusal.
// Ordered, not timed: the reply and a request behind it go out on the one
// connection, and the server takes both in order on its io thread, so a
// refusal of the reply would come back before the request's.
TEST(ThreadedServer, AReplyArrivingWhileLeavingIsDroppedNotRefused) {
  InProcessMultiplexer mx;
  Served served(mx.port);
  Scripted& server = served.server;
  Requester requester(mx.port);
  requester.send("block");
  ASSERT_TRUE(eventually([&] { return server.pending() == 1; }));
  std::thread closing([&] { server.close(); });
  ASSERT_TRUE(eventually([&] { return server.draining(); }));
  multiplexer::MultiplexerMessage reply = requester.message("an answer", multiplexer::types::PYTHON_TEST_RESPONSE);
  reply.set_to(server.instance_id());
  reply.set_references(requester.client.random64());
  multiplexer::MultiplexerMessage late = requester.message("late", multiplexer::types::PYTHON_TEST_REQUEST);
  late.set_to(server.instance_id());
  requester.client.schedule_one(reply);
  requester.client.flush(requester.client.schedule_one(late), 5);
  for (;;) {
    multiplexer::IncomingMessage got = requester.client.read_raw_message(10);
    ASSERT_NE(reply.id(), got.third->references()) << "the reply was answered, refused rather than dropped";
    if (got.third->references() == late.id()) {
      EXPECT_EQ(multiplexer::types::DELIVERY_ERROR, got.third->type()) << "a request is still refused";
      break;
    }
  }
  server.release();
  closing.join();
  EXPECT_EQ(2u, server.dropped());
}

// start_draining() tells the multiplexer to route nothing new by the
// rules: once confirmed, a request and a search come back as the
// multiplexer's own DELIVERY_ERROR, and the queue is finished.
TEST(ThreadedServer, DrainingTakesTheBackendOutOfRoutingAndFinishesTheQueue) {
  InProcessMultiplexer mx;
  Served served(mx.port);
  Requester requester(mx.port);
  requester.send("block");
  for (int index = 0; index < 3; ++index) {
    requester.send("event-" + std::to_string(index));
  }
  ASSERT_TRUE(eventually([&] { return served.server.pending() == 4; }));
  served.server.start_draining();
  ASSERT_TRUE(eventually([&] { return served.server.client().routing_acknowledged(); }));
  EXPECT_EQ(multiplexer::types::DELIVERY_ERROR,
            requester.answer_to(requester.message("late", multiplexer::types::PYTHON_TEST_REQUEST), 5));
  EXPECT_EQ(multiplexer::types::DELIVERY_ERROR, requester.search(5));
  served.server.release();
  served.thread.join();  // drained at once: serve_forever() finishes the queue and returns
  EXPECT_EQ((std::vector<std::string>{"block", "event-0", "event-1", "event-2"}), served.server.snapshot());
  EXPECT_EQ(0u, served.server.dropped());
}

// The drain ends once the multiplexer confirmed the routing and the
// workers finished the queue, not before: confirmed while "block" holds
// the worker, it goes on turn after turn of the loop, and a request
// addressed to the server, which the drain routing still delivers, is
// queued behind and served, where the drain ended on the confirmation
// alone and the close refused it. With no cap nothing else ends the
// drain: serve_forever() returns once the queue is empty, every request
// handled and nothing refused.
TEST(ThreadedServer, ADrainEndsWhenConfirmedAndTheQueueIsEmpty) {
  InProcessMultiplexer mx;
  Periodic server(mx.port);
  ServingThread serving(server, 0.05f, -1.0f);  // no cap
  ASSERT_TRUE(eventually([&server] { return server.client().connections_count() == 1; }));
  Requester requester(mx.port);
  requester.send("block");
  for (int index = 0; index < 3; ++index) {
    requester.send("event-" + std::to_string(index));
  }
  ASSERT_TRUE(eventually([&] { return server.pending() == 4; }));
  server.start_draining();
  ASSERT_TRUE(eventually([&] { return server.client().routing_acknowledged(); }));
  // Two turns of the loop from here: drained() was asked at least once
  // since the confirmation, and said no.
  const int turns = server.calls.load();
  EXPECT_TRUE(eventually([&] { return server.calls.load() >= turns + 2; }, 5))
      << "the drain ended with the worker busy";
  multiplexer::MultiplexerMessage addressed = requester.message("addressed", multiplexer::types::PYTHON_TEST_REQUEST);
  addressed.set_to(server.instance_id());
  requester.client.flush(requester.client.schedule_one(addressed), 5);
  // Queued behind "block", or refused by a server that took its drain for over.
  EXPECT_TRUE(eventually([&] { return server.pending() == 5 || server.dropped() != 0; }));
  server.release();
  std::uint32_t answer = 0;
  try {
    answer = requester.answer(addressed.id(), 10);
  } catch (const multiplexer::Client::OperationTimedOut&) {
  }
  EXPECT_EQ(multiplexer::types::PYTHON_TEST_RESPONSE, answer) << "served, not refused";
  EXPECT_EQ((std::vector<std::string>{"block", "event-0", "event-1", "event-2", "addressed"}), server.snapshot());
  EXPECT_EQ(0u, server.dropped());
  ASSERT_TRUE(eventually([&] { return serving.returned.load(); })) << "the drain went on with the queue empty";
  EXPECT_FALSE(serving.failure);
}

// With `all` kept on, work may keep arriving, so the drain runs to its cap.
TEST(ThreadedServer, ADrainKeepingAPathOpenLastsItsPeriod) {
  InProcessMultiplexer mx;
  ThreadedServerOptions options;
  options.drain_routing.set_all(true);  // events kept: the default has both paths off
  Served served(mx.port, options, 1.5f);
  const std::chrono::steady_clock::time_point started = std::chrono::steady_clock::now();
  served.server.start_draining();
  ASSERT_TRUE(eventually([&] { return served.server.client().routing_acknowledged(); }));
  std::this_thread::sleep_for(std::chrono::milliseconds(500));
  EXPECT_TRUE(served.thread.joinable() && served.server.draining());
  served.thread.join();
  EXPECT_GE(std::chrono::steady_clock::now() - started, std::chrono::milliseconds(1400));
}

// Asked for again on every poll, as a program checking a flag does, the
// drain still ends at its cap, counted from the first call.
TEST(ThreadedServer, ADrainAskedForOnEveryPollStillEndsAtItsCap) {
  InProcessMultiplexer mx;
  ThreadedServerOptions options;
  options.drain_routing.set_all(true);  // a path kept open: only the cap ends the drain
  Served served(mx.port, options, 1.0f);
  std::atomic<bool> asking{true};
  std::thread asker([&] {
    while (asking) {
      served.server.start_draining();
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
  });
  EXPECT_TRUE(eventually([&] { return served.returned.load(); }, 4))
      << "serve_forever() returned a second after the first call";
  asking = false;
  asker.join();
}

// A drain with no cap, a negative drain_seconds as an infinite one, is
// never over by time: with a path kept open only stop() or close() ends
// it, where the negative cap had passed as the drain began.
TEST(ThreadedServer, ADrainWithNoCapIsNotOverByTime) {
  InProcessMultiplexer mx;
  ThreadedServerOptions options;
  options.drain_routing.set_all(true);  // a path kept open: only the cap could end the drain
  Periodic server(mx.port, options);
  ServingThread serving(server, 0.05f, -1.0f);
  ASSERT_TRUE(eventually([&server] { return server.client().connections_count() == 1; }));
  server.leave = true;
  ASSERT_TRUE(eventually([&server] { return server.checked.load(); }));
  EXPECT_FALSE(server.drained_at_once.load()) << "no cap";
}

// With no deadline for its poll, a negative one as an infinite one,
// serve_forever() calls periodic_task() only when woken, by a drain,
// stop() or close(): the wait ended at once, and periodic_task() ran over
// and over. A query answered meanwhile shows it serves.
TEST(ThreadedServer, APollWithNoDeadlineWaitsUntilWoken) {
  InProcessMultiplexer mx;
  Periodic server(mx.port);
  {
    ServingThread serving(server, -1.0f);
    ASSERT_TRUE(eventually([&server] { return server.client().connections_count() == 1; }));
    Requester requester(mx.port);
    EXPECT_EQ("SERVED", requester.query("served"));
    EXPECT_EQ(0, server.calls.load()) << "nothing woke it";
  }  // stopped: woken, the loop ends
  EXPECT_EQ(0, server.calls.load()) << "a stop ends the loop before periodic_task()";
}

// A stop() from periodic_task() ends a serve_forever() whose poll has no
// deadline, negative or infinite: its wake, sent while the serving thread
// was not waiting, was lost, and the next wait never ended. A drain that
// cannot end by itself, a path kept open and a cap far off, wakes the
// loop, waiting or about to, into periodic_task(); a stop() from the test
// frees a loop that lost the wake.
TEST(ThreadedServer, AStopFromPeriodicTaskEndsAPollWithNoDeadline) {
  InProcessMultiplexer mx;
  ThreadedServerOptions options;
  options.drain_routing.set_all(true);  // a path kept open: only the cap could end the drain
  for (const float poll : {-1.0f, std::numeric_limits<float>::infinity()}) {
    SCOPED_TRACE(poll);
    SelfStopping server(mx.port, options);
    std::promise<void> returned;
    std::thread serving([&server, &returned, poll] {
      try {
        server.serve_forever(poll, 1e9f);
      } catch (...) {
      }
      returned.set_value();
    });
    server.start_draining();  // wakes the loop: periodic_task() stops it
    EXPECT_TRUE(returned.get_future().wait_for(std::chrono::seconds(10)) == std::future_status::ready)
        << "serve_forever() did not return: the wake periodic_task() sent was lost";
    server.stop();  // wakes a loop that lost it
    serving.join();
  }
}

// close() from another thread while serve_forever() drains: serve_forever()
// returns, it does not throw NotConnected out of drained().
TEST(ThreadedServer, CloseFromAnotherThreadReturnsFromServeForever) {
  InProcessMultiplexer mx;
  Served served(mx.port, ThreadedServerOptions(), 10.0f);
  Requester requester(mx.port);
  requester.send("block");
  ASSERT_TRUE(eventually([&] { return served.server.pending() == 1; }));
  std::thread closing([&] { served.server.close(); });
  ASSERT_TRUE(eventually([&] { return served.server.draining(); }));
  served.server.release();
  closing.join();
  served.thread.join();
  EXPECT_FALSE(served.failure) << "serve_forever() returned rather than threw";
}

// A reply still being written when close() begins reaches the requester:
// close() writes out what was sent before it, `timeout` seconds at most,
// and only then shuts the sockets. The multiplexer is frozen while the
// reply, behind more than the two sockets between them hold, events nobody
// takes, is sent and close() begins, and reads again once the client's
// shutdown is under way: the reply arrives after close() began, where a
// close that shut the sockets at once cut it off. A reply with nothing
// before it is written before close() gets there, and passes either way.
TEST(ThreadedServer, TheLastReplyIsWrittenBeforeTheClose) {
  InProcessMultiplexer mx;
  Served served(mx.port);
  Requester requester(mx.port);
  const multiplexer::MultiplexerMessage request = requester.message("later", multiplexer::types::PYTHON_TEST_REQUEST);
  requester.client.flush(requester.client.schedule_one(request), 5);
  ASSERT_TRUE(eventually([&] { return served.server.kept_request() != nullptr; }));
  Freeze frozen(mx);
  const std::string filler(fill_size(), 'f');
  for (int index = 0; index < FILL_FRAMES; ++index) {  // more than the sockets hold, ahead of the reply
    served.server.client().send(served.server.client().new_message(multiplexer::types::TEST_UNROUTED, filler));
  }
  served.server.kept_request()->reply(std::string(1024, 'r'), multiplexer::types::PYTHON_TEST_RESPONSE);
  EXPECT_FALSE(served.server.client().flush_all(0)) << "the reply written at once: nothing left for close()";
  std::thread closing([&served] { served.server.close(60); });  // a write-out deadline only a failure reaches
  // The client's shutdown under way: its calls throw from then on.
  EXPECT_TRUE(eventually([&served] {
    try {
      served.server.client().connections_count();
      return false;
    } catch (const ThreadedClient::NotConnected&) {
      return true;
    }
  })) << "close() never reached the client's shutdown";
  frozen.release();
  closing.join();
  std::uint32_t answer = 0;
  try {
    answer = requester.answer(request.id(), 10);
  } catch (const multiplexer::Client::OperationTimedOut&) {
  }
  EXPECT_EQ(multiplexer::types::PYTHON_TEST_RESPONSE, answer) << "the reply cut off by the close";
}

TEST(ThreadedServer, NothingIsConnectedOrHandledBeforeServeForever) {
  InProcessMultiplexer mx;
  Scripted server(mx.port);  // built, as a subclass's constructor would leave it
  EXPECT_NE(0u, server.instance_id()) << "the id is known before serving";
  EXPECT_EQ(0u, server.client().connections_count()) << "and nothing is connected";
  Requester requester(mx.port);
  try {
    EXPECT_NE(multiplexer::types::PING, requester.search(1)) << "no backend to answer";
  } catch (const multiplexer::Client::OperationTimedOut&) {
    // no answer at all: as good
  }
  server.connect();  // a program that announces itself before serving: the workers are up
  EXPECT_EQ(1u, server.client().connections_count());
  EXPECT_EQ(multiplexer::types::PING, requester.search(5)) << "the search answered";
  std::thread serving([&] { server.serve_forever(0.05f); });  // connects nothing more
  EXPECT_EQ("LATE", requester.query("late"));
  server.stop();
  serving.join();
}
