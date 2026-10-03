// A backend built on one thread, connected and served from another: serve_forever()
// adopts the serving thread, so the debug-build thread checks, which bind a
// client and its connections to the thread that made them, do not fire. And
// what a BaseMultiplexerServer sends: a PING whose echo would be over
// MAX_MESSAGE_SIZE answered with BACKEND_ERROR by a backend that goes on
// serving, a reply that throws answered the same way, a reply naming its
// multiplexer, what periodic_task() sends routed by its type, the
// acknowledgement of notify_start() as a query's on_received hears it, and
// the answer to the PING that locates it for an addressed query, which a
// backend that declines searches sends too. And a handler's own
// OperationTimedOut ends serve_forever() when on_handler_exception() says
// so, as any exception does. A reply given as a whole MultiplexerMessage
// gets its empty fields from the request it answers, and message fields
// beside one are refused.
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <future>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "lib/kwargs.h"
#include "multiplexer/backend/base_multiplexer_server.h"
#include "multiplexer/in_process_multiplexer.h"
#include "multiplexer/multiplexer.constants.h"
#include "multiplexer/threaded_client.h"

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

// The counts a flood's sender and the SlowBackend it floods keep against
// each other, so that the sender is ahead by construction, whatever the
// speed of either thread: the backend handles a request only once the
// sender has sent at least twice as many as it had handled, and four
// more, and the sender sends only while it is fewer than eight beyond
// that, so that what comes back to it stays far below what a client holds
// unread. The flood is over once the backend has left or the sender gave
// up, which ends every wait; each wait also has a bound only a failure
// reaches.
class Flood {
 public:
  // The backend, before it handles a request, having handled `handled`.
  void wait_to_handle(int handled) {
    std::unique_lock<std::mutex> lock(mutex_);
    handled_ = handled;
    changed_.notify_all();  // room for the sender
    changed_.wait_for(lock, std::chrono::seconds(30), [&] { return sent_ >= 2 * handled + 4 || over_; });
  }
  // The sender, before it sends one more: false once the flood is over
  // or `until` passed.
  bool wait_to_send(std::chrono::steady_clock::time_point until) {
    std::unique_lock<std::mutex> lock(mutex_);
    changed_.wait_until(lock, until, [&] { return sent_ < 2 * handled_ + 8 || over_; });
    return !over_ && std::chrono::steady_clock::now() < until;
  }
  // The sender sent one more.
  void sent_one() {
    std::lock_guard<std::mutex> lock(mutex_);
    ++sent_;
    changed_.notify_all();
  }
  // The flood is over: `left` when the backend left, else the sender gave up.
  void end(bool left) {
    std::lock_guard<std::mutex> lock(mutex_);
    over_ = true;
    left_ = left_ || left;
    changed_.notify_all();
  }
  // Whether the backend left while the sender was still at it.
  bool left() {
    std::lock_guard<std::mutex> lock(mutex_);
    return left_;
  }

 private:
  std::mutex mutex_;
  std::condition_variable changed_;
  int sent_ = 0;
  int handled_ = 0;
  bool over_ = false;
  bool left_ = false;
};

// Answers every request after a pause, so that the rest queue up; with a
// `flood`, only once the flood's sender is far enough ahead (Flood). Given
// `drain_after`, it starts its own drain, from periodic_task(), once it has
// handled that many requests: a count, not another thread's timing. With
// `drain_ends_with_waiting`, its drain is over, besides the default's cap,
// only when requests are read and waiting, so that the drain ends with
// something for what follows the loop to serve, by construction. Notes
// the moment its drain ended: how many requests it had handled by then,
// and whether more were read and waiting.
class SlowBackend : public BaseMultiplexerServer {
 public:
  explicit SlowBackend(const MultiplexerAddresses& addresses, int drain_after = -1,
                       bool drain_ends_with_waiting = false)
      : BaseMultiplexerServer(addresses, multiplexer::peers::PYTHON_TEST_SERVER),
        drain_after_(drain_after),
        drain_ends_with_waiting_(drain_ends_with_waiting),
        instance_id_(conn->instance_id()) {}
  std::atomic<int> handled{0};
  std::atomic<bool> serving{false};                   // connected and looping: the first periodic_task() ran
  mutable std::atomic<int> handled_when_drained{-1};  // -1 until the drain ended
  mutable std::atomic<bool> waiting_when_drained{false};
  Flood* flood = nullptr;                                     // set before serving
  std::uint64_t instance_id() const { return instance_id_; }  // the `from` of its refusals, after close() too

 protected:
  void handle_message(multiplexer::MultiplexerMessage& mxmsg) override {
    if (flood) {
      flood->wait_to_handle(handled.load());
    }
    ++handled;
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    send_message(mx::util::kwargs::Kwargs()
                     .set("message", mxmsg.message())
                     .set("type", static_cast<std::uint32_t>(multiplexer::types::PYTHON_TEST_RESPONSE)));
  }
  void periodic_task() override {
    serving = true;
    if (drain_after_ >= 0 && !draining() && handled.load() >= drain_after_) {
      start_draining();
    }
  }
  // The default, with requests read and waiting too when asked for, noting
  // the moment it first holds, when serve_forever() leaves its loop with
  // what was read still to serve.
  bool drained() const override {
    const bool over = BaseMultiplexerServer::drained() && (!drain_ends_with_waiting_ || conn->has_incoming_messages());
    if (over && handled_when_drained.load() < 0) {
      handled_when_drained = handled.load();
      waiting_when_drained = conn->has_incoming_messages();
    }
    return over;
  }

 private:
  const int drain_after_;  // -1: no drain of its own
  const bool drain_ends_with_waiting_;
  const std::uint64_t instance_id_;
};

// Once asked to leave, starts a drain that keeps a path open from
// periodic_task(), and says whether drained() held right after, on the
// serving thread.
class OpenDrainBackend : public BaseMultiplexerServer {
 public:
  explicit OpenDrainBackend(const MultiplexerAddresses& addresses)
      : BaseMultiplexerServer(addresses, multiplexer::peers::PYTHON_TEST_SERVER) {
    multiplexer::Routing routing = drain_routing();
    routing.set_all(true);  // events kept: only the cap could end the drain
    set_drain_routing(routing);
  }
  std::atomic<bool> serving{false};
  std::atomic<bool> leave{false};
  std::atomic<bool> checked{false};
  std::atomic<bool> drained_at_once{false};

 protected:
  void handle_message(multiplexer::MultiplexerMessage&) override { no_response(); }
  void periodic_task() override {
    serving = true;
    if (leave.load() && !draining()) {
      start_draining();
      drained_at_once = drained();
      checked = true;
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

// Answers requests as told, and sends a TEST_EVENT from periodic_task():
// once before any request arrived, and once after each it answered.
// NOTIFY_THEN_HOLD acknowledges a request and never answers it, a handler
// still at work as far as the requester can tell. With
// `declining_searches` it leaves every search unanswered, as a saturated
// backend does.
class ReplyingBackend : public BaseMultiplexerServer {
 public:
  // WHOLE answers with a whole MultiplexerMessage that sets only its type
  // and payload; WHOLE_WITH_TYPE gives a `type` beside it too.
  enum Reply { ANSWER, ANSWER_EVERYWHERE, THROW, NOTIFY_THEN_ANSWER, NOTIFY_THEN_HOLD, WHOLE, WHOLE_WITH_TYPE };
  ReplyingBackend(const MultiplexerAddresses& addresses, Reply reply)
      : BaseMultiplexerServer(addresses, multiplexer::peers::PYTHON_TEST_SERVER), reply_(reply) {}
  std::atomic<bool> serving{false};
  std::atomic<bool> leave{false};
  std::atomic<int> exceptions{0};
  std::atomic<bool> declining_searches{false};
  std::uint64_t instance_id() const { return conn->instance_id(); }
  // How many handler exceptions were counted, once `count` were, 30 s at
  // most, a failure detector: the report a requester gets goes out before
  // the server hands the exception to on_handler_exception().
  int exceptions_once(int count) {
    std::unique_lock<std::mutex> lock(exceptions_mutex_);
    exceptions_counted_.wait_for(lock, std::chrono::seconds(30), [&] { return exceptions.load() >= count; });
    return exceptions.load();
  }

 protected:
  // Declines every search while `declining_searches` is set.
  bool should_respond_to_backend_for_packet_search() const override { return !declining_searches.load(); }
  void handle_message(multiplexer::MultiplexerMessage& mxmsg) override {
    if (reply_ == NOTIFY_THEN_ANSWER || reply_ == NOTIFY_THEN_HOLD) {
      notify_start();  // what a long handler does first
    }
    if (reply_ == NOTIFY_THEN_HOLD) {
      no_response();
      return;
    }
    if (reply_ == WHOLE || reply_ == WHOLE_WITH_TYPE) {
      multiplexer::MultiplexerMessage whole;
      whole.set_type(multiplexer::types::PYTHON_TEST_RESPONSE);
      whole.set_message("re: " + mxmsg.message());
      mx::util::kwargs::Kwargs reply;
      reply.set("message", static_cast<const multiplexer::MultiplexerMessage*>(&whole));
      if (reply_ == WHOLE_WITH_TYPE) {
        reply.set("type", static_cast<std::uint32_t>(multiplexer::types::PYTHON_TEST_RESPONSE));
      }
      send_message(reply);
      ++announcements_;
      return;
    }
    mx::util::kwargs::Kwargs reply;
    reply.set("message", "re: " + mxmsg.message());
    if (reply_ != THROW) {  // a reply without a type throws KeyError where it is built
      reply.set("type", static_cast<std::uint32_t>(multiplexer::types::PYTHON_TEST_RESPONSE));
    }
    if (reply_ == ANSWER_EVERYWHERE) {
      reply.set("multiplexer", ALL);  // as documented, which needs the constant's definition unoptimized
    }
    send_message(reply);
    ++announcements_;
  }
  bool on_handler_exception(const std::exception&) override {
    std::lock_guard<std::mutex> lock(exceptions_mutex_);
    ++exceptions;
    exceptions_counted_.notify_all();
    return true;
  }
  void periodic_task() override {
    if (!serving.exchange(true)) {
      ++announcements_;  // before any request: there is none to reply to
    }
    for (; announcements_ > 0; --announcements_) {
      send_message(mx::util::kwargs::Kwargs()
                       .set("message", std::string("announced"))
                       .set("type", static_cast<std::uint32_t>(multiplexer::types::TEST_EVENT)));
    }
    if (leave.load()) {
      working = false;
    }
  }

 private:
  const Reply reply_;
  int announcements_ = 0;
  std::mutex exceptions_mutex_;
  std::condition_variable exceptions_counted_;
};

// Serves `backend` on a thread of its own until destroyed.
struct Serving {
  explicit Serving(ReplyingBackend& backend) : backend(backend), thread([&backend] { backend.serve_forever(0.05f); }) {
    for (int waited = 0; waited < 500 && !backend.serving.load(); ++waited) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
  }
  ~Serving() {
    backend.leave = true;
    thread.join();
  }
  ReplyingBackend& backend;
  std::thread thread;
};

// A request through `client`, flushed; its id.
std::uint64_t request(multiplexer::Client& client, const std::string& payload) {
  multiplexer::MultiplexerMessage msg;
  msg.set_id(client.random64());
  msg.set_from(client.instance_id());
  msg.set_type(multiplexer::types::PYTHON_TEST_REQUEST);
  msg.set_message(payload);
  client.flush(client.schedule_one(msg), 5);
  return msg.id();
}

// The payloads of the next `count` messages `listener` receives, fewer if
// they do not come within 10 s.
std::vector<std::string> payloads(multiplexer::Client& listener, int count) {
  std::vector<std::string> got;
  const std::chrono::steady_clock::time_point deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (static_cast<int>(got.size()) < count && std::chrono::steady_clock::now() < deadline) {
    try {
      got.push_back(listener.receive_message(1).first->message());
    } catch (const multiplexer::Client::OperationTimedOut&) {
    }
  }
  return got;
}

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

// A search for a backend is answered with a PING carrying the search back,
// as a PING is; it came back empty. One whose echo would be over the limit
// gets BACKEND_ERROR saying so, and the backend serves on.
TEST(ServeThread, ASearchIsAnsweredWithItsPayloadEchoed) {
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
  multiplexer::MultiplexerMessage search;
  search.set_id(client.random64());
  search.set_from(client.instance_id());
  search.set_to(backend.conn_for_test()->instance_id());
  search.set_type(multiplexer::types::BACKEND_FOR_PACKET_SEARCH);
  search.set_message("what the searcher sent");
  client.flush(client.schedule_one(search), 10);
  std::shared_ptr<multiplexer::MultiplexerMessage> answer = reply_to(client, search.id());
  ASSERT_TRUE(answer) << "no answer to the search";
  EXPECT_EQ(multiplexer::types::PING, answer->type());
  EXPECT_EQ("what the searcher sent", answer->message());
  search.set_id(client.random64());
  search.set_message(std::string(multiplexer::MAX_MESSAGE_SIZE - search.ByteSizeLong() - 16, 's'));
  while (search.ByteSizeLong() < multiplexer::MAX_MESSAGE_SIZE) {
    search.mutable_message()->push_back('s');
  }
  client.flush(client.schedule_one(search), 10);
  answer = reply_to(client, search.id());
  ASSERT_TRUE(answer) << "no answer to the big search";
  EXPECT_EQ(multiplexer::types::BACKEND_ERROR, answer->type());
  EXPECT_NE(std::string::npos, answer->message().find("echo of a search")) << answer->message();
  backend.leave = true;
  server.join();
  EXPECT_EQ(0, backend.exceptions.load());
  EXPECT_FALSE(ended_by_exception.load());
}

// What a backend sends from periodic_task() is no reply: routed by its
// type, before any request arrived as after one. It dereferenced a null
// request before the first, and after one it went to that requester,
// referencing the request, rather than to the peers of the event's type.
TEST(ServeThread, WhatPeriodicTaskSendsIsRoutedByItsType) {
  InProcessMultiplexer mx;
  MultiplexerAddresses addresses;
  addresses.push_back(std::make_pair("127.0.0.1", mx.port));
  multiplexer::Client listener(multiplexer::peers::TEST_EVENT_BACKEND);
  ASSERT_TRUE(listener.connect("127.0.0.1", mx.port, 5));
  ReplyingBackend backend(addresses, ReplyingBackend::ANSWER);
  Serving serving(backend);
  ASSERT_TRUE(backend.serving.load());
  multiplexer::Client client(multiplexer::peers::WEBSITE);
  ASSERT_TRUE(client.connect("127.0.0.1", mx.port, 5));
  std::shared_ptr<multiplexer::MultiplexerMessage> answer = reply_to(client, request(client, "question"));
  ASSERT_TRUE(answer) << "no answer";
  EXPECT_EQ("re: question", answer->message());
  EXPECT_EQ(std::vector<std::string>({"announced", "announced"}), payloads(listener, 2))
      << "the one before the request and the one after it";
}

// A reply that threw where it was built is no answer: the requester gets
// BACKEND_ERROR, where the reply counted as sent before it was, and the
// requester waited out its timeout.
TEST(ServeThread, AReplyThatThrowsIsAnsweredWithBackendError) {
  InProcessMultiplexer mx;
  MultiplexerAddresses addresses;
  addresses.push_back(std::make_pair("127.0.0.1", mx.port));
  ReplyingBackend backend(addresses, ReplyingBackend::THROW);
  Serving serving(backend);
  ASSERT_TRUE(backend.serving.load());
  multiplexer::Client client(multiplexer::peers::WEBSITE);
  ASSERT_TRUE(client.connect("127.0.0.1", mx.port, 5));
  std::shared_ptr<multiplexer::MultiplexerMessage> answer = reply_to(client, request(client, "question"));
  ASSERT_TRUE(answer) << "no answer";
  EXPECT_EQ(multiplexer::types::BACKEND_ERROR, answer->type());
  EXPECT_EQ(1, backend.exceptions_once(1));
}

// A backend that calls notify_start() first, as the docs suggest for a long
// handler: the synchronous client's query returns the reply, where its
// first stage took the REQUEST_RECEIVED acknowledgement for the answer and
// left the reply a stray.
TEST(ServeThread, AQueryIgnoresTheBackendsAcknowledgement) {
  InProcessMultiplexer mx;
  MultiplexerAddresses addresses;
  addresses.push_back(std::make_pair("127.0.0.1", mx.port));
  ReplyingBackend backend(addresses, ReplyingBackend::NOTIFY_THEN_ANSWER);
  Serving serving(backend);
  ASSERT_TRUE(backend.serving.load());
  multiplexer::Client client(multiplexer::peers::WEBSITE);
  ASSERT_TRUE(client.connect("127.0.0.1", mx.port, 5));
  multiplexer::IncomingMessage reply = client.query("question", multiplexer::types::PYTHON_TEST_REQUEST, 10);
  EXPECT_EQ(multiplexer::types::PYTHON_TEST_RESPONSE, reply.third->type());
  EXPECT_EQ("re: question", reply.third->message());
}

// Throws on every message but the marker, as a handler that parses every
// payload does: sends `peer` an event at its first poll, and the marker
// once its handler threw on a BACKEND_ERROR, behind any report of that.
class RaisingBackend : public BaseMultiplexerServer {
 public:
  explicit RaisingBackend(const MultiplexerAddresses& addresses)
      : BaseMultiplexerServer(addresses, multiplexer::peers::PYTHON_TEST_SERVER) {}
  std::uint64_t instance_id() const { return conn->instance_id(); }
  std::atomic<std::uint64_t> peer{0};
  std::atomic<bool> serving{false};
  std::atomic<bool> leave{false};
  std::atomic<int> reports{0};  // the BACKEND_ERRORs its handler got
  std::atomic<bool> marked{false};

 protected:
  void handle_message(multiplexer::MultiplexerMessage& mxmsg) override {
    if (mxmsg.type() == multiplexer::types::PYTHON_TEST_RESPONSE) {
      marked = true;
      no_response();
      return;
    }
    raised_on_report_ = mxmsg.type() == multiplexer::types::BACKEND_ERROR;
    if (raised_on_report_) {
      ++reports;
    }
    throw std::runtime_error("unexpected type " + std::to_string(mxmsg.type()));
  }
  bool on_handler_exception(const std::exception&) override {
    handled_report_ = handled_report_ || raised_on_report_;  // after the report, if any
    return true;
  }
  void periodic_task() override {
    serving = true;
    const std::uint64_t to = peer.load();
    if (to && !sent_event_) {
      sent_event_ = true;
      send_message(mx::util::kwargs::Kwargs()
                       .set("message", std::string("unexpected"))
                       .set("type", static_cast<std::uint32_t>(multiplexer::types::PYTHON_TEST_REQUEST))
                       .set("to", to));
    }
    if (handled_report_ && !sent_marker_) {
      sent_marker_ = true;
      send_message(mx::util::kwargs::Kwargs()
                       .set("message", std::string("marker"))
                       .set("type", static_cast<std::uint32_t>(multiplexer::types::PYTHON_TEST_RESPONSE))
                       .set("to", to));
    }
    if (leave.load()) {
      working = false;
    }
  }

 private:
  bool raised_on_report_ = false, handled_report_ = false, sent_event_ = false, sent_marker_ = false;
};

// X sends Y an event, Y's handler throws and Y reports it to X with
// BACKEND_ERROR, and X's handler throws on that report: X sends no report
// back, where the two answered each other's reports for good. X's marker,
// sent after it handled the report, reaches Y behind anything X sent.
TEST(ServeThread, TwoBackendsAnswerNoReportWithAReport) {
  InProcessMultiplexer mx;
  MultiplexerAddresses addresses;
  addresses.push_back(std::make_pair("127.0.0.1", mx.port));
  RaisingBackend x(addresses), y(addresses);
  std::thread y_thread([&y] { y.serve_forever(0.05f); });
  std::thread x_thread([&x] { x.serve_forever(0.05f); });
  for (int waited = 0; waited < 500 && !(x.serving.load() && y.serving.load()); ++waited) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  x.peer = y.instance_id();
  for (int waited = 0; waited < 1000 && !y.marked.load(); ++waited) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  x.leave = true;
  y.leave = true;
  x_thread.join();
  y_thread.join();
  EXPECT_TRUE(y.marked.load()) << "the marker never came";
  EXPECT_EQ(1, x.reports.load()) << "X got Y's report, once";
  EXPECT_EQ(0, y.reports.load()) << "X did not report on Y's report";
}

// A query's on_received hears the instance id of the backend that
// acknowledged the request with notify_start(), once, before the reply:
// on SyncClient on the caller's thread inside query(), on ThreadedClient
// on the io thread, in both of its forms.
TEST(ServeThread, AQueryHearsWhichBackendAcknowledgedIt) {
  InProcessMultiplexer mx;
  MultiplexerAddresses addresses;
  addresses.push_back(std::make_pair("127.0.0.1", mx.port));
  ReplyingBackend backend(addresses, ReplyingBackend::NOTIFY_THEN_ANSWER);
  Serving serving(backend);
  ASSERT_TRUE(backend.serving.load());
  const std::vector<std::uint64_t> once{backend.instance_id()};

  multiplexer::Client client(multiplexer::peers::WEBSITE);
  ASSERT_TRUE(client.connect("127.0.0.1", mx.port, 5));
  std::vector<std::uint64_t> heard;
  std::thread::id heard_on;
  multiplexer::IncomingMessage reply = client.query("question", multiplexer::types::PYTHON_TEST_REQUEST, 10,
                                                    multiplexer::LanePtr(), [&](std::uint64_t from) {
                                                      heard.push_back(from);
                                                      heard_on = std::this_thread::get_id();
                                                    });
  EXPECT_EQ("re: question", reply.third->message());
  EXPECT_EQ(once, heard) << "the SyncClient's, by the time query() returned";
  EXPECT_EQ(std::this_thread::get_id(), heard_on);

  // The ThreadedClient's: what the io thread told, in its order. Before the
  // client: on an early exit, its shutdown still runs the callbacks.
  std::mutex lock;
  std::vector<std::string> told;
  auto tell = [&](const std::string& what) {
    std::lock_guard<std::mutex> hold(lock);
    told.push_back(what);
  };
  const std::string received = "received from " + std::to_string(backend.instance_id());
  multiplexer::ReceivedCallback on_received = [&](std::uint64_t from) {
    tell("received from " + std::to_string(from));
  };
  std::promise<multiplexer::ThreadedClient::Result> done;
  multiplexer::ThreadedClient threaded(multiplexer::peers::WEBSITE);
  ASSERT_TRUE(threaded.connect("127.0.0.1", mx.port, 5));
  multiplexer::ThreadedClient::Result result =
      threaded.query(threaded.new_message(multiplexer::types::PYTHON_TEST_REQUEST, "blocking"), 10,
                     multiplexer::LanePtr(), on_received);
  ASSERT_EQ(multiplexer::ThreadedClient::REPLIED, result.outcome);
  tell("returned");
  threaded.query(
      threaded.new_message(multiplexer::types::PYTHON_TEST_REQUEST, "callback"),
      [&](const multiplexer::ThreadedClient::Result& result) {
        tell("answered");
        done.set_value(result);
      },
      10, multiplexer::LanePtr(), on_received);
  std::future<multiplexer::ThreadedClient::Result> answered = done.get_future();
  ASSERT_EQ(std::future_status::ready, answered.wait_for(std::chrono::seconds(10)));
  EXPECT_EQ("re: callback", answered.get().reply.third->message());
  threaded.shutdown();
  std::lock_guard<std::mutex> hold(lock);
  EXPECT_EQ((std::vector<std::string>{received, "returned", received, "answered"}), told);
}

// Two backends, each behind a multiplexer of its own: the first
// acknowledges a request and never answers it, the second acknowledges and
// answers.
struct TwoBackends {
  TwoBackends()
      : first(new InProcessMultiplexer()),
        holding({{"127.0.0.1", first->port}}, ReplyingBackend::NOTIFY_THEN_HOLD),
        answering({{"127.0.0.1", second.port}}, ReplyingBackend::NOTIFY_THEN_ANSWER),
        held(holding),
        served(answering) {}
  std::unique_ptr<InProcessMultiplexer> first;  // reset() takes it away
  InProcessMultiplexer second;
  ReplyingBackend holding;
  ReplyingBackend answering;
  Serving held;
  Serving served;
};

// The first backend acknowledges the request and its multiplexer goes away
// under the wait: the request goes again through the other multiplexer, to
// the other backend, which acknowledges it too, and on_received hears
// both, the second telling the caller the request may be running twice.
// The SyncClient's callback takes the multiplexer away itself, on the
// caller's thread, where it runs.
TEST(ServeThread, AQueryHearsEachBackendItsRetriesReached) {
  TwoBackends backends;
  ASSERT_TRUE(backends.holding.serving.load() && backends.answering.serving.load());
  multiplexer::Client client(multiplexer::peers::WEBSITE);
  multiplexer::ConnectionWrapper to_first = client.connect("127.0.0.1", backends.first->port, 5);
  ASSERT_TRUE(to_first);
  ASSERT_TRUE(client.connect("127.0.0.1", backends.second.port, 5));
  std::vector<std::uint64_t> heard;
  multiplexer::IncomingMessage reply =
      client.query("question", multiplexer::types::PYTHON_TEST_REQUEST, 10,
                   std::make_shared<multiplexer::Lane>(to_first), [&](std::uint64_t from) {
                     heard.push_back(from);
                     if (heard.size() == 1) {
                       backends.first.reset();
                     }
                   });
  EXPECT_EQ("re: question", reply.third->message());
  EXPECT_EQ((std::vector<std::uint64_t>{backends.holding.instance_id(), backends.answering.instance_id()}), heard);
}

// The same on ThreadedClient, connected to the first multiplexer alone
// until the first backend acknowledged, then to the second too, and the
// first goes.
TEST(ServeThread, AThreadedQueryHearsEachBackendItsRetriesReached) {
  TwoBackends backends;
  ASSERT_TRUE(backends.holding.serving.load() && backends.answering.serving.load());
  // Before the client: its shutdown may still tell them, and they must be alive.
  std::mutex lock;
  std::vector<std::uint64_t> heard;
  std::promise<void> acknowledged;
  std::promise<multiplexer::ThreadedClient::Result> done;
  multiplexer::ThreadedClient client(multiplexer::peers::WEBSITE);
  ASSERT_TRUE(client.connect("127.0.0.1", backends.first->port, 5));
  client.query(
      client.new_message(multiplexer::types::PYTHON_TEST_REQUEST, "question"),
      [&](const multiplexer::ThreadedClient::Result& result) { done.set_value(result); }, 10, multiplexer::LanePtr(),
      [&](std::uint64_t from) {
        std::lock_guard<std::mutex> hold(lock);
        heard.push_back(from);
        if (heard.size() == 1) {
          acknowledged.set_value();
        }
      });
  ASSERT_EQ(std::future_status::ready, acknowledged.get_future().wait_for(std::chrono::seconds(10)));
  ASSERT_TRUE(client.connect("127.0.0.1", backends.second.port, 5));
  backends.first.reset();
  std::future<multiplexer::ThreadedClient::Result> answered = done.get_future();
  ASSERT_EQ(std::future_status::ready, answered.wait_for(std::chrono::seconds(10)));
  multiplexer::ThreadedClient::Result result = answered.get();
  ASSERT_EQ(multiplexer::ThreadedClient::REPLIED, result.outcome);
  EXPECT_EQ("re: question", result.reply.third->message());
  std::lock_guard<std::mutex> hold(lock);
  EXPECT_EQ((std::vector<std::uint64_t>{backends.holding.instance_id(), backends.answering.instance_id()}), heard);
}

// A backend that declines every search, as a saturated one does, behind
// the second of two multiplexers, and a SyncClient on both: a typed query
// sent through the first finds nobody, the search being declined; an
// addressed one meets a delivery error there, locates the backend with a
// PING, which it answers whatever its search policy, and is answered
// through the second.
TEST(ServeThread, AnAddressedQueryLocatesABackendThatDeclinesSearches) {
  InProcessMultiplexer first;
  InProcessMultiplexer second;
  ReplyingBackend backend({{"127.0.0.1", second.port}}, ReplyingBackend::ANSWER);
  backend.declining_searches = true;
  Serving serving(backend);
  ASSERT_TRUE(backend.serving.load());
  multiplexer::Client client(multiplexer::peers::WEBSITE);
  multiplexer::ConnectionWrapper wrong = client.connect("127.0.0.1", first.port, 5);
  ASSERT_TRUE(wrong);
  multiplexer::ConnectionWrapper right = client.connect("127.0.0.1", second.port, 5);
  ASSERT_TRUE(right);
  EXPECT_THROW(
      client.query("typed", multiplexer::types::PYTHON_TEST_REQUEST, 1.5f, std::make_shared<multiplexer::Lane>(wrong)),
      multiplexer::Client::OperationTimedOut)
      << "the search found it";
  multiplexer::MultiplexerMessage request;
  request.set_type(multiplexer::types::PYTHON_TEST_REQUEST);
  request.set_message("saturated");
  request.set_to(backend.instance_id());
  multiplexer::IncomingMessage reply = client.query(request, 5, std::make_shared<multiplexer::Lane>(wrong));
  EXPECT_EQ("re: saturated", reply.third->message());
  EXPECT_TRUE(reply.second.is_same_connection(right)) << "not through the second multiplexer";
}

// A reply may name the connections it goes through, `multiplexer` being
// one of send_message()'s documented keys, which the debug-build check of
// the keys refused.
// A reply given as a whole MultiplexerMessage that sets only its type and
// payload gets the rest from the request it answers, as a threaded
// server's Request::reply fills it: to, references and workflow, so the
// requester reads it as its answer, where it went out as it was, an
// answer to nothing.
TEST(ServeThread, AWholeMessageReplyAnswersTheRequest) {
  InProcessMultiplexer mx;
  ReplyingBackend backend(MultiplexerAddresses{{"127.0.0.1", mx.port}}, ReplyingBackend::WHOLE);
  Serving serving(backend);
  ASSERT_TRUE(backend.serving.load());
  multiplexer::Client client(multiplexer::peers::WEBSITE);
  ASSERT_TRUE(client.connect("127.0.0.1", mx.port, 5));
  std::shared_ptr<multiplexer::MultiplexerMessage> answer = reply_to(client, request(client, "question"));
  ASSERT_TRUE(answer) << "no answer";
  EXPECT_EQ(multiplexer::types::PYTHON_TEST_RESPONSE, answer->type());
  EXPECT_EQ("re: question", answer->message());
}

// A whole MultiplexerMessage goes as it is: given with a `type` beside it,
// send_message() throws std::invalid_argument, which the server reports to
// the requester as BACKEND_ERROR, where the type was dropped unsaid and the
// message went out as it was.
TEST(ServeThread, FieldsBesideAWholeMessageAreRefused) {
  InProcessMultiplexer mx;
  ReplyingBackend backend(MultiplexerAddresses{{"127.0.0.1", mx.port}}, ReplyingBackend::WHOLE_WITH_TYPE);
  Serving serving(backend);
  ASSERT_TRUE(backend.serving.load());
  multiplexer::Client client(multiplexer::peers::WEBSITE);
  ASSERT_TRUE(client.connect("127.0.0.1", mx.port, 5));
  std::shared_ptr<multiplexer::MultiplexerMessage> answer = reply_to(client, request(client, "question"));
  ASSERT_TRUE(answer) << "no answer";
  EXPECT_EQ(multiplexer::types::BACKEND_ERROR, answer->type());
  EXPECT_EQ(1, backend.exceptions_once(1));
}

TEST(ServeThread, AReplyMayNameItsMultiplexer) {
  InProcessMultiplexer mx;
  MultiplexerAddresses addresses;
  addresses.push_back(std::make_pair("127.0.0.1", mx.port));
  ReplyingBackend backend(addresses, ReplyingBackend::ANSWER_EVERYWHERE);
  Serving serving(backend);
  ASSERT_TRUE(backend.serving.load());
  multiplexer::Client client(multiplexer::peers::WEBSITE);
  ASSERT_TRUE(client.connect("127.0.0.1", mx.port, 5));
  std::shared_ptr<multiplexer::MultiplexerMessage> answer = reply_to(client, request(client, "question"));
  ASSERT_TRUE(answer) << "no answer";
  EXPECT_EQ("re: question", answer->message());
}

namespace {

// How a drain_twelve() went: how long the backend took to leave, from the
// first request, the moment its drain ended, and what it refused itself.
struct TwelveDrained {
  std::chrono::steady_clock::duration took;
  bool waiting;       // requests read and not handled when the drain ended
  bool served_after;  // requests handled after the drain ended
  int refused_by_backend;
};

// Twelve requests at once to a SlowBackend that starts its drain once it
// has handled two, the drain capped at `drain_seconds` and, with
// `drain_ends_with_waiting`, over only with requests read and waiting.
// Every request has one outcome: a response from the backend, or a
// delivery error, the backend's or the multiplexer's, told apart by their
// `from`; none vanishes into a timeout, and what the backend served was
// answered.
TwelveDrained drain_twelve(float drain_seconds, bool drain_ends_with_waiting) {
  InProcessMultiplexer mx;
  SlowBackend backend(MultiplexerAddresses{{"127.0.0.1", mx.port}}, 2, drain_ends_with_waiting);
  std::future<void> served =
      std::async(std::launch::async, [&backend, drain_seconds] { backend.serve_forever(0.05f, drain_seconds); });
  for (int waited = 0; waited < 500 && !backend.serving.load(); ++waited) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));  // registered before anything is sent
  }
  EXPECT_TRUE(backend.serving.load());
  multiplexer::Client client(multiplexer::peers::WEBSITE);
  EXPECT_TRUE(client.connect("127.0.0.1", mx.port, 5));
  const std::chrono::steady_clock::time_point started = std::chrono::steady_clock::now();
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
  if (served.wait_for(std::chrono::seconds(30)) != std::future_status::ready) {
    ADD_FAILURE() << "the drain never ended";
    backend.stop();
  }
  served.get();
  const std::chrono::steady_clock::duration took = std::chrono::steady_clock::now() - started;
  int responses = 0;
  int refused_by_backend = 0;
  int refused_by_multiplexer = 0;
  const std::chrono::steady_clock::time_point deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!sent.empty() && std::chrono::steady_clock::now() < deadline) {
    try {
      std::pair<std::shared_ptr<multiplexer::MultiplexerMessage>, multiplexer::ConnectionWrapper> got =
          client.receive_message(0.5f);
      if (!sent.erase(got.first->references())) {
        continue;
      }
      if (got.first->type() != multiplexer::types::DELIVERY_ERROR) {
        ++responses;
      } else if (got.first->from() == backend.instance_id()) {
        ++refused_by_backend;
      } else {
        ++refused_by_multiplexer;
      }
    } catch (const multiplexer::Client::OperationTimedOut&) {
    }
  }
  EXPECT_EQ(12, responses + refused_by_backend + refused_by_multiplexer) << "none vanished into a timeout";
  EXPECT_EQ(backend.handled.load(), responses) << "what the backend served was answered";
  return TwelveDrained{took, backend.waiting_when_drained.load(),
                       backend.handled.load() > backend.handled_when_drained.load(), refused_by_backend};
}

}  // namespace

// A draining BaseMultiplexerServer serves what it had already read: the
// requests the synchronous client pulled off the socket while a reply was
// being sent are handled before the connections close (drain_twelve). The
// drain routing turns every path off, so the drain ends on the
// multiplexer's confirmation, which comes behind every request routed to
// the backend and counts only once those read are handled: well within
// the cap, nothing left waiting, and nothing the backend had read refused
// by it, where the close refused what the loop had not handled.
TEST(ServeThread, ADrainServesWhatWasAlreadyRead) {
  const TwelveDrained drained = drain_twelve(10.0f, false);
  EXPECT_LT(drained.took, std::chrono::seconds(5)) << "on the confirmation, not the cap";
  EXPECT_FALSE(drained.waiting) << "the confirmation counted with requests waiting";
  EXPECT_EQ(0, drained.refused_by_backend) << "the backend refused what it had read when the drain ended";
}

// A drain on its cap, 0.2 s, that the backend says is over only with
// requests read and waiting, so that they are waiting when it ends by
// construction: it serves them after the drain ended, the loop gone,
// where the close refused them.
TEST(ServeThread, ADrainOnItsCapServesWhatWasAlreadyRead) {
  const TwelveDrained drained = drain_twelve(0.2f, true);
  ASSERT_TRUE(drained.waiting) << "nothing waiting when the drain ended";
  EXPECT_TRUE(drained.served_after) << "what it had read when the drain ended was refused, not served";
}

// A drain with no cap, a negative drain_seconds as an infinite one, is
// never over by time: with a path kept open only stop() ends it, where the
// negative cap had passed as the drain began.
TEST(ServeThread, ADrainWithNoCapIsNotOverByTime) {
  InProcessMultiplexer mx;
  OpenDrainBackend backend(MultiplexerAddresses{{"127.0.0.1", mx.port}});
  std::thread server([&backend] { backend.serve_forever(0.05f, -1.0f); });
  for (int waited = 0; waited < 500 && !backend.serving.load(); ++waited) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  EXPECT_TRUE(backend.serving.load());
  backend.leave = true;
  for (int waited = 0; waited < 500 && !backend.checked.load(); ++waited) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  EXPECT_TRUE(backend.checked.load());
  EXPECT_FALSE(backend.drained_at_once.load()) << "no cap";
  backend.stop();  // the thread joined whatever was found: an ASSERT would leave it running
  server.join();
}

// A drain with no time, a NaN drain_seconds as 0, is over as it begins,
// whatever path it keeps open, where NaN, compared with what had passed,
// was no cap at all.
TEST(ServeThread, ADrainWithNoTimeIsOverAtOnce) {
  InProcessMultiplexer mx;
  OpenDrainBackend backend(MultiplexerAddresses{{"127.0.0.1", mx.port}});
  std::thread server([&backend] { backend.serve_forever(0.05f, std::nanf("")); });
  for (int waited = 0; waited < 500 && !backend.serving.load(); ++waited) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  EXPECT_TRUE(backend.serving.load());
  backend.leave = true;
  for (int waited = 0; waited < 500 && !backend.checked.load(); ++waited) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  EXPECT_TRUE(backend.checked.load());
  EXPECT_TRUE(backend.drained_at_once.load()) << "no time";
  backend.stop();  // the thread joined whatever was found: an ASSERT would leave it running
  server.join();
}

// stop() with requests read and not handled yet: the close refuses them,
// so that their senders retry elsewhere at once, where they vanished into
// the senders' timeouts. Every request has one outcome.
TEST(ServeThread, AStopWithoutADrainRefusesWhatWasRead) {
  InProcessMultiplexer mx;
  MultiplexerAddresses addresses;
  addresses.push_back(std::make_pair("127.0.0.1", mx.port));
  SlowBackend backend(addresses);
  std::thread server([&backend] { backend.serve_forever(0.05f, 0.0f); });
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
  backend.stop();
  server.join();
  int responses = 0;
  int refused = 0;
  const std::chrono::steady_clock::time_point deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!sent.empty() && std::chrono::steady_clock::now() < deadline) {
    try {
      std::pair<std::shared_ptr<multiplexer::MultiplexerMessage>, multiplexer::ConnectionWrapper> got =
          client.receive_message(0.5f);
      if (!sent.erase(got.first->references())) {
        continue;
      }
      if (got.first->type() == multiplexer::types::DELIVERY_ERROR) {
        ++refused;
      } else {
        ++responses;
      }
    } catch (const multiplexer::Client::OperationTimedOut&) {
    }
  }
  EXPECT_EQ(12, responses + refused) << "none vanished into a timeout";
  EXPECT_GT(refused, 0) << "refused at the close";
}

// A lone backend that drains keeping its last-resort path open, under a
// flood that stays ahead of it by count (Flood), whatever the speed of
// either thread: it leaves at the end of its drain, having handled what it
// had read by then and refused what arrived later, where every reply's
// turn of the loop read more, handled in turn, so that it never left. The
// backend starts its drain once it has handled two, and says it is over,
// at its cap, only with requests read and waiting, so that there are some
// when it ends by construction: they are served after the loop, where the
// close refused them. Every request the flood sent has one outcome: a
// response; a delivery error, the backend's or, once it is gone, the
// multiplexer's; or, for one the multiplexer routed to the backend, still
// its last resort, in the moment it closed, a drop the backend counts and
// logs, which no answer follows.
TEST(ServeThread, ADrainUnderAFloodEnds) {
  InProcessMultiplexer mx;
  SlowBackend backend(MultiplexerAddresses{{"127.0.0.1", mx.port}}, 2, true);
  Flood flood;
  backend.flood = &flood;
  multiplexer::Routing last_resort;
  last_resort.set_any(false);
  last_resort.set_all(false);
  last_resort.set_last_resort(true);
  backend.set_drain_routing(last_resort);
  std::thread server([&backend, &flood] {
    backend.serve_forever(0.05f, 0.3f);
    flood.end(true);  // the backend left: the sender stops
  });
  for (int waited = 0; waited < 500 && !backend.serving.load(); ++waited) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));  // registered before anything is sent
  }
  ASSERT_TRUE(backend.serving.load());
  multiplexer::Client client(multiplexer::peers::WEBSITE);
  ASSERT_TRUE(client.connect("127.0.0.1", mx.port, 5));
  std::set<std::uint64_t> sent;
  const std::chrono::steady_clock::time_point flood_until = std::chrono::steady_clock::now() + std::chrono::seconds(20);
  while (flood.wait_to_send(flood_until)) {  // as fast as the lockstep lets it, until the backend has left
    multiplexer::MultiplexerMessage msg;
    msg.set_id(client.random64());
    msg.set_from(client.instance_id());
    msg.set_type(multiplexer::types::PYTHON_TEST_REQUEST);
    msg.set_message("f" + std::to_string(sent.size()));
    sent.insert(msg.id());
    client.flush(client.schedule_one(msg), 5);
    flood.sent_one();
  }
  const bool left_under_the_flood = flood.left();
  flood.end(false);  // the sender stopped: whatever waits for it stops waiting
  if (!left_under_the_flood) {
    backend.stop();  // the thread is joined whatever was found
  }
  server.join();
  EXPECT_TRUE(left_under_the_flood) << "serve_forever() went on under the flood";
  ASSERT_TRUE(backend.waiting_when_drained.load()) << "nothing waiting when the drain ended";
  // Counted, not waited out: the answers come until only the drops are
  // left, which get none.
  const std::size_t count = sent.size();
  const std::size_t dropped = backend.dropped_while_closing();
  std::size_t responses = 0;
  std::size_t refused = 0;
  const std::chrono::steady_clock::time_point deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (sent.size() > dropped && std::chrono::steady_clock::now() < deadline) {
    try {
      std::pair<std::shared_ptr<multiplexer::MultiplexerMessage>, multiplexer::ConnectionWrapper> got =
          client.receive_message(0.5f);
      if (!sent.erase(got.first->references())) {
        continue;
      }
      if (got.first->type() == multiplexer::types::DELIVERY_ERROR) {
        ++refused;
      } else {
        ++responses;
      }
    } catch (const multiplexer::Client::OperationTimedOut&) {
    }
  }
  EXPECT_EQ(count, responses + refused + dropped)
      << "none vanished into a timeout unless the backend counted it dropped at its close";
  EXPECT_EQ(static_cast<std::size_t>(backend.handled.load()), responses) << "what the backend served was answered";
  EXPECT_GT(backend.handled.load(), backend.handled_when_drained.load())
      << "what it had read when the drain ended was refused, not served";
  // The backend handled each request only once more than twice as many as
  // it had handled were sent, so more of the flood was refused or dropped
  // than served, by construction; which of the two is not, a request that
  // reaches the backend as it closes being dropped.
  EXPECT_GT(refused + dropped, static_cast<std::size_t>(backend.handled.load())) << "the flood outran it";
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

namespace {

// Throws an OperationTimedOut of its own from its handler, as one whose
// own query timed out does, and asks to stop on what its handler throws.
class TimingOutBackend : public BaseMultiplexerServer {
 public:
  explicit TimingOutBackend(const MultiplexerAddresses& addresses)
      : BaseMultiplexerServer(addresses, multiplexer::peers::PYTHON_TEST_SERVER) {}
  std::atomic<int> iterations{0};

 protected:
  void handle_message(multiplexer::MultiplexerMessage&) override { throw multiplexer::Client::OperationTimedOut(); }
  bool on_handler_exception(const std::exception&) override { return false; }
  void periodic_task() override { ++iterations; }
};

}  // namespace

// A handler's own OperationTimedOut goes by on_handler_exception() as any
// exception does: returning false ends serve_forever(), which throws it,
// where serve_forever() took it for its poll's timeout and served on. The
// poll's own timeouts still only end an iteration.
TEST(ServeThread, AHandlersOwnTimeoutEndsServeForeverWhenAskedTo) {
  InProcessMultiplexer mx;
  TimingOutBackend backend({{"127.0.0.1", mx.port}});
  backend.connect();
  std::future<void> served = std::async(std::launch::async, [&backend] { backend.serve_forever(0.05f); });
  for (int waited = 0; waited < 1000 && backend.iterations.load() < 2; ++waited) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  ASSERT_GE(backend.iterations.load(), 2) << "polls that ran out ended the loop";
  multiplexer::ThreadedClient client(multiplexer::peers::WEBSITE);
  ASSERT_TRUE(client.connect("127.0.0.1", mx.port, 5));
  client.query("request", multiplexer::types::PYTHON_TEST_REQUEST, 5);  // answered with the BACKEND_ERROR report
  const bool ended = served.wait_for(std::chrono::seconds(10)) == std::future_status::ready;
  if (!ended) {
    backend.stop();  // it served on
  }
  EXPECT_TRUE(ended) << "the handler's OperationTimedOut did not end serve_forever()";
  EXPECT_THROW(served.get(), multiplexer::Client::OperationTimedOut);
}
