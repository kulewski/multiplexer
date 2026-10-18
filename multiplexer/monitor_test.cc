// multiplexer::Monitor's tests: /healthz and /readyz answered from the
// loops of an in-process multiplexer on one loop and on three, a loop held
// in a handler named in a 503 until it runs again, and /readyz from the
// moment the multiplexer stops; /metrics in Prometheus' text format, a
// held loop's lag, and a held main loop's 503. What /metrics counts is
// metrics_test.py's.
#include "multiplexer/monitor.h"

#include <gtest/gtest.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <asio/ip/address.hpp>
#include <cstdlib>
#include <future>
#include <string>
#include <vector>

#include "lib/release.h"
#include "multiplexer/Multiplexer.pb.h" /* generated */
#include "multiplexer/connections_manager.h"
#include "multiplexer/in_process_multiplexer.h"
#include "multiplexer/io/raw_message.h"
#include "multiplexer/multiplexer.constants.h" /* generated */

namespace {

using multiplexer::Monitor;
using multiplexer::testing::InProcessMultiplexer;

const char* const OK = "HTTP/1.0 200 OK\nok\n";

asio::ip::tcp::endpoint loopback(std::uint16_t port = 0) {
  return asio::ip::tcp::endpoint(asio::ip::make_address("127.0.0.1"), port);
}

// The whole answer to GET `path`, headers included, asked on a connection
// of its own as a probe asks.
std::string fetch(std::uint16_t port, const std::string& path) {
  const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  timeval backstop{60, 0};  // for a test that failed: every answer comes well before
  ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &backstop, sizeof(backstop));
  const asio::ip::tcp::endpoint endpoint = loopback(port);
  std::string text;
  if (::connect(fd, endpoint.data(), endpoint.size()) == 0) {
    const std::string request = "GET " + path + " HTTP/1.1\r\nHost: localhost\r\n\r\n";
    if (::send(fd, request.data(), request.size(), MSG_NOSIGNAL) == static_cast<ssize_t>(request.size())) {
      char chunk[4096];
      ssize_t bytes;
      while ((bytes = ::recv(fd, chunk, sizeof(chunk), 0)) > 0) {
        text.append(chunk, static_cast<std::size_t>(bytes));
      }
    }
  }
  ::close(fd);
  return text;
}

// The status line and the body of the answer to GET `path`.
std::string ask(std::uint16_t port, const std::string& path) {
  const std::string text = fetch(port, path);
  const std::size_t line_end = text.find("\r\n");
  const std::size_t head_end = text.find("\r\n\r\n");
  if (line_end == std::string::npos || head_end == std::string::npos) {
    return "no answer: '" + text + "'";
  }
  return text.substr(0, line_end) + "\n" + text.substr(head_end + 4);
}

// The body of /metrics' answer, which must be a 200 in Prometheus' text
// format; empty, the test failed, otherwise.
std::string metrics(std::uint16_t port) {
  const std::string text = fetch(port, "/metrics");
  EXPECT_EQ(0u, text.find("HTTP/1.0 200 OK\r\n")) << text;
  EXPECT_NE(std::string::npos, text.find("\r\nContent-Type: " + std::string(Monitor::METRICS_CONTENT_TYPE) + "\r\n"))
      << text;
  const std::size_t head_end = text.find("\r\n\r\n");
  return head_end == std::string::npos ? "" : text.substr(head_end + 4);
}

// The value of the sample `series`, a name and its labels as the body
// writes them, or -1 when the body has none.
double sample(const std::string& body, const std::string& series) {
  const std::string lines = "\n" + body;
  const std::size_t at = lines.find("\n" + series + " ");
  return at == std::string::npos ? -1 : std::strtod(lines.c_str() + at + series.size() + 2, nullptr);
}

// Writes `frame` to `fd`: whether all of it went.
bool send_frame(int fd, const multiplexer::RawMessage& frame) {
  for (const asio::const_buffer& part : frame.get_message_buffer()) {
    if (::send(fd, part.data(), part.size(), MSG_NOSIGNAL) != static_cast<ssize_t>(part.size())) {
      return false;
    }
  }
  return true;
}

// Reads exactly `size` bytes into `at`, a minute at most, the backstop of
// a test that failed: whether they came.
bool read_exactly(int fd, void* buffer, std::size_t size) {
  char* at = static_cast<char*>(buffer);
  while (size) {
    pollfd readable{fd, POLLIN, 0};
    const ssize_t got = ::poll(&readable, 1, 60000) == 1 ? ::recv(fd, at, size, 0) : -1;
    if (got <= 0) {
      return false;
    }
    at += got;
    size -= static_cast<std::size_t>(got);
  }
  return true;
}

// The next frame on `fd` that is not a heartbeat, parsed into `message`:
// whether one came.
bool read_message(int fd, multiplexer::MultiplexerMessage* message) {
  for (;;) {
    multiplexer::RawMessage frame;
    if (!read_exactly(fd, frame.get_header_buffer().data(), frame.get_header_buffer().size()) ||
        !frame.unpack_header() || !read_exactly(fd, frame.get_body_buffer().data(), frame.get_body_buffer().size()) ||
        !frame.verify() || !message->ParseFromString(frame.get_message())) {
      return false;
    }
    if (message->type() != multiplexer::types::HEARTBIT) {
      return true;
    }
  }
}

// A peer of instance `id` on a connection of its own, its welcome sent and
// the multiplexer's read: the descriptor, or -1.
int welcomed_peer(std::uint16_t port, std::uint64_t id) {
  const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  const asio::ip::tcp::endpoint endpoint = loopback(port);
  multiplexer::MultiplexerMessage answer;
  if (fd < 0 || ::connect(fd, endpoint.data(), endpoint.size()) != 0 ||
      !send_frame(fd, *multiplexer::impl::create_welcome_message(multiplexer::peers::PYTHON_TEST_SERVER, id)) ||
      !read_message(fd, &answer) || answer.type() != multiplexer::types::CONNECTION_WELCOME) {
    if (fd >= 0) {
      ::close(fd);
    }
    return -1;
  }
  return fd;
}

// A handler that holds its loop until released, as a stuck one would.
class Hold {
 public:
  Hold() : released_(release_.get_future().share()) {}
  ~Hold() { release(); }
  void on(InProcessMultiplexer& mx, unsigned int loop) {
    std::shared_future<void> released = released_;
    mx.server->post_to_loop(loop, [released] { released.wait(); });
  }
  void release() {
    if (!done_) {
      done_ = true;
      release_.set_value();
    }
  }

 private:
  std::promise<void> release_;
  std::shared_future<void> released_;
  bool done_ = false;
};

// On one loop and on three, both answer 200 while every loop runs; a query
// string is ignored, a path not served is 404.
TEST(Monitor, BothAnswerOkWhileEveryLoopRuns) {
  for (unsigned int threads : {1u, 3u}) {
    InProcessMultiplexer mx(0, threads);
    Monitor monitor(mx.server, loopback());
    EXPECT_EQ(ask(monitor.port(), "/healthz"), OK) << threads;
    EXPECT_EQ(ask(monitor.port(), "/readyz"), OK) << threads;
    EXPECT_EQ(ask(monitor.port(), "/healthz?verbose=1"), OK) << threads;
    EXPECT_EQ(ask(monitor.port(), "/status"),
              "HTTP/1.0 404 Not Found\nnot found: /healthz, /readyz and /metrics are served here\n")
        << threads;
  }
}

// A loop held in a handler fails both, named, until it runs again. The
// second probe posts nothing more there: it waits again for the check the
// first one posted, which then runs as soon as the loop does.
TEST(Monitor, AStuckLoopFailsBothUntilItRunsAgain) {
  InProcessMultiplexer mx(0, 3);
  Monitor monitor(mx.server, loopback());
  Hold hold;
  hold.on(mx, 2);
  const std::string stuck =
      "HTTP/1.0 503 Service Unavailable\nevent loop(s) 2 of 3 (0 the main one) did not run a check within 2 s\n";
  EXPECT_EQ(ask(monitor.port(), "/healthz"), stuck);
  EXPECT_EQ(ask(monitor.port(), "/readyz"), stuck);
  hold.release();
  EXPECT_EQ(ask(monitor.port(), "/healthz"), OK);
}

TEST(Monitor, AStuckLoopOnItsOwnFailsHealthz) {
  InProcessMultiplexer mx(0, 1);
  Monitor monitor(mx.server, loopback());
  Hold hold;
  hold.on(mx, 0);
  EXPECT_EQ(ask(monitor.port(), "/healthz"),
            "HTTP/1.0 503 Service Unavailable\nthe event loop did not run a check within 2 s\n");
  hold.release();
  EXPECT_EQ(ask(monitor.port(), "/healthz"), OK);
}

// From the moment stop() began, a drain say, /readyz answers 503 without
// asking the loops.
TEST(Monitor, ReadyzFailsOnceTheMultiplexerStops) {
  InProcessMultiplexer mx(0, 3);
  Monitor monitor(mx.server, loopback());
  std::promise<void> stopped;
  mx.io_service.post([&] {
    mx.server->stop();
    stopped.set_value();
  });
  stopped.get_future().wait();
  EXPECT_EQ(ask(monitor.port(), "/readyz"), "HTTP/1.0 503 Service Unavailable\nthe multiplexer is stopping\n");
}

// /metrics on one loop and on three, of a multiplexer nobody used: a lag
// for every loop and none past them, the counts at 0, a peer type of the
// rules with no peer at 0, the version, and the process's own figures.
TEST(Monitor, MetricsAnswerInPrometheusTextFormat) {
  for (unsigned int threads : {1u, 3u}) {
    InProcessMultiplexer mx(0, threads);
    Monitor monitor(mx.server, loopback());
    const std::string body = metrics(monitor.port());
    for (unsigned int loop = 0; loop < threads; ++loop) {
      EXPECT_GE(sample(body, "mx_event_loop_lag_seconds{loop=\"" + std::to_string(loop) + "\"}"), 0) << body;
    }
    EXPECT_EQ(-1, sample(body, "mx_event_loop_lag_seconds{loop=\"" + std::to_string(threads) + "\"}")) << body;
    EXPECT_EQ(0, sample(body, "mx_messages_received_total")) << body;
    EXPECT_EQ(0, sample(body, "mx_received_bytes_total")) << body;
    EXPECT_EQ(0, sample(body, "mx_peers{peer_type=\"TEST_EVENT_BACKEND\"}")) << body;
    EXPECT_EQ(0, sample(body, "mx_queued_messages{peer_type=\"TEST_EVENT_BACKEND\"}")) << body;
    EXPECT_EQ(-1, sample(body, "mx_peers{peer_type=\"MULTIPLEXER\"}")) << "a reserved type is no peer's";
    EXPECT_EQ(1, sample(body, "mx_build_info{version=\"" + std::string(mx::release::version) + "\"}")) << body;
    EXPECT_EQ(0, sample(body, "mx_stopping")) << body;
    EXPECT_EQ(0, sample(body, "mx_rules_file_failing")) << body;
    EXPECT_GT(sample(body, "process_open_fds"), 0) << body;
  }
}

// A loop held in a handler: the scrape still answers, from the main loop,
// that loop's lag the CHECK_SECONDS its check waited at least.
TEST(Monitor, AStuckLoopShowsInItsLag) {
  InProcessMultiplexer mx(0, 3);
  Monitor monitor(mx.server, loopback());
  Hold hold;
  hold.on(mx, 2);
  const std::string body = metrics(monitor.port());
  EXPECT_GE(sample(body, "mx_event_loop_lag_seconds{loop=\"2\"}"), Monitor::CHECK_SECONDS) << body;
}

// The main loop held: no snapshot of the routing, so no metrics, a 503,
// until it runs again.
TEST(Monitor, AStuckMainLoopFailsMetrics) {
  InProcessMultiplexer mx(0, 1);
  Monitor monitor(mx.server, loopback());
  Hold hold;
  hold.on(mx, 0);
  EXPECT_EQ(ask(monitor.port(), "/metrics"),
            "HTTP/1.0 503 Service Unavailable\nthe main event loop did not answer within 2 s\n");
  hold.release();
  EXPECT_EQ(0, sample(metrics(monitor.port()), "mx_messages_received_total"));
}

// On three loops, two peers on each, every one sending itself a frame and
// reading it back: /metrics counts the peers and the frames, and their
// bytes, walking connections that loops beside the main one run, which a
// ThreadSanitizer build checks.
TEST(Monitor, MetricsCountPeersAndFramesOnEveryLoop) {
  const int PEERS = 6;
  InProcessMultiplexer mx(0, 3);
  Monitor monitor(mx.server, loopback());
  std::vector<int> peers;
  std::size_t bytes = 0;
  for (int peer = 0; peer < PEERS; ++peer) {
    peers.push_back(welcomed_peer(mx.port, 1000 + peer));
    ASSERT_GE(peers.back(), 0);
  }
  for (int peer = 0; peer < PEERS; ++peer) {
    multiplexer::MultiplexerMessage message;
    message.set_id(5000 + peer);
    message.set_sender(1000 + peer);
    message.set_to(1000 + peer);
    message.set_type(multiplexer::types::PYTHON_TEST_REQUEST);
    message.set_message(std::string(peer, 'x'));
    const std::shared_ptr<const multiplexer::RawMessage> frame(multiplexer::RawMessage::FromMessage(message));
    bytes += frame->get_header_length() + frame->get_body_length();
    ASSERT_TRUE(send_frame(peers[peer], *frame));
    multiplexer::MultiplexerMessage back;
    ASSERT_TRUE(read_message(peers[peer], &back));
    EXPECT_EQ(message.id(), back.id());
  }
  const std::string body = metrics(monitor.port());
  EXPECT_EQ(PEERS, sample(body, "mx_peers{peer_type=\"PYTHON_TEST_SERVER\"}")) << body;
  EXPECT_EQ(PEERS, sample(body, "mx_messages_received_total")) << body;
  EXPECT_EQ(static_cast<double>(bytes), sample(body, "mx_received_bytes_total")) << body;
  EXPECT_EQ(0, sample(body, "mx_queued_messages{peer_type=\"PYTHON_TEST_SERVER\"}")) << body;
  for (const int fd : peers) {
    ::close(fd);
  }
}

}  // namespace
