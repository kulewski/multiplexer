// multiplexer::Monitor's tests: /healthz and /readyz answered from the
// loops of an in-process multiplexer on one loop and on three, a loop held
// in a handler named in a 503 until it runs again, and /readyz from the
// moment the multiplexer stops.
#include "multiplexer/monitor.h"

#include <gtest/gtest.h>
#include <sys/socket.h>
#include <unistd.h>

#include <asio/ip/address.hpp>
#include <future>
#include <string>

#include "multiplexer/in_process_multiplexer.h"

namespace {

using multiplexer::Monitor;
using multiplexer::testing::InProcessMultiplexer;

const char* const OK = "HTTP/1.0 200 OK\nok\n";

asio::ip::tcp::endpoint loopback(std::uint16_t port = 0) {
  return asio::ip::tcp::endpoint(asio::ip::make_address("127.0.0.1"), port);
}

// The status line and the body of the answer to GET `path`, asked on a
// connection of its own as a probe asks.
std::string ask(std::uint16_t port, const std::string& path) {
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
  const std::size_t line_end = text.find("\r\n");
  const std::size_t head_end = text.find("\r\n\r\n");
  if (line_end == std::string::npos || head_end == std::string::npos) {
    return "no answer: '" + text + "'";
  }
  return text.substr(0, line_end) + "\n" + text.substr(head_end + 4);
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
// string is ignored, another path is not served.
TEST(Monitor, BothAnswerOkWhileEveryLoopRuns) {
  for (unsigned int threads : {1u, 3u}) {
    InProcessMultiplexer mx(0, threads);
    Monitor monitor(mx.server, loopback());
    EXPECT_EQ(ask(monitor.port(), "/healthz"), OK) << threads;
    EXPECT_EQ(ask(monitor.port(), "/readyz"), OK) << threads;
    EXPECT_EQ(ask(monitor.port(), "/healthz?verbose=1"), OK) << threads;
    EXPECT_EQ(ask(monitor.port(), "/metrics"),
              "HTTP/1.0 404 Not Found\nnot found: /healthz and /readyz are served here\n")
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

}  // namespace
