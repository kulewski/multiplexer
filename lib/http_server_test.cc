// mx::HttpServer's tests, each through a socket as a kubelet's probe or a
// scrape connects: a GET answered by the handler, the methods and heads it
// refuses, the deadline, the cap on connections, an accept tried again
// once the process has descriptors again, stop(), and the addresses it
// binds.
#include "lib/http_server.h"

#include <gtest/gtest.h>
#include <poll.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <unistd.h>

#include <asio/ip/address.hpp>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <memory>
#include <stdexcept>
#include <string>
#include <system_error>
#include <vector>

#include "lib/mutex.h"

namespace {

// How long a wait below may take before the test calls it failed: a
// backstop, which a passing test never reaches, short of the minute Bazel
// gives a small test, so that a failure says what it saw.
constexpr int BACKSTOP_SECONDS = 20;

asio::ip::tcp::endpoint loopback(std::uint16_t port = 0) {
  return asio::ip::tcp::endpoint(asio::ip::make_address("127.0.0.1"), port);
}

// A client's connection, blocking, as a probe makes one.
class Probe {
 public:
  // A socket for `endpoint`, connected unless `connect_now` is false.
  explicit Probe(const asio::ip::tcp::endpoint& endpoint, bool connect_now = true) : endpoint_(endpoint) {
    fd_ = ::socket(endpoint.protocol().family(), SOCK_STREAM, 0);
    if (fd_ < 0) {
      throw std::system_error(errno, std::generic_category(), "socket");
    }
    timeval backstop{BACKSTOP_SECONDS, 0};
    ::setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &backstop, sizeof(backstop));
    if (connect_now) {
      try {
        connect();
      } catch (...) {
        ::close(fd_);
        throw;
      }
    }
  }
  ~Probe() { ::close(fd_); }
  Probe(const Probe&) = delete;
  Probe& operator=(const Probe&) = delete;

  void connect() {
    if (::connect(fd_, endpoint_.data(), endpoint_.size()) != 0) {
      throw std::system_error(errno, std::generic_category(), "connect");
    }
  }
  void send(const std::string& text) {
    std::size_t sent = 0;
    while (sent < text.size()) {
      const ssize_t bytes = ::send(fd_, text.data() + sent, text.size() - sent, MSG_NOSIGNAL);
      if (bytes < 0) {
        throw std::system_error(errno, std::generic_category(), "send");
      }
      sent += static_cast<std::size_t>(bytes);
    }
  }
  // Everything the server sends until it closes the connection.
  std::string receive_all() {
    std::string text;
    char chunk[4096];
    for (;;) {
      const ssize_t bytes = ::recv(fd_, chunk, sizeof(chunk), 0);
      if (bytes == 0) {
        return text;
      }
      if (bytes < 0) {
        EXPECT_NE(errno, EAGAIN) << "the server neither answered nor closed within " << BACKSTOP_SECONDS << " s";
        return text;
      }
      text.append(chunk, static_cast<std::size_t>(bytes));
    }
  }

 private:
  const asio::ip::tcp::endpoint endpoint_;
  int fd_ = -1;
};

// The answer to `request`, on a connection of its own.
std::string ask(std::uint16_t port, const std::string& request) {
  Probe probe(loopback(port));
  probe.send(request);
  return probe.receive_all();
}

// A response as the server writes one.
std::string response(const std::string& status, const std::string& body, const std::string& headers = "") {
  return "HTTP/1.0 " + status + "\r\nContent-Type: text/plain\r\nContent-Length: " + std::to_string(body.size()) +
         "\r\nConnection: close\r\n" + headers + "\r\n" + body;
}

// A handler answering every path with one status and body, and the paths
// it was asked for, which the server's thread writes and the test reads.
class Recorder {
 public:
  mx::HttpHandler handler(int status, const std::string& body) {
    return [this, status, body](const std::string& path, std::string& reply) {
      mx::MutexLock lock(mutex_);
      paths_.push_back(path);
      reply = body;
      return status;
    };
  }
  std::vector<std::string> paths() {
    mx::MutexLock lock(mutex_);
    return paths_;
  }

 private:
  mx::Mutex mutex_;
  std::vector<std::string> paths_ MX_GUARDED_BY(mutex_);
};

TEST(HttpServer, AGetIsAnsweredByTheHandler) {
  Recorder recorder;
  mx::HttpServer server(loopback(), recorder.handler(200, "ok\n"));
  ASSERT_NE(server.port(), 0);
  EXPECT_EQ(ask(server.port(),
                "GET /healthz?verbose HTTP/1.1\r\nHost: 10.0.0.5:9180\r\nUser-Agent: kube-probe/1.30\r\n"
                "Accept: */*\r\nConnection: close\r\n\r\n"),
            response("200 OK", "ok\n"));
  EXPECT_EQ(recorder.paths(), std::vector<std::string>{"/healthz?verbose"});
}

TEST(HttpServer, TheStatusTheHandlerGivesIsSent) {
  Recorder unavailable;
  mx::HttpServer server(loopback(), unavailable.handler(503, "loop 2 did not answer within 2 s\n"));
  EXPECT_EQ(ask(server.port(), "GET /healthz HTTP/1.1\r\n\r\n"),
            response("503 Service Unavailable", "loop 2 did not answer within 2 s\n"));
  Recorder unnamed;
  mx::HttpServer other(loopback(), unnamed.handler(299, ""));
  EXPECT_EQ(ask(other.port(), "GET / HTTP/1.1\r\n\r\n"), response("299 ", ""));
}

// A handler of the typed form sets the answer's content type, as /metrics
// does for Prometheus; an error the server itself answers stays text/plain.
TEST(HttpServer, ATypedHandlerSetsTheContentType) {
  mx::HttpServer server(loopback(),
                        mx::HttpTypedHandler([](const std::string&, std::string& body, std::string& content_type) {
                          body = "mx_up 1\n";
                          content_type = "text/plain; version=0.0.4; charset=utf-8";
                          return 200;
                        }));
  const std::string answer = ask(server.port(), "GET /metrics HTTP/1.1\r\n\r\n");
  EXPECT_EQ(answer,
            "HTTP/1.0 200 OK\r\nContent-Type: text/plain; version=0.0.4; charset=utf-8\r\nContent-Length: 8\r\n"
            "Connection: close\r\n\r\nmx_up 1\n");
  EXPECT_EQ(ask(server.port(), "POST /metrics HTTP/1.1\r\n\r\n"),
            response("405 Method Not Allowed", "GET is the one method served\n", "Allow: GET\r\n"));
}

TEST(HttpServer, AHeadEndingInBareLineFeedsIsAnswered) {
  Recorder recorder;
  mx::HttpServer server(loopback(), recorder.handler(200, "ok\n"));
  EXPECT_EQ(ask(server.port(), "GET /metrics HTTP/1.0\n\n"), response("200 OK", "ok\n"));
  EXPECT_EQ(recorder.paths(), std::vector<std::string>{"/metrics"});
}

TEST(HttpServer, AHeadSentInPiecesIsAnswered) {
  Recorder recorder;
  mx::HttpServer server(loopback(), recorder.handler(200, "ok\n"));
  Probe probe(loopback(server.port()));
  for (const char* piece : {"GET /hea", "lthz HTTP/1.1\r", "\nHost: x\r\n", "\r", "\n"}) {
    probe.send(piece);
  }
  EXPECT_EQ(probe.receive_all(), response("200 OK", "ok\n"));
  EXPECT_EQ(recorder.paths(), std::vector<std::string>{"/healthz"});
}

TEST(HttpServer, OnlyGetIsServed) {
  Recorder recorder;
  mx::HttpServer server(loopback(), recorder.handler(200, "ok\n"));
  for (const char* method : {"POST", "HEAD", "PUT", "get"}) {
    EXPECT_EQ(ask(server.port(), std::string(method) + " /healthz HTTP/1.1\r\nContent-Length: 0\r\n\r\n"),
              response("405 Method Not Allowed", "GET is the one method served\n", "Allow: GET\r\n"))
        << method;
  }
  EXPECT_TRUE(recorder.paths().empty());
}

TEST(HttpServer, AMalformedRequestLineIsAnswered400) {
  Recorder recorder;
  mx::HttpServer server(loopback(), recorder.handler(200, "ok\n"));
  for (const char* line : {"GET", "GET /healthz", "GET healthz HTTP/1.1", "GET /a b HTTP/1.1", " /healthz HTTP/1.1",
                           "GET  HTTP/1.1", "GET /healthz FTP/1.0", "GET /healthz HTTP/1.1 ", "\r"}) {
    EXPECT_EQ(ask(server.port(), std::string(line) + "\r\n\r\n"),
              response("400 Bad Request", "a request line is METHOD /path HTTP/version\n"))
        << line;
  }
  EXPECT_TRUE(recorder.paths().empty());
}

// The head at the limit, its blank line its last bytes, is answered; at
// the limit without one, it is refused. Exactly at the limit, so that the
// server has read all that was sent and its close sends no reset ahead of
// the answer.
TEST(HttpServer, AHeadOverTheLimitIsAnswered431) {
  Recorder recorder;
  mx::HttpServer server(loopback(), recorder.handler(200, "ok\n"));
  const std::string line = "GET /healthz HTTP/1.1\r\nX-Padding: ";
  const std::size_t padding = mx::HttpServer::MAX_REQUEST_BYTES - line.size() - 4;
  EXPECT_EQ(ask(server.port(), line + std::string(padding, 'a') + "\r\n\r\n"), response("200 OK", "ok\n"));
  EXPECT_EQ(ask(server.port(), line + std::string(padding + 4, 'a')),
            response("431 Request Header Fields Too Large", "a request head is 8192 bytes at most\n"));
  EXPECT_EQ(recorder.paths(), std::vector<std::string>{"/healthz"});
}

TEST(HttpServer, AHandlerThatFailsIsAnswered500AndTheServerServesOn) {
  mx::HttpServer server(loopback(), [](const std::string& path, std::string& body) {
    if (path == "/throws") {
      throw std::runtime_error("the handler's own error");
    }
    if (path == "/42") {
      return 42;
    }
    if (path == "/600") {
      return 600;
    }
    body = "ok\n";
    return 200;
  });
  for (const char* path : {"/throws", "/42", "/600"}) {
    EXPECT_EQ(ask(server.port(), std::string("GET ") + path + " HTTP/1.1\r\n\r\n"),
              response("500 Internal Server Error", "the handler failed\n"))
        << path;
  }
  EXPECT_EQ(ask(server.port(), "GET / HTTP/1.1\r\n\r\n"), response("200 OK", "ok\n"));
}

// A connection that says nothing, or not all of its head, is closed at its
// deadline, the timeout from its accept.
TEST(HttpServer, AConnectionIsClosedAtItsDeadline) {
  Recorder recorder;
  mx::HttpServer server(loopback(), recorder.handler(200, "ok\n"), 0.05);
  Probe silent(loopback(server.port()));
  Probe halfway(loopback(server.port()));
  halfway.send("GET /healthz HTTP/1.1\r\n");
  EXPECT_EQ(silent.receive_all(), "");
  EXPECT_EQ(halfway.receive_all(), "");
  EXPECT_TRUE(recorder.paths().empty());
}

// MAX_CONNECTIONS connections that send nothing, with no deadline, are
// served; the next one is closed as soon as it is accepted, the server
// accepting in the order the connections were made.
TEST(HttpServer, AConnectionBeyondTheCapIsClosedAtOnce) {
  Recorder recorder;
  mx::HttpServer server(loopback(), recorder.handler(200, "ok\n"), -1);
  std::vector<std::unique_ptr<Probe>> served;
  for (std::size_t connection = 0; connection < mx::HttpServer::MAX_CONNECTIONS; ++connection) {
    served.emplace_back(new Probe(loopback(server.port())));
  }
  Probe beyond(loopback(server.port()));
  EXPECT_EQ(beyond.receive_all(), "");
  served.front()->send("GET /healthz HTTP/1.1\r\n\r\n");
  EXPECT_EQ(served.front()->receive_all(), response("200 OK", "ok\n"));
}

// stop() closes the connections under way and the listening socket; a
// second call does nothing.
TEST(HttpServer, StopEndsServing) {
  Recorder recorder;
  mx::HttpServer server(loopback(), recorder.handler(200, "ok\n"), -1);
  const std::uint16_t port = server.port();
  EXPECT_EQ(ask(port, "GET / HTTP/1.1\r\n\r\n"), response("200 OK", "ok\n"));
  Probe halfway(loopback(port));
  halfway.send("GET /healthz HTTP/1.1\r\n");
  server.stop();
  EXPECT_EQ(halfway.receive_all(), "");
  server.stop();
  EXPECT_THROW(Probe refused(loopback(port)), std::system_error);
  EXPECT_EQ(recorder.paths(), std::vector<std::string>{"/"});
}

TEST(HttpServer, AnAddressInUseThrows) {
  Recorder recorder;
  mx::HttpServer server(loopback(), recorder.handler(200, "ok\n"));
  EXPECT_THROW(mx::HttpServer(loopback(server.port()), recorder.handler(200, "ok\n")), std::system_error);
}

TEST(HttpServer, ServesOnIpv6) {
  Recorder recorder;
  const asio::ip::tcp::endpoint any(asio::ip::make_address("::1"), 0);
  std::unique_ptr<mx::HttpServer> server;
  try {
    server.reset(new mx::HttpServer(any, recorder.handler(200, "ok\n")));
  } catch (const std::system_error& error) {
    GTEST_SKIP() << "no IPv6 loopback here: " << error.what();
  }
  Probe probe(asio::ip::tcp::endpoint(asio::ip::make_address("::1"), server->port()));
  probe.send("GET /healthz HTTP/1.1\r\n\r\n");
  EXPECT_EQ(probe.receive_all(), response("200 OK", "ok\n"));
}

// What the process writes to stderr, the log's text, through a pipe this
// test reads, from construction to destruction, which writes it all to
// the stderr of before.
class StderrPipe {
 public:
  StderrPipe() {
    int ends[2];
    if (::pipe(ends) != 0) {
      throw std::system_error(errno, std::generic_category(), "pipe");
    }
    read_end_ = ends[0];
    saved_ = ::dup(STDERR_FILENO);
    ::dup2(ends[1], STDERR_FILENO);
    ::close(ends[1]);
  }
  ~StderrPipe() {
    ::dup2(saved_, STDERR_FILENO);
    ::close(saved_);
    ::close(read_end_);
    ssize_t ignored = ::write(STDERR_FILENO, text_.data(), text_.size());
    (void)ignored;
  }
  StderrPipe(const StderrPipe&) = delete;
  StderrPipe& operator=(const StderrPipe&) = delete;

  // The line holding `text`, read until one does; empty at the backstop.
  std::string wait_for(const std::string& text) {
    for (;;) {
      const std::size_t found = text_.find(text, scanned_);
      const std::size_t line_end = found == std::string::npos ? found : text_.find('\n', found);
      if (line_end != std::string::npos) {
        const std::size_t line_start = text_.rfind('\n', found);
        scanned_ = line_end + 1;
        return text_.substr(line_start == std::string::npos ? 0 : line_start + 1,
                            line_end - (line_start == std::string::npos ? 0 : line_start + 1));
      }
      pollfd readable{read_end_, POLLIN, 0};
      if (::poll(&readable, 1, BACKSTOP_SECONDS * 1000) != 1) {
        ADD_FAILURE() << "no line with '" << text << "' in the log within " << BACKSTOP_SECONDS << " s";
        return "";
      }
      char chunk[4096];
      const ssize_t bytes = ::read(read_end_, chunk, sizeof(chunk));
      if (bytes <= 0) {
        ADD_FAILURE() << "the log's pipe ended";
        return "";
      }
      text_.append(chunk, static_cast<std::size_t>(bytes));
    }
  }

 private:
  int read_end_ = -1;
  int saved_ = -1;
  std::string text_;
  std::size_t scanned_ = 0;
};

// No descriptor for the process from construction to destruction: the soft
// limit on descriptors set to the lowest one free, which the next to be
// made would take.
class NoDescriptors {
 public:
  NoDescriptors() {
    ::getrlimit(RLIMIT_NOFILE, &saved_);
    const int lowest_free = ::dup(STDERR_FILENO);
    if (lowest_free < 0) {
      throw std::system_error(errno, std::generic_category(), "dup");
    }
    ::close(lowest_free);
    rlimit lowered = saved_;
    lowered.rlim_cur = static_cast<rlim_t>(lowest_free);
    if (::setrlimit(RLIMIT_NOFILE, &lowered) != 0) {
      throw std::system_error(errno, std::generic_category(), "setrlimit");
    }
  }
  ~NoDescriptors() { ::setrlimit(RLIMIT_NOFILE, &saved_); }
  NoDescriptors(const NoDescriptors&) = delete;
  NoDescriptors& operator=(const NoDescriptors&) = delete;

 private:
  rlimit saved_;
};

// An accept that fails for want of a descriptor is tried again
// ACCEPT_RETRY_SECONDS later, the connection waiting in the listening
// socket's queue meanwhile, and served once there are descriptors again.
// Trying again at once, the listening socket still readable, failed over
// and over for as long as there were none; the failures the log counts
// show it.
TEST(HttpServer, AnAcceptThatFailsIsTriedAgainLater) {
  Recorder recorder;
  mx::HttpServer server(loopback(), recorder.handler(200, "ok\n"), -1);
  // A request answered first: by then the server's thread has made its
  // first accept, armed the next and closed that connection, and calls
  // accept() again only for a new one. An accept() holds the lowest free
  // descriptor for a moment while it looks at the queue, and one at the
  // moment NoDescriptors measured took it out of the measure: the limit
  // came out one too high, the probe was accepted, and only the accept
  // after it failed.
  ASSERT_EQ(ask(server.port(), "GET / HTTP/1.1\r\n\r\n"), response("200 OK", "ok\n"));
  StderrPipe log;
  Probe probe(loopback(server.port()), /*connect_now=*/false);
  const std::chrono::steady_clock::time_point start = std::chrono::steady_clock::now();
  {
    NoDescriptors none;
    probe.connect();
    log.wait_for("accept on port " + std::to_string(server.port()) + " failed");
  }
  probe.send("GET /healthz HTTP/1.1\r\n\r\n");
  EXPECT_EQ(probe.receive_all(), response("200 OK", "ok\n"));
  const std::string again = log.wait_for("works again, after ");
  const std::chrono::duration<double> elapsed = std::chrono::steady_clock::now() - start;
  const std::size_t after = again.find("after ");
  ASSERT_NE(after, std::string::npos) << again;
  const unsigned long failures = std::strtoul(again.c_str() + after + 6, nullptr, 10);
  EXPECT_GE(failures, 1u) << again;
  EXPECT_LE(failures, elapsed.count() / mx::HttpServer::ACCEPT_RETRY_SECONDS + 1) << again;
}

}  // namespace
