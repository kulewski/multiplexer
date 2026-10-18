// mx::HttpServer: a minimal HTTP/1.0 server for a process's health and
// metrics endpoints, on a thread of its own: GET only, one request per
// connection, the reply text/plain, or the type the handler gives, with
// its length, then the connection closed. What a kubelet probe or a
// Prometheus scrape needs and no more.
// Used by the multiplexer's monitor (multiplexer/monitor.h, run_multiplexer
// --monitor-address) and by a program's own health endpoint
// (docs/recipes/check_backend_health.md).
//
// A connection has `timeout` seconds from its accept to send its request
// head, the request line and the headers up to the blank line, at most
// MAX_REQUEST_BYTES, and to take the reply; one that does not is closed,
// so that a client that connects and says nothing holds up nobody. Beyond
// MAX_CONNECTIONS at once a new connection is closed at once. An accept
// that fails, at the descriptor limit say, is tried again
// ACCEPT_RETRY_SECONDS later, where trying again at once would spin the
// thread on a listening socket that stays readable.
//
// Everything happens on the server's thread, its own io_service: the
// accepts, the reads and writes and their deadlines, and the handler's
// calls, one at a time, so that a handler need not be thread-safe against
// itself. A handler may block, briefly: every other connection waits
// meanwhile, and stop() waits for a call under way.
#ifndef MX_LIB_HTTP_SERVER_H_
#define MX_LIB_HTTP_SERVER_H_

#include <asio/io_service.hpp>
#include <asio/ip/tcp.hpp>
#include <asio/steady_timer.hpp>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <set>
#include <string>
#include <thread>

#include "lib/thread_annotations.h"
#include "lib/thread_checker.h"

namespace mx {

// Answers a GET: returns the status, 200, 404 or 503 say, and fills `body`,
// sent as text/plain. `path` is the request target as sent, "/healthz" or
// "/metrics", a query string included. A handler that throws has the
// request answered 500.
typedef std::function<int(const std::string& path, std::string& body)> HttpHandler;
// The same, and `content_type`, text/plain when it comes, the handler may
// set to another: "text/plain; version=0.0.4; charset=utf-8" is what a
// Prometheus scrape of /metrics wants.
typedef std::function<int(const std::string& path, std::string& body, std::string& content_type)> HttpTypedHandler;

class HttpServer {
 public:
  // Listens on `endpoint`, port 0 for one the system picks, and serves
  // `handler` on a thread of its own until stop() or the destructor; a
  // connection has `timeout` seconds, negative for no deadline
  // (lib/seconds.h). Throws std::system_error when the address cannot be
  // bound.
  HttpServer(const asio::ip::tcp::endpoint& endpoint, HttpHandler handler, float timeout = DEFAULT_TIMEOUT);
  HttpServer(const asio::ip::tcp::endpoint& endpoint, HttpTypedHandler handler, float timeout = DEFAULT_TIMEOUT);
  // stop().
  ~HttpServer();
  HttpServer(const HttpServer&) = delete;
  HttpServer& operator=(const HttpServer&) = delete;

  // The port listened on, the one the system picked for port 0; from any
  // thread.
  std::uint16_t port() const { return port_; }
  // Serving ends: the listening socket and every connection closed, a
  // handler's call under way finished, the thread joined. From any thread
  // but the server's own, one at a time; a second call does nothing.
  void stop();

  // A connection's time from its accept to its reply written, in seconds.
  static constexpr float DEFAULT_TIMEOUT = 10;
  // The most a request head may be, request line and headers; a longer one
  // is answered 431.
  static constexpr std::size_t MAX_REQUEST_BYTES = 8192;
  // The most connections served at once: probes and scrapes open one or
  // two; what is beyond this is something else.
  static constexpr std::size_t MAX_CONNECTIONS = 16;
  // How long after a failed accept the next one is tried, in seconds.
  static constexpr float ACCEPT_RETRY_SECONDS = 0.1f;

 private:
  class Connection;
  // Waits for the next connection.
  void _accept() MX_RUN_ON(&checker_);
  // The accept's end: the connection served, or closed at once beyond
  // MAX_CONNECTIONS, or, after a failure, the next accept armed later.
  void _accepted(const std::shared_ptr<Connection>& connection, const asio::error_code& error) MX_RUN_ON(&checker_);
  // Everything closed, for stop(): the thread's run() then returns once
  // the aborted operations have ended.
  void _close_all() MX_RUN_ON(&checker_);
  // The reply to the request head `head`: the handler's for a GET.
  std::string _reply(const std::string& head) MX_RUN_ON(&checker_);
  // The thread's body: the io_service run until stop() left it no work.
  void _run();

  const HttpTypedHandler handler_;
  const std::chrono::microseconds timeout_;
  // The server's thread, which everything below belongs to; bound by its
  // first handler.
  mx::ThreadChecker checker_{mx::ThreadChecker::BIND_LATER};
  asio::io_service io_service_;
  asio::ip::tcp::acceptor acceptor_;
  asio::steady_timer retry_;
  std::uint16_t port_ = 0;  // set by the constructor, read from any thread
  // The connections being served, each until it closes; for the cap and
  // for _close_all().
  std::set<std::shared_ptr<Connection>> live_ MX_GUARDED_BY(checker_);
  // Failed accepts in a row: the first is logged, and how many once one
  // succeeds again.
  unsigned long accept_failures_ MX_GUARDED_BY(checker_) = 0;
  bool closing_ MX_GUARDED_BY(checker_) = false;
  std::thread thread_;  // last: started once the rest is in place
};

}  // namespace mx

#endif  // MX_LIB_HTTP_SERVER_H_
