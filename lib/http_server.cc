// mx::HttpServer: the accepts, each connection's request head read and its
// reply written under one deadline, and the replies; see http_server.h.
#include "lib/http_server.h"

#include <fcntl.h>

#include <array>
#include <asio/post.hpp>
#include <asio/write.hpp>
#include <exception>
#include <utility>
#include <vector>

#include "lib/assertion.h"
#include "lib/logging/logging.h"
#include "lib/repr.h"
#include "lib/seconds.h"

namespace mx {

using namespace ::mx::logging::consts;

namespace {

// Closed on exec, as the multiplexer's sockets are (multiplexer/server.cc):
// a program the process runs holds neither the port nor a connection.
void close_on_exec(int descriptor) {
  const int flags = ::fcntl(descriptor, F_GETFD);
  if (flags != -1) {
    ::fcntl(descriptor, F_SETFD, flags | FD_CLOEXEC);
  }
}

// The reason phrase of `status`, empty for one not listed here, which the
// status line allows.
const char* reason(int status) {
  switch (status) {
    case 200:
      return "OK";
    case 204:
      return "No Content";
    case 400:
      return "Bad Request";
    case 403:
      return "Forbidden";
    case 404:
      return "Not Found";
    case 405:
      return "Method Not Allowed";
    case 431:
      return "Request Header Fields Too Large";
    case 500:
      return "Internal Server Error";
    case 503:
      return "Service Unavailable";
    default:
      return "";
  }
}

// A whole response: the status line, the headers, `headers` among them,
// each ending in CRLF, and the body.
std::string response(int status, const std::string& body, const std::string& headers = "") {
  std::string text = "HTTP/1.0 " + std::to_string(status) + " " + reason(status) + "\r\n";
  text += "Content-Type: text/plain\r\nContent-Length: " + std::to_string(body.size()) + "\r\nConnection: close\r\n";
  text += headers;
  text += "\r\n";
  text += body;
  return text;
}

// Whether `head` holds the blank line that ends a request head: CRLF CRLF,
// as HTTP has it, or LF LF, which it lets a server take, what a request
// typed into nc sends.
bool head_ended(const char* head, std::size_t size) {
  for (std::size_t at = 0; at < size; ++at) {
    if (head[at] != '\n') {
      continue;
    }
    std::size_t next = at + 1;
    if (next < size && head[next] == '\r') {
      ++next;
    }
    if (next < size && head[next] == '\n') {
      return true;
    }
  }
  return false;
}

}  // namespace

// One connection, from its accept to its close, on the server's thread: the
// request head read, the reply written, both before the deadline armed at
// the accept. Held by the server's live_ until it closes, and by each of
// its handlers pending meanwhile.
class HttpServer::Connection : public std::enable_shared_from_this<Connection> {
 public:
  explicit Connection(HttpServer& server)
      : server_(server), socket_(server.io_service_), deadline_(server.io_service_) {}
  Connection(const Connection&) = delete;
  Connection& operator=(const Connection&) = delete;

  asio::ip::tcp::socket& socket() { return socket_; }
  // Serving starts, once accepted: the deadline armed, the head read. On
  // the server's thread, as close() is.
  void start();
  // Serving ends, unless it has: the socket closed, which ends the read or
  // the write under way, the deadline cancelled, and the connection out of
  // the server's live ones.
  void close();

 private:
  // Reads on into the rest of head_.
  void _read() MX_RUN_ON(&server_.checker_);
  // A read's end: the reply written once the head is whole, 431 once
  // head_ is full without it, the connection closed when the client went.
  void _on_read(const asio::error_code& error, std::size_t bytes) MX_RUN_ON(&server_.checker_);
  // Writes reply_, then closes.
  void _write() MX_RUN_ON(&server_.checker_);

  HttpServer& server_;
  asio::ip::tcp::socket socket_;
  asio::steady_timer deadline_;
  std::array<char, MAX_REQUEST_BYTES> head_;
  std::size_t size_ = 0;  // bytes of head_ read
  std::string reply_;     // being written
  bool closed_ = false;
};

void HttpServer::Connection::start() {
  MX_DCHECK_RUN_ON(&server_.checker_);
  close_on_exec(socket_.native_handle());
  deadline_.expires_after(server_.timeout_);
  deadline_.async_wait([self = shared_from_this()](const asio::error_code& error) {
    MX_DCHECK_RUN_ON(&self->server_.checker_);
    if (error != asio::error::operation_aborted) {
      self->close();
    }
  });
  _read();
}

void HttpServer::Connection::close() {
  MX_DCHECK_RUN_ON(&server_.checker_);
  if (closed_) {
    return;
  }
  closed_ = true;
  asio::error_code ignored;
  socket_.close(ignored);
  deadline_.cancel(ignored);
  server_.live_.erase(shared_from_this());
}

void HttpServer::Connection::_read() {
  socket_.async_read_some(asio::buffer(head_.data() + size_, head_.size() - size_),
                          [self = shared_from_this()](const asio::error_code& error, std::size_t bytes) {
                            MX_DCHECK_RUN_ON(&self->server_.checker_);
                            self->_on_read(error, bytes);
                          });
}

void HttpServer::Connection::_on_read(const asio::error_code& error, std::size_t bytes) {
  if (closed_) {
    return;
  }
  if (error) {
    close();  // the client went before its head was whole
    return;
  }
  size_ += bytes;
  if (head_ended(head_.data(), size_)) {
    reply_ = server_._reply(std::string(head_.data(), size_));
  } else if (size_ == head_.size()) {
    reply_ = response(431, "a request head is " + repr(MAX_REQUEST_BYTES) + " bytes at most\n");
  } else {
    _read();
    return;
  }
  _write();
}

void HttpServer::Connection::_write() {
  asio::async_write(socket_, asio::buffer(reply_), [self = shared_from_this()](const asio::error_code&, std::size_t) {
    MX_DCHECK_RUN_ON(&self->server_.checker_);
    self->close();
  });
}

// Bound and listening here, on the caller's thread, so that an address in
// use throws to the caller rather than on the thread; the first accept is
// the thread's.
HttpServer::HttpServer(const asio::ip::tcp::endpoint& endpoint, HttpHandler handler, float timeout)
    : handler_(std::move(handler)),
      timeout_(from_seconds(timeout)),
      acceptor_(io_service_, endpoint),
      retry_(io_service_) {
  close_on_exec(acceptor_.native_handle());
  port_ = acceptor_.local_endpoint().port();
  asio::post(io_service_, [this] {
    MX_DCHECK_RUN_ON(&checker_);
    _accept();
  });
  thread_ = std::thread(&HttpServer::_run, this);
}

HttpServer::~HttpServer() { stop(); }

void HttpServer::stop() {
  if (!thread_.joinable()) {
    return;
  }
  AssertMsg(thread_.get_id() != std::this_thread::get_id(),
            "HttpServer::stop() on the server's own thread, a handler's, which cannot join itself");
  asio::post(io_service_, [this] {
    MX_DCHECK_RUN_ON(&checker_);
    _close_all();
  });
  thread_.join();
}

void HttpServer::_accept() {
  std::shared_ptr<Connection> connection = std::make_shared<Connection>(*this);
  acceptor_.async_accept(connection->socket(), [this, connection](const asio::error_code& error) {
    MX_DCHECK_RUN_ON(&checker_);
    _accepted(connection, error);
  });
}

void HttpServer::_accepted(const std::shared_ptr<Connection>& connection, const asio::error_code& error) {
  if (closing_) {
    return;  // the socket, accepted or not, closes with `connection`
  }
  if (error) {
    if (++accept_failures_ == 1) {
      MX_LOG(WARNING, LOWVERBOSITY,
             CTX("http_server") TEXT("accept on port " + repr(port_) + " failed: " + error.message() +
                                     "; trying again every " + repr(ACCEPT_RETRY_SECONDS) + " s"));
    }
    retry_.expires_after(from_seconds(ACCEPT_RETRY_SECONDS));
    retry_.async_wait([this](const asio::error_code& wait_error) {
      MX_DCHECK_RUN_ON(&checker_);
      if (!wait_error && !closing_) {
        _accept();
      }
    });
    return;
  }
  if (accept_failures_) {
    MX_LOG(INFO, LOWVERBOSITY,
           CTX("http_server")
               TEXT("accept on port " + repr(port_) + " works again, after " + repr(accept_failures_) + " failure(s)"));
    accept_failures_ = 0;
  }
  if (live_.size() < MAX_CONNECTIONS) {
    live_.insert(connection);
    connection->start();
  } else {
    asio::error_code ignored;
    connection->socket().close(ignored);
  }
  _accept();
}

void HttpServer::_close_all() {
  closing_ = true;
  asio::error_code ignored;
  acceptor_.close(ignored);
  retry_.cancel(ignored);
  // A copy: each close() takes its connection out of live_.
  const std::vector<std::shared_ptr<Connection>> live(live_.begin(), live_.end());
  for (const std::shared_ptr<Connection>& connection : live) {
    connection->close();
  }
}

// The request line is METHOD SP request-target SP HTTP-version; the
// headers after it say nothing a GET here needs.
std::string HttpServer::_reply(const std::string& head) {
  std::string line = head.substr(0, head.find('\n'));
  if (!line.empty() && line.back() == '\r') {
    line.pop_back();
  }
  const std::size_t method_end = line.find(' ');
  const std::size_t target_end = method_end == std::string::npos ? std::string::npos : line.find(' ', method_end + 1);
  if (target_end == std::string::npos || method_end == 0 || target_end == method_end + 1 ||
      line[method_end + 1] != '/' || line.compare(target_end + 1, 5, "HTTP/") != 0 ||
      line.find(' ', target_end + 1) != std::string::npos) {
    return response(400, "a request line is METHOD /path HTTP/version\n");
  }
  if (line.compare(0, method_end, "GET") != 0) {
    return response(405, "GET is the one method served\n", "Allow: GET\r\n");
  }
  const std::string target = line.substr(method_end + 1, target_end - method_end - 1);
  std::string body;
  int status = 500;
  try {
    status = handler_(target, body);
  } catch (const std::exception& error) {
    MX_LOG(ERROR, LOWVERBOSITY, CTX("http_server") TEXT("the handler threw for " + target + ": " + error.what()));
    return response(500, "the handler failed\n");
  } catch (...) {
    MX_LOG(ERROR, LOWVERBOSITY, CTX("http_server") TEXT("the handler threw for " + target));
    return response(500, "the handler failed\n");
  }
  if (status < 100 || status > 599) {
    MX_LOG(ERROR, LOWVERBOSITY,
           CTX("http_server")
               TEXT("the handler answered " + target + " with status " + repr(status) + ", not one from 100 to 599"));
    return response(500, "the handler failed\n");
  }
  return response(status, body);
}

// What escapes a handler, a std::bad_alloc say, is logged and the thread
// goes on, as the multiplexer's loops do (multiplexer/server_loops.cc).
void HttpServer::_run() {
  for (;;) {
    try {
      io_service_.run();
      return;
    } catch (const std::exception& error) {
      MX_LOG(ERROR, LOWVERBOSITY,
             CTX("http_server") TEXT(std::string("exception escaped an HTTP server's handler: ") + error.what()));
    }
  }
}

}  // namespace mx
