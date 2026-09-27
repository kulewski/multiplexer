// A synchronous client's call returns while a multiplexer floods it: the
// loop a call runs without waiting, a receive's last look once its time is
// up and the look before every send, reads a queue's worth of frames at
// most. A frame read from a socket that holds more makes the next read
// ready at once, so a peer that writes faster than the client reads would
// keep a loop that runs until nothing is ready going for good, the call
// never returning and the incoming queue full and dropping. Counted, not
// timed: a multiplexer of the test's own fills the client's socket before
// the call, writing until the kernel takes no more, then keeps it full, the
// same frames written again as fast as the kernel takes them, and the bound
// on the call only detects a failure. Each failed on the code before: the
// send stayed in its look for good, and the receive whose time was up gave
// up without reading.
#include <fcntl.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <future>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>

#include "multiplexer/client.h"
#include "multiplexer/connections_manager.h"
#include "multiplexer/multiplexer.constants.h"

using multiplexer::Client;
using multiplexer::MultiplexerMessage;
using multiplexer::RawMessage;
namespace types = multiplexer::types;
namespace peers = multiplexer::peers;

namespace {

const int BOUND = 30;     // seconds a call may take before the test calls it stuck: a failure detector only
const int FRAMES = 4096;  // frames written again and again: more than the queue holds and the copies' window

// Reads exactly `size` bytes into `at`: false when the stream ended or failed first.
bool read_exactly(int fd, void* at, std::size_t size) {
  char* into = static_cast<char*>(at);
  while (size) {
    const ssize_t got = ::recv(fd, into, size, 0);
    if (got == 0 || (got < 0 && errno != EINTR)) {
      return false;
    }
    if (got > 0) {
      into += got;
      size -= static_cast<std::size_t>(got);
    }
  }
  return true;
}

// The bytes of `frame` as it goes on the wire, header and body.
std::string wire_bytes(const RawMessage& frame) {
  std::string bytes;
  for (const asio::const_buffer& part : frame.get_message_buffer()) {
    bytes.append(static_cast<const char*>(part.data()), part.size());
  }
  return bytes;
}

// A multiplexer of the test's own on 127.0.0.1, for one client: a thread
// accepts it and exchanges welcomes; flood_until_full() then has it write
// the client messages until the kernel takes no more, which it reports,
// and go on writing them as fast as the kernel takes them, until stop() or
// the end of the stand-in.
class Flooding {
 public:
  Flooding() {
    listener_ = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in address = {};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    socklen_t length = sizeof address;
    if (::bind(listener_, reinterpret_cast<sockaddr*>(&address), length) != 0 || ::listen(listener_, 1) != 0 ||
        ::getsockname(listener_, reinterpret_cast<sockaddr*>(&address), &length) != 0) {
      ::close(listener_);
      throw std::runtime_error("no socket to listen on");
    }
    port = ntohs(address.sin_port);
    writer_ = std::thread([this] { _serve(); });
  }
  ~Flooding() {
    stop();
    ::shutdown(listener_, SHUT_RDWR);  // an accept still waiting returns
    writer_.join();
    if (fd_ >= 0) {
      ::close(fd_);
    }
    ::close(listener_);
  }

  // Starts the flood and waits, BOUND seconds at most, until the client's
  // socket takes no more: whether it came to that.
  bool flood_until_full() {
    std::unique_lock<std::mutex> lock(mutex_);
    flooding_ = true;
    changed_.notify_all();
    changed_.wait_for(lock, std::chrono::seconds(BOUND), [this] { return full_ || stopped_; });
    return full_;
  }

  // Ends the flood and the client's connection.
  void stop() {
    std::lock_guard<std::mutex> lock(mutex_);
    stopped_ = true;
    if (fd_ >= 0) {
      ::shutdown(fd_, SHUT_RDWR);  // a write under way fails, and the client sees its end
    }
    changed_.notify_all();
  }

  unsigned short port = 0;

 private:
  // The writer thread: the client's connection, the welcomes, then, once
  // asked, the flood.
  void _serve() {
    const int fd = ::accept(listener_, nullptr, nullptr);
    if (fd < 0) {
      return;
    }
    {
      std::lock_guard<std::mutex> lock(mutex_);
      fd_ = fd;
    }
    RawMessage theirs;
    if (!read_exactly(fd, theirs.get_header_buffer().data(), theirs.get_header_buffer().size()) ||
        !theirs.unpack_header() ||
        !read_exactly(fd, theirs.get_body_buffer().data(), theirs.get_body_buffer().size()) ||
        !_write(fd, wire_bytes(*multiplexer::impl::create_welcome_message(peers::MULTIPLEXER, 0x5100 + port)))) {
      return;
    }
    std::string batch;
    for (int index = 0; index < FRAMES; ++index) {
      MultiplexerMessage msg;
      msg.set_id((std::uint64_t(port) << 32) + 1 + index);  // ids of its own: a repeated id is dropped as a copy
      msg.set_type(types::PYTHON_TEST_REQUEST);
      msg.set_message("x");
      batch += wire_bytes(*std::unique_ptr<RawMessage>(RawMessage::FromMessage(msg)));
    }
    {
      std::unique_lock<std::mutex> lock(mutex_);
      changed_.wait(lock, [this] { return flooding_ || stopped_; });
      if (stopped_) {
        return;
      }
    }
    // Until the kernel takes no more, without blocking; then as fast as it
    // takes them, blocking, until the connection ends.
    ::fcntl(fd, F_SETFL, ::fcntl(fd, F_GETFL) | O_NONBLOCK);
    std::size_t at = 0;
    for (;;) {
      const ssize_t sent = ::send(fd, batch.data() + at, batch.size() - at, MSG_NOSIGNAL);
      if (sent < 0 && errno == EINTR) {
        continue;
      }
      if (sent < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
        break;
      }
      if (sent < 0) {
        return;
      }
      at = (at + static_cast<std::size_t>(sent)) % batch.size();
    }
    ::fcntl(fd, F_SETFL, ::fcntl(fd, F_GETFL) & ~O_NONBLOCK);
    {
      std::lock_guard<std::mutex> lock(mutex_);
      full_ = true;
      changed_.notify_all();
    }
    for (;;) {
      const ssize_t sent = ::send(fd, batch.data() + at, batch.size() - at, MSG_NOSIGNAL);
      if (sent < 0 && errno == EINTR) {
        continue;
      }
      if (sent <= 0) {
        return;  // stop(), or the client's end
      }
      at = (at + static_cast<std::size_t>(sent)) % batch.size();
    }
  }

  // Writes `bytes` whole, blocking: false when the connection failed.
  static bool _write(int fd, const std::string& bytes) {
    std::size_t at = 0;
    while (at < bytes.size()) {
      const ssize_t sent = ::send(fd, bytes.data() + at, bytes.size() - at, MSG_NOSIGNAL);
      if (sent < 0 && errno == EINTR) {
        continue;
      }
      if (sent <= 0) {
        return false;
      }
      at += static_cast<std::size_t>(sent);
    }
    return true;
  }

  int listener_ = -1;
  int fd_ = -1;
  std::thread writer_;
  std::mutex mutex_;
  std::condition_variable changed_;
  bool flooding_ = false;
  bool full_ = false;
  bool stopped_ = false;
};

// Runs `call` on a thread of its own with a client of its own, connected
// to `mx` and flooded, a client being used on the thread that made it, and
// waits BOUND seconds for it: what it returned, or a failure.
template <typename Call>
::testing::AssertionResult returns_while_flooded(Flooding& mx, Call call) {
  std::future<bool> done = std::async(std::launch::async, [&mx, call] {
    Client client(peers::WEBSITE);
    if (!client.connect("127.0.0.1", mx.port, 5)) {
      throw std::runtime_error("the client did not connect");
    }
    if (!mx.flood_until_full()) {
      throw std::runtime_error("the client's socket never filled");
    }
    return call(client);
  });
  if (done.wait_for(std::chrono::seconds(BOUND)) != std::future_status::ready) {
    mx.stop();  // the call returns once the flood ends, and the test with it
    return ::testing::AssertionFailure() << "still in the call " << BOUND << " s into the flood";
  }
  try {
    return done.get() ? ::testing::AssertionSuccess() : ::testing::AssertionFailure() << "the call returned nothing";
  } catch (const std::exception& error) {
    return ::testing::AssertionFailure() << "the call threw: " << error.what();
  }
}

}  // namespace

TEST(FloodedClient, AReceiveWhoseTimeIsUpReturnsAMessage) {
  Flooding mx;
  EXPECT_TRUE(returns_while_flooded(mx, [](Client& client) { return bool(client.receive_message(0).first); }));
}

TEST(FloodedClient, ASendReturns) {
  Flooding mx;
  EXPECT_TRUE(returns_while_flooded(mx, [](Client& client) {
    MultiplexerMessage msg;
    msg.set_id(client.random64());
    msg.set_type(types::PYTHON_TEST_REQUEST);
    msg.set_message("a send while flooded");
    return bool(client.schedule_one(msg, 5));
  }));
}
