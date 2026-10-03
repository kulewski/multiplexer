// A welcome claiming the instance id of a live connection is refused when
// the newcomer's own socket is gone, one that sent its welcome and was
// reset before the multiplexer read it, from another host or the same:
// the live connection keeps the id, where the multiplexer took a newcomer
// it could not get the address of for one from the same host, dropped the
// live connection and left the id to a dead one. A live newcomer from the
// same host still replaces the connection, as a peer that came back does.
// Counted, not timed: the multiplexer is frozen while the newcomer sends
// and resets, the test waits for the multiplexer to end the newcomer's
// connection, then a message addressed to the id says who has it.
#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <string>
#include <thread>

#include "multiplexer/client.h"
#include "multiplexer/connections_manager.h"
#include "multiplexer/in_process_multiplexer.h"
#include "multiplexer/multiplexer.constants.h"

using multiplexer::MultiplexerMessage;
using multiplexer::RawMessage;
using multiplexer::testing::InProcessMultiplexer;

namespace {

const std::uint64_t CLAIMED_ID = 0x5eed1d0000c1a1edULL;  // the owner's, which the newcomer claims too
const int WAIT_MS = 30000;                               // a failure detector only

// Holds a multiplexer's io thread, so that it reads nothing while the
// sockets stay open: a multiplexer frozen, until the end of the scope.
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
  ~Freeze() { released.set_value(); }
  std::promise<void> released;
};

// How many connections the multiplexer has accepted and not ended, asked
// on its io thread.
std::size_t accepted(InProcessMultiplexer& mx) {
  std::promise<std::size_t> count;
  mx.io_service.post([&mx, &count] { count.set_value(mx.server->accepted_count()); });
  return count.get_future().get();
}

// Whether `holds` came to say yes of the multiplexer's accepted count
// within WAIT_MS.
bool wait_for_accepted(InProcessMultiplexer& mx, const std::function<bool(std::size_t)>& holds) {
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(WAIT_MS);
  while (!holds(accepted(mx))) {
    if (std::chrono::steady_clock::now() > deadline) {
      return false;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  return true;
}

// A TCP connection to the multiplexer from `source`, an address of this
// host: the descriptor, or -1.
int connect_from(const char* source, unsigned short port) {
  const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  sockaddr_in local{};
  local.sin_family = AF_INET;
  ::inet_pton(AF_INET, source, &local.sin_addr);
  sockaddr_in remote{};
  remote.sin_family = AF_INET;
  remote.sin_port = htons(port);
  ::inet_pton(AF_INET, "127.0.0.1", &remote.sin_addr);
  if (fd < 0 || ::bind(fd, reinterpret_cast<sockaddr*>(&local), sizeof local) ||
      ::connect(fd, reinterpret_cast<sockaddr*>(&remote), sizeof remote)) {
    if (fd >= 0) {
      ::close(fd);
    }
    return -1;
  }
  return fd;
}

// Closes `fd` with a reset, its unread and unsent data thrown away.
void reset(int fd) {
  const linger at_once{1, 0};
  ::setsockopt(fd, SOL_SOCKET, SO_LINGER, &at_once, sizeof at_once);
  ::close(fd);
}

// Reads exactly `size` bytes into `at` within WAIT_MS: 0, -1 at the end of
// the stream, ETIMEDOUT, or the errno.
int read_exactly(int fd, void* at, std::size_t size) {
  char* into = static_cast<char*>(at);
  while (size) {
    pollfd readable{fd, POLLIN, 0};
    const int ready = ::poll(&readable, 1, WAIT_MS);
    if (ready == 0) {
      return ETIMEDOUT;
    }
    const ssize_t got = ready < 0 ? -1 : ::recv(fd, into, size, 0);
    if (got == 0) {
      return -1;
    }
    if (got < 0) {
      if (errno == EINTR) {
        continue;
      }
      return errno;
    }
    into += got;
    size -= static_cast<std::size_t>(got);
  }
  return 0;
}

// The next frame on `fd` that is not a heartbeat, parsed: 0, -1 at the end
// of the stream, ETIMEDOUT, or the errno.
int read_message(int fd, MultiplexerMessage* msg) {
  for (;;) {
    RawMessage frame;
    if (int error = read_exactly(fd, frame.get_header_buffer().data(), frame.get_header_buffer().size())) {
      return error;
    }
    if (!frame.unpack_header()) {
      return EPROTO;
    }
    if (int error = read_exactly(fd, frame.get_body_buffer().data(), frame.get_body_buffer().size())) {
      return error;
    }
    if (!frame.verify() || !msg->ParseFromString(frame.get_message())) {
      return EPROTO;
    }
    if (msg->type() != multiplexer::types::HEARTBIT) {
      return 0;
    }
  }
}

// Writes the welcome of a peer with `id`: 0 or the errno.
int send_welcome(int fd, std::uint64_t id) {
  std::shared_ptr<const RawMessage> welcome =
      multiplexer::impl::create_welcome_message(multiplexer::peers::PYTHON_TEST_SERVER, id);
  for (const asio::const_buffer& part : welcome->get_message_buffer()) {
    if (::send(fd, part.data(), part.size(), MSG_NOSIGNAL) != static_cast<ssize_t>(part.size())) {
      return errno;
    }
  }
  return 0;
}

// A peer on its own socket from `source`, welcomed as CLAIMED_ID: the
// descriptor, once the multiplexer's welcome came back.
int welcomed_peer(InProcessMultiplexer& mx, const char* source) {
  const int fd = connect_from(source, mx.port);
  MultiplexerMessage welcome;
  if (fd < 0 || send_welcome(fd, CLAIMED_ID) || read_message(fd, &welcome) ||
      welcome.type() != multiplexer::types::CONNECTION_WELCOME) {
    return -1;
  }
  return fd;
}

// Sends a message addressed to CLAIMED_ID through a client of its own and
// returns its payload, which whoever has the id receives.
std::string send_marker(InProcessMultiplexer& mx) {
  multiplexer::Client sender(multiplexer::peers::WEBSITE);
  sender.wait_for_connection(sender.connect("127.0.0.1", mx.port, 5), 5);
  MultiplexerMessage marker;
  marker.set_id(sender.random64());
  marker.set_from(sender.instance_id());
  marker.set_to(CLAIMED_ID);
  marker.set_type(multiplexer::types::PYTHON_TEST_REQUEST);
  marker.set_message("to whoever has the id");
  sender.send(marker, 5);
  sender.shutdown();
  return marker.message();
}

}  // namespace

// The newcomer, from another host or the same one, sends its welcome and
// is reset while the multiplexer is frozen: refused, the owner keeps the
// id and gets what is addressed to it.
TEST(LiveId, ANewcomerResetBeforeItsWelcomeIsReadIsRefused) {
  for (const char* source : {"127.0.0.2", "127.0.0.1"}) {
    SCOPED_TRACE(source);
    InProcessMultiplexer mx;
    const int owner = welcomed_peer(mx, "127.0.0.1");
    ASSERT_GE(owner, 0);
    const int newcomer = connect_from(source, mx.port);
    ASSERT_GE(newcomer, 0);
    ASSERT_TRUE(wait_for_accepted(mx, [](std::size_t count) { return count == 2; })) << "the newcomer accepted";
    {
      Freeze frozen(mx);
      ASSERT_EQ(0, send_welcome(newcomer, CLAIMED_ID));
      reset(newcomer);
    }
    ASSERT_TRUE(wait_for_accepted(mx, [](std::size_t count) { return count <= 1; })) << "the newcomer ended";
    const std::string sent = send_marker(mx);
    MultiplexerMessage got;
    ASSERT_EQ(0, read_message(owner, &got)) << "the owner's connection ended";
    EXPECT_EQ(sent, got.message());
    ::close(owner);
  }
}

// A live newcomer from the same host replaces the owner, as a peer that
// lost its connection and came back does: the owner's connection ends and
// the newcomer gets what is addressed to the id.
TEST(LiveId, ALiveNewcomerFromTheSameHostReplacesTheOwner) {
  InProcessMultiplexer mx;
  const int owner = welcomed_peer(mx, "127.0.0.1");
  ASSERT_GE(owner, 0);
  const int newcomer = welcomed_peer(mx, "127.0.0.1");
  ASSERT_GE(newcomer, 0);
  MultiplexerMessage got;
  EXPECT_EQ(-1, read_message(owner, &got)) << "the owner's connection ends";
  const std::string sent = send_marker(mx);
  ASSERT_EQ(0, read_message(newcomer, &got));
  EXPECT_EQ(sent, got.message());
  ::close(owner);
  ::close(newcomer);
}
