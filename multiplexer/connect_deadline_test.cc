// A TCP connect to an address that drops the SYNs ends at
// CONNECT_ATTEMPT_SECONDS, and the next address the target resolved to is
// tried: it held the attempt for the kernel's own timeout, two minutes on
// Linux, the name's other addresses untried meanwhile. A listener whose
// accept queue is full drops the SYNs of a new connect, as a firewall that
// drops them does: one with a backlog of 0, its one place taken by a
// connection nobody accepts. The client's resolver gives that address
// first and a multiplexer's second, and SyncClient and ThreadedClient both
// connect by the name within a wait far shorter than the kernel's timeout.
// A deadline that expires in the loop pass that also reads the first
// address's refusal leaves the second address's connect alone.
#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <cstdint>
#include <string>
#include <thread>
#include <vector>

#include "lib/seconds.h"
#include "multiplexer/client.h"
#include "multiplexer/defaults.h"
#include "multiplexer/in_process_multiplexer.h"
#include "multiplexer/multiplexer.constants.h"
#include "multiplexer/threaded_client.h"

namespace {

using multiplexer::testing::InProcessMultiplexer;

// Seconds a connect may wait: a failure detector, under the kernel's two
// minutes and over CONNECT_ATTEMPT_SECONDS.
const float WAIT = 30;

// A listener on 127.0.0.1 that drops every SYN: listening with a backlog
// of 0, its accept queue holds one connection, which `filler` takes and
// nobody accepts.
struct DropsSyns {
  DropsSyns() {
    listener = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    sockaddr_in address = {};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    socklen_t length = sizeof address;
    if (::bind(listener, reinterpret_cast<sockaddr*>(&address), length) != 0 || ::listen(listener, 0) != 0 ||
        ::getsockname(listener, reinterpret_cast<sockaddr*>(&address), &length) != 0) {
      ADD_FAILURE() << "no listener";
    }
    port = ntohs(address.sin_port);
    filler = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (::connect(filler, reinterpret_cast<sockaddr*>(&address), sizeof address) != 0) {
      ADD_FAILURE() << "the queue's one place not taken";
    }
  }
  ~DropsSyns() {
    ::close(filler);
    ::close(listener);
  }
  int listener;
  int filler;
  unsigned short port;
};

// The resolver of a name with two addresses on 127.0.0.1: one that fails
// first, the multiplexer second.
multiplexer::BasicClient::Resolver failing_first(unsigned short failing, unsigned short live) {
  return [failing, live](const std::string&, std::uint16_t, asio::error_code&) {
    std::vector<asio::ip::tcp::endpoint> addresses;
    addresses.emplace_back(asio::ip::make_address("127.0.0.1"), failing);
    addresses.emplace_back(asio::ip::make_address("127.0.0.1"), live);
    return addresses;
  };
}

// A port on 127.0.0.1 nothing listens on: the kernel refuses a connect to
// it at once, which a client hears of at its next loop pass.
unsigned short refusing_port() {
  const int probe = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
  sockaddr_in address = {};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  socklen_t length = sizeof address;
  if (::bind(probe, reinterpret_cast<sockaddr*>(&address), length) != 0 ||
      ::getsockname(probe, reinterpret_cast<sockaddr*>(&address), &length) != 0) {
    ADD_FAILURE() << "no port";
  }
  ::close(probe);
  return ntohs(address.sin_port);
}

TEST(ConnectDeadline, AThreadedClientMovesOnFromAnAddressThatDropsSyns) {
  DropsSyns dropping;
  InProcessMultiplexer mx;
  multiplexer::ThreadedClient client(multiplexer::peers::WEBSITE);
  client.set_resolver(failing_first(dropping.port, mx.port));
  EXPECT_TRUE(client.connect("mx", 1980, WAIT)) << "the second address never tried";
}

TEST(ConnectDeadline, ASyncClientMovesOnFromAnAddressThatDropsSyns) {
  DropsSyns dropping;
  InProcessMultiplexer mx;
  multiplexer::Client client(multiplexer::peers::WEBSITE);
  client.set_resolver(failing_first(dropping.port, mx.port));
  const multiplexer::ConnectionWrapper connected = client.connect("mx", 1980, WAIT);
  ASSERT_TRUE(connected) << "the second address never tried";
  EXPECT_EQ(mx.port, connected.endpoint().port());
}

// The deadline of a connect that was refused leaves the next address's
// connect alone when it expired in the loop pass that read the refusal:
// asio ran the refusal first, which started the next address, and the
// deadline's handler then took that connect for its own and cancelled it
// as timed out, so the client gave up a multiplexer that answered until
// its reconnect. A SyncClient makes its first connect inside the call, and
// nothing runs its loop until the deadline has passed: the refusal and the
// expiry are read in one pass.
TEST(ConnectDeadline, ADeadlinePastWithARefusalLeavesTheNextAddressAlone) {
  const unsigned short refusing = refusing_port();
  InProcessMultiplexer mx;
  multiplexer::Client client(multiplexer::peers::WEBSITE);
  client.set_resolver(failing_first(refusing, mx.port));
  const multiplexer::ConnectionWrapper connecting = client.async_connect("mx", 1980);
  std::this_thread::sleep_for(mx::from_seconds(multiplexer::CONNECT_ATTEMPT_SECONDS + 0.5f));
  // The loop runs until the client is connected, or gave the attempt up
  // and armed its reconnect; the wait only detects a failure.
  for (int passes = 0; passes < 300 && client.connections_count() == 0 && client.reconnects_pending() == 0; ++passes) {
    client.wait_for_connection(connecting, 0.1f);
  }
  EXPECT_EQ(0u, client.reconnects_pending()) << "the multiplexer's address given up as timed out";
  EXPECT_EQ(1u, client.connections_count());
}

}  // namespace
