// A TCP connect to an address that drops the SYNs ends at
// CONNECT_ATTEMPT_SECONDS, and the next address the target resolved to is
// tried: it held the attempt for the kernel's own timeout, two minutes on
// Linux, the name's other addresses untried meanwhile. A listener whose
// accept queue is full drops the SYNs of a new connect, as a firewall that
// drops them does: one with a backlog of 0, its one place taken by a
// connection nobody accepts. The client's resolver gives that address
// first and a multiplexer's second, and both client classes connect by the
// name within a wait far shorter than the kernel's timeout.
#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstdint>
#include <string>
#include <vector>

#include "multiplexer/client.h"
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

// The resolver of a name with two addresses: the one that drops the SYNs
// first, the multiplexer second.
multiplexer::BasicClient::Resolver dropping_first(unsigned short dropping, unsigned short live) {
  return [dropping, live](const std::string&, std::uint16_t, asio::error_code&) {
    std::vector<asio::ip::tcp::endpoint> addresses;
    addresses.emplace_back(asio::ip::make_address("127.0.0.1"), dropping);
    addresses.emplace_back(asio::ip::make_address("127.0.0.1"), live);
    return addresses;
  };
}

TEST(ConnectDeadline, AThreadedClientMovesOnFromAnAddressThatDropsSyns) {
  DropsSyns dropping;
  InProcessMultiplexer mx;
  multiplexer::ThreadedClient client(multiplexer::peers::WEBSITE);
  client.set_resolver(dropping_first(dropping.port, mx.port));
  EXPECT_TRUE(client.connect("mx", 1980, WAIT)) << "the second address never tried";
}

TEST(ConnectDeadline, ASyncClientMovesOnFromAnAddressThatDropsSyns) {
  DropsSyns dropping;
  InProcessMultiplexer mx;
  multiplexer::Client client(multiplexer::peers::WEBSITE);
  client.set_resolver(dropping_first(dropping.port, mx.port));
  const multiplexer::ConnectionWrapper connected = client.connect("mx", 1980, WAIT);
  ASSERT_TRUE(connected) << "the second address never tried";
  EXPECT_EQ(mx.port, connected.endpoint().port());
}

}  // namespace
