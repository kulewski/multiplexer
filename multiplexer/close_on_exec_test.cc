// Every socket the clients and the multiplexer open is closed on exec: a
// program the process runs, which inherits every other descriptor, does
// not keep a connection open after its parent closed it or died, where
// the multiplexer routed to the silent copy until the heartbeats dropped
// it, or for as long as the program ran when the peer was passive. Each
// socket of this process connected to the in-process multiplexer, or
// listening or accepted on its port, carries FD_CLOEXEC: both C++ clients'
// sockets, and the multiplexer's listening and accepted ones.
#include <fcntl.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/resource.h>
#include <sys/socket.h>

#include <algorithm>
#include <vector>

#include "multiplexer/client.h"
#include "multiplexer/in_process_multiplexer.h"
#include "multiplexer/multiplexer.constants.h"
#include "multiplexer/threaded_client.h"

using multiplexer::testing::InProcessMultiplexer;

namespace {

// This process's sockets with `port` at their `peer` end, or else at
// their own: getpeername() or getsockname() on every number below the
// descriptor limit.
std::vector<int> sockets_with(unsigned short port, bool peer) {
  rlimit limit{};
  getrlimit(RLIMIT_NOFILE, &limit);
  const int highest = static_cast<int>(std::min<rlim_t>(limit.rlim_cur, 65536));
  std::vector<int> found;
  for (int fd = 0; fd < highest; ++fd) {
    sockaddr_in address{};
    socklen_t length = sizeof address;
    const int named = peer ? getpeername(fd, reinterpret_cast<sockaddr*>(&address), &length)
                           : getsockname(fd, reinterpret_cast<sockaddr*>(&address), &length);
    if (named == 0 && address.sin_family == AF_INET && ntohs(address.sin_port) == port) {
      found.push_back(fd);
    }
  }
  return found;
}

// Whether `fd` is closed on exec.
bool closed_on_exec(int fd) {
  const int flags = fcntl(fd, F_GETFD);
  return flags != -1 && (flags & FD_CLOEXEC) != 0;
}

}  // namespace

TEST(CloseOnExec, EverySocketOfAConnectionIsClosedOnExec) {
  InProcessMultiplexer mx;
  multiplexer::Client sync(multiplexer::peers::WEBSITE);
  ASSERT_TRUE(sync.wait_for_connection(sync.connect("127.0.0.1", mx.port, 5), 5));
  multiplexer::ThreadedClient threaded(multiplexer::peers::WEBSITE);
  ASSERT_TRUE(threaded.connect("127.0.0.1", mx.port, 5));
  const std::vector<int> clients = sockets_with(mx.port, true);
  const std::vector<int> multiplexer = sockets_with(mx.port, false);
  ASSERT_EQ(2u, clients.size()) << "one per client";
  ASSERT_EQ(3u, multiplexer.size()) << "the listening one, and one accepted per client";
  for (int fd : clients) {
    EXPECT_TRUE(closed_on_exec(fd)) << "a client's socket, descriptor " << fd;
  }
  for (int fd : multiplexer) {
    EXPECT_TRUE(closed_on_exec(fd)) << "a socket of the multiplexer's, descriptor " << fd;
  }
}
