// InProcessMultiplexer: a multiplexer on a free port, driven by a thread of
// its own, for unit tests that need a real one without the integration
// harness. Uses the tests' rules file, tests/testing.rules. Also what a
// test that fills a connection whose far end reads nothing sends first,
// whatever this machine's TCP buffers: FILL_FRAMES frames of fill_size().
#ifndef MX_MULTIPLEXER_IN_PROCESS_MULTIPLEXER_H_
#define MX_MULTIPLEXER_IN_PROCESS_MULTIPLEXER_H_

#include <asio/io_service.hpp>
#include <cstddef>
#include <cstdlib>
#include <fstream>
#include <future>
#include <string>
#include <thread>

#include "multiplexer/server.h"

namespace multiplexer {
namespace testing {

struct InProcessMultiplexer {
  // On an ephemeral port, or on `port`: a test that kills one and brings
  // it back on the same address gives the port the first one got.
  explicit InProcessMultiplexer(unsigned short listen_port = 0) {
    // Everything of the server's happens on its io thread, including its
    // creation: connections bind their thread checker where they are made.
    std::promise<unsigned short> bound;
    thread = std::thread([this, &bound, listen_port] {
      const char* srcdir = getenv("TEST_SRCDIR");
      std::string rules = std::string(srcdir ? srcdir : ".") + (srcdir ? "/mx/" : "/") + "tests/testing.rules";
      server = multiplexer::Server::Create(io_service, "127.0.0.1", listen_port);
      server->set_rules_file(rules);
      std::string error;
      AssertMsg(server->load_rules(&error) == multiplexer::Server::RulesLoad::LOADED, error);
      server->start();
      bound.set_value(server->local_port());
      io_service.run();
    });
    port = bound.get_future().get();
  }
  ~InProcessMultiplexer() {
    io_service.post([this] { server->stop(); });
    thread.join();
  }
  asio::io_service io_service;
  multiplexer::Server::pointer server;
  unsigned short port = 0;
  std::thread thread;
};

// The most the two sockets of one connection may hold: the largest send
// buffer and the largest receive buffer the kernel allows, the third field
// of /proc/sys/net/ipv4/tcp_wmem and tcp_rmem, 8 MiB each where /proc does
// not say.
inline std::size_t socket_bytes() {
  std::size_t total = 0;
  for (const char* path : {"/proc/sys/net/ipv4/tcp_wmem", "/proc/sys/net/ipv4/tcp_rmem"}) {
    std::ifstream limits(path);
    std::size_t least = 0, initial = 0, largest = 8 << 20;  // a guess where /proc does not say
    limits >> least >> initial >> largest;
    total += largest;
  }
  return total;
}

// A test that needs messages to wait for room first sends FILL_FRAMES frames
// of fill_size() bytes: together twice socket_bytes(), so that the sockets
// are full however far the kernel grew them, in so few frames that a queue
// counting messages keeps room for the test's own. Filled with frames of
// the test's own size instead, a connection took hundreds of thousands of
// them where the kernel allows large buffers.
constexpr int FILL_FRAMES = 32;
inline std::size_t fill_size(std::size_t least = 64 * 1024) {
  const std::size_t each = (2 * socket_bytes() + FILL_FRAMES - 1) / FILL_FRAMES;
  return each > least ? each : least;
}

}  // namespace testing
}  // namespace multiplexer

#endif  // MX_MULTIPLEXER_IN_PROCESS_MULTIPLEXER_H_
