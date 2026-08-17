// InProcessMultiplexer: a multiplexer on a free port, driven by a thread of
// its own, for unit tests that need a real one without the integration
// harness. Uses the repository's multiplexer.rules.
#ifndef MX_MULTIPLEXER_IN_PROCESS_MULTIPLEXER_H_
#define MX_MULTIPLEXER_IN_PROCESS_MULTIPLEXER_H_

#include <cstdlib>
#include <future>
#include <string>
#include <thread>

#include <boost/asio/io_service.hpp>

#include "multiplexer/server.h"

namespace multiplexer {
namespace testing {

struct InProcessMultiplexer {
  InProcessMultiplexer() {
    // Everything of the server's happens on its io thread, including its
    // creation: connections bind their thread checker where they are made.
    std::promise<unsigned short> bound;
    thread = std::thread([this, &bound] {
      const char *srcdir = getenv("TEST_SRCDIR");
      std::string rules = std::string(srcdir ? srcdir : ".") + (srcdir ? "/mx/" : "/") + "multiplexer.rules";
      server = multiplexer::Server::Create(io_service, "127.0.0.1", 0);
      server->clear_rules();
      server->read_rules(rules);
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
  boost::asio::io_service io_service;
  multiplexer::Server::pointer server;
  unsigned short port = 0;
  std::thread thread;
};

} // namespace testing
} // namespace multiplexer

#endif // MX_MULTIPLEXER_IN_PROCESS_MULTIPLEXER_H_
