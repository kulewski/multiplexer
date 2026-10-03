// A Server an embedder releases after stop(), while the loop it ran on
// still has work queued, the aborted accept's handler among it, is gone
// for that handler, which does nothing, where it ran on the freed Server
// through the raw pointer it held: under AddressSanitizer a
// heap-use-after-free. Counted, not timed: the loop runs to its end on
// the test's own thread after the release.
#include <gtest/gtest.h>

#include <asio/io_service.hpp>
#include <cstdlib>
#include <memory>
#include <string>

#include "multiplexer/server.h"

TEST(ServerRelease, AfterStopWhileTheLoopHasWorkQueued) {
  asio::io_service io_service;
  std::weak_ptr<multiplexer::Server> released;
  {
    multiplexer::Server::pointer server = multiplexer::Server::Create(io_service, "127.0.0.1", 0);
    const char* srcdir = getenv("TEST_SRCDIR");
    server->set_rules_file(std::string(srcdir ? srcdir : ".") + (srcdir ? "/mx/" : "/") + "tests/testing.rules");
    std::string error;
    ASSERT_EQ(multiplexer::Server::RulesLoad::LOADED, server->load_rules(&error)) << error;
    server->start();
    server->stop();  // the accept is aborted: its handler waits for the loop
    released = server;
  }
  EXPECT_TRUE(released.expired()) << "nothing else holds the Server";
  io_service.run();  // the aborted accept's handler, and whatever else was queued, after the release
}
