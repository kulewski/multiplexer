// The rules check interval the multiplexer's C++ API is given never makes
// it re-read the rules file in a loop: NaN is off, as 0 and a negative
// interval are, where its timer, armed for no time at all, expired as it
// was armed, again and again; a positive interval below
// Server::MIN_RULES_CHECK_INTERVAL, which mx::from_seconds truncated to the
// same nothing under a microsecond, is refused with std::invalid_argument
// saying why, the interval set before kept. Counted, not timed: on an
// io_service of the test's own, with no peer and nothing else to do, the
// handlers a poll runs are the checks, each a read of the file; there is
// none.
#include <gtest/gtest.h>

#include <asio/io_service.hpp>
#include <cmath>
#include <cstdlib>
#include <stdexcept>
#include <string>

#include "multiplexer/server.h"

using multiplexer::Server;

namespace {

// The tests' rules file, as InProcessMultiplexer finds it.
std::string rules_path() {
  const char* srcdir = getenv("TEST_SRCDIR");
  return std::string(srcdir ? srcdir : ".") + (srcdir ? "/mx/" : "/") + "tests/testing.rules";
}

// A multiplexer on `io_service`, on a free port, its rules loaded.
Server::pointer made(asio::io_service& io_service) {
  Server::pointer server = Server::Create(io_service, "127.0.0.1", 0);
  server->set_rules_file(rules_path());
  std::string error;
  EXPECT_EQ(Server::RulesLoad::LOADED, server->load_rules(&error)) << error;
  return server;
}

// How many handlers `polls` polls of `io_service` ran: with no peer, the
// rules checks the timer started.
std::size_t checks(asio::io_service& io_service, int polls) {
  std::size_t ran = 0;
  for (int poll = 0; poll < polls; ++poll) {
    ran += io_service.poll_one();
  }
  return ran;
}

// What set_rules_check_interval(`seconds`) threw, std::invalid_argument's
// text, or "accepted".
std::string refusal(Server& server, float seconds) {
  try {
    server.set_rules_check_interval(seconds);
  } catch (const std::invalid_argument& error) {
    return error.what();
  }
  return "accepted";
}

// The server's stop, and what its loop still has to run for it.
void stop(asio::io_service& io_service, const Server::pointer& server) {
  server->stop();
  io_service.run();
}

}  // namespace

TEST(RulesCheckInterval, NanChecksNothing) {
  asio::io_service io_service;
  Server::pointer server = made(io_service);
  EXPECT_EQ("accepted", refusal(*server, std::nanf("")));
  server->start();
  EXPECT_EQ(0u, checks(io_service, 100000));
  stop(io_service, server);
}

TEST(RulesCheckInterval, BelowTheShortestIsRefusedAndChecksNothing) {
  asio::io_service io_service;
  Server::pointer server = made(io_service);
  EXPECT_NE(std::string::npos, refusal(*server, 0.009f).find("below the shortest rules check interval"));
  EXPECT_NE(std::string::npos, refusal(*server, 1e-7f).find("below the shortest rules check interval"));
  server->start();
  EXPECT_EQ(0u, checks(io_service, 100000)) << "the interval before, 0, kept";
  stop(io_service, server);
}

TEST(RulesCheckInterval, TheShortestAndLongerPassAsDoTheOffOnes) {
  asio::io_service io_service;
  Server::pointer server = made(io_service);
  for (float seconds : {0.01f, 2.0f, INFINITY, 0.0f, -1.0f, std::nanf("")}) {
    EXPECT_EQ("accepted", refusal(*server, seconds)) << seconds;
  }
  stop(io_service, server);
}
