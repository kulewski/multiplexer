// The peers file is written at start(), empty with nobody connected: a
// multiplexer restarted after a crash on the same file listed the dead
// process's peers until its first arrival or departure. Counted: the file
// is read right after start(), no peer having come.
#include <gtest/gtest.h>

#include <asio/io_service.hpp>
#include <cstdlib>
#include <fstream>
#include <sstream>
#include <string>

#include "multiplexer/server.h"

using multiplexer::Server;

namespace {

// The tests' rules file, as InProcessMultiplexer finds it.
std::string rules_path() {
  const char* srcdir = getenv("TEST_SRCDIR");
  return std::string(srcdir ? srcdir : ".") + (srcdir ? "/mx/" : "/") + "tests/testing.rules";
}

// The whole of the file at `path`.
std::string contents(const std::string& path) {
  std::ifstream in(path.c_str(), std::ios::binary);
  std::ostringstream text;
  text << in.rdbuf();
  return text.str();
}

}  // namespace

TEST(PeersFile, IsWrittenAtStart) {
  const char* directory = getenv("TEST_TMPDIR");
  const std::string path = std::string(directory ? directory : "/tmp") + "/peers";
  std::ofstream(path.c_str()) << "1234 WEBSITE 102\n";  // what a process that died left
  asio::io_service io_service;
  Server::pointer server = Server::Create(io_service, "127.0.0.1", 0);
  server->set_rules_file(rules_path());
  std::string error;
  ASSERT_EQ(Server::RulesLoad::LOADED, server->load_rules(&error)) << error;
  server->set_peers_file(path);
  server->start();
  EXPECT_EQ("", contents(path)) << "nobody is connected";
  server->stop();
}
