// A client given two targets that reach one multiplexer, its address and
// the name localhost, says so once, at WARNING, naming both: their
// connections replace each other at every reconnect, which only a line at
// HIGHVERBOSITY told. The replacing goes on, as it did; the warning does
// not repeat. Counted, not timed: the welcomes the client's observer
// hears, three, the second target's and then the first's again at its
// reconnect, and the warning once among them.
#include <gtest/gtest.h>
#include <unistd.h>

#include <asio/io_service.hpp>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <memory>
#include <sstream>
#include <string>

#include "multiplexer/basic_client.h"
#include "multiplexer/in_process_multiplexer.h"
#include "multiplexer/multiplexer.constants.h"

namespace {

using multiplexer::testing::InProcessMultiplexer;

// Descriptor 2, which everything in this process logs to, goes to a file
// while it lives; text() is what was logged meanwhile.
class StderrToFile {
 public:
  StderrToFile() : path_(std::string(std::getenv("TEST_TMPDIR") ? std::getenv("TEST_TMPDIR") : "/tmp") + "/stderr") {
    std::fflush(stderr);
    saved_ = ::dup(2);
    std::FILE* file = std::fopen(path_.c_str(), "w");
    ::dup2(::fileno(file), 2);
    std::fclose(file);
  }
  ~StderrToFile() { restore(); }
  std::string text() {
    restore();
    std::ifstream in(path_);
    std::stringstream read;
    read << in.rdbuf();
    return read.str();
  }

 private:
  void restore() {
    if (saved_ >= 0) {
      std::fflush(stderr);
      ::dup2(saved_, 2);
      ::close(saved_);
      saved_ = -1;
    }
  }
  std::string path_;
  int saved_ = -1;
};

// The number of times `text` holds `part`.
int count(const std::string& text, const std::string& part) {
  int found = 0;
  for (std::size_t at = text.find(part); at != std::string::npos; at = text.find(part, at + 1)) {
    ++found;
  }
  return found;
}

TEST(DuplicateTarget, TwoTargetsReachingOneMultiplexerAreSaidOnce) {
  InProcessMultiplexer mx;
  const std::string port = std::to_string(mx.port);
  StderrToFile logged;
  int welcomes = 0;
  {
    asio::io_service io_service;
    std::shared_ptr<multiplexer::BasicClient> client =
        multiplexer::BasicClient::Create(io_service, multiplexer::peers::WEBSITE);
    client->set_connection_observer([&welcomes](const multiplexer::ConnectionWrapper&, bool up) {
      if (up) {
        ++welcomes;
      }
    });
    ASSERT_TRUE(client->wait_for_connection(client->async_connect("127.0.0.1", mx.port), 30));
    client->async_connect("localhost", mx.port);
    std::unique_ptr<mx::SimpleTimer> timer = client->create_timer(30);
    while (welcomes < 3 && !timer->expired()) {
      client->run_one();  // the second target's welcome, then the first's again at its reconnect
    }
    client->shutdown();
  }
  const std::string text = logged.text();
  EXPECT_LE(3, welcomes) << "the two connections did not replace each other";
  EXPECT_EQ(1, count(text, "is reached through two targets, 127.0.0.1:" + port + " and localhost:" + port)) << text;
}

}  // namespace
