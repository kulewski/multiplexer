// The peers file is written at start(), empty with nobody connected: a
// multiplexer restarted after a crash on the same file listed the dead
// process's peers until its first arrival or departure. Counted: the file
// is read right after start(), no peer having come. And a write of it that
// fails, as on a full disk, leaves the file as it was, where the temporary
// file, empty or cut short, was renamed over it; a file that stays
// unwritable is said once at ERROR with the reason, where every arrival
// and departure logged the same line without one.
#include <gtest/gtest.h>
#include <sys/stat.h>
#include <unistd.h>

#include <asio/io_service.hpp>
#include <chrono>
#include <cstdlib>
#include <fstream>
#include <future>
#include <memory>
#include <sstream>
#include <string>
#include <thread>

#include "multiplexer/client.h"
#include "multiplexer/multiplexer.constants.h"
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

// The number of times `text` holds `part`.
int occurrences(const std::string& text, const std::string& part) {
  int found = 0;
  for (std::string::size_type at = text.find(part); at != std::string::npos; at = text.find(part, at + 1)) {
    ++found;
  }
  return found;
}

// A peer on its own thread: connected once constructed, gone once leave()
// returns, its connection closed by the client's destructor.
class Peer {
 public:
  explicit Peer(unsigned short port)
      : thread_([port, gone = gone_.get_future().share()] {
          multiplexer::Client client(multiplexer::peers::WEBSITE);
          client.connect("127.0.0.1", port, 30);
          gone.wait();
        }) {}
  ~Peer() { leave(); }
  void leave() {
    if (thread_.joinable()) {
      gone_.set_value();
      thread_.join();
    }
  }

 private:
  std::promise<void> gone_;
  std::thread thread_;
};

// Runs the server's loop until `done` says so, at most 30 s.
template <typename Done>
bool run_until(asio::io_service& io_service, Done done) {
  for (int pass = 0; pass < 3000 && !done(); ++pass) {
    io_service.run_one_for(std::chrono::milliseconds(10));
  }
  return done();
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

// The temporary file's name is taken by a link to /dev/full, which opens
// and fails every write with ENOSPC, as a full disk does; a peer's arrival
// makes the server write the file again, a line to write this time.
// Counted: the loop runs until the write was tried, the link gone either
// way, renamed or removed; the bound only detects a failure.
TEST(PeersFile, AFailedWriteLeavesTheFileAsItWas) {
  const char* directory = getenv("TEST_TMPDIR");
  const std::string path = std::string(directory ? directory : "/tmp") + "/peers_full";
  const std::string tmp = path + ".tmp";
  asio::io_service io_service;
  Server::pointer server = Server::Create(io_service, "127.0.0.1", 0);
  server->set_rules_file(rules_path());
  std::string error;
  ASSERT_EQ(Server::RulesLoad::LOADED, server->load_rules(&error)) << error;
  server->set_peers_file(path);
  server->start();  // written at once, empty: nobody is connected
  ASSERT_EQ("", contents(path));
  ASSERT_EQ(0, ::symlink("/dev/full", tmp.c_str()));
  std::promise<void> checked;
  std::shared_future<void> done = checked.get_future().share();
  const unsigned short port = server->local_port();
  std::thread peer([port, done] {
    multiplexer::Client client(multiplexer::peers::WEBSITE);
    client.connect("127.0.0.1", port, 30);
    done.wait();  // connected while the file is looked at
  });
  struct stat status;
  for (int pass = 0; pass < 3000 && ::lstat(tmp.c_str(), &status) == 0; ++pass) {
    io_service.run_one_for(std::chrono::milliseconds(10));
  }
  const bool tried = ::lstat(tmp.c_str(), &status) != 0;
  const bool regular = ::lstat(path.c_str(), &status) == 0 && S_ISREG(status.st_mode);
  checked.set_value();
  std::future<void> left = std::async(std::launch::async, [&peer] { peer.join(); });
  while (left.wait_for(std::chrono::seconds(0)) != std::future_status::ready) {
    io_service.run_one_for(std::chrono::milliseconds(10));  // the peer's close is answered
  }
  server->stop();
  EXPECT_TRUE(tried) << "the file was never written again";
  ASSERT_TRUE(regular) << "the temporary file went over the peers file";
  EXPECT_EQ("", contents(path)) << "the file as it was";
}

// Three changes of the peers in a row, two arrivals and a departure, each
// with the temporary file's name a link to /dev/full, whose writes fail
// with ENOSPC, then a fourth, an arrival, with nothing in the way: one
// ERROR, with the reason, for the three failed writes, and one line that
// the file is written again after them. Counted: each change runs the loop
// until the link is gone, the write tried; the last one until the file
// lists the two peers left.
TEST(PeersFile, AFileThatStaysUnwritableIsSaidOnce) {
  const char* directory = getenv("TEST_TMPDIR");
  const std::string path = std::string(directory ? directory : "/tmp") + "/peers_unwritable";
  const std::string tmp = path + ".tmp";
  asio::io_service io_service;
  Server::pointer server = Server::Create(io_service, "127.0.0.1", 0);
  server->set_rules_file(rules_path());
  std::string error;
  ASSERT_EQ(Server::RulesLoad::LOADED, server->load_rules(&error)) << error;
  server->set_peers_file(path);
  server->start();  // written at once, empty: nobody is connected
  ASSERT_EQ("", contents(path));
  const unsigned short port = server->local_port();
  struct stat status;
  const auto tried = [&tmp, &status] { return ::lstat(tmp.c_str(), &status) != 0; };

  ::testing::internal::CaptureStderr();
  ASSERT_EQ(0, ::symlink("/dev/full", tmp.c_str()));
  std::unique_ptr<Peer> first(new Peer(port));
  const bool first_tried = run_until(io_service, tried);
  ASSERT_EQ(0, ::symlink("/dev/full", tmp.c_str()));
  std::unique_ptr<Peer> second(new Peer(port));
  const bool second_tried = run_until(io_service, tried);
  ASSERT_EQ(0, ::symlink("/dev/full", tmp.c_str()));
  std::future<void> left = std::async(std::launch::async, [&first] { first->leave(); });
  const bool third_tried = run_until(io_service, tried);
  run_until(io_service, [&left] { return left.wait_for(std::chrono::seconds(0)) == std::future_status::ready; });
  std::unique_ptr<Peer> third(new Peer(port));
  const bool written = run_until(io_service, [&path] { return occurrences(contents(path), " WEBSITE ") == 2; });
  const std::string log = ::testing::internal::GetCapturedStderr();

  std::future<void> gone = std::async(std::launch::async, [&second, &third] {
    second->leave();
    third->leave();
  });
  run_until(io_service, [&gone] { return gone.wait_for(std::chrono::seconds(0)) == std::future_status::ready; });
  server->stop();
  ASSERT_TRUE(first_tried && second_tried && third_tried) << "a change left the file unwritten";
  ASSERT_TRUE(written) << "the file was never written again";
  EXPECT_EQ(1, occurrences(log, "cannot write peers file")) << log;
  EXPECT_EQ(1, occurrences(log, "No space left on device")) << log;
  EXPECT_EQ(1, occurrences(log, "written again, after 3 failed write(s)")) << log;
}
