// A rules file the multiplexer cannot read is said with the reason,
// "cannot read FILE: REASON", and nothing throws. A directory, which an
// ifstream opened, threw std::ios_base::failure out of the read, which
// stopped the periodic check for good, as it did the reload on SIGHUP, and
// left a RELOAD unanswered; and a file not opened, descriptors exhausted
// say, was "cannot read FILE" with no reason, which pointed at the file.
// The periodic check goes on through a file it cannot read, and applies
// the file once it is one again. Counted, not timed: on an io_service of
// the test's own, with no peer, the handlers run one at a time are the
// checks.
#include <gtest/gtest.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <unistd.h>

#include <asio/io_service.hpp>
#include <chrono>
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

// `name` in a directory of the test's own.
std::string scratch(const std::string& name) {
  const char* directory = getenv("TEST_TMPDIR");
  return std::string(directory ? directory : "/tmp") + "/" + name;
}

// The whole of the file at `path`.
std::string contents(const std::string& path) {
  std::ifstream in(path.c_str(), std::ios::binary);
  std::ostringstream text;
  text << in.rdbuf();
  return text.str();
}

// Writes `text` to `path`, replacing what was there.
void write(const std::string& path, const std::string& text) { std::ofstream(path.c_str(), std::ios::binary) << text; }

// What load_rules() answered, and its error, for `path`; "threw: WHAT"
// when it threw.
std::string loaded(Server& server, const std::string& path) {
  server.set_rules_file(path);
  std::string error;
  try {
    return server.load_rules(&error) == Server::RulesLoad::FAILED ? error : "not refused";
  } catch (const std::exception& thrown) {
    return std::string("threw: ") + thrown.what();
  }
}

}  // namespace

TEST(RulesReadErrors, ADirectoryIsSaidWithTheReason) {
  asio::io_service io_service;
  Server::pointer server = Server::Create(io_service, "127.0.0.1", 0);
  const std::string directory = scratch("a_directory.rules");
  ::mkdir(directory.c_str(), 0700);
  EXPECT_EQ("cannot read " + directory + ": Is a directory", loaded(*server, directory));
}

TEST(RulesReadErrors, DescriptorsExhaustedAreSaid) {
  asio::io_service io_service;
  Server::pointer server = Server::Create(io_service, "127.0.0.1", 0);
  rlimit was{};
  ASSERT_EQ(0, getrlimit(RLIMIT_NOFILE, &was));
  rlimit none = was;
  none.rlim_cur = 3;  // the standard three: no number free for the file
  ASSERT_EQ(0, setrlimit(RLIMIT_NOFILE, &none));
  const std::string said = loaded(*server, rules_path());
  setrlimit(RLIMIT_NOFILE, &was);
  EXPECT_EQ("cannot read " + rules_path() + ": Too many open files", said);
}

TEST(RulesReadErrors, TheCheckGoesOnThroughAFileItCannotRead) {
  asio::io_service io_service;
  Server::pointer server = Server::Create(io_service, "127.0.0.1", 0);
  const std::string path = scratch("checked.rules");
  const std::string original = contents(rules_path());
  write(path, original);
  ASSERT_EQ("not refused", loaded(*server, path));
  const std::string first = server->rules_fingerprint();
  server->set_rules_check_interval(Server::MIN_RULES_CHECK_INTERVAL);
  server->start();
  ::unlink(path.c_str());
  ::mkdir(path.c_str(), 0700);
  // A check runs every MIN_RULES_CHECK_INTERVAL; 30 s without one is a
  // check that was never armed again, a failure detector only.
  const std::chrono::seconds stuck(30);
  for (int check = 0; check < 3; ++check) {
    std::size_t ran = 0;
    ASSERT_NO_THROW(ran = io_service.run_one_for(stuck)) << "a check of the directory";
    ASSERT_EQ(1u, ran) << "the checks go on";
  }
  ::rmdir(path.c_str());
  write(path, original + "\n# edited\n");
  for (int check = 0; check < 10 && server->rules_fingerprint() == first; ++check) {
    ASSERT_EQ(1u, io_service.run_one_for(stuck));  // put in use at the second check that reads it
  }
  EXPECT_NE(first, server->rules_fingerprint()) << "the checks went on and applied the file";
  server->stop();
}
