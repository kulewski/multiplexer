// A file session's max_seconds deadline stops that session only. When it
// expires as the session is stopped and another started, its completion
// already queued, the cancel too late, it leaves the new session open,
// where it closed it as 'max_seconds reached'. Counted, not timed: a timer
// of the test's, armed just before the session's, has expired with it when
// the loop runs, and runs first, stopping the session and starting the
// next; the wait only makes sure both have expired.
#include <gtest/gtest.h>

#include <asio/io_service.hpp>
#include <asio/steady_timer.hpp>
#include <chrono>
#include <cstdlib>
#include <string>
#include <thread>

#include "multiplexer/server.h"

TEST(RecordingSession, ADeadlineThatComesTooLateLeavesTheNextSessionOpen) {
  asio::io_service io_service;
  multiplexer::Server::pointer server = multiplexer::Server::Create(io_service, "127.0.0.1", 0);
  const char* srcdir = std::getenv("TEST_SRCDIR");
  server->set_rules_file(std::string(srcdir ? srcdir : ".") + (srcdir ? "/mx/" : "/") + "tests/testing.rules");
  std::string error;
  ASSERT_EQ(multiplexer::Server::RulesLoad::LOADED, server->load_rules(&error)) << error;
  const char* directory = std::getenv("TEST_TMPDIR");
  const std::string stem = std::string(directory ? directory : "/tmp") + "/recording_session_test.";

  asio::steady_timer earlier(io_service);  // expires before the session's deadline, armed after it
  earlier.expires_after(std::chrono::milliseconds(900));
  ASSERT_TRUE(server->start_recording(stem + "first.rec", "first", 0, 0, /*max_seconds=*/1, &error)) << error;
  bool second_started = false;
  earlier.async_wait([&](const asio::error_code&) {
    server->stop_recording("stopped by the test");
    second_started = server->start_recording(stem + "second.rec", "second", 0, 0, 0, &error);
  });
  std::this_thread::sleep_for(std::chrono::milliseconds(1100));  // both have expired: one loop pass finds them
  io_service.poll();
  ASSERT_TRUE(second_started) << error;
  EXPECT_TRUE(server->recording()) << "the second session is open";
  server->stop_recording("done");
  io_service.poll();
}
