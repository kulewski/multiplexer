// The session label rules and the session file name, the parts of remote
// recording that keep a request from choosing a path.
#include <gtest/gtest.h>

#include "multiplexer/recorder.h"

using multiplexer::recording::session_path;
using multiplexer::recording::valid_label;

TEST(RecordingLabel, AcceptsSafeNames) {
  EXPECT_TRUE(valid_label("session"));
  EXPECT_TRUE(valid_label("checkout-bug_2"));
  EXPECT_TRUE(valid_label("A"));
  EXPECT_TRUE(valid_label(std::string(64, 'x')));
}

TEST(RecordingLabel, RefusesAnythingThatCouldBeAPath) {
  EXPECT_FALSE(valid_label(""));
  EXPECT_FALSE(valid_label("../etc"));
  EXPECT_FALSE(valid_label("a/b"));
  EXPECT_FALSE(valid_label("a b"));
  EXPECT_FALSE(valid_label("a.rec"));
  EXPECT_FALSE(valid_label(std::string(65, 'x')));
}

TEST(SessionPath, NamesTheMultiplexerAndTheTime) {
  // 2026-09-10T18:30:12.123456Z
  const boost::uint64_t started_us = 1789065012123456ULL;
  EXPECT_EQ("/var/recordings/session.20260910T183012.123456Z.42.rec",
            session_path("/var/recordings", "session", 42, started_us));
  EXPECT_EQ("/var/recordings/session.20260910T183012.123456Z.42.rec",
            session_path("/var/recordings/", "session", 42, started_us));
}

TEST(SessionPath, DiffersPerMultiplexerAndPerSession) {
  const boost::uint64_t started_us = 1789065012123456ULL;
  EXPECT_NE(session_path("d", "s", 1, started_us), session_path("d", "s", 2, started_us));
  EXPECT_NE(session_path("d", "s", 1, started_us), session_path("d", "s", 1, started_us + 1));
}
