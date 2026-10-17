// ServerLoops deals the loops out round robin from the main one, runs each
// other loop on a thread of its own, lets them run out of work and joins
// them at finish(), keeps a loop going when a handler throws, and makes
// every loop's reactor with the loop, so that no accept later needs a
// descriptor for one. Counted, not timed: handlers count what they ran and
// say on which thread, and the process's descriptors are counted in
// /proc/self/fd.
#include "multiplexer/server_loops.h"

#include <dirent.h>
#include <gtest/gtest.h>

#include <asio/ip/tcp.hpp>
#include <asio/post.hpp>
#include <asio/steady_timer.hpp>
#include <atomic>
#include <future>
#include <set>
#include <stdexcept>
#include <thread>
#include <vector>

namespace {

using multiplexer::ServerLoops;

// The descriptors the process has open, as /proc/self/fd lists them.
int open_descriptors() {
  DIR* directory = ::opendir("/proc/self/fd");
  if (!directory) {
    return -1;
  }
  int count = 0;
  while (::readdir(directory)) {
    ++count;
  }
  ::closedir(directory);
  return count;
}

TEST(ServerLoops, DealsTheLoopsOutRoundRobinFromTheMainOne) {
  asio::io_service main;
  ServerLoops loops(main, 3);
  EXPECT_EQ(3u, loops.count());
  std::vector<asio::io_service*> dealt;
  for (int connection = 0; connection < 6; ++connection) {
    dealt.push_back(&loops.next());
  }
  EXPECT_EQ(&main, dealt[0]);
  EXPECT_NE(&main, dealt[1]);
  EXPECT_NE(&main, dealt[2]);
  EXPECT_NE(dealt[1], dealt[2]);
  EXPECT_EQ(std::vector<asio::io_service*>(dealt.begin(), dealt.begin() + 3),
            std::vector<asio::io_service*>(dealt.begin() + 3, dealt.end()));
}

// Each loop beside the main one runs on a thread of its own, none of them
// the caller's, which runs the main loop.
TEST(ServerLoops, RunsEachOtherLoopOnAThreadOfItsOwn) {
  asio::io_service main;
  ServerLoops loops(main, 3);
  loops.next();  // the main loop's turn
  std::vector<std::future<std::thread::id>> ran;
  for (int loop = 1; loop < 3; ++loop) {
    std::shared_ptr<std::promise<std::thread::id>> on = std::make_shared<std::promise<std::thread::id>>();
    ran.push_back(on->get_future());
    asio::post(loops.next(), [on] { on->set_value(std::this_thread::get_id()); });
  }
  loops.start();
  std::set<std::thread::id> threads;
  for (std::future<std::thread::id>& thread : ran) {
    threads.insert(thread.get());
  }
  loops.finish();
  EXPECT_EQ(2u, threads.size());
  EXPECT_EQ(0u, threads.count(std::this_thread::get_id()));
}

// finish() lets each loop run what it was given before its thread is
// joined, and a second call does nothing.
TEST(ServerLoops, FinishRunsWhatEachLoopWasGiven) {
  asio::io_service main;
  ServerLoops loops(main, 3);
  loops.next();
  asio::io_service& first = loops.next();
  asio::io_service& second = loops.next();
  std::atomic<int> handled{0};
  const int EACH = 1000;
  for (int handler = 0; handler < EACH; ++handler) {
    asio::post(first, [&handled] { handled.fetch_add(1); });
    asio::post(second, [&handled] { handled.fetch_add(1); });
  }
  loops.start();
  loops.finish();
  EXPECT_EQ(2 * EACH, handled.load());
  loops.finish();
}

// A handler that throws is logged, and the loop runs the next one.
TEST(ServerLoops, KeepsALoopGoingWhenAHandlerThrows) {
  asio::io_service main;
  ServerLoops loops(main, 2);
  loops.next();
  asio::io_service& other = loops.next();
  std::atomic<int> handled{0};
  asio::post(other, [] { throw std::runtime_error("a handler's fault"); });
  asio::post(other, [&handled] { handled.fetch_add(1); });
  loops.start();
  loops.finish();
  EXPECT_EQ(1, handled.load());
}

// Every loop's reactor exists once the loops do: the first timer and the
// first socket of a loop open no descriptor. Made at a loop's first
// connection instead, the reactor's three descriptors were what an accept
// at the descriptor limit could not have.
TEST(ServerLoops, MakesEachLoopsReactorWithTheLoop) {
  asio::io_service main;
  asio::steady_timer main_reactor(main);
  ServerLoops loops(main, 3);
  const int before = open_descriptors();
  ASSERT_GT(before, 0);
  std::vector<std::unique_ptr<asio::steady_timer>> timers;
  std::vector<std::unique_ptr<asio::ip::tcp::socket>> sockets;
  for (int loop = 0; loop < 3; ++loop) {
    asio::io_service& io_service = loops.next();
    timers.emplace_back(new asio::steady_timer(io_service));
    sockets.emplace_back(new asio::ip::tcp::socket(io_service));
  }
  EXPECT_EQ(before, open_descriptors());
}

}  // namespace
