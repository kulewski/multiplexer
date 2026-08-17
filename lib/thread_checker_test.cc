// Unit tests for lib/thread_checker.h and lib/mutex.h.
#include <thread>

#include <gtest/gtest.h>

#include "lib/mutex.h"
#include "lib/thread_checker.h"

using mx::Mutex;
using mx::MutexLock;
using mx::ThreadChecker;

TEST(ThreadChecker, BindsToConstructingThread) {
  ThreadChecker checker;
  EXPECT_TRUE(checker.is_current());
  bool other = true;
  std::thread([&] { other = checker.is_current(); }).join();
  EXPECT_FALSE(other);
}

TEST(ThreadChecker, BindLaterBindsToFirstCaller) {
  ThreadChecker checker(ThreadChecker::BIND_LATER);
  bool other = false;
  std::thread([&] { other = checker.is_current(); }).join();
  EXPECT_TRUE(other);
  EXPECT_FALSE(checker.is_current());
}

TEST(ThreadChecker, DetachLetsAnotherThreadTakeOver) {
  ThreadChecker checker;
  checker.detach();
  bool other = false;
  std::thread([&] { other = checker.is_current(); }).join();
  EXPECT_TRUE(other);
}

TEST(ThreadChecker, BindToCurrentTakesOverExplicitly) {
  ThreadChecker checker;
  bool other = false;
  std::thread([&] {
    checker.bind_to_current();
    other = checker.is_current();
  }).join();
  EXPECT_TRUE(other);
  EXPECT_FALSE(checker.is_current());
}

TEST(ThreadChecker, DcheckRunOnPassesOnOwner) {
  struct Owner {
    void touch() {
      MX_DCHECK_RUN_ON(&checker);
      ++count;
    }
    ThreadChecker checker;
    int count MX_GUARDED_BY(checker) = 0;
  } owner;
  owner.touch();
  bool threw = false;
  std::thread([&] {
    try {
      owner.touch();
    } catch (const mx::AssertionError &) {
      threw = true;
    }
  }).join();
#ifdef NDEBUG
  EXPECT_FALSE(threw) << "the check is compiled out with NDEBUG";
#else
  EXPECT_TRUE(threw);
#endif
}

TEST(Mutex, GuardsACounter) {
  struct Counter {
    void add() MX_EXCLUDES(mu) {
      MutexLock lock(mu);
      ++value;
    }
    Mutex mu;
    int value MX_GUARDED_BY(mu) = 0;
  } counter;
  std::thread first([&] {
    for (int index = 0; index < 1000; ++index)
      counter.add();
  });
  std::thread second([&] {
    for (int index = 0; index < 1000; ++index)
      counter.add();
  });
  first.join();
  second.join();
  MutexLock lock(counter.mu);
  EXPECT_EQ(2000, counter.value);
}
