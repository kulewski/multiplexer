// mx::AdaptiveMutex keeps threads out of each other's critical sections,
// whether a waiter gets the mutex while it spins or after it blocked:
// counted, not timed. Threads add to a plain counter under the mutex, and
// the total is exact; a waiter whose spins are 0 blocks at once, and takes
// the mutex only once the holder let go. Under --config=tsan the sanitizer
// checks the same for every access.
#include "lib/adaptive_mutex.h"

#include <gtest/gtest.h>

#include <atomic>
#include <mutex>
#include <thread>
#include <vector>

namespace {

const int THREADS = 8;
const int ROUNDS = 20000;  // per thread

// THREADS threads, each adding 1 ROUNDS times to a counter of no
// synchronization of its own, under `mutex`: the total each round's.
void count_under(mx::AdaptiveMutex& mutex) {
  long counter = 0;
  std::vector<std::thread> threads;
  for (int thread = 0; thread < THREADS; ++thread) {
    threads.emplace_back([&mutex, &counter] {
      for (int round = 0; round < ROUNDS; ++round) {
        mx::AdaptiveMutexLock lock(mutex);
        ++counter;
      }
    });
  }
  for (std::thread& thread : threads) {
    thread.join();
  }
  EXPECT_EQ(static_cast<long>(THREADS) * ROUNDS, counter);
}

TEST(AdaptiveMutex, KeepsThreadsApartWhileSpinning) {
  mx::AdaptiveMutex mutex;
  count_under(mutex);
}

TEST(AdaptiveMutex, KeepsThreadsApartWhenEveryWaiterBlocks) {
  mx::AdaptiveMutex mutex(/*spins=*/0);
  count_under(mutex);
}

TEST(AdaptiveMutex, TryLockFailsWhileAnotherThreadHoldsIt) {
  mx::AdaptiveMutex mutex;
  mutex.lock();
  bool taken = true;
  std::thread([&mutex, &taken] {
    taken = false;
    if (mutex.try_lock()) {
      taken = true;
      mutex.unlock();
    }
  }).join();
  EXPECT_FALSE(taken);
  mutex.unlock();
  std::thread([&mutex, &taken] {
    taken = false;
    if (mutex.try_lock()) {
      taken = true;
      mutex.unlock();
    }
  }).join();
  EXPECT_TRUE(taken);
}

// A waiter that does not spin blocks in lock() while the holder holds the
// mutex, and returns once it let go: what it saw of the holder's work is
// all of it.
TEST(AdaptiveMutex, BlockedWaiterTakesItAfterTheHolder) {
  mx::AdaptiveMutex mutex(/*spins=*/0);
  std::atomic<bool> waiting{false};
  int written = 0;  // the holder's, under the mutex
  int seen = -1;    // what the waiter read under it
  mutex.lock();
  std::thread waiter([&] {
    waiting.store(true);
    std::lock_guard<mx::AdaptiveMutex> lock(mutex);  // a BasicLockable
    seen = written;
  });
  while (!waiting.load()) {
    std::this_thread::yield();
  }
  written = 42;
  mutex.unlock();
  waiter.join();
  EXPECT_EQ(42, seen);
}

}  // namespace
