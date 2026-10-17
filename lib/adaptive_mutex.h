// mx::AdaptiveMutex: a mutex that spins a little before it blocks, for
// critical sections a few dozen nanoseconds long that several threads take
// often: a thread that finds it held tries again for a while, longer than
// the sections it guards, before it sleeps in the kernel, which would cost
// it a wake-up and the holder a system call at its unlock. The routing
// lock and the connections' send locks of a multiplexer on several event
// loops (multiplexer/server.h, multiplexer/io/shared_outgoing.h).
//
// Annotated as mx::Mutex is (lib/mutex.h), so that members declared
// MX_GUARDED_BY one are checked under --config=clang; a BasicLockable, so
// that std::lock_guard takes it too. A waiter reads a flag of the mutex's
// own before each try, so that the waiters share the cache line and the
// holder keeps it, where each try, a compare-and-swap, would take it from
// the holder. Portable: std::mutex underneath, no futex of its own.
#ifndef MX_LIB_ADAPTIVE_MUTEX_H_
#define MX_LIB_ADAPTIVE_MUTEX_H_

#include <atomic>
#include <mutex>

#include "lib/thread_annotations.h"

namespace mx {

class MX_CAPABILITY("mutex") AdaptiveMutex {
 public:
  // How many times lock() tries again before it blocks: with a pause each,
  // about a few microseconds, longer than the sections it guards.
  static constexpr int DEFAULT_SPINS = 100;

  // A mutex whose lock() tries `spins` times before it blocks; 0 blocks at
  // once, as a test of the blocking path asks.
  explicit AdaptiveMutex(int spins = DEFAULT_SPINS) : spins_(spins) {}
  AdaptiveMutex(const AdaptiveMutex&) = delete;
  AdaptiveMutex& operator=(const AdaptiveMutex&) = delete;

  // Takes the mutex: at once when it is free; else, `spins` times, a pause
  // and another try once the holder let go; else by blocking until it is
  // free.
  void lock() MX_ACQUIRE() {
    if (try_lock()) {
      return;
    }
    for (int spin = 0; spin < spins_; ++spin) {
      _pause();
      if (!held_.load(std::memory_order_relaxed) && try_lock()) {
        return;
      }
    }
    mutex_.lock();
    held_.store(true, std::memory_order_relaxed);
  }

  // Lets go of the mutex.
  void unlock() MX_RELEASE() {
    held_.store(false, std::memory_order_relaxed);
    mutex_.unlock();
  }

  // Takes the mutex if it is free, without waiting: whether it did.
  bool try_lock() MX_TRY_ACQUIRE(true) {
    if (!mutex_.try_lock()) {
      return false;
    }
    held_.store(true, std::memory_order_relaxed);
    return true;
  }

 private:
  // Tells the processor this thread is spinning: the other hyperthread of
  // its core runs meanwhile, and leaving the loop does not flush the
  // pipeline.
  static void _pause() {
#if defined(__x86_64__) || defined(__i386__)
    __builtin_ia32_pause();
#elif defined(__aarch64__)
    asm volatile("yield");
#endif
  }

  std::mutex mutex_;
  // Whether a thread holds mutex_: a hint for the waiters alone, read
  // before a try; mutex_ decides.
  std::atomic<bool> held_{false};
  const int spins_;
};

// Holds an AdaptiveMutex for its scope.
class MX_SCOPED_CAPABILITY AdaptiveMutexLock {
 public:
  explicit AdaptiveMutexLock(AdaptiveMutex& mutex) MX_ACQUIRE(mutex) : mutex_(mutex) { mutex_.lock(); }
  ~AdaptiveMutexLock() MX_RELEASE() { mutex_.unlock(); }
  AdaptiveMutexLock(const AdaptiveMutexLock&) = delete;
  AdaptiveMutexLock& operator=(const AdaptiveMutexLock&) = delete;

 private:
  AdaptiveMutex& mutex_;
};

}  // namespace mx

#endif  // MX_LIB_ADAPTIVE_MUTEX_H_
