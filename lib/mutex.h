// mx::Mutex and mx::MutexLock: std::mutex with thread-safety annotations, so
// that members declared MX_GUARDED_BY(mu_) are checked under
// --config=clang. Mutex is a BasicLockable and works with
// std::condition_variable_any.
#ifndef MX_LIB_MUTEX_H_
#define MX_LIB_MUTEX_H_

#include <mutex>

#include "lib/thread_annotations.h"

namespace mx {

class MX_CAPABILITY("mutex") Mutex {
public:
  Mutex() = default;
  Mutex(const Mutex &) = delete;
  Mutex &operator=(const Mutex &) = delete;

  void lock() MX_ACQUIRE() { mutex_.lock(); }
  void unlock() MX_RELEASE() { mutex_.unlock(); }
  bool try_lock() MX_TRY_ACQUIRE(true) { return mutex_.try_lock(); }

private:
  std::mutex mutex_;
};

// Holds a Mutex for its scope.
class MX_SCOPED_CAPABILITY MutexLock {
public:
  explicit MutexLock(Mutex &mutex) MX_ACQUIRE(mutex) : mutex_(mutex) { mutex_.lock(); }
  ~MutexLock() MX_RELEASE() { mutex_.unlock(); }
  MutexLock(const MutexLock &) = delete;
  MutexLock &operator=(const MutexLock &) = delete;

private:
  Mutex &mutex_;
};

} // namespace mx

#endif // MX_LIB_MUTEX_H_
