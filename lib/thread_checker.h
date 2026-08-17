// mx::ThreadChecker and MX_DCHECK_RUN_ON: assert that code runs on the
// thread that owns an object, the way WebRTC's SequenceChecker and
// RTC_DCHECK_RUN_ON do.
//
//   class Connection {
//     void on_read() { MX_DCHECK_RUN_ON(&io_thread_); state_ = ...; }
//     mx::ThreadChecker io_thread_;
//     State state_ MX_GUARDED_BY(io_thread_);
//   };
//
// At run time, in builds without NDEBUG, MX_DCHECK_RUN_ON fails an assertion
// when called from another thread. At compile time under --config=clang the
// checker is a capability, so members guarded by it may only be touched in
// a scope that ran MX_DCHECK_RUN_ON or in a function declared
// MX_RUN_ON(&checker). Both cost nothing in release builds beyond a thread
// id compare that the optimizer removes with the assertion.
//
// A checker binds to the thread that constructs it, or, with BIND_LATER,
// to the first thread that checks it; detach() lets another thread take
// over, and bind_to_current() binds explicitly, for objects handed from a
// constructing thread to a working one. Note that a negative check such as
// DbgAssert(!checker.is_current()) also binds an unbound checker, so bind
// explicitly before any such check can run.
#ifndef MX_LIB_THREAD_CHECKER_H_
#define MX_LIB_THREAD_CHECKER_H_

#include <mutex>
#include <thread>

#include "lib/assertion.h"
#include "lib/preproc/common.h"
#include "lib/thread_annotations.h"

namespace mx {

class MX_CAPABILITY("thread") ThreadChecker {
public:
  enum Binding { BIND_NOW, BIND_LATER };
  explicit ThreadChecker(Binding binding = BIND_NOW)
      : owner_(binding == BIND_NOW ? std::this_thread::get_id() : std::thread::id()) {}

  // True on the owning thread. An unbound checker binds to the caller.
  bool is_current() const {
    std::lock_guard<std::mutex> guard(mutex_);
    if (owner_ == std::thread::id())
      owner_ = std::this_thread::get_id();
    return owner_ == std::this_thread::get_id();
  }

  // Forget the owner; the next is_current() binds to its caller.
  void detach() {
    std::lock_guard<std::mutex> guard(mutex_);
    owner_ = std::thread::id();
  }

  // Make the calling thread the owner, whatever the state. For an object
  // that starts its own thread and hands itself to it.
  void bind_to_current() {
    std::lock_guard<std::mutex> guard(mutex_);
    owner_ = std::this_thread::get_id();
  }

private:
  mutable std::mutex mutex_;
  mutable std::thread::id owner_;
};

// Tells the static analysis that the checker's capability is held for the
// rest of the scope; the run-time check is in the macro below.
class MX_SCOPED_CAPABILITY ThreadCheckerScope {
public:
  explicit ThreadCheckerScope(const ThreadChecker *checker) MX_ACQUIRE(checker) { (void)checker; }
  ~ThreadCheckerScope() MX_RELEASE() {}
};

} // namespace mx

// Use as the first statement of a function that must run on the checker's
// thread. `x` is a pointer to a ThreadChecker.
#define MX_DCHECK_RUN_ON(x)                                                                                            \
  ::mx::ThreadCheckerScope MX_UNIQUE_NAME(_mx_run_on_)(x);                                                             \
  DbgAssertMsg((x)->is_current(), "called on a thread other than the one that owns this object")

// On a function declaration: it may only be called on the checker's thread.
#define MX_RUN_ON(x) MX_REQUIRES(x)

#endif // MX_LIB_THREAD_CHECKER_H_
