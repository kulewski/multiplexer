// Clang thread-safety annotations, in the style of WebRTC's RTC_GUARDED_BY
// and friends. They are checked by `bazel build --config=clang`, which turns
// on -Wthread-safety; under GCC they expand to nothing. Use them on every
// member that a mutex or a thread checker protects, and on every function
// that must be called with one held: the analysis then proves, at compile
// time, that no access happens without it.
//
//   mx::Mutex mu_;
//   std::deque<Item> queue_ MX_GUARDED_BY(mu_);
//   void push(Item) MX_EXCLUDES(mu_);        // takes mu_ itself
//   void drain_locked() MX_REQUIRES(mu_);    // caller holds mu_
//
//   mx::ThreadChecker io_thread_;
//   State state_ MX_GUARDED_BY(io_thread_);
//   void on_read() { MX_DCHECK_RUN_ON(&io_thread_); ... }   // thread_checker.h
//
// lib/mutex.h and lib/thread_checker.h provide the capabilities these refer to.
#ifndef MX_LIB_THREAD_ANNOTATIONS_H_
#define MX_LIB_THREAD_ANNOTATIONS_H_

#if defined(__clang__) && !defined(SWIG)
#define MX_THREAD_ANNOTATION_ATTRIBUTE(x) __attribute__((x))
#else
#define MX_THREAD_ANNOTATION_ATTRIBUTE(x)
#endif

// On a class: it is a lock-like capability, named for messages.
#define MX_CAPABILITY(x) MX_THREAD_ANNOTATION_ATTRIBUTE(capability(x))
// On a class: its lifetime holds a capability (a scoped lock).
#define MX_SCOPED_CAPABILITY MX_THREAD_ANNOTATION_ATTRIBUTE(scoped_lockable)
// On a member: may only be accessed with `x` held.
#define MX_GUARDED_BY(x) MX_THREAD_ANNOTATION_ATTRIBUTE(guarded_by(x))
// On a pointer member: what it points to may only be accessed with `x` held.
#define MX_PT_GUARDED_BY(x) MX_THREAD_ANNOTATION_ATTRIBUTE(pt_guarded_by(x))
// On a function: the caller must hold these.
#define MX_REQUIRES(...) MX_THREAD_ANNOTATION_ATTRIBUTE(requires_capability(__VA_ARGS__))
#define MX_REQUIRES_SHARED(...) MX_THREAD_ANNOTATION_ATTRIBUTE(requires_shared_capability(__VA_ARGS__))
// On a function: the caller must not hold these (the function takes them).
#define MX_EXCLUDES(...) MX_THREAD_ANNOTATION_ATTRIBUTE(locks_excluded(__VA_ARGS__))
// On a function: it acquires / releases these.
#define MX_ACQUIRE(...) MX_THREAD_ANNOTATION_ATTRIBUTE(acquire_capability(__VA_ARGS__))
#define MX_ACQUIRE_SHARED(...) MX_THREAD_ANNOTATION_ATTRIBUTE(acquire_shared_capability(__VA_ARGS__))
#define MX_RELEASE(...) MX_THREAD_ANNOTATION_ATTRIBUTE(release_capability(__VA_ARGS__))
#define MX_TRY_ACQUIRE(...) MX_THREAD_ANNOTATION_ATTRIBUTE(try_acquire_capability(__VA_ARGS__))
// On a function: it asserts, at run time, that these are held.
#define MX_ASSERT_CAPABILITY(x) MX_THREAD_ANNOTATION_ATTRIBUTE(assert_capability(x))
// On a function: skip the analysis (say why in a comment).
#define MX_NO_THREAD_SAFETY_ANALYSIS MX_THREAD_ANNOTATION_ATTRIBUTE(no_thread_safety_analysis)

#endif // MX_LIB_THREAD_ANNOTATIONS_H_
