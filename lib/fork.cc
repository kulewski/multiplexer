// See fork.h. The child handler only bumps a counter: after fork the child
// has one thread and any mutex another thread held is locked forever, so
// nothing else is safe to do there.
#include "lib/fork.h"

#include <pthread.h>

#include <atomic>

namespace mx {

std::atomic<unsigned int> fork_internal::generation{0};

namespace {

void in_child() { fork_internal::generation.fetch_add(1, std::memory_order_relaxed); }

// Registered as this file is loaded, which every reader of the count makes
// it be: the count is defined here.
struct Registration {
  Registration() { pthread_atfork(nullptr, nullptr, in_child); }
} registration;

}  // namespace

}  // namespace mx
