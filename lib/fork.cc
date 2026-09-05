// See fork.h. The child handler only bumps a counter: after fork the child
// has one thread and any mutex another thread held is locked forever, so
// nothing else is safe to do there.
#include "lib/fork.h"

#include <atomic>
#include <pthread.h>

namespace mx {
namespace {

std::atomic<unsigned int> generation{0};

void in_child() { generation.fetch_add(1, std::memory_order_relaxed); }

struct Registration {
  Registration() { pthread_atfork(nullptr, nullptr, in_child); }
} registration;

} // namespace

unsigned int fork_generation() { return generation.load(std::memory_order_relaxed); }

} // namespace mx
