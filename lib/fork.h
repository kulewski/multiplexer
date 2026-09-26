// mx::fork_generation(): how many times this process is a forked child of
// the process that loaded the library, bumped by a pthread_atfork child
// handler. An object records the generation it was created under and
// compares later: a mismatch means the object was inherited across a
// fork, where its threads do not exist and its locks may be held by
// nobody, and it must not be used. One load per check, nothing at all when
// nobody forks, and it covers every fork, not only the ones Python makes.
#ifndef MX_LIB_FORK_H_
#define MX_LIB_FORK_H_

#include <atomic>

namespace mx {

namespace fork_internal {
// The count fork_generation() reads, bumped by fork.cc's child handler;
// here so that a check is a load, inlined, not a call.
extern std::atomic<unsigned int> generation;
}  // namespace fork_internal

inline unsigned int fork_generation() { return fork_internal::generation.load(std::memory_order_relaxed); }

}  // namespace mx

#endif  // MX_LIB_FORK_H_
