// ThreadedClientInbox: what the callers' threads hand a ThreadedClient's io
// thread, in the order they handed it over, which the io thread takes a
// batch at a time (ThreadedClient::Core::_drain, threaded_client_inbox.cc).
// Only the hand-over that finds no wake-up pending wakes the io thread, so
// a caller that sends many messages while the io thread is busy wakes it
// once, not once per message.
//
// A call is a function to run there. A plain send, an event nobody follows
// sent while something is batched, comes as its bytes (PlainSend) rather
// than as a function and a frame, and the bodies the batching is done with
// come back the other way, for the callers to serialize their next
// messages into: in steady state no thread frees what another allocated,
// which glibc's malloc makes costly (a block goes back to the arena that
// allocated it, under that arena's lock, which the allocating thread takes
// too). The bodies waiting to be taken stay within SpentBodies' limits;
// the rest are freed by the io thread.
//
// Thread-safe: every member is guarded by the inbox's own lock, held for a
// push or a take only. Nothing here knows asio; the Core posts the wake-up,
// in a block of its own (WakeMemory).
#ifndef MX_MULTIPLEXER_THREADED_CLIENT_INBOX_H_
#define MX_MULTIPLEXER_THREADED_CLIENT_INBOX_H_

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <new>
#include <string>
#include <vector>

#include "lib/mutex.h"
#include "multiplexer/spent_bodies.h"

namespace multiplexer {

// A plain send as the inbox carries it: the serialized message, its id
// and type, and its deadline, from the send. `read`: the client read the
// message as it serialized it, so it is a plain event (RawMessage::
// plain_event), keyed or not, its key the last `key_bytes` of `body`, after
// the message, so that no string of its own goes from the thread that made
// it to the io thread; else bytes the program gave, which the batching
// reads (BasicClient::send_bytes).
struct PlainSend {
  std::string body;
  std::uint64_t id = 0;
  std::uint32_t type = 0;
  bool read = false;
  bool keyed = false;
  std::size_t key_bytes = 0;
  std::chrono::steady_clock::time_point deadline;
};

class ThreadedClientInbox {
 public:
  // One thing handed over: a call to run on the io thread, or, with none,
  // a plain send.
  struct Entry {
    std::function<void()> call;
    PlainSend plain;
  };

  // `call` appended. True when the io thread is to be woken for it: no
  // wake-up was pending.
  bool push(std::function<void()> call) MX_EXCLUDES(mutex_);
  // `plain` appended, its body swapped out of `body`, which takes a body
  // handed back, empty, to serialize the next message into, when one
  // waits. True as push().
  bool push_plain(PlainSend* plain, std::string* body) MX_EXCLUDES(mutex_);
  // The io thread's: everything handed over so far, moved into `taken`,
  // which must be empty and whose room the inbox keeps for what comes
  // next; and `spent`, the bodies the batching is done with, handed back
  // as far as SpentBodies keeps them, the others left in it for the caller
  // to free (SpentBodies::clear), outside the lock. The next push wakes the
  // io thread.
  void take(std::vector<Entry>* taken, SpentBodies* spent) MX_EXCLUDES(mutex_);
  // How many bodies wait to be taken, and their capacity together; for
  // tests.
  std::size_t returned_count() MX_EXCLUDES(mutex_);
  std::size_t returned_bytes() MX_EXCLUDES(mutex_);

 private:
  // The push's own part: true when this one wakes the io thread.
  bool _wake() MX_REQUIRES(mutex_);

  mx::Mutex mutex_;
  std::vector<Entry> entries_ MX_GUARDED_BY(mutex_);
  bool wake_pending_ MX_GUARDED_BY(mutex_) = false;  // woken for entries_, which it has not taken yet
  SpentBodies returned_ MX_GUARDED_BY(mutex_);       // handed back, for the callers to take
};

// The block the io thread's wake-up handler is posted in: at most one
// wake-up is pending at a time (ThreadedClientInbox's wake_pending_), so
// one block serves them all, and a wake allocates nothing, where asio would
// allocate the handler on the thread that wakes the io thread and free it
// on the io thread. A handler bigger than the block, or one asked for while
// the block is taken, takes the heap. Thread-safe: taken on the waking
// thread, given back on the io thread.
class WakeMemory {
 public:
  static constexpr std::size_t BYTES = 256;
  void* allocate(std::size_t size) {
    if (size <= BYTES && !taken_.exchange(true, std::memory_order_acquire)) {
      return storage_;
    }
    return ::operator new(size);
  }
  void deallocate(void* memory) noexcept {
    if (memory == storage_) {
      taken_.store(false, std::memory_order_release);
      return;
    }
    ::operator delete(memory);
  }

 private:
  alignas(std::max_align_t) unsigned char storage_[BYTES];
  std::atomic<bool> taken_{false};
};

// The allocator a wake-up handler is posted with (asio's associated
// allocator), every copy of it allocating from one WakeMemory.
template <typename T>
struct WakeAllocator {
  typedef T value_type;
  explicit WakeAllocator(WakeMemory* memory) noexcept : memory(memory) {}
  template <typename U>
  WakeAllocator(const WakeAllocator<U>& other) noexcept : memory(other.memory) {}
  T* allocate(std::size_t count) { return static_cast<T*>(memory->allocate(sizeof(T) * count)); }
  void deallocate(T* pointer, std::size_t) noexcept { memory->deallocate(pointer); }
  template <typename U>
  bool operator==(const WakeAllocator<U>& other) const noexcept {
    return memory == other.memory;
  }
  template <typename U>
  bool operator!=(const WakeAllocator<U>& other) const noexcept {
    return memory != other.memory;
  }
  WakeMemory* memory;
};

}  // namespace multiplexer

#endif  // MX_MULTIPLEXER_THREADED_CLIENT_INBOX_H_
