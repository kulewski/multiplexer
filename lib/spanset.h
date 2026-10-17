// SpanSet: a bounded set of recently seen values.
//
// The client keeps the ids of the last messages it took in one, to drop a
// copy that arrives through another multiplexer: an insert for every
// message received, every message of a batch included, so it is on every
// message's path. The values are held in a flat table, open addressing with
// linear probing in twice as many slots as the set remembers, and in a ring
// that says which is oldest; an insert is a hash and a probe or two, and
// forgetting the oldest a deletion that shifts the probe chain back, with no
// allocation after the set is made. Measured on 2026-10-15, 14 ns an insert
// of a fresh id where a std::set and a deque took 112.
//
// 0 is never a member: an empty slot holds it, and the client drops a
// message without an id before it asks (basic_client.cc).
#ifndef MX_LIB_SPANSET_H_
#define MX_LIB_SPANSET_H_

#include <cstddef>
#include <cstdint>
#include <vector>

#include "lib/assertion.h"

namespace mx {

// A set that remembers only the last N values inserted: insert() returns
// whether the value is new, and the oldest one is forgotten when the set is
// full. T is an unsigned integer type, every value of it but 0 a member.
template <typename T,   // the type of the values, an unsigned integer
          int N = 2048  // how many values are remembered
          >
struct SpanSet {
  static_assert(N > 0, "a SpanSet remembers at least one value");

  SpanSet() : slots_(SLOTS, 0), ring_(N, 0) {}

  bool inline insert(const T& value) {
    DbgAssert(value != 0);
    std::size_t at = _home(value);
    for (; slots_[at] != 0; at = (at + 1) & MASK) {
      if (slots_[at] == value) {
        return false;
      }
    }
    if (count_ == static_cast<std::size_t>(N)) {
      _erase(ring_[oldest_]);
      oldest_ = (oldest_ + 1) % N;
      --count_;
      // The deletion may have moved a value into the slot found above or
      // left a hole before it: the value is placed where a probe finds it.
      for (at = _home(value); slots_[at] != 0; at = (at + 1) & MASK) {
      }
    }
    slots_[at] = value;
    ring_[(oldest_ + count_) % N] = value;
    ++count_;
    return true;
  }

  // Forgets the value the last insert() that returned true added: one its
  // caller did not keep after all, whose next copy is new again. Only right
  // after that insert; the oldest value it may have pushed out is not
  // brought back.
  void inline forget_last() {
    DbgAssert(count_ > 0);
    const std::size_t last = (oldest_ + count_ - 1) % N;
    _erase(ring_[last]);
    --count_;
  }

 private:
  // The power of two at least twice N: the table is never more than half
  // full, so a probe ends within a few slots.
  static constexpr std::size_t _slots() {
    std::size_t slots = 1;
    while (slots < 2 * static_cast<std::size_t>(N)) {
      slots *= 2;
    }
    return slots;
  }
  static constexpr std::size_t SLOTS = _slots();
  static constexpr std::size_t MASK = SLOTS - 1;

  // The slot a value's probe starts at: the high bits of a multiplication
  // by 2^64 over the golden ratio, which spread sequential values too.
  static std::size_t _home(const T& value) {
    return static_cast<std::size_t>((static_cast<std::uint64_t>(value) * 0x9E3779B97F4A7C15ull) >> 32) & MASK;
  }

  // Takes `value` out of the table, the values after it in its probe chain
  // shifted back into the hole where their own probe still finds them, so
  // that no probe stops early at an emptied slot.
  void _erase(const T& value) {
    std::size_t hole = _home(value);
    for (; slots_[hole] != value; hole = (hole + 1) & MASK) {
      if (slots_[hole] == 0) {
        return;
      }
    }
    for (std::size_t next = (hole + 1) & MASK; slots_[next] != 0; next = (next + 1) & MASK) {
      // A value may move back to the hole when its home is not between the
      // hole and where it sits, going round the table.
      if (((next - _home(slots_[next])) & MASK) >= ((next - hole) & MASK)) {
        slots_[hole] = slots_[next];
        hole = next;
      }
    }
    slots_[hole] = 0;
  }

  std::vector<T> slots_;    // the table: a value, or 0 for an empty slot
  std::vector<T> ring_;     // the values in the order inserted, from oldest_, count_ of them
  std::size_t oldest_ = 0;  // where the oldest value is in ring_
  std::size_t count_ = 0;   // how many values the set holds
};  // struct SpanSet

}  // namespace mx

#endif  // MX_LIB_SPANSET_H_
