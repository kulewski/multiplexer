// SpanSet: a bounded set of recently seen values.
#ifndef MX_LIB_SPANSET_H_
#define MX_LIB_SPANSET_H_

#include <deque>
#include <set>

#include "lib/assertion.h"

namespace mx {

// A set that remembers only the last N elements inserted: insert() returns
// whether the element is new, and the oldest one is forgotten when the set
// is full. The client uses it to drop copies of a message that arrived
// through several multiplexers, keyed by message id. O(log N) per insert.
template <typename T,    // the type of elements held in the set
          int N = 2048,  // number of elements remembered
          typename InnerSetImpl = std::set<T>
          // the type of subset for checking existence
          >
struct SpanSet {
  SpanSet() {}

  bool inline insert(const T& t) {
    DbgAssert(queue_.size() == set_.size());
    if (set_.insert(t).second) {
      if (queue_.size() == N) {
        set_.erase(queue_.front());
        queue_.pop_front();
      }
      queue_.push_back(t);
      DbgAssert(queue_.size() == set_.size());
      return true;
    }
    DbgAssert(queue_.size() == set_.size());
    return false;
  }

  // Forgets the element the last insert() that returned true added: one
  // its caller did not keep after all, whose next copy is new again. Only
  // right after that insert; the oldest element it may have pushed out is
  // not brought back.
  void inline forget_last() {
    DbgAssert(!queue_.empty());
    set_.erase(queue_.back());
    queue_.pop_back();
  }

 private:
  InnerSetImpl set_;
  std::deque<T> queue_;
};  // struct SpanSet
};  // namespace mx

#endif  // MX_LIB_SPANSET_H_
