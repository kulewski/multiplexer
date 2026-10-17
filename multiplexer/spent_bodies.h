// SpentBodies: message bodies a client's batching is done with, once their
// batch's frame is made (BasicClient::keep_spent_bodies), kept for the
// client to serialize its next messages into rather than freed: a
// SyncClient's queue() takes one per message, and a ThreadedClient's io
// thread hands them back through its inbox to the threads that sent them
// (threaded_client_inbox.h), so that no thread frees what another
// allocated. A body bigger than BODY_BYTES_KEPT is not kept, and those kept
// stay within BYTES_KEPT together: a client that stops sending such
// messages holds no more than that.
//
// Not thread-safe: its owner's thread, or its owner's lock.
#ifndef MX_MULTIPLEXER_SPENT_BODIES_H_
#define MX_MULTIPLEXER_SPENT_BODIES_H_

#include <cstddef>
#include <string>
#include <utility>
#include <vector>

namespace multiplexer {

class SpentBodies {
 public:
  static constexpr std::size_t BODY_BYTES_KEPT = 64 * 1024;
  static constexpr std::size_t BYTES_KEPT = 16 * 1024 * 1024;

  // `body` moved in, left empty, when it has room of its own, beyond what
  // a string holds inline, which reusing saves nothing of, is small enough
  // and there is room; else left as it is, for the caller to free. True
  // when kept.
  bool keep(std::string* body) {
    const std::size_t capacity = body->capacity();
    if (capacity <= INLINE_BYTES || capacity > BODY_BYTES_KEPT || bytes_ + capacity > BYTES_KEPT) {
      return false;
    }
    bytes_ += capacity;
    bodies_.push_back(std::move(*body));
    body->clear();
    return true;
  }
  // A kept body swapped into `body`, which goes in its place and is
  // dropped, an empty string; false, `body` unchanged, when none is kept.
  bool take(std::string* body) {
    if (bodies_.empty()) {
      return false;
    }
    body->swap(bodies_.back());
    bodies_.pop_back();
    bytes_ -= body->capacity();
    return true;
  }
  // Every body of `other` kept as far as there is room. Those that are not
  // stay in `other`, among the emptied strings of those that were, for its
  // owner to free with clear() outside any lock the call is made under.
  void keep_all(SpentBodies* other) {
    std::size_t left = 0;
    for (std::string& body : other->bodies_) {
      if (!keep(&body)) {
        left += body.capacity();
      }
    }
    other->bytes_ = left;
  }
  // Every body let go of.
  void clear() {
    bodies_.clear();
    bytes_ = 0;
  }
  std::size_t count() const { return bodies_.size(); }
  std::size_t bytes() const { return bytes_; }

 private:
  static inline const std::size_t INLINE_BYTES = std::string().capacity();  // a string's room without a block
  std::vector<std::string> bodies_;
  std::size_t bytes_ = 0;  // the capacity of bodies_ together
};

}  // namespace multiplexer

#endif  // MX_MULTIPLEXER_SPENT_BODIES_H_
