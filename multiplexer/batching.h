// BasicClient's batching: the deliveries set, and the batches open.
// Internal to basic_client.cc and basic_client_batching.cc, which describes
// the design; the rules are at BasicClient::Delivery.
#ifndef MX_MULTIPLEXER_BATCHING_H_
#define MX_MULTIPLEXER_BATCHING_H_

#include <asio/steady_timer.hpp>
#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "lib/mutex.h"
#include "multiplexer/basic_client.h"

namespace multiplexer {

// The messages that go in one batch: of one type, with one key or none.
struct BatchGroup {
  std::uint32_t type = 0;
  bool keyed = false;
  std::string key;
};

// One message in a batch: the frame it would have gone as, or, sent as
// its bytes alone (send_plain, send_bytes), those bytes and its id, with no
// frame until one is needed (_record_frame); the tracker the program holds
// when it asked for one, and its `done`. Its time is its batch's.
struct BasicClient::BatchRecord {
  std::shared_ptr<const RawMessage> raw;  // null for bytes alone
  std::string body;                       // the bytes, when `raw` is null
  std::uint64_t id = 0;                   // their message's id
  std::shared_ptr<SendState> state;
  SendCallback done;
  // Its number, for a confirmed send (Delivery::confirm), from when its
  // first batch became a frame, kept when it goes again; 0 before.
  std::uint64_t number = 0;
};

// The buffers of batch frames that were written, for the next batches'
// frames (BasicClient::_batch_frame): a buffer back from a frame keeps its
// size, so serializing a frame of about the same size into it fills no
// bytes first, as a new string's resize does, and touches no page the
// process has not used. At most FRAME_BUFFERS_KEPT, each up to
// FRAME_BUFFER_BYTES_KEPT. Shared with the frames, which give their buffer
// back where their last holder lets go of them, hence the lock.
struct BatchFramePool {
  static constexpr std::size_t FRAME_BUFFERS_KEPT = 4;
  static constexpr std::size_t FRAME_BUFFER_BYTES_KEPT = 4 * 1024 * 1024;
  mx::Mutex mutex;
  std::vector<std::string> spare MX_GUARDED_BY(mutex);
};

// One batch open: its group, how it goes, and its messages so far.
struct BasicClient::Batch {
  typedef BatchRecord Record;
  std::uint64_t began = 0;  // its place in the order batches began
  BatchGroup group;
  Delivery delivery;
  std::vector<Record> records;
  std::size_t bytes = 0;  // the messages' serialized forms together: the least the batch's message holds
  // When what is not written of it, or with `confirm` not confirmed, is
  // given up on: its timeout (Delivery::timeout) from its first message,
  // which a batch sent again keeps; max() for none.
  std::chrono::steady_clock::time_point deadline = std::chrono::steady_clock::time_point::max();
  // With no delay a batch waits for a connection with nothing left to
  // write, a wait for room, which its deadline bounds: `due` is the
  // deadline, and the batch goes by it at the latest. With a delay `due` is
  // when the delay is over. max() for no time at all.
  bool waits_for_idle = false;
  std::chrono::steady_clock::time_point due = std::chrono::steady_clock::time_point::max();
  bool closed = false;
  // A confirmed batch's messages sent again: the back-off their batch had
  // reached after DELIVERY_ERRORs (Confirming::backoff), which goes on.
  float backoff = 0;
};

struct BasicClient::Batcher {
  explicit Batcher(asio::io_service& io_service) : timer(io_service) {}

  // What set_delivery() set: per type, and per key of a type, a message's
  // key looked up as it is, never copied for the look-up.
  std::unordered_map<std::uint32_t, Delivery> by_type;
  std::unordered_map<std::uint32_t, std::map<std::string, Delivery>> by_key;
  bool configured() const { return !by_type.empty() || !by_key.empty(); }
  // Where a frame made of bytes is read (_delivery_for): one message,
  // reused, keeps its strings' room from one read to the next, which a new
  // one would allocate each time, a third of the read.
  MultiplexerMessage scratch;
  // Where every batch is made (_send_batch), kept: its messages' strings,
  // emptied once a frame is made, take the next batch's bodies without an
  // allocation each.
  BatchEnvelope envelope;
  std::shared_ptr<BatchFramePool> frame_pool = std::make_shared<BatchFramePool>();

  // The batches open, in the order they began, and found by group: per
  // type, its keyless one and its keyed ones by key.
  struct OfType {
    BatchPtr keyless;
    std::map<std::string, BatchPtr> keyed;
  };
  std::unordered_map<std::uint32_t, OfType> by_group;
  // The batches open by the order they began, each step a look-up whatever
  // their number; of those, the ones that wait for a connection with
  // nothing left to write, max_delay 0, and the others by when their delay
  // is over, one timer running to the first of them.
  std::map<std::uint64_t, BatchPtr> open;
  std::set<std::uint64_t> waiting_for_idle;
  std::set<std::pair<std::chrono::steady_clock::time_point, std::uint64_t>> by_due;
  std::uint64_t began = 0;
  asio::steady_timer timer;
  std::chrono::steady_clock::time_point armed = std::chrono::steady_clock::time_point::max();

  // The open batch of a group, or null.
  BatchPtr find(std::uint32_t type, bool keyed, const std::string& key) const {
    std::unordered_map<std::uint32_t, OfType>::const_iterator of_type = by_group.find(type);
    if (of_type == by_group.end()) {
      return BatchPtr();
    }
    if (!keyed) {
      return of_type->second.keyless;
    }
    std::map<std::string, BatchPtr>::const_iterator found = of_type->second.keyed.find(key);
    return found != of_type->second.keyed.end() ? found->second : BatchPtr();
  }
};

}  // namespace multiplexer

#endif  // MX_MULTIPLEXER_BATCHING_H_
