// BasicClient's batching: what the program batches, and the batches open.
// Internal to basic_client.cc and basic_client_batching.cc, which describes
// the design; the rules are at BasicClient::set_batching.
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

#include "multiplexer/basic_client.h"

namespace multiplexer {

// The messages that go in one batch: of one type, with one key or none.
struct BatchGroup {
  std::uint32_t type = 0;
  bool keyed = false;
  std::string key;
};

// One batch open: its group, what it may hold, and its messages so far.
struct BasicClient::Batch {
  // One message: the frame it would have gone as, the tracker the program
  // holds when it asked for one, its `done`, its timeout, and, in a batch
  // that waits for a connection with nothing left to write, its deadline
  // from its send (see `waits_for_idle`).
  struct Record {
    std::shared_ptr<const RawMessage> raw;
    std::shared_ptr<SendState> state;
    SendCallback done;
    float timeout = 0;
    std::chrono::steady_clock::time_point deadline = std::chrono::steady_clock::time_point::max();
  };
  std::uint64_t began = 0;  // its place in the order batches began
  BatchGroup group;
  Batching batching;
  std::vector<Record> records;
  std::size_t bytes = 0;  // the messages' serialized forms together: the least the batch's message holds
  // With no delay a batch waits for a connection with nothing left to
  // write, a wait for room, which its messages' timeouts bound from their
  // sends: `due` is the earliest of their deadlines, and the batch goes by
  // it at the latest. With a delay `due` is when the delay is over, and a
  // message's time in the batch is the batching's, its timeout counting
  // from when the batch goes. max() for no time at all.
  bool waits_for_idle = false;
  std::chrono::steady_clock::time_point due = std::chrono::steady_clock::time_point::max();
  bool closed = false;
};

struct BasicClient::Batcher {
  explicit Batcher(asio::io_service& io_service) : timer(io_service) {}

  // What set_batching() set: per type, and per key of a type, a message's
  // key looked up as it is, never copied for the look-up.
  std::unordered_map<std::uint32_t, Batching> by_type;
  std::unordered_map<std::uint32_t, std::map<std::string, Batching>> by_key;
  bool configured() const { return !by_type.empty() || !by_key.empty(); }
  // Where a frame made of bytes is read (_batching_for): one message,
  // reused, keeps its strings' room from one read to the next, which a new
  // one would allocate each time, a third of the read.
  MultiplexerMessage scratch;

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
