// BasicClient's confirmed sends: what a producer keeps of the batches it
// wrote until their messages are confirmed, and what a consumer owes the
// producers whose messages it took. Internal to basic_client.cc,
// basic_client_batching.cc and basic_client_confirm.cc, which describes the
// design; the rules are at BasicClient::Delivery.
#ifndef MX_MULTIPLEXER_CONFIRMING_H_
#define MX_MULTIPLEXER_CONFIRMING_H_

#include <algorithm>
#include <asio/steady_timer.hpp>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <set>
#include <unordered_map>
#include <utility>
#include <vector>

#include "multiplexer/basic_client.h"
#include "multiplexer/batching.h"

namespace multiplexer {

// One flag a message, whether it is resolved, 64 to a word: a confirmation
// of a whole run marks words, not messages.
struct ResolvedFlags {
  std::vector<std::uint64_t> words;
  void assign(std::size_t count) { words.assign((count + 63) / 64, 0); }
  bool operator[](std::size_t index) const { return (words[index / 64] >> (index % 64)) & 1; }
  void set(std::size_t index) { words[index / 64] |= std::uint64_t(1) << (index % 64); }
  // The flags of [begin, end) set; how many of them were not.
  std::size_t set_range(std::size_t begin, std::size_t end) {
    std::size_t newly = 0;
    while (begin < end) {
      const std::size_t bit = begin % 64;
      const std::size_t span = std::min<std::size_t>(64 - bit, end - begin);
      const std::uint64_t mask = (span == 64 ? ~std::uint64_t(0) : (std::uint64_t(1) << span) - 1) << bit;
      std::uint64_t& word = words[begin / 64];
      newly += static_cast<std::size_t>(__builtin_popcountll(mask & ~word));
      word |= mask;
      begin += span;
    }
    return newly;
  }
};

// A batch written whose messages wait for their confirmation: its frame,
// which holds their bytes for a send again and their ids
// (RawMessage::batch_ids), what it went with, its messages' numbers, the
// ends of the few someone follows, and per message whether it is
// resolved, confirmed or given up on; the connection it was placed on,
// when placed at once, and when its unresolved messages go again. Nothing
// in it is per message but a bit, and a number for a batch sent again.
struct BasicClient::Confirming {
  std::uint64_t id = 0;  // its envelope's, which a DELIVERY_ERROR references
  BatchGroup group;
  Delivery delivery;
  std::shared_ptr<const RawMessage> frame;
  std::size_t count = 0;  // its messages
  // Their numbers: `first_number` and on, one each, as a batch's first
  // frame numbers them, when `numbers` is empty; else each message's, as a
  // batch sent again keeps those its messages had.
  std::uint64_t first_number = 0;
  std::vector<std::uint64_t> numbers;
  // The messages whose end someone hears, a tracker or a `done`, by their
  // place in the frame, in order: none, mostly, and then a confirmation
  // marks a whole run of them at once (_on_confirmation).
  struct Followed {
    std::size_t index = 0;
    std::shared_ptr<SendState> state;
    SendCallback done;
  };
  std::vector<Followed> followed;
  ResolvedFlags resolved;
  std::size_t unresolved = 0;
  // Where its runs begin, their keys in Confirmer::runs (_rebuild_runs).
  std::vector<std::uint64_t> run_keys;
  // The connection it was placed on, when it went at once; null when it was
  // held for one, or once that connection closed. Its frame's tracker,
  // which reads SENT once the frame is written, ahead of the callback that
  // says so (_confirming_written).
  const Connection* placed_on = nullptr;
  BasicScheduledMessageTracker frame_state;
  bool written = false;  // its frame was written, or nobody took it
  // When what is unresolved goes again: from the write, or the last
  // confirmation of one of its messages, retry_after later; after a
  // DELIVERY_ERROR, `backoff` later.
  std::chrono::steady_clock::time_point retry_at = std::chrono::steady_clock::time_point::max();
  float backoff = 0;
  // Its batch's (Batch::deadline), when what is unresolved is given up on.
  std::chrono::steady_clock::time_point deadline = std::chrono::steady_clock::time_point::max();
  // Its entry in Confirmer::by_due: the earliest of retry_at, once written,
  // and `deadline`.
  std::chrono::steady_clock::time_point due = std::chrono::steady_clock::time_point::max();

  std::uint64_t number(std::size_t index) const { return numbers.empty() ? first_number + index : numbers[index]; }
  std::uint64_t id_of(std::size_t index) const { return (*frame->batch_ids())[index]; }
  // The end message `index` has, or null.
  Followed* follow(std::size_t index) {
    std::vector<Followed>::iterator found = std::lower_bound(
        followed.begin(), followed.end(), index, [](const Followed& each, std::size_t at) { return each.index < at; });
    return found != followed.end() && found->index == index ? &*found : nullptr;
  }
};

// What a consumer owes one producer: the numbers, by what became of them,
// as (first, count) pairs in the order confirmed, how many, and the
// connection the producer's messages came on last.
struct BasicClient::OwedConfirmations {
  Connection::weak_pointer connection;
  std::vector<std::uint64_t> processed;
  std::vector<std::uint64_t> refused;
  std::vector<std::uint64_t> failed;
  std::size_t count = 0;
};

struct BasicClient::Confirmer {
  explicit Confirmer(asio::io_service& io_service) : timer(io_service) {}

  // The producer's side. The last number given; each run of consecutive
  // numbers in a batch waiting, by its first number: the batch, where in it
  // the run begins, its length, and how many of it are unresolved, the run
  // forgotten once none is (a flush waits while one is at or below its
  // mark); the batches by their envelope's id; and by when they are due,
  // one timer running to the first.
  std::uint64_t last_number = 0;
  struct Run {
    ConfirmingPtr batch;
    std::size_t index = 0;
    std::size_t count = 0;
    std::size_t unresolved = 0;
  };
  std::map<std::uint64_t, Run> runs;
  std::unordered_map<std::uint64_t, ConfirmingPtr> by_id;
  std::set<std::pair<std::chrono::steady_clock::time_point, std::uint64_t>> by_due;
  asio::steady_timer timer;
  std::chrono::steady_clock::time_point armed = std::chrono::steady_clock::time_point::max();
  std::size_t unconfirmed = 0;   // messages unresolved, every batch's together
  std::uint64_t sent_again = 0;  // messages sent again, for tests
  // The ends of the sends resolved, run in the pass that runs the sends'
  // callbacks (_process_callbacks): each `done` and what it hears.
  std::vector<std::pair<SendCallback, unsigned int>> ends;
  // Where a frame's messages are read back for a send again, and where a
  // CONFIRMATION is read and made.
  MultiplexerMessage scratch;
  MessageBatch batch_scratch;
  Confirmation confirmation;

  // `count` more messages of the batch's run that holds `number` resolved;
  // the run forgotten once none of it is left.
  void resolved_in_run(const ConfirmingPtr& batch, std::uint64_t number, std::size_t count) {
    std::map<std::uint64_t, Run>::iterator run = runs.upper_bound(number);
    if (run == runs.begin()) {
      return;
    }
    --run;
    if (run->second.batch != batch || number >= run->first + run->second.count) {
      return;
    }
    run->second.unresolved -= std::min(count, run->second.unresolved);
    if (!run->second.unresolved) {
      runs.erase(run);
    }
  }

  // The consumer's side: per producer, what is owed it, and every
  // producer's count together. Past OWED_KEPT producers, those owed nothing
  // are forgotten (send_confirmations).
  static constexpr std::size_t OWED_KEPT = 4096;
  std::unordered_map<std::uint64_t, OwedConfirmations> owed;
  std::size_t owed_count = 0;
};

}  // namespace multiplexer

#endif  // MX_MULTIPLEXER_CONFIRMING_H_
