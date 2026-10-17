// BasicClient's sends of a message given as its bytes alone (send_plain,
// send_bytes): what a client that batches hands over without making a
// frame of it. A message for a batch joins it as the bytes it is, a record
// with no frame (BatchRecord), and the batch's frame is made of them as of
// any message's body (basic_client_batching.cc); one that goes alone, its
// type not batched, too big for a batch, a batch of one or with no time to
// wait, is framed here, unsealed, and sent as send() sends a frame, which
// seals it (_send). So a ThreadedClient's io thread, which these serve,
// makes no frame per message, and a SyncClient's queue() none for a
// message that joins a batch.
//
// What send() decides for a frame that may batch is decided here the same
// way and in the same order: the batching for the message's type and key,
// the loop run first for a synchronous client (poll_first,
// _batch_needs_loop), every batch let go before a message that goes alone.
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "multiplexer/basic_client.h"
#include "multiplexer/batching.h"
#include "multiplexer/defaults.h"

namespace multiplexer {

void BasicClient::keep_spent_bodies(std::shared_ptr<SpentBodies> spent) {
  MX_DCHECK_RUN_ON(&owner_thread());
  spent_bodies_ = std::move(spent);
}

bool BasicClient::send_plain(std::string* body, std::uint64_t id, std::uint32_t type, bool keyed,
                             const std::string& key, float timeout, std::vector<BasicScheduledMessageTracker>* trackers,
                             const SendCallback& done, bool poll_first) {
  MX_DCHECK_RUN_ON(&owner_thread());
  if (shuts_down_) {
    return false;
  }
  BatchRecord record;
  record.body.swap(*body);
  record.id = id;
  const std::size_t bytes = record.body.size();
  // One with no time to wait, 0 or NaN, goes alone, as send() has it.
  if ((timeout > 0 || timeout < 0) && batcher_->configured()) {
    const Batching* batching = _batching_for(type, keyed, key);
    if (batching && poll_first && _batch_needs_loop(type, keyed, key, bytes, *batching)) {
      poll();
      poll_first = false;
      if (shuts_down_) {
        return false;
      }
      batching = _batching_for(type, keyed, key);  // what the loop ran may have changed it
    }
    if (batching) {
      return _batch_record(&record, type, keyed, key, bytes, *batching, timeout, trackers, done);
    }
  }
  if (poll_first) {
    poll();
    if (shuts_down_) {
      return false;
    }
  }
  close_batches();
  return _send(_record_frame(&record, type), false, LanePtr(), _deadline_after(timeout), 0, trackers, NULL, done);
}

bool BasicClient::send_bytes(std::string* body, std::uint64_t id, std::uint32_t type, float timeout,
                             std::vector<BasicScheduledMessageTracker>* trackers, const SendCallback& done,
                             bool poll_first) {
  MX_DCHECK_RUN_ON(&owner_thread());
  // Read only for a type something is set for, as _batching_for reads a
  // frame made of bytes; the key copied out of the message read, which a
  // send made while the loop runs below would read over.
  Batcher& batcher = *batcher_;
  if (batcher.configured() && (batcher.by_type.count(type) || batcher.by_key.count(type))) {
    MultiplexerMessage& parsed = batcher.scratch;
    if (parsed.ParseFromString(*body) && parsed.type() == type && parsed.key().size() <= MAX_KEY_SIZE &&
        RawMessage::plain_event(parsed)) {
      const bool keyed = parsed.has_key();
      const std::string key = keyed ? parsed.key() : std::string();
      return send_plain(body, id, type, keyed, key, timeout, trackers, done, poll_first);
    }
  }
  // Not a plain event of a batched type: alone, as send() sends its frame.
  if (shuts_down_) {
    return false;
  }
  if (poll_first) {
    poll();
    if (shuts_down_) {
      return false;
    }
  }
  close_batches();
  BatchRecord record;
  record.body.swap(*body);
  record.id = id;
  return _send(_record_frame(&record, type), false, LanePtr(), _deadline_after(timeout), 0, trackers, NULL, done);
}

// A record's frame: its own, or one made now of its bytes, swapped out of
// it, unsealed, for _send to seal as it goes alone.
std::shared_ptr<const RawMessage> BasicClient::_record_frame(BatchRecord* record, std::uint32_t type) {
  if (record->raw) {
    return record->raw;
  }
  return std::make_shared<RawMessage>(RawMessage::Unsealed(), &record->body, record->id, type);
}

}  // namespace multiplexer
