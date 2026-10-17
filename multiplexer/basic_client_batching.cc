// BasicClient's delivery: the events the program sends without waiting for
// them, of the types and keys it set a delivery for (set_delivery), go by
// it: with `batch` together, several in one message
// (MultiplexerMessage.batch), which the receiving library splits back into
// one message each (_split_batch in basic_client.cc); with `confirm` kept
// until confirmed (basic_client_confirm.cc); with neither as any send, by
// the setting's timeout. The rules are at BasicClient::Delivery; the
// structures in batching.h.
//
// A message for a batch joins the open batch of its group, its type and
// key, or opens one, whose deadline is its timeout from then. A batch is a
// list of the messages' frames: nothing is serialized again until it goes.
// It goes (_close_batch) when it is full, by messages or by bytes, when its
// max_delay is over, one timer running to the earliest, or, with max_delay
// 0, when a connection has nothing left to write, on it: while one has, a
// message goes at once and alone, as without batching, and only while
// every connection is busy do messages gather, each batch going as soon as
// one is not (Connection::notify_when_idle), or at its deadline, the time
// it waited being a wait for room. A batch that goes on its own account,
// full, at its delay or at its deadline, takes those waiting for a
// connection with nothing left to write with it. Anything sent that does
// not join a batch, a flush, a shutdown and a change of what is set first
// let every batch go, in the order they began, so that a message overtakes
// no batched one sent before it; a batch may go before an older one of
// another group. Open batches are indexed by the order they began and by
// when they are due, so that each step is a look-up however many keys are
// open. A confirmed message of a type not batched goes at once, a batch of
// one.
//
// Going, a batch becomes one frame (_send_batch): an envelope with the
// group's type and key, a fresh id and this client as its sender, and the
// messages' serialized forms in a MessageBatch inside it (BatchEnvelope,
// serialized once, straight into the frame), measured as it goes on the
// wire and cut where it would be over max_bytes, as many at once as cover
// the excess, what is cut going next in a batch of its own with the same
// deadline; a batch of one goes as the message itself, unless confirmed.
// The frame then goes as any message does (_send): round robin, waiting
// for room and for a connection until the batch's deadline. Its end is
// each message's: their trackers and `done`s hear it from the frame's,
// followed as any send's (outbox_flush.cc), and a drop of the frame is told
// for each message (report_drop, RawMessage::batch_ids).
#include <algorithm>
#include <cmath>
#include <map>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

#include "lib/logging/logging.h"
#include "lib/repr.h"
#include "lib/resize_uninitialized.h"
#include "lib/seconds.h"
#include "multiplexer/batching.h"
#include "multiplexer/multiplexer.constants.h"
#include "multiplexer/outbox.h"

namespace multiplexer {

using mx::repr;

void BasicClient::check_delivery(std::uint32_t type, const std::string* key, const Delivery& delivery) {
  if (type <= types::MAX_MULTIPLEXER_META_PACKET) {
    throw std::invalid_argument("a delivery for message type " + std::to_string(type) + ", a protocol type");
  }
  if (key && key->size() > MAX_KEY_SIZE) {
    throw std::invalid_argument("a delivery for a key of " + std::to_string(key->size()) +
                                " bytes, over the limit of " + std::to_string(MAX_KEY_SIZE) + " (MAX_KEY_SIZE)");
  }
  if (delivery.max_records < 1 || delivery.max_records > MAX_BATCH_RECORDS) {
    throw std::invalid_argument("a delivery with max_records " + std::to_string(delivery.max_records) +
                                ": 1 to MAX_BATCH_RECORDS (" + std::to_string(MAX_BATCH_RECORDS) + ")");
  }
  if (delivery.max_bytes < 1 || delivery.max_bytes > MAX_MESSAGE_SIZE) {
    throw std::invalid_argument("a delivery with max_bytes " + std::to_string(delivery.max_bytes) +
                                ": 1 to MAX_MESSAGE_SIZE (" + std::to_string(MAX_MESSAGE_SIZE) + ")");
  }
  if (!(delivery.max_delay >= 0 && delivery.max_delay <= mx::MAX_SECONDS)) {  // NaN and infinity too
    throw std::invalid_argument("a delivery with max_delay " + repr(delivery.max_delay) +
                                ": a number of seconds, 0 to MAX_SECONDS (1e9)");
  }
  if (delivery.confirm && !(delivery.retry_after > 0 && delivery.retry_after <= mx::MAX_SECONDS)) {
    throw std::invalid_argument("a delivery with retry_after " + repr(delivery.retry_after) +
                                ": a number of seconds, over 0 and up to MAX_SECONDS (1e9)");
  }
  // A batch's time counts from its first message: one no longer than its
  // delay would give up on it as it goes, and 0 on a message alone at once.
  if (std::isnan(delivery.timeout) ||
      (delivery.timeout >= 0 && delivery.timeout <= (delivery.batch ? delivery.max_delay : 0))) {
    throw std::invalid_argument("a delivery with timeout " + repr(delivery.timeout) + ": a number of seconds over " +
                                (delivery.batch ? "max_delay" : "0") + ", or negative for no deadline");
  }
}

void BasicClient::set_delivery(std::uint32_t type, const Delivery& delivery) {
  MX_DCHECK_RUN_ON(&owner_thread());
  check_delivery(type, nullptr, delivery);
  close_batches();
  batcher_->by_type[type] = delivery;
}

void BasicClient::set_delivery(std::uint32_t type, const std::string& key, const Delivery& delivery) {
  MX_DCHECK_RUN_ON(&owner_thread());
  check_delivery(type, &key, delivery);
  close_batches();
  batcher_->by_key[type].insert_or_assign(key, delivery);
}

void BasicClient::clear_delivery(std::uint32_t type) {
  MX_DCHECK_RUN_ON(&owner_thread());
  close_batches();
  batcher_->by_type.erase(type);
  batcher_->by_key.erase(type);
}

void BasicClient::clear_delivery(std::uint32_t type, const std::string& key) {
  MX_DCHECK_RUN_ON(&owner_thread());
  close_batches();
  Batcher& batcher = *batcher_;
  std::unordered_map<std::uint32_t, std::map<std::string, Delivery>>::iterator keys = batcher.by_key.find(type);
  if (keys != batcher.by_key.end() && keys->second.erase(key) && keys->second.empty()) {
    batcher.by_key.erase(keys);
  }
}

bool BasicClient::batch_timer_armed() const {
  MX_DCHECK_RUN_ON(&owner_thread());
  return batcher_->armed != std::chrono::steady_clock::time_point::max();
}

bool BasicClient::has_delivery() const {
  MX_DCHECK_RUN_ON(&owner_thread());
  return batcher_->configured();
}

std::size_t BasicClient::batched_messages() const {
  MX_DCHECK_RUN_ON(&owner_thread());
  std::size_t messages = 0;
  for (const std::pair<const std::uint64_t, BatchPtr>& open : batcher_->open) {
    messages += open.second->records.size();
  }
  return messages;
}

// The delivery that applies to `raw`, or null: the setting for its type and
// key, else for its type, when it is a plain event. A frame made of
// serialized bytes is read once, and only for a type something is set for.
const BasicClient::Delivery* BasicClient::_delivery_for(const std::shared_ptr<const RawMessage>& raw) {
  Batcher& batcher = *batcher_;
  if (!batcher.configured()) {
    return nullptr;
  }
  const std::uint32_t type = raw->type();
  std::unordered_map<std::uint32_t, Delivery>::const_iterator by_type = batcher.by_type.find(type);
  std::unordered_map<std::uint32_t, std::map<std::string, Delivery>>::const_iterator keys = batcher.by_key.find(type);
  if (by_type == batcher.by_type.end() && keys == batcher.by_key.end()) {
    return nullptr;
  }
  if (raw->batchable() == RawMessage::Batchable::UNKNOWN) {
    // Made of bytes the program gave, which nothing checked: one that is
    // not the type it was given, or whose key is too long, goes alone, as
    // with no delivery set.
    MultiplexerMessage& parsed = batcher.scratch;
    if (parsed.ParseFromString(raw->get_message()) && parsed.type() == type && parsed.key().size() <= MAX_KEY_SIZE &&
        RawMessage::plain_event(parsed)) {
      const bool keyed = parsed.has_key();
      raw->mark_batchable(RawMessage::Batchable::YES, keyed, keyed ? parsed.key() : std::string());
    } else {
      raw->mark_batchable(RawMessage::Batchable::NO, false, std::string());
    }
  }
  if (raw->batchable() != RawMessage::Batchable::YES) {
    return nullptr;
  }
  return _delivery_for(type, raw->batch_keyed(), raw->batch_key());
}

// The delivery that applies to a plain event of `type` with its key, `key`
// when `keyed`, or null: the setting for its key, else for its type.
const BasicClient::Delivery* BasicClient::_delivery_for(std::uint32_t type, bool keyed, const std::string& key) const {
  const Batcher& batcher = *batcher_;
  if (keyed) {
    std::unordered_map<std::uint32_t, std::map<std::string, Delivery>>::const_iterator keys = batcher.by_key.find(type);
    if (keys != batcher.by_key.end()) {
      std::map<std::string, Delivery>::const_iterator found = keys->second.find(key);
      if (found != keys->second.end()) {
        return &found->second;
      }
    }
  }
  std::unordered_map<std::uint32_t, Delivery>::const_iterator by_type = batcher.by_type.find(type);
  return by_type != batcher.by_type.end() ? &by_type->second : nullptr;
}

// Whether a synchronous client's send of `raw` by `delivery` must run the
// loop first (send's `poll_first`): the message chooses a connection in
// this call, going alone, not batched, too big or with no delay, where a
// free connection decides, or letting its batch go, full with it or
// without room for it; or a batch's delay is over, which only the loop's
// timer lets go. Else it joins its batch and nothing is written, and the
// call runs no loop, a system call spared for each message.
bool BasicClient::_batch_needs_loop(std::uint32_t type, bool keyed, const std::string& key, std::size_t bytes,
                                    const Delivery& delivery) const {
  const Batcher& batcher = *batcher_;
  if (!delivery.batch || delivery.max_delay == 0 ||
      (!batcher.by_due.empty() && batcher.by_due.begin()->first <= std::chrono::steady_clock::now())) {
    return true;
  }
  const BatchPtr batch = batcher.find(type, keyed, key);
  const std::size_t records = batch ? batch->records.size() : 0;
  return records + 1 >= delivery.max_records || (batch ? batch->bytes : 0) + bytes >= delivery.max_bytes;
}

bool BasicClient::_batch(const std::shared_ptr<const RawMessage>& raw, const Delivery& delivery,
                         std::vector<BasicScheduledMessageTracker>* trackers, const SendCallback& done) {
  BatchRecord record;
  record.raw = raw;
  return _batch_record(&record, raw->type(), raw->batch_keyed(), raw->batch_key(), raw->get_message().size(), delivery,
                       trackers, done);
}

// `record`, a frame or bytes alone of `bytes` bytes, by `delivery`, which
// batches or confirms: moved into the open batch of its group, `type` and
// its key, `key` when `keyed`, or one opened for it; or sent alone at once.
bool BasicClient::_batch_record(BatchRecord* record, std::uint32_t type, bool keyed, const std::string& key,
                                std::size_t bytes, const Delivery& delivery,
                                std::vector<BasicScheduledMessageTracker>* trackers, const SendCallback& done) {
  Batcher& batcher = *batcher_;
  BatchPtr batch = delivery.batch ? batcher.find(type, keyed, key) : BatchPtr();
  // A message the batch has no room for lets it go first.
  if (batch && (batch->records.size() >= delivery.max_records || batch->bytes + bytes > delivery.max_bytes)) {
    _close_on_its_own(batch);
    batch.reset();
  }
  if (!batch) {
    // Not batched, too big for a batch, or, with no delay, a connection
    // free to write it now, or none to write anything: it goes alone, at
    // once, as without batching, after every batch, by its timeout from
    // now; a confirmed one as a batch of one, which numbers it.
    if (!delivery.batch || bytes >= delivery.max_bytes || (delivery.max_delay == 0 && _idle_or_none())) {
      close_batches();
      if (!delivery.confirm) {
        return _send(_record_frame(record, type), false, LanePtr(), _deadline_after(delivery.timeout), 0, trackers,
                     NULL, done);
      }
      BatchPtr alone = std::make_shared<Batch>();
      alone->group.type = type;
      alone->group.keyed = keyed;
      alone->group.key = key;
      alone->delivery = delivery;
      alone->deadline = _deadline_after(delivery.timeout);
      alone->closed = true;
      record->done = done;
      if (trackers) {
        record->state = std::make_shared<SendState>(SendState::QUEUED);
        trackers->push_back(record->state);
      }
      alone->bytes = bytes;
      alone->records.push_back(std::move(*record));
      _send_batch(alone, nullptr);
      return true;
    }
    batch = std::make_shared<Batch>();
    batch->began = ++batcher.began;
    batch->group.type = type;
    batch->group.keyed = keyed;
    batch->group.key = key;  // once a batch: a message's is never copied
    batch->delivery = delivery;
    const std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now();
    batch->deadline = _deadline_after(delivery.timeout, now);
    Batcher::OfType& of_type = batcher.by_group[type];
    if (keyed) {
      of_type.keyed.emplace(batch->group.key, batch);
    } else {
      of_type.keyless = batch;
    }
    batcher.open.emplace(batch->began, batch);
    if (delivery.max_delay > 0) {
      _due_at(batch, now + mx::from_seconds(delivery.max_delay));
    } else {
      // A wait for room, which goes at the deadline at the latest.
      batch->waits_for_idle = true;
      batcher.waiting_for_idle.insert(batch->began);
      if (batch->deadline != std::chrono::steady_clock::time_point::max()) {
        _due_at(batch, batch->deadline);
      }
      _wait_for_idle();
    }
  }
  record->done = done;
  if (trackers) {
    record->state = std::make_shared<SendState>(SendState::QUEUED);
    trackers->push_back(record->state);
  }
  batch->bytes += bytes;
  batch->records.push_back(std::move(*record));
  if (batch->records.size() >= delivery.max_records || batch->bytes >= delivery.max_bytes) {
    _close_on_its_own(batch);
  }
  return true;
}

// `batch` goes at `due` at the latest, the timer moved to it when it is the
// earliest.
void BasicClient::_due_at(const BatchPtr& batch, std::chrono::steady_clock::time_point due) {
  Batcher& batcher = *batcher_;
  if (batch->due != std::chrono::steady_clock::time_point::max()) {
    batcher.by_due.erase(std::make_pair(batch->due, batch->began));
  }
  batch->due = due;
  batcher.by_due.emplace(due, batch->began);
  if (due < batcher.armed) {
    _arm_batch_timer();
  }
}

void BasicClient::close_batches() {
  MX_DCHECK_RUN_ON(&owner_thread());
  Batcher& batcher = *batcher_;
  if (batcher.open.empty()) {
    return;
  }
  std::map<std::uint64_t, BatchPtr> open;
  open.swap(batcher.open);  // taken whole, in the order they began
  for (const std::pair<const std::uint64_t, BatchPtr>& each : open) {
    _close_batch(each.second);
  }
}

// A batch goes on its own account, full, at its delay or at its deadline,
// and with it every batch that waits for a connection with nothing left to
// write, which would only be written after it, in the order they began:
// while batches keep going, a group with fewer messages waits no longer
// than until the next one goes.
void BasicClient::_close_on_its_own(const BatchPtr& batch) {
  Batcher& batcher = *batcher_;
  std::set<std::uint64_t> going(batcher.waiting_for_idle);
  going.insert(batch->began);
  for (std::uint64_t began : going) {
    std::map<std::uint64_t, BatchPtr>::iterator found = batcher.open.find(began);
    if (found != batcher.open.end()) {
      const BatchPtr each = found->second;  // a copy: the close takes it out of the map
      _close_batch(each);
    }
  }
}

// The batch goes: out of the open ones, and the timer stopped with the
// last batch that has a time to go by, so that nothing waits for it; then
// sent, on `idle` when given, a connection that has nothing left to write.
void BasicClient::_close_batch(const BatchPtr& batch, Connection* idle) {
  if (batch->closed) {
    return;
  }
  batch->closed = true;
  Batcher& batcher = *batcher_;
  std::unordered_map<std::uint32_t, Batcher::OfType>::iterator of_type = batcher.by_group.find(batch->group.type);
  if (of_type != batcher.by_group.end()) {
    if (batch->group.keyed) {
      of_type->second.keyed.erase(batch->group.key);
    } else {
      of_type->second.keyless.reset();
    }
    if (!of_type->second.keyless && of_type->second.keyed.empty()) {
      batcher.by_group.erase(of_type);
    }
  }
  batcher.open.erase(batch->began);
  if (batch->waits_for_idle) {
    batcher.waiting_for_idle.erase(batch->began);
  }
  if (batch->due != std::chrono::steady_clock::time_point::max()) {
    batcher.by_due.erase(std::make_pair(batch->due, batch->began));
    if (batcher.by_due.empty() && batcher.armed != std::chrono::steady_clock::time_point::max()) {
      asio::error_code cancelled;
      batcher.timer.cancel(cancelled);
      batcher.armed = std::chrono::steady_clock::time_point::max();
    }
  }
  _send_batch(batch, idle);
}

void BasicClient::_send_batch(const BatchPtr& batch, Connection* idle) {
  // On the connection that said it has nothing left to write, rather than
  // where the round robin points, which may be one still writing.
  LanePtr lane;
  if (idle && idle->living()) {
    lane = std::make_shared<Lane>(_wrap(idle->shared_from_this()), false);
  }
  // A confirmed batch always goes as an envelope, which numbers its
  // messages, a batch of one too (basic_client_confirm.cc).
  const bool confirm = batch->delivery.confirm;
  std::vector<Batch::Record>& records = batch->records;
  std::size_t begin = 0;
  while (begin < records.size()) {
    std::size_t end = records.size();
    std::shared_ptr<const RawMessage> frame;
    if (end - begin > 1 || confirm) {
      // The envelope holds the batch itself, so that its messages are
      // serialized once, straight into the frame (BatchEnvelope): the same
      // bytes as a MultiplexerMessage whose batch holds them serialized.
      BatchEnvelope& envelope = batcher_->envelope;
      envelope.Clear();
      envelope.set_id(random64());
      envelope.set_sender(instance_id());
      envelope.set_type(batch->group.type);
      if (batch->group.keyed) {
        envelope.set_key(batch->group.key);
      }
      // Each message's body moved into the batch: a frame's, not copied
      // where the frame has no other holder (RawMessage::TakeBody), or the
      // bytes a message came as; given back to a message that goes on
      // outside this batch (_give_back).
      MessageBatch& inside = *envelope.mutable_batch();
      inside.mutable_message()->Reserve(static_cast<int>(end - begin));
      for (std::size_t index = begin; index < end; ++index) {
        inside.mutable_message()->Add(records[index].raw ? RawMessage::TakeBody(records[index].raw)
                                                         : std::move(records[index].body));
      }
      if (confirm) {
        _numbered(records, begin, end, &envelope);  // measured with the rest
      }
      // Where the message would be over max_bytes, measured as it goes on
      // the wire, its last messages are cut, to go next: at once as many as
      // their own sizes cover the excess with, which each adds at least to
      // the message, and the rest measured again, nothing serialized until
      // the frame is made.
      for (std::size_t size = envelope.ByteSizeLong(); end - begin > 1 && size > batch->delivery.max_bytes;
           size = envelope.ByteSizeLong()) {
        std::size_t cut = 0;
        while (end - begin > 1 && cut < size - batch->delivery.max_bytes) {
          std::string* last = inside.mutable_message(inside.message_size() - 1);
          cut += last->size();
          _give_back(&records[end - 1], last);
          inside.mutable_message()->RemoveLast();
          --end;
        }
        if (confirm) {
          _numbered(records, begin, end, &envelope);  // those cut keep their numbers, next
        }
      }
      if (end - begin > 1 || confirm) {
        try {
          frame = _batch_frame(envelope);
        } catch (const std::exception& error) {
          // Nothing it holds should make it fail; should one, its messages
          // go alone, each as it would have without batching, their bodies
          // back in their frames.
          MX_LOG(ERROR, LOWVERBOSITY,
                 CTX("BasicClient")
                     TEXT(std::string("a batch's message could not be made, its messages go alone: ") + error.what()));
          for (std::size_t index = begin; index < end; ++index) {
            _give_back(&records[index], inside.mutable_message(static_cast<int>(index - begin)));
          }
        }
        if (frame) {
          std::shared_ptr<std::vector<std::uint64_t>> ids = std::make_shared<std::vector<std::uint64_t>>();
          ids->reserve(end - begin);
          for (std::size_t index = begin; index < end; ++index) {
            ids->push_back(records[index].raw ? records[index].raw->id() : records[index].id);
          }
          frame->mark_batch(ids);
          // The bodies, in the frame now, go where the client reuses them
          // (keep_spent_bodies) or are let go, not kept in the envelope
          // until the next batch.
          for (std::string& body : *inside.mutable_message()) {
            if (!spent_bodies_ || !spent_bodies_->keep(&body)) {
              std::string().swap(body);
            }
          }
        }
      }
    }
    if (!frame && confirm) {
      _cannot_confirm(batch, begin, end);  // no envelope holds them: given up on, as they cannot be numbered
      begin = end;
      continue;
    }
    if (!frame) {
      end = begin + 1;
      frame = _record_frame(&records[begin], batch->group.type);  // a batch of one goes as the message itself
    }
    if (confirm) {
      // Kept until confirmed, the frame waiting for room and for a
      // connection until the batch's deadline.
      frame->mark_confirmed();  // its drop told by the confirmed sends
      _wait_for_confirmation(batch, begin, end, frame, lane);
      begin = end;
      continue;
    }
    // The frame waits for room and for a connection until the batch's
    // deadline. Its end is each message's that someone follows, in the
    // order they were sent; the rest keep nothing.
    std::shared_ptr<std::vector<Batch::Record>> taken;
    for (std::size_t index = begin; index < end; ++index) {
      Batch::Record& record = records[index];
      if (record.state || record.done) {
        if (!taken) {
          taken = std::make_shared<std::vector<Batch::Record>>();
        }
        record.raw.reset();  // its bytes are in the frame now: only its end is kept
        taken->push_back(std::move(record));
      }
    }
    SendCallback ended;
    if (taken) {
      ended = [taken](unsigned int written) {
        for (Batch::Record& record : *taken) {
          if (record.state && *record.state == SendState::QUEUED) {
            *record.state = written ? SendState::SENT : SendState::LOST;
          }
          if (record.done) {
            SendCallback done;
            done.swap(record.done);
            try {
              done(written);
            } catch (const std::exception& error) {
              MX_LOG(ERROR, LOWVERBOSITY,
                     CTX("BasicClient") TEXT(std::string("a send's callback raised: ") + error.what()));
            }
          }
        }
      };
    }
    _send(frame, false, lane, batch->deadline, 0, NULL, NULL, ended);
    begin = end;
  }
}

// A batch's frame, `envelope` serialized into a buffer a written batch
// frame left (BatchFramePool), else a new one; the frame gives its buffer
// back to the pool once its last holder lets go of it. Throws as
// RawMessage::FromMessage does, the protobuf measured first.
std::shared_ptr<const RawMessage> BasicClient::_batch_frame(const BatchEnvelope& envelope) {
  check_key_size(envelope.key().size());
  const std::size_t size = envelope.ByteSizeLong();
  check_message_size(size);
  const std::shared_ptr<BatchFramePool>& pool = batcher_->frame_pool;
  std::string buffer;
  {
    mx::MutexLock lock(pool->mutex);
    if (!pool->spare.empty()) {
      buffer.swap(pool->spare.back());
      pool->spare.pop_back();
    }
  }
  mx::resize_uninitialized(&buffer, size);  // serialized over whole next: nothing filled first
  envelope.SerializeWithCachedSizesToArray(reinterpret_cast<std::uint8_t*>(&buffer[0]));
  RawMessage* frame = new RawMessage(&buffer, envelope.id(), envelope.type());
  return std::shared_ptr<const RawMessage>(frame, [pool](const RawMessage* written) {
    std::string body = RawMessage::ReleaseBody(written);
    if (body.capacity() <= BatchFramePool::FRAME_BUFFER_BYTES_KEPT) {
      mx::MutexLock lock(pool->mutex);
      if (pool->spare.size() < BatchFramePool::FRAME_BUFFERS_KEPT) {
        pool->spare.push_back(std::move(body));
      }
    }
    delete written;
  });
}

// A body taken into a batch given back to its message, which goes on
// outside the batch: to its frame, or as its bytes.
void BasicClient::_give_back(BatchRecord* record, std::string* body) {
  if (record->raw) {
    RawMessage::GiveBodyBack(record->raw, body);
  } else {
    record->body.swap(*body);
  }
}

// A connection with nothing queued, written or waiting for room, or none
// live at all, which a message would then be held for.
bool BasicClient::_idle_or_none() {
  bool live = false;
  for (ConnectionById::const_iterator entry = connection_by_id_.begin(); entry != connection_by_id_.end(); ++entry) {
    Connection::pointer conn = entry->second.lock();
    if (!conn || !conn->living()) {
      continue;
    }
    live = true;
    if (conn->outgoing_queue_empty()) {
      Backlog* backlog = _backlog(conn.get());
      if (!backlog || !backlog->live) {
        return true;
      }
    }
  }
  return !live;
}

// Every connection says when it has nothing left to write, for the batches
// that wait for that.
void BasicClient::_wait_for_idle() {
  for (ConnectionById::const_iterator entry = connection_by_id_.begin(); entry != connection_by_id_.end(); ++entry) {
    Connection::pointer conn = entry->second.lock();
    if (conn && conn->living()) {
      conn->notify_when_idle();
    }
  }
}

void BasicClient::outgoing_queue_idle(Connection* conn) {
  MX_DCHECK_RUN_ON(&owner_thread());
  _close_idle_batches(conn);
}

// The batches that wait for a connection with nothing left to write go,
// in the order they began, on `idle` when given.
void BasicClient::_close_idle_batches(Connection* idle) {
  Batcher& batcher = *batcher_;
  if (batcher.waiting_for_idle.empty()) {
    return;
  }
  std::vector<BatchPtr> waiting;  // copies: each close takes its batch out of the maps
  waiting.reserve(batcher.waiting_for_idle.size());
  for (std::uint64_t began : batcher.waiting_for_idle) {
    waiting.push_back(batcher.open[began]);
  }
  for (const BatchPtr& batch : waiting) {
    _close_batch(batch, idle);
  }
}

// The timer runs to the earliest max_delay's end among the batches open.
void BasicClient::_arm_batch_timer() {
  Batcher& batcher = *batcher_;
  if (batcher.by_due.empty()) {
    return;
  }
  const std::chrono::steady_clock::time_point earliest = batcher.by_due.begin()->first;
  if (earliest >= batcher.armed) {
    return;
  }
  batcher.armed = earliest;
  batcher.timer.expires_at(earliest);
  std::weak_ptr<BasicClient> self = weak_from_this();
  batcher.timer.async_wait([self, earliest](const asio::error_code& error) {
    if (error) {
      return;  // cancelled, or moved to an earlier end
    }
    if (std::shared_ptr<BasicClient> client = self.lock()) {
      if (client->batcher_->armed == earliest) {
        client->_batch_timer_fired();
      }
    }
  });
}

// The batches whose time is up go, first come first, each on its own
// account; the timer runs on to the next.
void BasicClient::_batch_timer_fired() {
  MX_DCHECK_RUN_ON(&owner_thread());
  Batcher& batcher = *batcher_;
  batcher.armed = std::chrono::steady_clock::time_point::max();
  const std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now();
  std::vector<BatchPtr> due;  // copies: each close takes its batch out of the maps
  for (std::set<std::pair<std::chrono::steady_clock::time_point, std::uint64_t>>::const_iterator each =
           batcher.by_due.begin();
       each != batcher.by_due.end() && each->first <= now; ++each) {
    due.push_back(batcher.open[each->second]);
  }
  for (const BatchPtr& batch : due) {
    _close_on_its_own(batch);
  }
  _arm_batch_timer();
}

}  // namespace multiplexer
