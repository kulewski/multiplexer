// BasicClient's confirmed sends, both sides (docs/delivery.md "Confirmed
// sends"; the rules at BasicClient::Delivery): a producer keeps every
// message of a confirmed batch until a consumer's library confirms it, and
// sends it again or gives up on it; a consumer's library gathers what it
// owes each producer into one CONFIRMATION.
//
// The producer. A confirmed batch goes as an envelope whose `confirm`
// numbers its messages (_numbered, one counter per client), a batch of one
// too, and waits once its frame is made (_wait_for_confirmation): the frame
// kept, which holds the messages' bytes and ids, their numbers, as a first
// one and a count for a batch's first frame, the ends of the messages
// someone follows, few or none, and a bit per message, resolved or not;
// found by each run of consecutive numbers and by the envelope's id. A
// CONFIRMATION resolves numbers (_on_confirmation): processed ends the send
// with 1, a run at a time where nobody follows its messages, failed gives
// it up, refused sends it again at once. What is unresolved goes again
// (_send_again), keeping its numbers, read back out of the frame into a
// batch of its own that goes at once with the same deadline: when nobody
// took the batch (a DELIVERY_ERROR, after a back-off), when the connection
// it was written on closed, and when nothing confirmed any of it for
// retry_after; what is left of a batch is given up on, NOT_CONFIRMED, at
// its deadline, and every message left, SHUT_DOWN, at shutdown. A
// confirmed frame's drop is told here too (_confirmed_frame_dropped,
// through report_drop), for the messages not resolved yet, so that each
// message is told once, whichever of its frame's drop, its deadline and the
// shutdown comes first. One timer runs to the earliest of every batch's
// retry time and deadline, and stops once nothing waits. The ends run in
// the callbacks pass, as a send's do, and a flush waits while a number at
// or below its mark is unresolved. A confirmation costs a look-up per run
// of numbers it names, not per message.
//
// The consumer. A message of a confirmed batch is handed out with its
// number (_split_batch); confirm() adds it to what this client owes its
// producer, (first, count) pairs per outcome, which go in one CONFIRMATION
// when this client has nothing more in hand for the program
// (send_confirmations): once what one read brought is handed over
// (read_drained) where handlers run as messages arrive, else as a receive
// waits; at CONFIRMATIONS_GATHERED_MAX owed; at a flush; and at shutdown.
#include <algorithm>
#include <cstddef>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "lib/logging/logging.h"
#include "lib/repr.h"
#include "lib/seconds.h"
#include "multiplexer/batching.h"
#include "multiplexer/confirming.h"
#include "multiplexer/multiplexer.constants.h"
#include "multiplexer/outbox.h"

namespace multiplexer {

namespace {

// `number` appended to `pairs`, (first, count) pairs, the last one grown
// when the number continues it.
void add_number(google::protobuf::RepeatedField<std::uint64_t>* pairs, std::uint64_t number) {
  const int size = pairs->size();
  if (size >= 2 && pairs->Get(size - 2) + pairs->Get(size - 1) == number) {
    pairs->Set(size - 1, pairs->Get(size - 1) + 1);
    return;
  }
  pairs->Add(number);
  pairs->Add(1);
}

void add_number(std::vector<std::uint64_t>* pairs, std::uint64_t number) {
  const std::size_t size = pairs->size();
  if (size >= 2 && (*pairs)[size - 2] + (*pairs)[size - 1] == number) {
    ++(*pairs)[size - 1];
    return;
  }
  pairs->push_back(number);
  pairs->push_back(1);
}

}  // namespace

// What a list of a CONFIRMATION names that this client waits for: per run
// it meets, the batch, the run's key and the messages' places in the batch.
struct BasicClient::Segment {
  ConfirmingPtr batch;
  std::uint64_t key;
  std::size_t begin;
  std::size_t end;
};

// The records [begin, end) numbered, those with no number yet given the
// next, and their numbers into the envelope's `confirm`.
void BasicClient::_numbered(std::vector<BatchRecord>& records, std::size_t begin, std::size_t end,
                            BatchEnvelope* envelope) {
  envelope->clear_confirm();
  for (std::size_t index = begin; index < end; ++index) {
    if (!records[index].number) {
      records[index].number = ++confirmer_->last_number;
    }
    add_number(envelope->mutable_confirm(), records[index].number);
  }
}

// The batch's records [begin, end), whose frame is made, kept until each is
// resolved: their numbers and the ends someone follows, the frame holding
// their bytes and ids, the records themselves left to the batch. The frame
// sent, by the batch's deadline, its write heard (_confirming_written), and
// the connection it was placed on kept when it went at once.
void BasicClient::_wait_for_confirmation(const BatchPtr& batch, std::size_t begin, std::size_t end,
                                         const std::shared_ptr<const RawMessage>& frame, const LanePtr& lane) {
  Confirmer& confirmer = *confirmer_;
  ConfirmingPtr confirming = std::make_shared<Confirming>();
  confirming->id = frame->id();
  confirming->group = batch->group;
  confirming->delivery = batch->delivery;
  confirming->frame = frame;
  confirming->backoff = batch->backoff;
  confirming->deadline = batch->deadline;
  std::vector<BatchRecord>& records = batch->records;
  const std::size_t count = end - begin;
  confirming->count = count;
  confirming->first_number = records[begin].number;
  bool consecutive = true;
  for (std::size_t index = begin; index < end; ++index) {
    BatchRecord& record = records[index];
    consecutive = consecutive && record.number == confirming->first_number + (index - begin);
    if (record.state || record.done) {
      Confirming::Followed followed;
      followed.index = index - begin;
      followed.state = std::move(record.state);
      followed.done.swap(record.done);
      confirming->followed.push_back(std::move(followed));
    }
  }
  if (!consecutive) {
    // Sent again, among others resolved before: each its own.
    confirming->numbers.reserve(count);
    for (std::size_t index = begin; index < end; ++index) {
      confirming->numbers.push_back(records[index].number);
    }
  }
  confirming->resolved.assign(count);
  confirming->unresolved = count;
  confirmer.unconfirmed += count;
  confirmer.by_id[confirming->id] = confirming;
  _rebuild_runs(confirming);
  std::vector<BasicScheduledMessageTracker> trackers;
  ConnectionWrapper used;
  std::weak_ptr<BasicClient> self = weak_from_this();
  _send(frame, false, lane, confirming->deadline, 0, &trackers, &used, [self, confirming](unsigned int written) {
    if (std::shared_ptr<BasicClient> client = self.lock()) {
      client->_confirming_written(confirming, written);
    }
  });
  if (!trackers.empty()) {
    confirming->frame_state = trackers.front();
  }
  if (Connection::pointer conn = used.lock()) {
    confirming->placed_on = conn.get();
  }
  _confirming_due(confirming);
}

// A confirmed send's end, the message `id` numbered `number`, of `type`:
// processed, its tracker, `state` when given, reads SENT and its `done`, when
// given, hears 1; else it is given up on for `reason`, told as a drop unless
// `report` is false (its frame's drop told it), a flush that waits for it
// not all written, its tracker LOST and its `done` 0. The `done` runs in
// the callbacks pass.
void BasicClient::_confirmed_end(std::uint64_t id, std::uint64_t number, std::uint32_t type,
                                 std::shared_ptr<SendState>* state, SendCallback* done, bool processed,
                                 DropReason reason, bool report) {
  if (!processed) {
    for (const FlushPtr& flush : outbox_->flushes) {
      if (number <= flush->confirm_mark) {
        flush->lost = true;
      }
    }
    if (report) {
      _report_record_drop(id, type, reason);
    }
  }
  if (state && *state && **state == SendState::QUEUED) {
    **state = processed ? SendState::SENT : SendState::LOST;
  }
  if (done && *done) {
    confirmer_->ends.emplace_back(SendCallback(), processed ? 1 : 0);
    confirmer_->ends.back().first.swap(*done);
  }
  if (!confirmer_->ends.empty() || !outbox_->flushes.empty()) {
    _post_callbacks();  // the ends, and a flush that waited for this
  }
}

// The batch's records [begin, end), which no frame could hold, past
// MAX_MESSAGE_SIZE in an envelope, given up on, NOT_CONFIRMED: none is
// sent unnumbered.
void BasicClient::_cannot_confirm(const BatchPtr& batch, std::size_t begin, std::size_t end) {
  MX_LOG(ERROR, LOWVERBOSITY,
         CTX("BasicClient") TEXT("a confirmed batch's message could not be made, its " + mx::repr(end - begin) +
                                 " message(s) are given up on; type " + mx::repr(batch->group.type)));
  for (std::size_t index = begin; index < end; ++index) {
    BatchRecord& record = batch->records[index];
    _confirmed_end(record.raw ? record.raw->id() : record.id, record.number, batch->group.type, &record.state,
                   &record.done, false, DropReason::NOT_CONFIRMED, true);
  }
}

// The batch's runs of consecutive numbers among its unresolved messages,
// registered anew: each unresolved number in exactly one run, so that a
// message sent again, which keeps its number, is found in its new batch.
void BasicClient::_rebuild_runs(const ConfirmingPtr& confirming) {
  Confirmer& confirmer = *confirmer_;
  for (std::uint64_t key : confirming->run_keys) {
    std::map<std::uint64_t, Confirmer::Run>::iterator found = confirmer.runs.find(key);
    if (found != confirmer.runs.end() && found->second.batch == confirming) {
      confirmer.runs.erase(found);
    }
  }
  confirming->run_keys.clear();
  if (!confirming->unresolved) {
    return;  // none left: its runs erased
  }
  const std::size_t count = confirming->count;
  if (confirming->unresolved == count && confirming->numbers.empty()) {
    // Nothing resolved and the numbers consecutive, as a batch's are when
    // it is made: one run, found at once.
    Confirmer::Run& run = confirmer.runs[confirming->first_number];
    run.batch = confirming;
    run.index = 0;
    run.count = count;
    run.unresolved = count;
    confirming->run_keys.push_back(confirming->first_number);
    return;
  }
  for (std::size_t index = 0; index < count;) {
    if (confirming->resolved[index]) {
      ++index;
      continue;
    }
    const std::uint64_t first = confirming->number(index);
    std::size_t length = 1;
    while (index + length < count && !confirming->resolved[index + length] &&
           confirming->number(index + length) == first + length) {
      ++length;
    }
    Confirmer::Run& run = confirmer.runs[first];
    run.batch = confirming;
    run.index = index;
    run.count = length;
    run.unresolved = length;
    confirming->run_keys.push_back(first);
    index += length;
  }
}

// The frame written: what is unresolved goes again retry_after from now,
// unless a DELIVERY_ERROR set an earlier time. Given up on instead, its
// drop told and its messages resolved by then (_confirmed_frame_dropped):
// nothing left, as a frame's every end but its write is a drop.
void BasicClient::_confirming_written(const ConfirmingPtr& confirming, unsigned int written) {
  std::unordered_map<std::uint64_t, ConfirmingPtr>::iterator found = confirmer_->by_id.find(confirming->id);
  if (found == confirmer_->by_id.end() || found->second != confirming) {
    return;  // resolved, sent again or given up on meanwhile
  }
  if (!written) {
    for (std::size_t index = 0; index < confirming->count; ++index) {
      _resolved(confirming, index, false, DropReason::NO_CONNECTION);
    }
    _forget_resolved(confirming);
    return;
  }
  confirming->written = true;
  confirming->retry_at = std::min(confirming->retry_at, _deadline_after(confirming->delivery.retry_after));
  _confirming_due(confirming);
}

// A confirmed batch's frame given up on (report_drop): each of its
// messages not resolved yet given up on for `reason` and told, once.
// Nothing for a batch forgotten, its messages resolved and told before, or
// sent again in another frame.
void BasicClient::_confirmed_frame_dropped(std::uint64_t envelope_id, DropReason reason) {
  std::unordered_map<std::uint64_t, ConfirmingPtr>::iterator found = confirmer_->by_id.find(envelope_id);
  if (found == confirmer_->by_id.end()) {
    return;
  }
  const ConfirmingPtr confirming = found->second;
  for (std::size_t index = 0; index < confirming->count; ++index) {
    _resolved(confirming, index, false, reason);
  }
  _forget_resolved(confirming);
}

// Message `index` of the batch resolved, unless it was: processed, its send
// ends with 1; else given up on for `reason`, told as a drop unless
// `report` is false (_confirmed_end). Its run is left to the caller
// (_resolve).
bool BasicClient::_resolved(const ConfirmingPtr& confirming, std::size_t index, bool processed, DropReason reason,
                            bool report) {
  if (confirming->resolved[index]) {
    return false;
  }
  confirming->resolved.set(index);
  --confirming->unresolved;
  --confirmer_->unconfirmed;
  Confirming::Followed* followed = confirming->followed.empty() ? nullptr : confirming->follow(index);
  _confirmed_end(!processed && report ? confirming->id_of(index) : 0, confirming->number(index), confirming->group.type,
                 followed ? &followed->state : nullptr, followed ? &followed->done : nullptr, processed, reason,
                 report);
  return true;
}

// The same, its run told.
void BasicClient::_resolve(const ConfirmingPtr& confirming, std::size_t index, bool processed, DropReason reason,
                           bool report) {
  if (_resolved(confirming, index, processed, reason, report)) {
    confirmer_->resolved_in_run(confirming, confirming->number(index), 1);
  }
}

// A batch with nothing unresolved left, forgotten.
void BasicClient::_forget_resolved(const ConfirmingPtr& confirming) {
  if (confirming->unresolved) {
    return;
  }
  Confirmer& confirmer = *confirmer_;
  std::unordered_map<std::uint64_t, ConfirmingPtr>::iterator found = confirmer.by_id.find(confirming->id);
  if (found != confirmer.by_id.end() && found->second == confirming) {
    confirmer.by_id.erase(found);
  }
  if (confirming->due != std::chrono::steady_clock::time_point::max()) {
    confirmer.by_due.erase(std::make_pair(confirming->due, confirming->id));
    confirming->due = std::chrono::steady_clock::time_point::max();
    _disarm_confirm_timer_when_idle();
  }
  _rebuild_runs(confirming);  // none left: its runs erased
}

// The unresolved messages `indexes` of the batch go again, in a batch of
// their own that goes at once with the same deadline, each keeping its
// number and end, its bytes and id read back out of the frame, and
// resolved here as moved, without an end; past the deadline they are given
// up on instead.
void BasicClient::_send_again(const ConfirmingPtr& confirming, const std::vector<std::size_t>& indexes) {
  Confirmer& confirmer = *confirmer_;
  MultiplexerMessage& envelope = confirmer.scratch;
  MessageBatch& inside = confirmer.batch_scratch;
  const bool late = confirming->deadline <= std::chrono::steady_clock::now();
  if (!late &&
      (!envelope.ParseFromString(confirming->frame->get_message()) || !inside.ParseFromString(envelope.batch()) ||
       static_cast<std::size_t>(inside.message_size()) != confirming->count)) {
    // Its own frame, which reads back; should it not, what would go again
    // is given up on.
    MX_LOG(ERROR, LOWVERBOSITY,
           CTX("BasicClient") TEXT("a confirmed batch's frame does not read back; its messages are given up on, id " +
                                   mx::repr(confirming->id)));
    for (std::size_t index : indexes) {
      _resolve(confirming, index, false, DropReason::NOT_CONFIRMED);
    }
    _forget_resolved(confirming);
    return;
  }
  BatchPtr again = std::make_shared<Batch>();
  again->group = confirming->group;
  again->delivery = confirming->delivery;
  again->backoff = confirming->backoff;
  again->deadline = confirming->deadline;
  again->closed = true;
  for (std::size_t index : indexes) {
    if (confirming->resolved[index]) {
      continue;
    }
    if (late) {
      _resolved(confirming, index, false, DropReason::NOT_CONFIRMED);
      continue;
    }
    confirming->resolved.set(index);
    --confirming->unresolved;
    --confirmer.unconfirmed;  // counted again in its new batch
    BatchRecord record;
    record.id = confirming->id_of(index);
    record.number = confirming->number(index);
    if (Confirming::Followed* followed = confirming->followed.empty() ? nullptr : confirming->follow(index)) {
      record.state = std::move(followed->state);
      record.done.swap(followed->done);
    }
    record.body.swap(*inside.mutable_message(static_cast<int>(index)));
    again->bytes += record.body.size();
    again->records.push_back(std::move(record));
  }
  _rebuild_runs(confirming);
  _forget_resolved(confirming);
  if (!again->records.empty()) {
    confirmer.sent_again += again->records.size();
    _send_batch(again, nullptr);
  }
}

void BasicClient::_send_unresolved_again(const ConfirmingPtr& confirming) {
  std::vector<std::size_t> indexes;
  indexes.reserve(confirming->unresolved);
  for (std::size_t index = 0; index < confirming->count; ++index) {
    if (!confirming->resolved[index]) {
      indexes.push_back(index);
    }
  }
  _send_again(confirming, indexes);
}

// The batch's place among those due: the earliest of its retry time, once
// written, and its deadline; the timer moved to it when it is the earliest.
void BasicClient::_confirming_due(const ConfirmingPtr& confirming) {
  Confirmer& confirmer = *confirmer_;
  const std::chrono::steady_clock::time_point due = std::min(
      confirming->written ? confirming->retry_at : std::chrono::steady_clock::time_point::max(), confirming->deadline);
  if (due == confirming->due) {
    return;
  }
  if (confirming->due != std::chrono::steady_clock::time_point::max()) {
    confirmer.by_due.erase(std::make_pair(confirming->due, confirming->id));
  }
  confirming->due = due;
  if (due != std::chrono::steady_clock::time_point::max()) {
    confirmer.by_due.emplace(due, confirming->id);
    if (due < confirmer.armed) {
      _arm_confirm_timer();
    }
  } else {
    _disarm_confirm_timer_when_idle();
  }
}

// With nothing due, the timer stopped: an armed timer is work for the
// loop, which a SyncClient's wait with no deadline would wait out where it
// ends at once with nothing to wait for.
void BasicClient::_disarm_confirm_timer_when_idle() {
  Confirmer& confirmer = *confirmer_;
  if (confirmer.by_due.empty() && confirmer.armed != std::chrono::steady_clock::time_point::max() &&
      confirmer.armed != std::chrono::steady_clock::time_point::min()) {
    asio::error_code cancelled;
    confirmer.timer.cancel(cancelled);
    confirmer.armed = std::chrono::steady_clock::time_point::max();
  }
}

void BasicClient::_arm_confirm_timer() {
  Confirmer& confirmer = *confirmer_;
  if (confirmer.by_due.empty()) {
    return;
  }
  const std::chrono::steady_clock::time_point earliest = confirmer.by_due.begin()->first;
  if (earliest >= confirmer.armed) {
    return;
  }
  confirmer.armed = earliest;
  confirmer.timer.expires_at(earliest);
  std::weak_ptr<BasicClient> self = weak_from_this();
  confirmer.timer.async_wait([self, earliest](const asio::error_code& error) {
    if (error) {
      return;  // cancelled, or moved to an earlier time
    }
    if (std::shared_ptr<BasicClient> client = self.lock()) {
      if (client->confirmer_->armed == earliest) {
        client->_confirm_timer_fired();
      }
    }
  });
}

// The batches whose time came: what is left of one past its deadline
// given up on, NOT_CONFIRMED, and what is unresolved of one whose retry
// time came sent again; the timer runs on to the next, armed once at the
// end.
void BasicClient::_confirm_timer_fired() {
  MX_DCHECK_RUN_ON(&owner_thread());
  Confirmer& confirmer = *confirmer_;
  confirmer.armed = std::chrono::steady_clock::time_point::min();  // armed below, once
  const std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now();
  std::vector<ConfirmingPtr> due;
  for (const std::pair<std::chrono::steady_clock::time_point, std::uint64_t>& entry : confirmer.by_due) {
    if (entry.first > now) {
      break;
    }
    std::unordered_map<std::uint64_t, ConfirmingPtr>::iterator found = confirmer.by_id.find(entry.second);
    if (found != confirmer.by_id.end()) {
      due.push_back(found->second);
    }
  }
  for (const ConfirmingPtr& confirming : due) {
    if (confirming->deadline <= now) {
      for (std::size_t index = 0; index < confirming->count; ++index) {
        _resolved(confirming, index, false, DropReason::NOT_CONFIRMED);
      }
    }
    if (!confirming->unresolved) {
      _forget_resolved(confirming);
    } else if (confirming->written && confirming->retry_at <= now) {
      _send_unresolved_again(confirming);
    } else {
      _confirming_due(confirming);
    }
  }
  confirmer.armed = std::chrono::steady_clock::time_point::max();
  _arm_confirm_timer();
}

// The segments of this client's runs that a CONFIRMATION's list names,
// found before any is resolved: resolving erases a run once none of it is
// left. A pair that does not read, empty or past the last number there is,
// names nothing.
std::vector<BasicClient::Segment> BasicClient::_segments(
    const google::protobuf::RepeatedField<std::uint64_t>& pairs) const {
  std::map<std::uint64_t, Confirmer::Run>& runs = confirmer_->runs;
  std::vector<Segment> found;
  for (int pair = 0; pair + 1 < pairs.size(); pair += 2) {
    const std::uint64_t first = pairs.Get(pair);
    const std::uint64_t past = first + pairs.Get(pair + 1);
    if (past <= first) {
      continue;
    }
    std::map<std::uint64_t, Confirmer::Run>::iterator run = runs.upper_bound(first);
    if (run != runs.begin()) {
      --run;
    }
    for (; run != runs.end() && run->first < past; ++run) {
      const std::uint64_t from = std::max(first, run->first);
      const std::uint64_t to = std::min(past, run->first + run->second.count);
      if (from < to) {
        found.push_back(Segment{run->second.batch, run->first, run->second.index + (from - run->first),
                                run->second.index + (to - run->first)});
      }
    }
  }
  return found;
}

// A CONFIRMATION: processed numbers end their sends with 1, failed ones are
// given up on, refused ones go again at once; a batch that heard of one of
// its messages waits retry_after again before it sends what is left. A
// number this client does not wait for, resolved before, is passed by, as
// is a payload that does not read: what it names goes again in time.
void BasicClient::_on_confirmation(const MultiplexerMessage& mxmsg) {
  Confirmer& confirmer = *confirmer_;
  Confirmation& confirmation = confirmer.confirmation;
  if (!confirmation.ParseFromString(mxmsg.message())) {
    return;
  }
  std::vector<ConfirmingPtr> heard;
  const auto resolve = [this, &confirmer, &heard](const std::vector<Segment>& found, bool processed,
                                                  DropReason reason) {
    for (const Segment& segment : found) {
      std::size_t resolved = 0;
      if (processed && segment.batch->followed.empty()) {
        // Nothing of it hears an end: its flags set a word at a time.
        resolved = segment.batch->resolved.set_range(segment.begin, segment.end);
        segment.batch->unresolved -= resolved;
        confirmer.unconfirmed -= resolved;
        if (resolved && !outbox_->flushes.empty()) {
          _post_callbacks();  // a flush that waited for these
        }
      } else {
        for (std::size_t index = segment.begin; index < segment.end; ++index) {
          resolved += _resolved(segment.batch, index, processed, reason);
        }
      }
      if (resolved) {
        confirmer.resolved_in_run(segment.batch, segment.key, resolved);
        if (heard.empty() || heard.back() != segment.batch) {
          heard.push_back(segment.batch);
        }
      }
    }
  };
  resolve(_segments(confirmation.processed()), true, DropReason::NOT_CONFIRMED);
  resolve(_segments(confirmation.failed()), false, DropReason::FAILED);
  std::vector<std::pair<ConfirmingPtr, std::vector<std::size_t>>> refused;
  for (const Segment& segment : _segments(confirmation.refused())) {
    if (refused.empty() || refused.back().first != segment.batch) {
      refused.emplace_back(segment.batch, std::vector<std::size_t>());
    }
    for (std::size_t index = segment.begin; index < segment.end; ++index) {
      refused.back().second.push_back(index);
    }
  }
  for (const std::pair<ConfirmingPtr, std::vector<std::size_t>>& each : refused) {
    _send_again(each.first, each.second);
  }
  std::sort(heard.begin(), heard.end());
  heard.erase(std::unique(heard.begin(), heard.end()), heard.end());
  const std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now();
  for (const ConfirmingPtr& confirming : heard) {
    if (!confirming->unresolved) {
      _forget_resolved(confirming);
      continue;
    }
    confirming->backoff = 0;  // somebody takes its messages
    if (confirming->written) {
      confirming->retry_at = _deadline_after(confirming->delivery.retry_after, now);
    }
    _confirming_due(confirming);
  }
}

// A DELIVERY_ERROR for a confirmed batch, which nobody took: what is
// unresolved goes again after a back-off, from 0.1 s, doubling to 1 s.
// False for an envelope this client does not wait for.
bool BasicClient::_confirmed_batch_not_taken(std::uint64_t envelope_id) {
  Confirmer& confirmer = *confirmer_;
  std::unordered_map<std::uint64_t, ConfirmingPtr>::iterator found = confirmer.by_id.find(envelope_id);
  if (found == confirmer.by_id.end()) {
    return false;
  }
  const ConfirmingPtr confirming = found->second;
  confirming->backoff = confirming->backoff > 0 ? std::min(1.0f, confirming->backoff * 2) : 0.1f;
  confirming->written = true;
  confirming->retry_at = _deadline_after(confirming->backoff);
  _confirming_due(confirming);
  return true;
}

// The batches, in the order of their first messages' numbers, which is
// the order they were sent in, but for what went again.
std::vector<BasicClient::ConfirmingPtr> BasicClient::_in_number_order(std::vector<ConfirmingPtr> batches) {
  std::sort(batches.begin(), batches.end(),
            [](const ConfirmingPtr& left, const ConfirmingPtr& right) { return left->number(0) < right->number(0); });
  return batches;
}

// A connection closed: the batches written on it whose messages are not all
// resolved go again at once, in the order they were sent. One placed on it
// and not written went to another connection with the rest of its queue
// (handle_orphaned_outgoing_messages), its connection unknown from now on.
// What a client whose handlers run as messages arrive owes goes now too,
// on another connection: the read it waited for will not come.
void BasicClient::_confirmations_on_closed(Connection* conn) {
  Confirmer& confirmer = *confirmer_;
  std::vector<ConfirmingPtr> again;
  for (const std::pair<const std::uint64_t, ConfirmingPtr>& entry : confirmer.by_id) {
    const ConfirmingPtr& confirming = entry.second;
    if (confirming->placed_on != conn) {
      continue;
    }
    confirming->placed_on = nullptr;
    const bool written =
        confirming->written || (confirming->frame_state && *confirming->frame_state == SendState::SENT);
    if (written && confirming->unresolved) {
      again.push_back(confirming);
    }
  }
  for (const ConfirmingPtr& confirming : _in_number_order(std::move(again))) {
    _send_unresolved_again(confirming);
  }
  if (incoming_sink_ && confirmer.owed_count && !shuts_down_) {
    send_confirmations();
  }
}

// Every message still waiting for its confirmation given up on, for
// `reason`, in the order sent: the client shuts down. A frame of theirs
// dropped later finds them resolved and tells nothing more.
void BasicClient::_give_up_confirmations(DropReason reason) {
  Confirmer& confirmer = *confirmer_;
  std::vector<ConfirmingPtr> waiting;
  waiting.reserve(confirmer.by_id.size());
  for (const std::pair<const std::uint64_t, ConfirmingPtr>& entry : confirmer.by_id) {
    waiting.push_back(entry.second);
  }
  confirmer.by_id.clear();
  for (const ConfirmingPtr& confirming : _in_number_order(std::move(waiting))) {
    for (std::size_t index = 0; index < confirming->count; ++index) {
      _resolved(confirming, index, false, reason);
    }
  }
  confirmer.runs.clear();
  confirmer.by_due.clear();
  asio::error_code cancelled;
  confirmer.timer.cancel(cancelled);
  confirmer.armed = std::chrono::steady_clock::time_point::max();
}

bool BasicClient::_confirming_up_to(std::uint64_t number) const {
  return !confirmer_->runs.empty() && confirmer_->runs.begin()->first <= number;
}

std::uint64_t BasicClient::_confirm_mark() const { return confirmer_->last_number; }

bool BasicClient::_confirmed_ends_due() const { return !confirmer_->ends.empty(); }

// The ends of the confirmed sends resolved, in the order resolved.
void BasicClient::_run_confirmed_ends() {
  std::vector<std::pair<SendCallback, unsigned int>> ends;
  ends.swap(confirmer_->ends);
  for (std::pair<SendCallback, unsigned int>& end : ends) {
    try {
      end.first(end.second);
    } catch (const std::exception& error) {
      MX_LOG(ERROR, LOWVERBOSITY, CTX("BasicClient") TEXT(std::string("a send's callback raised: ") + error.what()));
    }
  }
}

void BasicClient::_release_confirmed_ends() { confirmer_->ends.clear(); }

std::size_t BasicClient::unconfirmed() const {
  MX_DCHECK_RUN_ON(&owner_thread());
  return confirmer_->unconfirmed;
}

std::uint64_t BasicClient::sent_again() const {
  MX_DCHECK_RUN_ON(&owner_thread());
  return confirmer_->sent_again;
}

// The consumer's side.

void BasicClient::confirm(std::uint64_t sender, std::uint64_t number, Confirmed outcome) {
  MX_DCHECK_RUN_ON(&owner_thread());
  if (!number) {
    return;
  }
  Confirmer& confirmer = *confirmer_;
  OwedConfirmations& owed = confirmer.owed[sender];
  add_number(outcome == Confirmed::PROCESSED ? &owed.processed
             : outcome == Confirmed::REFUSED ? &owed.refused
                                             : &owed.failed,
             number);
  ++owed.count;
  ++confirmer.owed_count;
  if (owed.count >= CONFIRMATIONS_GATHERED_MAX) {
    _send_owed(sender, owed);
  }
}

void BasicClient::send_confirmations() {
  MX_DCHECK_RUN_ON(&owner_thread());
  Confirmer& confirmer = *confirmer_;
  if (!confirmer.owed_count) {
    return;
  }
  for (std::pair<const std::uint64_t, OwedConfirmations>& entry : confirmer.owed) {
    if (entry.second.count) {
      _send_owed(entry.first, entry.second);
    }
  }
  // A producer met keeps its entry, the connection its messages came on,
  // for the next ones; past a few thousand, those that owe nothing go, so
  // that producers met once each do not pile up.
  if (confirmer.owed.size() > Confirmer::OWED_KEPT) {
    for (std::unordered_map<std::uint64_t, OwedConfirmations>::iterator each = confirmer.owed.begin();
         each != confirmer.owed.end();) {
      each = each->second.count ? std::next(each) : confirmer.owed.erase(each);
    }
  }
}

std::size_t BasicClient::confirmations_owed() const {
  MX_DCHECK_RUN_ON(&owner_thread());
  return confirmer_->owed_count;
}

bool BasicClient::confirm_timer_armed() const {
  MX_DCHECK_RUN_ON(&owner_thread());
  return confirmer_->armed != std::chrono::steady_clock::time_point::max();
}

void BasicClient::_confirmed_batch_from(const Connection::pointer& conn, std::uint64_t producer) {
  confirmer_->owed[producer].connection = conn;
}

// What this client owes `producer`, in one CONFIRMATION, on the connection
// its messages came on, else another live one, DEFAULT_TIMEOUT at most.
void BasicClient::_send_owed(std::uint64_t producer, OwedConfirmations& owed) {
  Confirmer& confirmer = *confirmer_;
  Confirmation& confirmation = confirmer.confirmation;
  confirmation.Clear();
  confirmation.mutable_processed()->Add(owed.processed.begin(), owed.processed.end());
  confirmation.mutable_refused()->Add(owed.refused.begin(), owed.refused.end());
  confirmation.mutable_failed()->Add(owed.failed.begin(), owed.failed.end());
  confirmer.owed_count -= owed.count;
  owed.count = 0;
  owed.processed.clear();
  owed.refused.clear();
  owed.failed.clear();
  MultiplexerMessage mxmsg;
  mxmsg.set_id(random64());
  mxmsg.set_sender(instance_id());
  mxmsg.set_to(producer);
  mxmsg.set_type(types::CONFIRMATION);
  mxmsg.set_report_delivery_error(false);  // lost, its messages go again in time
  confirmation.SerializeToString(mxmsg.mutable_message());
  LanePtr lane;
  Connection::pointer conn = owed.connection.lock();
  if (conn && conn->living()) {
    lane = std::make_shared<Lane>(_wrap(conn), false);
  }
  std::shared_ptr<const RawMessage> raw(RawMessage::FromMessage(mxmsg));
  _send(raw, false, lane, _deadline_after(DEFAULT_TIMEOUT), 0, NULL, NULL, SendCallback());
}

}  // namespace multiplexer
