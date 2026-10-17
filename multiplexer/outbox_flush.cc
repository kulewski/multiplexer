// The outbox's ends: a send's `done` hears its end once, and flush_all()
// waits for what was sent before it (begin_flush). Part of the outbox; see
// outbox.cc.
//
// flush_all(): every message is numbered in the order sent. A flush waits
// for those numbered up to the last one at its start: the ones that still
// wait, for room or held, and per connection the last of them queued
// there, which everything queued there before it goes out ahead of. The
// copies of a followed send, written or given up on, are heard in one pass
// per loop turn (_process_callbacks), outside the connection's handlers
// and the outbox's bookkeeping, after the drop observer heard of the drops
// reported meanwhile, and a flush that waits for those callbacks ends after
// them.
#include <algorithm>

#include "lib/logging/logging.h"
#include "multiplexer/outbox.h"

namespace multiplexer {

// `done` hears the end of the send whose copies are `copies`; see send().
// A copy given up on already, with no time to wait, is heard as any end
// is, posted.
void BasicClient::_follow(const std::vector<BasicScheduledMessageTracker>& copies, SendCallback done) {
  FollowPtr follow(new Follow());
  follow->done = done;
  follow->copies = copies;
  follow->live = copies.size();
  for (const BasicScheduledMessageTracker& copy : copies) {
    outbox_->follows[copy.get()] = follow;
  }
  for (const BasicScheduledMessageTracker& copy : copies) {
    if (copy && *copy == SendState::LOST) {
      _follow_event(copy, false);
    }
  }
}

// A followed copy was written or given up on: noted, from inside the
// connection's handlers, and handled in one pass per loop turn.
void BasicClient::_follow_event(const std::shared_ptr<SendState>& state, bool written) {
  Outbox& outbox = *outbox_;
  if (outbox.follows.find(state.get()) == outbox.follows.end()) {
    return;  // a flush's mark, or a send nobody follows
  }
  outbox.follow_events.emplace_back(state, written);
  _post_callbacks();
}

// The pass that runs the program's callbacks, posted once for whatever
// gathers until it runs.
void BasicClient::_post_callbacks() {
  Outbox& outbox = *outbox_;
  if (outbox.callbacks_posted) {
    return;
  }
  outbox.callbacks_posted = true;
  std::weak_ptr<BasicClient> self = weak_from_this();
  io_service_.post([self] {
    if (std::shared_ptr<BasicClient> client = self.lock()) {
      client->outbox_->callbacks_posted = false;
      client->_process_callbacks();
    }
  });
}

// The program's callbacks of a loop turn: the drops reported, told to the
// observer, then the followed sends' ends, and then the flushes that
// waited for them. Nothing of the outbox is in the middle of a change
// here, so a callback may send, or shut the client down.
void BasicClient::_process_callbacks() {
  _tell_drops();
  _run_confirmed_ends();
  _process_follows();
}

// Each drop reported, told to the observer: the one there is now, called
// through a copy, since a callback that shuts a Python client down lets go
// of it. One let go of meanwhile is told nothing more.
void BasicClient::_tell_drops() {
  MX_DCHECK_RUN_ON(&owner_thread());
  std::vector<std::pair<std::uint64_t, DropReason>> events;
  events.swap(outbox_->drop_events);
  for (const std::pair<std::uint64_t, DropReason>& event : events) {
    DropObserver observer = drop_observer_;
    if (!observer) {
      break;
    }
    try {
      observer(event.first, event.second);
    } catch (const std::exception& error) {
      MX_LOG(ERROR, LOWVERBOSITY, CTX("BasicClient") TEXT(std::string("the drop observer raised: ") + error.what()));
    }
  }
}

// The followed sends whose copies were written or given up on: one written
// ends a send with 1, the last given up on with 0.
void BasicClient::_process_follows() {
  MX_DCHECK_RUN_ON(&owner_thread());
  std::vector<std::pair<std::shared_ptr<SendState>, bool>> events;
  events.swap(outbox_->follow_events);
  for (const std::pair<std::shared_ptr<SendState>, bool>& event : events) {
    std::unordered_map<const SendState*, FollowPtr>::iterator found = outbox_->follows.find(event.first.get());
    if (found == outbox_->follows.end()) {
      continue;  // its send ended meanwhile
    }
    FollowPtr follow = found->second;
    outbox_->follows.erase(found);
    --follow->live;
    if (event.second) {
      _end_follow(follow, 1);
    } else if (!follow->live) {
      _end_follow(follow, 0);
    }
  }
  _check_flushes();  // a flush waiting for these callbacks ends now
}

// A followed send ends: its copies are not followed any more, and `done`,
// called once, hears `written`.
void BasicClient::_end_follow(const FollowPtr& follow, unsigned int written) {
  if (follow->over) {
    return;
  }
  follow->over = true;
  for (const BasicScheduledMessageTracker& copy : follow->copies) {
    std::unordered_map<const SendState*, FollowPtr>::iterator found = outbox_->follows.find(copy.get());
    if (found != outbox_->follows.end() && found->second == follow) {
      outbox_->follows.erase(found);
    }
  }
  SendCallback done;
  done.swap(follow->done);
  try {
    done(written);
  } catch (const std::exception& error) {
    MX_LOG(ERROR, LOWVERBOSITY, CTX("BasicClient") TEXT(std::string("a send's callback raised: ") + error.what()));
  }
}

void BasicClient::release_follows() {
  MX_DCHECK_RUN_ON(&owner_thread());
  outbox_->follows.clear();
  outbox_->follow_events.clear();
  _release_confirmed_ends();
  _check_flushes();
}

void BasicClient::end_follows() {
  MX_DCHECK_RUN_ON(&owner_thread());
  _tell_drops();
  _run_confirmed_ends();
  std::vector<FollowPtr> left;
  for (const std::pair<const SendState* const, FollowPtr>& entry : outbox_->follows) {
    if (!entry.second->over) {
      left.push_back(entry.second);
    }
  }
  outbox_->follow_events.clear();
  for (const FollowPtr& follow : left) {
    _end_follow(follow, 0);
  }
  _check_flushes();
}

BasicClient::FlushPtr BasicClient::begin_flush(std::function<void(bool all_written)> done) {
  MX_DCHECK_RUN_ON(&owner_thread());
  // What waits in a batch goes now, and the flush waits for it; a batch
  // given up on at once, with no connection to wait for, is lost to it.
  // The confirmations this client owes go too. A confirmed send numbered
  // before it is waited for until confirmed or given up on.
  const std::uint64_t dropped = dropped_.load(std::memory_order_relaxed);
  close_batches();
  send_confirmations();
  const bool lost = dropped_.load(std::memory_order_relaxed) != dropped;
  if (!outbox_->follow_events.empty() || !outbox_->drop_events.empty() || _confirmed_ends_due()) {
    _process_callbacks();  // what ended before the flush began is heard before it ends
  }
  FlushPtr flush(new Flush());
  flush->last_number = outbox_->last_number;
  flush->confirm_mark = _confirm_mark();
  flush->waiting = outbox_->waiting;
  flush->done = done;
  flush->lost = lost;
  for (ConnectionById::const_iterator entry = connection_by_id_.begin(); entry != connection_by_id_.end(); ++entry) {
    Connection::pointer conn = entry->second.lock();
    if (conn && conn->living()) {
      if (BasicScheduledMessageTracker tracker = track_last_queued(conn)) {
        flush->marks.emplace_back(conn.get(), tracker);
      }
    }
  }
  outbox_->flushes.push_back(flush);
  _check_flushes();
  return flush;
}

bool BasicClient::flushed(const FlushPtr& flush) const { return flush->flushed; }

bool BasicClient::all_written(const FlushPtr& flush) const { return flush->flushed && !flush->lost; }

void BasicClient::end_flush(const FlushPtr& flush) {
  MX_DCHECK_RUN_ON(&owner_thread());
  outbox_->flushes.erase(std::remove(outbox_->flushes.begin(), outbox_->flushes.end(), flush), outbox_->flushes.end());
}

// `copies` more (or fewer) of message `number` wait: counted by every flush
// that waits for that message.
void BasicClient::_wait_flushes(std::uint64_t number, int copies) {
  for (const FlushPtr& flush : outbox_->flushes) {
    if (number > flush->last_number) {
      continue;
    }
    if (copies >= 0) {
      flush->waiting += static_cast<std::size_t>(copies);
    } else {
      flush->waiting -= static_cast<std::size_t>(-copies);
    }
  }
}

// A flush is done once the messages it counts no longer wait and every
// frame it marked is out, written or lost, every confirmed send numbered
// up to its mark is resolved, and the callbacks of the sends that ended
// meanwhile and of the drops have run: a pass of them due
// (_process_callbacks) checks again. `done` is told after the flush left
// the list.
void BasicClient::_check_flushes() {
  if (outbox_->flushes.empty() || !outbox_->follow_events.empty() || !outbox_->drop_events.empty() ||
      _confirmed_ends_due()) {
    return;
  }
  std::vector<FlushPtr> finished;
  for (std::vector<FlushPtr>::iterator each = outbox_->flushes.begin(); each != outbox_->flushes.end();) {
    Flush& flush = **each;
    flush.marks.erase(std::remove_if(flush.marks.begin(), flush.marks.end(),
                                     [](const std::pair<const Connection*, BasicScheduledMessageTracker>& mark) {
                                       return *mark.second != SendState::QUEUED;
                                     }),
                      flush.marks.end());
    if (flush.waiting || !flush.marks.empty() || (flush.confirm_mark && _confirming_up_to(flush.confirm_mark))) {
      ++each;
      continue;
    }
    flush.flushed = true;
    finished.push_back(*each);
    each = outbox_->flushes.erase(each);
  }
  for (const FlushPtr& flush : finished) {
    if (flush->done) {
      flush->done(!flush->lost);
    }
  }
}

}  // namespace multiplexer
