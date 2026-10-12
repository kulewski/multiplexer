// The outbox's ends: a send's `done` hears its end once, and flush_all()
// waits for what was sent before it (begin_flush). Part of the outbox; see
// outbox.cc.
//
// flush_all(): every message is numbered in the order sent. A flush waits
// for those numbered up to the last one at its start: the ones that still
// wait, for room or held, and per connection the last of them queued
// there, which everything queued there before it goes out ahead of. The
// copies of a followed send, written or given up on, are heard in one pass
// per loop turn (_process_follows), outside the connection's handlers, and
// a flush that waits for those callbacks ends after them.
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
  if (outbox.follow_posted) {
    return;
  }
  outbox.follow_posted = true;
  std::weak_ptr<BasicClient> self = weak_from_this();
  io_service_.post([self] {
    if (std::shared_ptr<BasicClient> client = self.lock()) {
      client->outbox_->follow_posted = false;
      client->_process_follows();
    }
  });
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
  _check_flushes();
}

void BasicClient::end_follows() {
  MX_DCHECK_RUN_ON(&owner_thread());
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
  if (!outbox_->follow_events.empty()) {
    _process_follows();  // what ended before the flush began is heard before it ends
  }
  FlushPtr flush(new Flush());
  flush->last_number = outbox_->last_number;
  flush->waiting = outbox_->waiting;
  flush->done = done;
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
// frame it marked is out, written or lost, and the callbacks of the sends
// that ended meanwhile have run: a pass of them due (_process_follows)
// checks again. `done` is told after the flush left the list.
void BasicClient::_check_flushes() {
  if (outbox_->flushes.empty() || !outbox_->follow_events.empty()) {
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
    if (flush.waiting || !flush.marks.empty()) {
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
