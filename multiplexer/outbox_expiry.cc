// The outbox's deadlines: a message waits `timeout` seconds at most, and
// one timer, run to the earliest deadline, drops what still waits then
// (_expire). Part of the outbox; see outbox.cc.
//
// What still waits at its deadline is dropped, its tracker reading LOST,
// and reported. A message with no time to wait at all is given up on at
// once (_give_up). The heap of deadlines skips the entries of messages
// that left, and clears them out when it has doubled; a queue clears out
// its ended entries once they are as many as the live ones and 64 more
// (_clear_out): what the outbox keeps stays bounded however long a
// connection stays full.
#include <algorithm>

#include "multiplexer/outbox.h"

namespace multiplexer {

void BasicClient::drop_held(const BasicScheduledMessageTracker& state) {
  MX_DCHECK_RUN_ON(&owner_thread());
  for (const WaitingPtr& waiting : outbox_->held) {
    if (!waiting->over && waiting->state == state) {
      _drop_expired(waiting);
      return;
    }
  }
}

// A message with no time left to wait, which no connection takes now:
// reported dropped for `reason` at once, its tracker, made here when
// null, reading LOST. Nothing counts it as waiting.
BasicClient::BasicScheduledMessageTracker BasicClient::_give_up(const std::shared_ptr<const RawMessage>& raw,
                                                                BasicScheduledMessageTracker state,
                                                                std::uint64_t number, DropReason reason) {
  raw->mark_number(number);
  report_drop(raw, reason);
  if (!state) {
    return std::make_shared<SendState>(SendState::LOST);
  }
  if (*state == SendState::QUEUED) {
    *state = SendState::LOST;
    if (state.use_count() > 1) {
      tracked_message_done(state, false);  // somebody follows it
    }
  }
  return state;
}

// A waiting message's deadline goes into the heap, and the timer moves to
// it when it is the earliest. Entries of messages that left are cleared out
// when the heap has doubled since the last time.
void BasicClient::_expire_at(const WaitingPtr& waiting) {
  if (waiting->deadline == std::chrono::steady_clock::time_point::max()) {
    return;  // no deadline: it waits until written or gone
  }
  Outbox& outbox = *outbox_;
  if (outbox.expiring.size() >= outbox.compact_at) {
    outbox.expiring.erase(std::remove_if(outbox.expiring.begin(), outbox.expiring.end(),
                                         [](const Outbox::Expiring& entry) {
                                           WaitingPtr each = entry.waiting.lock();
                                           return !each || each->over;
                                         }),
                          outbox.expiring.end());
    std::make_heap(outbox.expiring.begin(), outbox.expiring.end());
    outbox.compact_at = std::max<std::size_t>(64, 2 * outbox.expiring.size());
  }
  outbox.expiring.push_back(Outbox::Expiring{waiting->deadline, waiting});
  std::push_heap(outbox.expiring.begin(), outbox.expiring.end());
  if (waiting->deadline < outbox.armed) {
    _arm_expiry();
  }
}

// The timer set to the earliest deadline of a message still waiting, or
// stopped when none waits.
void BasicClient::_arm_expiry() {
  Outbox& outbox = *outbox_;
  while (!outbox.expiring.empty()) {
    WaitingPtr waiting = outbox.expiring.front().waiting.lock();
    if (waiting && !waiting->over) {
      break;
    }
    std::pop_heap(outbox.expiring.begin(), outbox.expiring.end());
    outbox.expiring.pop_back();
  }
  asio::error_code ignored;
  if (outbox.expiring.empty()) {
    outbox.timer.cancel(ignored);
    outbox.armed = std::chrono::steady_clock::time_point::max();
    return;
  }
  const std::chrono::steady_clock::time_point next = outbox.expiring.front().deadline;
  if (next == outbox.armed) {
    return;
  }
  outbox.armed = next;
  outbox.timer.expires_at(next);  // cancels a wait set for another time
  std::weak_ptr<BasicClient> self = weak_from_this();
  outbox.timer.async_wait([self](const asio::error_code& error) {
    if (error == asio::error::operation_aborted) {
      return;
    }
    if (std::shared_ptr<BasicClient> client = self.lock()) {
      client->outbox_->armed = std::chrono::steady_clock::time_point::max();
      client->_expire();
    }
  });
}

// The timer: what waited past its deadline is dropped, its tracker reading
// LOST, and reported.
void BasicClient::_expire() {
  Outbox& outbox = *outbox_;
  const std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now();
  while (!outbox.expiring.empty() && outbox.expiring.front().deadline <= now) {
    WaitingPtr waiting = outbox.expiring.front().waiting.lock();
    std::pop_heap(outbox.expiring.begin(), outbox.expiring.end());
    outbox.expiring.pop_back();
    if (!waiting || waiting->over) {
      continue;
    }
    _drop_expired(waiting);
  }
  outbox.backlogs.erase(std::remove_if(outbox.backlogs.begin(), outbox.backlogs.end(),
                                       [](const Backlog& backlog) { return !backlog.live; }),
                        outbox.backlogs.end());
  _arm_expiry();
  _check_flushes();
}

// A waiting message whose time is up: dropped and reported, its tracker
// reading LOST, and a send somebody follows told so. Its frame goes now,
// its entry when its queue is cleared out.
void BasicClient::_drop_expired(const WaitingPtr& waiting) {
  Outbox& outbox = *outbox_;
  Backlog* backlog = _backlog(waiting->on);
  if (backlog) {
    --backlog->live;
  }
  const bool held = !waiting->on;
  if (held) {
    --outbox.held_live;
  }
  waiting->over = true;
  --outbox.waiting;
  _wait_flushes(waiting->number, -1);
  // One that waited on a connection not registered yet waited for that
  // connection, not for room on it.
  Connection::pointer on = backlog ? backlog->connection.lock() : Connection::pointer();
  report_drop(waiting->raw, held || (on && !on->registered()) ? DropReason::NO_CONNECTION : DropReason::NO_ROOM);
  if (waiting->state && *waiting->state == SendState::QUEUED) {
    *waiting->state = SendState::LOST;
    if (waiting->state.use_count() > 1) {
      tracked_message_done(waiting->state, false);  // somebody follows it
    }
  }
  waiting->raw.reset();
  if (held) {
    _clear_out(outbox.held, outbox.held_live);
  } else if (backlog) {
    _clear_out(backlog->waiting, backlog->live);
  }
}

// Clears the entries of `queue` that ended there, `live` of them waiting
// still, once the ended ones are as many as the live ones and 64 more, or
// none is live: however long no connection comes up, or one stays full,
// while messages keep ending, the queue holds those that wait and a
// bounded rest, at an amortized constant cost per message.
void BasicClient::_clear_out(std::deque<WaitingPtr>& queue, std::size_t live) {
  if (live != 0 && queue.size() < 2 * live + 64) {
    return;
  }
  queue.erase(std::remove_if(queue.begin(), queue.end(), [](const WaitingPtr& waiting) { return waiting->over; }),
              queue.end());
}

}  // namespace multiplexer
