// The outbox: what a client's connections cannot take yet.
//
// A message goes into its connection's queue while the queue has room and
// nothing waits for that connection before it; otherwise it waits in the
// connection's backlog. When a full queue has room again, the connection
// says so (outgoing_queue_has_room) and the backlog moves in, in order, as
// far as the room goes: an event costs what it moves, and nothing polls. A
// message given no connection goes to one with room, round robin, or, with
// every one full, waits on the one with the least waiting. A message waits
// `timeout` seconds at most: one timer runs to the earliest deadline, and
// what still waits then is dropped, its tracker reading LOST. A connection
// that dies hands its queue to the others, as it always did, and after it
// what waited in its backlog: an ALL send's copy is dropped, the other
// connections having theirs, a pinned message is lost, and the rest goes
// to another connection, whose room it waits for in turn.
//
// Both clients share it. The threaded client's io thread and the
// synchronous client's calls run the same loop, so what waits moves as soon
// as the loop runs: for the synchronous client, inside its next call, which
// is also when anything it queued is written.
//
// flush_all(): every message is numbered in the order sent. A flush waits
// for those numbered up to the last one at its start: the ones that still
// wait, or that the caller holds (the threaded client, while no connection
// is live), and per connection the last of them queued there, which
// everything queued there before it goes out ahead of.
#include "multiplexer/outbox.h"

#include <algorithm>

#include "lib/logging/logging.h"
#include "lib/repr.h"

namespace multiplexer {

using mx::repr;

namespace {
// When a message given `timeout` seconds stops waiting; a timeout that is
// not positive gets the default.
std::chrono::steady_clock::time_point deadline_after(float timeout) {
  return std::chrono::steady_clock::now() +
         std::chrono::microseconds(static_cast<long>((timeout > 0 ? timeout : DEFAULT_TIMEOUT) * 1e6));
}
}  // namespace

std::uint64_t BasicClient::next_number() {
  MX_DCHECK_RUN_ON(&owner_thread());
  return ++outbox_->last_number;
}

std::uint64_t BasicClient::retries() const { return outbox_->retries; }

BasicClient::BasicScheduledMessageTracker BasicClient::schedule_one(std::shared_ptr<const RawMessage> raw,
                                                                    ConnectionWrapper* used, float timeout,
                                                                    std::uint64_t number, LanePtr lane) {
  MX_DCHECK_RUN_ON(&owner_thread());
  Connection::pointer conn = _choose_connection();
  if (!conn) {
    return BasicScheduledMessageTracker();
  }
  if (used) {
    *used = _wrap(conn);
  }
  return _place_on(conn, raw, BasicScheduledMessageTracker(), number ? number : next_number(), deadline_after(timeout),
                   false, lane);
}

BasicClient::BasicScheduledMessageTracker BasicClient::schedule_on(std::shared_ptr<const RawMessage> raw,
                                                                   const ConnectionWrapper& wrapper, float timeout,
                                                                   std::uint64_t number, LanePtr lane) {
  MX_DCHECK_RUN_ON(&owner_thread());
  Connection::pointer conn = wrapper.lock();
  if (!conn || !conn->living()) {
    return BasicScheduledMessageTracker();
  }
  return _place_on(conn, raw, BasicScheduledMessageTracker(), number ? number : next_number(), deadline_after(timeout),
                   false, lane);
}

unsigned int BasicClient::schedule_all(std::shared_ptr<const RawMessage> raw, std::vector<ConnectionWrapper>* used,
                                       float timeout, std::uint64_t number,
                                       std::vector<BasicScheduledMessageTracker>* trackers) {
  MX_DCHECK_RUN_ON(&owner_thread());
  if (!number) {
    number = next_number();
  }
  const std::chrono::steady_clock::time_point deadline = deadline_after(timeout);
  unsigned int copies = 0;
  for (ConnectionById::const_iterator entry = connection_by_id_.begin(); entry != connection_by_id_.end(); ++entry) {
    Connection::pointer conn = entry->second.lock();
    if (!conn || !conn->living()) {
      continue;
    }
    BasicScheduledMessageTracker state =
        _place_on(conn, raw, BasicScheduledMessageTracker(), number, deadline, /*copy=*/true, LanePtr());
    if (!state) {
      continue;
    }
    ++copies;
    if (used) {
      used->push_back(_wrap(conn));
    }
    if (trackers) {
      trackers->push_back(state);
    }
  }
  return copies;
}

BasicClient::Backlog* BasicClient::_backlog(const Connection* conn) {
  for (Backlog& backlog : outbox_->backlogs) {
    if (backlog.key == conn) {
      return &backlog;
    }
  }
  return nullptr;
}

// A connection for a message given none: one with room and nothing
// waiting, round robin, the one chosen moved to the back of the list; with
// none such, the live one with the least waiting. Null when none is live,
// or the client is shutting down.
BasicClient::Connection::pointer BasicClient::_choose_connection() {
  if (shuts_down_) {
    return Connection::pointer();
  }
  ConnectionsList& connections = connections_by_type_[peers::MULTIPLEXER];
  for (ConnectionsList::iterator entry = connections.begin();
       (entry = choose_free_connections(connections, entry)) != connections.end(); ++entry) {
    Connection::pointer conn = entry->lock();
    if (!conn) {
      continue;
    }
    Backlog* backlog = _backlog(conn.get());
    if (backlog && backlog->live) {
      continue;  // room, but what waits for it goes first
    }
    connections.splice(connections.end(), connections, entry);
    return conn;
  }
  Connection::pointer least;
  std::size_t least_waiting = 0;
  for (const Connection::weak_pointer& weak : connections) {
    Connection::pointer conn = weak.lock();
    if (!conn || !conn->living()) {
      continue;
    }
    Backlog* backlog = _backlog(conn.get());
    const std::size_t waiting = backlog ? backlog->live : 0;
    if (!least || waiting < least_waiting) {
      least = conn;
      least_waiting = waiting;
    }
  }
  return least;
}

// Into `conn`'s queue when it has room and nothing waits for it, else into
// its backlog; `state` is the message's tracker, made here when null.
BasicClient::BasicScheduledMessageTracker BasicClient::_place_on(
    const Connection::pointer& conn, std::shared_ptr<const RawMessage> raw, BasicScheduledMessageTracker state,
    std::uint64_t number, std::chrono::steady_clock::time_point deadline, bool copy, LanePtr lane) {
  if (!conn->living()) {
    return BasicScheduledMessageTracker();
  }
  if (!state) {
    state = std::make_shared<SendState>(SendState::QUEUED);
  }
  Backlog* backlog = _backlog(conn.get());
  if ((!backlog || !backlog->live) && conn->take_over(Connection::MessagesBuffer::value_type(state, raw))) {
    _queued(number, state, conn.get());
    return state;
  }
  if (!backlog) {
    outbox_->backlogs.push_back(Backlog());
    backlog = &outbox_->backlogs.back();
    backlog->connection = conn;
    backlog->key = conn.get();
  }
  WaitingPtr waiting(new Waiting());
  waiting->raw = raw;
  waiting->state = state;
  waiting->number = number;
  waiting->deadline = deadline;
  waiting->on = conn.get();
  waiting->copy = copy;
  waiting->lane = lane;
  backlog->waiting.push_back(waiting);
  ++backlog->live;
  ++outbox_->waiting;
  _wait_flushes(number, 1);
  _expire_at(waiting);
  return state;
}

// A message went into `conn`'s queue: a flush that waits for it follows it
// there, as the last of its messages on that connection.
void BasicClient::_queued(std::uint64_t number, const BasicScheduledMessageTracker& state, const Connection* conn) {
  for (const FlushPtr& flush : outbox_->flushes) {
    if (number > flush->last_number) {
      continue;
    }
    bool marked = false;
    for (std::pair<const Connection*, BasicScheduledMessageTracker>& mark : flush->marks) {
      if (mark.first == conn) {
        mark.second = state;
        marked = true;
        break;
      }
    }
    if (!marked) {
      flush->marks.emplace_back(conn, state);
    }
  }
}

void BasicClient::outgoing_queue_has_room(Connection* conn) {
  MX_DCHECK_RUN_ON(&owner_thread());
  Backlog* backlog = _backlog(conn);
  if (!backlog) {
    return;
  }
  while (backlog->live && !conn->outgoing_queue_full()) {
    WaitingPtr waiting = backlog->waiting.front();
    backlog->waiting.pop_front();
    if (waiting->over) {
      continue;  // dropped at its deadline meanwhile
    }
    waiting->over = true;
    --backlog->live;
    --outbox_->waiting;
    ++outbox_->retries;
    conn->take_over(Connection::MessagesBuffer::value_type(waiting->state, waiting->raw));
    _wait_flushes(waiting->number, -1);
    _queued(waiting->number, waiting->state, conn);
  }
  if (!backlog->live) {
    outbox_->backlogs.erase(std::remove_if(outbox_->backlogs.begin(), outbox_->backlogs.end(),
                                           [conn](const Backlog& each) { return each.key == conn; }),
                            outbox_->backlogs.end());
  }
  _check_flushes();
}

// A waiting message that goes nowhere: its tracker reads LOST, and one
// somebody follows is reported as a lost frame is.
void BasicClient::_lose(const WaitingPtr& waiting) {
  waiting->over = true;
  --outbox_->waiting;
  _wait_flushes(waiting->number, -1);
  if (waiting->state && *waiting->state == SendState::QUEUED) {
    *waiting->state = SendState::LOST;
    if (waiting->state.use_count() > 1) {
      tracked_message_done(waiting->state, false);
    }
  }
}

// A connection died: what waited for it is put aside, to be handed over
// after its queue (_replace_displaced), whose messages were sent first.
void BasicClient::_displace(Connection* conn) {
  Backlog* backlog = _backlog(conn);
  if (!backlog) {
    return;
  }
  for (const WaitingPtr& waiting : backlog->waiting) {
    if (!waiting->over) {
      outbox_->displaced.push_back(waiting);
    }
  }
  outbox_->backlogs.erase(std::remove_if(outbox_->backlogs.begin(), outbox_->backlogs.end(),
                                         [conn](const Backlog& each) { return each.key == conn; }),
                          outbox_->backlogs.end());
}

void BasicClient::handle_orphaned_outgoing_messages(Connection::MessagesBuffer& outgoing_messages) {
  MX_DCHECK_RUN_ON(&owner_thread());
  for (Connection::MessagesBuffer::value_type& message : outgoing_messages) {
    if (message.second->pinned()) {
      continue;  // lost with its connection, which is what the pin means
    }
    Connection::pointer conn = _choose_connection();
    if (!conn) {
      break;  // nothing is live: the connection reports the rest lost
    }
    ++outbox_->retries;
    if (_place_on(conn, message.second, message.first.lock(), 0, deadline_after(DEFAULT_TIMEOUT), false, LanePtr())) {
      message.first.reset();  // handed over: not reported lost
    }
  }
  _replace_displaced();
}

// What waited for a dead connection goes to another, the lane it went
// through, if any, adopting that one; a copy of an ALL send, a pinned
// message, and everything when nothing is live, is lost.
void BasicClient::_replace_displaced() {
  std::deque<WaitingPtr> displaced;
  displaced.swap(outbox_->displaced);
  for (const WaitingPtr& waiting : displaced) {
    if (waiting->over) {
      continue;
    }
    Connection::pointer conn;
    if (waiting->copy || waiting->raw->pinned() || !(conn = _choose_connection())) {
      _lose(waiting);
      continue;
    }
    waiting->over = true;  // placed anew, as a new entry when it waits again
    --outbox_->waiting;
    _wait_flushes(waiting->number, -1);
    ++outbox_->retries;
    _place_on(conn, waiting->raw, waiting->state, waiting->number, waiting->deadline, false, waiting->lane);
    if (waiting->lane) {
      waiting->lane->adopt(_wrap(conn));
    }
  }
  _check_flushes();
}

// A waiting message's deadline goes into the heap, and the timer moves to
// it when it is the earliest. Entries of messages that left are cleared out
// when the heap has doubled since the last time.
void BasicClient::_expire_at(const WaitingPtr& waiting) {
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
// LOST, with one warning for all of it.
void BasicClient::_expire() {
  Outbox& outbox = *outbox_;
  const std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now();
  std::size_t dropped = 0;
  while (!outbox.expiring.empty() && outbox.expiring.front().deadline <= now) {
    WaitingPtr waiting = outbox.expiring.front().waiting.lock();
    std::pop_heap(outbox.expiring.begin(), outbox.expiring.end());
    outbox.expiring.pop_back();
    if (!waiting || waiting->over) {
      continue;
    }
    if (Backlog* backlog = _backlog(waiting->on)) {
      --backlog->live;
    }
    waiting->over = true;
    --outbox.waiting;
    _wait_flushes(waiting->number, -1);
    if (waiting->state && *waiting->state == SendState::QUEUED) {
      *waiting->state = SendState::LOST;
    }
    ++dropped;
  }
  outbox.backlogs.erase(std::remove_if(outbox.backlogs.begin(), outbox.backlogs.end(),
                                       [](const Backlog& backlog) { return !backlog.live; }),
                        outbox.backlogs.end());
  if (dropped) {
    MX_LOG(WARNING, MEDIUMVERBOSITY,
           CTX("BasicClient")
               TEXT(repr(dropped) + " message(s) dropped: their connection had no room for them in time"));
  }
  _arm_expiry();
  _check_flushes();
}

BasicClient::FlushPtr BasicClient::begin_flush(std::function<void()> done) {
  MX_DCHECK_RUN_ON(&owner_thread());
  FlushPtr flush(new Flush());
  flush->last_number = outbox_->last_number;
  flush->waiting = outbox_->waiting + outbox_->held;
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

void BasicClient::end_flush(const FlushPtr& flush) {
  MX_DCHECK_RUN_ON(&owner_thread());
  outbox_->flushes.erase(std::remove(outbox_->flushes.begin(), outbox_->flushes.end(), flush), outbox_->flushes.end());
}

void BasicClient::hold(std::uint64_t number, unsigned int copies) {
  MX_DCHECK_RUN_ON(&owner_thread());
  outbox_->held += copies;
  _wait_flushes(number, static_cast<int>(copies));
}

void BasicClient::release(std::uint64_t number, unsigned int copies) {
  MX_DCHECK_RUN_ON(&owner_thread());
  outbox_->held -= copies;
  _wait_flushes(number, -static_cast<int>(copies));
  _check_flushes();
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
// frame it marked is out, written or lost; `done` is told after it left
// the list.
void BasicClient::_check_flushes() {
  if (outbox_->flushes.empty()) {
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
      flush->done();
    }
  }
}

// The client shuts down: nothing waits any more, every tracker reads LOST.
void BasicClient::_drop_outbox() {
  Outbox& outbox = *outbox_;
  for (Backlog& backlog : outbox.backlogs) {
    outbox.displaced.insert(outbox.displaced.end(), backlog.waiting.begin(), backlog.waiting.end());
  }
  for (const WaitingPtr& waiting : outbox.displaced) {
    if (!waiting->over && waiting->state && *waiting->state == SendState::QUEUED) {
      *waiting->state = SendState::LOST;
    }
    waiting->over = true;
  }
  outbox.backlogs.clear();
  outbox.displaced.clear();
  outbox.waiting = 0;
  outbox.expiring.clear();
  asio::error_code ignored;
  outbox.timer.cancel(ignored);
  outbox.armed = std::chrono::steady_clock::time_point::max();
}

}  // namespace multiplexer
