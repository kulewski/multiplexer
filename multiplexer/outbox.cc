// The outbox: what a client's connections cannot take yet, and the one way
// every client sends (BasicClient::send), placing each message. Its other
// parts, by job: outbox_handover.cc, what a connection that dies had not
// written; outbox_expiry.cc, a waiting message's deadline; outbox_flush.cc,
// the end of a send its `done` hears, and flush_all(). Its structures are
// in outbox.h.
//
// A message goes into its connection's queue while the queue has room and
// nothing waits for that connection before it; otherwise it waits in the
// connection's backlog. When a full queue has room again, the connection
// says so (outgoing_queue_has_room) and the backlog moves in, in order, as
// far as the room goes: an event costs what it moves, and nothing polls. A
// message given no connection goes to one with room, round robin, or, with
// every one full, waits on the one with the least waiting. With no
// connection live, it is held, in order, behind whatever was held before
// it, until one comes up (_place_held). A send to ALL gives every live
// connection its copy, and the connection of each multiplexer whose first
// connection is still on its way one in its backlog, which the welcome
// lets in (after_connection_registration): what is sent to ALL just after
// the start reaches every multiplexer, not only those whose handshakes
// ended first; with none live, it is held whole, and those copies are made
// when the first comes up. A message waits `timeout` seconds at most
// (outbox_expiry.cc). A message with no time to wait, a timeout of 0 or
// NaN or a deadline already past, goes only where a connection takes it
// now, and is dropped and reported at once otherwise (_give_up): it never
// enters a backlog or the held.
//
// Both clients share it. The threaded client's io thread and the
// synchronous client's calls run the same loop, so what waits moves as soon
// as the loop runs: for the synchronous client, inside its next call, which
// is also when anything it queued is written.
#include "multiplexer/outbox.h"

#include <algorithm>
#include <cmath>

#include "lib/logging/logging.h"
#include "lib/repr.h"
#include "lib/seconds.h"

namespace multiplexer {

using mx::repr;

namespace {
// When a message given `timeout` seconds stops waiting. A negative or an
// infinite timeout never ends, as lib/seconds.h reads every timeout: the
// message waits until it is written or goes nowhere. 0 and NaN are no time
// at all: a deadline already past, so that the message waits for nothing.
std::chrono::steady_clock::time_point deadline_after(float timeout) {
  if (timeout < 0 || std::isinf(timeout)) {
    return std::chrono::steady_clock::time_point::max();
  }
  if (!(timeout > 0)) {
    return std::chrono::steady_clock::time_point::min();  // 0 and NaN
  }
  return std::chrono::steady_clock::now() + mx::from_seconds(timeout);
}

// The log kind, and what its line says, of a drop for `reason`.
BasicClient::DropLine drop_line(DropReason reason) {
  switch (reason) {
    case DropReason::NO_ROOM:
      return BasicClient::SENT_NO_ROOM;
    case DropReason::NO_CONNECTION:
      return BasicClient::SENT_NO_CONNECTION;
    case DropReason::CONNECTION_LOST:
      return BasicClient::SENT_CONNECTION_LOST;
    case DropReason::SHUT_DOWN:
      break;
  }
  return BasicClient::SENT_SHUT_DOWN;
}
const char* drop_text(DropReason reason) {
  switch (reason) {
    case DropReason::NO_ROOM:
      return "message dropped: it waited for room on its connection past its timeout";
    case DropReason::NO_CONNECTION:
      return "message dropped: it waited for a connection past its timeout";
    case DropReason::CONNECTION_LOST:
      return "message dropped: its connection ended and nothing else could take it";
    case DropReason::SHUT_DOWN:
      break;
  }
  return "message dropped: the client shut down before it went";
}
}  // namespace

void BasicClient::report_drop(const std::shared_ptr<const RawMessage>& raw, DropReason reason) {
  MX_DCHECK_RUN_ON(&owner_thread());
  dropped_.fetch_add(1, std::memory_order_relaxed);
  if (const std::string* text =
          drop_lines_.first({drop_line(reason), WARNING, raw->type(), 0}, [reason] { return drop_text(reason); })) {
    MX_LOG(WARNING, LogSummary::VERBOSITY,
           CTX("BasicClient") TEXT(*text + "; id " + repr(raw->id()) + ", type " + repr(raw->type())));
  }
  // A flush that waits for the message ends with not all written.
  const std::uint64_t number = raw->number();
  for (const FlushPtr& flush : outbox_->flushes) {
    if (number && number <= flush->last_number) {
      flush->lost = true;
    }
  }
  if (drop_observer_) {
    try {
      drop_observer_(raw->id(), reason);
    } catch (const std::exception& error) {
      MX_LOG(ERROR, LOWVERBOSITY, CTX("BasicClient") TEXT(std::string("the drop observer raised: ") + error.what()));
    }
  }
}

std::uint64_t BasicClient::next_number() {
  MX_DCHECK_RUN_ON(&owner_thread());
  return ++outbox_->last_number;
}

std::uint64_t BasicClient::last_number() const { return outbox_->last_number; }

std::uint64_t BasicClient::retries() const { return outbox_->retries; }

std::size_t BasicClient::outbox_entries() const {
  std::size_t entries = outbox_->held.size() + outbox_->displaced.size();
  for (const Backlog& backlog : outbox_->backlogs) {
    entries += backlog.waiting.size();
  }
  return entries;
}

BasicClient::BasicScheduledMessageTracker BasicClient::schedule_one(std::shared_ptr<const RawMessage> raw,
                                                                    ConnectionWrapper* used, float timeout,
                                                                    std::uint64_t number, LanePtr lane) {
  MX_DCHECK_RUN_ON(&owner_thread());
  Connection::pointer conn;
  if (lane && !outbox_->handovers.empty() && lane->holds_connection()) {
    conn = _handed_to(lane->connection());  // its connection died: after the messages it had not written
  }
  if (!conn) {
    conn = _choose_connection();
  }
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
  if (wrapper.inherited()) {
    MXTHROW(UsedAfterFork());  // the parent's connection; see ConnectionWrapper
  }
  check_ours(wrapper);
  MX_DCHECK_RUN_ON(&owner_thread());
  Connection::pointer conn = wrapper.lock();
  if (shuts_down_ || !conn || !conn->living()) {
    return BasicScheduledMessageTracker();  // after shutdown() nothing is placed, as send() says
  }
  return _place_on(conn, raw, BasicScheduledMessageTracker(), number ? number : next_number(), deadline_after(timeout),
                   false, lane);
}

unsigned int BasicClient::schedule_all(std::shared_ptr<const RawMessage> raw, std::vector<ConnectionWrapper>* used,
                                       float timeout, std::uint64_t number,
                                       std::vector<BasicScheduledMessageTracker>* trackers) {
  MX_DCHECK_RUN_ON(&owner_thread());
  if (shuts_down_) {
    return 0;  // nothing is placed after shutdown(), as send() says
  }
  raw->mark_for_all();
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

bool BasicClient::send(std::shared_ptr<const RawMessage> raw, bool all, const LanePtr& lane, float timeout,
                       std::uint64_t number, std::vector<BasicScheduledMessageTracker>* trackers,
                       ConnectionWrapper* used, SendCallback done) {
  check_ours(lane);  // the caller's thread, for the synchronous client; ThreadedClient checked before posting
  MX_DCHECK_RUN_ON(&owner_thread());
  if (shuts_down_) {
    return false;
  }
  std::vector<BasicScheduledMessageTracker> own;
  if (!trackers && done) {
    trackers = &own;  // what `done` follows
  }
  const std::size_t before = trackers ? trackers->size() : 0;
  if (all) {
    raw->mark_for_all();
  } else if (lane && lane->pinned()) {
    raw->mark_pinned();  // never handed to another connection if this one dies under it
  }
  if (!number) {
    number = next_number();
  }
  const std::chrono::steady_clock::time_point deadline = deadline_after(timeout);
  bool refused = false;
  if (outbox_->held_live ||
      !_place_now(raw, all, lane, number, deadline, BasicScheduledMessageTracker(), trackers, used, &refused)) {
    // Something is held already, so no connection is live: a pinned lane
    // holding one has lost it.
    if (refused || (!all && lane && lane->pinned() && lane->holds_connection())) {
      return false;
    }
    if (deadline <= std::chrono::steady_clock::now()) {
      // No time to wait for a connection: given up on at once, an ALL send
      // once, as it would have been held.
      BasicScheduledMessageTracker state =
          _give_up(raw, BasicScheduledMessageTracker(), number, DropReason::NO_CONNECTION);
      if (trackers) {
        trackers->push_back(state);
      }
    } else {
      WaitingPtr held = _hold(raw, BasicScheduledMessageTracker(), number, deadline, all, lane);
      if (trackers) {
        trackers->push_back(held->state);
      }
    }
  }
  if (done) {
    _follow(std::vector<BasicScheduledMessageTracker>(trackers->begin() + before, trackers->end()), done);
  }
  return true;
}

// `raw` placed now, ONE way or to ALL, `state` the tracker of its first
// copy when given, as a held message keeps its own: false when no
// connection is live, or, *refused, when a pinned lane's connection is
// gone.
bool BasicClient::_place_now(const std::shared_ptr<const RawMessage>& raw, bool all, const LanePtr& lane,
                             std::uint64_t number, std::chrono::steady_clock::time_point deadline,
                             BasicScheduledMessageTracker state, std::vector<BasicScheduledMessageTracker>* trackers,
                             ConnectionWrapper* used, bool* refused) {
  if (shuts_down_) {
    return false;
  }
  if (all) {
    bool placed = false;
    for (ConnectionById::const_iterator entry = connection_by_id_.begin(); entry != connection_by_id_.end(); ++entry) {
      Connection::pointer conn = entry->second.lock();
      if (!conn || !conn->living()) {
        continue;
      }
      BasicScheduledMessageTracker copy = _place_on(conn, raw, placed ? BasicScheduledMessageTracker() : state, number,
                                                    deadline, /*copy=*/true, LanePtr());
      if (!placed && used) {
        *used = _wrap(conn);
      }
      placed = true;
      if (trackers) {
        trackers->push_back(copy);
      }
    }
    // A multiplexer whose first connection is still on its way gets its
    // copy too, in that connection's backlog until the welcome lets it in:
    // what is sent to ALL just after the start reaches every multiplexer,
    // not only those whose handshakes ended first. With none registered,
    // the message is held whole, and these copies are made when the first
    // comes up (_place_held); a message with no time to wait for one gets
    // none.
    if (placed && deadline > std::chrono::steady_clock::now()) {
      for (ConnectionByTarget::const_iterator entry = connection_by_target_.begin();
           entry != connection_by_target_.end(); ++entry) {
        Connection::pointer conn = entry->second.lock();
        if (!conn || !conn->living() || conn->registered() || first_connection_over_.count(entry->first)) {
          continue;
        }
        BasicScheduledMessageTracker copy =
            _place_on(conn, raw, BasicScheduledMessageTracker(), number, deadline, /*copy=*/true, LanePtr());
        if (trackers && copy) {
          trackers->push_back(copy);
        }
      }
    }
    return placed;
  }
  Connection::pointer conn;
  if (lane && lane->holds_connection()) {
    ConnectionWrapper held = lane->connection();
    Connection::pointer own = held.lock();
    if (own && own->living()) {
      BasicScheduledMessageTracker placed = _place_on(own, raw, state, number, deadline, false, lane);
      if (trackers) {
        trackers->push_back(placed);
      }
      if (used) {
        *used = held;
      }
      return true;
    }
    if (lane->pinned()) {
      *refused = true;
      return false;
    }
    conn = _handed_to(held);  // after the messages it had not written
  }
  if (!conn) {
    conn = _choose_connection();
  }
  if (!conn) {
    return false;
  }
  BasicScheduledMessageTracker placed = _place_on(conn, raw, state, number, deadline, false, lane);
  if (lane) {
    lane->adopt(_wrap(conn));
  }
  if (trackers) {
    trackers->push_back(placed);
  }
  if (used) {
    *used = _wrap(conn);
  }
  return true;
}

// `raw` held for a connection to come up, behind what is held already,
// with `state` as its tracker, made here when null.
BasicClient::WaitingPtr BasicClient::_hold(const std::shared_ptr<const RawMessage>& raw,
                                           BasicScheduledMessageTracker state, std::uint64_t number,
                                           std::chrono::steady_clock::time_point deadline, bool all,
                                           const LanePtr& lane) {
  raw->mark_number(number);
  raw->mark_deadline(deadline);
  WaitingPtr waiting(new Waiting());
  waiting->raw = raw;
  waiting->state = state ? state : std::make_shared<SendState>(SendState::QUEUED);
  waiting->number = number;
  waiting->deadline = deadline;
  waiting->all = all;
  waiting->lane = lane;
  outbox_->held.push_back(waiting);
  ++outbox_->held_live;
  ++outbox_->waiting;
  _wait_flushes(number, 1);
  _expire_at(waiting);
  return waiting;
}

// A connection came up: what was held for one is placed, in order, each
// with its own tracker and deadline, until nothing live takes it.
void BasicClient::_place_held() {
  Outbox& outbox = *outbox_;
  while (!outbox.held.empty()) {
    WaitingPtr waiting = outbox.held.front();
    if (waiting->over) {
      outbox.held.pop_front();
      continue;
    }
    bool refused = false;
    ConnectionWrapper placed_on;
    if (!_place_now(waiting->raw, waiting->all, waiting->lane, waiting->number, waiting->deadline, waiting->state, NULL,
                    &placed_on, &refused)) {
      if (!refused) {
        break;  // nothing live after all
      }
      outbox.held.pop_front();
      _lose(waiting);  // its pinned lane's connection is gone
      continue;
    }
    outbox.held.pop_front();
    if (!waiting->all && waiting->state.use_count() > 1) {
      _moved(waiting->state, placed_on);  // somebody follows it: where it went
    }
    waiting->over = true;
    --outbox.held_live;
    --outbox.waiting;
    _wait_flushes(waiting->number, -1);
    ++outbox.retries;
  }
  _check_flushes();
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
  raw->mark_number(number);
  raw->mark_deadline(deadline);
  if (lane) {
    lane->watch(conn);  // a seeded lane's first message: it learns the connection's live flag
  }
  if (!state) {
    state = std::make_shared<SendState>(SendState::QUEUED);
  }
  Backlog* backlog = _backlog(conn.get());
  if ((!backlog || !backlog->live) && conn->take_over(Connection::MessagesBuffer::value_type(state, raw))) {
    _queued(number, state, conn.get());
    return state;
  }
  if (deadline <= std::chrono::steady_clock::now()) {
    return _give_up(raw, state, number, DropReason::NO_ROOM);  // no time to wait for room
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
  const std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now();
  while (backlog->live && !conn->outgoing_queue_full()) {
    WaitingPtr waiting = backlog->waiting.front();
    backlog->waiting.pop_front();
    if (waiting->over) {
      continue;  // dropped at its deadline meanwhile
    }
    if (waiting->deadline <= now) {
      _drop_expired(waiting);  // its time is up and the timer has yet to run: dropped, not written late
      continue;
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

// The client shuts down: nothing waits any more, every tracker reads LOST.
void BasicClient::_drop_outbox() {
  Outbox& outbox = *outbox_;
  for (Backlog& backlog : outbox.backlogs) {
    outbox.displaced.insert(outbox.displaced.end(), backlog.waiting.begin(), backlog.waiting.end());
  }
  outbox.displaced.insert(outbox.displaced.end(), outbox.held.begin(), outbox.held.end());
  for (const WaitingPtr& waiting : outbox.displaced) {
    if (waiting->over) {
      continue;
    }
    waiting->over = true;
    report_drop(waiting->raw, DropReason::SHUT_DOWN);
    if (waiting->state && *waiting->state == SendState::QUEUED) {
      *waiting->state = SendState::LOST;
      if (waiting->state.use_count() > 1) {
        tracked_message_done(waiting->state, false);  // somebody follows it
      }
    }
  }
  outbox.backlogs.clear();
  outbox.displaced.clear();
  outbox.held.clear();
  outbox.held_live = 0;
  outbox.waiting = 0;
  outbox.expiring.clear();
  asio::error_code ignored;
  outbox.timer.cancel(ignored);
  outbox.armed = std::chrono::steady_clock::time_point::max();
}

}  // namespace multiplexer
