// The outbox: what a client's connections cannot take yet, and the one way
// every client sends (BasicClient::send).
//
// A message goes into its connection's queue while the queue has room and
// nothing waits for that connection before it; otherwise it waits in the
// connection's backlog. When a full queue has room again, the connection
// says so (outgoing_queue_has_room) and the backlog moves in, in order, as
// far as the room goes: an event costs what it moves, and nothing polls. A
// message given no connection goes to one with room, round robin, or, with
// every one full, waits on the one with the least waiting. With no
// connection live, it is held, in order, behind whatever was held before
// it, until one comes up (_place_held). A message waits `timeout` seconds
// at most: one timer runs to the earliest deadline, and what still waits
// then is dropped, its tracker reading LOST, and reported. A message with
// no time to wait, a timeout of 0 or NaN or a deadline already past, goes
// only where a connection takes it now, and is dropped and reported at
// once otherwise (_give_up): it never enters a backlog or the held.
//
// A connection that dies hands its queue to another, and after it what
// waited in its backlog: a pinned message is lost, which is what the pin
// means; an ALL send's copy is dropped, the other connections having
// theirs; the rest goes, in order, to one other connection, chosen once,
// whose room it waits for in turn, and a lane that held the dead
// connection follows it there (_handed_to). With no other connection
// live, the rest is held for one, and an ALL send is held once, whole, its
// other copies superseded.
//
// Both clients share it. The threaded client's io thread and the
// synchronous client's calls run the same loop, so what waits moves as soon
// as the loop runs: for the synchronous client, inside its next call, which
// is also when anything it queued is written.
//
// flush_all(): every message is numbered in the order sent. A flush waits
// for those numbered up to the last one at its start: the ones that still
// wait, for room or held, and per connection the last of them queued
// there, which everything queued there before it goes out ahead of.
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

void BasicClient::drop_held(const BasicScheduledMessageTracker& state) {
  MX_DCHECK_RUN_ON(&owner_thread());
  for (const WaitingPtr& waiting : outbox_->held) {
    if (!waiting->over && waiting->state == state) {
      _drop_expired(waiting);
      return;
    }
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

// Whether the ALL send whose copies share `raw` is held whole already.
bool BasicClient::_held_whole(const std::shared_ptr<const RawMessage>& raw) const {
  for (const WaitingPtr& waiting : outbox_->held) {
    if (!waiting->over && waiting->raw == raw) {
      return true;
    }
  }
  return false;
}

// A copy of an ALL send that waits for a connection whole already goes
// nowhere itself: its tracker reads LOST for whoever follows the copies,
// unreported, the message not being given up on.
void BasicClient::_supersede(const BasicScheduledMessageTracker& state) {
  if (state && *state == SendState::QUEUED) {
    *state = SendState::LOST;
    if (state.use_count() > 1) {
      tracked_message_done(state, false);
    }
  }
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

// The connection that takes what `dead` had not written: chosen at its
// first handover and kept, while it lives, for the frame a write still
// held, handed over after the rest; remembered for `dead`'s target, for
// the lanes that held `dead` (_handed_to). Null when none is live.
BasicClient::Connection::pointer BasicClient::_successor(Connection* dead) {
  const Target& target = dead->managers_private_data().target;
  std::vector<Outbox::Handover>& handovers = outbox_->handovers;
  std::vector<Outbox::Handover>::iterator entry = std::find_if(
      handovers.begin(), handovers.end(), [&target](const Outbox::Handover& each) { return each.target == target; });
  if (entry != handovers.end() && entry->from == dead) {
    Connection::pointer conn = entry->to.lock();
    if (conn && conn->living()) {
      return conn;
    }
  }
  Connection::pointer conn = _choose_connection();
  if (!conn) {
    return conn;
  }
  if (entry == handovers.end()) {
    entry = handovers.insert(handovers.end(), Outbox::Handover());
    entry->target = target;
  }
  entry->from = dead;
  entry->to = conn;
  entry->to_target = conn->managers_private_data().target;
  return conn;
}

// Where the messages of `gone`, a lane's connection, went once it died:
// the connection that took what it had not written, or, that one dead
// too, the one that took over from it, and so on, while it lives. Null
// while `gone` lives, and when nothing is recorded or live.
BasicClient::Connection::pointer BasicClient::_handed_to(const ConnectionWrapper& gone) {
  Connection::pointer own = gone.lock();
  if (own && own->living()) {
    return Connection::pointer();
  }
  const std::vector<Outbox::Handover>& handovers = outbox_->handovers;
  const Target* target = &gone.target();
  for (std::size_t hops = 0; hops < handovers.size(); ++hops) {  // the bound ends a cycle of failovers too
    std::vector<Outbox::Handover>::const_iterator entry = std::find_if(
        handovers.begin(), handovers.end(), [target](const Outbox::Handover& each) { return each.target == *target; });
    if (entry == handovers.end()) {
      break;
    }
    Connection::pointer conn = entry->to.lock();
    if (conn && conn->living()) {
      return conn;
    }
    target = &entry->to_target;
  }
  return Connection::pointer();
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

// A waiting message that goes nowhere: its tracker reads LOST, and one
// somebody follows is reported as a lost frame is.
void BasicClient::_lose(const WaitingPtr& waiting) {
  waiting->over = true;
  if (!waiting->on) {
    --outbox_->held_live;
  }
  --outbox_->waiting;
  _wait_flushes(waiting->number, -1);
  report_drop(waiting->raw, shuts_down_ ? DropReason::SHUT_DOWN : DropReason::CONNECTION_LOST);
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

void BasicClient::handle_orphaned_outgoing_messages(Connection* dead, Connection::MessagesBuffer& outgoing_messages) {
  MX_DCHECK_RUN_ON(&owner_thread());
  // One connection for all of it, the queue and then the backlog, so that
  // a stream's unwritten tail moves whole and in order; chosen only when
  // there is something to move.
  const Connection::pointer conn =
      outgoing_messages.empty() && outbox_->displaced.empty() ? Connection::pointer() : _successor(dead);
  Connection::MessagesBuffer left;  // what the connection reports lost
  const std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now();
  for (Connection::MessagesBuffer::value_type& message : outgoing_messages) {
    const std::shared_ptr<const RawMessage>& raw = message.second;
    if (raw->pinned() || shuts_down_) {
      left.push_back(message);  // lost with its connection, which is what the pin means; or nothing goes now
      continue;
    }
    BasicScheduledMessageTracker state = message.first.lock();
    // A message keeps its own deadline on the next connection: the time its
    // send gave it to wait for room or for a connection, which a send that
    // gave up on it has seen run out. One whose time is up waits for nothing.
    const std::chrono::steady_clock::time_point deadline = raw->deadline();
    if (raw->for_all()) {
      if (conn) {
        left.push_back(message);  // the other connections have their copies
      } else if (_held_whole(raw)) {
        _supersede(state);
      } else if (deadline <= now) {
        left.push_back(message);  // no time left to wait for a connection
      } else {
        _hold(raw, state, raw->number(), deadline, /*all=*/true, LanePtr());
      }
      continue;
    }
    if (!conn) {
      if (deadline <= now) {
        left.push_back(message);  // no time left to wait for a connection
      } else {
        _hold(raw, state, raw->number(), deadline, false, LanePtr());  // for the next connection
      }
      continue;
    }
    ++outbox_->retries;
    const BasicScheduledMessageTracker placed = _place_on(conn, raw, state, raw->number(), deadline, false, LanePtr());
    if (!placed) {
      left.push_back(message);
      continue;
    }
    if (*placed != SendState::LOST) {  // not given up on at once, with no room and no time left to wait for it
      _moved(state, _wrap(conn));
    }
  }
  outgoing_messages.swap(left);
  for (const Connection::MessagesBuffer::value_type& message : outgoing_messages) {
    report_drop(message.second, shuts_down_ ? DropReason::SHUT_DOWN : DropReason::CONNECTION_LOST);
  }
  _replace_displaced(conn);
}

// A message somebody follows went to `conn` from a dead connection, or
// from the held when a connection came up; see followed(). Nobody follows
// one whose tracker is null.
void BasicClient::_moved(const BasicScheduledMessageTracker& state, const ConnectionWrapper& conn) {
  if (!state) {
    return;
  }
  Outbox& outbox = *outbox_;
  if (outbox.moved.size() >= outbox.moved_compact_at) {
    outbox.moved.erase(std::remove_if(outbox.moved.begin(), outbox.moved.end(),
                                      [](const std::pair<std::weak_ptr<SendState>, ConnectionWrapper>& entry) {
                                        return entry.first.expired();
                                      }),
                       outbox.moved.end());
    outbox.moved_compact_at = std::max<std::size_t>(64, 2 * outbox.moved.size());
  }
  outbox.moved.emplace_back(state, conn);
}

ConnectionWrapper BasicClient::followed(const BasicScheduledMessageTracker& state, const ConnectionWrapper& first) {
  MX_DCHECK_RUN_ON(&owner_thread());
  ConnectionWrapper last = first;
  std::vector<std::pair<std::weak_ptr<SendState>, ConnectionWrapper>>& moved = outbox_->moved;
  if (moved.empty() || !state) {
    return last;
  }
  // In the order handed over: the last entry for the message is where it
  // went. Its entries leave the list, the others keep their order.
  std::size_t kept = 0;
  for (std::size_t index = 0; index < moved.size(); ++index) {
    if (moved[index].first.lock() == state) {
      last = moved[index].second;
      continue;
    }
    if (kept != index) {
      moved[kept] = moved[index];
    }
    ++kept;
  }
  moved.resize(kept);
  return last;
}

bool BasicClient::live(const ConnectionWrapper& wrapper) const {
  MX_DCHECK_RUN_ON(&owner_thread());
  Connection::pointer conn = wrapper.lock();
  return conn && conn->living();
}

// What waited for a dead connection goes to `conn`, the one its queue went
// to, the lane it went through, if any, adopting that one, or, with none
// live, is held for the next; a pinned message is lost, and so is a copy
// of an ALL send while another connection lives, else the message is held
// once, whole.
void BasicClient::_replace_displaced(const Connection::pointer& conn) {
  std::deque<WaitingPtr> displaced;
  displaced.swap(outbox_->displaced);
  for (const WaitingPtr& waiting : displaced) {
    if (waiting->over) {
      continue;
    }
    if (waiting->raw->pinned() || shuts_down_) {
      _lose(waiting);
      continue;
    }
    if (waiting->copy) {
      if (conn) {
        _lose(waiting);  // the other connections have their copies
      } else if (_held_whole(waiting->raw)) {
        waiting->over = true;
        --outbox_->waiting;
        _wait_flushes(waiting->number, -1);
        _supersede(waiting->state);
      } else {
        _rehold(waiting, /*all=*/true);
      }
      continue;
    }
    if (!conn) {
      _rehold(waiting, false);
      continue;
    }
    waiting->over = true;  // placed anew, as a new entry when it waits again
    --outbox_->waiting;
    _wait_flushes(waiting->number, -1);
    ++outbox_->retries;
    const bool followed = waiting->state.use_count() > 1;  // a tracker besides this entry's
    _place_on(conn, waiting->raw, waiting->state, waiting->number, waiting->deadline, false, waiting->lane);
    if (followed) {
      _moved(waiting->state, _wrap(conn));
    }
    if (waiting->lane) {
      waiting->lane->adopt(_wrap(conn));
    }
  }
  _check_flushes();
}

// A dead connection's waiting message held for the next connection, its
// tracker, deadline and place in the counts kept.
void BasicClient::_rehold(const WaitingPtr& waiting, bool all) {
  waiting->on = nullptr;
  waiting->copy = false;
  waiting->all = all;
  outbox_->held.push_back(waiting);
  ++outbox_->held_live;
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
  report_drop(waiting->raw, held ? DropReason::NO_CONNECTION : DropReason::NO_ROOM);
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
