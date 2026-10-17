// The outbox's handover: what a connection that dies had not written goes
// to another, or is held for one, and a lane that held the dead connection
// follows it (handle_orphaned_outgoing_messages). Part of the outbox; see
// outbox.cc.
//
// A connection that dies hands its queue to another, and after it what
// waited in its backlog: a pinned message is lost, which is what the pin
// means; an ALL send's copy is dropped, the other connections having
// theirs; the rest goes, in order, to one other connection, chosen once,
// whose room it waits for in turn, and a lane that held the dead
// connection follows it there (_handed_to). With no other connection
// live, the rest is held for one, and an ALL send is held once, whole, its
// other copies superseded.
#include <algorithm>

#include "multiplexer/outbox.h"

namespace multiplexer {

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

// A waiting message that goes nowhere: its tracker reads LOST, and one
// somebody follows is reported as a lost frame is.
void BasicClient::_lose(const WaitingPtr& waiting) {
  waiting->over = true;
  if (!waiting->on) {
    _held_left();
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
  // A batch that waited for a busy connection to finish waits no more once
  // none is busy or none is live: after what the dead one handed over,
  // which was sent before it.
  if (_idle_or_none()) {
    _close_idle_batches();
  }
}

// A message somebody follows went to `conn` from a dead connection, or
// from the held when a connection came up; see followed(). The last move
// wins. Nobody follows one whose tracker is null.
void BasicClient::_moved(const BasicScheduledMessageTracker& state, const ConnectionWrapper& conn) {
  if (!state) {
    return;
  }
  Outbox& outbox = *outbox_;
  if (outbox.moved.size() >= outbox.moved_compact_at) {
    for (auto entry = outbox.moved.begin(); entry != outbox.moved.end();) {
      if (entry->second.first.expired()) {
        entry = outbox.moved.erase(entry);
      } else {
        ++entry;
      }
    }
    outbox.moved_compact_at = std::max<std::size_t>(64, 2 * outbox.moved.size());
  }
  outbox.moved[state.get()] = std::make_pair(std::weak_ptr<SendState>(state), conn);
}

ConnectionWrapper BasicClient::followed(const BasicScheduledMessageTracker& state, const ConnectionWrapper& first) {
  MX_DCHECK_RUN_ON(&owner_thread());
  auto& moved = outbox_->moved;
  if (moved.empty() || !state) {
    return first;
  }
  const auto entry = moved.find(state.get());
  if (entry == moved.end()) {
    return first;
  }
  // The caller holds the tracker: an entry at its address whose tracker has
  // not expired is its own. The entry goes either way.
  const ConnectionWrapper last = entry->second.first.expired() ? first : entry->second.second;
  moved.erase(entry);
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

}  // namespace multiplexer
