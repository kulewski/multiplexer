// BasicClient's outbox: the structures behind what a client's connections
// cannot take yet. Internal to basic_client.cc and outbox.cc; the design is
// described in outbox.cc.
#ifndef MX_MULTIPLEXER_OUTBOX_H_
#define MX_MULTIPLEXER_OUTBOX_H_

#include <asio/steady_timer.hpp>
#include <chrono>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "multiplexer/basic_client.h"

namespace multiplexer {

// A message waiting for room on one connection, or, held, for a
// connection to come up.
struct BasicClient::Waiting {
  std::shared_ptr<const RawMessage> raw;
  BasicScheduledMessageTracker state;  // what its tracker reads: QUEUED while it waits
  std::uint64_t number = 0;            // its place in the order sent; 0 counts for every flush
  std::chrono::steady_clock::time_point deadline;
  const Connection* on = nullptr;  // the connection whose backlog holds it; null while held
  bool copy = false;               // an ALL send's copy, for this connection alone
  bool all = false;                // held: an ALL send, every live connection's when one comes up
  LanePtr lane;                    // adopts the connection it goes to if its own dies
  bool over = false;               // gone from where it waited: queued, dropped or lost
};

// What waits for one connection, in the order it came. `live` counts the
// entries not over; while it is not 0 the connection's queue is full, so
// that a connection with room never has anything waiting before a new
// message.
struct BasicClient::Backlog {
  Connection::weak_pointer connection;
  const Connection* key = nullptr;
  std::deque<WaitingPtr> waiting;
  std::size_t live = 0;
};

// A flush_all() in progress; see BasicClient::begin_flush().
struct BasicClient::Flush {
  std::uint64_t last_number = 0;  // the last message sent before it
  std::size_t waiting = 0;        // of those, how many still wait (or are held by the caller)
  // Per connection, the last of those queued there: once it is written or
  // lost, so is everything queued there before it.
  std::vector<std::pair<const Connection*, BasicScheduledMessageTracker>> marks;
  std::function<void(bool all_written)> done;
  bool flushed = false;
  bool lost = false;  // one of those was given up on (report_drop): not all written
};

// A send followed to its end for its `done` (BasicClient::send): the
// trackers of its copies, which keep their states alive, and how many are
// neither written nor given up on yet.
struct BasicClient::Follow {
  SendCallback done;
  std::vector<BasicScheduledMessageTracker> copies;
  std::size_t live = 0;
  bool over = false;
};

// The outbox itself: the backlogs, the deadlines and the flushes.
struct BasicClient::Outbox {
  // A deadline in the heap, whose top is the earliest.
  struct Expiring {
    std::chrono::steady_clock::time_point deadline;
    std::weak_ptr<Waiting> waiting;
    // The heap's order, which puts the largest on top: a later deadline is
    // the smaller.
    bool operator<(const Expiring& other) const { return deadline > other.deadline; }
  };

  explicit Outbox(asio::io_service& io_service) : timer(io_service) {}

  std::vector<Backlog> backlogs;
  std::size_t waiting = 0;           // live entries over every backlog, the displaced and the held
  std::deque<WaitingPtr> displaced;  // a dead connection's, until its queue was handed over
  // What waits for a connection to come up, in the order sent; placed as
  // soon as one is registered (place_held). `held_live` counts the entries
  // not over: while it is not 0 a new message waits behind them.
  std::deque<WaitingPtr> held;
  std::size_t held_live = 0;
  std::uint64_t last_number = 0;
  std::uint64_t retries = 0;
  std::vector<Expiring> expiring;  // entries of messages that left are skipped when met
  std::size_t compact_at = 64;     // and cleared out when the heap has doubled
  asio::steady_timer timer;
  std::chrono::steady_clock::time_point armed = std::chrono::steady_clock::time_point::max();
  std::vector<FlushPtr> flushes;
  // Messages a dead connection handed to another while somebody held
  // their tracker, with the connection each went to, for followed(). The
  // entries of trackers nobody holds any more are cleared out when the list
  // has doubled since the last time.
  std::vector<std::pair<std::weak_ptr<SendState>, ConnectionWrapper>> moved;
  std::size_t moved_compact_at = 64;
  // Where the last dead connection to each target handed what it had not
  // written (_successor), one entry per target: a lane that still holds a
  // dead connection follows its messages there, and on, through every
  // failover since, by the target of the connection that took them
  // (_handed_to). `from` names the dead connection while its handovers
  // last, the frame a write still held coming after the rest.
  struct Handover {
    Target target;
    const Connection* from = nullptr;
    Connection::weak_pointer to;
    Target to_target;
  };
  std::vector<Handover> handovers;
  // The sends followed, by the state of each copy; the events about their
  // copies, written or given up on, wait for the one pass per loop turn
  // that handles them (_process_follows), outside the connection's
  // handlers they came from.
  std::unordered_map<const SendState*, FollowPtr> follows;
  std::vector<std::pair<std::shared_ptr<SendState>, bool>> follow_events;
  bool follow_posted = false;
};

}  // namespace multiplexer

#endif  // MX_MULTIPLEXER_OUTBOX_H_
