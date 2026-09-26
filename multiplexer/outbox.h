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
#include <utility>
#include <vector>

#include "multiplexer/basic_client.h"

namespace multiplexer {

// A message waiting for room on one connection.
struct BasicClient::Waiting {
  std::shared_ptr<const RawMessage> raw;
  BasicScheduledMessageTracker state;  // what its tracker reads: QUEUED while it waits
  std::uint64_t number = 0;            // its place in the order sent; 0 counts for every flush
  std::chrono::steady_clock::time_point deadline;
  const Connection* on = nullptr;  // the connection whose backlog holds it
  bool copy = false;               // an ALL send's copy, for this connection alone
  LanePtr lane;                    // adopts the connection it goes to if its own dies
  bool over = false;               // gone from its backlog: queued, dropped or lost
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
  std::function<void()> done;
  bool flushed = false;
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
  std::size_t waiting = 0;           // live entries over every backlog, and the displaced
  std::size_t held = 0;              // the caller's own, counted by hold()
  std::deque<WaitingPtr> displaced;  // a dead connection's, until its queue was handed over
  std::uint64_t last_number = 0;
  std::uint64_t retries = 0;
  std::vector<Expiring> expiring;  // entries of messages that left are skipped when met
  std::size_t compact_at = 64;     // and cleared out when the heap has doubled
  asio::steady_timer timer;
  std::chrono::steady_clock::time_point armed = std::chrono::steady_clock::time_point::max();
  std::vector<FlushPtr> flushes;
};

}  // namespace multiplexer

#endif  // MX_MULTIPLEXER_OUTBOX_H_
