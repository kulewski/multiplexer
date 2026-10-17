// BasicClient: the parts that are not templates or one-liners, but for a
// connection's coming and going (basic_client_connect.cc), the routing and
// key filters (basic_client_routing.cc) and the outbox (outbox.cc). See
// basic_client.h for the design.

#include "multiplexer/basic_client.h"

#include <algorithm>
#include <cmath>
#include <stdexcept>

#include "lib/fork.h"
#include "lib/logging/logging.h"
#include "lib/repr.h"
#include "multiplexer/batching.h"
#include "multiplexer/confirming.h"
#include "multiplexer/multiplexer.constants.h"
#include "multiplexer/outbox.h"

using namespace mx;
using namespace multiplexer;
using mx::SimpleTimer;

BasicClient::BasicClient(asio::io_service& io_service, std::uint32_t client_type)
    : Base(io_service),
      client_type_(client_type),
      shuts_down_(false),
      incoming_queue_max_size_(DEFAULT_INCOMING_QUEUE_MAX_SIZE),
      drop_lines_(io_service, "BasicClient"),
      fork_generation_at_creation_(mx::fork_generation()),
      resolver_(io_service),
      outbox_(new Outbox(io_service)),
      batcher_(new Batcher(io_service)),
      confirmer_(new Confirmer(io_service)) {}

BasicClient::~BasicClient() {}

bool BasicClient::orphaned() const { return mx::fork_generation() != fork_generation_at_creation_; }

void BasicClient::check_not_orphaned() const {
  if (orphaned()) {
    MXTHROW(UsedAfterFork());
  }
}

// Every socket open at the fork, from the table the owner thread keeps for
// this: the maps and lists of connections are not read, as the owner
// thread may have been changing them at the fork, and a connection that
// is closing may be in none of them. Nothing allocated and no lock, as
// nothing that could wait on a lock a parent thread held.
void BasicClient::orphan_close_descriptors() {
  if (orphan_descriptors_closed_.exchange(true)) {
    return;
  }
  descriptors_.close_all();
}

void BasicClient::handle_message(const Connection::pointer& conn, const std::shared_ptr<const RawMessage>& raw,
                                 const std::shared_ptr<MultiplexerMessage>& mxmsg) {
  MX_DCHECK_RUN_ON(&owner_thread());
  ++frames_handled_;  // poll() reads a queue's worth at most

  // The multiplexer's answer to our PEER_CONTROL is this library's business.
  if (mxmsg->type() == PEER_STATUS) {
    _on_peer_status(conn, *mxmsg);
    return;
  }

  // Every message must carry an id: replies are matched by the id they
  // reference and duplicates are detected by it, so one without is useless.
  if (!mxmsg->id()) {
    return;
  }

  // A consumer's word on this client's confirmed sends, which the program
  // never sees (basic_client_confirm.cc).
  if (mxmsg->type() == types::CONFIRMATION) {
    _on_confirmation(*mxmsg);
    return;
  }

  // A copy of a message already seen, from another multiplexer: drop it.
  if (!last_seen_message_ids_.insert(mxmsg->id())) {
    return;
  }

  if (mxmsg->has_batch()) {
    _split_batch(conn, *mxmsg);
    return;
  }
  if (mxmsg->type() == types::DELIVERY_ERROR) {
    // Nobody took a confirmed batch: it goes again, which the program
    // hears nothing of until its messages are confirmed or given up on.
    if (mxmsg->references() && _confirmed_batch_not_taken(mxmsg->references())) {
      return;
    }
    if (_split_delivery_error(conn, *mxmsg)) {
      return;
    }
  }
  if (mxmsg->has_confirmation()) {
    mxmsg->clear_confirmation();  // a number only a confirmed batch's split gives
  }

  IncomingMessagesBuffer::value_type incoming = mx::make_triple(raw, _wrap(conn), mxmsg);
  if (incoming_sink_) {
    incoming_sink_(incoming);
    return;
  }
  queue_incoming(std::move(incoming));
}

void BasicClient::queue_incoming(IncomingMessagesBuffer::value_type incoming) {
  MX_DCHECK_RUN_ON(&owner_thread());
  if (incoming_queue_full()) {
    // Not seen after all: its copy through another multiplexer, coming
    // once there is room, is taken, where it was dropped as a duplicate of
    // this one, which the queue dropped.
    last_seen_message_ids_.forget_last();
    incoming_dropped_.fetch_add(1, std::memory_order_relaxed);
    if (drop_lines_.first({INCOMING_QUEUE_FULL, WARNING, 0, 0}, [] { return "incoming_queue_full, dropping"; })) {
      MX_LOG(WARNING, LogSummary::VERBOSITY,
             CTX("BasicClient.handle_message") TEXT("incoming_queue_full, dropping #" + repr(incoming.third->id())));
    }
    return;  // drop
  }
  _enqueue(std::move(incoming));
}

// Moved in, as everything the queue takes and gives: a copy of the triple
// would count each of its shared pointers up and down again.
void BasicClient::_enqueue(IncomingMessagesBuffer::value_type incoming) {
  incoming_messages_.push_back(std::move(incoming));
  ++enqueued_;
}

// Only the plain events of the batch's own type and key belong in it,
// which a client's batching packs (basic_client_batching.cc): anything else
// inside, or a batch of a protocol type, that does not parse or that holds
// more than MAX_BATCH_RECORDS messages, is dropped and said; a message seen
// before is dropped as one that came alone is. A full incoming queue drops
// the batch whole, as it drops a message, and takes its copy through
// another multiplexer once there is room; one with room takes every
// message of it, past its limit by one batch at most, and a poll counts
// each.
//
// The batch is read into strings kept from one batch to the next
// (split_scratch_), so that each message's bytes are copied into memory
// already there, and each message is parsed from its string: what a
// message from a batch costs is its own allocations, the message and its
// payload; once the strings kept hold more than KEPT_SPLIT_BYTES, each
// the room of the biggest message it ever held, or after a batch of more
// than MAX_BATCH_RECORDS messages, they go, so that neither one huge batch
// nor big messages at ever other places in batches under the limit hold
// their memory for good. A message queued goes without its raw frame, which most
// readers never ask for, receive_message() among them, and which
// with_raw_frame() makes for those that do; one handed to a sink goes
// with it, as a sink's reader may read it.
//
// A confirmed batch's messages (basic_client_confirm.cc) are handed out
// each with its number, the envelope's `confirm` read in step, and are not
// checked against the ids seen: one sent again, its confirmation lost, is
// handed out again. One that does not belong is confirmed FAILED, as
// nothing will ever process it; a full queue drops them unconfirmed, sent
// again in time. A sink that took the batch has had it in hand: what it
// confirmed goes once the connection handed over every frame its last
// read brought (read_drained), so that the batches of one read, of one
// message each when the producer does not batch, are confirmed together.
void BasicClient::_split_batch(const Connection::pointer& conn, const MultiplexerMessage& envelope) {
  MessageBatch& batch = split_scratch_;
  const bool split = envelope.type() > types::MAX_MULTIPLEXER_META_PACKET && batch.ParseFromString(envelope.batch()) &&
                     batch.message_size() <= static_cast<int>(MAX_BATCH_RECORDS);
  const bool full = split && !incoming_sink_ && incoming_queue_full();
  const int count = batch.message_size();
  const google::protobuf::RepeatedField<std::uint64_t>& numbers = envelope.confirm();
  const bool confirmed = numbers.size() >= 2;
  // Every message parsed first, then each checked against the ids seen and
  // handed on: the check's table stays in the cache for the batch, where
  // parsing a message between two checks pushed it out.
  std::vector<std::shared_ptr<MultiplexerMessage>> parsed;
  if (split && !full) {
    parsed.reserve(static_cast<std::size_t>(count));
    int pair = 0;            // in `numbers`, the pair the next message's number is in
    std::uint64_t past = 0;  // how many of that pair's numbers were given
    for (const std::string& body : batch.message()) {
      std::uint64_t number = 0;
      if (confirmed) {
        while (pair + 1 < numbers.size() && past >= numbers.Get(pair + 1)) {
          pair += 2;
          past = 0;
        }
        if (pair + 1 < numbers.size()) {
          number = numbers.Get(pair) + past++;
        }
      }
      std::shared_ptr<MultiplexerMessage> mxmsg = std::make_shared<MultiplexerMessage>();
      if (!mxmsg->ParseFromString(body) || !mxmsg->id() || !RawMessage::plain_event(*mxmsg) ||
          mxmsg->type() != envelope.type() || mxmsg->has_key() != envelope.has_key() ||
          mxmsg->key() != envelope.key()) {
        if (const std::string* line = drop_lines_.first({BATCH_NOT_SPLIT, WARNING, envelope.type(), 1}, [] {
              return "a message in a batch that arrived dropped: no plain event of the batch's type and key";
            })) {
          MX_LOG(WARNING, LogSummary::VERBOSITY,
                 CTX("BasicClient")
                     TEXT(*line + "; batch id " + repr(envelope.id()) + ", type " + repr(envelope.type())));
        }
        if (number) {
          confirm(envelope.sender(), number, Confirmed::FAILED);
        }
        continue;
      }
      if (number) {
        mxmsg->set_confirmation(number);
      } else if (mxmsg->has_confirmation()) {
        mxmsg->clear_confirmation();
      }
      parsed.push_back(std::move(mxmsg));
    }
  }
  // What the strings keep counted, the emptied ones a batch before left
  // among them: one pass over a batch's strings, not one per message.
  if (count > static_cast<int>(MAX_BATCH_RECORDS) || batch.message().SpaceUsedExcludingSelfLong() > KEPT_SPLIT_BYTES) {
    MessageBatch().Swap(&batch);  // a huge batch's memory goes, where a usual one's stays for the next
  }
  if (!split) {
    if (const std::string* line = drop_lines_.first({BATCH_NOT_SPLIT, WARNING, envelope.type(), 0}, [] {
          return "a batch that arrived dropped: of a protocol type, not parsing, or of more than MAX_BATCH_RECORDS "
                 "messages";
        })) {
      MX_LOG(WARNING, LogSummary::VERBOSITY,
             CTX("BasicClient") TEXT(*line + "; id " + repr(envelope.id()) + ", type " + repr(envelope.type())));
    }
    return;
  }
  if (full) {
    last_seen_message_ids_.forget_last();
    incoming_dropped_.fetch_add(count, std::memory_order_relaxed);
    if (drop_lines_.first({INCOMING_QUEUE_FULL, WARNING, 0, 0}, [] { return "incoming_queue_full, dropping"; })) {
      MX_LOG(WARNING, LogSummary::VERBOSITY,
             CTX("BasicClient.handle_message")
                 TEXT("incoming_queue_full, dropping a batch of " + repr(count) + ", #" + repr(envelope.id())));
    }
    return;
  }
  if (count > 1) {
    frames_handled_ += count - 1;  // poll() reads a queue's worth of messages at most
  }
  const ConnectionWrapper wrapper = _wrap(conn);
  if (confirmed) {
    _confirmed_batch_from(conn, envelope.sender());
  }
  for (std::shared_ptr<MultiplexerMessage>& mxmsg : parsed) {
    if (!mxmsg->has_confirmation() && !last_seen_message_ids_.insert(mxmsg->id())) {
      continue;  // seen before, as a message that came alone would be
    }
    IncomingMessagesBuffer::value_type incoming =
        mx::make_triple(std::shared_ptr<const RawMessage>(), wrapper, std::move(mxmsg));
    if (incoming_sink_) {
      with_raw_frame(&incoming);
      incoming_sink_(incoming);
    } else {
      _enqueue(std::move(incoming));
    }
  }
  if (confirmed && incoming_sink_) {
    conn->notify_when_read_drained();  // what the handlers confirm goes once the read is handed over
  }
}

void BasicClient::read_drained(Connection*) {
  MX_DCHECK_RUN_ON(&owner_thread());
  if (incoming_sink_ && !shuts_down_) {
    send_confirmations();
  }
}

void BasicClient::with_raw_frame(IncomingMessagesBuffer::value_type* incoming) {
  if (incoming->first || !incoming->third) {
    return;
  }
  std::string serialized = incoming->third->SerializeAsString();
  incoming->first = RawMessage::FromBatch(&serialized, incoming->third->id(), incoming->third->type());
}

// A DELIVERY_ERROR for a batch this client sent, which the multiplexer
// sends back whole for this (Server::MessageMetaHandler): one DELIVERY_ERROR
// for each message inside, as the multiplexer would have sent it for that
// message alone, the message's id as the packet's and the reference, its
// workflow, the message itself as the original, each handed out as one that
// came alone; a full incoming queue drops them all, as it drops a batch.
// False, and nothing done, for any other, which goes on as it is: one whose
// original is no batch, or was left out for its size, or never sent, by a
// multiplexer that does not send a batch back. Only a failure pays the read.
bool BasicClient::_split_delivery_error(const Connection::pointer& conn, const MultiplexerMessage& error) {
  DeliveryError report;
  MessageBatch batch;
  if (!report.ParseFromString(error.message()) || !report.has_original_message() ||
      !report.original_message().has_batch() || !batch.ParseFromString(report.original_message().batch()) ||
      batch.message_size() > static_cast<int>(MAX_BATCH_RECORDS)) {
    return false;
  }
  if (!incoming_sink_ && incoming_queue_full()) {
    last_seen_message_ids_.forget_last();
    incoming_dropped_.fetch_add(batch.message_size(), std::memory_order_relaxed);
    if (drop_lines_.first({INCOMING_QUEUE_FULL, WARNING, 0, 0}, [] { return "incoming_queue_full, dropping"; })) {
      MX_LOG(WARNING, LogSummary::VERBOSITY,
             CTX("BasicClient.handle_message") TEXT("incoming_queue_full, dropping the delivery errors of a batch of " +
                                                    repr(batch.message_size()) + ", #" + repr(error.id())));
    }
    return true;
  }
  if (batch.message_size() > 1) {
    frames_handled_ += batch.message_size() - 1;  // poll() reads a queue's worth of messages at most
  }
  report.clear_original_message();
  const ConnectionWrapper wrapper = _wrap(conn);
  for (const std::string& body : batch.message()) {
    MultiplexerMessage message;
    if (!message.ParseFromString(body) || !message.id()) {
      continue;  // nothing to tell it by
    }
    std::shared_ptr<MultiplexerMessage> told = std::make_shared<MultiplexerMessage>();
    told->set_id(random64());
    told->set_sender(error.sender());
    told->set_to(error.to());
    told->set_report_delivery_error(false);
    told->set_type(types::DELIVERY_ERROR);
    told->set_references(message.id());
    told->set_workflow(message.workflow());
    DeliveryError each(report);
    each.set_packet_id(message.id());
    each.mutable_original_message()->Swap(&message);
    each.SerializeToString(told->mutable_message());
    std::string serialized = told->SerializeAsString();
    IncomingMessagesBuffer::value_type incoming =
        mx::make_triple(RawMessage::FromBatch(&serialized, told->id(), told->type()), wrapper, std::move(told));
    if (incoming_sink_) {
      incoming_sink_(incoming);
    } else {
      _enqueue(std::move(incoming));
    }
  }
  return true;
}

bool BasicClient::wait_to_receive(mx::SimpleTimer& timer, bool end_on_acknowledgement) {
  MX_DCHECK_RUN_ON(&owner_thread());
  for (;;) {
    run_acknowledged_callbacks();
    if (has_incoming_messages()) {
      return true;
    }
    // A confirmation that came while messages routed before it were still
    // queued left the change to the receive that took the last of them:
    // told of now, its callbacks run above, so that a serve loop's wait
    // ends at it as at any other.
    _acknowledged_by_receive();
    if (end_on_acknowledgement && take_acknowledged_wake()) {
      return false;
    }
    send_confirmations();  // the program has nothing more in hand
    if (timer.expired()) {
      break;
    }
    if (run_one() == 0) {
      MXTHROW(NotConnected());
    }
  }
  poll();  // a last look, without waiting, as in wait_for_incoming_message
  run_acknowledged_callbacks();
  return has_incoming_messages();
}

// Idempotent, and the second call may come from any thread: a client that
// was shut down on its own thread is often destroyed elsewhere later, for
// instance by Python's garbage collector on the main thread.
void BasicClient::shutdown() {
  if (shuts_down_) {
    return;
  }
  MX_DCHECK_RUN_ON(&owner_thread());
  // What waits in a batch goes where every message goes now: written if
  // the connections still take it, else dropped and told with the rest;
  // so do the confirmations this client owes.
  close_batches();
  send_confirmations();
  asio::error_code cancelled;
  batcher_->timer.cancel(cancelled);
  shuts_down_ = true;
  resolver_.cancel();
  MX_LOG(DEBUG, HIGHVERBOSITY,
         CTX("BasicClient")
             TEXT("shutdown: " + repr(connection_by_target_.size()) + " target(s), " + repr(connection_by_id_.size()) +
                  " registered, " + repr(reconnect_timers_.size()) + " reconnect(s) pending"));
  // A reconnect armed by a connection lost before this call would open a
  // connection nobody closes, and keep the loop running until it fired:
  // each is cancelled and taken out, so that one whose handler is queued
  // already finds itself gone, as after disconnect().
  for (const ReconnectTimers::value_type& reconnect : reconnect_timers_) {
    reconnect.first->cancel();
  }
  reconnect_timers_.clear();
  for (ConnectionByTarget::iterator next = connection_by_target_.begin(), entry;
       next != connection_by_target_.end() && (entry = next++, true);) {
    if (Connection::pointer conn = entry->second.lock()) {
      // this does modify connection_by_target_, so we have to use two
      // iterators
      conn->close_gracefully(CLOSE_READ_SECONDS);  // among those closing then, see connection_destroyed
    }
  }
  _drop_outbox();                                 // what still waits goes nowhere now
  _give_up_confirmations(DropReason::SHUT_DOWN);  // nothing will hear a confirmation now
  acknowledged_callbacks_.clear();                // never called now
  drop_lines_.flush();
}

bool BasicClient::connection_live_or_coming() const {
  MX_DCHECK_RUN_ON(&owner_thread());
  if (shuts_down_) {
    return false;
  }
  if (!reconnect_timers_.empty()) {
    return true;  // a connection lost, to be tried again
  }
  for (ConnectionByTarget::const_iterator entry = connection_by_target_.begin(); entry != connection_by_target_.end();
       ++entry) {
    Connection::pointer conn = entry->second.lock();
    if (conn && !conn->shuts_down()) {
      return true;  // registered, or resolving, connecting or in its handshake
    }
  }
  return false;
}

bool BasicClient::closing() {
  MX_DCHECK_RUN_ON(&owner_thread());
  closing_.erase(std::remove_if(closing_.begin(), closing_.end(),
                                [](const Connection::weak_pointer& conn) { return conn.expired(); }),
                 closing_.end());
  return !closing_.empty();
}

// The socket is closed here, rather than when the connection is
// destroyed, so that the table a forked child reads forgets it first.
void BasicClient::connection_closed(Connection* conn) {
  MX_DCHECK_RUN_ON(&owner_thread());
  _close_socket(*conn);
  _confirmations_on_closed(conn);  // what was written on it and not confirmed goes again
  const std::uint64_t dropped = conn->dropped_while_closing();
  if (!dropped) {
    return;
  }
  dropped_while_closing_ += dropped;
  MX_LOG(WARNING, LOWVERBOSITY,
         CTX("BasicClient") TEXT(repr(dropped) + " message(s) from multiplexer " + repr(conn->peer_id()) +
                                 " arrived after the connection began closing, and were dropped: a request among "
                                 "them gets no answer"));
}

// Every connection that still exists: the targets', those closing, and
// those shut down whose handlers have not all run, which run on the new
// owner's calls and check its thread.
void BasicClient::bind_to_current_thread() {
  bind_owner_to_current_thread();
  for (const Connection::weak_pointer& made : made_) {
    if (Connection::pointer conn = made.lock()) {
      conn->bind_io_thread_to_current();
    }
  }
}

void BasicClient::set_incoming_aside() {
  MX_DCHECK_RUN_ON(&owner_thread());
  Assert(incoming_aside_.empty());
  incoming_aside_.swap(incoming_messages_);
}

void BasicClient::put_incoming_back() {
  MX_DCHECK_RUN_ON(&owner_thread());
  dequeued_ += incoming_messages_.size();  // taken by nobody, as if received
  incoming_messages_.clear();
  incoming_messages_.swap(incoming_aside_);
}

BasicClient::IncomingMessagesBuffer::value_type BasicClient::next_incoming_message() {
  MX_DCHECK_RUN_ON(&owner_thread());

  Assert(has_incoming_messages());
  IncomingMessagesBuffer::value_type next = std::move(incoming_messages_.front());
  incoming_messages_.pop_front();
  ++dequeued_;
  return next;
}

// A timer for one call's deadline; a negative timeout means never expires,
// and NaN, no time at all, is 0, as mx::from_seconds reads it.
std::unique_ptr<mx::SimpleTimer> BasicClient::create_timer(float timeout) const {
  using mx::SimpleTimer;
  typedef std::unique_ptr<SimpleTimer> SimpleTimerPtr;
  if (std::isnan(timeout)) {
    timeout = 0;
  }
  if (timeout >= 0) {
    return SimpleTimerPtr(new SimpleTimer(io_service_, timeout));
  }
  SimpleTimerPtr ptr(new SimpleTimer(io_service_));
  Assert(ptr->expired() == false);
  Assert(ptr->expired() == false);
  Assert(ptr->expired() == false);
  return ptr;
}

void BasicClient::check_ours(const ConnectionWrapper& connection) const {
  if (connection.owner_ && connection.owner_ != this) {
    throw std::invalid_argument("a connection of another client: a connection or lane belongs to its client");
  }
}

void BasicClient::check_ours(const LanePtr& lane) const {
  const BasicClient* owner = lane ? lane->owner() : nullptr;
  if (owner && owner != this) {
    throw std::invalid_argument("a lane of another client: a connection or lane belongs to its client");
  }
}

// Takes `connection`, and its live flag, on the client's thread, where a
// reference to the connection may be taken and dropped.
void Lane::adopt(const ConnectionWrapper& connection) {
  _check_made_here();
  if (connection.inherited()) {
    MXTHROW(ExceptionDefinitions::UsedAfterFork());
  }
  BasicClient::Connection::pointer conn = connection.lock();
  mx::MutexLock lock(mutex_);
  if (pinned_ && holds_) {
    return;
  }
  connection_ = connection;
  holds_ = true;
  owner_.store(connection.owner_, std::memory_order_release);
  living_ = conn ? conn->living_flag() : std::shared_ptr<const std::atomic<bool>>();
  watched_.store(static_cast<bool>(living_), std::memory_order_release);
}

// A seeded lane's first use: `conn` is the connection a message is placed
// on, its flag the lane's when it is the one the lane holds.
void Lane::_watch(const BasicClient::Connection::pointer& conn) {
  mx::MutexLock lock(mutex_);
  if (!living_ && holds_ && connection_.lock() == conn) {
    living_ = conn->living_flag();
  }
  watched_.store(static_cast<bool>(living_), std::memory_order_release);
}
