// The synchronous C++ API, Client, which the docs call SyncClient (the
// alias at the end): connect to multiplexers, send events, ask requests,
// receive messages. This is what peers use directly and what the Python
// binding (_native.cc) wraps; BaseMultiplexerServer builds backends on it.
// docs/api_cpp.md describes it from the caller's side.
//
// Client owns a BasicClient (the connections) and adds the query algorithm
// in _query, the piece of the protocol that makes requests reliable: send
// through one connection; on DELIVERY_ERROR, timeout or the connection
// lost, search every connection for a backend of the type
// (BACKEND_FOR_PACKET_SEARCH), take the first PING that answers, and
// repeat the request to that backend by instance id. The request goes out
// at most twice and a query never goes back a stage; a request a backend
// may have, after a timeout or a lost connection, is waited for where the
// query would otherwise fail. docs/query.md draws it and lists every way
// a stage ends. A request with `to` set is an addressed query,
// _query_addressed: the same shape, except that the middle stage locates
// that one instance, with a PING addressed to it, rather than any backend
// of the type, and the three stages share one timeout.
//
// A Lane (basic_client.h) given to send or query keeps a stream of messages
// on one connection, following a failover or, pinned, refusing one; a
// ConnectionWrapper given instead is the connection to prefer.
//
// All calls run the io_service inline until they are done or time out;
// nothing runs between calls. One Client per thread.
#ifndef MX_MULTIPLEXER_CLIENT_H_
#define MX_MULTIPLEXER_CLIENT_H_

#include <asio/io_service.hpp>
#include <cstdint>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "lib/logging/logging.h"
#include "lib/repr.h"
#include "lib/triple.h"
#include "lib/vector.h"
#include "multiplexer/basic_client.h"
#include "multiplexer/defaults.h"
#include "multiplexer/endpoint.h"

namespace multiplexer {

using mx::contains;
using mx::repr;
using mx::triple;
using std::shared_ptr;

typedef mx::triple<std::shared_ptr<const RawMessage>, ConnectionWrapper, std::shared_ptr<MultiplexerMessage>>
    IncomingMessage;

// See the file comment. Exceptions: NotConnected, OperationTimedOut,
// OperationFailed, inherited from ExceptionDefinitions.
class Client : public ExceptionDefinitions {
 public:
  typedef BasicClient::BasicScheduledMessageTracker BasicScheduledMessageTracker;

  // What schedule_one returns: whether the message is still queued, was
  // written to the socket (which the kernel then holds; the multiplexer
  // may not have it yet), or was dropped: its connection ended before
  // writing it, or it waited past its timeout. Null (false as bool) when no
  // connection took the message at all; test it first, since in_queue(),
  // is_sent() and is_lost() fail an Assert on a null tracker.
  struct ScheduledMessageTracker {
    ScheduledMessageTracker(BasicScheduledMessageTracker basic_tracker) : basic_tracker_(basic_tracker) {}

    inline operator bool() const { return (bool)basic_tracker_; }
    bool inline in_queue() const {
      Assert(*this);
      return *basic_tracker_ == SendState::QUEUED;
    }
    bool inline is_sent() const {
      Assert(*this);
      return *basic_tracker_ == SendState::SENT;
    }
    bool inline is_lost() const {
      Assert(*this);
      return *basic_tracker_ == SendState::LOST;
    }

   private:
    BasicScheduledMessageTracker basic_tracker_;
  };

  // With its own io_service, sharing one, or borrowing one that outlives it.
  // A call runs whatever handler of the service is ready, another client's
  // too, so the clients sharing one service are driven from one thread.
  Client(std::uint32_t client_type);
  Client(shared_ptr<asio::io_service> io_service, std::uint32_t client_type);
  Client(asio::io_service& io_service, std::uint32_t client_type);
  ~Client();  // shutdown(), on whichever thread destroys the client
  // Moved, not copied: a copy would share the client, and either one's
  // destructor would shut it down under the other. A moved-from client is
  // only destroyed, which does nothing; one that borrows an io_service
  // keeps borrowing it.
  Client(const Client&) = delete;
  Client& operator=(const Client&) = delete;
  Client(Client&& other) noexcept;

  // Connectivity; see BasicClient for the semantics. shutdown() first
  // writes what was sent before it, running the loop as flush_all() does,
  // `timeout` seconds at most, then closes every connection; what is still
  // unwritten is dropped and reported, at once with 0. Idempotent.
  void shutdown(float timeout = CLOSE_FLUSH_SECONDS);
  // Whether this client was inherited across a fork, in which case its
  // calls throw UsedAfterFork, connections_count() and the other getters of
  // its state included, and shutdown() and the destructor only close the
  // child's descriptor copies, once; see BasicClient::orphaned.
  bool orphaned() const { return basic_client_->orphaned(); }
  // Makes the calling thread the one this client is used from, for a client
  // built on one thread and driven from another; BaseMultiplexerServer's
  // serve_forever() calls it on entry. Only the debug-build thread checks
  // care, and they fail on the next call from the previous thread.
  void bind_to_current_thread() {
    basic_client_->check_not_orphaned();
    basic_client_->bind_to_current_thread();
  }
  ConnectionWrapper async_connect(const asio::ip::tcp::endpoint& peer_endpoint) {
    basic_client_->check_not_orphaned();
    return basic_client_->async_connect(peer_endpoint);
  }
  bool wait_for_connection(ConnectionWrapper connwrap, float timeout = DEFAULT_TIMEOUT) {
    basic_client_->check_not_orphaned();
    return basic_client_->wait_for_connection(connwrap, timeout);
  }
  ConnectionWrapper connect(const asio::ip::tcp::endpoint& peer_endpoint, float timeout = DEFAULT_TIMEOUT) {
    basic_client_->check_not_orphaned();
    return basic_client_->connect(peer_endpoint, timeout);
  }

  // A host name or an address in text. The name is resolved inside the
  // library, on every attempt, so a multiplexer that moved is found again.
  // Both forms log the attempt, as the server classes' connect() does. A
  // multiplexer the client has a connection to, live or on its way, keeps
  // it: connecting again returns that connection.
  ConnectionWrapper async_connect(const std::string& host, std::uint16_t port) {
    basic_client_->check_not_orphaned();
    MX_LOG(INFO, MEDIUMVERBOSITY, CTX("multiplexer.client") TEXT("connecting to " + format_endpoint(host, port)));
    return basic_client_->async_connect(host, port);
  }
  ConnectionWrapper connect(const std::string& host, std::uint16_t port, float timeout = DEFAULT_TIMEOUT) {
    basic_client_->check_not_orphaned();
    MX_LOG(INFO, MEDIUMVERBOSITY, CTX("multiplexer.client") TEXT("connecting to " + format_endpoint(host, port)));
    return basic_client_->connect(host, port, timeout);
  }
  // Drops the multiplexer given to connect() or async_connect() with this
  // host and port, or this address: no reconnect to it any more, unless
  // connect() is called again, and a live connection to it closed, what
  // it had not written handed to the other connections or held, as a lost
  // connection's is. Returns at once, whether the client had it; throws
  // NotConnected after shutdown(), as connect() does. See
  // BasicClient::disconnect.
  bool disconnect(const std::string& host, std::uint16_t port) {
    basic_client_->check_not_orphaned();
    return basic_client_->disconnect(host, port);
  }
  bool disconnect(const asio::ip::tcp::endpoint& peer_endpoint) {
    basic_client_->check_not_orphaned();
    return basic_client_->disconnect(peer_endpoint);
  }
  // How host names become addresses; for tests. See BasicClient::Resolver.
  void set_resolver(BasicClient::Resolver resolver) { basic_client_->set_resolver(resolver); }

  // Which of a multiplexer's routing paths reach this peer, and whether
  // every multiplexer has it in effect; see BasicClient::set_routing. The
  // control message goes out and the answers arrive as the loop runs,
  // inside the calls that run it. `acknowledged`, when given, is called
  // once this change, or a later one, is acknowledged, from inside a
  // receive_message(): the next one after every message read before the
  // acknowledgement was received, so that a loop of receives has handled
  // them, or the one waiting when it came. It must not receive or query
  // through this client.
  void set_routing(const Routing& routing,
                   BasicClient::AcknowledgedCallback acknowledged = BasicClient::AcknowledgedCallback()) {
    basic_client_->check_not_orphaned();
    basic_client_->set_routing(routing, std::move(acknowledged));
  }
  const Routing& routing() const { return basic_client_->routing(); }
  // Which keys of each message type reach this peer by the rules; see
  // BasicClient::set_key_filters. Acknowledged with the routing, and
  // `acknowledged` as for set_routing().
  void set_key_filters(const BasicClient::KeyFilters& filters,
                       BasicClient::AcknowledgedCallback acknowledged = BasicClient::AcknowledgedCallback()) {
    basic_client_->check_not_orphaned();
    basic_client_->set_key_filters(filters, std::move(acknowledged));
  }
  const BasicClient::KeyFilters& key_filters() const { return basic_client_->key_filters(); }
  bool routing_acknowledged() const {
    basic_client_->check_not_orphaned();  // the parent's state in a forked child: raise, as ThreadedClient's
    return basic_client_->routing_acknowledged();
  }
  // How many messages wait for a connection to come up; see
  // BasicClient::held_for_a_connection(). What the server classes' drains
  // wait for, the close dropping them.
  std::size_t held_for_a_connection() const {
    basic_client_->check_not_orphaned();
    return basic_client_->held_for_a_connection();
  }
  // Messages read off the sockets and not yet handed out by a receive.
  bool has_incoming_messages() const {
    basic_client_->check_not_orphaned();
    return basic_client_->has_incoming_messages();
  }

  // Leaving, for the server classes. From now on a request that arrives is
  // refused at once with the DELIVERY_ERROR a multiplexer sends for a peer
  // that is gone, naming this client's type, so that its sender retries
  // elsewhere; a message that answers another, and the protocol's own,
  // are dropped: nobody retries a reply, and refusing one could start a
  // loop. What answers a query under way, a handler's own, reaches it, as
  // it would otherwise. What was read before stays to be received, a
  // number fixed now, however fast more comes. The refusal is queued as a
  // reply is, on the request's connection while that lives.
  void refuse_arrivals();
  // The messages read and not received yet, refused or dropped by the same
  // rule: for a server that will not handle them, at its close.
  void refuse_unread();

  unsigned int inline connections_count() {  // live ones
    basic_client_->check_not_orphaned();
    return basic_client_->connections_count(true);
  }
  std::uint64_t inline instance_id() const { return basic_client_->instance_id(); }  // our `sender`
  // See BasicClient::reconnects_pending(); for tests.
  std::size_t reconnects_pending() const { return basic_client_->reconnects_pending(); }
  // See BasicClient::dropped_while_closing().
  std::uint64_t dropped_while_closing() const { return basic_client_->dropped_while_closing(); }
  std::uint32_t inline client_type() const { return basic_client_->client_type(); }  // our peer type

  // Incoming messages: the next one in arrival order, waiting up to
  // `timeout` (negative: forever). Throws OperationTimedOut, NotConnected.
  // A message taken from a batch gets its raw frame here, the message
  // serialized again (BasicClient::with_raw_frame).
  BasicClient::IncomingMessagesBuffer::value_type read_raw_message(float timeout = DEFAULT_READ_TIMEOUT) {
    return _with_raw_frame(_read(timeout));
  }
  BasicClient::IncomingMessagesBuffer::value_type read_raw_message(mx::SimpleTimer& timer) {
    return _with_raw_frame(_read(timer));
  }

  // The next message, as read_raw_message(), running the callbacks of
  // the changes acknowledged meanwhile (set_routing()). With
  // `end_on_acknowledgement` the wait also ends, throwing OperationTimedOut
  // as if the time had run out, as soon as a change is acknowledged or the
  // last message held for a connection goes (held_for_a_connection()):
  // what a loop of receives, the server classes' among them, acts on at
  // once rather than at its next poll.
  BasicClient::IncomingMessagesBuffer::value_type receive_raw_message(float timeout = DEFAULT_READ_TIMEOUT,
                                                                      bool end_on_acknowledgement = false) {
    return _with_raw_frame(_receive_next(timeout, end_on_acknowledgement));
  }
  // The message alone, which makes no raw frame for a message taken from
  // a batch.
  std::pair<shared_ptr<MultiplexerMessage>, ConnectionWrapper> receive_message(float timeout = DEFAULT_READ_TIMEOUT,
                                                                               bool end_on_acknowledgement = false) {
    BasicClient::IncomingMessagesBuffer::value_type next = _receive_next(timeout, end_on_acknowledgement);
    return std::make_pair(std::move(next.third), std::move(next.second));
  }

  // Sending. schedule_* queue the message; on an idle connection it is
  // written inside the call, otherwise while a later call runs the loop, so
  // a caller that wants every message out before it goes idle uses flush()
  // or flush_all(). A message a full connection cannot take
  // waits for its room, in order, `timeout` seconds at most, and goes in
  // as a later call runs the loop (see BasicClient). They hold nothing: a
  // null tracker from schedule_one(msg) means no connection is live, and
  // schedule_one(msg, wrapper) waits up to `timeout` for one instead and
  // throws NotConnected; queue() is the send that holds the message. `msg`
  // may be a MultiplexerMessage, or a frame, a shared_ptr<const RawMessage>
  // made of one or of its serialized bytes with its id and type.
  // Each of these polls the loop first (BasicClient::poll), so that a
  // connection the multiplexer closed while this client sat idle is retired
  // rather than written into.
  template <typename T>
  ScheduledMessageTracker schedule_one(const T& msg, float timeout = DEFAULT_TIMEOUT) {
    basic_client_->check_not_orphaned();
    basic_client_->poll();
    return ScheduledMessageTracker(basic_client_->schedule_one(_serialize(msg), NULL, timeout));
  }

  template <typename T>
  ScheduledMessageTracker schedule_one(const T& msg, ConnectionWrapper w, float timeout = DEFAULT_TIMEOUT) {
    basic_client_->check_not_orphaned();
    basic_client_->poll();
    return ScheduledMessageTracker(basic_client_->schedule_one(_serialize(msg), w, timeout));
  }

  // Runs the loop until the tracked message is written or lost, or the
  // timeout passes; at once, whatever the timeout, when no connection is
  // live or coming (BasicClient::connection_live_or_coming), which leaves
  // a held message held. Every batch goes first, the message's own among
  // them when it waits in one.
  void flush(ScheduledMessageTracker tracker, float timeout = DEFAULT_TIMEOUT) const {
    basic_client_->check_not_orphaned();

    std::unique_ptr<mx::SimpleTimer> timer = basic_client_->create_timer(timeout);
    return flush(tracker, *timer);
  }

  void flush(ScheduledMessageTracker tracker, mx::SimpleTimer& timer) const {
    basic_client_->check_not_orphaned();
    _poll_before_batches_go();
    basic_client_->close_batches();
    while (tracker && tracker.in_queue() && !timer.expired() && basic_client_->connection_live_or_coming()) {
      if (basic_client_->run_one() == 0) {
        break;  // nothing gives the loop work: nothing more can happen
      }
    }
  }

  // Runs the loop until everything sent before the call is written or
  // given up on, what still waits for room included, or `timeout` passes;
  // true when every one of them was written, false when one was given up
  // on (the drop observer says which) or the time ran out. False at once,
  // whatever the timeout, when something waits and no connection is live
  // or coming, as nothing could write it; where the loop then had no work,
  // a wait with no deadline spun. See BasicClient::begin_flush().
  bool flush_all(float timeout = DEFAULT_TIMEOUT) const {
    basic_client_->check_not_orphaned();
    std::unique_ptr<mx::SimpleTimer> timer = basic_client_->create_timer(timeout);
    _poll_before_batches_go();
    BasicClient::FlushPtr flush = basic_client_->begin_flush();
    while (!basic_client_->flushed(flush) && !timer->expired() && basic_client_->connection_live_or_coming()) {
      if (basic_client_->run_one() == 0) {
        break;  // nothing gives the loop work: nothing more can happen
      }
    }
    basic_client_->end_flush(flush);
    return basic_client_->all_written(flush);
  }

  // The SyncClient's form of ThreadedClient::send(msg): returns at once,
  // the message queued on one live connection, round robin or `lane`'s, or
  // held until one comes up, `timeout` seconds at most, and written as the
  // loop runs; a message the client gives up on is reported
  // (set_drop_observer). queue_all() gives every live connection a copy,
  // and a multiplexer whose first connection is still in its handshake one
  // at its welcome. A message queue() sends through no lane goes by the
  // delivery set for its type or key (set_delivery); queue() runs the
  // loop first unless the message only joins a batch with a delay
  // (BasicClient::send's poll_first).
  // `done`, when given, hears how the message ended, once, inside a later
  // call that runs the loop: 1 once it was written, the first copy for
  // ALL, 0 once it was given up on or shutdown() came first, as the Python
  // send_message(callback=) does. Returns the tracker of the message, the
  // first copy's for ALL; null, `done` never called, when nothing may take
  // it: a pinned lane whose connection is gone, or the client shut down.
  // See BasicClient::send.
  typedef BasicClient::SendCallback SendCallback;
  template <typename T>
  ScheduledMessageTracker queue(const T& msg, float timeout = DEFAULT_TIMEOUT, LanePtr lane = LanePtr(),
                                SendCallback done = SendCallback()) {
    basic_client_->check_not_orphaned();
    if constexpr (std::is_base_of<MultiplexerMessage, T>::value) {
      if (!lane && RawMessage::plain_event(msg) && basic_client_->has_delivery()) {
        return _queue_plain(msg, timeout, done);
      }
    }
    return _queue(_serialize(msg), false, lane, timeout, done);
  }
  template <typename T>
  ScheduledMessageTracker queue_all(const T& msg, float timeout = DEFAULT_TIMEOUT, SendCallback done = SendCallback()) {
    return _queue(_serialize(msg), true, LanePtr(), timeout, done);
  }

  // Writes `msg` to one connection and returns the one that wrote it,
  // waiting up to `timeout`: with no connection live the message is held
  // until one comes up, and a connection that dies with it unwritten hands
  // it to another, or has it held, so a multiplexer restart between two
  // calls costs the reconnect delay, not the message; through a pinned
  // lane the message is lost with its connection instead. Throws
  // NotConnected when nothing wrote it with no connection live, or when a
  // pinned lane's connection is gone, OperationTimedOut otherwise, as the
  // Python ThreadedClient raises. With a lane, through the lane's
  // connection, which takes the connection used when it has none or lost
  // its own.
  ConnectionWrapper send(const MultiplexerMessage& msg, float timeout = DEFAULT_TIMEOUT, LanePtr lane = LanePtr()) {
    basic_client_->check_not_orphaned();
    std::unique_ptr<mx::SimpleTimer> timer = basic_client_->create_timer(timeout);
    return _send_one(msg, *timer, ConnectionWrapper(), lane);
  }
  // Through `connection`, the one a reply came through, while it is live,
  // and through another when it is gone, as a backend's reply goes back the
  // way the request came. A pinned Lane seeded with the connection is the
  // form that refuses any other.
  ConnectionWrapper send(const MultiplexerMessage& msg, const ConnectionWrapper& connection,
                         float timeout = DEFAULT_TIMEOUT) {
    return send(msg, timeout, std::make_shared<Lane>(connection));
  }

  // A request with a reply. The payload overload fills in id, from and
  // type. Returns the reply as an IncomingMessage, whose `third` is the
  // parsed message and `second` the connection it came on. A typed request
  // gives each stage of the algorithm its own `timeout`; a request with
  // `to` set is addressed, its addressee located with a PING when the
  // request did not reach it, and its stages share one, see _query. With a
  // lane, the request goes through the lane's connection and the lane
  // adopts the connection the reply came through. `received`, when given,
  // is told here, on the caller's thread while the query waits (so it must
  // not read through this client), the instance id of each backend that
  // acknowledges an attempt with REQUEST_RECEIVED (notify_start()): once,
  // normally, or again when a retry reached a backend, the same or
  // another; nothing about the query changes for it.
  IncomingMessage query(const MultiplexerMessage& mxmsg, float timeout = DEFAULT_TIMEOUT, LanePtr lane = LanePtr(),
                        ReceivedCallback received = ReceivedCallback()) {
    basic_client_->check_not_orphaned();
    return _query(mxmsg, timeout, lane, received);
  }
  // Through `connection` while it is live, another when it is gone; a
  // pinned Lane seeded with the connection is the form that refuses any
  // other.
  IncomingMessage query(const MultiplexerMessage& mxmsg, const ConnectionWrapper& connection,
                        float timeout = DEFAULT_TIMEOUT, ReceivedCallback received = ReceivedCallback()) {
    return query(mxmsg, timeout, std::make_shared<Lane>(connection), received);
  }

  // The same, the request held by a shared_ptr.
  IncomingMessage query(shared_ptr<const MultiplexerMessage> mxmsg, float timeout = DEFAULT_TIMEOUT,
                        LanePtr lane = LanePtr(), ReceivedCallback received = ReceivedCallback()) {
    basic_client_->check_not_orphaned();
    return _query(*mxmsg, timeout, lane, received);
  }

  IncomingMessage query(const std::string& message, std::uint32_t type, float timeout = DEFAULT_TIMEOUT,
                        LanePtr lane = LanePtr(), ReceivedCallback received = ReceivedCallback()) {
    basic_client_->check_not_orphaned();

    MultiplexerMessage mxmsg;
    mxmsg.set_id(random64());
    mxmsg.set_sender(instance_id());
    mxmsg.set_type(type);
    mxmsg.set_message(message);
    return _query(mxmsg, timeout, lane, received);
  }

  // Queues `msg` on every live connection, a full one's copy waiting for
  // its room as above; returns how many connections got a copy, and puts
  // them in `used` when given.
  template <typename T>
  unsigned int schedule_all(const T& msg, float timeout = DEFAULT_TIMEOUT,
                            std::vector<ConnectionWrapper>* used = NULL) {
    basic_client_->check_not_orphaned();
    basic_client_->poll();
    return basic_client_->schedule_all(_serialize(msg), used, timeout);
  }

  mx::Random64::result_type random64() const { return basic_client_->random64(); }  // a message id

  // Delivery, docs/delivery.md: how the events of `type`, each key apart,
  // or of one key of it, that queue() sends go: several in one message,
  // which the receiving library splits back into one message each
  // (`batch`), kept until their consumer confirms them (`confirm`), or
  // both, and how long a batch has (`timeout`); see BasicClient::Delivery.
  // Throws std::invalid_argument as BasicClient::check_delivery says. A
  // batch goes full, after `max_delay`, or, with no delay, as soon as a
  // connection has nothing left to write; on this client, whose loop runs
  // inside its calls, inside a later call, as a queued message is written.
  typedef BasicClient::Delivery Delivery;
  void set_delivery(std::uint32_t type, const Delivery& delivery) {
    basic_client_->check_not_orphaned();
    _poll_before_batches_go();
    basic_client_->set_delivery(type, delivery);
  }
  void set_delivery(std::uint32_t type, const std::string& key, const Delivery& delivery) {
    basic_client_->check_not_orphaned();
    _poll_before_batches_go();
    basic_client_->set_delivery(type, key, delivery);
  }
  void clear_delivery(std::uint32_t type) {
    basic_client_->check_not_orphaned();
    _poll_before_batches_go();
    basic_client_->clear_delivery(type);
  }
  void clear_delivery(std::uint32_t type, const std::string& key) {
    basic_client_->check_not_orphaned();
    _poll_before_batches_go();
    basic_client_->clear_delivery(type, key);
  }
  // How many messages wait in batches, and whether the batches' timer is
  // armed: for tests.
  std::size_t batched_messages() const { return basic_client_->batched_messages(); }
  bool batch_timer_armed() const { return basic_client_->batch_timer_armed(); }

  // Every message the program sent that the client gives up on, each copy
  // of one sent to ALL, is told to `observer` with its id and why
  // (DropReason), once the handler it happened in is done, inside
  // whichever call runs the loop then, so that it may send or shut the
  // client down; dropped() counts them at once. See BasicClient::report_drop.
  void set_drop_observer(BasicClient::DropObserver observer) { basic_client_->set_drop_observer(observer); }
  std::uint64_t dropped() const { return basic_client_->dropped(); }
  // See BasicClient::incoming_dropped().
  std::uint64_t incoming_dropped() const { return basic_client_->incoming_dropped(); }

  // Confirmed sends (BasicClient::Delivery::confirm, docs/delivery.md): a
  // message received from a confirmed batch carries its number
  // (MultiplexerMessage.confirmation), and the program confirms it with
  // processed() once it is done with it, FAILED when it cannot process it
  // ever, REFUSED to have its producer send it again. Nothing for a message
  // with no number. What this client owes goes once it has no message left
  // for the program, as a receive waits, at flush_all(), at
  // CONFIRMATIONS_GATHERED_MAX owed, and at shutdown. unconfirmed(),
  // sent_again() and confirmations_owed() are for tests: see
  // BasicClient's.
  void confirm(const MultiplexerMessage& message, Confirmed outcome) { basic_client_->confirm(message, outcome); }
  void confirm(std::uint64_t sender, std::uint64_t number, Confirmed outcome) {
    basic_client_->confirm(sender, number, outcome);
  }
  void processed(const MultiplexerMessage& message) { basic_client_->processed(message); }
  std::size_t unconfirmed() const { return basic_client_->unconfirmed(); }
  std::uint64_t sent_again() const { return basic_client_->sent_again(); }
  std::size_t confirmations_owed() const { return basic_client_->confirmations_owed(); }
  bool confirm_timer_armed() const { return basic_client_->confirm_timer_armed(); }

 protected:
  // The query algorithm, its stages and the one reader of what comes back
  // that they share live in client_query.cc; see the comments there.
  // One of a query's attempts, the request or the request sent again: the
  // id it goes out under, the tracker of its message once it is placed,
  // which says whether the client gave it up, and whether it is struck
  // off: a delivery error said nobody has it, or the late wait found it
  // given up on.
  struct Attempt {
    std::uint64_t id;
    BasicClient::BasicScheduledMessageTracker state;
    bool struck = false;
  };
  // A query's stage, as _await reads what ends it.
  enum class Stage { REQUEST, SEARCH, DIRECT, LATE };
  // What a query sent, for _await to read what comes back by the table
  // every client follows (docs/query.md): its attempts, the request and
  // the request sent again, in order, and its search, or locating PING,
  // with the connections it went through that have not said nobody yet.
  struct Ledger {
    std::vector<Attempt> attempts;
    std::uint64_t search_id = 0;
    std::vector<ConnectionWrapper> searched;
    // The attempt that went out as `id`, or null.
    Attempt* attempt(std::uint64_t id);
    // Whether an attempt is left that a backend may have: one not struck off.
    bool any_left() const;
    // Strikes off the attempts the client gave up on, or never placed:
    // nobody can have them.
    void strike_given_up();
    // The search's "nobody" from `connection`: struck off the connections
    // it waits for; false when it waited for none from there.
    bool answered(const ConnectionWrapper& connection);
  };
  // How a stage's wait ended: an answer, a delivery error for the stage's
  // own attempt, the search's PING, nobody anywhere, or in the late wait no
  // attempt left, or the watched connection lost; `message` is what ended
  // it, but for a loss.
  struct Outcome {
    enum Kind { ANSWER, REFUSED, FOUND, NOBODY, LOST } kind;
    IncomingMessage message;
  };
  // The ids of `attempts`, which a reply may reference.
  static std::vector<uint64_t> _ids(const std::vector<Attempt>& attempts);
  IncomingMessage _query(const MultiplexerMessage& query, float timeout, LanePtr lane, ReceivedCallback received);
  IncomingMessage _query_addressed(const MultiplexerMessage& query, float timeout, LanePtr lane);
  ConnectionWrapper _send_attempt(const MultiplexerMessage& mxmsg, mx::SimpleTimer& timer, ConnectionWrapper preferred,
                                  const LanePtr& lane, Ledger* ledger);
  ConnectionWrapper _send_search(const MultiplexerMessage& mxmsg, mx::SimpleTimer& timer, const LanePtr& lane,
                                 Ledger* ledger);
  void _place_search(std::shared_ptr<const RawMessage> raw, mx::SimpleTimer& timer,
                     std::vector<ConnectionWrapper>* searched);
  Outcome _await(Ledger& ledger, mx::SimpleTimer& timer, Stage stage, const ConnectionWrapper* watch,
                 const LanePtr& lane);
  IncomingMessage _late(Ledger& ledger, mx::SimpleTimer& timer, const LanePtr& lane);
  // A query names each id it sends, its attempts' and its search's,
  // before it goes out, and forgets them as it ends: while the client
  // refuses what arrives (refuse_arrivals), a message that references one
  // answers the query, and the refusals pass it by to the queue the query
  // reads, from which what was read before is set aside for the query's
  // duration (BasicClient::set_incoming_aside). Nothing is kept for a
  // client that does not refuse.
  void _awaiting(std::uint64_t id) {
    if (!awaited_) {
      return;
    }
    if (awaited_->empty()) {
      basic_client_->set_incoming_aside();
    }
    awaited_->push_back(id);
  }
  void _awaiting_none() {
    if (!awaited_ || awaited_->empty()) {
      return;
    }
    awaited_->clear();
    basic_client_->put_incoming_back();
  }
  // `placed`, when given, gets the message's tracker once it is placed,
  // whatever the send then comes to.
  ConnectionWrapper _send_one(const MultiplexerMessage& mxmsg, mx::SimpleTimer& timer, ConnectionWrapper preferred,
                              LanePtr lane = LanePtr(), BasicClient::BasicScheduledMessageTracker* placed = NULL);
  ConnectionWrapper _send_one(std::shared_ptr<const RawMessage> raw, mx::SimpleTimer& timer,
                              ConnectionWrapper preferred, LanePtr lane,
                              BasicClient::BasicScheduledMessageTracker* placed = NULL);
  // How long a message of a call with `left` seconds may wait for room, or
  // for a connection: what the call has left and a moment more, so that
  // the call's own deadline comes first; no limit for a call with no
  // deadline, `left` negative.
  static float _room_for(float left);
  // The flushing send's core, both languages': BasicClient::send, then the
  // loop runs until a copy is written, the first for ALL, none is left that
  // could be, or `timer` expires. Returns the copies written, 0 or 1;
  // `used` gets the connection that wrote it; `taken` is false when
  // nothing took the message (BasicClient::send), `lost` when every copy
  // was given up on before the time ran out; `placed`, when given, the
  // first copy's tracker once the message is placed.
  unsigned int _send_and_wait(std::shared_ptr<const RawMessage> raw, bool all, const LanePtr& lane,
                              mx::SimpleTimer& timer, ConnectionWrapper* used, bool* taken, bool* lost,
                              BasicClient::BasicScheduledMessageTracker* placed = NULL);
  // A call that lets the batches go runs the loop first while any message
  // waits in one, so that a connection the peer closed meanwhile is retired
  // before a batch chooses one, as a send's poll does for its message: the
  // queue() calls that put them in their batches ran none (BasicClient::
  // send's poll_first). A send that lets them go polls already.
  void _poll_before_batches_go() const {
    if (basic_client_->batched_messages()) {
      basic_client_->poll();
    }
  }
  // queue() and queue_all(), and the binding's non-flushing send.
  // queue() for a plain event while something is batched: its bytes
  // alone (BasicClient::send_plain), so that a message that joins a batch
  // has no frame made for it.
  // Its body is one the batching handed back (BasicClient::
  // keep_spent_bodies) when one was.
  ScheduledMessageTracker _queue_plain(const MultiplexerMessage& msg, float timeout, const SendCallback& done) {
    if (!spent_bodies_) {
      spent_bodies_ = std::make_shared<SpentBodies>();
      basic_client_->keep_spent_bodies(spent_bodies_);
    }
    std::string body;
    spent_bodies_->take(&body);
    const std::uint64_t id = serialize_stamped(msg, instance_id(), [this] { return random64(); }, &body);
    Trackers trackers(&trackers_room_);
    // The loop runs first unless the message only joins a batch (poll_first).
    if (!basic_client_->send_plain(&body, id, msg.type(), msg.has_key(), msg.key(), timeout, &trackers.list, done,
                                   /*poll_first=*/true) ||
        trackers.list.empty()) {
      return ScheduledMessageTracker(BasicScheduledMessageTracker());
    }
    return ScheduledMessageTracker(trackers.list.front());
  }
  ScheduledMessageTracker _queue(std::shared_ptr<const RawMessage> raw, bool all, const LanePtr& lane, float timeout,
                                 const SendCallback& done) {
    basic_client_->check_not_orphaned();
    Trackers trackers(&trackers_room_);
    // The loop runs first unless the message only joins a batch (poll_first).
    if (!basic_client_->send(raw, all, lane, timeout, 0, &trackers.list, NULL, done, /*may_batch=*/true,
                             /*poll_first=*/true) ||
        trackers.list.empty()) {
      return ScheduledMessageTracker(BasicScheduledMessageTracker());
    }
    return ScheduledMessageTracker(trackers.list.front());
  }
  // A send's list of trackers in the room the last one left, taken for the
  // call and given back at its end, so that a queue() allocates none: a
  // queue() made inside it, from a callback the loop runs, finds no room
  // and makes its own.
  struct Trackers {
    explicit Trackers(std::vector<BasicScheduledMessageTracker>* room) : room(room) { list.swap(*room); }
    ~Trackers() {
      list.clear();
      list.swap(*room);
    }
    std::vector<BasicScheduledMessageTracker>* room;
    std::vector<BasicScheduledMessageTracker> list;
  };
  // What a flushing send that wrote nothing throws, as the Python
  // ThreadedClient raises: NotConnected when the message was given up on, when no
  // connection is live or `lane` is a pinned one whose connection is gone,
  // OperationTimedOut otherwise.
  [[noreturn]] void _raise_for_nothing_written(const LanePtr& lane, bool lost);
  MultiplexerMessage _locator_for(const MultiplexerMessage& query);
  // read_raw_message() and receive_raw_message() but for the raw frame of
  // a message taken from a batch, which they make and these leave null.
  IncomingMessage _read(float timeout) {
    basic_client_->check_not_orphaned();
    if (basic_client_->has_incoming_messages()) {
      return basic_client_->next_incoming_message();  // no timer for a wait that does not happen
    }
    std::unique_ptr<mx::SimpleTimer> timer = basic_client_->create_timer(timeout);
    return _read(*timer);
  }
  IncomingMessage _read(mx::SimpleTimer& timer) {
    basic_client_->check_not_orphaned();
    basic_client_->wait_for_incoming_message(timer);
    Assert(basic_client_->has_incoming_messages());
    return basic_client_->next_incoming_message();
  }
  IncomingMessage _receive_next(float timeout, bool end_on_acknowledgement) {
    basic_client_->check_not_orphaned();
    // A message waiting is taken with no timer made, armed and cancelled,
    // a syscall per message in a busy serve loop: as wait_to_receive()'s
    // first look, its callbacks run first.
    basic_client_->run_acknowledged_callbacks();
    if (basic_client_->has_incoming_messages()) {
      return basic_client_->next_incoming_message();
    }
    std::unique_ptr<mx::SimpleTimer> timer = basic_client_->create_timer(timeout);
    if (!basic_client_->wait_to_receive(*timer, end_on_acknowledgement)) {
      MXTHROW(OperationTimedOut());
    }
    return basic_client_->next_incoming_message();
  }
  static IncomingMessage _with_raw_frame(IncomingMessage incoming) {
    BasicClient::with_raw_frame(&incoming);
    return incoming;
  }
  IncomingMessage _receive(mx::SimpleTimer& timer, const std::vector<uint64_t>& accept_ids, uint32_t ignore_type,
                           uint64_t ignore_id, const ConnectionWrapper* watch = NULL, bool* lost = NULL);
  // A send's `msg` as a frame: a whole message stamped (frame_stamped),
  // marked for batching while anything is batched, a frame as it is.
  shared_ptr<const RawMessage> _serialize(const MultiplexerMessage& msg) {
    return frame_stamped(msg, instance_id(), [this] { return random64(); }, basic_client_->has_delivery());
  }
  shared_ptr<const RawMessage> _serialize(shared_ptr<const RawMessage> raw) { return raw; }

 private:
  shared_ptr<asio::io_service> io_service_ptr_;
  ReceivedCallback received_;  // the on_received of the query under way, see _query
  // queue()'s: the bodies the batching hands back, once a plain event was
  // queued, and the room of a send's list of trackers (Trackers).
  shared_ptr<SpentBodies> spent_bodies_;
  std::vector<BasicScheduledMessageTracker> trackers_room_;
  // The ids _awaiting() named, shared with the refusals, which a move of
  // the client leaves where they are; none before refuse_arrivals().
  shared_ptr<std::vector<std::uint64_t>> awaited_;

 protected:
  asio::io_service& io_service_;
  shared_ptr<BasicClient> basic_client_;
};

// SyncClient is this class's name in the docs since 2.4.0, so that no class
// is named like the client role. Client stays the class itself: what a
// forward declaration names and what compiler messages show.
using SyncClient = Client;

};  // namespace multiplexer

#endif  // MX_MULTIPLEXER_CLIENT_H_
