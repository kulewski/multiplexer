// BasicClient: the parts that are not templates or one-liners. See
// basic_client.h for the design.

#include "multiplexer/basic_client.h"

#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <stdexcept>

#include "lib/fork.h"
#include "lib/logging/logging.h"
#include "lib/repr.h"
#include "multiplexer/multiplexer.constants.h"
#include "multiplexer/outbox.h"

using namespace mx;
using namespace multiplexer;
using mx::SimpleTimer;
using std::cerr;

BasicClient::BasicClient(asio::io_service& io_service, std::uint32_t client_type)
    : Base(io_service),
      client_type_(client_type),
      shuts_down_(false),
      incoming_queue_max_size_(DEFAULT_INCOMING_QUEUE_MAX_SIZE),
      drop_lines_(io_service, "BasicClient"),
      fork_generation_at_creation_(mx::fork_generation()),
      resolver_(io_service),
      outbox_(new Outbox(io_service)) {}

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

void BasicClient::handle_message(Connection::pointer conn, std::shared_ptr<const RawMessage> raw,
                                 std::shared_ptr<MultiplexerMessage> mxmsg) {
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

  // A copy of a message already seen, from another multiplexer: drop it.
  if (!last_seen_message_ids_.insert(mxmsg->id())) {
    return;
  }

  IncomingMessagesBuffer::value_type incoming = mx::make_triple(raw, _wrap(conn), mxmsg);
  if (incoming_sink_) {
    incoming_sink_(incoming);
    return;
  }
  if (incoming_queue_full()) {
    if (drop_lines_.first({INCOMING_QUEUE_FULL, WARNING, 0, 0}, [] { return "incoming_queue_full, dropping"; })) {
      MX_LOG(WARNING, LogSummary::VERBOSITY,
             CTX("BasicClient.handle_message") TEXT("incoming_queue_full, dropping #" + repr(mxmsg->id())));
    }
    return;  // drop
  }
  incoming_messages_.push_back(incoming);
}

// After the socket's connect: the handshake, or the next address the
// target resolved to, or the end of this attempt.
void BasicClient::_connected(Connection::pointer conn, const asio::error_code& error) {
  if (!error) {
    conn->managers_private_data().routing_in_welcome = routing_version_;  // what start() sends
    conn->start();
  } else if (!conn->shuts_down()) {
    MX_LOG(DEBUG, MEDIUMVERBOSITY,
           CTX("BasicClient") TEXT("connect to " + repr(conn->managers_private_data().expected_endpoint) +
                                   " failed: " + error.message()));
    _try_next_candidate(conn);
  }
}

void BasicClient::set_routing(const Routing& routing) {
  MX_DCHECK_RUN_ON(&owner_thread());
  routing_ = routing;
  ++routing_version_;
  welcome_message_.reset();  // the next connection's welcome carries the new routing
  MX_LOG(INFO, LOWVERBOSITY, CTX("BasicClient") TEXT("routing " + routing_text(routing)));
  for (ConnectionById::iterator entry = connection_by_id_.begin(); entry != connection_by_id_.end(); ++entry) {
    if (Connection::pointer conn = entry->second.lock()) {
      if (conn->living()) {
        _send_routing(conn);
      }
    }
  }
}

// Forced past a full outgoing queue, since the peer that steps out is
// often the saturated one, and pinned, so that a connection that dies
// does not hand the frame to another multiplexer: each gets its own.
void BasicClient::_send_routing(Connection::pointer conn) {
  PeerControl control;
  *control.mutable_routing() = routing_;
  MultiplexerMessage mxmsg;
  mxmsg.set_id(random_());
  mxmsg.set_sender(instance_id_);
  mxmsg.set_type(PEER_CONTROL);
  control.SerializeToString(mxmsg.mutable_message());
  std::shared_ptr<const RawMessage> raw(RawMessage::FromMessage(mxmsg));
  raw->mark_pinned();
  raw->mark_own();  // a connection made since carries the routing in its welcome
  conn->managers_private_data().routing_request_id = mxmsg.id();
  if (!conn->schedule(raw, /*force=*/true)) {
    MX_LOG(WARNING, LOWVERBOSITY,
           CTX("BasicClient") TEXT("could not queue PEER_CONTROL on the connection to " + repr(conn->peer_id())));
  }
}

// The answer to the last PEER_CONTROL sent on this connection confirms the
// current routing; the answer to an older one, when two changes were made
// in a row, confirms nothing, whatever flags it carries.
void BasicClient::_on_peer_status(Connection::pointer conn, const MultiplexerMessage& mxmsg) {
  PeerStatus status;
  if (!status.ParseFromString(mxmsg.message()) || status.has_error()) {
    MX_LOG(WARNING, LOWVERBOSITY,
           CTX("BasicClient") TEXT("PEER_STATUS from " + repr(conn->peer_id()) + ": " +
                                   (status.has_error() ? status.error() : "garbled")));
    return;
  }
  auto& data = conn->managers_private_data();
  if (mxmsg.references() == data.routing_request_id && same_routing(status.routing(), routing_)) {
    data.routing_acknowledged = routing_version_;
  }
}

bool BasicClient::routing_acknowledged() const {
  MX_DCHECK_RUN_ON(&owner_thread());
  if (has_incoming_messages()) {
    return false;  // read, not yet handed out: still on its way
  }
  for (ConnectionByTarget::const_iterator entry = connection_by_target_.begin(); entry != connection_by_target_.end();
       ++entry) {
    if (Connection::pointer conn = entry->second.lock()) {
      if (!conn->living()) {
        continue;
      }
      // A handshake in flight: the multiplexer may have indexed the
      // connection, with whatever routing its welcome carried, before we
      // hear back.
      if (!conn->registered() || conn->managers_private_data().routing_acknowledged != routing_version_) {
        return false;
      }
    }
  }
  return true;
}

// Idempotent, and the second call may come from any thread: a client that
// was shut down on its own thread is often destroyed elsewhere later, for
// instance by Python's garbage collector on the main thread.
void BasicClient::shutdown() {
  if (shuts_down_) {
    return;
  }
  MX_DCHECK_RUN_ON(&owner_thread());
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
  _drop_outbox();  // what still waits goes nowhere now
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

// Called from Connection::shutdown for any reason: the multiplexer closed,
// the connect failed, the name did not resolve, or shutdown() or
// disconnect() here. Unless the client itself is shutting down, a timer is
// armed to connect to the same target again, resolving it afresh, while
// the target is the client's: the connection was its target's, which
// disconnect() takes out of the map before it closes the connection. The
// timer fires only while some call runs the loop, so a passive client
// reconnects during its next call at the earliest. A connection that
// reads on to its multiplexer's end, closed by shutdown() or disconnect()
// or after a failed write, joins those closing: the loop runs it while a
// shutdown waits for them, and a new owner thread takes it with them.
void BasicClient::connection_destroyed(Connection* conn) {
  MX_DCHECK_RUN_ON(&owner_thread());
  MX_LOG(DEBUG, HIGHVERBOSITY, CTX("BasicClient") TEXT("connection_destroyed(" + repr(conn) + ")"));
  if (!conn->shuts_down()) {
    closing_.push_back(conn->shared_from_this());
  }
  _displace(conn);  // what waited for it goes to another after its queue, in handle_orphaned_outgoing_messages
  const Target target = conn->managers_private_data().target;
  Connection::pointer target_connection;
  bool still_wanted = false;  // the connection was its target's: the client still has the target
  ConnectionByTarget::iterator target_entry = connection_by_target_.find(target);
  if (target_entry != connection_by_target_.end() && (target_connection = target_entry->second.lock()) &&
      target_connection.get() == conn) {
    connection_by_target_.erase(target_entry);
    still_wanted = true;
  }

  if (!shuts_down_ && still_wanted) {
    // auto reconnect after AUTO_RECONNECT_TIME seconds; armed before the
    // observer runs, so that nothing the observer does can skip it
    MX_LOG(DEBUG, LOWVERBOSITY,
           CTX("BasicClient") TEXT("scheduling reconnecting after " + repr(AUTO_RECONNECT_TIME) + " seconds to " +
                                   target.first + ":" + repr(target.second)));
    TimerPointer timer(new Timer(io_service_, std::chrono::seconds(AUTO_RECONNECT_TIME)));
    reconnect_timers_.emplace(timer, target);
    timer->async_wait([self = this->shared_from_this(), timer, target](const asio::error_code& error) {
      self->reconnect_after_timeout(timer, target, error);
    });
  }
  if (connection_observer_) {
    connection_observer_(
        ConnectionWrapper(Connection::pointer(), target, conn->managers_private_data().expected_endpoint, this), false);
  }
}

void BasicClient::reconnect_after_timeout(TimerPointer timer, Target target, const asio::error_code& error) {
  if (!reconnect_timers_.erase(timer)) {
    // disconnect() dropped the target and took the timer out, cancelling
    // it, which comes too late for a timer that had fired already, its
    // handler queued: no reconnect either way.
    return;
  }
  if (shuts_down_) {
    // Cancelled by shutdown(), or armed just before it and fired after: a
    // connection opened now would outlive the shutdown, keep a threaded
    // client's io thread from ending, and leave the peer registered.
    return;
  }
  if (!error) {
    if (connection_by_target_.find(target) == connection_by_target_.end()) {
      async_connect(target.first, target.second);
    }
  } else {
    MX_LOG(ERROR, HIGHVERBOSITY,
           CTX("BasicClient") TEXT("auto reconnect to " + target.first + ":" + repr(target.second) +
                                   " cancelled by error " + repr(error)));
  }
}

BasicClient::Connection::pointer BasicClient::_live_connection(const Target& target) const {
  MX_DCHECK_RUN_ON(&owner_thread());
  ConnectionByTarget::const_iterator entry = connection_by_target_.find(target);
  if (entry == connection_by_target_.end()) {
    return Connection::pointer();
  }
  Connection::pointer conn = entry->second.lock();
  return conn && !conn->shuts_down() ? conn : Connection::pointer();
}

// A new connection for `target`, in the map. async_connect keeps a live
// one, and the reconnect timer makes one only for a target that has none,
// so one found here is shutting down already, and is closed for good.
BasicClient::Connection::pointer BasicClient::_new_connection(const Target& target) {
  MX_DCHECK_RUN_ON(&owner_thread());
  ConnectionByTarget::iterator target_entry = connection_by_target_.find(target);
  if (target_entry != connection_by_target_.end()) {
    if (Connection::pointer conn = target_entry->second.lock()) {
      conn->shutdown();
    }
  }
#ifndef NDEBUG
  // discard weak references that point to no connections at all
  for (ConnectionByTarget::iterator next = connection_by_target_.begin(), entry;
       next != connection_by_target_.end() && (entry = next++, true);) {
    if (!entry->second.lock()) {
      MX_LOG(ERROR, LOWVERBOSITY,
             CTX("BasicClient.connect") TEXT("there should be no dangling weak references in "
                                             "connection_by_target_"));
      connection_by_target_.erase(entry);
    }
  }
#endif
  Connection::pointer new_connection = Connection::Create(io_service_, this->shared_from_this());
  new_connection->managers_private_data().target = target;
  new_connection->managers_private_data().owner = this;
  connection_by_target_.insert(std::make_pair(target, new_connection));
  made_.erase(
      std::remove_if(made_.begin(), made_.end(), [](const Connection::weak_pointer& made) { return made.expired(); }),
      made_.end());
  made_.push_back(new_connection);
  return new_connection;
}

// An address given as such: no resolving, the connect starts at once.
// After shutdown() nothing connects, as ThreadedClient::connect says: a
// connection opened then would stay registered, every send refused on it.
ConnectionWrapper BasicClient::async_connect(const asio::ip::tcp::endpoint& peer_endpoint) {
  if (shuts_down_) {
    MXTHROW(NotConnected());
  }
  const Target target = _target(peer_endpoint);
  if (Connection::pointer live = _live_connection(target)) {
    return _wrap(live);  // connected, or on its way, already: kept, not replaced
  }
  Connection::pointer conn = _new_connection(target);
  conn->managers_private_data().candidates.assign(1, peer_endpoint);
  _try_next_candidate(conn);
  return _wrap(conn);
}

// A host name, or an address in text: the name is resolved on this thread
// first, every address it has tried in turn; the wrapper is returned at
// once, unspecified until an address is in use. NotConnected after
// shutdown(), as above.
ConnectionWrapper BasicClient::async_connect(const std::string& host, std::uint16_t port) {
  if (shuts_down_) {
    MXTHROW(NotConnected());
  }
  asio::error_code literal;
  asio::ip::address address = asio::ip::make_address(host, literal);
  if (!literal) {
    return async_connect(Endpoint(address, port));
  }
  if (Connection::pointer live = _live_connection(Target(host, port))) {
    return _wrap(live);  // connected, or on its way, already: kept, not replaced
  }
  Connection::pointer conn = _new_connection(Target(host, port));
  _resolve_and_start(conn);
  return _wrap(conn);
}

BasicClient::Target BasicClient::_target(const std::string& host, std::uint16_t port) {
  asio::error_code literal;
  asio::ip::address address = asio::ip::make_address(host, literal);
  return literal ? Target(host, port) : _target(Endpoint(address, port));
}

bool BasicClient::disconnect(const std::string& host, std::uint16_t port) { return _disconnect(_target(host, port)); }

bool BasicClient::disconnect(const Endpoint& peer_endpoint) { return _disconnect(_target(peer_endpoint)); }

// The target leaves the map before its connection is closed, so that the
// connection's end arms no reconnect (connection_destroyed); a reconnect
// armed already is taken out and cancelled.
bool BasicClient::_disconnect(const Target& target) {
  MX_DCHECK_RUN_ON(&owner_thread());
  if (shuts_down_) {
    MXTHROW(NotConnected());
  }
  bool had = false;
  for (ReconnectTimers::iterator next = reconnect_timers_.begin(), reconnect;
       next != reconnect_timers_.end() && (reconnect = next++, true);) {
    if (reconnect->second == target) {
      reconnect->first->cancel();
      reconnect_timers_.erase(reconnect);
      had = true;
    }
  }
  Connection::pointer conn;
  ConnectionByTarget::iterator target_entry = connection_by_target_.find(target);
  if (target_entry != connection_by_target_.end()) {
    conn = target_entry->second.lock();
    connection_by_target_.erase(target_entry);
    had = true;
  }
  if (had) {
    MX_LOG(INFO, MEDIUMVERBOSITY,
           CTX("BasicClient") TEXT("disconnecting from " + target.first + ":" + repr(target.second)));
  }
  if (conn) {
    conn->close_gracefully(CLOSE_READ_SECONDS);  // among those closing then, see connection_destroyed
  }
  return had;
}

void BasicClient::_resolve_and_start(Connection::pointer conn) {
  MX_DCHECK_RUN_ON(&owner_thread());
  const Target& target = conn->managers_private_data().target;
  if (resolver_hook_) {
    asio::error_code error;
    std::vector<Endpoint> candidates = resolver_hook_(target.first, target.second, error);
    _resolved(conn, error, candidates);
    return;
  }
  resolver_.async_resolve(target.first, std::to_string(target.second),
                          [self = this->shared_from_this(), conn](const asio::error_code& error,
                                                                  asio::ip::tcp::resolver::results_type results) {
                            std::vector<Endpoint> candidates;
                            for (const asio::ip::tcp::resolver::results_type::value_type& entry : results) {
                              candidates.push_back(entry.endpoint());
                            }
                            self->_resolved(conn, error, candidates);
                          });
}

void BasicClient::_resolved(Connection::pointer conn, const asio::error_code& error, std::vector<Endpoint> candidates) {
  if (conn->shuts_down()) {
    return;
  }
  const Target& target = conn->managers_private_data().target;
  if (error || candidates.empty()) {
    MX_LOG(WARNING, MEDIUMVERBOSITY,
           CTX("BasicClient") TEXT(target.first + ":" + repr(target.second) + " does not resolve" +
                                   (error ? ": " + error.message() : std::string()) + "; trying again later"));
    conn->shutdown();  // arms the reconnect timer, which resolves again
    return;
  }
  conn->managers_private_data().candidates = candidates;
  conn->managers_private_data().next_candidate = 0;
  _try_next_candidate(conn);
}

// The next address of the connection's target, or the end of this attempt
// when every one failed; the reconnect timer then starts the next.
void BasicClient::_try_next_candidate(Connection::pointer conn) {
  auto& data = conn->managers_private_data();
  if (data.next_candidate >= data.candidates.size()) {
    conn->shutdown();
    return;
  }
  data.expected_endpoint = data.candidates[data.next_candidate++];
  _close_socket(*conn);  // the last address's, which failed to connect
  // Opened here rather than by async_connect, so that the table a forked
  // child reads has it before it connects; one that cannot be opened is
  // reported through the loop, as async_connect does.
  asio::error_code error;
  conn->socket().open(data.expected_endpoint.protocol(), error);
  if (error) {
    asio::post(io_service_, [self = this->shared_from_this(), conn, error] { self->_connected(conn, error); });
    return;
  }
  descriptors_.add(conn->socket().native_handle());
  conn->socket().async_connect(
      data.expected_endpoint,
      [self = this->shared_from_this(), conn](const asio::error_code& error) { self->_connected(conn, error); });
}

// Forgotten by the table first: a child forked between the two leaves the
// number open in its copy, where one forked after a close that came first
// could find it given to a file of the process, and close that.
void BasicClient::_close_socket(Connection& conn) {
  MX_DCHECK_RUN_ON(&owner_thread());
  if (!conn.socket().is_open()) {
    return;
  }
  descriptors_.remove(conn.socket().native_handle());
  asio::error_code ignored;
  conn.socket().close(ignored);
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

// Runs the loop until the connection is registered (handshake done), dead,
// or the timeout passed. False on the latter two; the caller decides whether
// to care, since the reconnect timer keeps trying regardless.
bool BasicClient::wait_for_connection(ConnectionWrapper connwrap, float timeout) const {
  MX_DCHECK_RUN_ON(&owner_thread());

  if (Connection::pointer conn = connwrap.lock()) {
    io_service_.reset();
    std::unique_ptr<SimpleTimer> timer = create_timer(timeout);
    Assert(timeout == 0 || std::isnan(timeout) || !timer->expired());  // NaN is 0 to create_timer
    MX_LOG(DEBUG, CHATTERBOX,
           CTX("basicClient") TEXT("waiting for connection " + repr(conn.get()) + "on IO=" + repr(&io_service_)));
    while (!conn->shuts_down() && !conn->registered() && !timer->expired()) {
      unsigned int c = io_service_.run_one();
      AssertMsg(c != 0, "io_service_.run_one() must return 1 here");
    }
    return conn->registered() && !conn->shuts_down();
  }
  return false;
}

ConnectionWrapper BasicClient::connect(const asio::ip::tcp::endpoint& peer_endpoint, float timeout) {
  ConnectionWrapper connwrap = async_connect(peer_endpoint);
  wait_for_connection(connwrap, timeout);
  return connwrap;
}

// A name resolves while the wait runs: the wrapper returned is made once
// the connection is up, so that its endpoint() is the address in use,
// where async_connect()'s was made before the name resolved.
ConnectionWrapper BasicClient::connect(const std::string& host, std::uint16_t port, float timeout) {
  ConnectionWrapper connwrap = async_connect(host, port);
  if (wait_for_connection(connwrap, timeout)) {
    if (Connection::pointer conn = connwrap.lock()) {
      return _wrap(conn);
    }
  }
  return connwrap;
}

BasicClient::IncomingMessagesBuffer::value_type BasicClient::next_incoming_message() {
  MX_DCHECK_RUN_ON(&owner_thread());

  Assert(has_incoming_messages());
  IncomingMessagesBuffer::value_type next = incoming_messages_.front();
  incoming_messages_.pop_front();
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
