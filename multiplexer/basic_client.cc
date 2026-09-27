// BasicClient: the parts that are not templates or one-liners. See
// basic_client.h for the design.

#include "multiplexer/basic_client.h"

#include <unistd.h>

#include <algorithm>
#include <chrono>

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

void BasicClient::orphan_close_descriptors() {
  if (orphan_descriptors_closed_.exchange(true)) {
    return;
  }
  for (ConnectionByTarget::iterator entry = connection_by_target_.begin(); entry != connection_by_target_.end();
       ++entry) {
    if (Connection::pointer conn = entry->second.lock()) {
      int fd = conn->socket().native_handle();
      if (fd >= 0) {
        ::close(fd);
      }
    }
  }
}

void BasicClient::handle_message(Connection::pointer conn, std::shared_ptr<const RawMessage> raw,
                                 std::shared_ptr<MultiplexerMessage> mxmsg) {
  MX_DCHECK_RUN_ON(&owner_thread());

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
  mxmsg.set_from(instance_id_);
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
  // connection nobody closes, and keep the loop running until it fired.
  for (const TimerPointer& timer : reconnect_timers_) {
    timer->cancel();
  }
  for (ConnectionByTarget::iterator next = connection_by_target_.begin(), entry;
       next != connection_by_target_.end() && (entry = next++, true);) {
    if (Connection::pointer conn = entry->second.lock()) {
      // this does modify connection_by_target_, so we have to use two
      // iterators
      if (conn->close_gracefully(CLOSE_READ_SECONDS)) {
        closing_.push_back(conn);
      }
    }
  }
  _drop_outbox();  // what still waits goes nowhere now
  drop_lines_.flush();
}

bool BasicClient::closing() {
  MX_DCHECK_RUN_ON(&owner_thread());
  closing_.erase(std::remove_if(closing_.begin(), closing_.end(),
                                [](const Connection::weak_pointer& conn) { return conn.expired(); }),
                 closing_.end());
  return !closing_.empty();
}

void BasicClient::connection_closed(Connection* conn) {
  MX_DCHECK_RUN_ON(&owner_thread());
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
// the connect failed, the name did not resolve, or shutdown() here. Unless
// the client itself is shutting down, a timer is armed to connect to the
// same target again, resolving it afresh. The timer fires only while some
// call runs the loop, so a passive client reconnects during its next call
// at the earliest.
void BasicClient::connection_destroyed(Connection* conn) {
  MX_DCHECK_RUN_ON(&owner_thread());
  MX_LOG(DEBUG, HIGHVERBOSITY, CTX("BasicClient") TEXT("connection_destroyed(" + repr(conn) + ")"));
  _displace(conn);  // what waited for it goes to another after its queue, in handle_orphaned_outgoing_messages
  const Target target = conn->managers_private_data().target;
  Connection::pointer c;
  ConnectionByTarget::iterator target_entry = connection_by_target_.find(target);
  if (target_entry != connection_by_target_.end() && (c = target_entry->second.lock()) && c.get() == conn) {
    connection_by_target_.erase(target_entry);
  }

  if (!shuts_down_) {
    // auto reconnect after AUTO_RECONNECT_TIME seconds; armed before the
    // observer runs, so that nothing the observer does can skip it
    MX_LOG(DEBUG, LOWVERBOSITY,
           CTX("BasicClient") TEXT("scheduling reconnecting after " + repr(AUTO_RECONNECT_TIME) + " seconds to " +
                                   target.first + ":" + repr(target.second)));
    TimerPointer timer(new Timer(io_service_, std::chrono::seconds(AUTO_RECONNECT_TIME)));
    reconnect_timers_.insert(timer);
    timer->async_wait([self = this->shared_from_this(), timer, target](const asio::error_code& error) {
      self->reconnect_after_timeout(timer, target, error);
    });
  }
  if (connection_observer_) {
    connection_observer_(
        ConnectionWrapper(Connection::pointer(), target, conn->managers_private_data().expected_endpoint), false);
  }
}

void BasicClient::reconnect_after_timeout(TimerPointer timer, Target target, const asio::error_code& error) {
  reconnect_timers_.erase(timer);
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

// A new connection for `target`, in the map. Connecting twice to one target
// replaces the earlier connection, which is also how the reconnect timer
// behaves if the caller connected again in the meantime.
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
  connection_by_target_.insert(std::make_pair(target, new_connection));
  return new_connection;
}

// An address given as such: no resolving, the connect starts at once.
ConnectionWrapper BasicClient::async_connect(const asio::ip::tcp::endpoint& peer_endpoint) {
  Connection::pointer conn = _new_connection(Target(peer_endpoint.address().to_string(), peer_endpoint.port()));
  conn->managers_private_data().candidates.assign(1, peer_endpoint);
  _try_next_candidate(conn);
  return _wrap(conn);
}

// A host name, or an address in text: the name is resolved on this thread
// first, every address it has tried in turn; the wrapper is returned at
// once, unspecified until an address is in use.
ConnectionWrapper BasicClient::async_connect(const std::string& host, std::uint16_t port) {
  asio::error_code literal;
  asio::ip::address address = asio::ip::make_address(host, literal);
  if (!literal) {
    return async_connect(Endpoint(address, port));
  }
  Connection::pointer conn = _new_connection(Target(host, port));
  _resolve_and_start(conn);
  return _wrap(conn);
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
  asio::error_code ignored;
  conn->socket().close(ignored);  // a socket that failed to connect is reopened by async_connect
  conn->socket().async_connect(
      data.expected_endpoint,
      [self = this->shared_from_this(), conn](const asio::error_code& error) { self->_connected(conn, error); });
}

void BasicClient::bind_to_current_thread() {
  bind_owner_to_current_thread();
  for (ConnectionByTarget::iterator entry = connection_by_target_.begin(); entry != connection_by_target_.end();
       ++entry) {
    if (Connection::pointer conn = entry->second.lock()) {
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
    Assert(timeout == 0 || !timer->expired());
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

ConnectionWrapper BasicClient::connect(const std::string& host, std::uint16_t port, float timeout) {
  ConnectionWrapper connwrap = async_connect(host, port);
  wait_for_connection(connwrap, timeout);
  return connwrap;
}

BasicClient::IncomingMessagesBuffer::value_type BasicClient::next_incoming_message() {
  MX_DCHECK_RUN_ON(&owner_thread());

  Assert(has_incoming_messages());
  IncomingMessagesBuffer::value_type next = incoming_messages_.front();
  incoming_messages_.pop_front();
  return next;
}

// A timer for one call's deadline; a negative timeout means never expires.
std::unique_ptr<mx::SimpleTimer> BasicClient::create_timer(float timeout) const {
  using mx::SimpleTimer;
  typedef std::unique_ptr<SimpleTimer> SimpleTimerPtr;
  if (timeout >= 0) {
    return SimpleTimerPtr(new SimpleTimer(io_service_, timeout));
  }
  SimpleTimerPtr ptr(new SimpleTimer(io_service_));
  Assert(ptr->expired() == false);
  Assert(ptr->expired() == false);
  Assert(ptr->expired() == false);
  return ptr;
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
