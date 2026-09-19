// BasicClient: the parts that are not templates or one-liners. See
// basic_client.h for the design.

#include "multiplexer/basic_client.h"

#include <unistd.h>

#include "lib/fork.h"
#include "lib/logging/logging.h"
#include "lib/repr.h"
#include "multiplexer/multiplexer.constants.h"
#include <chrono>

using namespace mx;
using namespace multiplexer;
using mx::SimpleTimer;
using std::cerr;

BasicClient::BasicClient(asio::io_service &io_service, std::uint32_t client_type)
    : Base(io_service), client_type_(client_type), shuts_down_(false),
      incoming_queue_max_size_(DEFAULT_INCOMING_QUEUE_MAX_SIZE), fork_generation_at_creation_(mx::fork_generation()),
      resolver_(io_service) {}

bool BasicClient::orphaned() const { return mx::fork_generation() != fork_generation_at_creation_; }

void BasicClient::check_not_orphaned() const {
  if (orphaned())
    MXTHROW(UsedAfterFork());
}

void BasicClient::orphan_close_descriptors() {
  for (ConnectionByTarget::iterator entry = connection_by_target_.begin(); entry != connection_by_target_.end();
       ++entry) {
    if (Connection::pointer conn = entry->second.lock()) {
      int fd = conn->socket().native_handle();
      if (fd >= 0)
        ::close(fd);
    }
  }
}

void BasicClient::handle_message(Connection::pointer conn, std::shared_ptr<const RawMessage> raw,
                                 std::shared_ptr<MultiplexerMessage> mxmsg) {
  MX_DCHECK_RUN_ON(&owner_thread());

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
    MX_LOG(WARNING, HIGHVERBOSITY,
           CTX("BasicClient.handle_message") TEXT("incoming_queue_full, dropping #" + repr(mxmsg->id())));
    return; // drop
  }
  incoming_messages_.push_back(incoming);
}

// After the socket's connect: the handshake, or the next address the
// target resolved to, or the end of this attempt.
void BasicClient::_connected(Connection::pointer conn, const asio::error_code &error) {
  if (!error) {
    conn->start();
  } else if (!conn->shuts_down()) {
    MX_LOG(DEBUG, MEDIUMVERBOSITY,
           CTX("BasicClient") TEXT("connect to " + repr(conn->managers_private_data().expected_endpoint) +
                                   " failed: " + error.message()));
    _try_next_candidate(conn);
  }
}

// Idempotent, and the second call may come from any thread: a client that
// was shut down on its own thread is often destroyed elsewhere later, for
// instance by Python's garbage collector on the main thread.
void BasicClient::shutdown() {
  if (shuts_down_)
    return;
  MX_DCHECK_RUN_ON(&owner_thread());
  shuts_down_ = true;
  resolver_.cancel();
  for (ConnectionByTarget::iterator next = connection_by_target_.begin(), entry;
       next != connection_by_target_.end() && (entry = next++, true);)
    if (Connection::pointer conn = entry->second.lock())
      // this does modify connection_by_target_, so we have to use two
      // iterators
      conn->shutdown();
}

// Called from Connection::shutdown for any reason: the multiplexer closed,
// the connect failed, the name did not resolve, or shutdown() here. Unless
// the client itself is shutting down, a timer is armed to connect to the
// same target again, resolving it afresh. The timer fires only while some
// call runs the loop, so a passive client reconnects during its next call
// at the earliest.
void BasicClient::connection_destroyed(Connection *conn) {
  MX_DCHECK_RUN_ON(&owner_thread());
  MX_LOG(DEBUG, HIGHVERBOSITY, CTX("BasicClient") TEXT("connection_destroyed(" + repr(conn) + ")"));
  const Target target = conn->managers_private_data().target;
  Connection::pointer c;
  ConnectionByTarget::iterator target_entry = connection_by_target_.find(target);
  if (target_entry != connection_by_target_.end() && (c = target_entry->second.lock()) && c.get() == conn) {
    connection_by_target_.erase(target_entry);
  }

  if (connection_observer_)
    connection_observer_(
        ConnectionWrapper(Connection::pointer(), target, conn->managers_private_data().expected_endpoint), false);
  if (!shuts_down_) {
    // auto reconnect after AUTO_RECONNECT_TIME seconds
    MX_LOG(DEBUG, LOWVERBOSITY,
           CTX("BasicClient") TEXT("scheduling reconnecting after " + repr(AUTO_RECONNECT_TIME) + " seconds to " +
                                   target.first + ":" + repr(target.second)));
    TimerPointer timer(new Timer(io_service_, std::chrono::seconds(AUTO_RECONNECT_TIME)));
    timer->async_wait([self = this->shared_from_this(), timer, target](const asio::error_code &error) {
      self->reconnect_after_timeout(timer, target, error);
    });
  }
}

void BasicClient::reconnect_after_timeout(TimerPointer, Target target, const asio::error_code &error) {
  if (!error) {
    if (connection_by_target_.find(target) == connection_by_target_.end())
      async_connect(target.first, target.second);
  } else {
    MX_LOG(ERROR, HIGHVERBOSITY,
           CTX("BasicClient") TEXT("auto reconnect to " + target.first + ":" + repr(target.second) +
                                   " cancelled by error " + repr(error)));
  }
}

// A new connection for `target`, in the map. Connecting twice to one target
// replaces the earlier connection, which is also how the reconnect timer
// behaves if the caller connected again in the meantime.
BasicClient::Connection::pointer BasicClient::_new_connection(const Target &target) {
  MX_DCHECK_RUN_ON(&owner_thread());
  ConnectionByTarget::iterator target_entry = connection_by_target_.find(target);
  if (target_entry != connection_by_target_.end()) {
    if (Connection::pointer conn = target_entry->second.lock())
      conn->shutdown();
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
ConnectionWrapper BasicClient::async_connect(const asio::ip::tcp::endpoint &peer_endpoint) {
  Connection::pointer conn = _new_connection(Target(peer_endpoint.address().to_string(), peer_endpoint.port()));
  conn->managers_private_data().candidates.assign(1, peer_endpoint);
  _try_next_candidate(conn);
  return _wrap(conn);
}

// A host name, or an address in text: the name is resolved on this thread
// first, every address it has tried in turn; the wrapper is returned at
// once, unspecified until an address is in use.
ConnectionWrapper BasicClient::async_connect(const std::string &host, std::uint16_t port) {
  asio::error_code literal;
  asio::ip::address address = asio::ip::make_address(host, literal);
  if (!literal)
    return async_connect(Endpoint(address, port));
  Connection::pointer conn = _new_connection(Target(host, port));
  _resolve_and_start(conn);
  return _wrap(conn);
}

void BasicClient::_resolve_and_start(Connection::pointer conn) {
  MX_DCHECK_RUN_ON(&owner_thread());
  const Target &target = conn->managers_private_data().target;
  if (resolver_hook_) {
    asio::error_code error;
    std::vector<Endpoint> candidates = resolver_hook_(target.first, target.second, error);
    _resolved(conn, error, candidates);
    return;
  }
  resolver_.async_resolve(target.first, std::to_string(target.second),
                          [self = this->shared_from_this(), conn](const asio::error_code &error,
                                                                  asio::ip::tcp::resolver::results_type results) {
                            std::vector<Endpoint> candidates;
                            for (const asio::ip::tcp::resolver::results_type::value_type &entry : results)
                              candidates.push_back(entry.endpoint());
                            self->_resolved(conn, error, candidates);
                          });
}

void BasicClient::_resolved(Connection::pointer conn, const asio::error_code &error, std::vector<Endpoint> candidates) {
  if (conn->shuts_down())
    return;
  const Target &target = conn->managers_private_data().target;
  if (error || candidates.empty()) {
    MX_LOG(WARNING, MEDIUMVERBOSITY,
           CTX("BasicClient") TEXT(target.first + ":" + repr(target.second) + " does not resolve" +
                                   (error ? ": " + error.message() : std::string()) + "; trying again later"));
    conn->shutdown(); // arms the reconnect timer, which resolves again
    return;
  }
  conn->managers_private_data().candidates = candidates;
  conn->managers_private_data().next_candidate = 0;
  _try_next_candidate(conn);
}

// The next address of the connection's target, or the end of this attempt
// when every one failed; the reconnect timer then starts the next.
void BasicClient::_try_next_candidate(Connection::pointer conn) {
  auto &data = conn->managers_private_data();
  if (data.next_candidate >= data.candidates.size()) {
    conn->shutdown();
    return;
  }
  data.expected_endpoint = data.candidates[data.next_candidate++];
  asio::error_code ignored;
  conn->socket().close(ignored); // a socket that failed to connect is reopened by async_connect
  conn->socket().async_connect(
      data.expected_endpoint,
      [self = this->shared_from_this(), conn](const asio::error_code &error) { self->_connected(conn, error); });
}

void BasicClient::bind_to_current_thread() {
  bind_owner_to_current_thread();
  for (ConnectionByTarget::iterator entry = connection_by_target_.begin(); entry != connection_by_target_.end();
       ++entry) {
    if (Connection::pointer conn = entry->second.lock())
      conn->bind_io_thread_to_current();
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

ConnectionWrapper BasicClient::connect(const asio::ip::tcp::endpoint &peer_endpoint, float timeout) {
  ConnectionWrapper connwrap = async_connect(peer_endpoint);
  wait_for_connection(connwrap, timeout);
  return connwrap;
}

ConnectionWrapper BasicClient::connect(const std::string &host, std::uint16_t port, float timeout) {
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
