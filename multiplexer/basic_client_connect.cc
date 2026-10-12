// BasicClient's connections coming and going: connecting to every address
// a target resolves to, each attempt with its deadline; the registration
// at the multiplexer's welcome; the end of a connection and the reconnect;
// disconnect() and the waits of connect(). Part of BasicClient; see
// basic_client.cc.

#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cmath>

#include "lib/logging/logging.h"
#include "lib/repr.h"
#include "lib/seconds.h"
#include "multiplexer/basic_client.h"
#include "multiplexer/endpoint.h"

using namespace mx;
using namespace multiplexer;
using mx::SimpleTimer;

// After the socket's connect: the handshake, or the next address the
// target resolved to, or the end of this attempt.
void BasicClient::_connected(Connection::pointer conn, const asio::error_code& error) {
  auto& data = conn->managers_private_data();
  const bool timed_out = data.connect_timed_out;
  data.connecting = false;
  data.connect_timed_out = false;
  if (data.connect_deadline) {
    data.connect_deadline->cancel();
  }
  if (!error) {
    data.routing_in_welcome = routing_version_;  // what start() sends
    conn->start();
  } else if (!conn->shuts_down()) {
    MX_LOG(DEBUG, MEDIUMVERBOSITY,
           CTX("BasicClient")
               TEXT("connect to " + repr(data.expected_endpoint) + " failed: " +
                    (timed_out ? "no answer within " + repr(CONNECT_ATTEMPT_SECONDS) + " s" : error.message())));
    _try_next_candidate(conn);
  }
}

void BasicClient::register_connection(Connection::pointer conn, const WelcomeMessage& welcome) {
  MX_DCHECK_RUN_ON(&owner_thread());
  // Multiplexer ids are random, one per process: a target whose last
  // welcome came from the same id reaches this multiplexer too. Its
  // connection is often gone already, the multiplexer having replaced it
  // with this one as this one welcomed, and comes back at its reconnect.
  const Target& target = conn->managers_private_data().target;
  for (const std::pair<const Target, std::uint64_t>& reached : reached_by_target_) {
    if (reached.second != welcome.id() || reached.first == target) {
      continue;
    }
    const std::pair<Target, Target> said(std::minmax(reached.first, target));
    if (one_multiplexer_said_.insert(said).second) {
      MX_LOG(WARNING, LOWVERBOSITY,
             CTX("BasicClient") TEXT("multiplexer " + repr(welcome.id()) + " is reached through two targets, " +
                                     format_endpoint(said.first.first, said.first.second) + " and " +
                                     format_endpoint(said.second.first, said.second.second) +
                                     ": list it once; while both are listed, their connections replace or refuse "
                                     "each other at every reconnect, and what was queued on one replaced is lost"));
    }
  }
  reached_by_target_[target] = welcome.id();
  Base::register_connection(conn, welcome);
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
    first_connection_over_.insert(target);  // its first connection ended before it came up, if it had not
  }

  if (!shuts_down_ && still_wanted) {
    // auto reconnect after AUTO_RECONNECT_TIME seconds; armed before the
    // observer runs, so that nothing the observer does can skip it
    MX_LOG(DEBUG, LOWVERBOSITY,
           CTX("BasicClient") TEXT("scheduling reconnecting after " + repr(AUTO_RECONNECT_TIME) + " seconds to " +
                                   format_endpoint(target.first, target.second)));
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
  if (!shuts_down_) {
    _update_confirmation();  // the one that had not confirmed may be this
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
           CTX("BasicClient") TEXT("auto reconnect to " + format_endpoint(target.first, target.second) +
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
  first_connection_over_.erase(target);  // a later connect() is a first connection again
  reached_by_target_.erase(target);
  if (had) {
    MX_LOG(INFO, MEDIUMVERBOSITY,
           CTX("BasicClient") TEXT("disconnecting from " + format_endpoint(target.first, target.second)));
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
           CTX("BasicClient") TEXT(format_endpoint(target.first, target.second) + " does not resolve" +
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
  // child reads has it before it connects, and closed on exec from its
  // first instant, which asio's open cannot ask for: a program the process
  // runs does not keep the connection open after the client closed it or
  // died. One that cannot be opened is reported through the loop, as
  // async_connect does.
  const asio::ip::tcp protocol = data.expected_endpoint.protocol();
  asio::error_code error;
  const int fd = ::socket(protocol.family(), protocol.type() | SOCK_CLOEXEC, protocol.protocol());
  if (fd < 0) {
    error.assign(errno, asio::error::get_system_category());
  } else {
    conn->socket().assign(protocol, fd, error);
    if (error) {
      ::close(fd);
    }
  }
  if (error) {
    asio::post(io_service_, [self = this->shared_from_this(), conn, error] { self->_connected(conn, error); });
    return;
  }
  descriptors_.add(fd);
  // An address that drops the SYNs would hold the attempt, and the
  // target's other addresses and its reconnects, for the kernel's own
  // timeout, two minutes: past CONNECT_ATTEMPT_SECONDS the connect is
  // cancelled, and _connected takes it for the failure it is. Only while it
  // is under way, so that a deadline met by a connect that had just
  // succeeded leaves the connection's first read alone.
  if (!data.connect_deadline) {
    data.connect_deadline = std::make_shared<Timer>(io_service_);
  }
  data.connecting = true;
  data.connect_deadline->expires_after(mx::from_seconds(CONNECT_ATTEMPT_SECONDS));
  data.connect_deadline->async_wait([conn](const asio::error_code& error) {
    auto& data = conn->managers_private_data();
    if (!error && data.connecting) {
      data.connect_timed_out = true;
      asio::error_code ignored;
      conn->socket().cancel(ignored);
    }
  });
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
