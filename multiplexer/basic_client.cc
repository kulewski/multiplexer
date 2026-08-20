// BasicClient: the parts that are not templates or one-liners. See
// basic_client.h for the design.

#include "multiplexer/basic_client.h"

#include <unistd.h>

#include "lib/fork.h"
#include "lib/logging/logging.h"
#include "lib/repr.h"
#include "multiplexer/multiplexer.constants.h"
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/foreach.hpp>

using namespace mx;
using namespace multiplexer;
using mx::SimpleTimer;
using std::cerr;

BasicClient::BasicClient(boost::asio::io_service &io_service, boost::uint32_t client_type)
    : Base(io_service), client_type_(client_type), shuts_down_(false),
      incoming_queue_max_size_(DEFAULT_INCOMING_QUEUE_MAX_SIZE), fork_generation_at_creation_(mx::fork_generation()) {}

bool BasicClient::orphaned() const { return mx::fork_generation() != fork_generation_at_creation_; }

void BasicClient::check_not_orphaned() const {
  if (orphaned())
    MXTHROW(UsedAfterFork());
}

void BasicClient::orphan_close_descriptors() {
  for (ConnectionByEndpoint::iterator entry = connection_by_endpoint_.begin(); entry != connection_by_endpoint_.end();
       ++entry) {
    if (Connection::pointer conn = entry->second.lock()) {
      int fd = conn->socket().native_handle();
      if (fd >= 0)
        ::close(fd);
    }
  }
}

void BasicClient::handle_message(Connection::pointer conn, boost::shared_ptr<const RawMessage> raw,
                                 boost::shared_ptr<MultiplexerMessage> mxmsg) {
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

  IncomingMessagesBuffer::value_type incoming =
      mx::make_triple(raw, ConnectionWrapper(conn, conn->managers_private_data().expected_endpoint), mxmsg);
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

// BasicClient::async_connect() helper function
static inline void handle_connect(BasicClient::Connection::pointer conn, const boost::system::error_code &error) {
  if (!error) {
    conn->start();
  } else {
    conn->shutdown();
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
  for (ConnectionByEndpoint::iterator next = connection_by_endpoint_.begin(), entry;
       next != connection_by_endpoint_.end() && (entry = next++, true);)
    if (Connection::pointer conn = entry->second.lock())
      // this does modify connection_by_endpoint_, so we have to use two
      // iterators
      conn->shutdown();
}

// Called from Connection::shutdown for any reason: the multiplexer closed,
// the connect failed, or shutdown() here. Unless the client itself is
// shutting down, a timer is armed to connect to the same endpoint again.
// The timer fires only while some call runs the loop, so a passive client
// reconnects during its next call at the earliest.
void BasicClient::connection_destroyed(Connection *conn) {
  MX_DCHECK_RUN_ON(&owner_thread());
  MX_LOG(DEBUG, HIGHVERBOSITY, CTX("BasicClient") TEXT("connection_destroyed(" + repr(conn) + ")"));
  Connection::pointer c;
  ConnectionByEndpoint::iterator endpoint_entry =
      connection_by_endpoint_.find(conn->managers_private_data().expected_endpoint);

  if (endpoint_entry != connection_by_endpoint_.end() && (c = endpoint_entry->second.lock()) && c.get() == conn) {
    connection_by_endpoint_.erase(endpoint_entry);
  }

  if (connection_observer_)
    connection_observer_(ConnectionWrapper(Connection::pointer(), conn->managers_private_data().expected_endpoint),
                         false);
  if (!shuts_down_) {
    // auto reconnect after AUTO_RECONNECT_TIME seconds
    MX_LOG(DEBUG, LOWVERBOSITY,
           CTX("BasicClient") TEXT("scheduling reconnecting after " + repr(AUTO_RECONNECT_TIME) + " seconds to " +
                                   repr(conn->managers_private_data().expected_endpoint)));
    TimerPointer timer(new Timer(io_service_, boost::posix_time::seconds(AUTO_RECONNECT_TIME)));
    timer->async_wait(boost::bind(&BasicClient::reconnect_after_timeout, this->shared_from_this(), timer,
                                  conn->managers_private_data().expected_endpoint, boost::asio::placeholders::error));
  }
}

void BasicClient::reconnect_after_timeout(TimerPointer, Endpoint peer_endpoint,
                                          const boost::system::error_code &error) {
  if (!error) {
    if (connection_by_endpoint_.find(peer_endpoint) == connection_by_endpoint_.end())
      async_connect(peer_endpoint);
  } else {
    MX_LOG(ERROR, HIGHVERBOSITY,
           CTX("BasicClient") TEXT("auto reconnect to " + repr(peer_endpoint) +
                                   "cancelled "
                                   "by error" +
                                   repr(error)));
  }
}

// Starts a connection to `peer_endpoint`; the handshake runs asynchronously
// once the socket connects (Connection::start). Connecting twice to one
// endpoint replaces the earlier connection, which is also how the reconnect
// timer behaves if the caller connected again in the meantime.
ConnectionWrapper BasicClient::async_connect(const boost::asio::ip::tcp::endpoint &peer_endpoint) {
  MX_DCHECK_RUN_ON(&owner_thread());

  // close any previous connections with the same endpoint
  ConnectionByEndpoint::iterator endpoint_entry = connection_by_endpoint_.find(peer_endpoint);
  if (endpoint_entry != connection_by_endpoint_.end()) {
    if (Connection::pointer conn = endpoint_entry->second.lock())
      conn->shutdown();
  }
#ifndef NDEBUG
  // discard weak references that point to no connections at all
  for (ConnectionByEndpoint::iterator next = connection_by_endpoint_.begin(), entry;
       next != connection_by_endpoint_.end() && (entry = next++, true);) {
    if (!entry->second.lock()) {
      MX_LOG(ERROR, LOWVERBOSITY,
             CTX("BasicClient.connect") TEXT("there should be no dangling weak references in "
                                             "connection_by_endpoint_"));
      connection_by_endpoint_.erase(entry);
    }
  }
#endif

  // save newly created connection in the map
  Connection::pointer new_connection = Connection::Create(io_service_, this->shared_from_this());
  new_connection->managers_private_data().expected_endpoint = peer_endpoint;
  connection_by_endpoint_.insert(std::make_pair(peer_endpoint, new_connection));

  // start connection asynchronously
  new_connection->socket().async_connect(peer_endpoint,
                                         boost::bind(handle_connect, new_connection, boost::placeholders::_1));

  return ConnectionWrapper(new_connection, new_connection->managers_private_data().expected_endpoint);
}

void BasicClient::bind_to_current_thread() {
  bind_owner_to_current_thread();
  for (ConnectionByEndpoint::iterator entry = connection_by_endpoint_.begin(); entry != connection_by_endpoint_.end();
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

ConnectionWrapper BasicClient::connect(const boost::asio::ip::tcp::endpoint &peer_endpoint, float timeout) {

  ConnectionWrapper connwrap = async_connect(peer_endpoint);
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
