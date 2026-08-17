// The client side of the protocol: connections to one or more multiplexers,
// an incoming queue, and reconnection. Used by Client (client.h), which adds
// the query algorithm and the calls peers actually make; nothing else
// includes this directly.
//
// A BasicClient is a ConnectionsManager whose peers are all multiplexers.
// It runs no thread of its own: every wait_* or flush call runs the shared
// io_service until its condition holds or its timer expires, and between
// calls nothing happens. That is why a peer built on it must be marked
// is_passive in the rules file unless it keeps calling in (a backend's
// serve_forever loop does).
//
// Tricky parts, each commented at the spot: the tribool message tracker that
// reports sent/lost per message; the round-robin choice of a connection in
// schedule_one; hand-over of unsent messages from a dead connection to a live
// one; the duplicate filter on message ids; and the 3 s reconnect timer.
#ifndef MX_MULTIPLEXER_BASIC_CLIENT_H_
#define MX_MULTIPLEXER_BASIC_CLIENT_H_

#include "lib/assertion.h"
#include "lib/functors.h"
#include "lib/spanset.h"
#include "lib/timer.h"
#include "lib/triple.h"
#include "multiplexer/connections_manager.h"
#include "multiplexer/defaults.h"
#include "multiplexer/io/connection.h"
#include <boost/asio/deadline_timer.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/cstdint.hpp>
#include <boost/logic/tribool.hpp>
#include <boost/logic/tribool_io.hpp>
#include <boost/shared_ptr.hpp>
#include <deque>

namespace multiplexer {

struct BasicClientTraits {
  typedef boost::asio::deadline_timer Timer;
  typedef boost::shared_ptr<Timer> TimerPointer;
  typedef boost::asio::ip::tcp::endpoint Endpoint;
};

class BasicClient;
class Client;

// How the client's connections queue messages and report on them. Each queue
// entry pairs the frame with a tribool: indeterminate while queued, true once
// written to the socket, false if the connection died first. The entry holds
// the tribool weakly and the tracker handed to the caller holds it strongly,
// so a caller that does not keep the tracker costs nothing.
template <> struct ConnectionsManagerTraits<BasicClient> : public DefaultConnectionsManagerTraits {

  typedef DefaultConnectionsManagerTraits Base;

  struct MessagesBufferTraits : public Base::MessagesBufferTraits {
    typedef ConnectionsManagerTraits::Base::MessagesBufferTraits Base;

    typedef std::pair<boost::shared_ptr<boost::tribool>, boost::shared_ptr<const RawMessage>> temporary_value_type;
    typedef std::pair<boost::weak_ptr<boost::tribool>, boost::shared_ptr<const RawMessage>> value_type;

    typedef mx::SecondFromPairExtractor<value_type> ToRawMessagePointerConverter;

    struct ToBufferRepresentationConverter
        : public std::function<temporary_value_type(boost::shared_ptr<const RawMessage>)> {

      temporary_value_type operator()(boost::shared_ptr<const RawMessage> raw) const {

        return temporary_value_type(temporary_value_type::first_type(new boost::tribool(boost::logic::indeterminate)),
                                    raw);
      }
    };
    typedef mx::FirstFromPairExtractor<temporary_value_type> SchedulingResultFunctor;

    struct SendingResultNotifier : public Base::SendingResultNotifier {

      template <typename ConnectionsManagerImplementationWeakPointer, typename QueueType>
      void notify_success(ConnectionsManagerImplementationWeakPointer, QueueType &qe) const {

        if (temporary_value_type::first_type tbp = qe.first.lock())
          *tbp = true;
      }

      template <typename ConnectionsManagerImplementationWeakPointer, typename QueueType>
      void notify_error(ConnectionsManagerImplementationWeakPointer, QueueType &qe) const {

        if (temporary_value_type::first_type tbp = qe.first.lock())
          *tbp = false;
      }
    };
  };

  // Stored inside each Connection: the address it was asked to connect to,
  // so that a dropped connection can be re-established without the caller.
  struct ConnectionManagerPrivateDataInConnection {
  private:
    BasicClientTraits::Endpoint expected_endpoint;
    friend class BasicClient;
  };

  typedef multiplexer::Connection<BasicClient> Connection;
};

// The handle callers see for a connection: a weak reference plus the
// endpoint. It is false once the connection is gone, and scheduling through
// it may reconnect to the remembered endpoint (see schedule_one). Replies
// carry the wrapper of the connection they arrived on, so a peer can answer
// through the same multiplexer.
class ConnectionWrapper {
public:
  inline operator bool() const { return static_cast<bool>(lock()); }
  // Same connection, or the same endpoint once it is gone (the observer's
  // "down" notification carries only the endpoint).
  bool is_same_connection(const ConnectionWrapper &other) const { return endpoint_ == other.endpoint_; }

private:
  typedef ConnectionsManagerTraits<BasicClient>::Connection Connection;

public:
  ConnectionWrapper() {}
  ConnectionWrapper(const ConnectionWrapper &) = default;

private:
  // ConnectionWrapper(Connection::pointer conn) : conn_(conn) {}
  ConnectionWrapper(Connection::pointer conn, const BasicClientTraits::Endpoint &endpoint)
      : conn_(conn), endpoint_(endpoint) {}
  Connection::pointer lock() const { return conn_.lock(); }

public:
  ConnectionWrapper &operator=(const ConnectionWrapper &other) {
    if (this == &other)
      return *this;
    conn_ = other.conn_;
    endpoint_ = other.endpoint_;
    return *this;
  }

private:
  Connection::weak_pointer conn_;
  BasicClientTraits::Endpoint endpoint_;

  friend class BasicClient;
  friend class Client;
  friend class ThreadedClient;
};

// The three ways a client call fails; Client inherits them and the Python
// binding maps them to exceptions of the same names. NotConnected: no live
// connection to send through. OperationTimedOut: the call's timer expired.
// OperationFailed: the multiplexer(s) answered that nobody can take the
// message. Each overrides what() so the types stay distinct for the binding.
struct ExceptionDefinitions {
  struct MxClientError : public mx::Exception {
    const char *what() const throw() { return mx::Exception::what(); }
  };
  struct NotConnected : public MxClientError {
    const char *what() const throw() { return MxClientError::what(); }
  };
  // The client was inherited across a fork; see lib/fork.h. A NotConnected,
  // so that handlers for a broker outage catch it, with a message that says
  // what really happened.
  struct UsedAfterFork : public NotConnected {
    const char *what() const throw() { return "client used after fork; create a new one in the child"; }
  };
  struct OperationTimedOut : public MxClientError {
    const char *what() const throw() { return MxClientError::what(); }
  };
  struct OperationFailed : public MxClientError {
    const char *what() const throw() { return MxClientError::what(); }
  };
};

// See the file comment. Created through Client; always held by shared_ptr
// because connections keep weak references to their manager.
class BasicClient : public ConnectionsManager<BasicClient>,
                    public boost::enable_shared_from_this<BasicClient>,
                    public BasicClientTraits,
                    public ExceptionDefinitions {

private:
  BasicClient(boost::asio::io_service &io_service, boost::uint32_t client_type);

public:
  // definitions
  typedef ConnectionsManager<BasicClient> Base;

  typedef std::deque<
      mx::triple<boost::shared_ptr<const RawMessage>, ConnectionWrapper, boost::shared_ptr<MultiplexerMessage>>>
      IncomingMessagesBuffer;
  typedef MessagesBufferTraits::SchedulingResultFunctor::result_type BasicScheduledMessageTracker;

  // public constructor-like function
  typedef boost::shared_ptr<BasicClient> pointer;
  typedef boost::weak_ptr<BasicClient> weak_pointer;

  // The only way to make one: connections keep weak references to their
  // manager, so it must live in a shared_ptr.
  static pointer Create(boost::asio::io_service &io_service, unsigned short port) {
    return pointer(new BasicClient(io_service, port));
  }

  // ConnectionsManager interface: what a Connection needs from its manager.
  boost::shared_ptr<const RawMessage> get_welcome_message() {
    if (!welcome_message_) {
      welcome_message_ = create_welcome_message(client_type_);
    }
    return welcome_message_;
  }

  // Called by a Connection with every parsed message: drops ones without an
  // id or seen before, then queues or hands to the sink.
  void handle_message(Connection::pointer conn, boost::shared_ptr<const RawMessage> raw,
                      boost::shared_ptr<MultiplexerMessage> mxmsg);

  // Connectivity. async_connect returns at once; connect runs the loop until
  // the handshake completed or `timeout` passed, and returns the wrapper
  // either way (check it). A connection that fails or drops is retried from
  // connection_destroyed after AUTO_RECONNECT_TIME, whenever the loop runs.
  void shutdown(); // close every connection; idempotent

  // Fork, see lib/fork.h: a client a forked child inherited is an orphan
  // there. Every public entry point checks first and throws UsedAfterFork,
  // before touching any lock; the teardown in the child closes the child's
  // copies of the socket descriptors with close(2) only (no goodbye, no
  // shutdown(2), which would end the parent's connection) and touches no
  // mutex, then the owner leaks the object so asio never sees those
  // descriptor numbers again.
  bool orphaned() const;
  void check_not_orphaned() const;
  void orphan_close_descriptors();
  // Makes the calling thread the owner of this client and of every
  // connection it has, live or still connecting. For a client built on one
  // thread and driven from another; BaseMultiplexerServer::serve_forever()
  // calls it on entry. Only the debug-build thread checks care.
  void bind_to_current_thread();
  ConnectionWrapper async_connect(const Endpoint &peer_endpoint);            // start connecting, return at once
  bool wait_for_connection(ConnectionWrapper connwrap, float timeout) const; // run the loop until registered
  ConnectionWrapper connect(const Endpoint &peer_endpoint, float timeout);   // async_connect + wait
  void connection_destroyed(Connection *conn); // a connection ended; schedule the reconnect
  void reconnect_after_timeout(TimerPointer, Endpoint peer_endpoint, const boost::system::error_code &);

public:
  // A deadline `timeout` seconds from now on this client's io_service;
  // negative means never.
  std::unique_ptr<mx::SimpleTimer> create_timer(float timeout) const;

public:
  // The only peer a client accepts a welcome from is a multiplexer.
  bool accept_peer_type(boost::uint32_t peer_type) const {
    return peer_type == peers::MULTIPLEXER && Base::accept_peer_type(peer_type);
  }

  // The connections, by peer id (the multiplexers' instance ids).
  ConnectionById::const_iterator begin() const { return connection_by_id_.begin(); }
  ConnectionById::iterator begin() { return connection_by_id_.begin(); }
  ConnectionById::const_iterator end() const { return connection_by_id_.end(); }
  ConnectionById::iterator end() { return connection_by_id_.end(); }

  // Incoming messages, queued by handle_message() as (frame, connection,
  // parsed message) and taken in order. The queue is bounded
  // (DEFAULT_INCOMING_QUEUE_MAX_SIZE) and drops when full: a caller that does
  // not read must not be able to grow memory without limit.
  // Alternatively, a sink called on the owner thread as each message arrives
  // (ThreadedClient); while one is set the queue is not used.
  typedef std::function<void(const IncomingMessagesBuffer::value_type &)> IncomingSink;
  void set_incoming_sink(IncomingSink sink) {
    MX_DCHECK_RUN_ON(&owner_thread());
    incoming_sink_ = sink;
  }

  // Told, on the owner thread, when a connection completes its handshake
  // (up) and when one is gone (down), so a caller can move work that was
  // bound to a connection.
  typedef std::function<void(const ConnectionWrapper &, bool up)> ConnectionObserver;
  void set_connection_observer(ConnectionObserver observer) {
    MX_DCHECK_RUN_ON(&owner_thread());
    connection_observer_ = observer;
  }
  void after_connection_registration(Connection::pointer conn, const WelcomeMessage &) {
    MX_DCHECK_RUN_ON(&owner_thread());
    if (connection_observer_)
      connection_observer_(ConnectionWrapper(conn, conn->managers_private_data().expected_endpoint), true);
  }

  inline bool has_incoming_messages() const { return !incoming_messages_.empty(); }
  IncomingMessagesBuffer::value_type next_incoming_message(); // pop the oldest; only when has_incoming_messages()
  inline bool incoming_queue_full() const { return incoming_queue_max_size_ <= incoming_messages_.size(); }

  // Runs the loop until a message is queued or `timeout` seconds pass;
  // throws OperationTimedOut, or NotConnected when no connection exists.
  void inline wait_for_incoming_message(float timeout = -1) const {
    if (has_incoming_messages())
      return;
    std::unique_ptr<mx::SimpleTimer> timer = create_timer(timeout);
    return wait_for_incoming_message(*timer);
  }

  // One step of the io loop. asio marks an io_service stopped once it runs
  // out of work, and run_one() on a stopped service returns at once without
  // running anything, not even a timer armed afterwards: a client with no
  // connections reaches that state after its first timed wait, and every
  // later wait would spin forever. Restarting the service first makes the
  // new wait's timer fire as intended.
  std::size_t run_one() const {
    if (io_service_.stopped())
      io_service_.reset();
    return io_service_.run_one();
  }

  // Every handler that is ready, without waiting. A peer that closed its
  // end while no call ran the loop is noticed here, and its connection
  // retired, which is why the synchronous client calls this before it
  // chooses a connection for a message: a write into a socket the other
  // side has closed succeeds, and the message would be lost with no error.
  void poll() const {
    MX_DCHECK_RUN_ON(&owner_thread());
    if (io_service_.stopped())
      io_service_.reset();
    io_service_.poll();
  }

  // Runs the loop until a message is queued or the timer expires. A socket
  // close, a reconnect timer or a heartbeat are all handled in here as a side
  // effect, which is the only time a passive client notices any of them.
  void inline wait_for_incoming_message(mx::SimpleTimer &timer) const {
    MX_DCHECK_RUN_ON(&owner_thread());
    unsigned int cc = 1;
    const bool DISABLE_IFCONNECTED_CHECK = true; // TODO
    while (!has_incoming_messages() && !timer.expired() && ((cc = connections_count(true)), DISABLE_IFCONNECTED_CHECK))
      run_one();

    if (!has_incoming_messages()) {
      if (timer.expired())
        MXTHROW(OperationTimedOut());
      else if (!cc)
        MXTHROW(NotConnected());
      else {
        // TODO logger.warning << "wait_for_incoming_message()
        // operation failed for unknown reason";
        MXTHROW(OperationFailed());
      }
    }
  }

  // Outgoing messages. schedule_all queues the frame on every live connection
  // with room and returns how many; the receivers drop the copies by id.
  unsigned int schedule_all(boost::shared_ptr<const RawMessage> raw) {
    MX_DCHECK_RUN_ON(&owner_thread());
    unsigned int c = 0;
    for (ConnectionById::const_iterator entry = connection_by_id_.begin(); entry != connection_by_id_.end(); ++entry) {

      if (Connection::pointer conn = entry->second.lock()) {
        if (conn->outgoing_queue_full() || !conn->living())
          continue;
        if (conn->schedule(raw))
          ++c;
      }
    }
    return c;
  }

  // Queues the frame on one connection, round robin over the multiplexers:
  // the connection used is moved to the back of the list, and connections
  // that are dead or full are skipped. Returns a null tracker when none took
  // it.
  BasicScheduledMessageTracker schedule_one(boost::shared_ptr<const RawMessage> raw, ConnectionWrapper *used = NULL) {
    MX_DCHECK_RUN_ON(&owner_thread());
    Connection::pointer conn;
    ConnectionsList &connections = connections_by_type_[peers::MULTIPLEXER];
    for (ConnectionsList::iterator entry = connections.begin();
         (entry = choose_free_connections(connections, entry)) != connections.end(); ++entry) {

      if (!(conn = entry->lock()))
        continue;
      BasicScheduledMessageTracker tracker = conn->schedule(raw);
      if (!tracker)
        continue;
      // round-robin: move *entry to the end of multiplexers list
      connections.splice(connections.end(), connections, entry);
      if (used)
        *used = ConnectionWrapper(conn, conn->managers_private_data().expected_endpoint);
      return tracker;
    }
    return BasicScheduledMessageTracker();
  }

  // Runs the loop until some connection is registered or the timer expires:
  // this is where a synchronous client's reconnect timers get to fire when
  // every connection is gone. True when a connection is available.
  bool wait_for_any_connection(mx::SimpleTimer &timer) {
    MX_DCHECK_RUN_ON(&owner_thread());
    while (connections_count(true) == 0 && !timer.expired())
      run_one();
    return connections_count(true) != 0;
  }

  // Like wait_for_incoming_message, but also returns, with false, as soon as
  // `watch` is no longer a live connection: a caller waiting for a reply
  // through it learns at once that the reply cannot come that way.
  bool wait_for_incoming_message_or_loss(mx::SimpleTimer &timer, const ConnectionWrapper &watch) const {
    MX_DCHECK_RUN_ON(&owner_thread());
    while (!has_incoming_messages() && !timer.expired() && watch)
      run_one();
    if (has_incoming_messages())
      return true;
    if (!watch)
      return false;
    MXTHROW(OperationTimedOut());
  }

  // Queues the frame on a specific connection, the one a request arrived on
  // or a reply came from. If that connection is gone, reconnects to its
  // endpoint within `timeout` and tries once more; with no timeout, throws.
  BasicScheduledMessageTracker schedule_one(boost::shared_ptr<const RawMessage> raw, ConnectionWrapper wrapper,
                                            float timeout) {
    MX_DCHECK_RUN_ON(&owner_thread());
    Connection::pointer conn;
    if ((conn = wrapper.lock()) && conn->living()) {
      // A connection closed by the peer while the loop did not run still
      // reads as living here; Client polls the loop before calling this.
      return (BasicScheduledMessageTracker)conn->schedule(raw);
    } else {
      assert(Endpoint().port() == 0);
      if (wrapper.endpoint_.port() && timeout > 0) {
        wrapper = this->connect(wrapper.endpoint_, timeout);
        return schedule_one(raw, wrapper, 0);
      } else
        MXTHROW(NotConnected());
    }
  }

  // A connection that shuts down with unsent messages offers them here; they
  // are spread round robin over the live connections, and whatever no
  // connection takes stays in the buffer and is reported lost by the caller.
  template <typename MessagesBuffer> void inline handle_orphaned_outgoing_messages(MessagesBuffer &outgoing_messages) {
    MX_DCHECK_RUN_ON(&owner_thread());
    std::list<Connection::pointer> working_connections;
    BOOST_FOREACH (Connection::weak_pointer connwp, connections_by_type_[peers::MULTIPLEXER])

      if (Connection::pointer conn = connwp.lock())
        if (conn->living())
          working_connections.push_back(conn);

    BOOST_FOREACH (typename MessagesBuffer::value_type &message, outgoing_messages) {

      for (size_t n = working_connections.size(); n; --n) {
        if (working_connections.front()->take_over(message)) {
          // move front element to the end
          working_connections.splice(working_connections.end(), working_connections, working_connections.begin());
          message.first.reset();
        } else
          working_connections.pop_front();
      }
      if (working_connections.empty())
        break;
    }
  }

  mx::Random64::result_type random64() { return random_(); }          // a message id
  boost::uint32_t inline client_type() const { return client_type_; } // this peer's type

private:
  typedef std::map<Endpoint, Connection::weak_pointer> ConnectionByEndpoint;

  /* instance properties */
  boost::uint32_t client_type_;
  bool shuts_down_;

  // received messages
  IncomingSink incoming_sink_;
  ConnectionObserver connection_observer_;
  IncomingMessagesBuffer incoming_messages_;
  unsigned int incoming_queue_max_size_;

  ConnectionByEndpoint connection_by_endpoint_;
  const unsigned int fork_generation_at_creation_;

  boost::shared_ptr<const RawMessage> welcome_message_;

  // Ids of the last 2048 messages received, across all connections. An event
  // sent through every multiplexer arrives once per multiplexer; the copies
  // are dropped here so the caller sees each id once.
  mx::SpanSet<boost::uint64_t, 2048> last_seen_message_ids_;
};

}; // namespace multiplexer

#endif // MX_MULTIPLEXER_BASIC_CLIENT_H_
