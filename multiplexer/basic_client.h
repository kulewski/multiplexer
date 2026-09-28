// The client side of the protocol: connections to one or more multiplexers,
// an incoming queue, and reconnection. Used by Client (client.h), which adds
// the query algorithm and the calls peers actually make; nothing else
// includes this directly.
//
// A BasicClient is a ConnectionsManager whose peers are all multiplexers.
// It runs no thread of its own: every wait_* or flush call runs the shared
// io_service until its condition holds or its timer expires, and between
// calls nothing happens. That is why a peer built on it must be marked
// is_passive in the rules file unless it keeps calling in
// (BaseMultiplexerServer's serve_forever loop does).
//
// Tricky parts, each commented at the spot: the tribool message tracker that
// reports sent/lost per message; the round-robin choice of a connection in
// schedule_one; hand-over of unsent messages from a dead connection to a live
// one; the duplicate filter on message ids; and the 3 s reconnect timer.
#ifndef MX_MULTIPLEXER_BASIC_CLIENT_H_
#define MX_MULTIPLEXER_BASIC_CLIENT_H_

#include <algorithm>
#include <asio/ip/tcp.hpp>
#include <asio/steady_timer.hpp>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "lib/assertion.h"
#include "lib/exception.h"
#include "lib/fork.h"
#include "lib/functors.h"
#include "lib/mutex.h"
#include "lib/spanset.h"
#include "lib/timer.h"
#include "lib/triple.h"
#include "multiplexer/connections_manager.h"
#include "multiplexer/defaults.h"
#include "multiplexer/io/connection.h"
#include "multiplexer/log_summary.h"

namespace multiplexer {

struct BasicClientTraits {
  typedef asio::steady_timer Timer;
  typedef std::shared_ptr<Timer> TimerPointer;
  typedef asio::ip::tcp::endpoint Endpoint;
  // What a connection was asked for: a host name or address, and a port.
  // Resolved on every attempt, so a multiplexer whose address changed is
  // found again at the next reconnect.
  typedef std::pair<std::string, std::uint16_t> Target;
};

class BasicClient;
class Client;

// `msg` framed for a send, as every send that takes a whole message frames
// it: as it is when it has an id and a sender, else a copy with an empty id
// made fresh by `fresh_id` and an empty sender set to `instance_id`, as
// new_message() and a reply fill them, since every receiver drops a message
// without an id.
template <typename FreshId>
std::shared_ptr<const RawMessage> frame_stamped(const MultiplexerMessage& msg, std::uint64_t instance_id,
                                                FreshId fresh_id) {
  if (msg.id() && msg.from()) {
    return std::shared_ptr<const RawMessage>(RawMessage::FromMessage(msg));
  }
  MultiplexerMessage stamped(msg);
  if (!stamped.id()) {
    stamped.set_id(fresh_id());
  }
  if (!stamped.from()) {
    stamped.set_from(instance_id);
  }
  return std::shared_ptr<const RawMessage>(RawMessage::FromMessage(stamped));
}

// A queued frame's fate: not written yet, written to the socket, or
// dropped.
enum class SendState : unsigned char { QUEUED, SENT, LOST };

// Why a client gave up on a message the program sent, as its drop observer
// and docs/semantics.md name it.
enum class DropReason : unsigned char {
  NO_ROOM,          // it waited for room on a full connection past its timeout, or had none to wait
  NO_CONNECTION,    // it waited for a connection to come up past its timeout, or had none to wait
  CONNECTION_LOST,  // its connection ended and nothing else could take it: a pinned lane's, a copy for ALL
  SHUT_DOWN,        // the client shut down before it went
};

// How the client's connections queue messages and report on them. Each queue
// entry pairs the frame with a SendState: QUEUED while queued, SENT once
// written to the socket, which is the kernel's buffer and not the
// multiplexer, LOST once dropped: its connection ended before writing it,
// or it waited past its deadline. A frame whose write had started when its
// connection ended reads what that write did. The entry holds the state
// weakly and the tracker handed to the caller holds it strongly, so a
// caller that does not keep the tracker costs nothing.
template <>
struct ConnectionsManagerTraits<BasicClient> : public DefaultConnectionsManagerTraits {
  typedef DefaultConnectionsManagerTraits Base;

  // A ThreadedClient holds back what a full connection cannot take and
  // queues it as soon as the connection says it has room.
  static constexpr bool REPORTS_ROOM = true;
  // What a connection that ends leaves and nothing can take, the client
  // reports itself (BasicClient::report_drop).
  static constexpr bool REPORTS_DROPS = true;
  // What a client's connection reads is for the client.
  static constexpr bool READS_END_HERE = true;

  struct MessagesBufferTraits : public Base::MessagesBufferTraits {
    typedef ConnectionsManagerTraits::Base::MessagesBufferTraits Base;

    typedef std::pair<std::shared_ptr<SendState>, std::shared_ptr<const RawMessage>> temporary_value_type;
    typedef std::pair<std::weak_ptr<SendState>, std::shared_ptr<const RawMessage>> value_type;

    typedef mx::SecondFromPairExtractor<value_type> ToRawMessagePointerConverter;

    struct ToBufferRepresentationConverter
        : public std::function<temporary_value_type(std::shared_ptr<const RawMessage>)> {
      temporary_value_type operator()(std::shared_ptr<const RawMessage> raw) const {
        return temporary_value_type(temporary_value_type::first_type(new SendState(SendState::QUEUED)), raw);
      }
    };
    typedef mx::FirstFromPairExtractor<temporary_value_type> SchedulingResultFunctor;

    struct SendingResultNotifier : public Base::SendingResultNotifier {
      template <typename ConnectionsManagerImplementationWeakPointer, typename QueueType>
      void notify_success(ConnectionsManagerImplementationWeakPointer manager, QueueType& qe) const {
        if (temporary_value_type::first_type state = qe.first.lock()) {
          *state = SendState::SENT;
          if (auto owner = manager.lock()) {
            owner->tracked_message_done(state, true);  // somebody holds the tracker: maybe waiting on it
          }
        }
      }

      template <typename ConnectionsManagerImplementationWeakPointer, typename QueueType>
      void notify_error(ConnectionsManagerImplementationWeakPointer manager, QueueType& qe) const {
        if (temporary_value_type::first_type state = qe.first.lock()) {
          *state = SendState::LOST;
          if (auto owner = manager.lock()) {
            owner->tracked_message_done(state, false);
          }
        }
      }
    };
  };

  // Stored inside each Connection: what it was asked to connect to, so that
  // a dropped connection can be re-established without the caller; the
  // addresses that resolved to this time, tried in turn; and the one in use.
  struct ConnectionManagerPrivateDataInConnection {
   private:
    BasicClientTraits::Target target;
    BasicClientTraits::Endpoint expected_endpoint;
    std::vector<BasicClientTraits::Endpoint> candidates;
    std::size_t next_candidate = 0;
    // The client's routing (see BasicClient::set_routing) as versions: the
    // one this connection's welcome carried, and the last one the
    // multiplexer confirmed, by its own welcome or by the PEER_STATUS
    // answering the last PEER_CONTROL sent here, whose id this keeps.
    unsigned int routing_in_welcome = 0;
    unsigned int routing_acknowledged = 0;
    std::uint64_t routing_request_id = 0;
    const BasicClient* owner = nullptr;  // the client that made it, which alone may place on it
    friend class BasicClient;
  };

  typedef multiplexer::Connection<BasicClient> Connection;
};

// The three ways a client call fails; Client inherits them and the Python
// binding maps them to exceptions of the same names. NotConnected: no live
// connection to send through. OperationTimedOut: the call's timer expired.
// OperationFailed: the multiplexer(s) answered that nobody can take the
// message. Each overrides what() so the types stay distinct for the binding.
struct ExceptionDefinitions {
  struct MxClientError : public mx::Exception {
    const char* what() const throw() { return mx::Exception::what(); }
  };
  struct NotConnected : public MxClientError {
    const char* what() const throw() { return MxClientError::what(); }
  };
  // The client was inherited across a fork; see lib/fork.h. A NotConnected,
  // so that handlers for a broker outage catch it, with a message that says
  // what really happened.
  struct UsedAfterFork : public NotConnected {
    const char* what() const throw() { return "client used after fork; create a new one in the child"; }
  };
  struct OperationTimedOut : public MxClientError {
    const char* what() const throw() { return MxClientError::what(); }
  };
  struct OperationFailed : public MxClientError {
    const char* what() const throw() { return MxClientError::what(); }
  };
};

// The handle callers see for a connection: a weak reference plus what it
// was asked to connect to and the address that resolved to. It is false
// once the connection is gone; schedule_one() then puts a message meant
// for it on another live connection. Replies carry the wrapper of the
// connection they arrived on, so a peer can answer through the same
// multiplexer. A wrapper made before a fork names the parent's connection
// in the child, a socket the parent still writes, and is refused there
// with UsedAfterFork (lib/fork.h).
class ConnectionWrapper {
 public:
  inline operator bool() const { return static_cast<bool>(lock()); }
  // Made before a fork this process is the child of.
  bool inherited() const { return generation_ != mx::fork_generation(); }
  // Same connection, or the same target once it is gone (the observer's
  // "down" notification carries only the target).
  bool is_same_connection(const ConnectionWrapper& other) const { return target_ == other.target_; }

 private:
  typedef ConnectionsManagerTraits<BasicClient>::Connection Connection;

 public:
  ConnectionWrapper() : generation_(mx::fork_generation()) {}
  ConnectionWrapper(const ConnectionWrapper&) = default;

 private:
  ConnectionWrapper(Connection::pointer conn, const BasicClientTraits::Target& target,
                    const BasicClientTraits::Endpoint& endpoint, const BasicClient* owner)
      : conn_(conn), target_(target), endpoint_(endpoint), generation_(mx::fork_generation()), owner_(owner) {}
  Connection::pointer lock() const { return conn_.lock(); }

 public:
  ConnectionWrapper& operator=(const ConnectionWrapper& other) {
    if (this == &other) {
      return *this;
    }
    conn_ = other.conn_;
    target_ = other.target_;
    endpoint_ = other.endpoint_;
    generation_ = other.generation_;
    owner_ = other.owner_;
    return *this;
  }
  // What the connection was asked for, host and port, kept after it is gone.
  const BasicClientTraits::Target& target() const { return target_; }
  // The address that resolved to and the connection used, kept after it is
  // gone; unspecified until a name resolved.
  const BasicClientTraits::Endpoint& endpoint() const { return endpoint_; }

 private:
  Connection::weak_pointer conn_;
  BasicClientTraits::Target target_;
  BasicClientTraits::Endpoint endpoint_;
  unsigned int generation_;  // mx::fork_generation() when it was made
  // The client whose connection it names, null for an empty wrapper: no
  // other client places on it (BasicClient::check_ours), and nothing but
  // the pointer is read, from any thread.
  const BasicClient* owner_ = nullptr;

  friend class BasicClient;
  friend class Client;
  friend class Lane;
  friend class ThreadedClient;
};

// One connection for a stream of messages, owned by the caller: a soft,
// late pin, or made pinned, a hard one. A message sent or queried with a
// lane goes through the lane's connection while it is live. Empty until
// first use, when it pins itself to the connection the library chose; a
// lane that is not pinned lets go at a failover, the library writing the
// new connection into it, the one that took what the dead one had not
// written (BasicClient::_handed_to), and adopts the connection a query's reply came
// through, so the messages after a query follow the query. A pinned lane
// is its first connection for good: once that connection is gone, every
// send and query through the lane fails with NotConnected until the
// caller makes a new lane, and a message of its that the dying connection
// had not written is reported lost, never handed to another connection. A lane holds its connection weakly, like a
// ConnectionWrapper, and the connection's live flag, which closed() reads,
// and nothing else, and the library keeps no registry of lanes, so a lane
// lives exactly as long as the caller's pointer and keeps no connection
// alive. Shared between the caller's thread and a
// ThreadedClient's io thread, hence the mutex. A lane made before a fork,
// or seeded with a connection from before one, is the parent's in the
// child: every call throws UsedAfterFork there before it takes the mutex,
// which a parent thread may have held at the fork (lib/fork.h), and so
// does adopt() given a connection from before the fork. Decided when a
// connection enters the lane, so that the check on every message is one
// load. docs/api_cpp.md, "Lanes".
class Lane {
 public:
  explicit Lane(bool pinned = false) : pinned_(pinned), generation_(mx::fork_generation()) {}
  // Seeded with a connection, the one a reply came through; the lane is
  // as old as the connection, and learns its live flag at its first use
  // (watch()).
  explicit Lane(const ConnectionWrapper& connection, bool pinned = false)
      : connection_(connection),
        pinned_(pinned),
        holds_(true),
        generation_(connection.generation_),
        owner_(connection.owner_) {}
  Lane(const Lane&) = delete;
  Lane& operator=(const Lane&) = delete;

  bool pinned() const { return pinned_; }
  // The client whose connection the lane took first, or was seeded with,
  // null before: the lane is that client's (BasicClient::check_ours). One
  // load, no lock, from any thread.
  const BasicClient* owner() const { return owner_.load(std::memory_order_acquire); }
  // The connection held; empty until the first message went through.
  ConnectionWrapper connection() const {
    _check_made_here();
    mx::MutexLock lock(mutex_);
    return connection_;
  }
  // Whether a connection was ever written into the lane.
  bool holds_connection() const {
    _check_made_here();
    mx::MutexLock lock(mutex_);
    return holds_;
  }
  // Whether the connection held is live; see closed().
  bool connected() const {
    _check_made_here();
    mx::MutexLock lock(mutex_);
    return holds_ && _live();
  }
  // A pinned lane whose connection is gone: nothing goes through it any
  // more. From any thread, and from the moment the connection stops being
  // live, though its object lives on a while as it reads to the end: the
  // lane reads the connection's live flag, which it has once the
  // connection entered it on the client's thread, or, seeded with one,
  // once a message went through it; before that, once the object is gone.
  bool closed() const {
    _check_made_here();
    mx::MutexLock lock(mutex_);
    return pinned_ && holds_ && !_live();
  }
  // The library writes the connection it used or a reply came through, on
  // the client's thread; public because the Python synchronous client runs
  // the algorithm in Python. A pinned lane takes the first connection only.
  void adopt(const ConnectionWrapper& connection);
  // The library, on the client's thread, placing a message on `conn`: a
  // lane seeded with that connection learns its live flag, once.
  void watch(const std::shared_ptr<ConnectionsManagerTraits<BasicClient>::Connection>& conn) {
    if (!watched_.load(std::memory_order_acquire)) {
      _watch(conn);
    }
  }
  // Throws UsedAfterFork for a lane that is the parent's in this process:
  // made before the fork, or seeded with a connection from before it. For
  // an entry point taking a lane, on the caller's thread, before anything
  // reaches the io thread; one load, no lock.
  void check_not_inherited() const { _check_made_here(); }

 private:
  // Throws UsedAfterFork for a lane from before a fork, before any lock.
  void _check_made_here() const {
    if (generation_ != mx::fork_generation()) {
      MXTHROW(ExceptionDefinitions::UsedAfterFork());
    }
  }
  // The connection held is live: its flag when the lane has it, else its
  // object exists, asked without taking a reference, which on this thread
  // could end up the last one.
  bool _live() const MX_REQUIRES(mutex_) {
    return living_ ? living_->load(std::memory_order_acquire) : !connection_.conn_.expired();
  }
  void _watch(const std::shared_ptr<ConnectionsManagerTraits<BasicClient>::Connection>& conn);

  mutable mx::Mutex mutex_;
  ConnectionWrapper connection_ MX_GUARDED_BY(mutex_);
  std::shared_ptr<const std::atomic<bool>> living_ MX_GUARDED_BY(mutex_);  // connection_'s live flag
  std::atomic<bool> watched_{false};                                       // living_ is set: watch() is done
  const bool pinned_;
  bool holds_ MX_GUARDED_BY(mutex_) = false;
  const unsigned int generation_;  // mx::fork_generation() when it, or its seed, was made
  std::atomic<const BasicClient*> owner_{nullptr};
};
typedef std::shared_ptr<Lane> LanePtr;

// A query's on_received: told the instance id of the backend that
// acknowledged one of the query's attempts with REQUEST_RECEIVED, what a
// server's notify_start() sends. Nothing about the query changes for it.
typedef std::function<void(std::uint64_t backend)> ReceivedCallback;

// See the file comment. Created through Client; always held by shared_ptr
// because connections keep weak references to their manager.
class BasicClient : public ConnectionsManager<BasicClient>,
                    public std::enable_shared_from_this<BasicClient>,
                    public BasicClientTraits,
                    public ExceptionDefinitions {
 private:
  BasicClient(asio::io_service& io_service, std::uint32_t client_type);

 public:
  // definitions
  typedef ConnectionsManager<BasicClient> Base;

  typedef std::deque<
      mx::triple<std::shared_ptr<const RawMessage>, ConnectionWrapper, std::shared_ptr<MultiplexerMessage>>>
      IncomingMessagesBuffer;
  typedef MessagesBufferTraits::SchedulingResultFunctor::result_type BasicScheduledMessageTracker;

  ~BasicClient();  // where the outbox (multiplexer/outbox.h) is complete

  // public constructor-like function
  typedef std::shared_ptr<BasicClient> pointer;
  typedef std::weak_ptr<BasicClient> weak_pointer;

  // The only way to make one: connections keep weak references to their
  // manager, so it must live in a shared_ptr.
  static pointer Create(asio::io_service& io_service, std::uint32_t client_type) {
    return pointer(new BasicClient(io_service, client_type));
  }

  // ConnectionsManager interface: what a Connection needs from its manager.
  std::shared_ptr<const RawMessage> get_welcome_message() {
    if (!welcome_message_) {
      welcome_message_ = create_welcome_message(client_type_, routing_);
    }
    return welcome_message_;
  }

  // Which of a multiplexer's routing paths reach this peer
  // (Multiplexer.proto's Routing; everything by default): told to every
  // registered connection with PEER_CONTROL now, carried in the welcome
  // of every connection made or remade from now on, and sent after the
  // handshake on a connection whose welcome was already out. A backend
  // draining sets `any` and `all` off. On the owner thread.
  void set_routing(const Routing& routing);
  const Routing& routing() const { return routing_; }
  // Whether every connection has the current routing in effect: the
  // multiplexer confirmed it, by its welcome for a routing the
  // connection's welcome carried or by PEER_STATUS for a change; a
  // handshake still in flight counts as not confirmed. Then nothing
  // routed to this peer by a path turned off since is on its way from
  // those multiplexers, except as a last resort; what this client has
  // read but not yet handed out (has_incoming_messages()) still is, so
  // that counts as not confirmed too.
  bool routing_acknowledged() const;

  // Called by a Connection with every parsed message: drops ones without an
  // id or seen before, then queues or hands to the sink.
  void handle_message(Connection::pointer conn, std::shared_ptr<const RawMessage> raw,
                      std::shared_ptr<MultiplexerMessage> mxmsg);

  // Connectivity. async_connect returns at once; connect runs the loop until
  // the handshake completed or `timeout` passed, and returns the wrapper
  // either way (check it). A host name is resolved on this client's thread,
  // asynchronously, and every address it resolves to is tried in turn; a
  // name that does not resolve counts as an attempt that failed. A
  // connection that fails or drops is retried from connection_destroyed
  // after AUTO_RECONNECT_TIME, whenever the loop runs, resolving the name
  // again each time, so a multiplexer that moved is found at the next try,
  // until disconnect() drops the target.
  // Closes every connection, the polite way (Connection::close_gracefully):
  // each goes on reading what its multiplexer still sends, while the loop
  // runs, until that multiplexer's end or CLOSE_READ_SECONDS, so that what
  // was written before arrives; closing() says whether any still is.
  // Idempotent. What still waits is dropped and reported: a client that
  // writes it first flushes before (Client::shutdown).
  void shutdown();
  bool shuts_down() const { return shuts_down_; }
  bool closing();
  // Whether a connection is live or could still come up: one registered,
  // connecting or resolving, or a reconnect timer armed. False on a client
  // never connected, made with no address, or shut down, where nothing
  // held could ever be written: a wait for that would wait for nothing,
  // spinning when nothing else gives the loop work.
  bool connection_live_or_coming() const;
  // How many reconnects are armed, a timer each: none once shutdown()
  // returned, which cancels them and takes them out. For tests.
  std::size_t reconnects_pending() const {
    MX_DCHECK_RUN_ON(&owner_thread());
    return reconnect_timers_.size();
  }
  // The messages the client's connections read after they began closing,
  // in shutdown() or after a failed write, and dropped: a request among
  // them gets no answer, its sender waits out its timeout. Every one is
  // logged, as a WARNING when its connection ends. The protocol's own
  // answers to what the client sent, a DELIVERY_ERROR, REQUEST_RECEIVED,
  // BACKEND_ERROR, a control frame's status or a PING that answers, are
  // not counted: only the client waited for them.
  std::uint64_t dropped_while_closing() const { return dropped_while_closing_; }

  // Fork, see lib/fork.h: a client a forked child inherited is an orphan
  // there. Every public entry point that would take a lock, run the loop
  // or touch a socket checks first and throws UsedAfterFork, and so do the
  // clients' getters of the connections' state, which would answer with
  // the parent's. The teardown in the child
  // closes the child's copies of the socket descriptors with close(2) only
  // (no goodbye, no shutdown(2), which would end the parent's connection)
  // and touches no mutex, then the owner leaks the object so asio never
  // sees those descriptor numbers again. The close happens once, however
  // many of shutdown() and the destructors run it: a second would close
  // numbers the child has reused since.
  bool orphaned() const;
  void check_not_orphaned() const;
  // Throws std::invalid_argument for a connection of another client, or a
  // lane that took one: placed here, a message would be written by this
  // client's thread on the other client's connection, and the events of
  // its write would go to the other client. From any thread, a pointer
  // compare; an empty wrapper and a lane that took no connection pass.
  void check_ours(const ConnectionWrapper& connection) const;
  void check_ours(const LanePtr& lane) const;
  void orphan_close_descriptors();
  // Makes the calling thread the owner of this client and of every
  // connection it has, live or still connecting. For a client built on one
  // thread and driven from another; BaseMultiplexerServer::serve_forever()
  // calls it on entry. Only the debug-build thread checks care.
  void bind_to_current_thread();
  ConnectionWrapper async_connect(const Endpoint& peer_endpoint);                // an address: start connecting
  ConnectionWrapper async_connect(const std::string& host, std::uint16_t port);  // a name or an address
  bool wait_for_connection(ConnectionWrapper connwrap, float timeout) const;     // run the loop until registered
  ConnectionWrapper connect(const Endpoint& peer_endpoint, float timeout);       // async_connect + wait
  ConnectionWrapper connect(const std::string& host, std::uint16_t port, float timeout);
  // Drops the target async_connect() was given with this host and port, an
  // address in text matched as asio writes it, so that two spellings of
  // one address are one target: its reconnect timer stops, and nothing
  // connects to it again unless async_connect() is called again. A
  // connection to it on its way is abandoned; a live one is closed the
  // polite way, as shutdown() closes each (Connection::close_gracefully),
  // which hands what it had not written to the other connections, or has
  // it held, by the rules for a lost connection's, and what it wrote still
  // arrives. True when the client had the target: a connection to it, live
  // or on its way, or a reconnect armed. Throws NotConnected after
  // shutdown(), as async_connect() does.
  bool disconnect(const std::string& host, std::uint16_t port);
  bool disconnect(const Endpoint& peer_endpoint);
  // A connection ended; a reconnect is armed while its target is the client's.
  void connection_destroyed(Connection* conn);
  // A connection has ended for good: the messages it read after it began
  // closing, which it could only drop, are added up and logged.
  void connection_closed(Connection* conn);
  // PEER_CONTROL with the current routing on `conn`.
  void _send_routing(Connection::pointer conn);
  // A PEER_STATUS from a multiplexer: the routing it now applies to us.
  void _on_peer_status(Connection::pointer conn, const MultiplexerMessage& mxmsg);
  void reconnect_after_timeout(TimerPointer, Target target, const asio::error_code&);

  // How a host name becomes addresses: the system resolver unless a test
  // installs one, a function of the host and the port that returns the
  // addresses, or none with `error` set. Called on this client's thread.
  typedef std::function<std::vector<Endpoint>(const std::string& host, std::uint16_t port, asio::error_code& error)>
      Resolver;
  void set_resolver(Resolver resolver) { resolver_hook_ = resolver; }

 public:
  // A deadline `timeout` seconds from now on this client's io_service;
  // negative means never.
  std::unique_ptr<mx::SimpleTimer> create_timer(float timeout) const;

 public:
  // The only peer a client accepts a welcome from is a multiplexer.
  bool accept_peer_type(std::uint32_t peer_type) const {
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
  typedef std::function<void(const IncomingMessagesBuffer::value_type&)> IncomingSink;
  void set_incoming_sink(IncomingSink sink) {
    MX_DCHECK_RUN_ON(&owner_thread());
    incoming_sink_ = sink;
  }

  // Told, on the owner thread, when a connection completes its handshake
  // (up) and when one is gone (down), so a caller can move work that was
  // bound to a connection.
  typedef std::function<void(const ConnectionWrapper&, bool up)> ConnectionObserver;
  void set_connection_observer(ConnectionObserver observer) {
    MX_DCHECK_RUN_ON(&owner_thread());
    connection_observer_ = observer;
  }
  // Called by a connection, from inside its handlers, when a frame whose
  // tracker somebody still holds has been written to its socket
  // (`written`) or lost with it: a send's `done` may wait on it, or a
  // flush, which ends once the callbacks of what it waited for have run.
  void tracked_message_done(const std::shared_ptr<SendState>& state, bool written) {
    MX_DCHECK_RUN_ON(&owner_thread());
    _follow_event(state, written);
    _check_flushes();
  }
  // Called by a connection whose outgoing queue was full and has room
  // again, from inside its write handler: what waits for it goes in, in
  // order, as far as the room goes. See "The outbox" in outbox.cc.
  void outgoing_queue_has_room(Connection* conn);
  // A tracker for the frame queued last on `conn`, which everything queued
  // there before it is written ahead of; null when the queue is empty.
  // What a caller waiting until everything queued so far is out follows.
  BasicScheduledMessageTracker track_last_queued(const Connection::pointer& conn) {
    MX_DCHECK_RUN_ON(&owner_thread());
    Connection::MessagesBuffer::value_type* entry = conn->last_queued();
    if (!entry) {
      return BasicScheduledMessageTracker();
    }
    BasicScheduledMessageTracker state = entry->first.lock();
    if (!state) {
      state = std::make_shared<SendState>(SendState::QUEUED);
      entry->first = state;  // the queue holds the state weakly, as for any tracker
    }
    return state;
  }
  void after_connection_registration(Connection::pointer conn, const WelcomeMessage&) {
    MX_DCHECK_RUN_ON(&owner_thread());
    // The multiplexer's welcome confirms the routing ours carried; a
    // change made while the handshake was under way goes out now.
    auto& data = conn->managers_private_data();
    data.routing_acknowledged = data.routing_in_welcome;
    if (data.routing_in_welcome != routing_version_) {
      _send_routing(conn);
    }
    _place_held();  // what waited for a connection goes out now, in order
    if (connection_observer_) {
      connection_observer_(_wrap(conn), true);
    }
  }

  inline bool has_incoming_messages() const { return !incoming_messages_.empty(); }
  IncomingMessagesBuffer::value_type next_incoming_message();  // pop the oldest; only when has_incoming_messages()
  inline bool incoming_queue_full() const { return incoming_queue_max_size_ <= incoming_messages_.size(); }

  // The lines about messages this client dropped, at most about two per
  // kind and second (see LogSummary); for the io thread only, the classes
  // built on this one included. The kinds the library tells apart:
  enum DropLine : unsigned int {
    INCOMING_QUEUE_FULL,   // nobody read the incoming queue in time
    NO_ON_MESSAGE,         // a ThreadedClient given no callback for messages
    REQUESTS_QUEUE_FULL,   // a threaded backend's workers fell behind
    SENT_NO_ROOM,          // a message sent, given up on: DropReason::NO_ROOM
    SENT_NO_CONNECTION,    // DropReason::NO_CONNECTION
    SENT_CONNECTION_LOST,  // DropReason::CONNECTION_LOST
    SENT_SHUT_DOWN,        // DropReason::SHUT_DOWN
    OWN_LINES = 1000,      // and up: a message callback's own kinds (ThreadedClient::drop_lines)
  };
  LogSummary& drop_lines() { return drop_lines_; }

  // Every message the program sent that this client gives up on, a copy
  // for ALL each, is counted, logged in drop_lines(), and told to the drop
  // observer with its id and why, on the owner thread, from inside the
  // loop's handlers, so an observer only takes note. dropped() is the
  // count so far, from any thread. report_drop() is the one place every
  // drop goes through, the ThreadedClient's own included.
  typedef std::function<void(std::uint64_t message_id, DropReason reason)> DropObserver;
  void set_drop_observer(DropObserver observer) {
    MX_DCHECK_RUN_ON(&owner_thread());
    drop_observer_ = observer;
  }
  std::uint64_t dropped() const { return dropped_.load(std::memory_order_relaxed); }
  void report_drop(const std::shared_ptr<const RawMessage>& raw, DropReason reason);
  // Lets go of the observer, from the thread that ends the client: a
  // ThreadedClient's io thread at its end, or a forked child's teardown.
  void release_drop_observer() { drop_observer_ = DropObserver(); }

  // Runs the loop until a message is queued or `timeout` seconds pass;
  // throws OperationTimedOut, or NotConnected when nothing could arrive.
  void inline wait_for_incoming_message(float timeout = -1) const {
    if (has_incoming_messages()) {
      return;
    }
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
    if (io_service_.stopped()) {
      io_service_.reset();
    }
    return io_service_.run_one();
  }

  // Every handler that is ready, without waiting, until it has read a
  // queue's worth of frames (incoming_queue_max_size_): a frame read from a
  // socket that holds more makes the next read ready at once, so a peer
  // that writes faster than this client reads, whose socket never drains,
  // would keep io_context::poll() going for good, the call never returning
  // and the queue full and dropping. A peer that closed its end while no call
  // ran the loop is noticed here, and its connection retired, which is why
  // the synchronous client calls this before it chooses a connection for a
  // message: a write into a socket the other side has closed succeeds, and
  // the message would be lost with no error. A closure behind more unread
  // frames than that is noticed by a later call.
  void poll() const {
    MX_DCHECK_RUN_ON(&owner_thread());
    if (io_service_.stopped()) {
      io_service_.reset();
    }
    const std::uint64_t until = frames_handled_ + std::max(1u, incoming_queue_max_size_);
    while (frames_handled_ < until && io_service_.poll_one()) {
    }
  }

  // Runs the loop until a message is queued or the timer expires. A socket
  // close, a reconnect timer or a heartbeat are all handled in here as a side
  // effect, which is the only time a passive client notices any of them. A
  // client with nothing it could receive from, no connection and none on
  // its way (never connected, made with no addresses, or shut down), leaves
  // the loop no work at all: run_one() returns at once having done nothing,
  // and the wait throws NotConnected rather than spin.
  void inline wait_for_incoming_message(mx::SimpleTimer& timer) const {
    MX_DCHECK_RUN_ON(&owner_thread());
    while (!has_incoming_messages() && !timer.expired()) {
      if (run_one() == 0) {
        MXTHROW(NotConnected());
      }
    }
    if (!has_incoming_messages()) {
      poll();  // a last look, without waiting, at what has arrived: a zero timeout, expired at once, reads too
    }
    if (!has_incoming_messages()) {
      MXTHROW(OperationTimedOut());
    }
  }

  // Outgoing messages; see "The outbox" in outbox.cc. A message goes into a
  // connection's queue when the connection has room and nothing waits for
  // it before; otherwise it waits in the connection's backlog, in order,
  // until the connection has room, `timeout` seconds at most, after which
  // it is dropped, its tracker reading LOST. The tracker reads QUEUED
  // either way until the message is written. `number` is its place in the
  // order sent, which flush_all() goes by; 0 takes the next one. What
  // waits for a connection that dies goes to another, which `lane`, when
  // given, adopts; a pinned message (RawMessage::pinned) is lost instead,
  // and so is an ALL send's copy while another connection lives, which
  // has its own; with none live, the copy is held whole.
  //
  // schedule_all: a copy on every live connection; how many, the
  // connections in `used` and the copies' trackers in `trackers` when
  // given. The receivers drop the copies by id.
  unsigned int schedule_all(std::shared_ptr<const RawMessage> raw, std::vector<ConnectionWrapper>* used = NULL,
                            float timeout = DEFAULT_TIMEOUT, std::uint64_t number = 0,
                            std::vector<BasicScheduledMessageTracker>* trackers = NULL);
  // schedule_one: on one connection, round robin over those with room and
  // nothing waiting, the one used moved to the back; with none such, the
  // live one with the least waiting. Null only when no connection is live.
  BasicScheduledMessageTracker schedule_one(std::shared_ptr<const RawMessage> raw, ConnectionWrapper* used = NULL,
                                            float timeout = DEFAULT_TIMEOUT, std::uint64_t number = 0,
                                            LanePtr lane = LanePtr());
  // The connection the message whose tracker is `state` went to last: the
  // one a dead connection handed it to, or `first`, the one it was queued
  // on, when it was not handed over. What a flushing send reports as the
  // connection it used, and what a reply to it comes back through.
  ConnectionWrapper followed(const BasicScheduledMessageTracker& state, const ConnectionWrapper& first);
  // The next number in the order sent, for a caller that holds a message
  // before it is scheduled (ThreadedClient, waiting for a connection).
  std::uint64_t next_number();
  // The last number next_number() gave: a shutdown flushes again while it
  // moves, for what was sent during its write-out.
  std::uint64_t last_number() const;
  // How many times a message was moved on after waiting: from a backlog
  // into its queue, or from a dead connection to another. For tests.
  std::uint64_t retries() const;
  // How many entries the outbox keeps: the messages waiting for room or
  // held for a connection, and those that ended there and are not cleared
  // out yet, a bounded few however long messages keep ending. For tests.
  std::size_t outbox_entries() const;

  // flush_all(): a flush waits for the messages numbered up to the last one
  // at its start, those still waiting, for room or held for a connection,
  // and the last of them queued on each connection. `done` is told once,
  // when they are out, with whether every one was written, none given up
  // on meanwhile (report_drop); flushed() says they are out, all_written()
  // that and the rest; end_flush() forgets a flush, one that ran out of
  // time.
  struct Flush;
  typedef std::shared_ptr<Flush> FlushPtr;
  FlushPtr begin_flush(std::function<void(bool all_written)> done = std::function<void(bool)>());
  bool flushed(const FlushPtr& flush) const;
  bool all_written(const FlushPtr& flush) const;
  void end_flush(const FlushPtr& flush);

  // How every client sends a message: ONE way, round robin or through
  // `lane` (its connection while that lives; a lane not pinned takes the
  // one chosen when it has none or lost its own), or to ALL. Returns at
  // once: the message goes into a connection's queue, or its backlog when
  // the connection is full (schedule_on, schedule_one, schedule_all), or,
  // with no connection live or others already waiting for one, it is held
  // until one comes up and then placed in order. Every wait is bounded by
  // `timeout`, after which the message is dropped and reported. A
  // connection that dies hands what it had not written to another, or,
  // with none live, has it held the same way (handle_orphaned_outgoing_
  // messages). The trackers of what was placed or held go to `trackers`
  // when given: one per copy for ALL, which a flushing send follows until
  // the first is written. `done`, when given, hears the message's end,
  // once, on the owner thread, posted so that it may send: 1 once a copy
  // is written, the first for ALL, 0 once every copy was given up on or
  // the client shut down first. False, with nothing placed or reported and
  // `done` not called, when nothing may take it: the client is shutting
  // down, or a pinned lane's connection is gone; the caller says so, by an
  // exception or a report.
  typedef std::function<void(unsigned int written)> SendCallback;
  bool send(std::shared_ptr<const RawMessage> raw, bool all, const LanePtr& lane, float timeout,
            std::uint64_t number = 0, std::vector<BasicScheduledMessageTracker>* trackers = NULL,
            ConnectionWrapper* used = NULL, SendCallback done = SendCallback());
  // Ends every send still followed, each `done` hearing 0, at once: for a
  // client whose loop will not run again, once its shutdown has let what
  // was being written settle.
  void end_follows();
  // Lets go of every send still followed, calling no `done`: for a client
  // freed where nothing may call back, as release_drop_observer() is.
  void release_follows();
  // Drops the message held for a connection whose tracker is `state`, when
  // it is held, reported NO_CONNECTION as one out of time is: what a
  // flushing send that gives up with nothing connected or coming does, so
  // that its message does not go out after the send said NotConnected.
  void drop_held(const BasicScheduledMessageTracker& state);

  // Runs the loop until some connection is registered or the timer expires:
  // this is where a synchronous client's reconnect timers get to fire when
  // every connection is gone. True when a connection is available; false
  // at once when none can come, the loop having no work at all.
  bool wait_for_any_connection(mx::SimpleTimer& timer) {
    MX_DCHECK_RUN_ON(&owner_thread());
    while (connections_count(true) == 0 && !timer.expired()) {
      if (run_one() == 0) {
        break;
      }
    }
    return connections_count(true) != 0;
  }

  // Like wait_for_incoming_message, but also returns, with false, as soon as
  // `watch` is no longer a live connection: a caller waiting for a reply
  // through it learns at once that the reply cannot come that way.
  bool wait_for_incoming_message_or_loss(mx::SimpleTimer& timer, const ConnectionWrapper& watch) const {
    MX_DCHECK_RUN_ON(&owner_thread());
    while (!has_incoming_messages() && !timer.expired() && watch) {
      if (run_one() == 0) {
        return has_incoming_messages();  // no work at all: `watch` is gone, as nothing can arrive
      }
    }
    if (!has_incoming_messages() && watch) {
      poll();  // a last look, as in wait_for_incoming_message
    }
    if (has_incoming_messages()) {
      return true;
    }
    if (!watch) {
      return false;
    }
    MXTHROW(OperationTimedOut());
  }

  // Queues the frame on a preferred connection, the one a request arrived on
  // or a reply came from, while it lives; once it is gone, on another, as
  // any message is, waiting up to `timeout` for one to come back when none
  // is live, as long as it takes for a negative one, not at all for 0 or
  // NaN; with none by then, throws NotConnected. The message waits for
  // room `timeout` at most, as every schedule does. Never a connection of
  // its own to the old one's address: the client already reconnects to
  // that multiplexer under the target it was given, a host name perhaps,
  // and a second connection to it would replace the first and be replaced
  // in turn.
  BasicScheduledMessageTracker schedule_one(std::shared_ptr<const RawMessage> raw, ConnectionWrapper wrapper,
                                            float timeout) {
    if (wrapper.inherited()) {
      MXTHROW(UsedAfterFork());  // the parent's connection; see ConnectionWrapper
    }
    check_ours(wrapper);
    MX_DCHECK_RUN_ON(&owner_thread());
    Connection::pointer conn;
    if ((conn = wrapper.lock()) && conn->living()) {
      // A connection closed by the peer while the loop did not run still
      // reads as living here; Client polls the loop before calling this.
      return schedule_on(raw, wrapper, timeout);
    }
    BasicScheduledMessageTracker tracker = schedule_one(raw, NULL, timeout);
    if (!tracker && (timeout > 0 || timeout < 0)) {
      std::unique_ptr<mx::SimpleTimer> timer = create_timer(timeout);
      if (wait_for_any_connection(*timer)) {
        tracker = schedule_one(raw, NULL, timeout);
      }
    }
    if (!tracker) {
      MXTHROW(NotConnected());
    }
    return tracker;
  }

  // On `wrapper`'s connection and never another, as a lane does, waiting
  // there when it is full: null only when that connection is gone.
  BasicScheduledMessageTracker schedule_on(std::shared_ptr<const RawMessage> raw, const ConnectionWrapper& wrapper,
                                           float timeout = DEFAULT_TIMEOUT, std::uint64_t number = 0,
                                           LanePtr lane = LanePtr());

  // A connection that shuts down offers what it had not written here, even
  // nothing (the frame it was writing is not offered: its write says what
  // became of it): every message goes to one other live connection,
  // chosen once for `conn`, waiting there when it is full, and leaves the
  // buffer; after them goes what waited in the dead connection's backlog,
  // to the same connection, so that a stream's unwritten tail moves whole
  // and in order, and a lane that held `conn` follows it there. A message pinned to
  // its connection (RawMessage::pinned, a pinned lane's) is never handed
  // over, nor is a copy of an ALL send while another connection lives,
  // which has its own: left in the buffer, it is reported dropped here
  // (report_drop), its tracker reading LOST, which is what the pin means.
  // With no connection live the rest is held for the next one, an ALL
  // send once, whole, DEFAULT_TIMEOUT from then; at shutdown everything is
  // left.
  void handle_orphaned_outgoing_messages(Connection* conn, Connection::MessagesBuffer& outgoing_messages);

  mx::Random64::result_type random64() { return random_(); }         // a message id
  std::uint32_t inline client_type() const { return client_type_; }  // this peer's type

 private:
  typedef std::map<Target, Connection::weak_pointer> ConnectionByTarget;
  typedef std::map<TimerPointer, Target> ReconnectTimers;

  // The wrapper of a connection, from what it stores.
  static ConnectionWrapper _wrap(const Connection::pointer& conn) {
    return ConnectionWrapper(conn, conn->managers_private_data().target,
                             conn->managers_private_data().expected_endpoint, conn->managers_private_data().owner);
  }
  // The target async_connect() makes of an address: as asio writes it.
  static Target _target(const Endpoint& peer_endpoint) {
    return Target(peer_endpoint.address().to_string(), peer_endpoint.port());
  }
  // The target of `host` and `port`: an address in text as asio writes it,
  // a name as given.
  static Target _target(const std::string& host, std::uint16_t port);
  // disconnect(), once the target is known.
  bool _disconnect(const Target& target);
  // A new connection for `target`, in the map, replacing an earlier one.
  Connection::pointer _new_connection(const Target& target);
  // Resolves the connection's target, then connects; on the io thread.
  void _resolve_and_start(Connection::pointer conn);
  void _resolved(Connection::pointer conn, const asio::error_code& error, std::vector<Endpoint> candidates);
  // Connects to the next address the target resolved to, or gives up.
  void _try_next_candidate(Connection::pointer conn);
  void _connected(Connection::pointer conn, const asio::error_code& error);

  /* instance properties */
  std::uint32_t client_type_;
  bool shuts_down_;

  // received messages
  IncomingSink incoming_sink_;
  ConnectionObserver connection_observer_;
  Routing routing_;
  unsigned int routing_version_ = 0;  // bumped by set_routing; 0 is the default routing
  IncomingMessagesBuffer incoming_messages_;
  unsigned int incoming_queue_max_size_;
  std::uint64_t frames_handled_ = 0;  // every frame handle_message() was given, owner thread; see poll()
  LogSummary drop_lines_;
  DropObserver drop_observer_;
  std::atomic<std::uint64_t> dropped_{0};

  // The targets the client has: those with a connection, live or on its
  // way, and those with a reconnect armed, by a lost connection, which
  // shutdown() cancels, as disconnect() does its target's.
  ConnectionByTarget connection_by_target_;
  ReconnectTimers reconnect_timers_;
  std::vector<Connection::weak_pointer> closing_;  // closed by shutdown() or disconnect(), reading to their end
  std::uint64_t dropped_while_closing_ = 0;        // see dropped_while_closing()
  const unsigned int fork_generation_at_creation_;
  std::atomic<bool> orphan_descriptors_closed_{false};
  asio::ip::tcp::resolver resolver_;
  Resolver resolver_hook_;

  std::shared_ptr<const RawMessage> welcome_message_;

  // Ids of the last 2048 messages received, across all connections. An event
  // sent through every multiplexer arrives once per multiplexer; the copies
  // are dropped here so the caller sees each id once.
  mx::SpanSet<std::uint64_t, 2048> last_seen_message_ids_;

  // What the connections cannot take yet; see "The outbox" in outbox.cc.
  struct Outbox;
  std::unique_ptr<Outbox> outbox_;
  struct Waiting;
  typedef std::shared_ptr<Waiting> WaitingPtr;
  struct Backlog;
  Backlog* _backlog(const Connection* conn);
  Connection::pointer _choose_connection();
  Connection::pointer _successor(Connection* dead);
  Connection::pointer _handed_to(const ConnectionWrapper& gone);
  BasicScheduledMessageTracker _place_on(const Connection::pointer& conn, std::shared_ptr<const RawMessage> raw,
                                         BasicScheduledMessageTracker state, std::uint64_t number,
                                         std::chrono::steady_clock::time_point deadline, bool copy, LanePtr lane);
  void _queued(std::uint64_t number, const BasicScheduledMessageTracker& state, const Connection* conn);
  void _moved(const BasicScheduledMessageTracker& state, const ConnectionWrapper& conn);
  bool _place_now(const std::shared_ptr<const RawMessage>& raw, bool all, const LanePtr& lane, std::uint64_t number,
                  std::chrono::steady_clock::time_point deadline, BasicScheduledMessageTracker state,
                  std::vector<BasicScheduledMessageTracker>* trackers, ConnectionWrapper* used, bool* refused);
  WaitingPtr _hold(const std::shared_ptr<const RawMessage>& raw, BasicScheduledMessageTracker state,
                   std::uint64_t number, std::chrono::steady_clock::time_point deadline, bool all, const LanePtr& lane);
  BasicScheduledMessageTracker _give_up(const std::shared_ptr<const RawMessage>& raw,
                                        BasicScheduledMessageTracker state, std::uint64_t number, DropReason reason);
  bool _held_whole(const std::shared_ptr<const RawMessage>& raw) const;
  void _supersede(const BasicScheduledMessageTracker& state);
  void _place_held();
  void _rehold(const WaitingPtr& waiting, bool all);
  struct Follow;
  typedef std::shared_ptr<Follow> FollowPtr;
  void _follow(const std::vector<BasicScheduledMessageTracker>& copies, SendCallback done);
  void _follow_event(const std::shared_ptr<SendState>& state, bool written);
  void _process_follows();
  void _end_follow(const FollowPtr& follow, unsigned int written);
  void _lose(const WaitingPtr& waiting);
  void _displace(Connection* conn);
  void _replace_displaced(const Connection::pointer& conn);
  void _expire_at(const WaitingPtr& waiting);
  void _arm_expiry();
  void _expire();
  void _drop_expired(const WaitingPtr& waiting);
  static void _clear_out(std::deque<WaitingPtr>& queue, std::size_t live);
  void _wait_flushes(std::uint64_t number, int copies);
  void _check_flushes();
  void _drop_outbox();
};

};  // namespace multiplexer

#endif  // MX_MULTIPLEXER_BASIC_CLIENT_H_
