// The client side of the protocol: connections to one or more multiplexers,
// an incoming queue, and reconnection. Used by Client (client.h), which adds
// the query algorithm and the calls peers actually make, and by
// ThreadedClient (threaded_client.h), which runs one on an io thread of its
// own; a peer uses one of those, never a BasicClient directly.
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
#include <limits>
#include <map>
#include <memory>
#include <set>
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
#include "multiplexer/descriptor_table.h"
#include "multiplexer/io/connection.h"
#include "multiplexer/log_summary.h"
#include "multiplexer/spent_bodies.h"

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

// The most the stamp serialize_stamped() appends can take: an id and a
// sender at their widest, measured. A body reserved with room for it takes
// a later message of the same size whatever its id, which a body reused
// for the next message (SpentBodies) needs, a random id's varint being 9
// or 10 bytes.
inline std::size_t widest_stamp_bytes() {
  static const std::size_t bytes = [] {
    MultiplexerMessage stamp;
    stamp.set_id(std::numeric_limits<std::uint64_t>::max());
    stamp.set_sender(std::numeric_limits<std::uint64_t>::max());
    return stamp.ByteSizeLong();
  }();
  return bytes;
}

// `msg` serialized for a send, as every send that takes a whole message
// serializes it, into `out`, which keeps its capacity: as it is when it has
// an id and a sender, else with an empty id made fresh by `fresh_id` and an
// empty sender set to `instance_id`, as new_message() and a reply fill
// them, since every receiver drops a message without an id. What it lacks
// is appended to its bytes rather than set in a copy of it: protobuf reads
// serializations put end to end as one message, the later fields' values
// winning, so the message is serialized once and its payload never copied
// in between. `room_after`: room taken at once for what the caller appends
// after the message. Returns the message's id. Throws std::length_error,
// before any room is taken, for a key over MAX_KEY_SIZE or a message over
// MAX_MESSAGE_SIZE, its size measured, and for one protobuf will not
// serialize.
template <typename FreshId>
std::uint64_t serialize_stamped(const MultiplexerMessage& msg, std::uint64_t instance_id, FreshId fresh_id,
                                std::string* out, std::size_t room_after = 0) {
  check_key_size(msg.key().size());
  MultiplexerMessage stamp;
  if (!msg.id()) {
    stamp.set_id(fresh_id());
  }
  if (!msg.sender()) {
    stamp.set_sender(instance_id);
  }
  const std::size_t message_bytes = msg.ByteSizeLong();
  check_message_size(message_bytes + stamp.ByteSizeLong());  // before any room is taken for it
  out->clear();
  out->reserve(message_bytes + widest_stamp_bytes() + room_after);
  if (!msg.AppendToString(out) || !stamp.AppendPartialToString(out)) {
    throw std::length_error("a message protobuf could not serialize, of " + std::to_string(message_bytes) + " bytes");
  }
  return msg.id() ? msg.id() : stamp.id();
}

// `msg` framed for a send (serialize_stamped). For a client with a
// delivery set (BasicClient::set_delivery), `delivery`, the frame is marked
// as its delivery needs it (RawMessage::batchable), and the frame of a
// plain event, which may go in a batch, is made unsealed
// (RawMessage::Unsealed), its checksum computed only if it goes alone; a
// client with none reads nothing more of the message for it.
template <typename FreshId>
std::shared_ptr<const RawMessage> frame_stamped(const MultiplexerMessage& msg, std::uint64_t instance_id,
                                                FreshId fresh_id, bool delivery = false) {
  const bool plain = delivery && RawMessage::plain_event(msg);
  std::string serialized;
  const std::uint64_t id = serialize_stamped(msg, instance_id, fresh_id, &serialized);
  std::shared_ptr<const RawMessage> raw =
      plain ? std::make_shared<RawMessage>(RawMessage::Unsealed(), &serialized, id, msg.type())
            : std::make_shared<RawMessage>(&serialized, id, msg.type());
  if (delivery) {
    raw->mark_batchable(plain ? RawMessage::Batchable::YES : RawMessage::Batchable::NO, plain && msg.has_key(),
                        plain ? msg.key() : std::string());
  }
  return raw;
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
  NOT_CONFIRMED,    // a confirmed send (Delivery::confirm): no consumer confirmed it by its batch's deadline
  FAILED,           // a confirmed send: the consumer's handler raised on it
};

// What became of a message taken from a confirmed batch, as its consumer's
// library tells its producer (BasicClient::confirm, Confirmation in
// Multiplexer.proto).
enum class Confirmed : unsigned char {
  PROCESSED,  // the program is done with it
  REFUSED,    // not processed: the consumer is leaving or closing; its producer sends it again
  FAILED,     // the handler raised on it; its producer gives up on it
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
  // A batch that goes as soon as a connection has nothing left to write
  // waits for the connection to say so (BasicClient::outgoing_queue_idle).
  static constexpr bool REPORTS_IDLE = true;
  // What a client owes the producers of the confirmed batches it took goes
  // once what one read brought is handed over (BasicClient::read_drained).
  static constexpr bool REPORTS_READ_DRAINED = true;
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
    // The TCP connect to expected_endpoint is under way, and its deadline,
    // CONNECT_ATTEMPT_SECONDS, which cancels it: past it the connect is a
    // failure, `connect_timed_out` says why (see _try_next_candidate). The
    // deadline's handler acts for its own attempt, `connect_attempt`, alone.
    bool connecting = false;
    bool connect_timed_out = false;
    std::uint64_t connect_attempt = 0;
    std::shared_ptr<asio::steady_timer> connect_deadline;
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
  // gone. For a host name, in the wrapper connect() returns once connected;
  // the one async_connect() returns for a name, made before the name
  // resolved, keeps it unspecified.
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

  // What arrived: the raw frame, the connection it came on and the message.
  // A message taken from a batch is queued without its raw frame, null,
  // which with_raw_frame() makes for whatever reads it.
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
      welcome_message_ = create_welcome_message(client_type_, routing_, key_filters_message_);
    }
    return welcome_message_;
  }

  // Called once a change of the routing or the key filters is
  // acknowledged (routing_acknowledged()), that change or a later one: see
  // set_routing() for where and when.
  typedef std::function<void()> AcknowledgedCallback;

  // Which of a multiplexer's routing paths reach this peer
  // (Multiplexer.proto's Routing; everything by default): told to every
  // registered connection with PEER_CONTROL now, carried in the welcome
  // of every connection made or remade from now on, and sent after the
  // handshake on a connection whose welcome was already out. A backend
  // draining sets `any` and `all` off. On the owner thread.
  // `acknowledged`, when given, is called once this change, or a later
  // one, is acknowledged: on the owner thread, in the handler that
  // completed it for a client whose messages go to a sink (ThreadedClient),
  // which has handed everything routed before to the sink by then;
  // otherwise from inside a receive (wait_to_receive()), when the caller
  // came back for more after everything read before the acknowledgement.
  // Never after shutdown(). What it throws is logged.
  void set_routing(const Routing& routing, AcknowledgedCallback acknowledged = AcknowledgedCallback());
  const Routing& routing() const { return routing_; }
  // Which keys of each message type reach this peer by the rules
  // (Multiplexer.proto's KeyFilter): per message type, RE2 patterns of
  // which one must match a message's key whole. A type not named takes
  // every key, one with no pattern none, and a message without a key
  // arrives whatever its type.
  // Replaces the filters set before, and goes with the routing: told to
  // every registered connection with PEER_CONTROL now, carried in the
  // welcome of every connection made or remade from now on, so set before
  // connecting to have it from the first message. The routing set by
  // set_routing() stays, as these stay when the routing changes. Throws
  // std::invalid_argument for a protocol message type (99 and below) or a
  // pattern over MAX_KEY_PATTERN_SIZE bytes; a pattern that does not
  // compile is refused by the multiplexer, which logs why and keeps the
  // filters it had, or closes a connection whose welcome carries it. On
  // the owner thread.
  // `acknowledged` as for set_routing().
  typedef std::map<std::uint32_t, std::vector<std::string>> KeyFilters;
  void set_key_filters(const KeyFilters& filters, AcknowledgedCallback acknowledged = AcknowledgedCallback());
  // The checks set_key_filters() makes before taking `filters`, for a
  // class that hands them to another thread: throws as it does.
  static void check_key_filters(const KeyFilters& filters);
  const KeyFilters& key_filters() const { return key_filters_; }
  // Whether every connection has the current routing and key filters in
  // effect: the multiplexer confirmed them, by its welcome for those the
  // connection's welcome carried or by PEER_STATUS for a change; a
  // handshake still in flight counts as not confirmed. Then nothing
  // routed to this peer by a path turned off since, or under a filter
  // replaced since, is on its way from those multiplexers, except as a
  // last resort; what this client read before the last confirmation and
  // has not handed out yet still is, so that counts as not confirmed too,
  // until it is handed out. A change a multiplexer refused stays
  // unconfirmed.
  bool routing_acknowledged() const;
  // The acknowledged changes' callbacks, run now, on the owner thread, when
  // routing_acknowledged() holds; nothing otherwise. What wait_to_receive()
  // calls for the synchronous client.
  void run_acknowledged_callbacks();
  // Whether a change was acknowledged, or the last message held for a
  // connection went, since the last call, which clears it: what ends a
  // serve loop's wait early (wait_to_receive()), each a reason for a drain
  // to look again.
  bool take_acknowledged_wake() {
    MX_DCHECK_RUN_ON(&owner_thread());
    const bool woken = acknowledged_wake_;
    acknowledged_wake_ = false;
    return woken;
  }

  // Called by a Connection with every parsed message: drops ones without an
  // id or seen before, then queues or hands to the sink; a batch, split
  // into its messages (_split_batch), each as one that came alone, and a
  // DELIVERY_ERROR for a batch into one for each message inside
  // (_split_delivery_error).
  void handle_message(const Connection::pointer& conn, const std::shared_ptr<const RawMessage>& raw,
                      const std::shared_ptr<MultiplexerMessage>& mxmsg);

  // Connectivity. async_connect returns at once; connect runs the loop until
  // the handshake completed or `timeout` passed, and returns the wrapper
  // either way (check it). A host name is resolved on this client's thread,
  // asynchronously, and every address it resolves to is tried in turn; a
  // name that does not resolve counts as an attempt that failed. A
  // connection that fails or drops is retried from connection_destroyed
  // after AUTO_RECONNECT_TIME, whenever the loop runs, resolving the name
  // again each time, so a multiplexer that moved is found at the next try,
  // until disconnect() drops the target. A target that has a connection,
  // live or on its way, keeps it: connecting to it again returns that one.
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
  // How many messages wait for a connection to come up: sent while none
  // was live, or left by one that died with none to take them, each going
  // out in order once one is registered, or given up on at its own
  // deadline. What the server classes' drains wait for, a close dropping
  // them; the last one to go, placed or given up on, ends a serve loop's
  // wait as an acknowledgement does (take_acknowledged_wake()).
  std::size_t held_for_a_connection() const;
  // How many reconnects are armed, a timer each: none once shutdown()
  // returned, which cancels them and takes them out. For tests.
  std::size_t reconnects_pending() const {
    MX_DCHECK_RUN_ON(&owner_thread());
    return reconnect_timers_.size();
  }
  // The messages the client's connections read after they began closing
  // the polite way, in shutdown() or disconnect(), and dropped: a request
  // among them gets no answer, its sender waits out its timeout. A
  // connection whose write failed delivers what it reads on. Every one is
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
  // connection it made that still exists: live, connecting, reading to its
  // end, or shut down with handlers still to run. For a client built on
  // one thread and driven from another; BaseMultiplexerServer::
  // serve_forever() and ~Client call it. Only the debug-build thread
  // checks care.
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
  // PEER_CONTROL with the current routing and key filters on `conn`.
  void _send_routing(Connection::pointer conn);
  // Whether every living connection is registered and has confirmed the
  // current routing.
  bool _all_confirmed() const;
  // After whatever can confirm the routing or take that back: a change, a
  // PEER_STATUS, a welcome, a connection gone. Notes where the incoming
  // queue stood when every connection confirmed, and tells of an
  // acknowledgement (_acknowledged()) once per change.
  void _update_confirmation();
  // A change acknowledged: the serve loop's wake, and a sink client's
  // callbacks at once.
  void _acknowledged();
  // A change that became acknowledged when a receive took the last message
  // read before every connection confirmed it, rather than at a
  // confirmation, which _update_confirmation() tells of: told of the same
  // way, once. What wait_to_receive() calls before it waits, nothing being
  // queued, so that nothing is added per message.
  void _acknowledged_by_receive();
  // The routing or the key filters changed: a new version, a new welcome,
  // and PEER_CONTROL on every connection.
  void _routing_changed();
  // A PEER_STATUS from a multiplexer: the routing and key filters it now
  // applies to us.
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
  // A tracker for the frame queued last on `conn` but its own frames
  // (Connection::last_queued), which everything queued there before it is
  // written ahead of; null when the queue holds none other. What a caller
  // waiting until everything queued so far is out follows.
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
  // ConnectionsManager hook, ahead of the base class's: a welcome on a
  // connection of one target from the multiplexer another target's last
  // welcome came from is one multiplexer reached through two targets, two
  // spellings of one address or a balancer in front of several, whose
  // connections then replace or refuse each other at every reconnect.
  // Said once for the two, at WARNING, so that the list can be fixed; the
  // base class then decides as for any peer.
  void register_connection(Connection::pointer conn, const WelcomeMessage& welcome);
  void after_connection_registration(Connection::pointer conn, const WelcomeMessage&) {
    MX_DCHECK_RUN_ON(&owner_thread());
    // The multiplexer's welcome confirms the routing ours carried; a
    // change made while the handshake was under way goes out now.
    auto& data = conn->managers_private_data();
    data.routing_acknowledged = data.routing_in_welcome;
    if (data.routing_in_welcome != routing_version_) {
      _send_routing(conn);
    }
    _update_confirmation();
    first_connection_over_.insert(data.target);
    // The queue took one until now: what waited on this connection for
    // room moves in under the peer type's size, ahead of what waited for
    // any connection, as no room event comes but for a queue that was full.
    outgoing_queue_has_room(conn.get());
    _place_held();                    // what waited for a connection goes out now, in order
    _close_idle_batches(conn.get());  // and the batches that waited for one with nothing to write, on it
    if (connection_observer_) {
      connection_observer_(_wrap(conn), true);
    }
  }

  inline bool has_incoming_messages() const { return !incoming_messages_.empty(); }
  IncomingMessagesBuffer::value_type next_incoming_message();  // pop the oldest; only when has_incoming_messages()
  // The raw frame of a message taken from a batch, made when something
  // reads it: such a message is queued without one (_split_batch), and
  // every call that hands raw frames out makes it, Client's
  // read_raw_message() and receive_raw_message(), Python's reads and the
  // split itself for a sink, of the message serialized again, for a
  // message one of the libraries made the bytes it was sent as. Nothing
  // for a message that has its frame. Any thread that holds `incoming`.
  static void with_raw_frame(IncomingMessagesBuffer::value_type* incoming);
  // The most bytes a batch that arrived may leave in the strings it is
  // read into, kept for the next (_split_batch): after a larger one they
  // go, so that one huge batch does not hold its memory for good.
  static constexpr std::size_t KEPT_SPLIT_BYTES = 4 * 1024 * 1024;
  inline bool incoming_queue_full() const { return incoming_queue_max_size_ <= incoming_messages_.size(); }
  // Queues `incoming`, the message handle_message() was given last, for a
  // receive to take, as it does when no sink is set: a full queue drops
  // it, and takes its copy through another multiplexer once there is room.
  // For a sink that passes a message by to the queue's reader, a query of
  // a leaving server's (Client::refuse_arrivals).
  void queue_incoming(IncomingMessagesBuffer::value_type incoming);
  // A query of a leaving server's sets what was read before aside, so that
  // it reads only what the refusals pass by to it, and puts it back as it
  // ends, in its order (Client::_awaiting): what was read stays to be
  // received, as refuse_arrivals() promised, where the query took it off
  // the queue and dropped it. The answers nobody took then go, as the
  // refusals drop a reply.
  void set_incoming_aside();
  void put_incoming_back();

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
    SENT_NOT_CONFIRMED,    // DropReason::NOT_CONFIRMED
    SENT_FAILED,           // DropReason::FAILED
    BATCH_NOT_SPLIT,       // a batch that arrived and does not parse, or a message in one that does not belong
    OWN_LINES = 1000,      // and up: a message callback's own kinds (ThreadedClient::drop_lines)
  };
  LogSummary& drop_lines() { return drop_lines_; }

  // Every message the program sent that this client gives up on, a copy
  // for ALL each, is counted and logged in drop_lines() at once, and told
  // to the drop observer with its id and why, on the owner thread, in the
  // one pass per loop turn that runs the sends' callbacks too, ahead of
  // them, outside the handlers and the outbox's bookkeeping the drop came
  // from: an observer may send, or shut the client down. A flush ends
  // after the drops it saw were told. dropped() is the count so far, from
  // any thread. report_drop() is the one place every drop goes through,
  // the ThreadedClient's own included.
  typedef std::function<void(std::uint64_t message_id, DropReason reason)> DropObserver;
  void set_drop_observer(DropObserver observer) {
    MX_DCHECK_RUN_ON(&owner_thread());
    drop_observer_ = observer;
  }
  std::uint64_t dropped() const { return dropped_.load(std::memory_order_relaxed); }
  // How many messages that arrived were dropped for a full incoming
  // queue (incoming_queue_full()), which a summarized WARNING only says;
  // from any thread.
  std::uint64_t incoming_dropped() const { return incoming_dropped_.load(std::memory_order_relaxed); }
  void report_drop(const std::shared_ptr<const RawMessage>& raw, DropReason reason);
  // Lets go of the observer, from the thread that ends the client: a
  // ThreadedClient's io thread at its end, or a forked child's teardown.
  void release_drop_observer() { drop_observer_ = DropObserver(); }

  // The consumer's side of confirmed sends (basic_client_confirm.cc): a
  // message taken from a batch whose producer asked for confirmation
  // carries its number (MultiplexerMessage.confirmation, with `sender`),
  // and confirm() tells that producer what became of it, gathered with the
  // rest this client owes it into one CONFIRMATION, which goes once this
  // client has nothing more in hand for the program (after a batch handed
  // to a sink, before a receive waits, at the end of a ThreadedClient's
  // inbox turn), at CONFIRMATIONS_GATHERED_MAX owed, or at shutdown, on
  // the connection the producer's messages came on, else another live
  // one. Nothing for a message with no number. The server classes and a
  // ThreadedClient confirm what their handlers returned from (PROCESSED)
  // or raised from (FAILED); a program that reads messages itself
  // confirms each with processed(). On the owner thread.
  void confirm(std::uint64_t sender, std::uint64_t number, Confirmed outcome);
  void confirm(const MultiplexerMessage& message, Confirmed outcome) {
    if (message.has_confirmation()) {
      confirm(message.sender(), message.confirmation(), outcome);
    }
  }
  void processed(const MultiplexerMessage& message) { confirm(message, Confirmed::PROCESSED); }
  // Every confirmation owed, sent now.
  void send_confirmations();
  // How many messages this client sent wait for their confirmation, how
  // many it sent again, how many confirmations it owes, and whether the
  // timer of what waits is armed: for tests.
  std::size_t unconfirmed() const;
  std::uint64_t sent_again() const;
  std::size_t confirmations_owed() const;
  bool confirm_timer_armed() const;

  // A receive's wait: runs the loop until a message is queued, true, or
  // `timer` expires, false, running the callbacks of the changes
  // acknowledged meanwhile (run_acknowledged_callbacks()), and, with
  // `end_on_acknowledgement`, false too as soon as a change is acknowledged
  // or the last message held for a connection goes
  // (take_acknowledged_wake()), what a serve loop acts on at once rather
  // than at its next poll. Throws NotConnected when nothing could arrive, for a
  // timer with no deadline; a finite one's wait runs its course then.
  bool wait_to_receive(mx::SimpleTimer& timer, bool end_on_acknowledgement);

  // Runs the loop until a message is queued or `timeout` seconds pass;
  // throws OperationTimedOut, or, with no deadline, NotConnected when
  // nothing could arrive.
  void inline wait_for_incoming_message(float timeout = -1) {
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
  void inline wait_for_incoming_message(mx::SimpleTimer& timer) {
    MX_DCHECK_RUN_ON(&owner_thread());
    if (!has_incoming_messages()) {
      send_confirmations();  // the program has nothing more in hand
    }
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
  // Whether `wrapper`'s connection is live, as schedule_on() decides it:
  // what a message placed on it now would need. On the client's thread.
  bool live(const ConnectionWrapper& wrapper) const;
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
  // one chosen when it has none or lost its own), or to ALL: every live
  // connection, and each still on its target's first way, whose copy the
  // welcome lets in (see _place_now in outbox.cc). Returns at
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
  // exception or a report. `may_batch`: a send nobody waits for, which goes
  // by the delivery set for its type (set_delivery below), `timeout` not
  // used when one is. `poll_first`: a synchronous
  // client's send, which runs the loop first (poll()) when the message is
  // to choose a connection, so that one the peer closed meanwhile is
  // retired first, or when something is due: always, but for a message
  // that only joins a batch with a delay, which chooses none, while that
  // batch has room and no batch's delay is over (_batch_needs_loop).
  typedef std::function<void(unsigned int written)> SendCallback;
  bool send(std::shared_ptr<const RawMessage> raw, bool all, const LanePtr& lane, float timeout,
            std::uint64_t number = 0, std::vector<BasicScheduledMessageTracker>* trackers = NULL,
            ConnectionWrapper* used = NULL, SendCallback done = SendCallback(), bool may_batch = false,
            bool poll_first = false);
  // send() with `may_batch`, through no lane and not to ALL, for a message
  // given as its bytes alone, `body`, swapped out and left empty, with its
  // id and type: what a client that batches hands over without making a
  // frame of it (basic_client_plain.cc). A message for a batch goes there
  // as the bytes it is, with no frame of its own; any other is framed and
  // sent as send() sends it. send_plain() takes a plain event
  // (RawMessage::plain_event) the client serialized itself, its key `key`
  // when `keyed`; send_bytes() takes bytes the program gave, read when
  // their type is batched, as a frame of them would be, and sent alone
  // when they are not a plain event of that type.
  bool send_plain(std::string* body, std::uint64_t id, std::uint32_t type, bool keyed, const std::string& key,
                  float timeout, std::vector<BasicScheduledMessageTracker>* trackers = NULL,
                  const SendCallback& done = SendCallback(), bool poll_first = false);
  bool send_bytes(std::string* body, std::uint64_t id, std::uint32_t type, float timeout,
                  std::vector<BasicScheduledMessageTracker>* trackers = NULL, const SendCallback& done = SendCallback(),
                  bool poll_first = false);
  // Where the bodies of a batch's messages go once its frame is made, as
  // far as it keeps them (SpentBodies), for the client to serialize its
  // next messages into rather than freed here: a SyncClient's queue()
  // takes them, a ThreadedClient's io thread hands them back to the threads
  // that sent them (threaded_client_inbox.h). Null, the default: freed.
  // Shared, as this client may outlive the one that set it.
  void keep_spent_bodies(std::shared_ptr<SpentBodies> spent);
  // Delivery (basic_client_batching.cc, basic_client_confirm.cc,
  // docs/delivery.md): how the events of a type, or of one key of a type,
  // that the program sends without waiting for them go: one by one or, with
  // `batch`, several in one message, which the receiving library splits
  // back into one message each, as if each had come alone; and, with
  // `confirm`, each kept until its consumer confirms it. `may_batch` above
  // marks those sends: queue(), the Python send_message() that does not
  // wait, without flush or with a callback, ThreadedClient's sends without
  // a timeout, through no lane and not to ALL. A message goes by a setting
  // when it is a plain event (RawMessage::plain_event) of a type or key set
  // here, a key's setting ahead of its type's. On the owner thread.
  //
  // `timeout` is a batch's, its send's own not used: a batch has that many
  // seconds from its first message to be written, and with `confirm` to be
  // confirmed, and what of it is not by then is given up on together. A
  // message that goes alone is a batch of one, and a batch sent again keeps
  // its deadline. A negative one sets no deadline. Without `batch` or
  // `confirm` the messages go as any send does, by `timeout`: a key's
  // setting that sends it so while its type is batched.
  //
  // With `batch`, every message inside a batch has the batch's type and
  // key, the keys of a type each batched apart. A batch goes when it holds
  // `max_records` messages, when one more would take its message past
  // `max_bytes`, as it goes on the wire, or `max_delay` seconds after its
  // first message; with `max_delay` 0, when a connection has nothing left
  // to write, on it, so that a message sent while one has goes at once,
  // alone, and those sent while every connection is busy go together as
  // soon as one is not, or at their deadline. A batch that goes on its own
  // takes those waiting for a connection with nothing left to write with
  // it. A send that does not batch, a flush, a shutdown and a change of
  // what is set first let every batch go, in the order they began: nothing
  // overtakes a message sent before it. A batch is one message for the
  // rest of the library, its connection, its wait for room and its
  // deadline, its loss told for each message inside (report_drop); each
  // message keeps its own tracker and `done`.
  //
  // Confirmed sends (basic_client_confirm.cc, docs/delivery.md "Confirmed
  // sends"): with `confirm`, every batch goes as an envelope that numbers
  // its messages, a batch of one included, and without `batch` each
  // message goes at once in one of its own; each message is kept until the
  // consumer's library confirms it: its tracker reads SENT and its `done`
  // hears 1 then, not when it is written, and a flush waits for it. It goes
  // again, keeping its number, when its consumer refused it, when nobody
  // took its batch (after a back-off from 0.1 s to 1 s), when the
  // connection it went on ended, and when nothing confirmed any message of
  // its batch for `retry_after` seconds; it is given up on, NOT_CONFIRMED,
  // at its batch's deadline, and FAILED when its consumer's handler raised.
  // Delivery is at least once: a message whose confirmation was lost is
  // processed again. What is kept has no cap: a stall of every consumer
  // grows this client by its messages until their deadline, and forever
  // with a negative timeout.
  struct Delivery {
    bool batch = false;
    unsigned int max_records = DEFAULT_BATCH_MAX_RECORDS;
    std::size_t max_bytes = DEFAULT_BATCH_MAX_BYTES;
    float max_delay = 0;  // seconds
    bool confirm = false;
    float retry_after = DEFAULT_RETRY_AFTER;  // seconds, with `confirm`
    float timeout = DEFAULT_TIMEOUT;          // seconds, a batch's
  };
  // For every key of `type`, each key's messages in batches of their own;
  // for one key, ahead of what is set for its type. Throws
  // std::invalid_argument for a protocol message type (99 and below), a
  // key over MAX_KEY_SIZE, no record or more than MAX_BATCH_RECORDS in a
  // batch, no byte or more than MAX_MESSAGE_SIZE, a delay negative, over
  // MAX_SECONDS, infinite or NaN, with `confirm` a retry_after not over 0
  // or over MAX_SECONDS, and a timeout NaN, or 0 or more but not longer
  // than the delay (than 0 without `batch`), which would leave a batch no
  // time (check_delivery()), as the Rust crate refuses them.
  void set_delivery(std::uint32_t type, const Delivery& delivery);
  void set_delivery(std::uint32_t type, const std::string& key, const Delivery& delivery);
  // What set_delivery() set for `type`, its keys' included, or for one key
  // of it: their messages go as any send from now on.
  void clear_delivery(std::uint32_t type);
  void clear_delivery(std::uint32_t type, const std::string& key);
  static void check_delivery(std::uint32_t type, const std::string* key, const Delivery& delivery);
  // Every batch goes now, in the order they began; what a flush and a send
  // that does not batch do first.
  void close_batches();
  // Called by a connection that was asked to (Connection::notify_when_idle)
  // when it has nothing left to write: the batches waiting for that go.
  void outgoing_queue_idle(Connection* conn);
  // Called by a connection that handed over every whole frame it had read
  // and reads its socket again (REPORTS_READ_DRAINED): the confirmations a
  // client whose handlers run as messages arrive (set_incoming_sink) owes
  // go, gathered over what one read brought rather than sent per batch.
  void read_drained(Connection* conn);
  // How many messages wait in batches, and whether the batches' timer is
  // armed: for tests.
  std::size_t batched_messages() const;
  bool batch_timer_armed() const;
  // Whether a delivery is set for some type or key: a client frames its
  // messages for it then (frame_stamped).
  bool has_delivery() const;

  // Ends every send still followed, each `done` hearing 0, at once, after
  // the drop observer heard of every drop reported before: for a client
  // whose loop will not run again, once its shutdown has let what was
  // being written settle.
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
  bool wait_for_incoming_message_or_loss(mx::SimpleTimer& timer, const ConnectionWrapper& watch) {
    MX_DCHECK_RUN_ON(&owner_thread());
    if (!has_incoming_messages()) {
      send_confirmations();  // the program has nothing more in hand
    }
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
  // send once, whole, each within the deadline its send gave it
  // (RawMessage::deadline), one whose time is up dropped and reported at
  // once; at shutdown everything is left.
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
  // The connection `target` has, live or on its way, that nobody shut
  // down; null when it has none.
  Connection::pointer _live_connection(const Target& target) const;
  // A new connection for `target`, in the map, closing one it had that is
  // shutting down already.
  Connection::pointer _new_connection(const Target& target);
  // Resolves the connection's target, then connects; on the io thread.
  void _resolve_and_start(Connection::pointer conn);
  void _resolved(Connection::pointer conn, const asio::error_code& error, std::vector<Endpoint> candidates);
  // Connects to the next address the target resolved to, or gives up.
  void _try_next_candidate(Connection::pointer conn);
  // Closes the connection's socket, if open, gone from descriptors_ first.
  void _close_socket(Connection& conn);
  void _connected(Connection::pointer conn, const asio::error_code& error);

  /* instance properties */
  std::uint32_t client_type_;
  bool shuts_down_;

  // received messages
  IncomingSink incoming_sink_;
  ConnectionObserver connection_observer_;
  Routing routing_;
  KeyFilters key_filters_;
  google::protobuf::RepeatedPtrField<KeyFilter> key_filters_message_;  // key_filters_ as the protocol has them
  unsigned int routing_version_ = 0;  // bumped by set_routing and set_key_filters; 0 is the default
  // The callbacks of the changes not acknowledged yet, all run together
  // once the latest is: an acknowledgement of a change is one of every
  // change before it.
  std::vector<AcknowledgedCallback> acknowledged_callbacks_;
  // Messages through the incoming queue, counted in and out, and
  // enqueued_ when every connection last came to confirm the current
  // routing: what was read before that may have been routed under the
  // routing before, and is handed out once dequeued_ reaches it.
  std::uint64_t enqueued_ = 0;
  std::uint64_t dequeued_ = 0;
  bool confirmed_ = false;
  std::uint64_t confirmed_enqueued_ = 0;
  unsigned int acknowledged_version_ = 0;  // the last version told of (_acknowledged)
  bool acknowledged_wake_ = false;         // see take_acknowledged_wake()
  IncomingMessagesBuffer incoming_messages_;
  IncomingMessagesBuffer incoming_aside_;  // see set_incoming_aside()
  // The strings a batch that arrived is read into, kept with their
  // capacity for the next batch, owner thread; see _split_batch.
  MessageBatch split_scratch_;
  unsigned int incoming_queue_max_size_;
  std::uint64_t frames_handled_ = 0;  // every frame handle_message() was given, owner thread; see poll()
  LogSummary drop_lines_;
  DropObserver drop_observer_;
  std::atomic<std::uint64_t> dropped_{0};
  std::atomic<std::uint64_t> incoming_dropped_{0};  // see incoming_dropped()

  // The targets the client has: those with a connection, live or on its
  // way, and those with a reconnect armed, by a lost connection, which
  // shutdown() cancels, as disconnect() does its target's.
  ConnectionByTarget connection_by_target_;
  ReconnectTimers reconnect_timers_;
  // The targets whose first connection is over: it came up, or it ended
  // before it did. A send to ALL gives a copy to the connection of every
  // other target on the way (_place_now), which the welcome lets in, so
  // that one made just after the start reaches every multiplexer, not
  // only those whose handshakes ended first. disconnect() forgets a
  // target, and a later connect() is a first connection again.
  std::set<Target> first_connection_over_;
  // The multiplexer id each target's last welcome came from, and the pairs
  // of targets found to reach one multiplexer, said once each
  // (register_connection).
  std::map<Target, std::uint64_t> reached_by_target_;
  std::set<std::pair<Target, Target>> one_multiplexer_said_;
  std::vector<Connection::weak_pointer> closing_;  // reading to their end; see connection_destroyed()
  // Every connection made that still exists: one shut down lives on until
  // its last handler ran, and bind_to_current_thread() takes it along.
  // Those gone are dropped as new ones are made.
  std::vector<Connection::weak_pointer> made_;
  std::uint64_t dropped_while_closing_ = 0;  // see dropped_while_closing()
  const unsigned int fork_generation_at_creation_;
  std::atomic<bool> orphan_descriptors_closed_{false};
  DescriptorTable descriptors_;  // every socket open, for a forked child to close
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
  // What is batched, and the batches open; see basic_client_batching.cc.
  struct Batcher;
  std::unique_ptr<Batcher> batcher_;
  struct Batch;
  typedef std::shared_ptr<Batch> BatchPtr;
  struct BatchRecord;  // one message in a batch, a frame or its bytes alone; batching.h
  // Confirmed sends: the batches written whose messages wait for their
  // confirmation, and the confirmations this client owes; confirming.h,
  // basic_client_confirm.cc.
  struct Confirmer;
  std::unique_ptr<Confirmer> confirmer_;
  struct Confirming;
  typedef std::shared_ptr<Confirming> ConfirmingPtr;
  struct OwedConfirmations;
  struct Segment;
  void _numbered(std::vector<BatchRecord>& records, std::size_t begin, std::size_t end, BatchEnvelope* envelope);
  void _wait_for_confirmation(const BatchPtr& batch, std::size_t begin, std::size_t end,
                              const std::shared_ptr<const RawMessage>& frame, const LanePtr& lane);
  void _confirmed_end(std::uint64_t id, std::uint64_t number, std::uint32_t type, std::shared_ptr<SendState>* state,
                      SendCallback* done, bool processed, DropReason reason, bool report);
  void _confirming_written(const ConfirmingPtr& confirming, unsigned int written);
  void _confirmed_frame_dropped(std::uint64_t envelope_id, DropReason reason);
  void _cannot_confirm(const BatchPtr& batch, std::size_t begin, std::size_t end);
  void _on_confirmation(const MultiplexerMessage& mxmsg);
  std::vector<Segment> _segments(const google::protobuf::RepeatedField<std::uint64_t>& pairs) const;
  bool _confirmed_batch_not_taken(std::uint64_t envelope_id);
  bool _resolved(const ConfirmingPtr& confirming, std::size_t index, bool processed, DropReason reason,
                 bool report = true);
  void _resolve(const ConfirmingPtr& confirming, std::size_t index, bool processed, DropReason reason,
                bool report = true);
  void _rebuild_runs(const ConfirmingPtr& confirming);
  void _forget_resolved(const ConfirmingPtr& confirming);
  void _give_up_confirmations(DropReason reason);
  void _send_again(const ConfirmingPtr& confirming, const std::vector<std::size_t>& indexes);
  void _send_unresolved_again(const ConfirmingPtr& confirming);
  void _confirming_due(const ConfirmingPtr& confirming);
  void _arm_confirm_timer();
  void _disarm_confirm_timer_when_idle();
  void _confirm_timer_fired();
  void _confirmations_on_closed(Connection* conn);
  static std::vector<ConfirmingPtr> _in_number_order(std::vector<ConfirmingPtr> batches);
  bool _confirming_up_to(std::uint64_t number) const;
  std::uint64_t _confirm_mark() const;
  bool _confirmed_ends_due() const;
  void _report_record_drop(std::uint64_t id, std::uint32_t type, DropReason reason);
  void _run_confirmed_ends();
  void _release_confirmed_ends();
  void _confirmed_batch_from(const Connection::pointer& conn, std::uint64_t producer);
  void _send_owed(std::uint64_t producer, OwedConfirmations& owed);
  const Delivery* _delivery_for(const std::shared_ptr<const RawMessage>& raw);
  const Delivery* _delivery_for(std::uint32_t type, bool keyed, const std::string& key) const;
  bool _batch_needs_loop(std::uint32_t type, bool keyed, const std::string& key, std::size_t bytes,
                         const Delivery& delivery) const;
  bool _batch(const std::shared_ptr<const RawMessage>& raw, const Delivery& delivery,
              std::vector<BasicScheduledMessageTracker>* trackers, const SendCallback& done);
  bool _batch_record(BatchRecord* record, std::uint32_t type, bool keyed, const std::string& key, std::size_t bytes,
                     const Delivery& delivery, std::vector<BasicScheduledMessageTracker>* trackers,
                     const SendCallback& done);
  static std::shared_ptr<const RawMessage> _record_frame(BatchRecord* record, std::uint32_t type);
  static void _give_back(BatchRecord* record, std::string* body);
  std::shared_ptr<const RawMessage> _batch_frame(const BatchEnvelope& envelope);
  std::shared_ptr<SpentBodies> spent_bodies_;  // see keep_spent_bodies()
  void _close_batch(const BatchPtr& batch, Connection* idle = nullptr);
  void _close_on_its_own(const BatchPtr& batch);
  void _due_at(const BatchPtr& batch, std::chrono::steady_clock::time_point due);
  void _send_batch(const BatchPtr& batch, Connection* idle);
  bool _idle_or_none();
  void _wait_for_idle();
  void _close_idle_batches(Connection* idle = nullptr);
  void _arm_batch_timer();
  void _batch_timer_fired();
  // A batch that arrived, split into its messages, each handed out as one
  // that came alone; see handle_message().
  void _split_batch(const Connection::pointer& conn, const MultiplexerMessage& envelope);
  // A DELIVERY_ERROR for a batch, told for each message inside; see
  // basic_client.cc.
  bool _split_delivery_error(const Connection::pointer& conn, const MultiplexerMessage& error);
  void _enqueue(IncomingMessagesBuffer::value_type incoming);
  // send() but for batching.
  bool _send(const std::shared_ptr<const RawMessage>& raw, bool all, const LanePtr& lane,
             std::chrono::steady_clock::time_point deadline, std::uint64_t number,
             std::vector<BasicScheduledMessageTracker>* trackers, ConnectionWrapper* used, const SendCallback& done);
  // When a message given `timeout` seconds from `now` stops waiting; see
  // outbox.cc.
  static std::chrono::steady_clock::time_point _deadline_after(
      float timeout, std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now());
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
  void _held_left();
  void _rehold(const WaitingPtr& waiting, bool all);
  struct Follow;
  typedef std::shared_ptr<Follow> FollowPtr;
  void _follow(const std::vector<BasicScheduledMessageTracker>& copies, SendCallback done);
  void _follow_event(const std::shared_ptr<SendState>& state, bool written);
  void _post_callbacks();
  void _process_callbacks();
  void _tell_drops();
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
