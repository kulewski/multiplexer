// The C++ client API: connect to multiplexers, send events, ask requests,
// receive messages. This is what peers use directly and what the Python
// binding (_native.cc) wraps; BaseMultiplexerServer builds backends on it.
// docs/api_cpp.md describes it from the caller's side.
//
// Client owns a BasicClient (the connections) and adds the query algorithm
// in _query, the piece of the protocol that makes requests reliable: send
// through one connection; on DELIVERY_ERROR or timeout, search every
// connection for a backend of the type (BACKEND_FOR_PACKET_SEARCH), take the
// first PING that answers, and repeat the request to that backend by
// instance id. docs/query.md draws it. A request with `to` set is an
// addressed query, _query_addressed: the same shape, except that the
// middle stage locates that one instance rather than any backend of the
// type, and the three stages share one timeout.
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
#include <utility>

#include "lib/logging/logging.h"
#include "lib/repr.h"
#include "lib/triple.h"
#include "lib/vector.h"
#include "multiplexer/basic_client.h"
#include "multiplexer/defaults.h"

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
  // written to the socket, or was lost with its connection. Null (false as
  // bool) when no connection took the message at all.
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
  Client(std::uint32_t client_type);
  Client(shared_ptr<asio::io_service> io_service, std::uint32_t client_type);
  Client(asio::io_service& io_service, std::uint32_t client_type);
  ~Client();  // shutdown(), on whichever thread destroys the client

  // Connectivity; see BasicClient for the semantics.
  void shutdown();
  // Whether this client was inherited across a fork, in which case every
  // call throws UsedAfterFork and the destructor only closes the child's
  // descriptor copies; see BasicClient::orphaned.
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
  ConnectionWrapper async_connect(const std::string& host, std::uint16_t port) {
    basic_client_->check_not_orphaned();
    return basic_client_->async_connect(host, port);
  }
  ConnectionWrapper connect(const std::string& host, std::uint16_t port, float timeout = DEFAULT_TIMEOUT) {
    basic_client_->check_not_orphaned();
    MX_LOG(INFO, MEDIUMVERBOSITY, CTX("multiplexer.client") TEXT("connecting to " + host + ":" + repr(port)));
    return basic_client_->connect(host, port, timeout);
  }
  // How host names become addresses; for tests. See BasicClient::Resolver.
  void set_resolver(BasicClient::Resolver resolver) { basic_client_->set_resolver(resolver); }

  unsigned int inline connections_count() { return basic_client_->connections_count(true); }  // live ones
  std::uint64_t inline instance_id() const { return basic_client_->instance_id(); }           // our `from`
  std::uint32_t inline client_type() const { return basic_client_->client_type(); }           // our peer type

  // Incoming messages: the next one in arrival order, waiting up to
  // `timeout` (negative: forever). Throws OperationTimedOut, NotConnected.
  BasicClient::IncomingMessagesBuffer::value_type read_raw_message(float timeout = DEFAULT_READ_TIMEOUT) {
    basic_client_->check_not_orphaned();
    std::unique_ptr<mx::SimpleTimer> timer = basic_client_->create_timer(timeout);
    return read_raw_message(*timer);
  }
  BasicClient::IncomingMessagesBuffer::value_type read_raw_message(mx::SimpleTimer& timer) {
    basic_client_->check_not_orphaned();
    basic_client_->wait_for_incoming_message(timer);
    Assert(basic_client_->has_incoming_messages());
    return basic_client_->next_incoming_message();
  }

  std::pair<shared_ptr<MultiplexerMessage>, ConnectionWrapper> receive_message(float timeout = DEFAULT_READ_TIMEOUT) {
    BasicClient::IncomingMessagesBuffer::value_type raw = read_raw_message(timeout);
    return std::make_pair(raw.third, raw.second);
  }

  // Sending. schedule_* only queue; the write happens while any call runs
  // the loop, so a caller that wants the message out before it goes idle
  // uses flush() or flush_all(). `msg` may be a MultiplexerMessage, an
  // already serialized std::string, or a RawMessage.
  // Each of these polls the loop first (BasicClient::poll), so that a
  // connection the multiplexer closed while this client sat idle is retired
  // rather than written into.
  template <typename T>
  ScheduledMessageTracker schedule_one(const T& msg) {
    basic_client_->check_not_orphaned();
    basic_client_->poll();
    return ScheduledMessageTracker(basic_client_->schedule_one(_serialize(msg)));
  }

  template <typename T>
  ScheduledMessageTracker schedule_one(const T& msg, ConnectionWrapper w, float timeout = DEFAULT_TIMEOUT) {
    basic_client_->check_not_orphaned();
    basic_client_->poll();
    return ScheduledMessageTracker(basic_client_->schedule_one(_serialize(msg), w, timeout));
  }

  // Runs the loop until the tracked message is written or lost, or the
  // timeout passes.
  void flush(ScheduledMessageTracker tracker, float timeout = DEFAULT_TIMEOUT) const {
    basic_client_->check_not_orphaned();

    std::unique_ptr<mx::SimpleTimer> timer = basic_client_->create_timer(timeout);
    return flush(tracker, *timer);
  }

  void flush(ScheduledMessageTracker tracker, mx::SimpleTimer& timer) const {
    std::size_t n;
    while (tracker && tracker.in_queue() && !timer.expired()) {
      n = basic_client_->run_one();
      Assert(n);
    }
  }

  // Runs the loop until every connection's queue is empty; false on timeout.
  bool flush_all(float timeout = DEFAULT_TIMEOUT) const {
    basic_client_->check_not_orphaned();

    std::unique_ptr<mx::SimpleTimer> timer = basic_client_->create_timer(timeout);
    std::size_t n;

    BasicClient::Connection::pointer conn;
    std::list<BasicClient::Connection::weak_pointer> current_connections;
    for (BasicClient::ConnectionById::const_iterator entry = basic_client_->begin(); entry != basic_client_->end();
         ++entry) {
      current_connections.push_back(entry->second);
    }
    std::list<BasicClient::Connection::weak_pointer>::iterator entry = current_connections.begin(),
                                                               connend = current_connections.end();

    while (entry != connend) {
      if (timer->expired()) {
        return false;
      }

      // find a Connection that has some outgoing messages
      while (entry != connend && (!(conn = entry->lock()) || conn->outgoing_queue_empty())) {
        ++entry;
      }

      if (entry != connend) {
        n = basic_client_->run_one();
        Assert(n);
      }
    }
    return true;
  }

  // Writes `msg` to one connection and returns it, waiting up to `timeout`:
  // a connection that dies under the write is replaced by another, or by
  // the same one once reconnected, so a multiplexer restart between two
  // calls costs the reconnect delay, not the message. Throws NotConnected
  // when no connection exists by the deadline, OperationTimedOut otherwise.
  // With a lane, through the lane's connection, which takes the connection
  // used when it has none or lost its own; a pinned lane whose connection
  // is gone throws NotConnected.
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
  // `to` set is addressed and its stages share one, see _query. With a
  // lane, the request goes through the lane's connection and the lane
  // adopts the connection the reply came through; `probe` is how an
  // addressed query locates its addressee.
  IncomingMessage query(const MultiplexerMessage& mxmsg, float timeout = DEFAULT_TIMEOUT, LanePtr lane = LanePtr(),
                        Probe probe = PROBE_SEARCH) {
    basic_client_->check_not_orphaned();
    return _query(mxmsg, timeout, lane, probe);
  }
  // Through `connection` while it is live, another when it is gone; a
  // pinned Lane seeded with the connection is the form that refuses any
  // other.
  IncomingMessage query(const MultiplexerMessage& mxmsg, const ConnectionWrapper& connection,
                        float timeout = DEFAULT_TIMEOUT, Probe probe = PROBE_SEARCH) {
    return query(mxmsg, timeout, std::make_shared<Lane>(connection), probe);
  }

  IncomingMessage query(shared_ptr<const MultiplexerMessage> mxmsg, float timeout = DEFAULT_TIMEOUT,
                        LanePtr lane = LanePtr(), Probe probe = PROBE_SEARCH) {
    basic_client_->check_not_orphaned();
    return _query(*mxmsg, timeout, lane, probe);
  }

  IncomingMessage query(const std::string& message, std::uint32_t type, float timeout = DEFAULT_TIMEOUT,
                        LanePtr lane = LanePtr()) {
    basic_client_->check_not_orphaned();

    MultiplexerMessage mxmsg;
    mxmsg.set_id(random64());
    mxmsg.set_from(instance_id());
    mxmsg.set_type(type);
    mxmsg.set_message(message);
    return _query(mxmsg, timeout, lane, PROBE_SEARCH);
  }

  // Queues `msg` on every live connection; returns how many took it.
  template <typename T>
  unsigned int schedule_all(T& msg) {
    basic_client_->poll();
    return basic_client_->schedule_all(_serialize(msg));
  }

  template <typename T>
  unsigned int schedule_all(const T& msg) {
    basic_client_->check_not_orphaned();
    basic_client_->poll();
    return basic_client_->schedule_all(_serialize(msg));
  }

  mx::Random64::result_type random64() const { return basic_client_->random64(); }  // a message id

 protected:
  // The query algorithm and the send-and-receive it is built on live in
  // client.cc; see the comments there.
  IncomingMessage _query(const MultiplexerMessage& query, float timeout, LanePtr lane, Probe probe);
  IncomingMessage _query_addressed(const MultiplexerMessage& query, float timeout, LanePtr lane, Probe probe);
  IncomingMessage _send_and_receive(const MultiplexerMessage& mxmsg, mx::SimpleTimer& timer, bool schedule_all = false,
                                    bool handle_delivery_errors = false, std::uint64_t accept_id = 0,
                                    std::uint32_t ignore_type = 0, std::uint64_t ignore_id = -1,
                                    ConnectionWrapper connection = ConnectionWrapper(), LanePtr lane = LanePtr());
  IncomingMessage _send_and_receive_one(MultiplexerMessage mxmsg, mx::SimpleTimer& timer,
                                        std::vector<uint64_t> accept_ids, std::uint32_t ignore_type,
                                        std::uint64_t ignore_id, ConnectionWrapper connection, LanePtr lane);
  ConnectionWrapper _send_one(const MultiplexerMessage& mxmsg, mx::SimpleTimer& timer, ConnectionWrapper preferred,
                              LanePtr lane = LanePtr());
  MultiplexerMessage _probe_for(const MultiplexerMessage& query, Probe probe);
  IncomingMessage _receive(mx::SimpleTimer& timer, const std::vector<uint64_t>& accept_ids, uint32_t ignore_type,
                           uint64_t ignore_id, const ConnectionWrapper* watch = NULL, bool* lost = NULL);

  /**
   * _serialize
   */
  shared_ptr<const RawMessage> _serialize(const MultiplexerMessage& msg) {
    return shared_ptr<const RawMessage>(RawMessage::FromMessage(msg));
  }
  shared_ptr<const RawMessage> _serialize(std::string* serialized) {
    return shared_ptr<const RawMessage>(new RawMessage(serialized));
  }
  shared_ptr<const RawMessage> _serialize(shared_ptr<const RawMessage> raw) { return raw; }

 private:
  shared_ptr<asio::io_service> io_service_ptr_;

 protected:
  asio::io_service& io_service_;
  shared_ptr<BasicClient> basic_client_;
};

};  // namespace multiplexer

#endif  // MX_MULTIPLEXER_CLIENT_H_
