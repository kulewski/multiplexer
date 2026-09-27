// The synchronous C++ API, Client, which the docs call SyncClient (the
// alias at the end): connect to multiplexers, send events, ask requests,
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

  // Which of a multiplexer's routing paths reach this peer, and whether
  // every multiplexer has it in effect; see BasicClient::set_routing. The
  // control message goes out and the answers arrive as the loop runs,
  // inside the calls that run it.
  void set_routing(const Routing& routing) {
    basic_client_->check_not_orphaned();
    basic_client_->set_routing(routing);
  }
  const Routing& routing() const { return basic_client_->routing(); }
  bool routing_acknowledged() const {
    basic_client_->check_not_orphaned();  // the parent's state in a forked child: raise, as ThreadedClient's
    return basic_client_->routing_acknowledged();
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
  // loop. What was read before stays to be received, a number fixed now,
  // however fast more comes. The refusal is queued as a reply is, on the
  // request's connection while that lives.
  void refuse_arrivals();
  // The messages read and not received yet, refused or dropped by the same
  // rule: for a server that will not handle them, at its close.
  void refuse_unread();

  unsigned int inline connections_count() {  // live ones
    basic_client_->check_not_orphaned();
    return basic_client_->connections_count(true);
  }
  std::uint64_t inline instance_id() const { return basic_client_->instance_id(); }  // our `from`
  // See BasicClient::dropped_while_closing().
  std::uint64_t dropped_while_closing() const { return basic_client_->dropped_while_closing(); }
  std::uint32_t inline client_type() const { return basic_client_->client_type(); }  // our peer type

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

  // Sending. schedule_* queue the message; on an idle connection it is
  // written inside the call, otherwise while a later call runs the loop, so
  // a caller that wants every message out before it goes idle uses flush()
  // or flush_all(). A message a full connection cannot take
  // waits for its room, in order, `timeout` seconds at most, and goes in
  // as a later call runs the loop (see BasicClient). They hold nothing: a
  // null tracker from schedule_one(msg) means no connection is live, and
  // schedule_one(msg, wrapper) waits up to `timeout` for one instead and
  // throws NotConnected; queue() is the send that holds the message. `msg`
  // may be a MultiplexerMessage, an already serialized std::string, or a
  // RawMessage.
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
  // timeout passes.
  void flush(ScheduledMessageTracker tracker, float timeout = DEFAULT_TIMEOUT) const {
    basic_client_->check_not_orphaned();

    std::unique_ptr<mx::SimpleTimer> timer = basic_client_->create_timer(timeout);
    return flush(tracker, *timer);
  }

  void flush(ScheduledMessageTracker tracker, mx::SimpleTimer& timer) const {
    basic_client_->check_not_orphaned();
    std::size_t n;
    while (tracker && tracker.in_queue() && !timer.expired()) {
      n = basic_client_->run_one();
      Assert(n);
    }
  }

  // Runs the loop until everything sent before the call is written or
  // given up on, what still waits for room included, or `timeout` passes;
  // true when every one of them was written, false when one was given up
  // on (the drop observer says which) or the time ran out. See
  // BasicClient::begin_flush().
  bool flush_all(float timeout = DEFAULT_TIMEOUT) const {
    basic_client_->check_not_orphaned();
    std::unique_ptr<mx::SimpleTimer> timer = basic_client_->create_timer(timeout);
    BasicClient::FlushPtr flush = basic_client_->begin_flush();
    while (!basic_client_->flushed(flush) && !timer->expired()) {
      basic_client_->run_one();
    }
    basic_client_->end_flush(flush);
    return basic_client_->all_written(flush);
  }

  // The SyncClient's form of ThreadedClient::send(msg): returns at once,
  // the message queued on one live connection, round robin or `lane`'s, or
  // held until one comes up, `timeout` seconds at most, and written as the
  // loop runs; a message the client gives up on is reported
  // (set_drop_observer). queue_all() gives every live connection a copy.
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
  // `to` set is addressed and its stages share one, see _query. With a
  // lane, the request goes through the lane's connection and the lane
  // adopts the connection the reply came through; `probe` is how an
  // addressed query locates its addressee. `received`, when given, is told
  // here, on the caller's thread while the query waits (so it must not
  // read through this client), the instance id of each backend that
  // acknowledges an attempt with REQUEST_RECEIVED (notify_start()): once,
  // normally, or again when a retry reached a backend, the same or
  // another; nothing about the query changes for it.
  IncomingMessage query(const MultiplexerMessage& mxmsg, float timeout = DEFAULT_TIMEOUT, LanePtr lane = LanePtr(),
                        Probe probe = PROBE_SEARCH, ReceivedCallback received = ReceivedCallback()) {
    basic_client_->check_not_orphaned();
    return _query(mxmsg, timeout, lane, probe, received);
  }
  // Through `connection` while it is live, another when it is gone; a
  // pinned Lane seeded with the connection is the form that refuses any
  // other.
  IncomingMessage query(const MultiplexerMessage& mxmsg, const ConnectionWrapper& connection,
                        float timeout = DEFAULT_TIMEOUT, Probe probe = PROBE_SEARCH,
                        ReceivedCallback received = ReceivedCallback()) {
    return query(mxmsg, timeout, std::make_shared<Lane>(connection), probe, received);
  }

  IncomingMessage query(shared_ptr<const MultiplexerMessage> mxmsg, float timeout = DEFAULT_TIMEOUT,
                        LanePtr lane = LanePtr(), Probe probe = PROBE_SEARCH,
                        ReceivedCallback received = ReceivedCallback()) {
    basic_client_->check_not_orphaned();
    return _query(*mxmsg, timeout, lane, probe, received);
  }

  IncomingMessage query(const std::string& message, std::uint32_t type, float timeout = DEFAULT_TIMEOUT,
                        LanePtr lane = LanePtr(), ReceivedCallback received = ReceivedCallback()) {
    basic_client_->check_not_orphaned();

    MultiplexerMessage mxmsg;
    mxmsg.set_id(random64());
    mxmsg.set_from(instance_id());
    mxmsg.set_type(type);
    mxmsg.set_message(message);
    return _query(mxmsg, timeout, lane, PROBE_SEARCH, received);
  }

  // Queues `msg` on every live connection, a full one's copy waiting for
  // its room as above; returns how many connections got a copy.
  template <typename T>
  unsigned int schedule_all(const T& msg, float timeout = DEFAULT_TIMEOUT) {
    basic_client_->check_not_orphaned();
    basic_client_->poll();
    return basic_client_->schedule_all(_serialize(msg), NULL, timeout);
  }

  mx::Random64::result_type random64() const { return basic_client_->random64(); }  // a message id

  // Every message the program sent that the client gives up on, each copy
  // of one sent to ALL, is told to `observer` with its id and why
  // (DropReason), inside whichever call runs the loop when it happens;
  // dropped() counts them. See BasicClient::report_drop.
  void set_drop_observer(BasicClient::DropObserver observer) { basic_client_->set_drop_observer(observer); }
  std::uint64_t dropped() const { return basic_client_->dropped(); }

 protected:
  // The query algorithm and the send-and-receive it is built on live in
  // client.cc; see the comments there.
  IncomingMessage _query(const MultiplexerMessage& query, float timeout, LanePtr lane, Probe probe,
                         ReceivedCallback received);
  IncomingMessage _query_addressed(const MultiplexerMessage& query, float timeout, LanePtr lane, Probe probe);
  IncomingMessage _send_and_receive(const MultiplexerMessage& mxmsg, mx::SimpleTimer& timer, bool schedule_all = false,
                                    bool handle_delivery_errors = false,
                                    const std::vector<uint64_t>& also_accept = std::vector<uint64_t>(),
                                    std::uint32_t ignore_type = 0, std::uint64_t ignore_id = -1,
                                    ConnectionWrapper connection = ConnectionWrapper(), LanePtr lane = LanePtr(),
                                    std::vector<uint64_t>* sent_ids = NULL);
  IncomingMessage _send_and_receive_one(MultiplexerMessage mxmsg, mx::SimpleTimer& timer,
                                        std::vector<uint64_t> accept_ids, std::uint32_t ignore_type,
                                        std::uint64_t ignore_id, ConnectionWrapper connection, LanePtr lane,
                                        std::vector<uint64_t>* sent_ids = NULL);
  ConnectionWrapper _send_one(const MultiplexerMessage& mxmsg, mx::SimpleTimer& timer, ConnectionWrapper preferred,
                              LanePtr lane = LanePtr());
  ConnectionWrapper _send_one(std::shared_ptr<const RawMessage> raw, mx::SimpleTimer& timer,
                              ConnectionWrapper preferred, LanePtr lane);
  // The flushing send's core, both languages': BasicClient::send, then the
  // loop runs until a copy is written, the first for ALL, none is left that
  // could be, or `timer` expires. Returns the copies written, 0 or 1;
  // `used` gets the connection that wrote it; `taken` is false when
  // nothing took the message (BasicClient::send), `lost` when every copy
  // was given up on before the time ran out.
  unsigned int _send_and_wait(std::shared_ptr<const RawMessage> raw, bool all, const LanePtr& lane,
                              mx::SimpleTimer& timer, ConnectionWrapper* used, bool* taken, bool* lost);
  // queue() and queue_all(), and the binding's non-flushing send.
  ScheduledMessageTracker _queue(std::shared_ptr<const RawMessage> raw, bool all, const LanePtr& lane, float timeout,
                                 const SendCallback& done) {
    basic_client_->check_not_orphaned();
    basic_client_->poll();
    std::vector<BasicScheduledMessageTracker> trackers;
    if (!basic_client_->send(raw, all, lane, timeout, 0, &trackers, NULL, done) || trackers.empty()) {
      return ScheduledMessageTracker(BasicScheduledMessageTracker());
    }
    return ScheduledMessageTracker(trackers.front());
  }
  // What a flushing send that wrote nothing throws, as the Python
  // ThreadedClient raises: NotConnected when the message was given up on, when no
  // connection is live or `lane` is a pinned one whose connection is gone,
  // OperationTimedOut otherwise.
  [[noreturn]] void _raise_for_nothing_written(const LanePtr& lane, bool lost);
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
  ReceivedCallback received_;  // the on_received of the query under way, see _query

 protected:
  asio::io_service& io_service_;
  shared_ptr<BasicClient> basic_client_;
};

// SyncClient is this class's name in the docs after 2.3.1, so that no class
// is named like the client role. Client stays the class itself: what a
// forward declaration names and what compiler messages show.
using SyncClient = Client;

};  // namespace multiplexer

#endif  // MX_MULTIPLEXER_CLIENT_H_
