// One TCP connection between a peer and a multiplexer, seen from either side.
//
// Connection<Manager> owns the socket, the outgoing queue and the two
// heartbeat timers, and does all the asynchronous I/O: it reads frames
// (RawMessage) and hands parsed messages to its manager, and it writes queued
// frames one at a time. The same template serves the multiplexer (Server) and
// the client library (BasicClient); the Manager decides what to do with a
// message and what a queue entry looks like, through ConnectionsManagerTraits.
//
// Lifecycle: Create() -> start() or start_only_read() + start_rest() -> ...
// -> shutdown(), after which the object only waits for its pending handlers
// to return; close_gracefully(), or a failed write, first reads on to the
// peer's end of the stream, a bounded time, and close_when_flushed() first
// writes what is queued. Every asynchronous handler holds
// a shared_ptr to the connection (via shared_from_this), so a Connection is
// destroyed only when no I/O is in flight; the manager keeps weak_ptrs.
// Nothing here is thread-safe: one io_service, one thread.
//
// Protocol pieces that live here: the CONNECTION_WELCOME handshake, the
// heartbeat timers with the passive-peer exemption, and the rule that any
// malformed frame or out-of-order message closes the connection.
#ifndef MX_MULTIPLEXER_IO_CONNECTION_H_
#define MX_MULTIPLEXER_IO_CONNECTION_H_

#include <google/protobuf/message.h>

#include <algorithm>
#include <asio/io_service.hpp>
#include <asio/ip/tcp.hpp>
#include <asio/post.hpp>
#include <asio/read.hpp>
#include <asio/steady_timer.hpp>
#include <asio/streambuf.hpp>
#include <asio/write.hpp>
#include <atomic>
#include <chrono>
#include <cstring>
#include <deque>
#include <exception>
#include <limits>
#include <list>
#include <memory>
#include <string>

#include "lib/functors.h"
#include "lib/logging/logging.h"
#include "lib/repr.h"
#include "lib/seconds.h"
#include "lib/thread_checker.h"
#include "multiplexer/Multiplexer.pb.h" /* generated */
#include "multiplexer/defaults.h"
#include "multiplexer/io/raw_message.h"
#include "multiplexer/multiplexer.constants.h" /* generated */

namespace multiplexer {

using mx::repr;

// Specialized per manager (Server, BasicClient) in the manager's own header,
// which includes this one; declared here so that this header stands alone.
template <typename ConnectionsManagerImplementation>
struct ConnectionsManagerTraits;

// The completion condition of a read or a write of a whole frame: every
// step asks for all that is left, where asio's own, transfer_all, stops
// each at 64 KiB, a frame of 1 MiB sixteen system calls each way.
struct TransferRest {
  std::size_t operator()(const asio::error_code& error, std::size_t) const {
    return error ? 0 : std::numeric_limits<std::size_t>::max();
  }
};

// The connection itself; see the file comment. `ConnectionsManagerImplementation`
// is Server or BasicClient, and must provide: instance_id(),
// get_welcome_message(), register_connection(), after_connection_registration(),
// unregister_connection(), connection_destroyed(), connection_closed(),
// handle_message() and handle_orphaned_outgoing_messages(). See
// connections_manager.h for the shared implementation of most of them.
template <class ConnectionsManagerImplementation>
class Connection : public std::enable_shared_from_this<Connection<ConnectionsManagerImplementation>> {
 public:
  typedef multiplexer::ConnectionsManagerTraits<ConnectionsManagerImplementation> ConnectionsManagerTraits;
  typedef typename ConnectionsManagerTraits::MessagesBufferTraits MessagesBufferTraits;
  typedef std::deque<typename MessagesBufferTraits::value_type> MessagesBuffer;

  // Each direction of the socket is a channel with its own state. Outgoing:
  // FREE (nothing being written), BUSY (one async_write in flight). Incoming:
  // FREE, READING (into the read buffer), READING_BODY (the rest of a frame
  // too big for it, into its own). BROKEN, in either, means shutdown() ran
  // and any handler that still fires must return without touching the
  // socket.
  struct ChannelState {
    enum _ChannelState { FREE, BUSY, BROKEN, READING, READING_BODY };
  };
  typedef typename ChannelState::_ChannelState ChannelStateT;

  /* instance members */
 private:
  Connection(asio::io_service& io_service, std::shared_ptr<ConnectionsManagerImplementation> manager)
      : socket_(io_service),
        peer_type_(0),
        peer_id_(0),
        is_passive_(false),
        is_living_(false),
        living_flag_(std::make_shared<std::atomic<bool>>(false)),
        shuts_down_(false),
        is_registered_(false),
        told_gone_(false),
        delivering_(true),
        close_when_flushed_(false),
        close_bound_(0),
        dropped_while_closing_(0),
        manager_(manager),
        should_send_heartbit_(true),
        outgoing_channel_state_(ChannelState::FREE),
        outgoing_queue_max_size_(1),
        incoming_channel_state_(ChannelState::FREE),
        send_heartbit_timer_(io_service),
        require_heartbit_timer_(io_service)
  //, send_heartbit_timer_(io_service,
  // std::chrono::microseconds(HEARTBIT_INTERVAL * 1000000)) ,
  // require_heartbit_timer_(io_service,
  // std::chrono::microseconds(NO_HEARTBIT_SO_DROP_INTERVAL * 1000000))
  {
    MX_DCHECK_RUN_ON(&io_thread_);
    // The heartbeat frame never changes, so it is serialized once per
    // connection and re-queued by pointer; id 0 marks it as not a real message.
    MultiplexerMessage mxmsg;
    mxmsg.set_id(0);
    mxmsg.set_type(types::HEARTBIT);
    mxmsg.set_sender(manager->instance_id());
    heartbit_message_.reset(RawMessage::FromMessage(mxmsg));
    heartbit_message_->mark_own();
  }

 public:
  typedef std::shared_ptr<Connection> pointer;
  typedef std::weak_ptr<Connection> weak_pointer;
  typedef std::shared_ptr<ConnectionsManagerImplementation> ManagerPointer;

  /**
   * Create
   *  A factory function that ensures that every instance is managed by a
   * shared_ptr, so that having a shared_ptr is always equal to having living
   * instance.
   */
  static pointer Create(asio::io_service& io_service, std::shared_ptr<ConnectionsManagerImplementation> manager) {
    pointer created(new Connection(io_service, manager));
    MX_LOG(DEBUG, HIGHVERBOSITY, TEXT("created new Connection " + repr((void*)created.get())));
    return created;
  }

  ~Connection() {
    MX_LOG(DEBUG, HIGHVERBOSITY, TEXT("destroying Connection " + repr((void*)this)));
    if (!shuts_down_) {
      shutdown();
    }
    // A write whose handler was destroyed unrun, its io_service gone: what
    // became of the frame is unknown, and it reads lost rather than queued.
    if (!set_aside_.empty()) {
      _give_up(set_aside_, set_aside_manager_);
    }
  }

  asio::ip::tcp::socket& socket() { return socket_; }

 public:
  // A client starts reading and sends its welcome at once. The multiplexer
  // calls the two halves separately: it reads only, until the peer's welcome
  // has been accepted (see Server::after_connection_registration), and only
  // then sends its own welcome and arms the heartbeats.
  void start() {
    start_only_read();
    start_rest();
  }

  void start_only_read() {
    MX_DCHECK_RUN_ON(&io_thread_);
    MX_LOG(DEBUG, HIGHVERBOSITY, TEXT("starting Connection " + repr((void*)this)));
    Assert(!is_living_);
    Assert(!shuts_down_);
    _set_living(true);
    // Every write is one whole frame, so Nagle's algorithm has nothing to
    // coalesce and only delays a small frame sent right after another one
    // until the peer's ACK arrives. Off on both sides.
    asio::error_code ignored;
    socket_.set_option(asio::ip::tcp::no_delay(true), ignored);
    // No deadline for the welcome: a synchronous client sends it only when
    // a call of its runs the loop, which may be long after its connect
    // completed, and a connection dropped meanwhile would cost it that
    // multiplexer until a later call reconnects.
    _start_read();
  }

  void start_rest() {
    MX_DCHECK_RUN_ON(&io_thread_);
    if (shuts_down_) {
      return;
    }
    Assert(is_living_);
    ManagerPointer manager = manager_.lock();
    std::shared_ptr<const RawMessage> welcome = manager ? manager->get_welcome_message() : nullptr;
    if (welcome) {
      welcome->mark_own();
    }
    if (welcome && schedule(welcome, true, true)) {
      _send_heartbit_later();  // included in schedule() >> _process_send_queue()
      _require_heartbit_later();
    } else {
      shutdown();
    }
  }

  // Set by the multiplexer once it knows the peer's type, and again by a
  // rules reload. A passive peer runs no loop between calls, so it is
  // neither expected to send heartbeats nor sent more than one heartbeat
  // per message it delivers (see _send_heartbit_later); cancelling the
  // require timer is what exempts it. A peer turned active is required to
  // send from then on and sent a heartbeat every HEARTBIT_INTERVAL of
  // silence, the send timer turned on if it was off; one turned passive
  // is sent the heartbeat it is owed, if any, and then none. The same flag
  // again changes nothing: a rules reload sets it on every connected peer,
  // and re-arming the require timer each time would restart the drop of a
  // peer gone silent, which reloads less than its 30 + 60 s apart would
  // keep connected for good. start_rest() arms both timers.
  inline void set_is_passive(bool is_passive) {
    MX_DCHECK_RUN_ON(&io_thread_);
    if (is_passive == is_passive_) {
      return;
    }
    is_passive_ = is_passive;
    _require_heartbit_later();
    if (heartbit_timer_off_) {
      _send_heartbit_later();
    }
  }

  // Which routing paths reach this peer (Multiplexer.proto's Routing): set
  // by the multiplexer from the peer's welcome and its PEER_CONTROL, and
  // consulted when it routes by a rule; a message with `to` always
  // arrives. A client's connections to multiplexers keep the defaults.
  inline const Routing& routing() const { return routing_; }
  inline void set_routing(const Routing& routing) {
    MX_DCHECK_RUN_ON(&io_thread_);
    routing_ = routing;
  }
  inline bool accepts_any() const { return routing_.any(); }
  inline bool accepts_all() const { return routing_.all(); }
  inline bool last_resort() const { return routing_.last_resort(); }

  // The peer's key filters (Multiplexer.proto's KeyFilter), as its welcome
  // or its last PEER_CONTROL declared them, and the number they are held
  // under in the multiplexer's index (KeyPatterns::Owner in
  // multiplexer/key_filters.h), which routing asks whether the peer takes
  // a key: set by the multiplexer from the peer's registration on. A
  // client's connections keep none.
  inline const google::protobuf::RepeatedPtrField<KeyFilter>& key_filters() const { return key_filters_; }
  inline std::uint32_t key_owner() const { return key_owner_; }
  inline void set_key_filters(const google::protobuf::RepeatedPtrField<KeyFilter>& filters, std::uint32_t owner) {
    MX_DCHECK_RUN_ON(&io_thread_);
    key_filters_ = filters;
    key_owner_ = owner;
  }

  // Stops all I/O, tells the manager, and drops or hands back the outgoing
  // queue. Safe to call at any point of the lifecycle, including on a socket
  // that was never opened, and idempotent. Pending handlers see BROKEN. A
  // connection whose write failed did the telling and the handing back
  // already, and reads on until the peer's end first (_write_failed). The
  // manager hears last, through connection_closed(), that the connection
  // has ended for good.
  void shutdown() {
    MX_DCHECK_RUN_ON(&io_thread_);
    MX_LOG(DEBUG, HIGHVERBOSITY, TEXT("shutdown called on " + repr((void*)this)));
    if (shuts_down_) {
      MX_LOG(ERROR, HIGHVERBOSITY, TEXT("shutdown called twice on " + repr((void*)this)));
      return;
    }

    // Mark as dead before the manager hears of it: what its callbacks do
    // may reach this connection again, as a client's resend of a query
    // through the lane that still holds it does, and living() must say no
    // by then, so that schedule() drops the frame and another connection
    // takes it, rather than the assertion below throwing out of the
    // callback and leaving the shutdown, and the reconnect, undone.
    shuts_down_ = true;
    _set_living(false);
    _tell_manager_gone();
    is_registered_ = false;

    // stop doing I/O
    outgoing_queue_max_size_ = 0;
    incoming_channel_state_ = ChannelState::BROKEN;
    outgoing_channel_state_ = ChannelState::BROKEN;
    // The socket may never have been opened (a connection created for an
    // accept that was cancelled) or may already be closed, so none of these
    // may throw. Cancelling the timers lets the io loop drain after shutdown.
    asio::error_code ignored;
    socket_.cancel(ignored);
    socket_.shutdown(asio::ip::tcp::socket::shutdown_both, ignored);
    send_heartbit_timer_.cancel(ignored);
    require_heartbit_timer_.cancel(ignored);

    // cancel outgoing messages
    _orphan_outgoing_messages();

    if (ManagerPointer manager = manager_.lock()) {
      manager->connection_closed(this);
    }
    // die and let live or somehow ;)
    manager_.reset();
  }

 private:
  // Live or not, for this thread (is_living_) and for the others (living_flag_).
  void _set_living(bool living) {
    is_living_ = living;
    living_flag_->store(living, std::memory_order_release);
  }

  // Tells the manager, once, that the connection is gone: unregistered if it
  // was registered, then destroyed. What its callbacks do is theirs: an
  // exception out of one is logged, and the rest of the shutdown, the
  // socket, the timers and the queue, happens all the same.
  void _tell_manager_gone() {
    MX_DCHECK_RUN_ON(&io_thread_);
    if (told_gone_) {
      return;
    }
    told_gone_ = true;
    ManagerPointer manager = manager_.lock();
    try {
      if (is_registered_ && manager) {
        manager->unregister_connection(this);
      }
      if (manager) {
        manager->connection_destroyed(this);
      }
    } catch (const std::exception& error) {
      MX_LOG(ERROR, LOWVERBOSITY,
             TEXT("the manager's callback raised on the shutdown of " + repr((void*)this) + ": " + error.what()));
    }
  }

  // A write failed: the peer is gone or going, and nothing more can be
  // written. What it sent before may still wait in the socket, readable
  // after a reset, so the connection reads on until the peer's end,
  // HEARTBIT_INTERVAL at most, and hands it to the manager before it
  // shuts down: the last messages of a peer that closed while a message to
  // it was on its way are not lost to that write. A connection whose
  // handshake is not done has nothing to read on for and shuts down at
  // once.
  void _write_failed() {
    MX_DCHECK_RUN_ON(&io_thread_);
    if (shuts_down_ || told_gone_) {
      return;
    }
    if (!is_registered_) {
      shutdown();
      return;
    }
    _read_to_the_end(HEARTBIT_INTERVAL, /*deliver=*/true);
  }

  // Stops writing and reads on until the peer's end of the stream, `bound`
  // seconds at most, then shuts down. The manager is told the connection
  // is gone and its queue handed back, as by shutdown(), and this end of
  // the stream goes out after what the kernel still holds. What is read
  // meanwhile goes to the manager when `deliver`, or is dropped.
  void _read_to_the_end(float bound, bool deliver) {
    MX_DCHECK_RUN_ON(&io_thread_);
    // Dead to the manager before it hears of it, as in shutdown().
    _set_living(false);
    outgoing_channel_state_ = ChannelState::BROKEN;
    delivering_ = deliver;
    _tell_manager_gone();
    _orphan_outgoing_messages();
    asio::error_code ignored;
    socket_.shutdown(asio::ip::tcp::socket::shutdown_send, ignored);
    require_wait_ = 0;  // a frame read on arms it afresh
    require_heartbit_timer_.cancel(ignored);
    send_wait_ = 0;  // the send timer bounds the reading now
    _do_later(send_heartbit_timer_, bound, [self = this->shared_from_this()](const asio::error_code& error) {
      if (error != asio::error::operation_aborted && !self->shuts_down_) {
        self->shutdown();  // the peer's end never came
      }
    });
  }

 public:
  // Ends the connection the polite way, for a peer that is leaving: the
  // manager is told at once and the queue handed back, as by shutdown(),
  // and nothing more is written; what the kernel still holds goes out ahead
  // of this end of the stream, and what arrives meanwhile is read and
  // dropped until the peer's end, `bound` seconds at most. A socket closed
  // with something unread makes the kernel reset the connection and throw
  // away what it had not sent yet; closed after the peer's end, it has
  // nothing unread, and what was written before arrives. True when it
  // reads on; false when it shut down at once, before its handshake was
  // done, or was already closing.
  bool close_gracefully(float bound) {
    MX_DCHECK_RUN_ON(&io_thread_);
    if (shuts_down_ || told_gone_) {
      return false;
    }
    if (!is_registered_) {
      shutdown();
      return false;
    }
    _read_to_the_end(bound, /*deliver=*/false);
    return true;
  }

  // Ends the connection once what is queued is written, for a multiplexer
  // that stops: until then the connection goes on as it was, read, written
  // to and routed to; then it closes as close_gracefully(bound) does, so
  // that this end of the stream follows everything written. A connection
  // whose handshake is not done shuts down at once.
  void close_when_flushed(float bound) {
    MX_DCHECK_RUN_ON(&io_thread_);
    if (shuts_down_ || told_gone_) {
      return;
    }
    if (!is_registered_) {
      shutdown();
      return;
    }
    close_when_flushed_ = true;
    close_bound_ = bound;
    if (outgoing_queue_.empty()) {
      close_gracefully(bound);
    }
  }

  // Messages, not counting heartbeats, nor, on a client's connection
  // (READS_END_HERE), the protocol's answers to what the client sent, that
  // arrived after the connection began closing and were dropped (see
  // close_gracefully).
  std::uint64_t dropped_while_closing() const { return dropped_while_closing_; }

  // Queues a frame for writing and starts the write if the channel is free.
  // Returns what the manager's traits say a scheduling result is: a tribool
  // tracker for the client (unknown until written or lost), a bool for the
  // multiplexer. A full queue drops the message with a warning, which is the
  // only back-pressure there is; `force` lets protocol messages (welcome,
  // heartbeat, a status reply, a routing request) past a full queue, by
  // FORCED_FRAMES_PAST_FULL_QUEUE frames at most, and `asap` puts them right
  // behind the frame being written so that a long queue cannot delay the
  // handshake or a heartbeat.
  typename MessagesBufferTraits::SchedulingResultFunctor::result_type schedule(std::shared_ptr<const RawMessage> msg,
                                                                               bool force = false, bool asap = false) {
    MX_DCHECK_RUN_ON(&io_thread_);

    using mx::repr;

    if (!is_living_) {
      return scheduling_result_type_default_functor_();  // drop
    }

    Assert(msg->usability() == RawMessage::WRITING);
    Assert(!shuts_down_);

    if (outgoing_queue_full() &&
        (!force || outgoing_queue_.size() >= std::size_t(outgoing_queue_max_size_) + FORCED_FRAMES_PAST_FULL_QUEUE)) {
      // Per message, so off unless asked for: the manager, which knows the
      // peer and the message, says it (the multiplexer through LogSummary).
      MX_LOG(DEBUG, CHATTERBOX, TEXT("outgoing queue full, dropping message"));
      return scheduling_result_type_default_functor_();  // drop
    }

    typename MessagesBufferTraits::ToBufferRepresentationConverter::result_type value = raw_to_buffer_converter_(msg);
    if (asap) {
      // insert `msg' as close to the outgoing_queue_.front() as possible
      if (outgoing_channel_state_ == ChannelState::FREE) {
        Assert(outgoing_queue_.empty());
        outgoing_queue_.push_front(value);
      } else {
        // outgoing_queue_.front() is being sent; let's insert msg just behind
        // it
        Assert(!outgoing_queue_.empty());
        typename MessagesBuffer::iterator pos = outgoing_queue_.begin();
        Assert(pos != outgoing_queue_.end());
        ++pos;
        outgoing_queue_.insert(pos, value);
      }
    } else {
      outgoing_queue_.push_back(value);
    }

    _process_send_queue();
    AssertMsg(outgoing_queue_.empty() == (outgoing_channel_state_ == ChannelState::FREE),
              repr(outgoing_queue_.empty()) + " == (" + repr(outgoing_channel_state_) +
                  " == " + repr(ChannelState::FREE) + ") failed");
    return scheduling_result_type_functor_(value);
  }

  // Queues an entry the manager built itself: a message that waited for
  // room, or one another connection gave up on shutdown (see
  // _orphan_outgoing_messages), with its tracker. False when the
  // connection is dead or full.
  bool inline take_over(typename MessagesBuffer::value_type internal) {
    MX_DCHECK_RUN_ON(&io_thread_);
    if (!is_living_ || outgoing_queue_full()) {
      return false;
    }
    outgoing_queue_.push_back(internal);
    _process_send_queue();
    return true;
  }

  inline std::uint32_t peer_type() const { return peer_type_; }
  inline std::uint64_t peer_id() const { return peer_id_; }
  inline bool registered() const { return is_registered_; }

  inline void set_outgoing_queue_max_size(unsigned int ms) { outgoing_queue_max_size_ = ms; }
  inline bool outgoing_queue_full() const {
    MX_DCHECK_RUN_ON(&io_thread_);
    return outgoing_queue_max_size_ <= outgoing_queue_.size();
  }
  inline bool outgoing_queue_empty() const {
    MX_DCHECK_RUN_ON(&io_thread_);
    return outgoing_queue_.empty();
  }
  // The entry queued last but the connection's own frames, which
  // everything queued before it is written ahead of; null when the queue
  // holds none other. For a manager that follows it, a client waiting
  // until what it queued so far is out: an own frame, a PEER_CONTROL or a
  // heartbeat, goes with the connection when it dies, told to nobody, and
  // one followed would be waited for for good, where the frames ahead of
  // it go to another connection, and the one followed with them.
  typename MessagesBuffer::value_type* last_queued() {
    MX_DCHECK_RUN_ON(&io_thread_);
    typename ConnectionsManagerTraits::MessagesBufferTraits::ToRawMessagePointerConverter to_raw;
    for (typename MessagesBuffer::reverse_iterator entry = outgoing_queue_.rbegin(); entry != outgoing_queue_.rend();
         ++entry) {
      if (!to_raw(*entry)->own()) {
        return &*entry;
      }
    }
    return nullptr;
  }
  inline bool living() const { return is_living_; }
  // living(), readable from any thread without a reference to the
  // connection: what a lane holding it reads (Lane::closed), since a dead
  // connection's object lives on while it reads to the end.
  std::shared_ptr<const std::atomic<bool>> living_flag() const { return living_flag_; }
  inline bool shuts_down() const { return shuts_down_; }

 private:
  /* Heartbeats. Two timers: send_heartbit_timer_ comes due HEARTBIT_INTERVAL
   * after the outgoing queue last emptied and queues a heartbeat, so an idle
   * connection carries one frame every interval. require_heartbit_timer_
   * comes due NO_HEARTBIT_SO_PREPARE_DROP after the last frame read, then
   * waits NO_HEARTBIT_SO_REALLY_DROP more and drops the connection, when
   * nothing arrives. Both are off for passive peers, in the sense described
   * at set_is_passive(): the send timer runs for one only while a heartbeat
   * is owed, so an idle passive connection costs no wakeup at all.
   *
   * Neither is moved by every frame. A write that empties the queue stamps
   * quiet_since_ and a frame read stamps heard_at_, a clock read each; a
   * wait already armed stays, and when it comes due before its stamp's
   * moment it arms itself again for that moment. Re-arming at every frame,
   * as these timers did, cancelled the wait before, whose handler the loop
   * then ran, and moved the timer in asio's heap, a timerfd_settime(2)
   * whenever it was the earliest: per message, in every peer and the
   * multiplexer. Each arming has a number, which its handler is given: one
   * replaced, or come due in the loop pass that also read a frame, which
   * cannot take back a wait the pass already collected, finds a newer
   * number, or none, and does nothing. */
  template <typename WaitHandler>
  void _do_later(asio::steady_timer& timer, const float seconds, WaitHandler handler) {
    timer.expires_after(mx::from_seconds(seconds));
    timer.async_wait(handler);
  }

  // Arms the send timer, HEARTBIT_INTERVAL from now, with nothing queued;
  // with something queued the write in progress calls this again when the
  // queue is empty. To a passive peer, at most one heartbeat per frame
  // received: it reads only inside calls, and a stream of heartbeats would
  // fill its socket buffer and then its incoming queue while it is away.
  // One owed nothing leaves the timer off, heartbit_timer_off_, until it
  // sends a frame (_handle_read_body) or its type turns active
  // (set_is_passive). Not on a connection that is no longer living, whose
  // reading to the end bounds itself with this timer (_read_to_the_end).
  void _send_heartbit_later() {
    MX_DCHECK_RUN_ON(&io_thread_);
    if (!is_living_) {
      return;
    }
    heartbit_timer_off_ = false;
    if (!outgoing_queue_.empty()) {
      return;
    }
    if (is_passive_ && !should_send_heartbit_) {
      heartbit_timer_off_ = true;
      return;
    }
    quiet_since_ = std::chrono::steady_clock::now();
    if (!send_wait_) {  // one armed moves on to this moment when it comes due
      _arm_send(quiet_since_ + mx::from_seconds(HEARTBIT_INTERVAL));
    }
  }

  void _arm_send(std::chrono::steady_clock::time_point due) {
    MX_DCHECK_RUN_ON(&io_thread_);
    send_wait_ = ++waits_armed_;
    send_heartbit_timer_.expires_at(due);
    send_heartbit_timer_.async_wait([self = this->shared_from_this(), arming = send_wait_](
                                        const asio::error_code& error) { self->_send_heartbit_now(error, arming); });
  }

  void _send_heartbit_now(const asio::error_code& error, std::uint64_t arming) {
    MX_DCHECK_RUN_ON(&io_thread_);
    if (error == asio::error::operation_aborted || arming != send_wait_) {
      return;  // cancelled, or the timer serves the reading to the end now
    }
    send_wait_ = 0;
    // Not while closing either: a wait that had expired just before the
    // connection began closing gracefully runs all the same, and would
    // log that the heartbeat could not be queued.
    if (shuts_down_ || !is_living_) {
      return;
    }
    // The queue emptied since the wait was armed: it moves on.
    const std::chrono::steady_clock::time_point due = quiet_since_ + mx::from_seconds(HEARTBIT_INTERVAL);
    if (due > std::chrono::steady_clock::now()) {
      _arm_send(due);
      return;
    }
    // A passive peer is sent the heartbeat it is owed (_send_heartbit_later);
    // one owed nothing, turned passive while the timer ran, leaves it off.
    if (is_passive_) {
      if (!should_send_heartbit_) {
        heartbit_timer_off_ = true;
        return;
      }
      should_send_heartbit_ = false;
    }
    if (!schedule(heartbit_message_, true, true)) {
      MX_LOG(ERROR, MEDIUMVERBOSITY,
             CTX("connection") TEXT("failed to schedule Heartbit message, we may lose the connection")
             // TODO add peer_type() with DATA()
      );
    }
  }

  // A frame read, or the peer's type turned active: the drop's first wait
  // runs from now. One armed moves on to this moment when it comes due;
  // the second wait, the drop itself, gives way to a first.
  inline void _require_heartbit_later() {
    MX_DCHECK_RUN_ON(&io_thread_);
    if (is_passive_) {
      // we can't require Heartbits from passive clients
      if (require_wait_) {
        require_wait_ = 0;
        require_heartbit_timer_.cancel();
      }
      return;
    }
    heard_at_ = std::chrono::steady_clock::now();
    if (!require_wait_ || second_wait_) {
      _arm_require(heard_at_ + mx::from_seconds(NO_HEARTBIT_SO_PREPARE_DROP_INTERVAL), /*second=*/false);
    }
  }

  void _arm_require(std::chrono::steady_clock::time_point due, bool second) {
    MX_DCHECK_RUN_ON(&io_thread_);
    second_wait_ = second;
    require_wait_ = ++waits_armed_;
    require_heartbit_timer_.expires_at(due);
    require_heartbit_timer_.async_wait(
        [self = this->shared_from_this(), arming = require_wait_](const asio::error_code& error) {
          self->_require_heartbit_due(error, arming);
        });
  }

  // The drop's wait came due. A frame read in the same pass of the loop,
  // ahead of this handler, stamped heard_at_, or armed a first wait in
  // place of the second, whose number this handler then no longer has:
  // a synchronous client, which runs the loop only inside its calls, has a
  // stalled multiplexer's frames read in the pass that runs the expired
  // wait, and must not shut down a connection that is fine.
  void _require_heartbit_due(const asio::error_code& error, std::uint64_t arming) {
    MX_DCHECK_RUN_ON(&io_thread_);
    if (error == asio::error::operation_aborted || arming != require_wait_) {
      return;  // cancelled, replaced, or the peer turned passive
    }
    require_wait_ = 0;
    if (shuts_down_) {
      return;
    }
    const std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now();
    if (!second_wait_) {
      const std::chrono::steady_clock::time_point due =
          heard_at_ + mx::from_seconds(NO_HEARTBIT_SO_PREPARE_DROP_INTERVAL);
      if (due > now) {
        _arm_require(due, /*second=*/false);  // a frame read since: the wait moves on
        return;
      }
      // First phase of the drop: nothing arrived for the prepare interval.
      // Wait once more before really closing, so that a short stall on a
      // busy peer does not cost it the connection. From now, when this
      // runs: a synchronous client handles it at its next call, however
      // long it idled, and idling alone must not cost it the connection.
      _arm_require(now + mx::from_seconds(NO_HEARTBIT_SO_REALLY_DROP_INTERVAL), /*second=*/true);
      return;
    }
    shutdown();  // the second phase over with nothing read
  }

  /* Receiving. The socket is read into the connection's buffer, as much as
   * the buffer has room for, and the frames in it are handed over one per
   * step of the loop, each step after the first posted to it, before the
   * socket is read again: a burst of small frames costs one read, and a
   * lone frame one read and the one that finds the socket empty, where
   * every frame cost a read for its header, one for its body and that
   * last one. A frame too big for the buffer gets one of its own, the rest
   * of its body read straight into it, and no read or write stops at
   * asio's 64 KiB a step (TransferRest). A manager that does not keep up
   * still slows the peer down through TCP rather than by growing memory
   * here: the buffer is the most that waits, a client's poll() counts the
   * frames as they are handed over, and the socket is read again only once
   * the buffer holds no whole frame. The buffer is made at the first read,
   * and a read starts at its front when what was read before is all taken,
   * so that a connection of small frames keeps a page of it in memory. Any
   * malformed frame ends the connection: input from the network is never
   * trusted with an Assert. */
  static constexpr std::size_t READ_BUFFER_SIZE = 64 * 1024;

  // The next step of reading: the buffer's next frame handed over, or a
  // read of the socket for more.
  void _start_read() {
    MX_DCHECK_RUN_ON(&io_thread_);
    if (incoming_channel_state_ == ChannelState::BROKEN) {
      return;
    }
    Assert(incoming_channel_state_ == ChannelState::FREE);
    if (!read_buffer_) {
      read_buffer_.reset(new char[READ_BUFFER_SIZE]);
    }
    const std::size_t held = read_end_ - read_begin_;
    if (held >= RawMessage::HEADER_LENGTH) {
      const char* const start = read_buffer_.get() + read_begin_;
      const std::size_t length = RawMessage::body_length(start);
      const std::size_t whole = RawMessage::HEADER_LENGTH + length;
      if (!length) {
        MX_LOG(WARNING, LOWVERBOSITY,
               CTX("connection") TEXT("invalid frame header from peer; dropping connection " + repr((void*)this)));
        shutdown();
        return;
      }
      if (held >= whole || whole > READ_BUFFER_SIZE) {
        // Whole, or too big for the buffer: what it holds of the body goes
        // into the frame's own, the header good as body_length() found it.
        std::shared_ptr<RawMessage> frame = std::make_shared<RawMessage>();
        frame->unpack_header(start);
        const std::size_t have = std::min(held, whole) - RawMessage::HEADER_LENGTH;
        asio::buffer_copy(frame->get_body_buffer(), asio::buffer(start + RawMessage::HEADER_LENGTH, have));
        read_begin_ += RawMessage::HEADER_LENGTH + have;
        if (have == length) {
          _frame_read(std::move(frame));
          return;
        }
        // The header counts as heard; the rest of the body may take long.
        incoming_channel_state_ = ChannelState::READING_BODY;
        _require_heartbit_later();
        asio::async_read(
            socket_, frame->get_body_buffer() + have, TransferRest(),
            [self = this->shared_from_this(), frame, rest = length - have](
                const asio::error_code& error, size_t bytes) { self->_handle_read_body(frame, rest, error, bytes); });
        return;
      }
    }
    // More is needed, for a frame or the rest of one begun: what the buffer
    // holds moves to its front, and the rest of it is read into.
    if (read_begin_) {
      std::memmove(read_buffer_.get(), read_buffer_.get() + read_begin_, held);
      read_begin_ = 0;
      read_end_ = held;
    }
    incoming_channel_state_ = ChannelState::READING;
    socket_.async_read_some(asio::buffer(read_buffer_.get() + read_end_, READ_BUFFER_SIZE - read_end_),
                            [self = this->shared_from_this()](const asio::error_code& error, size_t bytes) {
                              self->_handle_read(error, bytes);
                            });
  }
  void _handle_read(const asio::error_code& error, size_t bytes_transferred) {
    MX_DCHECK_RUN_ON(&io_thread_);
    if (incoming_channel_state_ == ChannelState::BROKEN) {
      return;
    }
    if (bytes_transferred == 0) {
      MX_LOG(DEBUG, HIGHVERBOSITY, TEXT("peer shut its end down \"gracefully\""));
      incoming_channel_state_ = ChannelState::BROKEN;  // this shouldn't be needed
      shutdown();
      return;
    }
    if (error) {
      MX_LOG(ERROR, HIGHVERBOSITY,
             TEXT("read error on " + repr((void*)this) + "error=" + repr(error) +
                  "bytes_transferred=" + repr(bytes_transferred)));
      shutdown();
      return;
    }
    Assert(incoming_channel_state_ == ChannelState::READING);
    incoming_channel_state_ = ChannelState::FREE;
    read_end_ += bytes_transferred;
    _start_read();
  }
  // The rest of a frame too big for the buffer, `rest` bytes, read.
  void _handle_read_body(std::shared_ptr<RawMessage> frame, size_t rest, const asio::error_code& error,
                         size_t bytes_transferred) {
    MX_DCHECK_RUN_ON(&io_thread_);
    if (incoming_channel_state_ == ChannelState::BROKEN) {
      return;
    }
    if (error || bytes_transferred != rest) {
      MX_LOG(ERROR, HIGHVERBOSITY,
             TEXT("read body error on " + repr((void*)this) + "error=" + repr(error) +
                  "bytes_transferred=" + repr(bytes_transferred) + "expected_transferred=" + repr(rest)));
      shutdown();
      return;
    }
    Assert(incoming_channel_state_ == ChannelState::READING_BODY);
    incoming_channel_state_ = ChannelState::FREE;
    _frame_read(std::move(frame));
  }

  // A whole frame read: its checksum checked, the frame handed over, and the
  // next step of reading taken, as a step of the loop of its own when the
  // buffer holds the next frame whole.
  void _frame_read(std::shared_ptr<RawMessage> frame) {
    MX_DCHECK_RUN_ON(&io_thread_);
    should_send_heartbit_ = true;
    if (!frame->verify()) {
      MX_LOG(ERROR, HIGHVERBOSITY, TEXT("incoming message verification failed"));
      shutdown();
      return;
    }
    _require_heartbit_later();
    if (heartbit_timer_off_) {
      _send_heartbit_later();  // the heartbeat this frame is owed
    }
    // Whatever handling the frame throws, the connection reads on: the
    // exception used to leave it registered, writing and never reading
    // again. The clients catch their callbacks' own exceptions; this is
    // the net for anything else, the frame logged and dropped.
    try {
      _receive_message(std::shared_ptr<const RawMessage>(std::move(frame)));
    } catch (const std::exception& e) {
      MX_LOG(ERROR, LOWVERBOSITY,
             CTX("connection")
                 TEXT("handling a frame from peer " + repr(peer_id_) + " failed, frame dropped: " + e.what()));
    }
    if (_next_frame_held()) {
      asio::post(socket_.get_executor(), [self = this->shared_from_this()] { self->_start_read(); });
    } else {
      _start_read();
    }
  }
  // Whether the buffer holds what the next step takes without a read: a
  // whole frame, or the header of one it cannot take, too big for the
  // buffer or malformed.
  bool _next_frame_held() const {
    MX_DCHECK_RUN_ON(&io_thread_);
    const std::size_t held = read_end_ - read_begin_;
    if (held < RawMessage::HEADER_LENGTH) {
      return false;
    }
    const std::size_t length = RawMessage::body_length(read_buffer_.get() + read_begin_);
    return !length || held >= RawMessage::HEADER_LENGTH + length ||
           RawMessage::HEADER_LENGTH + length > READ_BUFFER_SIZE;
  }

  // Whether `msg` is one of the protocol's own answers to what this peer
  // sent: a delivery error, a backend's acknowledgement or error report, a
  // control frame's status, or a PING answering a PING or a search.
  static bool _answers_this_peer(const MultiplexerMessage& msg) {
    switch (msg.type()) {
      case types::DELIVERY_ERROR:
      case types::REQUEST_RECEIVED:
      case types::BACKEND_ERROR:
      case types::RECORDING_STATUS:
      case types::RULES_STATUS:
      case types::PEER_STATUS:
        return true;
      case types::PING:
        return msg.references() != 0;
      default:
        return false;
    }
  }

  // The frame is passed on as the RawMessage it arrived in, so that the
  // multiplexer can forward it to other connections without re-serializing.
  void _receive_message(const std::shared_ptr<const RawMessage>& message) {
    MX_DCHECK_RUN_ON(&io_thread_);
    if (!delivering_) {
      // leaving: read only so that nothing is left unread when the socket
      // closes; what was a message, rather than a heartbeat or, at a
      // client, an answer only it waited for, is counted
      MultiplexerMessage dropped;
      if (dropped.ParseFromString(message->get_message()) && dropped.type() != types::HEARTBIT &&
          !(ConnectionsManagerTraits::READS_END_HERE && _answers_this_peer(dropped))) {
        ++dropped_while_closing_;
      }
      return;
    }

    ManagerPointer manager = manager_.lock();
    if (!manager) {
      MX_LOG(DEBUG, HIGHVERBOSITY, TEXT("manager is gone for " + repr((void*)this)));
      shutdown();
      return;
    }

    std::shared_ptr<MultiplexerMessage> mxmsg(new MultiplexerMessage());
    if (!mxmsg->ParseFromString(message->get_message())) {
      MX_LOG(DEBUG, HIGHVERBOSITY,
             TEXT("received invalid message on " + repr((void*)this) + "(maybe id=" + repr(mxmsg->id()) + ")"));
      shutdown();
      return;
    }

    if (_receive_internal_message(*mxmsg, manager)) {
      return;  // already handled
    }

    if (!is_registered_) {
      MX_LOG(WARNING, LOWVERBOSITY,
             CTX("connection") TEXT("message of type " + repr(mxmsg->type()) +
                                    " before CONNECTION_WELCOME; dropping connection " + repr((void*)this)));
      shutdown();
      return;
    }

    manager->handle_message(this->shared_from_this(), message, mxmsg);
  }

  // The two message types a connection handles itself: the welcome, which
  // registers the peer with the manager (exactly once, first, or the
  // connection is closed), and heartbeats, which have no content. Everything
  // else goes to the manager, but only once registered.
  inline bool _receive_internal_message(MultiplexerMessage& mxmsg, ManagerPointer manager) {
    MX_DCHECK_RUN_ON(&io_thread_);
    switch (mxmsg.type()) {
      case types::CONNECTION_WELCOME:
        if (!is_registered_) {
          WelcomeMessage welcome;
          bool ok = false;
          do {
            if (mxmsg.type() != types::CONNECTION_WELCOME) {
              break;
            }
            if (!welcome.ParseFromString(mxmsg.message())) {
              break;
            }
            ok = true;
          } while (0);

          if (!ok) {
            // TODO logger.error << "received invalid CONNECTION_WELCOME message;
            // shutting down";
            shutdown();

          } else {
            peer_id_ = welcome.id();
            peer_type_ = welcome.type();
            manager->register_connection(this->shared_from_this(), welcome);
            if (shuts_down_) {
              return true;  // register_connection refused this connection
            }
            // Registered before the hook runs, as the manager's maps
            // already have it: a client's confirmation of its routing
            // counts this connection, whose welcome carried that routing.
            is_registered_ = true;
            manager->after_connection_registration(this->shared_from_this(), welcome);
          }
        } else {
          // TODO logger.error << "received repeated CONNECTION_WELCOME message;
          // shutting down";
          shutdown();
        }
        return true;
        // types::CONNECTION_WELCOME

      case types::HEARTBIT:
        if (!is_registered_) {
          // The welcome must come first, from any peer; a library sends
          // its heartbeats only after it.
          shutdown();
        }
        return true;

      default:
        return false;
    }
  }

  /* Sending. One async_write in flight at a time, always for the queue's
   * front; _handle_write pops it, notifies the manager's tracker, starts
   * the next and, for a manager that asks (REPORTS_ROOM), says when a full
   * queue has room again. A short or failed write ends the connection,
   * since the frame boundary would be lost. */
  void _process_send_queue() {
    MX_DCHECK_RUN_ON(&io_thread_);
    // check if the outgoing channel is free and we have anything to send
    if (outgoing_channel_state_ == ChannelState::FREE && !outgoing_queue_.empty() && !shuts_down_) {
      Assert(is_living_);
      typename ConnectionsManagerTraits::MessagesBufferTraits::ToRawMessagePointerConverter to_raw_converter;
      const typename ConnectionsManagerTraits::MessagesBufferTraits::ToRawMessagePointerConverter::result_type raw =
          to_raw_converter(outgoing_queue_.front());

      // start writing
      outgoing_channel_state_ = ChannelState::BUSY;
      write_in_flight_ = true;
      Assert(raw->get_message_buffer().size());
      // The handler holds the connection and the frame: the write goes on in
      // steps after shutdown() handed the queue over or dropped it.
      asio::async_write(socket_, raw->get_message_buffer(), TransferRest(),
                        [self = this->shared_from_this(), raw](const asio::error_code& error, size_t bytes) {
                          self->_handle_write(error, bytes);
                        });
    }
  }
  void _handle_write(const asio::error_code& error, size_t bytes_transferred) {
    MX_DCHECK_RUN_ON(&io_thread_);
    write_in_flight_ = false;
    if (error) {
      MX_LOG(DEBUG, HIGHVERBOSITY,
             TEXT("write error on " + repr((void*)this) + " error=" + repr(error) +
                  " bytes_transferred=" + repr(bytes_transferred)));
    }

    if (outgoing_channel_state_ == ChannelState::BROKEN) {
      _settle_set_aside(error, bytes_transferred);
      return;
    }
    if (bytes_transferred == 0) {
      // is it exceptional?
      MX_LOG(DEBUG, HIGHVERBOSITY, TEXT("i have managed to write nothing. Shut down"));
      _write_failed();
      return;
    }
    if (error) {
      _write_failed();
      return;
    }

    // TODO handle error conditions (pass it to handle_write(..., false) ???
    Assert(outgoing_channel_state_ == ChannelState::BUSY);
    outgoing_channel_state_ = ChannelState::FREE;

    Assert(outgoing_queue_.size());
    typename ConnectionsManagerTraits::MessagesBufferTraits::ToRawMessagePointerConverter to_raw_converter;
    const typename ConnectionsManagerTraits::MessagesBufferTraits::ToRawMessagePointerConverter::result_type raw =
        to_raw_converter(outgoing_queue_.front());
    if (bytes_transferred != raw->get_header_length() + raw->get_body_length()) {
      MX_LOG(DEBUG, HIGHVERBOSITY, TEXT("i have managed to write partial. Shut down"));
      _write_failed();
      return;
    }

    message_sending_notifier_.notify_success(manager_, outgoing_queue_.front());
    const bool was_full = outgoing_queue_full();
    outgoing_queue_.pop_front();
    if (close_when_flushed_ && outgoing_queue_.empty()) {
      close_gracefully(close_bound_);  // everything is written: now this end of the stream
      return;
    }
    _send_heartbit_later();

    _process_send_queue();
    // A full queue with room again is news for a manager holding messages
    // back for it; the multiplexer's traits compile this out.
    if constexpr (ConnectionsManagerTraits::REPORTS_ROOM) {
      if (was_full && !outgoing_queue_full()) {
        if (ManagerPointer manager = manager_.lock()) {
          manager->outgoing_queue_has_room(this);
        }
      }
    }
    // Nothing left to write is news for a manager that asked
    // (notify_when_idle), once; the multiplexer's traits compile this out.
    if constexpr (ConnectionsManagerTraits::REPORTS_IDLE) {
      if (notify_when_idle_ && outgoing_queue_.empty()) {
        notify_when_idle_ = false;
        if (ManagerPointer manager = manager_.lock()) {
          manager->outgoing_queue_idle(this);
        }
      }
    }
  }

 public:
  // The next time the queue is left empty by a write, the manager hears
  // of it (outgoing_queue_idle), once: a client whose batches wait for a
  // connection with nothing left to write.
  void notify_when_idle() {
    MX_DCHECK_RUN_ON(&io_thread_);
    notify_when_idle_ = true;
  }

 private:
  // On shutdown, offers the unsent queue to the manager (the client moves
  // entries to another connection and takes them out of the queue, and
  // after them what waited for this one, so it is told even of an empty
  // queue) and reports the rest as lost. The frame being written is not
  // the queue's to give: its bytes may be in the kernel already, written
  // inside the call that queued it, and the write's handler, still to run,
  // says whether they are; it is set aside for that handler to settle
  // (_settle_set_aside), so that it is neither reported lost although it
  // arrives nor handed to another connection and sent twice.
  void inline _orphan_outgoing_messages() {
    MX_DCHECK_RUN_ON(&io_thread_);
    if (write_in_flight_ && !outgoing_queue_.empty()) {
      set_aside_.push_back(outgoing_queue_.front());
      set_aside_manager_ = manager_;
      outgoing_queue_.pop_front();
    }
    _give_up(outgoing_queue_, manager_);
  }

  // The write of the frame set aside has ended, the connection closed
  // meanwhile: a frame written whole is reported written, one that was not
  // goes the way the rest of the queue went.
  void _settle_set_aside(const asio::error_code& error, size_t bytes_transferred) {
    MX_DCHECK_RUN_ON(&io_thread_);
    if (set_aside_.empty()) {
      return;
    }
    typename ConnectionsManagerTraits::MessagesBufferTraits::ToRawMessagePointerConverter to_raw_converter;
    const bool whole = !error && bytes_transferred == to_raw_converter(set_aside_.front())->get_header_length() +
                                                          to_raw_converter(set_aside_.front())->get_body_length();
    std::weak_ptr<ConnectionsManagerImplementation> manager;
    manager.swap(set_aside_manager_);
    if (whole) {
      message_sending_notifier_.notify_success(manager, set_aside_.front());
      set_aside_.clear();
      return;
    }
    _give_up(set_aside_, manager);
  }

  // Offers `entries` to `manager`, which takes out of the buffer what it
  // hands to another connection, and reports what it left as lost. The
  // connection's own frames go first, silently (RawMessage::own).
  void _give_up(MessagesBuffer& entries, const std::weak_ptr<ConnectionsManagerImplementation>& manager) {
    MX_DCHECK_RUN_ON(&io_thread_);
    typename ConnectionsManagerTraits::MessagesBufferTraits::ToRawMessagePointerConverter to_raw;
    entries.erase(
        std::remove_if(entries.begin(), entries.end(),
                       [&to_raw](const typename MessagesBuffer::value_type& entry) { return to_raw(entry)->own(); }),
        entries.end());
    if (ManagerPointer owner = manager.lock()) {
      owner->handle_orphaned_outgoing_messages(this, entries);
    }
    if (entries.empty()) {
      return;
    }
    if constexpr (!ConnectionsManagerTraits::REPORTS_DROPS) {
      MX_LOG(WARNING, HIGHVERBOSITY,
             TEXT("Connection shutdown on " + repr((void*)this) + ", dropping " + repr(entries.size()) +
                  " outgoing message(s)"));
    }
    for (typename MessagesBuffer::value_type& entry : entries) {
      message_sending_notifier_.notify_error(manager, entry);
    }
    entries.clear();
  }

 public:
  typename ConnectionsManagerTraits::ConnectionManagerPrivateDataInConnection& managers_private_data() {
    return managers_private_data_;
  }
  const typename ConnectionsManagerTraits::ConnectionManagerPrivateDataInConnection& managers_private_data() const {
    return managers_private_data_;
  }

  // Where the manager keeps this connection among those of its peer type,
  // while it is registered, so that unregistering takes it out in O(1).
  typename std::list<weak_pointer>::iterator type_list_place;
  bool in_type_list = false;

  /* members */
 private:
  asio::ip::tcp::socket socket_;
  std::uint32_t peer_type_;
  std::uint64_t peer_id_;
  bool is_passive_;
  Routing routing_;
  google::protobuf::RepeatedPtrField<KeyFilter> key_filters_;
  std::uint32_t key_owner_ = 0xffffffffu;  // KeyPatterns::NO_OWNER until registered

  bool is_living_;
  std::shared_ptr<std::atomic<bool>> living_flag_;  // is_living_, for other threads; see living_flag()
  bool shuts_down_;
  bool is_registered_;
  bool told_gone_;           // the manager heard the connection is gone: once, see _tell_manager_gone()
  bool delivering_;          // what is read goes to the manager; not while leaving, see close_gracefully()
  bool close_when_flushed_;  // close_gracefully(close_bound_) once the queue is written
  float close_bound_;
  std::uint64_t dropped_while_closing_;
  std::weak_ptr<ConnectionsManagerImplementation> manager_;
  bool should_send_heartbit_;  // a frame arrived since the last heartbeat: a passive peer is owed one
  // The send timer neither waits nor will be armed by a write in progress:
  // a passive peer owed nothing (see _send_heartbit_later).
  bool heartbit_timer_off_ = false;
  // The heartbeat timers' stamps and armings (see "Heartbeats"): when the
  // queue last emptied, when a frame last arrived, the number of each
  // timer's wait armed, 0 for none, from a counter, and whether the
  // require timer's is the drop's second phase.
  std::chrono::steady_clock::time_point quiet_since_;
  std::chrono::steady_clock::time_point heard_at_;
  std::uint64_t send_wait_ = 0;
  std::uint64_t require_wait_ = 0;
  std::uint64_t waits_armed_ = 0;
  bool second_wait_ = false;

 public:
  // Makes the calling thread the one this connection is driven from, for a
  // client that was built on one thread and is served from another; see
  // BasicClient::bind_to_current_thread().
  void bind_io_thread_to_current() { io_thread_.bind_to_current(); }

 private:
  // The thread that runs this connection's io_service: every method here
  // runs on it, and the members below may only be touched on it.
  mx::ThreadChecker io_thread_;

  /* outgoing channel */
  MessagesBuffer outgoing_queue_ MX_GUARDED_BY(io_thread_);
  ChannelStateT outgoing_channel_state_ MX_GUARDED_BY(io_thread_);
  std::uint32_t outgoing_queue_max_size_;
  // An async_write was started and its handler has not run yet. On a
  // shutdown meanwhile, the frame it writes is set aside, with the manager
  // to tell, for that handler to settle (see _orphan_outgoing_messages).
  bool write_in_flight_ MX_GUARDED_BY(io_thread_) = false;
  bool notify_when_idle_ MX_GUARDED_BY(io_thread_) = false;  // see notify_when_idle()
  MessagesBuffer set_aside_ MX_GUARDED_BY(io_thread_);
  std::weak_ptr<ConnectionsManagerImplementation> set_aside_manager_ MX_GUARDED_BY(io_thread_);

  /* incoming channel */
  // Bytes read and not yet taken as frames: read_buffer_[read_begin_,
  // read_end_), made at the first read; see _start_read.
  std::unique_ptr<char[]> read_buffer_ MX_GUARDED_BY(io_thread_);
  std::size_t read_begin_ MX_GUARDED_BY(io_thread_) = 0;
  std::size_t read_end_ MX_GUARDED_BY(io_thread_) = 0;
  ChannelStateT incoming_channel_state_ MX_GUARDED_BY(io_thread_);

  /* heartbiting */
  asio::steady_timer send_heartbit_timer_;
  asio::steady_timer require_heartbit_timer_;
  std::shared_ptr<const RawMessage> heartbit_message_;

  /* Manager's data pool */
  typename ConnectionsManagerTraits::ConnectionManagerPrivateDataInConnection managers_private_data_;

  /* some functors */
  typename MessagesBufferTraits::SendingResultNotifier message_sending_notifier_;
  typename MessagesBufferTraits::ToBufferRepresentationConverter raw_to_buffer_converter_;
  typename MessagesBufferTraits::SchedulingResultFunctor scheduling_result_type_functor_;
  mx::DefaultConstructingFactory<typename MessagesBufferTraits::SchedulingResultFunctor::result_type>
      scheduling_result_type_default_functor_;

  // friend class ConnectionsManagerImplementation;

};  // class Connection

};  // namespace multiplexer

#endif  // MX_MULTIPLEXER_IO_CONNECTION_H_
