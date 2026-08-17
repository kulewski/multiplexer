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
// to return. Every asynchronous handler holds a shared_ptr to the connection
// (via shared_from_this), so a Connection is destroyed only when no I/O is in
// flight; the manager keeps weak_ptrs. Nothing here is thread-safe: one
// io_service, one thread.
//
// Protocol pieces that live here: the CONNECTION_WELCOME handshake, the
// heartbeat timers with the passive-peer exemption, and the rule that any
// malformed frame or out-of-order message closes the connection.
#ifndef MX_MULTIPLEXER_IO_CONNECTION_H_
#define MX_MULTIPLEXER_IO_CONNECTION_H_

#include <deque>
#include <string>

#include <boost/asio/deadline_timer.hpp>
#include <boost/asio/io_service.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/placeholders.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/streambuf.hpp>
#include <boost/asio/write.hpp>
#include <boost/bind/bind.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/foreach.hpp>
#include <boost/numeric/conversion/cast.hpp>
#include <boost/random/linear_congruential.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/weak_ptr.hpp>
#include <google/protobuf/message.h>

#include "lib/functors.h"
#include "lib/logging/logging.h"
#include "lib/repr.h"
#include "lib/thread_checker.h"
#include "multiplexer/Multiplexer.pb.h" /* generated */
#include "multiplexer/defaults.h"
#include "multiplexer/io/raw_message.h"
#include "multiplexer/multiplexer.constants.h" /* generated */

namespace multiplexer {

using mx::repr;

// Specialized per manager (Server, BasicClient) in the manager's own header,
// which includes this one; declared here so that this header stands alone.
template <typename ConnectionsManagerImplementation> struct ConnectionsManagerTraits;

// The connection itself; see the file comment. `ConnectionsManagerImplementation`
// is Server or BasicClient, and must provide: instance_id(),
// get_welcome_message(), register_connection(), after_connection_registration(),
// unregister_connection(), connection_destroyed(), handle_message() and
// handle_orphaned_outgoing_messages(). See connections_manager.h for the
// shared implementation of most of them.
template <class ConnectionsManagerImplementation>
class Connection : public boost::enable_shared_from_this<Connection<ConnectionsManagerImplementation>> {

public:
  typedef multiplexer::ConnectionsManagerTraits<ConnectionsManagerImplementation> ConnectionsManagerTraits;
  typedef typename ConnectionsManagerTraits::MessagesBufferTraits MessagesBufferTraits;
  typedef std::deque<typename MessagesBufferTraits::value_type> MessagesBuffer;

  // Each direction of the socket is a channel with its own state. Outgoing:
  // FREE (nothing being written), BUSY (one async_write in flight). Incoming:
  // FREE, READING_HEADER, READING_BODY. BROKEN, in either, means shutdown()
  // ran and any handler that still fires must return without touching the
  // socket.
  struct ChannelState {
    enum _ChannelState { FREE, BUSY, BROKEN, READING_HEADER, READING_BODY };
  };
  typedef typename ChannelState::_ChannelState ChannelStateT;

  /* instance members */
private:
  Connection(boost::asio::io_service &io_service, boost::shared_ptr<ConnectionsManagerImplementation> manager)
      : socket_(io_service), peer_type_(0), peer_id_(0), is_passive_(false), is_living_(false), shuts_down_(false),
        is_registered_(false), manager_(manager), should_send_heartbit_(true),
        outgoing_channel_state_(ChannelState::FREE), outgoing_queue_max_size_(1), incoming_message_(new RawMessage()),
        incoming_channel_state_(ChannelState::FREE), send_heartbit_timer_(io_service),
        require_heartbit_timer_(io_service)
  //, send_heartbit_timer_(io_service,
  // boost::posix_time::microseconds(HEARTBIT_INTERVAL * 1000000)) ,
  // require_heartbit_timer_(io_service,
  // boost::posix_time::microseconds(NO_HEARTBIT_SO_DROP_INTERVAL * 1000000))
  {
    MX_DCHECK_RUN_ON(&io_thread_);
    // The heartbeat frame never changes, so it is serialized once per
    // connection and re-queued by pointer; id 0 marks it as not a real message.
    MultiplexerMessage mxmsg;
    mxmsg.set_id(0);
    mxmsg.set_type(types::HEARTBIT);
    mxmsg.set_from(manager->instance_id());
    heartbit_message_.reset(RawMessage::FromMessage(mxmsg));
  }

public:
  typedef boost::shared_ptr<Connection> pointer;
  typedef boost::weak_ptr<Connection> weak_pointer;
  typedef boost::shared_ptr<ConnectionsManagerImplementation> ManagerPointer;

  /**
   * Create
   *  A factory function that ensures that every instance is managed by a
   * shared_ptr, so that having a shared_ptr is always equal to having living
   * instance.
   */
  static pointer Create(boost::asio::io_service &io_service,
                        boost::shared_ptr<ConnectionsManagerImplementation> manager) {
    pointer created(new Connection(io_service, manager));
    MX_LOG(DEBUG, HIGHVERBOSITY, TEXT("created new Connection " + repr((void *)created.get())));
    return created;
  }

  ~Connection() {
    MX_LOG(DEBUG, HIGHVERBOSITY, TEXT("destroying Connection " + repr((void *)this)));
    if (!shuts_down_)
      shutdown();
  }

  boost::asio::ip::tcp::socket &socket() { return socket_; }

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
    MX_LOG(DEBUG, HIGHVERBOSITY, TEXT("starting Connection " + repr((void *)this)));
    Assert(!is_living_);
    Assert(!shuts_down_);
    is_living_ = true;
    _start_read();
  }

  void start_rest() {
    MX_DCHECK_RUN_ON(&io_thread_);
    if (shuts_down_)
      return;
    Assert(is_living_);
    ManagerPointer manager = manager_.lock();
    if (manager && schedule(manager->get_welcome_message(), true, true)) {
      _send_heartbit_later(); // included in schedule() >> _process_send_queue()
      _require_heartbit_later();
    } else {
      shutdown();
    }
  }

  // Set by the multiplexer once it knows the peer's type. A passive peer runs
  // no loop between calls, so it is neither expected to send heartbeats nor
  // sent more than one heartbeat per message it delivers (see
  // _send_heartbit_now); cancelling the require timer is what exempts it.
  inline void set_is_passive(bool is_passive) {
    MX_DCHECK_RUN_ON(&io_thread_);
    is_passive_ = is_passive;
    _require_heartbit_later();
  }

  // Stops all I/O, tells the manager, and drops or hands back the outgoing
  // queue. Safe to call at any point of the lifecycle, including on a socket
  // that was never opened, and idempotent. Pending handlers see BROKEN.
  void shutdown() {
    MX_DCHECK_RUN_ON(&io_thread_);
    MX_LOG(DEBUG, HIGHVERBOSITY, TEXT("shutdown called on " + repr((void *)this)));
    if (shuts_down_) {
      MX_LOG(ERROR, HIGHVERBOSITY, TEXT("shutdown called twice on " + repr((void *)this)));
      return;
    }

    // mark as dead
    shuts_down_ = true;
    ManagerPointer manager = manager_.lock();

    // notify our Manager
    if (is_registered_ && manager)
      manager->unregister_connection(this);
    is_registered_ = false;
    if (manager)
      manager->connection_destroyed(this);
    is_living_ = false;

    // stop doing I/O
    outgoing_queue_max_size_ = 0;
    incoming_channel_state_ = ChannelState::BROKEN;
    outgoing_channel_state_ = ChannelState::BROKEN;
    // The socket may never have been opened (a connection created for an
    // accept that was cancelled) or may already be closed, so none of these
    // may throw. Cancelling the timers lets the io loop drain after shutdown.
    boost::system::error_code ignored;
    socket_.cancel(ignored);
    socket_.shutdown(boost::asio::ip::tcp::socket::shutdown_both, ignored);
    send_heartbit_timer_.cancel(ignored);
    require_heartbit_timer_.cancel(ignored);
    incoming_message_.reset();

    // cancel outgoing messages
    _orphan_outgoing_messages();

    // die and let live or somehow ;)
    manager_.reset();
  }

  // Queues a frame for writing and starts the write if the channel is free.
  // Returns what the manager's traits say a scheduling result is: a tribool
  // tracker for the client (unknown until written or lost), a bool for the
  // multiplexer. A full queue drops the message with a warning, which is the
  // only back-pressure there is; `force` bypasses that for protocol messages
  // (welcome, heartbeat) and `asap` puts them right behind the frame being
  // written so that a long queue cannot delay the handshake or a heartbeat.
  typename MessagesBufferTraits::SchedulingResultFunctor::result_type schedule(boost::shared_ptr<const RawMessage> msg,
                                                                               bool force = false, bool asap = false) {
    MX_DCHECK_RUN_ON(&io_thread_);

    using mx::repr;

    if (!is_living_)
      return scheduling_result_type_default_functor_(); // drop

    Assert(msg->usability() == RawMessage::WRITING);
    Assert(!shuts_down_);

    if (!force && outgoing_queue_full()) {
      MX_LOG(WARNING, HIGHVERBOSITY, TEXT("outgoing queue full, dropping message"));
      return scheduling_result_type_default_functor_(); // drop
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

  // Re-queues an entry that another connection gave up on shutdown (see
  // _orphan_outgoing_messages); the client uses it to move messages from a
  // dead multiplexer connection to a live one.
  bool inline take_over(typename MessagesBuffer::value_type internal) {
    MX_DCHECK_RUN_ON(&io_thread_);
    if (!is_living_ || outgoing_queue_full())
      return false;
    outgoing_queue_.push_back(internal);
    _process_send_queue();
    return true;
  }

  inline boost::uint32_t peer_type() const { return peer_type_; }
  inline boost::uint64_t peer_id() const { return peer_id_; }
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
  inline bool living() const { return is_living_; }
  inline bool shuts_down() const { return shuts_down_; }

private:
  /* Heartbeats. Two timers: send_heartbit_timer_ fires HEARTBIT_INTERVAL after
   * the last write and queues a heartbeat, so an idle connection carries one
   * frame every interval. require_heartbit_timer_ is re-armed on every frame
   * read and drops the connection in two phases, NO_HEARTBIT_SO_PREPARE_DROP
   * then NO_HEARTBIT_SO_REALLY_DROP, when nothing arrives. Both are off for
   * passive peers, in the sense described at set_is_passive(). */
  template <typename WaitHandler>
  void _do_later(boost::asio::deadline_timer &timer, const float seconds, WaitHandler handler) {
    // timer.cancel();
    timer.expires_from_now(boost::posix_time::microseconds(boost::numeric_cast<long>(seconds * 1e6)));
    timer.async_wait(handler);
  }

  void _send_heartbit_later() {
    MX_DCHECK_RUN_ON(&io_thread_);
    if (outgoing_queue_.empty())
      _do_later(
          send_heartbit_timer_, HEARTBIT_INTERVAL,
          boost::bind(&Connection::_send_heartbit_now, this->shared_from_this(), boost::asio::placeholders::error));
  }

  void _send_heartbit_now(const boost::system::error_code &error) {
    MX_DCHECK_RUN_ON(&io_thread_);
    if (error == boost::asio::error::operation_aborted || shuts_down_)
      return;
    // To a passive peer, at most one heartbeat per frame received: it reads
    // only inside calls, and a stream of heartbeats would fill its socket
    // buffer and then its incoming queue while it is away.
    if (is_passive_) {
      if (!should_send_heartbit_) {
        _send_heartbit_later();
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

  inline void _require_heartbit_later() {
    MX_DCHECK_RUN_ON(&io_thread_);
    if (is_passive_) {
      // we can't require Heartbits from passive clients
      require_heartbit_timer_.cancel();
      return;
    }

    _do_later(
        require_heartbit_timer_, NO_HEARTBIT_SO_PREPARE_DROP_INTERVAL,
        boost::bind(&Connection::_require_heartbit_soon, this->shared_from_this(), boost::asio::placeholders::error));
  }

  void _require_heartbit_soon(const boost::system::error_code &error) {
    MX_DCHECK_RUN_ON(&io_thread_);
    // First phase of the drop: nothing arrived for the prepare interval. Wait
    // once more before really closing, so that a short stall on a busy peer
    // does not cost it the connection.
    if (error == boost::asio::error::operation_aborted || shuts_down_)
      return;
    _do_later(
        require_heartbit_timer_, NO_HEARTBIT_SO_REALLY_DROP_INTERVAL,
        boost::bind(&Connection::_require_heartbit_now, this->shared_from_this(), boost::asio::placeholders::error));
  }

  void _require_heartbit_now(const boost::system::error_code &error) {
    MX_DCHECK_RUN_ON(&io_thread_);
    if (error == boost::asio::error::operation_aborted || shuts_down_)
      return;
    // TODO logger.error << "no received messages for " <<
    // NO_HEARTBIT_SO_PREPARE_DROP_INTERVAL +
    // NO_HEARTBIT_SO_REALLY_DROP_INTERVAL
    //<< " seconds; shutting down... (peer_type = " << peer_type_ << ")";
    shutdown();
  }

  /* Receiving. One frame at a time: header, then body, then parse, then hand
   * over; a new async_read is issued only after the previous frame was
   * handled, so a manager that does not keep up slows the peer down through
   * TCP rather than by growing memory here. Any malformed frame ends the
   * connection: input from the network is never trusted with an Assert. */
  void _start_read() {
    MX_DCHECK_RUN_ON(&io_thread_);
    if (incoming_channel_state_ == ChannelState::BROKEN)
      return;

    Assert(incoming_channel_state_ == ChannelState::FREE);

    incoming_channel_state_ = ChannelState::READING_HEADER;
    boost::asio::async_read(socket_, boost::asio::buffer(incoming_message_->get_header_buffer()),
                            boost::bind(&Connection::_handle_read_header, this->shared_from_this(),
                                        boost::asio::placeholders::error,
                                        boost::asio::placeholders::bytes_transferred));
  }
  void _handle_read_header(const boost::system::error_code &error, size_t bytes_transferred) {
    MX_DCHECK_RUN_ON(&io_thread_);
    if (incoming_channel_state_ == ChannelState::BROKEN)
      return;

    if (bytes_transferred == 0) {
      MX_LOG(DEBUG, HIGHVERBOSITY, TEXT("peer shut its end down \"gracefully\""));
      incoming_channel_state_ = ChannelState::BROKEN; // this shouldn't be needed
      shutdown();
      return;
    }
    if (error) {
      MX_LOG(ERROR, HIGHVERBOSITY,
             TEXT("read header error on " + repr((void *)this) + "error=" + repr(error) +
                  "bytes_transferred=" + repr(bytes_transferred)));
      shutdown();
      return;
    }

    Assert(incoming_channel_state_ == ChannelState::READING_HEADER);
    Assert(bytes_transferred == incoming_message_->get_header_length());
    if (!incoming_message_->unpack_header()) {
      MX_LOG(WARNING, LOWVERBOSITY,
             CTX("connection") TEXT("invalid frame header from peer; dropping connection " + repr((void *)this)));
      shutdown();
      return;
    }

    incoming_channel_state_ = ChannelState::READING_BODY;
    _require_heartbit_later();
    boost::asio::async_read(socket_, boost::asio::buffer(incoming_message_->get_body_buffer()),
                            boost::bind(&Connection::_handle_read_body, this->shared_from_this(),
                                        boost::asio::placeholders::error,
                                        boost::asio::placeholders::bytes_transferred));
  }
  void _handle_read_body(const boost::system::error_code &error, size_t bytes_transferred) {
    MX_DCHECK_RUN_ON(&io_thread_);
    should_send_heartbit_ = true;
    if (incoming_channel_state_ == ChannelState::BROKEN)
      return;

    if (error || bytes_transferred != incoming_message_->get_body_length()) {
      MX_LOG(ERROR, HIGHVERBOSITY,
             TEXT("read body error on " + repr((void *)this) + "error=" + repr(error) + "bytes_transferred=" +
                  repr(bytes_transferred) + "expected_transferred=" + repr(incoming_message_->get_body_length())));
      shutdown();
      return;
    }

    Assert(bytes_transferred == incoming_message_->get_body_length());
    Assert(incoming_channel_state_ == ChannelState::READING_BODY);
    incoming_channel_state_ = ChannelState::FREE;

    if (!incoming_message_->verify()) {
      MX_LOG(ERROR, HIGHVERBOSITY, TEXT("incoming message verification failed"));
      shutdown();

    } else {
      _require_heartbit_later();
      _receive_message();
      _start_read();
    }
  }

  void _receive_message() {
    MX_DCHECK_RUN_ON(&io_thread_);
    // The frame is passed on as the RawMessage it arrived in, so that the
    // multiplexer can forward it to other connections without re-serializing.
    boost::shared_ptr<const RawMessage> message = incoming_message_;
    incoming_message_.reset(new RawMessage());

    ManagerPointer manager = manager_.lock();
    if (!manager) {
      MX_LOG(DEBUG, HIGHVERBOSITY, TEXT("manager is gone for " + repr((void *)this)));
      shutdown();
      return;
    }

    boost::shared_ptr<MultiplexerMessage> mxmsg(new MultiplexerMessage());
    if (!mxmsg->ParseFromString(message->get_message())) {
      MX_LOG(DEBUG, HIGHVERBOSITY,
             TEXT("received invalid message on " + repr((void *)this) + "(maybe id=" + repr(mxmsg->id()) + ")"));
      shutdown();
      return;
    }

    if (_receive_internal_message(*mxmsg, manager))
      return; // already handled

    if (!is_registered_) {
      MX_LOG(WARNING, LOWVERBOSITY,
             CTX("connection") TEXT("message of type " + repr(mxmsg->type()) +
                                    " before CONNECTION_WELCOME; dropping connection " + repr((void *)this)));
      shutdown();
      return;
    }

    manager->handle_message(this->shared_from_this(), message, mxmsg);
  }

  // The two message types a connection handles itself: the welcome, which
  // registers the peer with the manager (exactly once, first, or the
  // connection is closed), and heartbeats, which have no content. Everything
  // else goes to the manager, but only once registered.
  inline bool _receive_internal_message(MultiplexerMessage &mxmsg, ManagerPointer manager) {
    MX_DCHECK_RUN_ON(&io_thread_);
    switch (mxmsg.type()) {
    case types::CONNECTION_WELCOME:
      if (!is_registered_) {
        WelcomeMessage welcome;
        bool ok = false;
        do {
          if (mxmsg.type() != types::CONNECTION_WELCOME)
            break;
          if (!welcome.ParseFromString(mxmsg.message()))
            break;
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
          if (shuts_down_)
            return true; // register_connection refused this connection
          manager->after_connection_registration(this->shared_from_this(), welcome);
          is_registered_ = true;
        }
      } else {
        // TODO logger.error << "received repeated CONNECTION_WELCOME message;
        // shutting down";
        shutdown();
      }
      return true;
      // types::CONNECTION_WELCOME

    case types::HEARTBIT:
      return true;

    default:
      return false;
    }
  }

  /* Sending. One async_write in flight at a time, always for the queue's
   * front; _handle_write pops it, notifies the manager's tracker and starts
   * the next. A short or failed write ends the connection, since the frame
   * boundary would be lost. */
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
      Assert(raw->get_message_buffer().size());
      boost::asio::async_write(
          socket_, raw->get_message_buffer(),
          boost::bind(&Connection::_handle_write, this->shared_from_this(), /* making shared_ptr we ensure *this is
                                                                               not GCed in the middle */
                      boost::asio::placeholders::error, boost::asio::placeholders::bytes_transferred));
    }
  }
  void _handle_write(const boost::system::error_code &error, size_t bytes_transferred) {
    MX_DCHECK_RUN_ON(&io_thread_);
    if (error) {
      MX_LOG(DEBUG, HIGHVERBOSITY,
             TEXT("write error on " + repr((void *)this) + " error=" + repr(error) +
                  " bytes_transferred=" + repr(bytes_transferred)));
    }

    if (outgoing_channel_state_ == ChannelState::BROKEN)
      return;
    if (bytes_transferred == 0) {
      // is it exceptional?
      MX_LOG(DEBUG, HIGHVERBOSITY, TEXT("i have managed to write nothing. Shut down"));
      shutdown();
      return;
    }
    if (error) {
      shutdown();
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
      shutdown();
      return;
    }

    message_sending_notifier_.notify_success(manager_, outgoing_queue_.front());
    outgoing_queue_.pop_front();
    _send_heartbit_later();

    _process_send_queue();
  }

  // On shutdown, offers the unsent queue to the manager (the client moves the
  // entries to another connection) and reports the rest as lost.
  void inline _orphan_outgoing_messages() {
    MX_DCHECK_RUN_ON(&io_thread_);
    if (outgoing_queue_.empty())
      return;
    if (ManagerPointer manager = manager_.lock()) {
      manager->handle_orphaned_outgoing_messages(outgoing_queue_);
      if (outgoing_queue_.empty())
        return;
    }
    // we haven't managed to transfer messages ownership to the manager for
    // eventual resending
    MX_LOG(WARNING, HIGHVERBOSITY,
           TEXT("Connection shutdown on " + repr((void *)this) + ", dropping about " + repr(outgoing_queue_.size()) +
                " outgoing messages"));
    BOOST_FOREACH (typename MessagesBuffer::value_type &entry, outgoing_queue_)
      message_sending_notifier_.notify_error(manager_, entry);
    outgoing_queue_.clear();
  }

public:
  typename ConnectionsManagerTraits::ConnectionManagerPrivateDataInConnection &managers_private_data() {
    return managers_private_data_;
  }
  const typename ConnectionsManagerTraits::ConnectionManagerPrivateDataInConnection &managers_private_data() const {
    return managers_private_data_;
  }

  /* members */
private:
  boost::asio::ip::tcp::socket socket_;
  boost::uint32_t peer_type_;
  boost::uint64_t peer_id_;
  bool is_passive_;

  bool is_living_;
  bool shuts_down_;
  bool is_registered_;
  boost::weak_ptr<ConnectionsManagerImplementation> manager_;
  bool should_send_heartbit_;

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
  boost::uint32_t outgoing_queue_max_size_;

  /* incoming channel */
  boost::shared_ptr<RawMessage> incoming_message_ MX_GUARDED_BY(io_thread_);
  ChannelStateT incoming_channel_state_ MX_GUARDED_BY(io_thread_);

  /* heartbiting */
  boost::asio::deadline_timer send_heartbit_timer_;
  boost::asio::deadline_timer require_heartbit_timer_;
  boost::shared_ptr<const RawMessage> heartbit_message_;

  /* Manager's data pool */
  typename ConnectionsManagerTraits::ConnectionManagerPrivateDataInConnection managers_private_data_;

  /* some functors */
  typename MessagesBufferTraits::SendingResultNotifier message_sending_notifier_;
  typename MessagesBufferTraits::ToBufferRepresentationConverter raw_to_buffer_converter_;
  typename MessagesBufferTraits::SchedulingResultFunctor scheduling_result_type_functor_;
  mx::DefaultConstructingFactory<typename MessagesBufferTraits::SchedulingResultFunctor::result_type>
      scheduling_result_type_default_functor_;

  // friend class ConnectionsManagerImplementation;

}; // class Connection

}; // namespace multiplexer

#endif // MX_MULTIPLEXER_IO_CONNECTION_H_
