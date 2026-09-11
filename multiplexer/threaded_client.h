// A client with a thread of its own: the connections live on an io thread
// that runs all the time, so heartbeats flow and reconnects happen whether
// or not the program is calling in. Any thread may send and query.
//
// This is the general form of what a peer needs from the library: the io
// thread waits on the network, on messages to hand to a processing thread,
// and on work posted by other threads. A peer built on it need not be
// passive in the rules file (see docs/handshake.md); the synchronous Client
// remains for programs that prefer no thread.
//
// Queries run as a state machine on the io thread, the same three stages as
// Client::_query (request; search for a backend on every connection; the
// request again to the backend found), each stage with its own deadline
// timer. A connection dying under a query does not cost the query its
// timeout: the request is sent again through another connection, or as
// soon as one comes back, the way the synchronous Client does inside a
// call. Many queries may be in flight at once, from any number of threads:
// replies are matched by the ids they reference, never by arrival order.
// The asynchronous form calls back on the io thread; the synchronous form
// is the same thing behind a future.
//
// Everything else that arrives is handled here or handed to the on_message
// callback given at construction: a late reply to a query that already
// ended is dropped (the ids of recently finished queries are remembered, a
// bounded number of them), REQUEST_RECEIVED for an unknown id is dropped, a
// PING without references is answered, and the rest, events and requests
// addressed to this peer, go to on_message, or are logged and dropped when
// there is none. Nothing is ever queued for a reader that may never come.
//
// Threading, as declared: everything under io_thread_ runs on the io thread
// only, reached from other threads through io_service::post. Callbacks and
// on_message run on the io thread and must return quickly; they may call
// the asynchronous query() and send() but not the blocking query(), which
// throws std::logic_error there rather than deadlock the thread it runs on.
#ifndef MX_MULTIPLEXER_THREADED_CLIENT_H_
#define MX_MULTIPLEXER_THREADED_CLIENT_H_

#include <deque>
#include <functional>
#include <future>
#include <memory>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <boost/asio/deadline_timer.hpp>
#include <boost/asio/io_service.hpp>
#include <boost/cstdint.hpp>
#include <boost/shared_ptr.hpp>

#include "lib/mutex.h"
#include "lib/random.h"
#include "lib/thread_checker.h"
#include "multiplexer/basic_client.h"
#include "multiplexer/client.h"
#include "multiplexer/defaults.h"

namespace multiplexer {

class ThreadedClient : public ExceptionDefinitions {
public:
  // How a query ended. REPLIED: `reply` holds the answer. TIMED_OUT: a stage
  // ran out of time. FAILED: every multiplexer reported no backend of the
  // type, or the backend found could not be reached. NOT_CONNECTED: no live
  // connection to send through. SHUT_DOWN: shutdown() ran first.
  enum Outcome { REPLIED, TIMED_OUT, FAILED, NOT_CONNECTED, SHUT_DOWN };
  struct Result {
    Outcome outcome;
    IncomingMessage reply; // set when outcome == REPLIED
    // The reply, or the exception the synchronous Client would have thrown.
    const IncomingMessage &check() const;
  };
  typedef std::function<void(const Result &)> Callback;
  typedef std::function<void(const IncomingMessage &)> MessageSink;

  // `on_message` receives, on the io thread, every message that is not a
  // reply to a query or one of the protocol's own (see the file comment);
  // without it such messages are logged and dropped.
  explicit ThreadedClient(boost::uint32_t peer_type, MessageSink on_message = MessageSink());
  ~ThreadedClient(); // shutdown() if not done, then joins the io thread

  boost::uint64_t instance_id() const { return instance_id_; }
  boost::uint32_t peer_type() const { return peer_type_; }
  boost::uint64_t random64(); // thread-safe

  // Connects and waits up to `timeout` for the handshake; true when the
  // connection is registered. False is not final: the io thread keeps
  // reconnecting every AUTO_RECONNECT_TIME seconds on its own.
  bool connect(const std::string &host, boost::uint16_t port, float timeout = DEFAULT_TIMEOUT);
  unsigned int connections_count();

  // Sending. The message must carry its id and from; new_message() fills
  // those in. send() queues it on one live connection (round robin),
  // send_all() on every one, and both return at once: the write happens on
  // the io thread right after, so they are safe from callbacks. With no
  // live connection the message waits, on the io thread, for one to come
  // up within DEFAULT_TIMEOUT and is dropped with a warning after that.
  void send(const MultiplexerMessage &msg);
  void send_all(const MultiplexerMessage &msg);
  // The flushing forms, from any thread but the io thread: wait until the
  // message reached the socket, on one connection (sent again through
  // another if the first dies under it, the way the synchronous Client's
  // flush does) or on every connection, or until `timeout` passes. Return
  // the number of connections it was written to; 0 means none in time.
  unsigned int send(const MultiplexerMessage &msg, float timeout);
  unsigned int send_all(const MultiplexerMessage &msg, float timeout);
  // The same three for an already serialized MultiplexerMessage (the
  // Python side).
  void send_serialized(std::string serialized);
  void send_all_serialized(std::string serialized);
  unsigned int send_serialized_and_wait(std::string serialized, bool all, float timeout);
  // The flushing send with a callback instead of a wait, safe from any
  // thread including the io thread: `done(written)` runs on the io thread
  // once the message reached the socket(s), or with 0 when `timeout`
  // passed or the client shut down first. What an asyncio layer awaits.
  typedef std::function<void(unsigned int)> SendCallback;
  void send_serialized_with_callback(std::string serialized, bool all, float timeout, SendCallback done);
  MultiplexerMessage new_message(boost::uint32_t type, const std::string &payload);

  // A request with a reply, see the file comment. The callback runs on the
  // io thread. The blocking form throws std::logic_error when called on the
  // io thread, that is from a callback, where it would deadlock.
  void query(const std::string &payload, boost::uint32_t type, Callback callback, float timeout = DEFAULT_TIMEOUT);
  Result query(const std::string &payload, boost::uint32_t type, float timeout = DEFAULT_TIMEOUT);

  // Ends every in-flight query with SHUT_DOWN, closes the connections and
  // stops the io thread. Idempotent; the destructor calls it.
  void shutdown();
  // Whether this client was inherited across a fork: every call then
  // throws UsedAfterFork; see BasicClient::orphaned.
  bool orphaned() const { return basic_client_->orphaned(); }

private:
  struct InFlight;
  struct PendingSend;
  typedef std::shared_ptr<PendingSend> PendingSendPtr;
  void _orphan_teardown();
  void _submit_send(boost::shared_ptr<const RawMessage> raw, bool all, bool wait, float timeout, SendCallback done);
  void _attempt_send(const PendingSendPtr &pending) MX_RUN_ON(io_thread_);
  void _advance_sends() MX_RUN_ON(io_thread_);
  typedef boost::shared_ptr<InFlight> InFlightPtr;

  void _io_thread_main();
  template <typename F> void _post(F function);
  template <typename F> auto _call(F function) -> decltype(function());

  void _on_incoming(const BasicClient::IncomingMessagesBuffer::value_type &incoming) MX_RUN_ON(io_thread_);
  void _on_connection(const ConnectionWrapper &connection, bool up) MX_RUN_ON(io_thread_);

  void _start_query(InFlightPtr in_flight, bool keep_deadline) MX_RUN_ON(io_thread_);
  void _advance(InFlightPtr in_flight, const IncomingMessage &incoming) MX_RUN_ON(io_thread_);
  void _search(InFlightPtr in_flight) MX_RUN_ON(io_thread_);
  void _direct(InFlightPtr in_flight, const IncomingMessage &ping) MX_RUN_ON(io_thread_);
  void _arm(InFlightPtr in_flight, float timeout) MX_RUN_ON(io_thread_);
  void _on_deadline(InFlightPtr in_flight, unsigned int generation, const boost::system::error_code &error)
      MX_RUN_ON(io_thread_);
  void _finish(InFlightPtr in_flight, Outcome outcome, const IncomingMessage *reply) MX_RUN_ON(io_thread_);
  void _track(InFlightPtr in_flight, boost::uint64_t id) MX_RUN_ON(io_thread_);
  void _remember_finished(boost::uint64_t id) MX_RUN_ON(io_thread_);
  void _on_unmatched(const IncomingMessage &incoming) MX_RUN_ON(io_thread_);

  const boost::uint32_t peer_type_;
  // Held by pointer so that an orphan (a client inherited across a fork,
  // see BasicClient::orphaned) can leak it instead of running asio's
  // destructors with the parent's locks in an unknown state.
  std::unique_ptr<boost::asio::io_service> io_service_holder_;
  boost::asio::io_service &io_service_;
  std::unique_ptr<boost::asio::io_service::work> work_;
  boost::shared_ptr<BasicClient> basic_client_;
  const boost::uint64_t instance_id_;

  mx::ThreadChecker io_thread_{mx::ThreadChecker::BIND_LATER};
  std::unordered_map<boost::uint64_t, InFlightPtr> by_id_ MX_GUARDED_BY(io_thread_);
  std::vector<InFlightPtr> in_flight_ MX_GUARDED_BY(io_thread_); // every query, tracked by id or waiting
  // The ids of recently finished queries, so that a late reply to one is
  // recognised and dropped instead of reaching on_message: a bounded ring,
  // small because a late reply arrives within a timeout of its query, and
  // one that slips through only costs on_message an unexpected message.
  static const std::size_t REMEMBERED_FINISHED_IDS = 1024;
  std::deque<boost::uint64_t> finished_order_ MX_GUARDED_BY(io_thread_);
  std::unordered_set<boost::uint64_t> finished_ids_ MX_GUARDED_BY(io_thread_);
  const MessageSink on_message_;
  // Sends not yet written: waiting for a connection, or flushing ones
  // waiting for their write. Polled every few milliseconds by
  // send_timer_ while any exist, and on every connection coming up.
  std::vector<PendingSendPtr> pending_sends_ MX_GUARDED_BY(io_thread_);
  std::unique_ptr<boost::asio::deadline_timer> send_timer_ MX_GUARDED_BY(io_thread_);
  bool shut_down_ MX_GUARDED_BY(io_thread_) = false;

  mx::Mutex random_mutex_;
  mx::Random64 random_ MX_GUARDED_BY(random_mutex_);

  // shutdown() is the only thing that touches the thread; every other entry
  // point checks stopped_ first so that nothing posts to a stopped loop.
  mx::Mutex lifecycle_mutex_;
  bool stopped_ MX_GUARDED_BY(lifecycle_mutex_) = false;
  std::thread thread_;
  bool _stopped() MX_EXCLUDES(lifecycle_mutex_) {
    mx::MutexLock lock(lifecycle_mutex_);
    return stopped_;
  }
};

} // namespace multiplexer

#endif // MX_MULTIPLEXER_THREADED_CLIENT_H_
