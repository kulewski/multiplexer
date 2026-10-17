// ThreadedClient::Core: everything a ThreadedClient is, behind the handle
// the program holds. The io thread keeps a reference to the Core until it
// ends, so a handle destroyed on the io thread itself, from a callback or by
// the last reference dropped there, leaves the Core alive under the
// handlers still running; see ThreadedClient::shutdown. Internal to
// threaded_client.cc and its parts, threaded_client_io.cc and
// threaded_client_query.cc; the calls are described in threaded_client.h.
#ifndef MX_MULTIPLEXER_THREADED_CLIENT_CORE_H_
#define MX_MULTIPLEXER_THREADED_CLIENT_CORE_H_

#include <asio/io_service.hpp>
#include <asio/steady_timer.hpp>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <exception>
#include <functional>
#include <future>
#include <list>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "lib/logging/logging.h"
#include "lib/mutex.h"
#include "lib/random.h"
#include "lib/thread_checker.h"
#include "multiplexer/threaded_client.h"

namespace multiplexer {

class ThreadedClient::Core : public std::enable_shared_from_this<ThreadedClient::Core> {
 public:
  Core(std::uint32_t peer_type, MessageSink on_message);
  // Starts the io thread, which holds `self` until it ends, so that the
  // Core outlives a handle destroyed on that thread; see shutdown().
  void start(const std::shared_ptr<Core>& self);
  void set_search_policy(SearchPolicy answer);
  void set_drop_observer(DropObserver observer);
  std::uint64_t dropped();
  void set_resolver(BasicClient::Resolver resolver);
  void set_routing(const Routing& routing, BasicClient::AcknowledgedCallback acknowledged);
  void set_key_filters(const BasicClient::KeyFilters& filters, BasicClient::AcknowledgedCallback acknowledged);
  void set_batching(std::uint32_t type, const std::string* key, const BasicClient::Batching& batching);
  void clear_batching(std::uint32_t type, const std::string* key);
  bool routing_acknowledged();
  std::size_t held_for_a_connection();
  bool flush_all(float timeout);
  void flush_all_with_callback(float timeout, FlushCallback done);
  std::uint64_t instance_id() const { return instance_id_; }
  std::uint32_t peer_type() const { return peer_type_; }
  std::uint64_t random64();  // thread-safe
  bool connect(const std::string& host, std::uint16_t port, float timeout = DEFAULT_TIMEOUT);
  unsigned int connect_all(const std::vector<std::pair<std::string, std::uint16_t>>& addresses,
                           float timeout = DEFAULT_TIMEOUT);
  bool disconnect(const std::string& host, std::uint16_t port);
  unsigned int connections_count();
  std::size_t watched_ids();
  void forget_finished_ids();
  std::uint64_t retries();
  std::size_t waiting_queries();
  std::size_t waiting_messages();
  void send(const MultiplexerMessage& msg);
  void send(const MultiplexerMessage& msg, LanePtr lane, SendCallback done);
  void send_all(const MultiplexerMessage& msg, SendCallback done);
  void send(const MultiplexerMessage& msg, const ConnectionWrapper& connection, SendCallback done);
  unsigned int send(const MultiplexerMessage& msg, float timeout);
  unsigned int send(const MultiplexerMessage& msg, LanePtr lane, float timeout);
  unsigned int send(const MultiplexerMessage& msg, const ConnectionWrapper& connection, float timeout);
  unsigned int send_all(const MultiplexerMessage& msg, float timeout);
  void send_serialized(std::string serialized, std::uint64_t id, std::uint32_t type, LanePtr lane, float timeout,
                       SendCallback done);
  void send_all_serialized(std::string serialized, std::uint64_t id, std::uint32_t type, float timeout,
                           SendCallback done);
  unsigned int send_serialized_and_wait(std::string serialized, std::uint64_t id, std::uint32_t type, bool all,
                                        float timeout, LanePtr lane, bool* not_connected);
  void send_serialized_with_callback(std::string serialized, std::uint64_t id, std::uint32_t type, bool all,
                                     float timeout, SendCallback done, LanePtr lane = LanePtr());
  void send_serialized_and_notify(std::string serialized, std::uint64_t id, std::uint32_t type, bool all, float timeout,
                                  FlushedCallback done, LanePtr lane);
  MultiplexerMessage new_message(std::uint32_t type, const std::string& payload);
  void query(const std::string& payload, std::uint32_t type, Callback callback, float timeout = DEFAULT_TIMEOUT,
             LanePtr lane = LanePtr(), ReceivedCallback received = ReceivedCallback());
  Result query(const std::string& payload, std::uint32_t type, float timeout = DEFAULT_TIMEOUT,
               LanePtr lane = LanePtr(), ReceivedCallback received = ReceivedCallback());
  void query(const MultiplexerMessage& msg, Callback callback, float timeout = DEFAULT_TIMEOUT,
             LanePtr lane = LanePtr(), ReceivedCallback received = ReceivedCallback());
  Result query(const MultiplexerMessage& msg, float timeout = DEFAULT_TIMEOUT, LanePtr lane = LanePtr(),
               ReceivedCallback received = ReceivedCallback());
  void query(const MultiplexerMessage& msg, const ConnectionWrapper& connection, Callback callback,
             float timeout = DEFAULT_TIMEOUT, ReceivedCallback received = ReceivedCallback());
  Result query(const MultiplexerMessage& msg, const ConnectionWrapper& connection, float timeout = DEFAULT_TIMEOUT,
               ReceivedCallback received = ReceivedCallback());
  void shutdown(float timeout);
  bool orphaned() const { return basic_client_->orphaned(); }
  LogSummary& drop_lines() { return basic_client_->drop_lines(); }  // the io thread only

 private:
  struct InFlight;
  struct PendingSend;
  struct Expiring;
  struct FlushWait;
  struct ConnectWaiter;
  typedef std::shared_ptr<PendingSend> PendingSendPtr;
  typedef std::shared_ptr<InFlight> InFlightPtr;
  typedef BasicClient::BasicScheduledMessageTracker Tracker;
  void _orphan_teardown();
  void _teardown() MX_RUN_ON(io_thread_);
  void _write_out() MX_RUN_ON(io_thread_);
  bool _writing_out_here();
  void _release_callbacks();

  // A caller's whole message framed, its empty id and sender filled.
  std::shared_ptr<const RawMessage> _stamped(const MultiplexerMessage& msg);
  // Sends; see "Sends" in the .cc.
  void _submit_send(std::shared_ptr<const RawMessage> raw, bool all, bool wait, float timeout, SendCallback done,
                    LanePtr lane, FlushedCallback flushed = FlushedCallback());
  void _place(const PendingSendPtr& pending) MX_RUN_ON(io_thread_);
  void _refuse(const PendingSendPtr& pending) MX_RUN_ON(io_thread_);
  void _settle(const PendingSendPtr& pending, unsigned int written, bool not_connected) MX_RUN_ON(io_thread_);
  void _post_fill() MX_RUN_ON(io_thread_);
  void _fill() MX_RUN_ON(io_thread_);
  void _expire_at(const PendingSendPtr& pending) MX_RUN_ON(io_thread_);
  void _arm_expiry() MX_RUN_ON(io_thread_);
  void _expire() MX_RUN_ON(io_thread_);
  bool _any_live_connection() MX_RUN_ON(io_thread_);
  BasicClient::BasicScheduledMessageTracker _schedule(const std::shared_ptr<const RawMessage>& raw, const LanePtr& lane,
                                                      ConnectionWrapper* used, bool* refused, float timeout,
                                                      std::uint64_t number = 0) MX_RUN_ON(io_thread_);
  // flush_all() and connect(); see the .cc.
  bool _begin_flush_all(float timeout, const FlushCallback& done);
  void _end_flush(const std::shared_ptr<FlushWait>& wait, bool flushed) MX_RUN_ON(io_thread_);
  std::future<bool> _start_connect(const std::string& host, std::uint16_t port, float timeout);
  void _end_connect(const std::shared_ptr<ConnectWaiter>& waiter, bool up) MX_RUN_ON(io_thread_);

  void _io_thread_main();
  void _wait_ended();
  template <typename F, typename D>
  void _guarded(F function, D description);
  template <typename F>
  void _post(F function);
  template <typename F>
  bool _post_unless_stopped(F function) MX_EXCLUDES(lifecycle_mutex_);
  void _end_posting() MX_EXCLUDES(lifecycle_mutex_);
  template <typename F>
  auto _call(F function) -> decltype(function());

  void _on_incoming(const BasicClient::IncomingMessagesBuffer::value_type& incoming) MX_RUN_ON(io_thread_);
  void _on_connection(const ConnectionWrapper& connection, bool up) MX_RUN_ON(io_thread_);
  void _queries_on_connection(const ConnectionWrapper& connection, bool up) MX_RUN_ON(io_thread_);

  void _start_query(InFlightPtr in_flight, bool keep_deadline) MX_RUN_ON(io_thread_);
  void _wait_for_connection(const InFlightPtr& in_flight, int resume) MX_RUN_ON(io_thread_);
  void _restart_waiting_queries() MX_RUN_ON(io_thread_);
  void _clear_out_waiting() MX_RUN_ON(io_thread_);
  void _advance(InFlightPtr in_flight, const IncomingMessage& incoming) MX_RUN_ON(io_thread_);
  void _acknowledged(const InFlightPtr& in_flight, const MultiplexerMessage& msg) MX_RUN_ON(io_thread_);
  void _search(InFlightPtr in_flight, bool keep_deadline) MX_RUN_ON(io_thread_);
  void _direct(InFlightPtr in_flight, const IncomingMessage& ping) MX_RUN_ON(io_thread_);
  void _send_direct(InFlightPtr in_flight, const ConnectionWrapper& preferred, bool keep_deadline)
      MX_RUN_ON(io_thread_);
  bool _answered(const InFlightPtr& in_flight, const ConnectionWrapper& connection) MX_RUN_ON(io_thread_);
  void _nobody(InFlightPtr in_flight) MX_RUN_ON(io_thread_);
  void _wait_late(InFlightPtr in_flight, bool direct) MX_RUN_ON(io_thread_);
  void _refused(InFlightPtr in_flight, std::uint64_t id) MX_RUN_ON(io_thread_);
  void _follow_handover(InFlightPtr in_flight) MX_RUN_ON(io_thread_);
  void _lost(InFlightPtr in_flight) MX_RUN_ON(io_thread_);
  void _arm(InFlightPtr in_flight, float timeout) MX_RUN_ON(io_thread_);
  float _stage_timeout(const InFlightPtr& in_flight) const MX_RUN_ON(io_thread_);
  void _on_deadline(InFlightPtr in_flight, unsigned int generation, const asio::error_code& error)
      MX_RUN_ON(io_thread_);
  void _finish(InFlightPtr in_flight, Outcome outcome, const IncomingMessage* reply) MX_RUN_ON(io_thread_);
  void _finish_every_query() MX_RUN_ON(io_thread_);
  void _track(InFlightPtr in_flight, std::uint64_t id) MX_RUN_ON(io_thread_);
  void _remember_finished(std::uint64_t id, std::chrono::steady_clock::time_point until) MX_RUN_ON(io_thread_);
  void _on_unmatched(const IncomingMessage& incoming) MX_RUN_ON(io_thread_);

  const std::uint32_t peer_type_;
  // Held by pointer so that an orphan (a client inherited across a fork,
  // see BasicClient::orphaned) can leak it instead of running asio's
  // destructors with the parent's locks in an unknown state.
  std::unique_ptr<asio::io_service> io_service_holder_;
  asio::io_service& io_service_;
  std::unique_ptr<asio::io_service::work> work_;
  std::shared_ptr<BasicClient> basic_client_;
  const std::uint64_t instance_id_;

  mx::ThreadChecker io_thread_{mx::ThreadChecker::BIND_LATER};
  std::unordered_map<std::uint64_t, InFlightPtr> by_id_ MX_GUARDED_BY(io_thread_);
  // Every query, tracked by id or waiting, in the order they started; a
  // query keeps its place, so that its end takes it out in O(1).
  std::list<InFlightPtr> in_flight_ MX_GUARDED_BY(io_thread_);
  // The ids of finished queries, so that a late reply to one is recognised
  // and dropped instead of reaching on_message, where a threaded server
  // would queue it as a request: each kept until its reply can no longer
  // be expected, twice the longer of its query's timeout and
  // DEFAULT_TIMEOUT after the query ended, in the order they ended, and
  // at most FINISHED_IDS_KEPT of them, the oldest going first, which
  // bounds the memory, about 64 bytes an id.
  static const std::size_t FINISHED_IDS_KEPT = 1 << 17;
  std::deque<std::pair<std::uint64_t, std::chrono::steady_clock::time_point>> finished_order_
      MX_GUARDED_BY(io_thread_);  // each id and until when it is kept
  std::unordered_set<std::uint64_t> finished_ids_ MX_GUARDED_BY(io_thread_);
  // The caller's callbacks, released once nothing can call them any more
  // (_release_callbacks).
  MessageSink on_message_ MX_GUARDED_BY(io_thread_);
  SearchPolicy search_policy_ MX_GUARDED_BY(io_thread_);
  std::uint64_t retries_ MX_GUARDED_BY(io_thread_) = 0;  // see retries(): the queries' part
  bool fill_posted_ MX_GUARDED_BY(io_thread_) = false;   // a _fill() is on its way
  // Queries that found no connection live, waiting for one, and of them
  // how many ended meanwhile and are not cleared out yet.
  std::deque<InFlightPtr> waiting_queries_ MX_GUARDED_BY(io_thread_);
  std::size_t waiting_ended_ MX_GUARDED_BY(io_thread_) = 0;
  // The deadlines of the flushing sends, as a heap with the
  // earliest on top, and the one timer set to it. Entries of sends that
  // ended are skipped when met, and cleared out once they are half of it.
  std::vector<Expiring> expiring_ MX_GUARDED_BY(io_thread_);
  std::size_t expiring_compact_at_ MX_GUARDED_BY(io_thread_) = 64;
  std::unique_ptr<asio::steady_timer> expiry_timer_ MX_GUARDED_BY(io_thread_);
  std::chrono::steady_clock::time_point expiry_armed_ MX_GUARDED_BY(io_thread_) =
      std::chrono::steady_clock::time_point::max();
  std::vector<std::shared_ptr<FlushWait>> flushes_ MX_GUARDED_BY(io_thread_);       // flush_all() calls waiting
  std::vector<std::shared_ptr<ConnectWaiter>> connects_ MX_GUARDED_BY(io_thread_);  // connect() calls waiting
  // shutdown() was called: no new query, send, connect or routing, but a
  // send made on the io thread meanwhile (_writing_out_here). What was sent
  // before is written meanwhile, and what the io thread sent since
  // (drain_, until drain_timer_), then the connections close (_teardown),
  // once: torn_down_.
  bool shut_down_ MX_GUARDED_BY(io_thread_) = false;
  BasicClient::FlushPtr drain_ MX_GUARDED_BY(io_thread_);
  std::unique_ptr<asio::steady_timer> drain_timer_ MX_GUARDED_BY(io_thread_);
  bool torn_down_ MX_GUARDED_BY(io_thread_) = false;

  mx::Mutex random_mutex_;
  mx::Random64 random_ MX_GUARDED_BY(random_mutex_);
  // Whether set_batching() was ever called: the callers' threads then mark
  // the frames they make for it (_stamped); a hint, a frame made unmarked
  // read on the io thread instead.
  std::atomic<bool> batching_{false};

  // shutdown() is the only thing that touches the thread. Every other entry
  // point posts through _post_unless_stopped(), which counts itself in
  // posting_ and then reads stopped_, while shutdown() sets stopped_ and
  // then waits for posting_ to be 0 before it posts its own handler: a
  // post that found the client up is queued ahead of that handler and runs
  // before the loop ends, and nothing posts to a stopped loop. No lock on
  // the way: posts from several threads go on side by side.
  std::atomic<bool> stopped_{false};
  std::atomic<int> posting_{0};
  mx::Mutex lifecycle_mutex_;
  std::condition_variable_any posted_;  // posting_ fell to 0 once stopped_ was set
  std::thread thread_;
  // The io thread's end, told once it let go of the Core, which may go
  // with it, hence an object of its own: what a shutdown() that found the
  // shutdown begun, on another thread or on the io thread, waits for
  // (_wait_ended), as the one that began it waits for the thread.
  struct End {
    mx::Mutex mutex;
    std::condition_variable_any changed;
    bool ended MX_GUARDED_BY(mutex) = false;
  };
  const std::shared_ptr<End> end_ = std::make_shared<End>();
};

// The structs and member templates the parts of ThreadedClient share.

// A deadline in expiring_, a heap whose top is the earliest deadline.
struct ThreadedClient::Core::Expiring {
  std::chrono::steady_clock::time_point deadline;
  std::weak_ptr<PendingSend> send;
  // The heap's order, which puts the largest on top: a later deadline is
  // the smaller.
  bool operator<(const Expiring& other) const { return deadline > other.deadline; }
};

// flush_all(): BasicClient's flush, which counts what waits for room and
// what this client holds waiting for a connection, with the timer of the
// call's timeout and the promise its caller waits on.
struct ThreadedClient::Core::FlushWait {
  BasicClient::FlushPtr flush;
  FlushCallback done;
  std::unique_ptr<asio::steady_timer> timer;
};

// connect(): a connection on its way, until the observer says up or down,
// or the timer runs out.
struct ThreadedClient::Core::ConnectWaiter {
  ConnectionWrapper connection;
  std::shared_ptr<std::promise<bool>> done;
  std::unique_ptr<asio::steady_timer> timer;
};

// Runs a callback of the caller's on the io thread. One that throws is
// logged, with `description()` of what it was called for, and the client
// goes on: the exception would otherwise unwind through the handler that
// read the frame and leave that connection unread. Costs nothing while
// nothing throws.
template <typename F, typename D>
void ThreadedClient::Core::_guarded(F function, D description) {
  try {
    function();
  } catch (const std::exception& e) {
    MX_LOG(ERROR, LOWVERBOSITY, CTX("ThreadedClient") TEXT(description() + " threw: " + e.what()));
  }
}

template <typename F>
void ThreadedClient::Core::_post(F function) {
  io_service_.post(function);
}

// Posts `function` unless shutdown() came first, and says which. A call
// counts itself in posting_ before it reads stopped_, and shutdown() sets
// stopped_ before it reads posting_, all sequentially consistent, so one of
// the two sees the other: a call that found the client up has posted
// before shutdown() posts its own handler, which the loop runs before it
// can end, and no caller waits on a loop that has ended. The first read
// keeps the calls made after shutdown() off posting_, which shutdown()
// waits on. The function is moved in, so that no copy of the caller's
// callbacks runs while shutdown() may wait; what a call prepares, it
// prepares before.
template <typename F>
bool ThreadedClient::Core::_post_unless_stopped(F function) {
  if (stopped_.load()) {
    return false;
  }
  posting_.fetch_add(1);
  struct Posting {
    Core* core;
    ~Posting() { core->_end_posting(); }
  } posting{this};
  if (stopped_.load()) {
    return false;
  }
  io_service_.post(std::move(function));
  return true;
}

}  // namespace multiplexer

#endif  // MX_MULTIPLEXER_THREADED_CLIENT_CORE_H_
