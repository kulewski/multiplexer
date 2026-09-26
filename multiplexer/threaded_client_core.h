// ThreadedClient::Core: everything a ThreadedClient is, behind the handle
// the program holds. The io thread keeps a reference to the Core until it
// ends, so a handle destroyed on the io thread itself, from a callback or by
// the last reference dropped there, leaves the Core alive under the
// handlers still running; see ThreadedClient::shutdown. Internal to
// threaded_client.cc; the calls are described in threaded_client.h.
#ifndef MX_MULTIPLEXER_THREADED_CLIENT_CORE_H_
#define MX_MULTIPLEXER_THREADED_CLIENT_CORE_H_

#include <asio/io_service.hpp>
#include <asio/steady_timer.hpp>
#include <chrono>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "lib/mutex.h"
#include "lib/random.h"
#include "lib/thread_checker.h"
#include "multiplexer/threaded_client.h"

namespace multiplexer {

class ThreadedClient::Core {
 public:
  Core(std::uint32_t peer_type, MessageSink on_message);
  // Starts the io thread, which holds `self` until it ends, so that the
  // Core outlives a handle destroyed on that thread; see shutdown().
  void start(const std::shared_ptr<Core>& self);
  void set_search_policy(SearchPolicy answer);
  void set_resolver(BasicClient::Resolver resolver);
  void set_routing(const Routing& routing);
  bool routing_acknowledged();
  bool flush_all(float timeout);
  std::uint64_t instance_id() const { return instance_id_; }
  std::uint32_t peer_type() const { return peer_type_; }
  std::uint64_t random64();  // thread-safe
  bool connect(const std::string& host, std::uint16_t port, float timeout = DEFAULT_TIMEOUT);
  unsigned int connections_count();
  std::size_t watched_ids();
  std::uint64_t retries();
  void send(const MultiplexerMessage& msg);
  void send(const MultiplexerMessage& msg, LanePtr lane);
  void send_all(const MultiplexerMessage& msg);
  void send(const MultiplexerMessage& msg, const ConnectionWrapper& connection);
  unsigned int send(const MultiplexerMessage& msg, float timeout);
  unsigned int send(const MultiplexerMessage& msg, LanePtr lane, float timeout);
  unsigned int send(const MultiplexerMessage& msg, const ConnectionWrapper& connection, float timeout);
  unsigned int send_all(const MultiplexerMessage& msg, float timeout);
  void send_serialized(std::string serialized, LanePtr lane = LanePtr(), float timeout = DEFAULT_TIMEOUT);
  void send_all_serialized(std::string serialized, float timeout = DEFAULT_TIMEOUT);
  unsigned int send_serialized_and_wait(std::string serialized, bool all, float timeout, LanePtr lane = LanePtr());
  void send_serialized_with_callback(std::string serialized, bool all, float timeout, SendCallback done,
                                     LanePtr lane = LanePtr());
  MultiplexerMessage new_message(std::uint32_t type, const std::string& payload);
  void query(const std::string& payload, std::uint32_t type, Callback callback, float timeout = DEFAULT_TIMEOUT,
             LanePtr lane = LanePtr());
  Result query(const std::string& payload, std::uint32_t type, float timeout = DEFAULT_TIMEOUT,
               LanePtr lane = LanePtr());
  void query(const MultiplexerMessage& msg, Callback callback, float timeout = DEFAULT_TIMEOUT,
             LanePtr lane = LanePtr(), Probe probe = PROBE_SEARCH);
  Result query(const MultiplexerMessage& msg, float timeout = DEFAULT_TIMEOUT, LanePtr lane = LanePtr(),
               Probe probe = PROBE_SEARCH);
  void query(const MultiplexerMessage& msg, const ConnectionWrapper& connection, Callback callback,
             float timeout = DEFAULT_TIMEOUT, Probe probe = PROBE_SEARCH);
  Result query(const MultiplexerMessage& msg, const ConnectionWrapper& connection, float timeout = DEFAULT_TIMEOUT,
               Probe probe = PROBE_SEARCH);
  void shutdown();
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
  void _release_callbacks();

  // Sends; see "Sends" in the .cc.
  void _submit_send(std::shared_ptr<const RawMessage> raw, bool all, bool wait, float timeout, SendCallback done,
                    LanePtr lane);
  void _place(const PendingSendPtr& pending) MX_RUN_ON(io_thread_);
  bool _try_place(const PendingSendPtr& pending) MX_RUN_ON(io_thread_);
  void _queued(const PendingSendPtr& pending, const Tracker& tracker) MX_RUN_ON(io_thread_);
  void _wait_any(const PendingSendPtr& pending) MX_RUN_ON(io_thread_);
  void _unwait(const PendingSendPtr& pending) MX_RUN_ON(io_thread_);
  void _refuse(const PendingSendPtr& pending) MX_RUN_ON(io_thread_);
  void _retry(const PendingSendPtr& pending) MX_RUN_ON(io_thread_);
  void _settle(const PendingSendPtr& pending) MX_RUN_ON(io_thread_);
  void _untrack(const PendingSendPtr& pending) MX_RUN_ON(io_thread_);
  void _post_fill() MX_RUN_ON(io_thread_);
  void _fill() MX_RUN_ON(io_thread_);
  void _on_tracked(const std::shared_ptr<SendState>& state, bool written) MX_RUN_ON(io_thread_);
  void _process_tracked() MX_RUN_ON(io_thread_);
  void _expire_at(const PendingSendPtr& pending) MX_RUN_ON(io_thread_);
  void _arm_expiry() MX_RUN_ON(io_thread_);
  void _expire() MX_RUN_ON(io_thread_);
  bool _any_live_connection() MX_RUN_ON(io_thread_);
  BasicClient::BasicScheduledMessageTracker _schedule(const std::shared_ptr<const RawMessage>& raw, const LanePtr& lane,
                                                      ConnectionWrapper* used, bool* refused, float timeout,
                                                      std::uint64_t number = 0) MX_RUN_ON(io_thread_);
  // flush_all() and connect(); see the .cc.
  void _end_flush(const std::shared_ptr<FlushWait>& wait, bool flushed) MX_RUN_ON(io_thread_);
  void _end_connect(const std::shared_ptr<ConnectWaiter>& waiter, bool up) MX_RUN_ON(io_thread_);

  void _io_thread_main();
  template <typename F, typename D>
  void _guarded(F function, D description);
  template <typename F>
  void _post(F function);
  template <typename F>
  auto _call(F function) -> decltype(function());

  void _on_incoming(const BasicClient::IncomingMessagesBuffer::value_type& incoming) MX_RUN_ON(io_thread_);
  void _on_connection(const ConnectionWrapper& connection, bool up) MX_RUN_ON(io_thread_);

  void _start_query(InFlightPtr in_flight, bool keep_deadline) MX_RUN_ON(io_thread_);
  void _wait_for_connection(const InFlightPtr& in_flight) MX_RUN_ON(io_thread_);
  void _restart_waiting_queries() MX_RUN_ON(io_thread_);
  void _advance(InFlightPtr in_flight, const IncomingMessage& incoming) MX_RUN_ON(io_thread_);
  void _search(InFlightPtr in_flight) MX_RUN_ON(io_thread_);
  void _direct(InFlightPtr in_flight, const IncomingMessage& ping) MX_RUN_ON(io_thread_);
  bool _answered(const InFlightPtr& in_flight, const ConnectionWrapper& connection) MX_RUN_ON(io_thread_);
  void _lost(InFlightPtr in_flight) MX_RUN_ON(io_thread_);
  void _arm(InFlightPtr in_flight, float timeout) MX_RUN_ON(io_thread_);
  float _stage_timeout(const InFlightPtr& in_flight) const MX_RUN_ON(io_thread_);
  void _on_deadline(InFlightPtr in_flight, unsigned int generation, const asio::error_code& error)
      MX_RUN_ON(io_thread_);
  void _finish(InFlightPtr in_flight, Outcome outcome, const IncomingMessage* reply) MX_RUN_ON(io_thread_);
  void _track(InFlightPtr in_flight, std::uint64_t id) MX_RUN_ON(io_thread_);
  void _remember_finished(std::uint64_t id) MX_RUN_ON(io_thread_);
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
  std::vector<InFlightPtr> in_flight_ MX_GUARDED_BY(io_thread_);  // every query, tracked by id or waiting
  // The ids of recently finished queries, so that a late reply to one is
  // recognised and dropped instead of reaching on_message: a bounded ring,
  // small because a late reply arrives within a timeout of its query, and
  // one that slips through only costs on_message an unexpected message.
  static const std::size_t REMEMBERED_FINISHED_IDS = 1024;
  std::deque<std::uint64_t> finished_order_ MX_GUARDED_BY(io_thread_);
  std::unordered_set<std::uint64_t> finished_ids_ MX_GUARDED_BY(io_thread_);
  // The caller's callbacks, released once nothing can call them any more
  // (_release_callbacks).
  MessageSink on_message_ MX_GUARDED_BY(io_thread_);
  SearchPolicy search_policy_ MX_GUARDED_BY(io_thread_);
  // Sends made while no connection was live, waiting for one to come up, in
  // the order made; what waits for room waits in BasicClient.
  std::deque<PendingSendPtr> waiting_any_ MX_GUARDED_BY(io_thread_);
  std::uint64_t retries_ MX_GUARDED_BY(io_thread_) = 0;  // see retries(): this client's part
  bool fill_posted_ MX_GUARDED_BY(io_thread_) = false;   // a _fill() is on its way
  // Queries that found no connection live, waiting for one.
  std::deque<InFlightPtr> waiting_queries_ MX_GUARDED_BY(io_thread_);
  // Flushing sends by the state of each copy queued: written or lost, the
  // connection says so through the tracked observer, and the events wait in
  // tracked_events_ for the one pass per loop turn that handles them.
  std::unordered_map<const SendState*, PendingSendPtr> tracked_sends_ MX_GUARDED_BY(io_thread_);
  std::vector<std::pair<std::shared_ptr<SendState>, bool>> tracked_events_ MX_GUARDED_BY(io_thread_);
  bool tracked_posted_ MX_GUARDED_BY(io_thread_) = false;
  // The deadlines of the sends that have one running (flushing sends, and
  // those waiting for a connection), as a heap with the earliest on top,
  // and the one timer set to it. Entries of sends that ended are skipped
  // when met, and cleared out once they are half of it.
  std::vector<Expiring> expiring_ MX_GUARDED_BY(io_thread_);
  std::size_t expiring_compact_at_ MX_GUARDED_BY(io_thread_) = 64;
  std::unique_ptr<asio::steady_timer> expiry_timer_ MX_GUARDED_BY(io_thread_);
  std::chrono::steady_clock::time_point expiry_armed_ MX_GUARDED_BY(io_thread_) =
      std::chrono::steady_clock::time_point::max();
  std::vector<std::shared_ptr<FlushWait>> flushes_ MX_GUARDED_BY(io_thread_);       // flush_all() calls waiting
  std::vector<std::shared_ptr<ConnectWaiter>> connects_ MX_GUARDED_BY(io_thread_);  // connect() calls waiting
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

}  // namespace multiplexer

#endif  // MX_MULTIPLEXER_THREADED_CLIENT_CORE_H_
