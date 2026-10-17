// ThreadedClient: the handle, the io thread's life and the calls from
// other threads, shutdown() among them. The io thread's work is in
// threaded_client_io.cc and the query state machine in
// threaded_client_query.cc. See the header for the design.
#include "multiplexer/threaded_client.h"

#include <algorithm>
#include <asio/ip/tcp.hpp>
#include <chrono>
#include <future>
#include <stdexcept>

#include "lib/logging/logging.h"
#include "lib/repr.h"
#include "lib/seconds.h"
#include "multiplexer/threaded_client_core.h"

namespace multiplexer {

using mx::repr;

const IncomingMessage& ThreadedClient::Result::check() const {
  switch (outcome) {
    case REPLIED:
      return reply;
    case TIMED_OUT:
      MXTHROW(OperationTimedOut());
    case FAILED:
      MXTHROW(OperationFailed());
    case NOT_CONNECTED:
    case SHUT_DOWN:
      MXTHROW(NotConnected());
  }
  MXTHROW(OperationFailed());  // unreachable
}

// The handle the program holds; everything else is the Core's, which the io
// thread co-owns.
ThreadedClient::ThreadedClient(std::uint32_t peer_type, MessageSink on_message)
    : core_(std::make_shared<Core>(peer_type, on_message)), peer_type_(peer_type), instance_id_(core_->instance_id()) {
  core_->start(core_);
}

// shutdown() with its default drain. On the io thread itself, in a
// callback or where the last reference to a Python client was dropped, it
// leaves the thread to end on its own and the Core with it; an orphan gets
// the orphan teardown.
ThreadedClient::~ThreadedClient() {
  if (core_) {  // none once moved from
    core_->shutdown(CLOSE_FLUSH_SECONDS);
  }
}

ThreadedClient::ThreadedClient(ThreadedClient&& other) noexcept
    : core_(std::move(other.core_)), peer_type_(other.peer_type_), instance_id_(other.instance_id_) {}

void ThreadedClient::set_search_policy(SearchPolicy answer) { core_->set_search_policy(answer); }
void ThreadedClient::set_drop_observer(DropObserver observer) { core_->set_drop_observer(observer); }
std::uint64_t ThreadedClient::dropped() { return core_->dropped(); }
void ThreadedClient::set_resolver(BasicClient::Resolver resolver) { core_->set_resolver(resolver); }
void ThreadedClient::set_routing(const Routing& routing, BasicClient::AcknowledgedCallback acknowledged) {
  core_->set_routing(routing, std::move(acknowledged));
}
void ThreadedClient::set_key_filters(const BasicClient::KeyFilters& filters,
                                     BasicClient::AcknowledgedCallback acknowledged) {
  core_->set_key_filters(filters, std::move(acknowledged));
}
bool ThreadedClient::routing_acknowledged() { return core_->routing_acknowledged(); }
std::size_t ThreadedClient::held_for_a_connection() { return core_->held_for_a_connection(); }
bool ThreadedClient::flush_all(float timeout) { return core_->flush_all(timeout); }
void ThreadedClient::flush_all_with_callback(float timeout, FlushCallback done) {
  core_->flush_all_with_callback(timeout, done);
}
std::uint64_t ThreadedClient::random64() { return core_->random64(); }
bool ThreadedClient::connect(const std::string& host, std::uint16_t port, float timeout) {
  return core_->connect(host, port, timeout);
}
unsigned int ThreadedClient::connect_all(const std::vector<std::pair<std::string, std::uint16_t>>& addresses,
                                         float timeout) {
  return core_->connect_all(addresses, timeout);
}
bool ThreadedClient::disconnect(const std::string& host, std::uint16_t port) { return core_->disconnect(host, port); }
unsigned int ThreadedClient::connections_count() { return core_->connections_count(); }
std::size_t ThreadedClient::watched_ids() { return core_->watched_ids(); }
void ThreadedClient::forget_finished_ids() { core_->forget_finished_ids(); }
std::uint64_t ThreadedClient::retries() { return core_->retries(); }
std::size_t ThreadedClient::waiting_queries() { return core_->waiting_queries(); }
std::size_t ThreadedClient::waiting_messages() { return core_->waiting_messages(); }
void ThreadedClient::set_batching(std::uint32_t type, const Batching& batching) {
  core_->set_batching(type, nullptr, batching);
}
void ThreadedClient::set_batching(std::uint32_t type, const std::string& key, const Batching& batching) {
  core_->set_batching(type, &key, batching);
}
void ThreadedClient::clear_batching(std::uint32_t type) { core_->clear_batching(type, nullptr); }
void ThreadedClient::clear_batching(std::uint32_t type, const std::string& key) { core_->clear_batching(type, &key); }
void ThreadedClient::send(const MultiplexerMessage& msg) { core_->send(msg); }
void ThreadedClient::send(const MultiplexerMessage& msg, LanePtr lane, SendCallback done) {
  core_->send(msg, lane, done);
}
void ThreadedClient::send_all(const MultiplexerMessage& msg, SendCallback done) { core_->send_all(msg, done); }
void ThreadedClient::send(const MultiplexerMessage& msg, const ConnectionWrapper& connection, SendCallback done) {
  core_->send(msg, connection, done);
}
unsigned int ThreadedClient::send(const MultiplexerMessage& msg, float timeout) { return core_->send(msg, timeout); }
unsigned int ThreadedClient::send(const MultiplexerMessage& msg, LanePtr lane, float timeout) {
  return core_->send(msg, lane, timeout);
}
unsigned int ThreadedClient::send(const MultiplexerMessage& msg, const ConnectionWrapper& connection, float timeout) {
  return core_->send(msg, connection, timeout);
}
unsigned int ThreadedClient::send_all(const MultiplexerMessage& msg, float timeout) {
  return core_->send_all(msg, timeout);
}
void ThreadedClient::send_serialized(std::string serialized, std::uint64_t id, std::uint32_t type, LanePtr lane,
                                     float timeout, SendCallback done) {
  core_->send_serialized(std::move(serialized), id, type, lane, timeout, done);
}
void ThreadedClient::send_all_serialized(std::string serialized, std::uint64_t id, std::uint32_t type, float timeout,
                                         SendCallback done) {
  core_->send_all_serialized(std::move(serialized), id, type, timeout, done);
}
unsigned int ThreadedClient::send_serialized_and_wait(std::string serialized, std::uint64_t id, std::uint32_t type,
                                                      bool all, float timeout, LanePtr lane, bool* not_connected) {
  return core_->send_serialized_and_wait(std::move(serialized), id, type, all, timeout, lane, not_connected);
}
void ThreadedClient::send_serialized_with_callback(std::string serialized, std::uint64_t id, std::uint32_t type,
                                                   bool all, float timeout, SendCallback done, LanePtr lane) {
  core_->send_serialized_with_callback(std::move(serialized), id, type, all, timeout, done, lane);
}
void ThreadedClient::send_serialized_and_notify(std::string serialized, std::uint64_t id, std::uint32_t type, bool all,
                                                float timeout, FlushedCallback done, LanePtr lane) {
  core_->send_serialized_and_notify(std::move(serialized), id, type, all, timeout, done, lane);
}
MultiplexerMessage ThreadedClient::new_message(std::uint32_t type, const std::string& payload) {
  return core_->new_message(type, payload);
}
void ThreadedClient::query(const std::string& payload, std::uint32_t type, Callback callback, float timeout,
                           LanePtr lane, ReceivedCallback received) {
  core_->query(payload, type, callback, timeout, lane, received);
}
ThreadedClient::Result ThreadedClient::query(const std::string& payload, std::uint32_t type, float timeout,
                                             LanePtr lane, ReceivedCallback received) {
  return core_->query(payload, type, timeout, lane, received);
}
void ThreadedClient::query(const MultiplexerMessage& msg, Callback callback, float timeout, LanePtr lane,
                           ReceivedCallback received) {
  core_->query(msg, callback, timeout, lane, received);
}
ThreadedClient::Result ThreadedClient::query(const MultiplexerMessage& msg, float timeout, LanePtr lane,
                                             ReceivedCallback received) {
  return core_->query(msg, timeout, lane, received);
}
void ThreadedClient::query(const MultiplexerMessage& msg, const ConnectionWrapper& connection, Callback callback,
                           float timeout, ReceivedCallback received) {
  core_->query(msg, connection, callback, timeout, received);
}
ThreadedClient::Result ThreadedClient::query(const MultiplexerMessage& msg, const ConnectionWrapper& connection,
                                             float timeout, ReceivedCallback received) {
  return core_->query(msg, connection, timeout, received);
}
void ThreadedClient::shutdown(float timeout) { core_->shutdown(timeout); }
LogSummary& ThreadedClient::drop_lines() { return core_->drop_lines(); }

bool ThreadedClient::orphaned() const { return core_->orphaned(); }

ThreadedClient::Core::Core(std::uint32_t peer_type, MessageSink on_message)
    : peer_type_(peer_type),
      io_service_holder_(new asio::io_service()),
      io_service_(*io_service_holder_),
      work_(new asio::io_service::work(io_service_)),
      basic_client_(BasicClient::Create(io_service_, peer_type)),
      instance_id_(basic_client_->instance_id()),
      on_message_(on_message) {
  mx::MutexLock lock(random_mutex_);
  plain_id_start_ = random_();
}

void ThreadedClient::Core::start(const std::shared_ptr<Core>& self) {
  // The io thread binds the checkers to itself and installs the sink before
  // the constructor returns, so no call from another thread can run first
  // and bind them to the wrong thread.
  std::promise<void> ready;
  io_service_.post([this, &ready] {
    io_thread_.bind_to_current();
    MX_DCHECK_RUN_ON(&io_thread_);
    basic_client_->set_incoming_sink([this](const BasicClient::IncomingMessagesBuffer::value_type& incoming) {
      MX_DCHECK_RUN_ON(&io_thread_);
      _on_incoming(incoming);
    });
    basic_client_->set_connection_observer([this](const ConnectionWrapper& connection, bool up) {
      MX_DCHECK_RUN_ON(&io_thread_);
      _on_connection(connection, up);
    });
    basic_client_->keep_spent_bodies(spent_);  // handed back at the inbox's next turn
    ready.set_value();
  });
  thread_ = std::thread([self = std::shared_ptr<Core>(self), end = end_]() mutable {
    self->_io_thread_main();
    self.reset();  // the Core goes here when nothing else holds it
    mx::MutexLock lock(end->mutex);
    end->ended = true;
    end->changed.notify_all();
  });
  ready.get_future().wait();
}

// The child's side of a fork: the io thread does not exist here (joining
// its handle would hang, destroying it joinable would terminate), the
// sockets are the parent's, and any lock a parent thread held is held
// forever. So: let go of the handle without a call on it, close the child's
// descriptor copies with close(2) only, and leak everything asio owns. Not
// detached: glibc hands the parent thread's handle to the next thread the
// child starts, a fresh client's io thread say, which detach() would hit.
void ThreadedClient::Core::_orphan_teardown() {
  if (thread_.joinable()) {
    new std::thread(std::move(thread_));  // leaked on purpose, with no pthread call
  }
  basic_client_->orphan_close_descriptors();
  new std::shared_ptr<BasicClient>(basic_client_);  // leaked on purpose, see the header
  work_.release();
  io_service_holder_.release();
  _release_callbacks();
}

// The caller's callbacks go once nothing can call them any more, so that a
// client that was shut down holds none of the caller's objects. In Python
// each one refers back to the client's wrapper, which every class built on
// it passes its own method to, and the binding holds it here, in C++, where
// the collector cannot see the cycle: kept as long as the Core, it would
// keep the wrapper, and so the Core, alive for good. Two callers, neither
// with a thread to race: the io thread once its loop has ended, and a
// forked child's teardown, where the io thread does not exist.
void ThreadedClient::Core::_release_callbacks() MX_NO_THREAD_SAFETY_ANALYSIS {
  on_message_ = MessageSink();
  search_policy_ = SearchPolicy();
  basic_client_->release_drop_observer();
}

void ThreadedClient::Core::_io_thread_main() {
  // A bug in one handler must not take the whole client down: log and keep
  // running, as the multiplexer's own loop does.
  for (;;) {
    try {
      io_service_.run();
      MX_LOG(DEBUG, HIGHVERBOSITY, CTX("ThreadedClient") TEXT("io thread done"));
      basic_client_->end_follows();  // a send still followed hears 0: nothing more runs here
      _release_callbacks();
      return;
    } catch (const std::exception& e) {
      MX_LOG(ERROR, LOWVERBOSITY,
             CTX("ThreadedClient") TEXT(std::string("exception escaped an io handler: ") + e.what()));
    }
  }
}

// Waits for the io thread's end, told once it let go of the Core.
void ThreadedClient::Core::_wait_ended() {
  const std::shared_ptr<End> end = end_;
  mx::UniqueLock lock(end->mutex);
  while (!end->ended) {
    end->changed.wait(lock);
  }
}

// A post that counted itself is over; the last one tells shutdown(), if it
// waits.
void ThreadedClient::Core::_end_posting() {
  if (posting_.fetch_sub(1) == 1 && stopped_.load()) {
    mx::MutexLock lock(lifecycle_mutex_);
    posted_.notify_all();
  }
}

// Runs `function` on the io thread and returns its result; for the short
// bookkeeping calls only. Deadlocks if called on the io thread, hence the
// assertion. The fork check comes first, here and in every blocking call:
// in a forked child nothing may come before it that a thread of the
// parent could have held (lib/fork.h).
template <typename F>
auto ThreadedClient::Core::_call(F function) -> decltype(function()) {
  basic_client_->check_not_orphaned();
  if (io_thread_.is_current()) {
    throw std::logic_error("blocking ThreadedClient call on the io thread, from a callback");
  }
  typedef decltype(function()) R;
  std::promise<R> promise;
  std::future<R> future = promise.get_future();
  const bool posted = _post_unless_stopped([&] {
    try {
      promise.set_value(function());
    } catch (...) {
      promise.set_exception(std::current_exception());  // never leave the caller waiting
    }
  });
  if (!posted) {
    MXTHROW(NotConnected());
  }
  return future.get();
}

std::uint64_t ThreadedClient::Core::random64() {
  basic_client_->check_not_orphaned();
  mx::MutexLock lock(random_mutex_);
  return random_();
}

MultiplexerMessage ThreadedClient::Core::new_message(std::uint32_t type, const std::string& payload) {
  MultiplexerMessage msg;
  msg.set_id(random64());
  msg.set_sender(instance_id_);
  msg.set_type(type);
  msg.set_message(payload);
  return msg;
}

// Connects and waits for the handshake: the connection observer ends the
// wait, up or down, and a timer ends it at `timeout`.
bool ThreadedClient::Core::connect(const std::string& host, std::uint16_t port, float timeout) {
  return _start_connect(host, port, timeout).get();
}

// Every connect started before any is waited for, so that their waits,
// each armed on the io thread as it is posted, run together.
unsigned int ThreadedClient::Core::connect_all(const std::vector<std::pair<std::string, std::uint16_t>>& addresses,
                                               float timeout) {
  std::vector<std::future<bool>> started;
  for (const std::pair<std::string, std::uint16_t>& address : addresses) {
    started.push_back(_start_connect(address.first, address.second, timeout));
  }
  unsigned int up = 0;
  for (std::future<bool>& registered : started) {
    up += registered.get() ? 1 : 0;
  }
  return up;
}

// A connect() handed to the io thread: the connection started there and its
// wait armed, `timeout` from then; the future says how it ended.
std::future<bool> ThreadedClient::Core::_start_connect(const std::string& host, std::uint16_t port, float timeout) {
  basic_client_->check_not_orphaned();
  if (io_thread_.is_current()) {
    throw std::logic_error("blocking ThreadedClient::connect() called on the io thread, from a callback");
  }
  std::shared_ptr<std::promise<bool>> done(new std::promise<bool>());
  std::future<bool> future = done->get_future();
  const bool posted = _post_unless_stopped([this, host = std::string(host), port, timeout, done] {
    MX_DCHECK_RUN_ON(&io_thread_);
    try {
      if (shut_down_) {
        done->set_value(false);
        return;
      }
      ConnectionWrapper connection = basic_client_->async_connect(host, port);
      BasicClient::Connection::pointer conn = connection.lock();
      if (!conn || conn->shuts_down() || conn->registered()) {
        done->set_value(conn && conn->registered());  // failed at once, or connected already
        return;
      }
      std::shared_ptr<ConnectWaiter> waiter(new ConnectWaiter());
      waiter->connection = connection;
      waiter->done = done;
      waiter->timer.reset(new asio::steady_timer(io_service_));
      waiter->timer->expires_after(mx::from_seconds(timeout));
      waiter->timer->async_wait([this, waiter](const asio::error_code& error) {
        MX_DCHECK_RUN_ON(&io_thread_);
        if (error != asio::error::operation_aborted) {
          _end_connect(waiter, false);
        }
      });
      connects_.push_back(waiter);
    } catch (...) {
      done->set_exception(std::current_exception());  // never leave the caller waiting
    }
  });
  if (!posted) {
    MXTHROW(NotConnected());
  }
  return future;
}

void ThreadedClient::Core::set_resolver(BasicClient::Resolver resolver) {
  _call([&] {
    MX_DCHECK_RUN_ON(&io_thread_);
    basic_client_->set_resolver(resolver);
    return 0;
  });
}

void ThreadedClient::Core::set_search_policy(SearchPolicy answer) {
  _call([&] {
    MX_DCHECK_RUN_ON(&io_thread_);
    search_policy_ = answer;
    return 0;
  });
}

void ThreadedClient::Core::set_drop_observer(DropObserver observer) {
  _call([&] {
    MX_DCHECK_RUN_ON(&io_thread_);
    basic_client_->set_drop_observer(observer);
    return 0;
  });
}

std::uint64_t ThreadedClient::Core::dropped() { return basic_client_->dropped(); }

// BasicClient's, on the io thread; the connection observer hears of the
// connection it closes as of one lost (_on_connection).
bool ThreadedClient::Core::disconnect(const std::string& host, std::uint16_t port) {
  return _call([&] {
    MX_DCHECK_RUN_ON(&io_thread_);
    return basic_client_->disconnect(host, port);
  });
}

unsigned int ThreadedClient::Core::connections_count() {
  return _call([&] {
    MX_DCHECK_RUN_ON(&io_thread_);
    return basic_client_->connections_count(true);
  });
}

std::size_t ThreadedClient::Core::watched_ids() {
  return _call([&] {
    MX_DCHECK_RUN_ON(&io_thread_);
    return by_id_.size();
  });
}

void ThreadedClient::Core::forget_finished_ids() {
  auto forget = [this] {
    MX_DCHECK_RUN_ON(&io_thread_);
    std::unordered_set<std::uint64_t>().swap(finished_ids_);  // the buckets too
    std::deque<std::pair<std::uint64_t, std::chrono::steady_clock::time_point>>().swap(finished_order_);
    return 0;
  };
  if (io_thread_.is_current()) {
    forget();  // from a callback, where a test that counts its replies takes its measure
    return;
  }
  _call(forget);
}

std::size_t ThreadedClient::Core::waiting_queries() {
  return _call([&] {
    MX_DCHECK_RUN_ON(&io_thread_);
    return waiting_queries_.size();
  });
}

std::size_t ThreadedClient::Core::waiting_messages() {
  return _call([&] {
    MX_DCHECK_RUN_ON(&io_thread_);
    return basic_client_->outbox_entries();
  });
}

std::uint64_t ThreadedClient::Core::retries() {
  return _call([&] {
    MX_DCHECK_RUN_ON(&io_thread_);
    return basic_client_->retries() + retries_;
  });
}

void ThreadedClient::Core::set_key_filters(const BasicClient::KeyFilters& filters,
                                           BasicClient::AcknowledgedCallback acknowledged) {
  basic_client_->check_not_orphaned();
  BasicClient::check_key_filters(filters);              // here, where the caller hears of a refusal
  _post_unless_stopped([this, filters, acknowledged] {  // after shutdown(), nothing
    MX_DCHECK_RUN_ON(&io_thread_);
    if (!shut_down_) {
      basic_client_->set_key_filters(filters, acknowledged);
    }
  });
}

void ThreadedClient::Core::set_batching(std::uint32_t type, const std::string* key,
                                        const BasicClient::Batching& batching) {
  basic_client_->check_not_orphaned();
  BasicClient::check_batching(type, key, batching);  // here, where the caller hears of a refusal
  batching_.store(true, std::memory_order_relaxed);  // frames are marked for it from now on (_stamped)
  const bool keyed = key != nullptr;
  const std::string one = keyed ? *key : std::string();
  _post_unless_stopped([this, type, keyed, one, batching] {  // after shutdown(), nothing
    MX_DCHECK_RUN_ON(&io_thread_);
    if (shut_down_) {
      return;
    }
    if (keyed) {
      basic_client_->set_batching(type, one, batching);
    } else {
      basic_client_->set_batching(type, batching);
    }
  });
}

void ThreadedClient::Core::clear_batching(std::uint32_t type, const std::string* key) {
  basic_client_->check_not_orphaned();
  const bool keyed = key != nullptr;
  const std::string one = keyed ? *key : std::string();
  _post_unless_stopped([this, type, keyed, one] {
    MX_DCHECK_RUN_ON(&io_thread_);
    if (shut_down_) {
      return;
    }
    if (keyed) {
      basic_client_->clear_batching(type, one);
    } else {
      basic_client_->clear_batching(type);
    }
  });
}

void ThreadedClient::Core::set_routing(const Routing& routing, BasicClient::AcknowledgedCallback acknowledged) {
  basic_client_->check_not_orphaned();
  _post_unless_stopped([this, routing, acknowledged] {  // after shutdown(), nothing
    MX_DCHECK_RUN_ON(&io_thread_);
    if (!shut_down_) {
      basic_client_->set_routing(routing, acknowledged);
    }
  });
}

bool ThreadedClient::Core::routing_acknowledged() {
  return _call([&] {
    MX_DCHECK_RUN_ON(&io_thread_);
    return basic_client_->routing_acknowledged();
  });
}

std::size_t ThreadedClient::Core::held_for_a_connection() {
  return _call([&] {
    MX_DCHECK_RUN_ON(&io_thread_);
    return basic_client_->held_for_a_connection();
  });
}

bool ThreadedClient::Core::flush_all(float timeout) {
  basic_client_->check_not_orphaned();
  if (io_thread_.is_current()) {
    throw std::logic_error("blocking ThreadedClient::flush_all() called on the io thread, from a callback");
  }
  std::shared_ptr<std::promise<bool>> promise(new std::promise<bool>());
  std::future<bool> future = promise->get_future();
  if (!_begin_flush_all(timeout, [promise](bool flushed) { promise->set_value(flushed); })) {
    return true;  // after shutdown(): nothing left to write
  }
  return future.get();
}

void ThreadedClient::Core::flush_all_with_callback(float timeout, FlushCallback done) {
  basic_client_->check_not_orphaned();
  if (!_begin_flush_all(timeout, done)) {
    done(true);  // nothing left to write, as flush_all() says
  }
}

// A flush on the io thread: BasicClient's, which counts what was sent
// before it, with a timer of its own; `done` hears how it ended, once
// (_end_flush). False, and `done` not called, after shutdown().
bool ThreadedClient::Core::_begin_flush_all(float timeout, const FlushCallback& done) {
  // A copy of its own, not const as `done` is, so that the post moves it.
  return _post_unless_stopped([this, done = FlushCallback(done), timeout] {
    MX_DCHECK_RUN_ON(&io_thread_);
    if (torn_down_) {
      _guarded([&] { done(true); }, [] { return std::string("a flush_all() callback"); });
      return;
    }
    std::shared_ptr<FlushWait> wait(new FlushWait());
    wait->done = done;
    wait->timer.reset(new asio::steady_timer(io_service_));
    flushes_.push_back(wait);
    std::weak_ptr<FlushWait> weak = wait;
    wait->flush = basic_client_->begin_flush([this, weak](bool all_written) {
      MX_DCHECK_RUN_ON(&io_thread_);
      if (std::shared_ptr<FlushWait> flushed = weak.lock()) {
        _end_flush(flushed, all_written);
      }
    });
    if (std::find(flushes_.begin(), flushes_.end(), wait) == flushes_.end()) {
      return;  // nothing to wait for: ended already
    }
    wait->timer->expires_after(mx::from_seconds(timeout));
    wait->timer->async_wait([this, weak](const asio::error_code& error) {
      MX_DCHECK_RUN_ON(&io_thread_);
      std::shared_ptr<FlushWait> timed_out = weak.lock();
      if (error != asio::error::operation_aborted && timed_out) {
        _end_flush(timed_out, false);
      }
    });
  });
}

void ThreadedClient::Core::shutdown(float timeout) {
  if (orphaned()) {
    _orphan_teardown();
    return;
  }
  // The Core outlives this call whoever lets go of it meanwhile, a handle
  // destroyed on another thread or the io thread at its end: the thread
  // is joined, or waited for, before the Core can go.
  const std::shared_ptr<Core> keep = shared_from_this();
  if (stopped_.exchange(true)) {
    // Begun already, on another thread or on the io thread: this call, as
    // the one that began it, returns once it is over, the io thread ended,
    // but on the io thread itself, which cannot wait for its own end.
    if (!io_thread_.is_current()) {
      _wait_ended();
    }
    return;
  }
  {
    mx::UniqueLock lock(lifecycle_mutex_);
    posted_.wait(lock, [this] { return posting_.load() == 0; });  // what found the client up is queued
  }
  _post([this, timeout] {
    MX_DCHECK_RUN_ON(&io_thread_);
    shut_down_ = true;
    MX_LOG(DEBUG, HIGHVERBOSITY,
           CTX("ThreadedClient") TEXT("shutting down: " + repr(in_flight_.size()) + " queries in flight"));
    // A callback that throws is logged and the others still run
    // (_guarded): the teardown below must happen, or the io thread runs on
    // and join() waits for good.
    _finish_every_query();
    // A connect() still waiting ends: nothing new comes up for this client.
    asio::error_code ignored;
    for (const std::shared_ptr<ConnectWaiter>& waiter : connects_) {
      waiter->timer->cancel(ignored);
      waiter->done->set_value(false);
    }
    connects_.clear();
    if (timeout == 0) {
      _teardown();
      return;
    }
    // What was sent before is written first: the connections close once it
    // is, or at `timeout`, never for a negative one, as mx::from_seconds
    // reads it, and at once for NaN.
    _write_out();
    drain_timer_.reset(new asio::steady_timer(io_service_));
    drain_timer_->expires_after(mx::from_seconds(timeout));
    drain_timer_->async_wait([this](const asio::error_code& error) {
      MX_DCHECK_RUN_ON(&io_thread_);
      if (error != asio::error::operation_aborted) {
        _teardown();
      }
    });
  });
  if (io_thread_.is_current()) {
    // From a callback, or from the destructor of a handle whose last
    // reference was dropped in one: this thread cannot wait for itself.
    // The teardown above runs once the current handler returns, and the
    // thread ends on its own when its handlers are done, holding the Core
    // until then (see start()).
    thread_.detach();
    return;
  }
  thread_.join();
}

}  // namespace multiplexer
