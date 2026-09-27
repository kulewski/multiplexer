// ThreadedClient: the io thread, the cross-thread calls, and the query
// state machine. See the header for the design.
#include "multiplexer/threaded_client.h"

#include <algorithm>
#include <asio/ip/tcp.hpp>
#include <chrono>
#include <future>
#include <limits>
#include <stdexcept>

#include "lib/logging/logging.h"
#include "lib/repr.h"
#include "lib/seconds.h"
#include "multiplexer/Multiplexer.pb.h" /* generated */
#include "multiplexer/multiplexer.constants.h"
#include "multiplexer/threaded_client_core.h"

namespace multiplexer {

using mx::repr;

// One query in flight. Owned by the io thread; the shared_ptr keeps it alive
// for the timer callback.
struct ThreadedClient::Core::InFlight {
  // WAITING: no connection was live when the request had to be (re)sent; it
  // goes out as soon as one registers, the deadline still running. SEARCH
  // is the locate phase of an addressed query too: the PING addressed to
  // the instance out, the answers counted the same way.
  enum Stage { REQUEST, SEARCH, DIRECT, WAITING } stage = REQUEST;
  ConnectionWrapper sent_via;    // REQUEST and DIRECT: the connection used
  MultiplexerMessage prototype;  // the request; id and from set per attempt
  float timeout = 0;
  // An addressed query's one deadline across its stages; a typed query
  // arms each stage with `timeout`.
  std::chrono::steady_clock::time_point deadline;
  LanePtr lane;  // held until the query ends, then released
  std::uint64_t request_id = 0, search_id = 0, direct_id = 0;
  // The request and direct ids of attempts sent again since, tracked still
  // for a late reply; empty unless a connection was lost under the query.
  std::vector<std::uint64_t> earlier_ids;
  // During SEARCH: the connections the search went through that have not
  // answered it yet, with a delivery error or by going down. Only these
  // count: another connection ending, a failed connect for instance, says
  // nothing about the search.
  std::vector<ConnectionWrapper> searched;
  std::vector<std::uint64_t> search_ids;  // every search the query made, the current one last
  unsigned int generation = 0;            // bumped per stage so stale deadlines are ignored
  bool listed = false;                    // in waiting_queries_
  bool started = false;                   // in in_flight_, at `place`
  std::list<InFlightPtr>::iterator place;
  Callback callback;
  ReceivedCallback received;  // told of each REQUEST_RECEIVED for an attempt, until the query ends
  std::unique_ptr<asio::steady_timer> timer;
  bool addressed() const { return prototype.to() != 0; }
  bool pinned() const { return lane && lane->pinned(); }
};

// One send on its way; see "Sends" below.
struct ThreadedClient::Core::PendingSend {
  std::shared_ptr<const RawMessage> raw;
  bool all = false;          // a copy for every live connection, or one message
  bool wait = false;         // a flushing send: `done` hears when a copy is written
  std::uint64_t number = 0;  // BasicClient's, in the order sends were made, for flush_all()
  std::chrono::steady_clock::time_point deadline;
  bool expiring = false;  // its deadline is in expiring_
  LanePtr lane;
  SendCallback done;        // its end, once: the written copies, 1 at the first, or 0
  FlushedCallback flushed;  // or that, and whether the message was given up on
  bool over = false;        // ended: where it is still listed, it is skipped
};

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
ThreadedClient::~ThreadedClient() { core_->shutdown(CLOSE_FLUSH_SECONDS); }

void ThreadedClient::set_search_policy(SearchPolicy answer) { core_->set_search_policy(answer); }
void ThreadedClient::set_drop_observer(DropObserver observer) { core_->set_drop_observer(observer); }
std::uint64_t ThreadedClient::dropped() { return core_->dropped(); }
void ThreadedClient::set_resolver(BasicClient::Resolver resolver) { core_->set_resolver(resolver); }
void ThreadedClient::set_routing(const Routing& routing) { core_->set_routing(routing); }
bool ThreadedClient::routing_acknowledged() { return core_->routing_acknowledged(); }
bool ThreadedClient::flush_all(float timeout) { return core_->flush_all(timeout); }
void ThreadedClient::flush_all_with_callback(float timeout, FlushCallback done) {
  core_->flush_all_with_callback(timeout, done);
}
std::uint64_t ThreadedClient::random64() { return core_->random64(); }
bool ThreadedClient::connect(const std::string& host, std::uint16_t port, float timeout) {
  return core_->connect(host, port, timeout);
}
unsigned int ThreadedClient::connections_count() { return core_->connections_count(); }
std::size_t ThreadedClient::watched_ids() { return core_->watched_ids(); }
std::uint64_t ThreadedClient::retries() { return core_->retries(); }
std::size_t ThreadedClient::waiting_queries() { return core_->waiting_queries(); }
std::size_t ThreadedClient::waiting_messages() { return core_->waiting_messages(); }
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
void ThreadedClient::send_serialized(std::string serialized, LanePtr lane, float timeout, SendCallback done) {
  core_->send_serialized(std::move(serialized), lane, timeout, done);
}
void ThreadedClient::send_all_serialized(std::string serialized, float timeout, SendCallback done) {
  core_->send_all_serialized(std::move(serialized), timeout, done);
}
unsigned int ThreadedClient::send_serialized_and_wait(std::string serialized, bool all, float timeout, LanePtr lane,
                                                      bool* not_connected) {
  return core_->send_serialized_and_wait(std::move(serialized), all, timeout, lane, not_connected);
}
void ThreadedClient::send_serialized_with_callback(std::string serialized, bool all, float timeout, SendCallback done,
                                                   LanePtr lane) {
  core_->send_serialized_with_callback(std::move(serialized), all, timeout, done, lane);
}
void ThreadedClient::send_serialized_and_notify(std::string serialized, bool all, float timeout, FlushedCallback done,
                                                LanePtr lane) {
  core_->send_serialized_and_notify(std::move(serialized), all, timeout, done, lane);
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
      on_message_(on_message) {}

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
    ready.set_value();
  });
  thread_ = std::thread([self] { self->_io_thread_main(); });
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

// A message as a log line names it.
static std::string describe(const MultiplexerMessage& msg) {
  return "message #" + repr(msg.id()) + " of type " + repr(msg.type()) + " from " + repr(msg.from());
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
  msg.set_from(instance_id_);
  msg.set_type(type);
  msg.set_message(payload);
  return msg;
}

// Connects and waits for the handshake: the connection observer ends the
// wait, up or down, and a timer ends it at `timeout`.
bool ThreadedClient::Core::connect(const std::string& host, std::uint16_t port, float timeout) {
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
  return future.get();
}

// A connect() call ends: connected, failed, or out of time.
void ThreadedClient::Core::_end_connect(const std::shared_ptr<ConnectWaiter>& waiter, bool up) {
  std::vector<std::shared_ptr<ConnectWaiter>>::iterator found = std::find(connects_.begin(), connects_.end(), waiter);
  if (found == connects_.end()) {
    return;  // ended already
  }
  connects_.erase(found);
  asio::error_code ignored;
  waiter->timer->cancel(ignored);
  waiter->done->set_value(up);
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

void ThreadedClient::Core::set_routing(const Routing& routing) {
  basic_client_->check_not_orphaned();
  _post_unless_stopped([this, routing] {  // after shutdown(), nothing
    MX_DCHECK_RUN_ON(&io_thread_);
    if (!shut_down_) {
      basic_client_->set_routing(routing);
    }
  });
}

bool ThreadedClient::Core::routing_acknowledged() {
  return _call([&] {
    MX_DCHECK_RUN_ON(&io_thread_);
    return basic_client_->routing_acknowledged();
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

// A flush_all() call ends, flushed or out of time: BasicClient forgets its
// flush, and the caller is told.
void ThreadedClient::Core::_end_flush(const std::shared_ptr<FlushWait>& wait, bool flushed) {
  std::vector<std::shared_ptr<FlushWait>>::iterator found = std::find(flushes_.begin(), flushes_.end(), wait);
  if (found == flushes_.end()) {
    return;  // ended already
  }
  flushes_.erase(found);
  if (wait->flush) {
    basic_client_->end_flush(wait->flush);
  }
  asio::error_code ignored;
  wait->timer->cancel(ignored);
  FlushCallback done;
  done.swap(wait->done);
  _guarded([&] { done(flushed); }, [] { return std::string("a flush_all() callback"); });
}

// ---------------------------------------------------------------------------
// Sends
//
// A send goes to the io thread, which hands it to BasicClient, as every
// client does: queued on a connection at once, waiting there for room, or
// held until a connection comes up (outbox.cc). A send with a callback
// or a wait has BasicClient follow it to its end: the connection says
// when a frame is written or lost, so nothing is polled. A flushing send,
// waited for or with a callback (send_serialized_with_callback), also
// ends at its deadline, one timer running to the earliest, with 0 when
// nothing was written by then.

namespace {
// A blocking send's completion: the promise its caller waits on.
ThreadedClient::SendCallback settle(std::shared_ptr<std::promise<unsigned int>> promise) {
  return [promise](unsigned int written) { promise->set_value(written); };
}
}  // namespace

// A caller's whole message framed, its empty id and sender filled (frame_stamped).
std::shared_ptr<const RawMessage> ThreadedClient::Core::_stamped(const MultiplexerMessage& msg) {
  return frame_stamped(msg, instance_id_, [this] { return random64(); });
}

void ThreadedClient::Core::send(const MultiplexerMessage& msg) { send(msg, LanePtr(), SendCallback()); }

void ThreadedClient::Core::send(const MultiplexerMessage& msg, LanePtr lane, SendCallback done) {
  if (lane && lane->closed()) {
    MXTHROW(NotConnected());  // a pinned lane whose connection is gone; the Python client raises it too
  }
  _submit_send(_stamped(msg), false, false, DEFAULT_TIMEOUT, done, lane);
}

void ThreadedClient::Core::send_all(const MultiplexerMessage& msg, SendCallback done) {
  _submit_send(_stamped(msg), true, false, DEFAULT_TIMEOUT, done, LanePtr());
}

void ThreadedClient::Core::send(const MultiplexerMessage& msg, const ConnectionWrapper& connection, SendCallback done) {
  send(msg, std::make_shared<Lane>(connection), done);
}

unsigned int ThreadedClient::Core::send(const MultiplexerMessage& msg, float timeout) {
  return send(msg, LanePtr(), timeout);
}

unsigned int ThreadedClient::Core::send(const MultiplexerMessage& msg, LanePtr lane, float timeout) {
  basic_client_->check_not_orphaned();
  if (io_thread_.is_current()) {
    throw std::logic_error("flushing ThreadedClient::send() called on the io thread, from a callback");
  }
  std::shared_ptr<std::promise<unsigned int>> promise(new std::promise<unsigned int>());
  std::future<unsigned int> future = promise->get_future();
  _submit_send(_stamped(msg), false, true, timeout, settle(promise), lane);
  return future.get();
}

unsigned int ThreadedClient::Core::send(const MultiplexerMessage& msg, const ConnectionWrapper& connection,
                                        float timeout) {
  return send(msg, std::make_shared<Lane>(connection), timeout);
}

unsigned int ThreadedClient::Core::send_all(const MultiplexerMessage& msg, float timeout) {
  basic_client_->check_not_orphaned();
  if (io_thread_.is_current()) {
    throw std::logic_error("flushing ThreadedClient::send_all() called on the io thread, from a callback");
  }
  std::shared_ptr<std::promise<unsigned int>> promise(new std::promise<unsigned int>());
  std::future<unsigned int> future = promise->get_future();
  _submit_send(_stamped(msg), true, true, timeout, settle(promise), LanePtr());
  return future.get();
}

void ThreadedClient::Core::send_serialized(std::string serialized, LanePtr lane, float timeout, SendCallback done) {
  if (lane && lane->closed()) {
    MXTHROW(NotConnected());  // as send() does
  }
  _submit_send(std::shared_ptr<const RawMessage>(new RawMessage(&serialized)), false, false, timeout, done, lane);
}

void ThreadedClient::Core::send_all_serialized(std::string serialized, float timeout, SendCallback done) {
  _submit_send(std::shared_ptr<const RawMessage>(new RawMessage(&serialized)), true, false, timeout, done, LanePtr());
}

unsigned int ThreadedClient::Core::send_serialized_and_wait(std::string serialized, bool all, float timeout,
                                                            LanePtr lane, bool* not_connected) {
  basic_client_->check_not_orphaned();
  if (io_thread_.is_current()) {
    throw std::logic_error("flushing ThreadedClient send called on the io thread, from a callback");
  }
  std::shared_ptr<std::promise<std::pair<unsigned int, bool>>> promise(
      new std::promise<std::pair<unsigned int, bool>>());
  std::future<std::pair<unsigned int, bool>> future = promise->get_future();
  _submit_send(std::shared_ptr<const RawMessage>(new RawMessage(&serialized)), all, true, timeout, SendCallback(), lane,
               [promise](unsigned int written, bool lost) { promise->set_value(std::make_pair(written, lost)); });
  const std::pair<unsigned int, bool> ended = future.get();
  if (not_connected) {
    *not_connected = ended.second;
  }
  return ended.first;
}

void ThreadedClient::Core::send_serialized_with_callback(std::string serialized, bool all, float timeout,
                                                         SendCallback done, LanePtr lane) {
  _submit_send(std::shared_ptr<const RawMessage>(new RawMessage(&serialized)), all, true, timeout, done, lane);
}

void ThreadedClient::Core::send_serialized_and_notify(std::string serialized, bool all, float timeout,
                                                      FlushedCallback done, LanePtr lane) {
  _submit_send(std::shared_ptr<const RawMessage>(new RawMessage(&serialized)), all, true, timeout, SendCallback(), lane,
               done);
}

// Hands a send to the io thread. Never blocks the caller: a plain post, so
// callbacks may send. A flushing send carries a completion the io thread
// calls once the message is written or the deadline passed.
void ThreadedClient::Core::_submit_send(std::shared_ptr<const RawMessage> raw, bool all, bool wait, float timeout,
                                        SendCallback done, LanePtr lane, FlushedCallback flushed) {
  basic_client_->check_not_orphaned();
  if (lane) {
    lane->check_not_inherited();  // here, not on the io thread, where it would throw into nobody
  }
  PendingSendPtr pending(new PendingSend());
  pending->raw = std::move(raw);
  pending->all = all;
  pending->wait = wait;
  pending->deadline = std::chrono::steady_clock::now() + mx::from_seconds(timeout);
  pending->done = std::move(done);
  pending->flushed = std::move(flushed);
  pending->lane = std::move(lane);
  const bool posted = _post_unless_stopped([this, pending] {
    MX_DCHECK_RUN_ON(&io_thread_);
    if (shut_down_) {
      basic_client_->report_drop(pending->raw, DropReason::SHUT_DOWN);  // posted just before the shutdown
      _settle(pending, 0, /*not_connected=*/true);
      return;
    }
    _place(pending);
  });
  if (posted) {
    return;
  }
  // After shutdown() a send fails, but one made on the io thread while
  // what was sent before is written out, a server's refusal of what still
  // arrives say, goes out with it, placed at once.
  if (!_writing_out_here()) {
    MXTHROW(NotConnected());
  }
  MX_DCHECK_RUN_ON(&io_thread_);
  _place(pending);
}

namespace {
// What is left of the time until `deadline`, for BasicClient: 0 for a
// message whose time is up, a send given 0 or NaN included, which waits
// for nothing there: placed now where a connection has room, or dropped at
// once.
float left_until(std::chrono::steady_clock::time_point deadline) {
  const float left = std::chrono::duration<float>(deadline - std::chrono::steady_clock::now()).count();
  return std::max(0.0f, left);
}
}  // namespace

// A new send: numbered and handed to BasicClient, which queues it, has it
// wait for room, or holds it until a connection comes up, and follows it
// to its end for `done` (BasicClient::send), which a message given up on
// ends with 0. A flushing send (`wait`) also ends at its deadline,
// whatever its message does, a timeout rather than a message given up on:
// the message waits ROOM_GRACE_SECONDS longer, as a synchronous flushing
// send's does, and is dropped then. A pinned lane whose connection is
// gone takes nothing: the message is dropped and reported.
void ThreadedClient::Core::_place(const PendingSendPtr& pending) {
  pending->number = basic_client_->next_number();
  if (pending->wait) {
    _expire_at(pending);
  }
  BasicClient::SendCallback followed;
  if (pending->done || pending->flushed) {
    followed = [this, pending](unsigned int written) {
      MX_DCHECK_RUN_ON(&io_thread_);
      _settle(pending, written, /*not_connected=*/written == 0);
    };
  }
  const float timeout = left_until(pending->deadline) + (pending->wait ? ROOM_GRACE_SECONDS : 0.0f);
  if (!basic_client_->send(pending->raw, pending->all, pending->lane, timeout, pending->number, NULL, NULL, followed)) {
    _refuse(pending);
    _settle(pending, 0, /*not_connected=*/true);
  }
}

// A pinned lane's connection is gone: the message goes nowhere.
void ThreadedClient::Core::_refuse(const PendingSendPtr& pending) {
  basic_client_->report_drop(pending->raw, DropReason::CONNECTION_LOST);
  pending->over = true;
}

// A send's end, once: `done` hears the copies written, 1 at the first, or
// 0 when none was, by the deadline or ever, and `flushed` whether the
// message was given up on as well; what follows is not heard.
void ThreadedClient::Core::_settle(const PendingSendPtr& pending, unsigned int written, bool not_connected) {
  pending->over = true;
  if (pending->flushed) {
    FlushedCallback flushed;
    flushed.swap(pending->flushed);
    _guarded([&] { flushed(written, not_connected); }, [] { return std::string("a send's callback"); });
    return;
  }
  if (!pending->done) {
    return;
  }
  SendCallback done;
  done.swap(pending->done);
  _guarded([&] { done(written); }, [] { return std::string("a send's callback"); });
}

// One _fill() on its way, however many events asked for it.
void ThreadedClient::Core::_post_fill() {
  if (fill_posted_) {
    return;
  }
  fill_posted_ = true;
  _post([this] {
    MX_DCHECK_RUN_ON(&io_thread_);
    fill_posted_ = false;
    if (!shut_down_) {
      _fill();
    }
  });
}

// A connection came up: the queries that waited for one start again. What
// was sent meanwhile BasicClient placed itself.
void ThreadedClient::Core::_fill() {
  if (!waiting_queries_.empty()) {
    _restart_waiting_queries();
  }
}

// `pending` has a deadline running: into the heap, the timer moved to it
// when it is the earliest. Entries of sends that ended are cleared out when
// the heap has doubled since the last time.
void ThreadedClient::Core::_expire_at(const PendingSendPtr& pending) {
  pending->expiring = true;
  if (expiring_.size() >= expiring_compact_at_) {
    expiring_.erase(std::remove_if(expiring_.begin(), expiring_.end(),
                                   [](const Expiring& entry) {
                                     PendingSendPtr send = entry.send.lock();
                                     return !send || send->over;
                                   }),
                    expiring_.end());
    std::make_heap(expiring_.begin(), expiring_.end());
    expiring_compact_at_ = std::max<std::size_t>(64, 2 * expiring_.size());
  }
  expiring_.push_back(Expiring{pending->deadline, pending});
  std::push_heap(expiring_.begin(), expiring_.end());
  if (pending->deadline < expiry_armed_) {
    _arm_expiry();
  }
}

// Sets the timer to the earliest deadline that still matters, if any: a
// flushing send not settled.
void ThreadedClient::Core::_arm_expiry() {
  while (!expiring_.empty()) {
    PendingSendPtr send = expiring_.front().send.lock();
    if (send && !send->over && (send->done || send->flushed)) {
      break;
    }
    std::pop_heap(expiring_.begin(), expiring_.end());
    expiring_.pop_back();
  }
  asio::error_code ignored;
  if (expiring_.empty()) {
    if (expiry_timer_) {
      expiry_timer_->cancel(ignored);
    }
    expiry_armed_ = std::chrono::steady_clock::time_point::max();
    return;
  }
  const std::chrono::steady_clock::time_point next = expiring_.front().deadline;
  if (next == expiry_armed_) {
    return;
  }
  if (!expiry_timer_) {
    expiry_timer_.reset(new asio::steady_timer(io_service_));
  }
  expiry_armed_ = next;
  expiry_timer_->expires_at(next);  // cancels a wait set for another time
  expiry_timer_->async_wait([this](const asio::error_code& error) {
    MX_DCHECK_RUN_ON(&io_thread_);
    if (error != asio::error::operation_aborted && !torn_down_) {
      expiry_armed_ = std::chrono::steady_clock::time_point::max();
      _expire();
    }
  });
}

// The deadline timer: a flushing send reports what was written by then,
// and, for one that wrote nothing, whether a connection was live, which
// the synchronous client asks at its deadline too: NotConnected or
// OperationTimedOut, told here rather than asked of the io thread later.
// What it sent waits on in BasicClient within its own timeout.
void ThreadedClient::Core::_expire() {
  const std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now();
  int none_live = -1;  // not looked at yet
  while (!expiring_.empty() && expiring_.front().deadline <= now) {
    PendingSendPtr pending = expiring_.front().send.lock();
    std::pop_heap(expiring_.begin(), expiring_.end());
    expiring_.pop_back();
    if (!pending || pending->over) {
      continue;
    }
    if (none_live < 0) {
      none_live = _any_live_connection() ? 0 : 1;
    }
    _settle(pending, 0, /*not_connected=*/none_live == 1);  // out of time
  }
  _arm_expiry();
}

// Whether some registered connection is live.
bool ThreadedClient::Core::_any_live_connection() {
  for (BasicClient::ConnectionById::const_iterator entry = basic_client_->begin(); entry != basic_client_->end();
       ++entry) {
    BasicClient::Connection::pointer conn = entry->second.lock();
    if (conn && conn->living()) {
      return true;
    }
  }
  return false;
}

// Hands `raw` to BasicClient through `lane`: on the lane's connection while
// it is live, waiting for its room when it is full, else on any live
// connection, which the lane adopts; with no lane, on any. Null when no
// connection is live; *refused is set when a pinned lane's connection is
// gone, so the caller stops trying.
BasicClient::BasicScheduledMessageTracker ThreadedClient::Core::_schedule(const std::shared_ptr<const RawMessage>& raw,
                                                                          const LanePtr& lane, ConnectionWrapper* used,
                                                                          bool* refused, float timeout,
                                                                          std::uint64_t number) {
  if (lane && lane->pinned()) {
    raw->mark_pinned();  // never handed to another connection if this one dies under it
  }
  if (lane && lane->holds_connection()) {
    ConnectionWrapper held = lane->connection();
    if (Tracker tracker = basic_client_->schedule_on(raw, held, timeout, number, lane)) {
      *used = held;
      return tracker;
    }
    if (lane->pinned()) {
      *refused = true;  // its connection is gone
      return Tracker();
    }
  }
  Tracker tracker = basic_client_->schedule_one(raw, used, timeout, number, lane);
  if (tracker && lane) {
    lane->adopt(*used);
  }
  return tracker;
}

void ThreadedClient::Core::query(const std::string& payload, std::uint32_t type, Callback callback, float timeout,
                                 LanePtr lane, ReceivedCallback received) {
  MultiplexerMessage msg;
  msg.set_type(type);
  msg.set_message(payload);
  query(msg, callback, timeout, lane, received);
}

ThreadedClient::Result ThreadedClient::Core::query(const std::string& payload, std::uint32_t type, float timeout,
                                                   LanePtr lane, ReceivedCallback received) {
  MultiplexerMessage msg;
  msg.set_type(type);
  msg.set_message(payload);
  return query(msg, timeout, lane, received);
}

void ThreadedClient::Core::query(const MultiplexerMessage& msg, Callback callback, float timeout, LanePtr lane,
                                 ReceivedCallback received) {
  basic_client_->check_not_orphaned();
  if (lane) {
    lane->check_not_inherited();
  }
  InFlightPtr in_flight(new InFlight());
  in_flight->prototype = msg;
  in_flight->prototype.set_from(instance_id_);
  if (msg.to()) {
    in_flight->prototype.set_report_delivery_error(true);  // "not behind this multiplexer" must come back
  }
  // Here, on the caller's thread, the request as the io thread frames it:
  // each attempt only gets its id, and a typed query the `to` of the
  // backend its search found (_direct), both measured at their largest.
  in_flight->prototype.set_id(std::numeric_limits<std::uint64_t>::max());
  const bool typed = !msg.to();
  if (typed) {
    in_flight->prototype.set_to(std::numeric_limits<std::uint64_t>::max());
  }
  check_message_size(in_flight->prototype.ByteSizeLong());
  if (typed) {
    in_flight->prototype.clear_to();
  }
  in_flight->timeout = timeout;
  in_flight->deadline = std::chrono::steady_clock::now() + mx::from_seconds(timeout);
  in_flight->lane = std::move(lane);
  in_flight->callback = std::move(callback);
  in_flight->received = std::move(received);
  in_flight->timer.reset(new asio::steady_timer(io_service_));
  const bool posted = _post_unless_stopped([this, in_flight] {
    MX_DCHECK_RUN_ON(&io_thread_);
    in_flight->place = in_flight_.insert(in_flight_.end(), in_flight);
    in_flight->started = true;
    _start_query(in_flight, /*keep_deadline=*/false);
  });
  if (!posted) {
    Result result;
    result.outcome = SHUT_DOWN;
    in_flight->callback(result);
  }
}

ThreadedClient::Result ThreadedClient::Core::query(const MultiplexerMessage& msg, float timeout, LanePtr lane,
                                                   ReceivedCallback received) {
  basic_client_->check_not_orphaned();
  if (io_thread_.is_current()) {
    throw std::logic_error(
        "blocking ThreadedClient::query() called on the io thread, from a callback; "
        "use the callback form there");
  }
  std::shared_ptr<std::promise<Result>> promise(new std::promise<Result>());
  std::future<Result> future = promise->get_future();
  query(msg, [promise](const Result& result) { promise->set_value(result); }, timeout, lane, received);
  return future.get();
}

void ThreadedClient::Core::query(const MultiplexerMessage& msg, const ConnectionWrapper& connection, Callback callback,
                                 float timeout, ReceivedCallback received) {
  query(msg, callback, timeout, std::make_shared<Lane>(connection), received);
}

ThreadedClient::Result ThreadedClient::Core::query(const MultiplexerMessage& msg, const ConnectionWrapper& connection,
                                                   float timeout, ReceivedCallback received) {
  return query(msg, timeout, std::make_shared<Lane>(connection), received);
}

void ThreadedClient::Core::shutdown(float timeout) {
  if (orphaned()) {
    _orphan_teardown();
    return;
  }
  if (stopped_.exchange(true)) {
    return;  // already done, or being done by another thread
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
    std::vector<InFlightPtr> pending(in_flight_.begin(), in_flight_.end());
    for (auto& in_flight : pending) {
      if (in_flight->callback) {
        _finish(in_flight, SHUT_DOWN, NULL);
      }
    }
    waiting_queries_.clear();
    waiting_ended_ = 0;
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

// The end of a shutdown, once, after its drain: a send BasicClient still
// follows hears its end from there, written, or 0 for what it drops and
// reports, at the latest when the io thread ends (end_follows); a
// flush_all() still waiting ends; the connections close, and the io
// thread ends once its handlers are done.
// The shutdown's write-out: what was sent before is written, and then
// what the io thread sent meanwhile, a flush again while anything newer
// was sent, until drain_timer_ ends it; then the teardown. A flush ends
// from wherever its last frame was written, a connection's handler maybe,
// so what follows it is posted.
void ThreadedClient::Core::_write_out() {
  const std::uint64_t sent = basic_client_->last_number();
  drain_ = basic_client_->begin_flush([this, sent](bool) {
    _post([this, sent] {
      MX_DCHECK_RUN_ON(&io_thread_);
      if (torn_down_) {
        return;
      }
      if (basic_client_->last_number() != sent) {
        _write_out();
      } else {
        _teardown();
      }
    });
  });
}

// Whether this is the io thread while shutdown() writes out what was sent
// before: a send made here then goes out with it.
bool ThreadedClient::Core::_writing_out_here() {
  if (!io_thread_.is_current()) {
    return false;
  }
  MX_DCHECK_RUN_ON(&io_thread_);
  return !torn_down_;
}

void ThreadedClient::Core::_teardown() {
  if (torn_down_) {
    return;
  }
  torn_down_ = true;
  asio::error_code ignored;
  if (drain_timer_) {
    drain_timer_->cancel(ignored);
  }
  if (drain_) {
    basic_client_->end_flush(drain_);
    drain_.reset();
  }
  expiring_.clear();
  if (expiry_timer_) {
    expiry_timer_->cancel(ignored);
  }
  std::vector<std::shared_ptr<FlushWait>> flushes = flushes_;
  for (const std::shared_ptr<FlushWait>& flush : flushes) {
    _end_flush(flush, false);
  }
  basic_client_->shutdown();
  work_.reset();
  MX_LOG(DEBUG, HIGHVERBOSITY, CTX("ThreadedClient") TEXT("shut down; the io thread ends when its handlers are done"));
}

// ---------------------------------------------------------------------------
// io thread only

void ThreadedClient::Core::_on_incoming(const BasicClient::IncomingMessagesBuffer::value_type& incoming) {
  const MultiplexerMessage& msg = *incoming.third;
  auto it = by_id_.find(msg.references());
  if (it != by_id_.end()) {
    if (msg.type() != types::REQUEST_RECEIVED) {
      _advance(it->second, incoming);
    } else {
      _acknowledged(it->second, msg);
    }
    return;
  }
  _on_unmatched(incoming);
}

// A message that is no reply to a query in flight: the protocol's own are
// handled or dropped here, the rest go to on_message.
void ThreadedClient::Core::_on_unmatched(const IncomingMessage& incoming) {
  const MultiplexerMessage& msg = *incoming.third;
  if (msg.references() && finished_ids_.count(msg.references())) {
    MX_LOG(DEBUG, MEDIUMVERBOSITY,
           CTX("ThreadedClient")
               TEXT("late reply #" + repr(msg.id()) + " to finished query #" + repr(msg.references()) + " dropped"));
    return;
  }
  if (msg.type() == types::REQUEST_RECEIVED) {
    return;  // for a query this client no longer tracks
  }
  if (msg.type() == types::PING || msg.type() == types::BACKEND_FOR_PACKET_SEARCH) {
    if (msg.references()) {
      return;  // an answer to a ping nobody here is waiting for
    }
    if (msg.type() == types::BACKEND_FOR_PACKET_SEARCH) {
      if (!search_policy_) {
        return;  // a search looks for a backend, which a client without a search policy is not
      }
      bool answer = false;  // a policy that throws answers no
      _guarded(
          [&] {
            MX_DCHECK_RUN_ON(&io_thread_);
            answer = search_policy_();
          },
          [&] { return "the search policy, for " + describe(msg); });
      if (!answer) {
        return;  // a backend whose policy declines, saturated for instance
      }
    }
    // An echo request, an addressed query's PING locating this instance
    // among them, or a search the policy said yes to: answered with a PING
    // carrying its payload back, through the connection it came on. An echo
    // that would be over MAX_MESSAGE_SIZE is answered with BACKEND_ERROR
    // saying so, rather than not at all.
    MultiplexerMessage pong = new_message(types::PING, msg.message());
    pong.set_to(msg.from());
    pong.set_references(msg.id());
    if (pong.ByteSizeLong() > MAX_MESSAGE_SIZE) {
      pong = new_message(types::BACKEND_ERROR,
                         std::string("the echo of a ") + (msg.type() == types::PING ? "PING" : "search") + " of " +
                             repr(msg.message().size()) + " bytes would be over MAX_MESSAGE_SIZE");
      pong.set_to(msg.from());
      pong.set_references(msg.id());
    }
    std::shared_ptr<const RawMessage> raw(RawMessage::FromMessage(pong));
    if (!basic_client_->schedule_on(raw, incoming.second)) {
      basic_client_->schedule_one(raw);
    }
    return;
  }
  if (on_message_) {
    _guarded(
        [&] {
          MX_DCHECK_RUN_ON(&io_thread_);
          on_message_(incoming);
        },
        [&] { return "on_message, for " + describe(msg); });
    return;
  }
  if (basic_client_->drop_lines().first({BasicClient::NO_ON_MESSAGE, WARNING, msg.type(), 0}, [&] {
        return "messages of type " + repr(msg.type()) + " dropped: this client has no on_message callback";
      })) {
    MX_LOG(WARNING, LogSummary::VERBOSITY,
           CTX("ThreadedClient") TEXT("message #" + repr(msg.id()) + " of type " + repr(msg.type()) +
                                      " dropped: this client has no on_message callback"));
  }
}

void ThreadedClient::Core::_remember_finished(std::uint64_t id) {
  if (!id || !finished_ids_.insert(id).second) {
    return;
  }
  finished_order_.push_back(id);
  if (finished_order_.size() > REMEMBERED_FINISHED_IDS) {
    finished_ids_.erase(finished_order_.front());
    finished_order_.pop_front();
  }
}

void ThreadedClient::Core::_track(InFlightPtr in_flight, std::uint64_t id) { by_id_[id] = in_flight; }

// A connection came (up) or went. A connect() waiting for it ends. Up, what
// waits for a connection, sends and queries, goes now. Down, queries whose
// request or direct request went through it are sent again through another
// one, or wait for one; the deadline keeps running throughout. (What waited
// for its room BasicClient hands over itself.)
void ThreadedClient::Core::_on_connection(const ConnectionWrapper& connection, bool up) {
  std::vector<InFlightPtr> queries(in_flight_.begin(), in_flight_.end());  // a copy: the loop may finish queries
  MX_LOG(DEBUG, HIGHVERBOSITY,
         CTX("ThreadedClient")
             TEXT(std::string("connection ") + (up ? "up" : "down") + "; queries in flight: " + repr(queries.size())));
  std::vector<std::shared_ptr<ConnectWaiter>> connects = connects_;
  for (const std::shared_ptr<ConnectWaiter>& waiter : connects) {
    if (waiter->connection.is_same_connection(connection)) {
      _end_connect(waiter, up);
    }
  }
  if (up) {
    _post_fill();
    return;
  }
  for (auto& in_flight : queries) {
    switch (in_flight->stage) {
      case InFlight::REQUEST:
      case InFlight::DIRECT:
        if (in_flight->sent_via.is_same_connection(connection)) {
          _lost(in_flight);
        }
        break;
      case InFlight::SEARCH:
        // One connection fewer to answer the search, if it carried it.
        if (_answered(in_flight, connection) && in_flight->searched.empty()) {
          _start_query(in_flight, /*keep_deadline=*/true);
        }
        break;
      case InFlight::WAITING:
        break;
    }
  }
}

// The connection a request or a direct request went through is gone. A
// typed query sends the request again through another connection; an
// addressed one locates its addressee instead, since the instance may be
// behind another multiplexer now; through a pinned lane there is nothing
// else to try.
void ThreadedClient::Core::_lost(InFlightPtr in_flight) {
  if (in_flight->pinned()) {
    _finish(in_flight, NOT_CONNECTED, NULL);
    return;
  }
  MX_LOG(WARNING, MEDIUMVERBOSITY,
         CTX("ThreadedClient") TEXT("connection lost under query " + repr(in_flight->request_id) +
                                    (in_flight->addressed() ? "; locating the addressee" : "; sending again")));
  if (in_flight->addressed()) {
    _search(in_flight);
  } else {
    _start_query(in_flight, /*keep_deadline=*/true);
  }
}

// The query's message found no connection live: it waits, and goes again
// from the request when one comes up, within its deadline.
void ThreadedClient::Core::_wait_for_connection(const InFlightPtr& in_flight) {
  in_flight->stage = InFlight::WAITING;
  if (!in_flight->listed) {
    in_flight->listed = true;
    waiting_queries_.push_back(in_flight);
  }
}

// A connection came up: the waiting queries go again, in order; one that
// finds none live after all waits on.
void ThreadedClient::Core::_restart_waiting_queries() {
  std::deque<InFlightPtr> pass;
  pass.swap(waiting_queries_);
  waiting_ended_ = 0;
  for (const InFlightPtr& in_flight : pass) {
    in_flight->listed = false;
    if (in_flight->stage != InFlight::WAITING || !in_flight->callback) {
      continue;  // moved on, or finished
    }
    ++retries_;
    _start_query(in_flight, /*keep_deadline=*/true);
  }
}

// What the next stage's timer gets: the whole `timeout` for a typed query,
// what is left of the one deadline for an addressed one.
float ThreadedClient::Core::_stage_timeout(const InFlightPtr& in_flight) const {
  if (!in_flight->addressed()) {
    return in_flight->timeout;
  }
  std::chrono::steady_clock::duration left = in_flight->deadline - std::chrono::steady_clock::now();
  return std::max(0.0f, std::chrono::duration<float>(left).count());
}

void ThreadedClient::Core::_start_query(InFlightPtr in_flight, bool keep_deadline) {
  if (shut_down_) {
    _finish(in_flight, SHUT_DOWN, NULL);
    return;
  }
  // Ids of an earlier attempt stay tracked, so a late reply is accepted,
  // until the query ends.
  if (in_flight->request_id) {
    in_flight->earlier_ids.push_back(in_flight->request_id);
  }
  MultiplexerMessage request = in_flight->prototype;
  request.set_id(random64());
  in_flight->request_id = request.id();
  std::shared_ptr<const RawMessage> raw(RawMessage::FromMessage(request));
  ConnectionWrapper used;
  bool refused = false;
  if (!_schedule(raw, in_flight->lane, &used, &refused, _stage_timeout(in_flight))) {
    if (refused) {
      _finish(in_flight, NOT_CONNECTED, NULL);  // the pinned lane's connection is gone
      return;
    }
    // Nothing to send through: wait for a connection, within the deadline.
    _wait_for_connection(in_flight);
    if (!keep_deadline) {
      _arm(in_flight, in_flight->timeout);
    }
    return;
  }
  in_flight->sent_via = used;
  MX_LOG(DEBUG, CHATTERBOX,  // per message: off by default, MX_LOG_VERBOSITY=DEBUG:CHATTERBOX shows the traffic
         CTX("ThreadedClient") TEXT("request " + repr(in_flight->request_id) + " via " + repr(used.endpoint_)));
  _track(in_flight, in_flight->request_id);
  in_flight->stage = InFlight::REQUEST;
  if (!keep_deadline) {
    _arm(in_flight, in_flight->timeout);
  }
}

// A backend acknowledged an attempt of `in_flight` (notify_start()): its
// on_received, if any, is told which, and the query goes on as before. A
// search is no attempt: nothing acknowledges one.
void ThreadedClient::Core::_acknowledged(const InFlightPtr& in_flight, const MultiplexerMessage& msg) {
  const std::vector<std::uint64_t>& searches = in_flight->search_ids;
  if (!in_flight->received || std::find(searches.begin(), searches.end(), msg.references()) != searches.end()) {
    return;
  }
  _guarded([&] { in_flight->received(msg.from()); },
           [&] { return "the on_received of a query of type " + repr(in_flight->prototype.type()); });
}

void ThreadedClient::Core::_advance(InFlightPtr in_flight, const IncomingMessage& incoming) {
  const MultiplexerMessage& msg = *incoming.third;
  // What the message answers: one of the query's searches, whose answers
  // are PINGs and delivery errors, or an attempt of the request itself (the
  // first, a resend after a lost connection, the direct request), whose
  // answer is the reply, accepted in any stage. A search's answer is never
  // the reply: arriving after the query moved on, it is dropped.
  const std::vector<std::uint64_t>& searches = in_flight->search_ids;
  const bool to_a_search = std::find(searches.begin(), searches.end(), msg.references()) != searches.end();
  const bool delivery_error = msg.type() == types::DELIVERY_ERROR;
  if (!to_a_search && !delivery_error) {
    _finish(in_flight, REPLIED, &incoming);
    return;
  }
  switch (in_flight->stage) {
    case InFlight::REQUEST:
      if (!to_a_search && msg.references() == in_flight->request_id) {
        _search(in_flight);  // nobody took the request
      }
      return;  // a delivery error for an earlier attempt, an earlier search's answer

    case InFlight::SEARCH:
      if (msg.references() != in_flight->search_id) {
        return;  // for an earlier search or an earlier attempt
      }
      if (delivery_error) {
        // Every multiplexer the search went through reported no backend of
        // the type, so nothing can answer any more.
        _answered(in_flight, incoming.second);
        if (in_flight->searched.empty()) {
          _finish(in_flight, FAILED, NULL);
        }
      } else if (msg.type() == types::PING) {
        _direct(in_flight, incoming);
      }
      return;

    case InFlight::DIRECT:
      if (!to_a_search && msg.references() == in_flight->direct_id) {
        _finish(in_flight, FAILED, NULL);  // the backend that answered the search is gone
      }
      return;  // a later PING from another backend, a stale delivery error

    case InFlight::WAITING:
      return;
  }
}

// The middle stage: a search for a backend of the type on every
// connection, or, for an addressed query, a PING addressed to the
// instance, which the server classes and ThreadedClient answer whatever
// their search policy, with delivery errors requested so that a
// multiplexer without the instance says so. Through a pinned lane the one
// multiplexer behind it is asked instead of all.
void ThreadedClient::Core::_search(InFlightPtr in_flight) {
  float left = _stage_timeout(in_flight);
  if (in_flight->addressed() && left <= 0) {
    _finish(in_flight, TIMED_OUT, NULL);
    return;
  }
  MultiplexerMessage msg;
  if (in_flight->addressed()) {
    msg = new_message(types::PING, std::string());
    msg.set_to(in_flight->prototype.to());
    msg.set_report_delivery_error(true);
  } else {
    BackendForPacketSearch search;
    search.set_packet_type(in_flight->prototype.type());
    msg = new_message(types::BACKEND_FOR_PACKET_SEARCH, std::string());
    search.SerializeToString(msg.mutable_message());
  }
  std::shared_ptr<const RawMessage> raw(RawMessage::FromMessage(msg));
  std::vector<ConnectionWrapper> sent;
  if (in_flight->pinned()) {
    ConnectionWrapper used;
    bool refused = false;
    if (_schedule(raw, in_flight->lane, &used, &refused, left > 0 ? left : in_flight->timeout)) {
      sent.push_back(used);
    } else if (refused) {
      _finish(in_flight, NOT_CONNECTED, NULL);
      return;
    }
  } else {
    basic_client_->schedule_all(raw, &sent, left > 0 ? left : in_flight->timeout);
  }
  if (sent.empty()) {
    _wait_for_connection(in_flight);  // the request goes out again when a connection is back
    return;
  }
  in_flight->search_id = msg.id();
  in_flight->search_ids.push_back(msg.id());
  in_flight->searched = sent;
  _track(in_flight, in_flight->search_id);
  in_flight->stage = InFlight::SEARCH;
  _arm(in_flight, left);
}

// The search went through `connection` and has its answer from it now, a
// delivery error or the connection going down: it counts no more. False
// when the search never went through it.
bool ThreadedClient::Core::_answered(const InFlightPtr& in_flight, const ConnectionWrapper& connection) {
  std::vector<ConnectionWrapper>& searched = in_flight->searched;
  for (std::vector<ConnectionWrapper>::iterator it = searched.begin(); it != searched.end(); ++it) {
    if (it->is_same_connection(connection)) {
      searched.erase(it);
      return true;
    }
  }
  return false;
}

void ThreadedClient::Core::_direct(InFlightPtr in_flight, const IncomingMessage& ping) {
  MultiplexerMessage request = in_flight->prototype;
  request.set_id(random64());
  request.set_to(ping.third->from());
  if (in_flight->direct_id) {
    in_flight->earlier_ids.push_back(in_flight->direct_id);  // tracked still, as a request's
  }
  in_flight->direct_id = request.id();
  std::shared_ptr<const RawMessage> raw(RawMessage::FromMessage(request));
  if (in_flight->pinned()) {
    raw->mark_pinned();  // never handed to another connection if this one dies under it
  }
  // Through the connection the PING came on, which is known to reach that
  // backend; if it died meanwhile, any connection, since `to` is set, but
  // through a pinned lane only the lane's. The lane adopts it: what follows
  // the query goes the same way.
  const float left = _stage_timeout(in_flight);
  ConnectionWrapper used = ping.second;
  BasicClient::BasicScheduledMessageTracker tracker = basic_client_->schedule_on(raw, ping.second, left);
  if (tracker && in_flight->lane) {
    in_flight->lane->adopt(used);
  }
  if (!tracker) {
    bool refused = false;
    tracker = _schedule(raw, in_flight->lane, &used, &refused, left);
    if (refused) {
      _finish(in_flight, NOT_CONNECTED, NULL);
      return;
    }
  }
  if (!tracker) {
    _wait_for_connection(in_flight);
    return;
  }
  in_flight->sent_via = used;
  _track(in_flight, in_flight->direct_id);
  in_flight->stage = InFlight::DIRECT;
  _arm(in_flight, _stage_timeout(in_flight));
}

void ThreadedClient::Core::_arm(InFlightPtr in_flight, float timeout) {
  unsigned int generation = ++in_flight->generation;
  in_flight->timer->expires_after(mx::from_seconds(timeout));
  in_flight->timer->async_wait([this, in_flight, generation](const asio::error_code& error) {
    MX_DCHECK_RUN_ON(&io_thread_);
    _on_deadline(in_flight, generation, error);
  });
}

void ThreadedClient::Core::_on_deadline(InFlightPtr in_flight, unsigned int generation, const asio::error_code& error) {
  if (error == asio::error::operation_aborted || generation != in_flight->generation || !in_flight->callback) {
    return;
  }
  if (in_flight->stage == InFlight::REQUEST) {
    _search(in_flight);
  } else if (in_flight->stage == InFlight::WAITING) {
    _finish(in_flight, NOT_CONNECTED, NULL);
  } else {
    _finish(in_flight, TIMED_OUT, NULL);
  }
}

void ThreadedClient::Core::_finish(InFlightPtr in_flight, Outcome outcome, const IncomingMessage* reply) {
  asio::error_code ignored;
  in_flight->timer->cancel(ignored);
  ++in_flight->generation;
  // Every id the query was tracked under: the current request's and
  // direct request's, every search's, every earlier attempt's.
  by_id_.erase(in_flight->request_id);
  _remember_finished(in_flight->request_id);
  if (in_flight->direct_id) {
    by_id_.erase(in_flight->direct_id);
    _remember_finished(in_flight->direct_id);
  }
  for (std::uint64_t id : in_flight->search_ids) {
    by_id_.erase(id);
    _remember_finished(id);
  }
  for (std::uint64_t id : in_flight->earlier_ids) {
    by_id_.erase(id);
    _remember_finished(id);
  }
  if (in_flight->started) {
    in_flight_.erase(in_flight->place);
    in_flight->started = false;
  }
  Callback callback;
  callback.swap(in_flight->callback);  // a finished query never reports twice
  Result result;
  result.outcome = outcome;
  if (reply) {
    result.reply = *reply;
    if (in_flight->lane) {
      in_flight->lane->adopt(reply->second);  // what follows the query goes where the answer came from
    }
  }
  in_flight->lane.reset();                   // the query held it only for its own duration
  in_flight->received = ReceivedCallback();  // nothing more is told
  if (callback) {
    _guarded([&] { callback(result); },
             [&] { return "the callback of a query of type " + repr(in_flight->prototype.type()); });
  }
  if (in_flight->listed) {
    in_flight->prototype.Clear();  // its request goes now, its entry when the list is cleared out
    ++waiting_ended_;
    _clear_out_waiting();
  }
}

// The waiting queries that ended are cleared out once they are as many as
// those still waiting and 64 more, or none waits any more: however long no
// connection comes up, the list holds what waits and a bounded rest, at an
// amortized constant cost per query.
void ThreadedClient::Core::_clear_out_waiting() {
  const std::size_t live = waiting_queries_.size() - waiting_ended_;
  if (live != 0 && waiting_ended_ < live + 64) {
    return;
  }
  waiting_queries_.erase(std::remove_if(waiting_queries_.begin(), waiting_queries_.end(),
                                        [](const InFlightPtr& in_flight) {
                                          if (in_flight->callback) {
                                            return false;  // waiting still
                                          }
                                          in_flight->listed = false;
                                          return true;
                                        }),
                         waiting_queries_.end());
  waiting_ended_ = 0;
}

}  // namespace multiplexer
