// BaseThreadedMultiplexerServer: the queue between the io thread and the
// workers, the workers, and the request's replies. See the header.
#include "multiplexer/backend/base_threaded_multiplexer_server.h"

#include <new>

#include "lib/exception.h"
#include "lib/logging/logging.h"
#include "lib/repr.h"
#include "multiplexer/Multiplexer.pb.h"
#include "multiplexer/multiplexer.constants.h"

namespace multiplexer {
namespace backend {

using mx::repr;

Request::~Request() {
  if (!answered_.load() && !dropped_) {
    MX_LOG(WARNING, LOWVERBOSITY,
           CTX("BaseThreadedMultiplexerServer")
               TEXT("request #" + repr(mxmsg().id()) + " of type " + repr(mxmsg().type()) +
                    " dropped without a reply or no_response()"));
  }
}

void Request::reply(const std::string& payload, std::uint32_t type) { reply(client_->new_message(type, payload)); }

void Request::reply(MultiplexerMessage msg) {
  answered_ = true;
  if (!msg.id()) {
    msg.set_id(client_->random64());
  }
  if (!msg.from()) {
    msg.set_from(client_->instance_id());
  }
  if (!msg.to()) {
    msg.set_to(mxmsg().from());
  }
  if (!msg.references()) {
    msg.set_references(mxmsg().id());
  }
  if (msg.workflow().empty()) {
    msg.set_workflow(mxmsg().workflow());
  }
  client_->send(msg, connection());  // the way the request came, another when it is gone
}

void Request::report_error(const std::string& message) { reply(message, types::BACKEND_ERROR); }

void Request::notify_start() {
  bool answered = answered_.load();
  reply(std::string(), types::REQUEST_RECEIVED);
  answered_ = answered;
}

BaseThreadedMultiplexerServer::BaseThreadedMultiplexerServer(const MultiplexerAddresses& addresses, PeerType type,
                                                             const Options& options)
    : options_(options),
      addresses_(addresses),
      type_(type),
      client_(type, [this](const IncomingMessage& incoming) { _on_message(incoming); }) {
  if (options_.workers < 1) {
    throw std::invalid_argument("workers must be at least 1");
  }
  // A closing server answers no search: the request that would follow is refused.
  client_.set_search_policy([this] { return !closed_.load() && should_respond_to_backend_for_packet_search(); });
}

BaseThreadedMultiplexerServer::~BaseThreadedMultiplexerServer() {
  if (client_.orphaned()) {
    _forget_the_parents_threads();
    return;
  }
  close();
}

// Throws UsedAfterFork in a forked child, on a server the parent made,
// before any lock: the workers and the io thread take the mutexes for
// every request, and a lock one of them held at the fork is held for good.
void BaseThreadedMultiplexerServer::_check_not_inherited() const {
  if (client_.orphaned()) {
    MXTHROW(ThreadedClient::UsedAfterFork());
  }
}

// A forked child's server, on its way out: the workers and the thread that
// served are the parent's and do not exist here, and they may have held
// the mutexes or waited on the condition variables at the fork. So the
// thread handles are detached (destroying one joinable would terminate),
// the condition variables get fresh state without their destructors
// running (glibc's waits for waiters that existed at the fork, which here
// is for good; their old state leaks), and the queued requests go
// without a warning. client_ then tears itself down as an orphan. No lock
// is taken: the child has no other thread that could race.
void BaseThreadedMultiplexerServer::_forget_the_parents_threads() MX_NO_THREAD_SAFETY_ANALYSIS {
  for (std::thread& thread : threads_) {
    if (thread.joinable()) {
      thread.detach();
    }
  }
  new (&cond_) std::condition_variable_any();
  new (&wake_) std::condition_variable_any();
  for (const RequestPtr& request : queue_) {
    request->dropped_ = true;
  }
}

void BaseThreadedMultiplexerServer::serve_forever(float poll, float drain_seconds) {
  _check_not_inherited();
  drain_seconds_ = drain_seconds;
  try {
    connect();
    for (;;) {
      {
        mx::UniqueLock lock(wake_mutex_);
        wake_.wait_for(lock, std::chrono::microseconds(static_cast<long>(poll * 1e6)));
      }
      if (!working.load() || (draining() && drained()) || failure_) {
        break;
      }
      periodic_task();
    }
  } catch (...) {
    close();
    throw;
  }
  close();
  if (failure_) {
    std::rethrow_exception(failure_);
  }
}

void BaseThreadedMultiplexerServer::close() {
  _check_not_inherited();
  std::vector<std::thread> threads;
  {
    mx::MutexLock lock(mutex_);
    for (const std::thread& thread : threads_) {
      if (thread.get_id() == std::this_thread::get_id()) {
        throw std::logic_error("close() called from a worker thread, which it would join; call stop() instead");
      }
    }
  }
  mx::MutexLock closing(close_mutex_);  // a second close() returns once the first is done, not before
  if (closed_.exchange(true)) {
    return;
  }
  {
    mx::MutexLock lock(mutex_);  // before the drain shows: a request seeing draining() must find the door shut
    accepting_ = false;
    cond_.notify_all();
    threads.swap(threads_);
  }
  start_draining();
  stop();  // a serve_forever() still running on another thread returns now rather than polling a closed client
  for (std::thread& thread : threads) {
    thread.join();
  }
  client_.flush_all(CLOSE_FLUSH_SECONDS);  // the last replies go out before the sockets close
  client_.shutdown();
}

void BaseThreadedMultiplexerServer::start_draining() {
  // The time first, so that no thread sees draining() without it, and only
  // by the first call: a later one must not move the start, or a program
  // that asks on every poll would never reach drain_seconds.
  std::chrono::steady_clock::rep unset = 0;
  draining_since_ticks_.compare_exchange_strong(unset, std::chrono::steady_clock::now().time_since_epoch().count());
  if (!draining_.exchange(true)) {
    client_.set_routing(options_.drain_routing);  // close()'s too; nothing on a client already shut down
    mx::MutexLock lock(wake_mutex_);
    wake_.notify_all();
  }
}

bool BaseThreadedMultiplexerServer::drained() const {
  if (!draining()) {
    return false;
  }
  if (closed_.load()) {
    return true;  // nothing more is coming through a closed client
  }
  const std::chrono::steady_clock::time_point since(std::chrono::steady_clock::duration(draining_since_ticks_.load()));
  if (std::chrono::steady_clock::now() - since >= std::chrono::microseconds(static_cast<long>(drain_seconds_ * 1e6))) {
    return true;
  }
  if (!nothing_more_arrives(options_.drain_routing) || pending() != 0) {
    return false;
  }
  try {
    return client_.routing_acknowledged();
  } catch (const ThreadedClient::NotConnected&) {
    return true;  // shut down under us: nothing more is coming
  }
}

void BaseThreadedMultiplexerServer::stop() {
  working = false;
  if (client_.orphaned()) {
    return;  // a forked child's: nothing serves here, and the lock may be the parent's serving thread's
  }
  mx::MutexLock lock(wake_mutex_);
  wake_.notify_all();
}

std::size_t BaseThreadedMultiplexerServer::pending() const {
  _check_not_inherited();
  mx::MutexLock lock(mutex_);
  return queue_.size() + busy_;
}

bool BaseThreadedMultiplexerServer::should_respond_to_backend_for_packet_search() const {
  if (options_.decline_searches_when_full) {
    mx::MutexLock lock(mutex_);
    return busy_ < options_.workers || queue_.empty();
  }
  return true;
}

// The io thread: queue the message for a worker; drop it when the queue
// is full, as a full queue on the multiplexer drops; refuse it when
// leaving, with the DELIVERY_ERROR a multiplexer sends for a peer that is
// gone, so that a query retries elsewhere at once. Few arrive then: the
// multiplexers route nothing new to a draining backend once they have
// its routing; this covers what was routed before.
void BaseThreadedMultiplexerServer::_on_message(const IncomingMessage& incoming) {
  RequestPtr request(new Request(&client_, incoming));
  bool accepting;
  {
    mx::MutexLock lock(mutex_);
    if (accepting_ && queue_.size() < options_.queue_size) {
      queue_.push_back(request);
      cond_.notify_one();
      return;
    }
    accepting = accepting_;
    ++dropped_;
  }
  request->dropped_ = true;  // said below, not by the destructor
  const std::string what = "request #" + repr(incoming.third->id()) + " of type " + repr(incoming.third->type());
  if (accepting) {
    // Counted with the client's own drop lines: at most about two a second.
    if (client_.drop_lines().first({BasicClient::REQUESTS_QUEUE_FULL, WARNING, 0, 0},
                                   [] { return "requests dropped: queue full"; })) {
      MX_LOG(WARNING, LogSummary::VERBOSITY, CTX("BaseThreadedMultiplexerServer") TEXT(what + " dropped: queue full"));
    }
    return;
  }
  const MultiplexerMessage& msg = *incoming.third;
  if (msg.references()) {
    // A message that answers another is dropped: nobody retries a reply, and
    // refusing one could start a loop, a peer whose handler raised on the
    // refusal answering it with BACKEND_ERROR, refused in turn, until the
    // close ended.
    MX_LOG(DEBUG, LOWVERBOSITY,
           CTX("BaseThreadedMultiplexerServer")
               TEXT("reply #" + repr(msg.id()) + " of type " + repr(msg.type()) + " dropped: leaving"));
    return;
  }
  MX_LOG(DEBUG, LOWVERBOSITY, CTX("BaseThreadedMultiplexerServer") TEXT(what + " refused: leaving"));
  // A rule reports delivery errors unless told not to, and so does this; a
  // sender that set the message's own flag to false hears nothing.
  const bool wanted = !msg.has_report_delivery_error() || msg.report_delivery_error();
  if (wanted && msg.type() > types::MAX_MULTIPLEXER_META_PACKET) {
    DeliveryError error;
    error.set_packet_id(incoming.third->id());
    error.add_failed_type(type_);
    request->reply(error.SerializeAsString(), types::DELIVERY_ERROR);
  }
}

void BaseThreadedMultiplexerServer::connect() {
  _check_not_inherited();
  if (connected_.exchange(true) || closed_.load()) {
    return;
  }
  _start_workers();  // before the first connection, so that nothing waits for a worker
  for (const MultiplexerAddress& address : addresses_) {
    client_.connect(address.first, address.second, options_.connect_timeout);
  }
}

// The workers, started once, from connect() and before it connects:
// a request must never reach handle_message() on an object whose subclass
// is not built yet, and once connected it must not wait for a worker.
void BaseThreadedMultiplexerServer::_start_workers() {
  mx::MutexLock lock(mutex_);
  if (!accepting_ || !threads_.empty()) {
    return;  // closed already, or served before
  }
  for (unsigned int index = 0; index < options_.workers; ++index) {
    threads_.emplace_back(&BaseThreadedMultiplexerServer::_work, this);
  }
}

// A worker: the next request through the handler, until told to leave and
// the queue is empty.
void BaseThreadedMultiplexerServer::_work() {
  for (;;) {
    RequestPtr request;
    {
      mx::UniqueLock lock(mutex_);
      while (queue_.empty() && accepting_) {
        cond_.wait(lock);
      }
      if (queue_.empty()) {
        return;
      }
      request = queue_.front();
      queue_.pop_front();
      ++busy_;
    }
    _handle(request);
    mx::MutexLock lock(mutex_);
    --busy_;
  }
}

// One request through handle_message, with the exception rules of
// BaseMultiplexerServer: the requester hears BACKEND_ERROR unless a reply
// went out, then on_handler_exception decides.
void BaseThreadedMultiplexerServer::_handle(const RequestPtr& request) {
  try {
    handle_message(request);
  } catch (const std::exception& error) {
    MX_LOG(ERROR, LOWVERBOSITY,
           CTX("BaseThreadedMultiplexerServer") TEXT(std::string("exception in handle_message: ") + error.what()));
    if (!request->answered()) {
      request->report_error(error.what());
    }
    if (!on_handler_exception(error)) {
      failure_ = std::current_exception();
      stop();
    }
  }
}

}  // namespace backend
}  // namespace multiplexer
