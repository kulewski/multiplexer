// BaseThreadedMultiplexerServer: the queue between the io thread and the
// workers, the workers, and the request's replies. See the header.
#include "multiplexer/backend/base_threaded_multiplexer_server.h"

#include "lib/logging/logging.h"
#include "lib/repr.h"
#include "multiplexer/multiplexer.constants.h"

namespace multiplexer {
namespace backend {

using mx::repr;

Request::~Request() {
  if (!answered_.load() && !dropped_)
    MX_LOG(WARNING, LOWVERBOSITY,
           CTX("BaseThreadedMultiplexerServer")
               TEXT("request #" + repr(mxmsg().id()) + " of type " + repr(mxmsg().type()) +
                    " dropped without a reply or no_response()"));
}

void Request::reply(const std::string &payload, boost::uint32_t type) { reply(client_->new_message(type, payload)); }

void Request::reply(MultiplexerMessage msg) {
  answered_ = true;
  if (!msg.id())
    msg.set_id(client_->random64());
  if (!msg.from())
    msg.set_from(client_->instance_id());
  if (!msg.to())
    msg.set_to(mxmsg().from());
  if (!msg.references())
    msg.set_references(mxmsg().id());
  if (msg.workflow().empty())
    msg.set_workflow(mxmsg().workflow());
  client_->send(msg, connection()); // the way the request came, another when it is gone
}

void Request::report_error(const std::string &message) { reply(message, types::BACKEND_ERROR); }

void Request::notify_start() {
  bool answered = answered_.load();
  reply(std::string(), types::REQUEST_RECEIVED);
  answered_ = answered;
}

BaseThreadedMultiplexerServer::BaseThreadedMultiplexerServer(const MultiplexerAddresses &addresses, PeerType type,
                                                             const Options &options)
    : options_(options), client_(type, [this](const IncomingMessage &incoming) { _on_message(incoming); }) {
  if (options_.workers < 1)
    throw std::invalid_argument("workers must be at least 1");
  client_.set_search_policy([this] { return should_respond_to_backend_for_packet_search(); });
  for (unsigned int index = 0; index < options_.workers; ++index)
    threads_.emplace_back(&BaseThreadedMultiplexerServer::_work, this);
  for (const MultiplexerAddress &address : addresses)
    client_.connect(address.first, address.second, options_.connect_timeout);
}

BaseThreadedMultiplexerServer::~BaseThreadedMultiplexerServer() { close(); }

void BaseThreadedMultiplexerServer::serve_forever(float poll, float drain_seconds) {
  drain_seconds_ = drain_seconds;
  try {
    for (;;) {
      {
        mx::UniqueLock lock(wake_mutex_);
        wake_.wait_for(lock, std::chrono::microseconds(static_cast<long>(poll * 1e6)));
      }
      if (!working.load() || (draining() && drained()) || failure_)
        break;
      periodic_task();
    }
  } catch (...) {
    close();
    throw;
  }
  close();
  if (failure_)
    std::rethrow_exception(failure_);
}

void BaseThreadedMultiplexerServer::close() {
  for (const std::thread &thread : threads_)
    if (thread.get_id() == std::this_thread::get_id())
      throw std::logic_error("close() called from a worker thread, which it would join; call stop() instead");
  if (closed_.exchange(true))
    return;
  {
    mx::MutexLock lock(mutex_);
    accepting_ = false;
    cond_.notify_all();
  }
  for (std::thread &thread : threads_)
    thread.join();
  threads_.clear();
  client_.shutdown();
}

void BaseThreadedMultiplexerServer::start_draining() {
  if (!draining_.exchange(true)) {
    draining_since_ = std::chrono::steady_clock::now();
    mx::MutexLock lock(wake_mutex_);
    wake_.notify_all();
  }
}

bool BaseThreadedMultiplexerServer::drained() const {
  return draining() && std::chrono::steady_clock::now() - draining_since_ >=
                           std::chrono::microseconds(static_cast<long>(drain_seconds_ * 1e6));
}

void BaseThreadedMultiplexerServer::stop() {
  working = false;
  mx::MutexLock lock(wake_mutex_);
  wake_.notify_all();
}

std::size_t BaseThreadedMultiplexerServer::pending() const {
  mx::MutexLock lock(mutex_);
  return queue_.size() + busy_;
}

bool BaseThreadedMultiplexerServer::should_respond_to_backend_for_packet_search() const {
  if (draining())
    return false;
  if (options_.decline_searches_when_full) {
    mx::MutexLock lock(mutex_);
    return busy_ < options_.workers || queue_.empty();
  }
  return true;
}

// The io thread: queue the message for a worker, or drop it when the
// queue is full, as a full queue on the multiplexer drops.
void BaseThreadedMultiplexerServer::_on_message(const IncomingMessage &incoming) {
  RequestPtr request(new Request(&client_, incoming));
  {
    mx::MutexLock lock(mutex_);
    if (accepting_ && queue_.size() < options_.queue_size) {
      queue_.push_back(request);
      cond_.notify_one();
      return;
    }
    request->dropped_ = true;
    ++dropped_;
    MX_LOG(WARNING, LOWVERBOSITY,
           CTX("BaseThreadedMultiplexerServer")
               TEXT("request #" + repr(incoming.third->id()) + " of type " + repr(incoming.third->type()) +
                    " dropped: " + (accepting_ ? "queue full" : "leaving")));
  }
}

// A worker: the next request through the handler, until told to leave and
// the queue is empty.
void BaseThreadedMultiplexerServer::_work() {
  for (;;) {
    RequestPtr request;
    {
      mx::UniqueLock lock(mutex_);
      while (queue_.empty() && accepting_)
        cond_.wait(lock);
      if (queue_.empty())
        return;
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
void BaseThreadedMultiplexerServer::_handle(const RequestPtr &request) {
  try {
    handle_message(request);
  } catch (const std::exception &error) {
    MX_LOG(ERROR, LOWVERBOSITY,
           CTX("BaseThreadedMultiplexerServer") TEXT(std::string("exception in handle_message: ") + error.what()));
    if (!request->answered())
      request->report_error(error.what());
    if (!on_handler_exception(error)) {
      failure_ = std::current_exception();
      stop();
    }
  }
}

} // namespace backend
} // namespace multiplexer
