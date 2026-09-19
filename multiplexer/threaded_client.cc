// ThreadedClient: the io thread, the cross-thread calls, and the query
// state machine. See the header for the design.
#include "multiplexer/threaded_client.h"

#include <algorithm>
#include <chrono>
#include <future>

#include <asio/ip/tcp.hpp>
#include <stdexcept>

#include <chrono>

#include "lib/logging/logging.h"
#include "lib/repr.h"
#include "multiplexer/Multiplexer.pb.h" /* generated */
#include "multiplexer/multiplexer.constants.h"

namespace multiplexer {

using mx::repr;

// One query in flight. Owned by the io thread; the shared_ptr keeps it alive
// for the timer callback.
struct ThreadedClient::InFlight {
  // WAITING: no connection was live when the request had to be (re)sent; it
  // goes out as soon as one registers, the deadline still running. SEARCH
  // is the locate phase of an addressed query too: the probe out, the
  // answers counted the same way.
  enum Stage { REQUEST, SEARCH, DIRECT, WAITING } stage = REQUEST;
  ConnectionWrapper sent_via;   // REQUEST and DIRECT: the connection used
  MultiplexerMessage prototype; // the request; id and from set per attempt
  float timeout = 0;
  // An addressed query's one deadline across its stages; a typed query
  // arms each stage with `timeout`.
  std::chrono::steady_clock::time_point deadline;
  Probe probe = PROBE_SEARCH;
  LanePtr lane; // held until the query ends, then released
  std::uint64_t request_id = 0, search_id = 0, direct_id = 0;
  unsigned int pending_delivery_errors = 0; // during SEARCH: connections yet to answer
  unsigned int generation = 0;              // bumped per stage so stale deadlines are ignored
  Callback callback;
  std::unique_ptr<asio::steady_timer> timer;
  bool addressed() const { return prototype.to() != 0; }
  bool pinned() const { return lane && lane->pinned(); }
};

const IncomingMessage &ThreadedClient::Result::check() const {
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
  MXTHROW(OperationFailed()); // unreachable
}

ThreadedClient::ThreadedClient(std::uint32_t peer_type, MessageSink on_message)
    : peer_type_(peer_type), io_service_holder_(new asio::io_service()), io_service_(*io_service_holder_),
      work_(new asio::io_service::work(io_service_)), basic_client_(BasicClient::Create(io_service_, peer_type)),
      instance_id_(basic_client_->instance_id()), on_message_(on_message) {
  // The io thread binds the checkers to itself and installs the sink before
  // the constructor returns, so no call from another thread can run first
  // and bind them to the wrong thread.
  std::promise<void> ready;
  io_service_.post([this, &ready] {
    io_thread_.bind_to_current();
    MX_DCHECK_RUN_ON(&io_thread_);
    basic_client_->set_incoming_sink([this](const BasicClient::IncomingMessagesBuffer::value_type &incoming) {
      MX_DCHECK_RUN_ON(&io_thread_);
      _on_incoming(incoming);
    });
    basic_client_->set_connection_observer([this](const ConnectionWrapper &connection, bool up) {
      MX_DCHECK_RUN_ON(&io_thread_);
      _on_connection(connection, up);
    });
    ready.set_value();
  });
  thread_ = std::thread(&ThreadedClient::_io_thread_main, this);
  ready.get_future().wait();
}

ThreadedClient::~ThreadedClient() {
  if (orphaned()) {
    _orphan_teardown();
    return;
  }
  shutdown();
}

// The child's side of a fork: the io thread does not exist here (joining
// its handle would hang, destroying it joinable would terminate), the
// sockets are the parent's, and any lock a parent thread held is held
// forever. So: detach the handle, close the child's descriptor copies with
// close(2) only, and leak everything asio owns.
void ThreadedClient::_orphan_teardown() {
  if (thread_.joinable())
    thread_.detach();
  basic_client_->orphan_close_descriptors();
  new std::shared_ptr<BasicClient>(basic_client_); // leaked on purpose, see the header
  work_.release();
  io_service_holder_.release();
}

void ThreadedClient::_io_thread_main() {
  // A bug in one handler must not take the whole client down: log and keep
  // running, as the multiplexer's own loop does.
  for (;;) {
    try {
      io_service_.run();
      return;
    } catch (const std::exception &e) {
      MX_LOG(ERROR, LOWVERBOSITY,
             CTX("ThreadedClient") TEXT(std::string("exception escaped an io handler: ") + e.what()));
    }
  }
}

template <typename F> void ThreadedClient::_post(F function) { io_service_.post(function); }

// Runs `function` on the io thread and returns its result; for the short
// bookkeeping calls only. Deadlocks if called on the io thread, hence the
// assertion.
template <typename F> auto ThreadedClient::_call(F function) -> decltype(function()) {
  DbgAssertMsg(!io_thread_.is_current(), "blocking call on the io thread");
  basic_client_->check_not_orphaned();
  if (_stopped())
    MXTHROW(NotConnected());
  typedef decltype(function()) R;
  std::promise<R> promise;
  std::future<R> future = promise.get_future();
  io_service_.post([&] {
    try {
      promise.set_value(function());
    } catch (...) {
      promise.set_exception(std::current_exception()); // never leave the caller waiting
    }
  });
  return future.get();
}

std::uint64_t ThreadedClient::random64() {
  basic_client_->check_not_orphaned();
  mx::MutexLock lock(random_mutex_);
  return random_();
}

MultiplexerMessage ThreadedClient::new_message(std::uint32_t type, const std::string &payload) {
  MultiplexerMessage msg;
  msg.set_id(random64());
  msg.set_from(instance_id_);
  msg.set_type(type);
  msg.set_message(payload);
  return msg;
}

bool ThreadedClient::connect(const std::string &host, std::uint16_t port, float timeout) {
  asio::ip::tcp::resolver resolver(io_service_);
  asio::ip::tcp::resolver::iterator found = resolver.resolve(asio::ip::tcp::resolver::query(host, repr(port)));
  asio::ip::tcp::endpoint endpoint = *found;
  ConnectionWrapper wrapper = _call([&] {
    MX_DCHECK_RUN_ON(&io_thread_);
    return basic_client_->async_connect(endpoint);
  });
  // The handshake completes on the io thread; wait for it here by asking
  // every 20 ms, up to the deadline. Connecting is rare, so polling is fine.
  std::chrono::steady_clock::time_point deadline =
      std::chrono::steady_clock::now() + std::chrono::milliseconds(static_cast<long>(timeout * 1000));
  for (;;) {
    int state = _call([&] {
      MX_DCHECK_RUN_ON(&io_thread_);
      if (BasicClient::Connection::pointer conn = wrapper.lock())
        return conn->registered() ? 1 : conn->shuts_down() ? -1 : 0;
      return -1;
    });
    if (state == 1)
      return true;
    if (state == -1 || std::chrono::steady_clock::now() >= deadline)
      return false;
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
}

void ThreadedClient::set_search_policy(SearchPolicy answer) {
  _call([&] {
    MX_DCHECK_RUN_ON(&io_thread_);
    search_policy_ = answer;
    return 0;
  });
}

unsigned int ThreadedClient::connections_count() {
  return _call([&] {
    MX_DCHECK_RUN_ON(&io_thread_);
    return basic_client_->connections_count(true);
  });
}

// One send that is not written yet; see _advance_sends.
struct ThreadedClient::PendingSend {
  std::shared_ptr<const RawMessage> raw;
  bool all = false;  // every connection, or one
  bool wait = false; // a flushing send: report through `done` when written
  std::chrono::steady_clock::time_point deadline;
  std::vector<Client::ScheduledMessageTracker> trackers; // one per connection written to (ALL), or one
  ConnectionWrapper used;                                // ONE: the connection the message is queued on
  LanePtr lane;                                          // ONE: the lane to go through, if any
  bool refused = false;                                  // a pinned lane's connection is gone: nothing more to try
  unsigned int sent = 0;
  SendCallback done; // a flushing send's completion: the count written, on the io thread
};

namespace {
// A blocking send's completion: the promise its caller waits on.
ThreadedClient::SendCallback settle(std::shared_ptr<std::promise<unsigned int>> promise) {
  return [promise](unsigned int written) { promise->set_value(written); };
}
} // namespace

void ThreadedClient::send(const MultiplexerMessage &msg) { send(msg, LanePtr()); }

void ThreadedClient::send(const MultiplexerMessage &msg, LanePtr lane) {
  _submit_send(std::shared_ptr<const RawMessage>(RawMessage::FromMessage(msg)), false, false, DEFAULT_TIMEOUT,
               ThreadedClient::SendCallback(), lane);
}

void ThreadedClient::send_all(const MultiplexerMessage &msg) {
  _submit_send(std::shared_ptr<const RawMessage>(RawMessage::FromMessage(msg)), true, false, DEFAULT_TIMEOUT,
               ThreadedClient::SendCallback(), LanePtr());
}

void ThreadedClient::send(const MultiplexerMessage &msg, const ConnectionWrapper &connection) {
  send(msg, std::make_shared<Lane>(connection));
}

unsigned int ThreadedClient::send(const MultiplexerMessage &msg, float timeout) {
  return send(msg, LanePtr(), timeout);
}

unsigned int ThreadedClient::send(const MultiplexerMessage &msg, LanePtr lane, float timeout) {
  if (io_thread_.is_current())
    throw std::logic_error("flushing ThreadedClient::send() called on the io thread, from a callback");
  std::shared_ptr<std::promise<unsigned int>> promise(new std::promise<unsigned int>());
  std::future<unsigned int> future = promise->get_future();
  _submit_send(std::shared_ptr<const RawMessage>(RawMessage::FromMessage(msg)), false, true, timeout, settle(promise),
               lane);
  return future.get();
}

unsigned int ThreadedClient::send(const MultiplexerMessage &msg, const ConnectionWrapper &connection, float timeout) {
  return send(msg, std::make_shared<Lane>(connection), timeout);
}

unsigned int ThreadedClient::send_all(const MultiplexerMessage &msg, float timeout) {
  if (io_thread_.is_current())
    throw std::logic_error("flushing ThreadedClient::send_all() called on the io thread, from a callback");
  std::shared_ptr<std::promise<unsigned int>> promise(new std::promise<unsigned int>());
  std::future<unsigned int> future = promise->get_future();
  _submit_send(std::shared_ptr<const RawMessage>(RawMessage::FromMessage(msg)), true, true, timeout, settle(promise),
               LanePtr());
  return future.get();
}

void ThreadedClient::send_serialized(std::string serialized, LanePtr lane) {
  _submit_send(std::shared_ptr<const RawMessage>(new RawMessage(&serialized)), false, false, DEFAULT_TIMEOUT,
               ThreadedClient::SendCallback(), lane);
}

void ThreadedClient::send_all_serialized(std::string serialized) {
  _submit_send(std::shared_ptr<const RawMessage>(new RawMessage(&serialized)), true, false, DEFAULT_TIMEOUT,
               ThreadedClient::SendCallback(), LanePtr());
}

unsigned int ThreadedClient::send_serialized_and_wait(std::string serialized, bool all, float timeout, LanePtr lane) {
  if (io_thread_.is_current())
    throw std::logic_error("flushing ThreadedClient send called on the io thread, from a callback");
  std::shared_ptr<std::promise<unsigned int>> promise(new std::promise<unsigned int>());
  std::future<unsigned int> future = promise->get_future();
  _submit_send(std::shared_ptr<const RawMessage>(new RawMessage(&serialized)), all, true, timeout, settle(promise),
               lane);
  return future.get();
}

void ThreadedClient::send_serialized_with_callback(std::string serialized, bool all, float timeout, SendCallback done,
                                                   LanePtr lane) {
  _submit_send(std::shared_ptr<const RawMessage>(new RawMessage(&serialized)), all, true, timeout, done, lane);
}

// Hands a send to the io thread. Never blocks the caller: a plain post, so
// callbacks may send. A flushing send carries a completion the io thread
// calls once the message is written or the deadline passed.
void ThreadedClient::_submit_send(std::shared_ptr<const RawMessage> raw, bool all, bool wait, float timeout,
                                  SendCallback done, LanePtr lane) {
  basic_client_->check_not_orphaned();
  if (_stopped())
    MXTHROW(NotConnected());
  PendingSendPtr pending(new PendingSend());
  pending->raw = raw;
  pending->all = all;
  pending->wait = wait;
  pending->deadline = std::chrono::steady_clock::now() + std::chrono::microseconds(static_cast<long>(timeout * 1e6));
  pending->done = done;
  pending->lane = lane;
  _post([this, pending] {
    MX_DCHECK_RUN_ON(&io_thread_);
    if (shut_down_) {
      if (pending->done)
        pending->done(0);
      return;
    }
    pending_sends_.push_back(pending);
    _advance_sends();
  });
}

// Queues `pending` on the connection(s) it still needs: for ONE, on some
// live connection unless it is already queued or written; for ALL, on
// every live connection the first time.
void ThreadedClient::_attempt_send(const PendingSendPtr &pending) {
  if (pending->all) {
    if (!pending->trackers.empty())
      return; // written to the connections that were live at the time
    for (BasicClient::ConnectionById::const_iterator entry = basic_client_->begin(); entry != basic_client_->end();
         ++entry) {
      if (BasicClient::Connection::pointer conn = entry->second.lock()) {
        if (!conn->living() || conn->outgoing_queue_full())
          continue;
        Client::ScheduledMessageTracker tracker(conn->schedule(pending->raw));
        if (tracker)
          pending->trackers.push_back(tracker);
      }
    }
    return;
  }
  if (!pending->trackers.empty() && !pending->trackers.front().is_lost())
    return; // queued or written already
  pending->trackers.clear();
  ConnectionWrapper used;
  Client::ScheduledMessageTracker tracker(_schedule(pending->raw, pending->lane, &used, &pending->refused));
  if (tracker) {
    pending->trackers.push_back(tracker);
    pending->used = used;
  }
}

// Queues `raw` through `lane`: on the lane's connection while it is live,
// else on any live connection, which a lane that is not pinned adopts;
// with no lane, on any. Null when nothing took it; *refused is set when a
// pinned lane's connection is gone or full, so the caller stops trying.
BasicClient::BasicScheduledMessageTracker ThreadedClient::_schedule(const std::shared_ptr<const RawMessage> &raw,
                                                                    const LanePtr &lane, ConnectionWrapper *used,
                                                                    bool *refused) {
  if (lane && lane->pinned())
    raw->mark_pinned(); // never handed to another connection if this one dies under it
  if (lane && lane->holds_connection()) {
    ConnectionWrapper held = lane->connection();
    if (BasicClient::Connection::pointer conn = held.lock())
      if (conn->living()) {
        BasicClient::BasicScheduledMessageTracker tracker = conn->schedule(raw);
        if (tracker) {
          *used = held;
          return tracker;
        }
      }
    if (lane->pinned()) {
      *refused = true;
      return BasicClient::BasicScheduledMessageTracker();
    }
  }
  BasicClient::BasicScheduledMessageTracker tracker = basic_client_->schedule_one(raw, used);
  if (tracker && lane)
    lane->adopt(*used);
  return tracker;
}

// One pass over the pending sends: attempt what is not queued, settle
// what is written or past its deadline, and arm the poll timer while any
// remain. A flushing ONE send whose connection died is sent again through
// another, so a multiplexer restart under an event costs the reconnect
// delay, not the event.
void ThreadedClient::_advance_sends() {
  std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now();
  std::vector<PendingSendPtr> still_pending;
  for (auto &pending : pending_sends_) {
    _attempt_send(pending);
    bool queued = !pending->trackers.empty();
    if (!pending->wait) {
      // Fire and forget: done once queued somewhere; the connection layer
      // takes it from here.
      if (queued)
        continue;
      if (pending->refused) {
        MX_LOG(WARNING, MEDIUMVERBOSITY,
               CTX("ThreadedClient") TEXT("message dropped: the pinned lane's connection is gone"));
        continue;
      }
      if (now >= pending->deadline) {
        MX_LOG(WARNING, MEDIUMVERBOSITY, CTX("ThreadedClient") TEXT("event dropped: no connection came up in time"));
        continue;
      }
      still_pending.push_back(pending);
      continue;
    }
    if (pending->refused) {
      pending->done(0); // nothing else may carry it
      continue;
    }
    unsigned int sent = 0, in_queue = 0;
    for (auto &tracker : pending->trackers) {
      if (tracker.is_sent())
        ++sent;
      else if (tracker.in_queue())
        ++in_queue;
    }
    if (queued && in_queue == 0 && (pending->all || sent)) {
      pending->done(sent); // every copy written, or lost on a connection that died
      continue;
    }
    if (now >= pending->deadline) {
      pending->done(sent);
      continue;
    }
    still_pending.push_back(pending);
  }
  pending_sends_.swap(still_pending);
  if (pending_sends_.empty())
    return;
  if (!send_timer_)
    send_timer_.reset(new asio::steady_timer(io_service_));
  send_timer_->expires_after(std::chrono::milliseconds(5));
  send_timer_->async_wait([this](const asio::error_code &error) {
    MX_DCHECK_RUN_ON(&io_thread_);
    if (!error && !shut_down_)
      _advance_sends();
  });
}

void ThreadedClient::query(const std::string &payload, std::uint32_t type, Callback callback, float timeout,
                           LanePtr lane) {
  MultiplexerMessage msg;
  msg.set_type(type);
  msg.set_message(payload);
  query(msg, callback, timeout, lane, PROBE_SEARCH);
}

ThreadedClient::Result ThreadedClient::query(const std::string &payload, std::uint32_t type, float timeout,
                                             LanePtr lane) {
  MultiplexerMessage msg;
  msg.set_type(type);
  msg.set_message(payload);
  return query(msg, timeout, lane, PROBE_SEARCH);
}

void ThreadedClient::query(const MultiplexerMessage &msg, Callback callback, float timeout, LanePtr lane, Probe probe) {
  basic_client_->check_not_orphaned();
  if (_stopped()) {
    Result result;
    result.outcome = SHUT_DOWN;
    callback(result);
    return;
  }
  InFlightPtr in_flight(new InFlight());
  in_flight->prototype = msg;
  in_flight->prototype.set_from(instance_id_);
  if (msg.to())
    in_flight->prototype.set_report_delivery_error(true); // "not behind this multiplexer" must come back
  in_flight->timeout = timeout;
  in_flight->deadline = std::chrono::steady_clock::now() + std::chrono::microseconds(static_cast<long>(timeout * 1e6));
  in_flight->probe = probe;
  in_flight->lane = lane;
  in_flight->callback = callback;
  in_flight->timer.reset(new asio::steady_timer(io_service_));
  _post([this, in_flight] {
    MX_DCHECK_RUN_ON(&io_thread_);
    in_flight_.push_back(in_flight);
    _start_query(in_flight, /*keep_deadline=*/false);
  });
}

ThreadedClient::Result ThreadedClient::query(const MultiplexerMessage &msg, float timeout, LanePtr lane, Probe probe) {
  if (io_thread_.is_current())
    throw std::logic_error("blocking ThreadedClient::query() called on the io thread, from a callback; "
                           "use the callback form there");
  std::shared_ptr<std::promise<Result>> promise(new std::promise<Result>());
  std::future<Result> future = promise->get_future();
  query(msg, [promise](const Result &result) { promise->set_value(result); }, timeout, lane, probe);
  return future.get();
}

void ThreadedClient::query(const MultiplexerMessage &msg, const ConnectionWrapper &connection, Callback callback,
                           float timeout, Probe probe) {
  query(msg, callback, timeout, std::make_shared<Lane>(connection), probe);
}

ThreadedClient::Result ThreadedClient::query(const MultiplexerMessage &msg, const ConnectionWrapper &connection,
                                             float timeout, Probe probe) {
  return query(msg, timeout, std::make_shared<Lane>(connection), probe);
}

void ThreadedClient::shutdown() {
  if (orphaned()) {
    _orphan_teardown();
    return;
  }
  DbgAssertMsg(!io_thread_.is_current(), "shutdown() from a callback would join the thread it runs on");
  {
    mx::MutexLock lock(lifecycle_mutex_);
    if (stopped_)
      return; // already done, or being done by another thread
    stopped_ = true;
  }
  _post([this] {
    MX_DCHECK_RUN_ON(&io_thread_);
    shut_down_ = true;
    std::vector<InFlightPtr> pending = in_flight_;
    for (auto &in_flight : pending)
      if (in_flight->callback)
        _finish(in_flight, SHUT_DOWN, NULL);
    for (auto &send : pending_sends_)
      if (send->done)
        send->done(0);
    pending_sends_.clear();
    basic_client_->shutdown();
    work_.reset();
  });
  thread_.join();
}

// ---------------------------------------------------------------------------
// io thread only

void ThreadedClient::_on_incoming(const BasicClient::IncomingMessagesBuffer::value_type &incoming) {
  const MultiplexerMessage &msg = *incoming.third;
  auto it = by_id_.find(msg.references());
  if (it != by_id_.end()) {
    if (msg.type() != types::REQUEST_RECEIVED)
      _advance(it->second, incoming);
    return;
  }
  _on_unmatched(incoming);
}

// A message that is no reply to a query in flight: the protocol's own are
// handled or dropped here, the rest go to on_message.
void ThreadedClient::_on_unmatched(const IncomingMessage &incoming) {
  const MultiplexerMessage &msg = *incoming.third;
  if (msg.references() && finished_ids_.count(msg.references())) {
    MX_LOG(DEBUG, MEDIUMVERBOSITY,
           CTX("ThreadedClient")
               TEXT("late reply #" + repr(msg.id()) + " to finished query #" + repr(msg.references()) + " dropped"));
    return;
  }
  if (msg.type() == types::REQUEST_RECEIVED)
    return; // for a query this client no longer tracks
  if (msg.type() == types::PING || msg.type() == types::BACKEND_FOR_PACKET_SEARCH) {
    if (msg.references())
      return; // an answer to a ping nobody here is waiting for
    if (msg.type() == types::BACKEND_FOR_PACKET_SEARCH) {
      if (search_policy_) {
        if (!search_policy_())
          return; // a backend that declines, draining or full
      } else if (msg.to() != instance_id_) {
        return; // a search by type: this peer is no backend and never answers one
      }
    }
    // An echo request, or a search addressed to this instance (an addressed
    // query locating it): answer with a PING, the payload echoed, through
    // the connection it came on, the way a backend does.
    MultiplexerMessage pong = new_message(types::PING, msg.message());
    pong.set_to(msg.from());
    pong.set_references(msg.id());
    std::shared_ptr<const RawMessage> raw(RawMessage::FromMessage(pong));
    if (BasicClient::Connection::pointer conn = incoming.second.lock())
      if (conn->living() && conn->schedule(raw))
        return;
    basic_client_->schedule_one(raw);
    return;
  }
  if (on_message_) {
    on_message_(incoming);
    return;
  }
  MX_LOG(WARNING, MEDIUMVERBOSITY,
         CTX("ThreadedClient") TEXT("message #" + repr(msg.id()) + " of type " + repr(msg.type()) +
                                    " dropped: this client has no on_message callback"));
}

void ThreadedClient::_remember_finished(std::uint64_t id) {
  if (!id || !finished_ids_.insert(id).second)
    return;
  finished_order_.push_back(id);
  if (finished_order_.size() > REMEMBERED_FINISHED_IDS) {
    finished_ids_.erase(finished_order_.front());
    finished_order_.pop_front();
  }
}

void ThreadedClient::_track(InFlightPtr in_flight, std::uint64_t id) { by_id_[id] = in_flight; }

// A connection came (up) or went. Queries whose request or direct request
// went through a connection that is gone are sent again through another
// one, or wait for one; the deadline keeps running throughout.
void ThreadedClient::_on_connection(const ConnectionWrapper &connection, bool up) {
  std::vector<InFlightPtr> queries = in_flight_; // a copy: the loop may finish queries
  MX_LOG(DEBUG, HIGHVERBOSITY,
         CTX("ThreadedClient")
             TEXT(std::string("connection ") + (up ? "up" : "down") + "; queries in flight: " + repr(queries.size())));
  if (up && !pending_sends_.empty())
    _advance_sends();
  for (auto &in_flight : queries) {
    if (up) {
      if (in_flight->stage == InFlight::WAITING)
        _start_query(in_flight, /*keep_deadline=*/true);
      continue;
    }
    switch (in_flight->stage) {
    case InFlight::REQUEST:
    case InFlight::DIRECT:
      if (in_flight->sent_via.is_same_connection(connection))
        _lost(in_flight);
      break;
    case InFlight::SEARCH:
      // One connection fewer to answer the search.
      if (in_flight->pending_delivery_errors > 0 && --in_flight->pending_delivery_errors == 0)
        _start_query(in_flight, /*keep_deadline=*/true);
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
void ThreadedClient::_lost(InFlightPtr in_flight) {
  if (in_flight->pinned()) {
    _finish(in_flight, NOT_CONNECTED, NULL);
    return;
  }
  MX_LOG(WARNING, MEDIUMVERBOSITY,
         CTX("ThreadedClient") TEXT("connection lost under query " + repr(in_flight->request_id) +
                                    (in_flight->addressed() ? "; locating the addressee" : "; sending again")));
  if (in_flight->addressed())
    _search(in_flight);
  else
    _start_query(in_flight, /*keep_deadline=*/true);
}

// What the next stage's timer gets: the whole `timeout` for a typed query,
// what is left of the one deadline for an addressed one.
float ThreadedClient::_stage_timeout(const InFlightPtr &in_flight) const {
  if (!in_flight->addressed())
    return in_flight->timeout;
  std::chrono::steady_clock::duration left = in_flight->deadline - std::chrono::steady_clock::now();
  return std::max(0.0f, std::chrono::duration<float>(left).count());
}

void ThreadedClient::_start_query(InFlightPtr in_flight, bool keep_deadline) {
  if (shut_down_) {
    _finish(in_flight, SHUT_DOWN, NULL);
    return;
  }
  // Ids of an earlier attempt stay tracked, so a late reply is accepted.
  MultiplexerMessage request = in_flight->prototype;
  request.set_id(random64());
  in_flight->request_id = request.id();
  std::shared_ptr<const RawMessage> raw(RawMessage::FromMessage(request));
  ConnectionWrapper used;
  bool refused = false;
  if (!_schedule(raw, in_flight->lane, &used, &refused)) {
    if (refused) {
      _finish(in_flight, NOT_CONNECTED, NULL); // the pinned lane's connection is gone
      return;
    }
    // Nothing to send through: wait for a connection, within the deadline.
    in_flight->stage = InFlight::WAITING;
    if (!keep_deadline)
      _arm(in_flight, in_flight->timeout);
    return;
  }
  in_flight->sent_via = used;
  MX_LOG(DEBUG, CHATTERBOX, // per message: off by default, MX_LOG_VERBOSITY=DEBUG:CHATTERBOX shows the traffic
         CTX("ThreadedClient") TEXT("request " + repr(in_flight->request_id) + " via " + repr(used.endpoint_)));
  _track(in_flight, in_flight->request_id);
  in_flight->stage = InFlight::REQUEST;
  if (!keep_deadline)
    _arm(in_flight, in_flight->timeout);
}

void ThreadedClient::_advance(InFlightPtr in_flight, const IncomingMessage &incoming) {
  const MultiplexerMessage &msg = *incoming.third;
  switch (in_flight->stage) {
  case InFlight::REQUEST:
    if (msg.type() == types::DELIVERY_ERROR)
      _search(in_flight);
    else
      _finish(in_flight, REPLIED, &incoming);
    return;

  case InFlight::SEARCH:
    if (msg.references() == in_flight->request_id) {
      // A late reply to the original request still counts.
      if (msg.type() != types::DELIVERY_ERROR)
        _finish(in_flight, REPLIED, &incoming);
      return;
    }
    if (msg.type() == types::DELIVERY_ERROR) {
      // Every multiplexer reported no backend of the type, so nothing can
      // answer any more.
      if (in_flight->pending_delivery_errors == 0 || --in_flight->pending_delivery_errors == 0)
        _finish(in_flight, FAILED, NULL);
    } else if (msg.type() == types::PING) {
      _direct(in_flight, incoming);
    }
    return;

  case InFlight::DIRECT:
    if (msg.references() == in_flight->search_id)
      return; // a later PING from another backend
    if (msg.type() == types::DELIVERY_ERROR)
      _finish(in_flight, FAILED, NULL);
    else
      _finish(in_flight, REPLIED, &incoming);
    return;

  case InFlight::WAITING:
    if (msg.type() != types::DELIVERY_ERROR)
      _finish(in_flight, REPLIED, &incoming); // a reply to an earlier attempt after all
    return;
  }
}

// The middle stage: a search for a backend of the type on every
// connection, or, for an addressed query, the probe addressed to the
// instance (a search, or a PING), with delivery errors requested so that a
// multiplexer without the instance says so. Through a pinned lane the one
// multiplexer behind it is asked instead of all.
void ThreadedClient::_search(InFlightPtr in_flight) {
  float left = _stage_timeout(in_flight);
  if (in_flight->addressed() && left <= 0) {
    _finish(in_flight, TIMED_OUT, NULL);
    return;
  }
  MultiplexerMessage msg;
  if (in_flight->addressed() && in_flight->probe == PROBE_PING) {
    msg = new_message(types::PING, std::string());
  } else {
    BackendForPacketSearch search;
    search.set_packet_type(in_flight->prototype.type());
    msg = new_message(types::BACKEND_FOR_PACKET_SEARCH, std::string());
    search.SerializeToString(msg.mutable_message());
  }
  if (in_flight->addressed()) {
    msg.set_to(in_flight->prototype.to());
    msg.set_report_delivery_error(true);
  }
  std::shared_ptr<const RawMessage> raw(RawMessage::FromMessage(msg));
  unsigned int sent = 0;
  if (in_flight->pinned()) {
    ConnectionWrapper used;
    bool refused = false;
    if (_schedule(raw, in_flight->lane, &used, &refused))
      sent = 1;
    else if (refused) {
      _finish(in_flight, NOT_CONNECTED, NULL);
      return;
    }
  } else {
    sent = basic_client_->schedule_all(raw);
  }
  if (sent == 0) {
    in_flight->stage = InFlight::WAITING; // the request goes out again when a connection is back
    return;
  }
  in_flight->search_id = msg.id();
  in_flight->pending_delivery_errors = sent;
  _track(in_flight, in_flight->search_id);
  in_flight->stage = InFlight::SEARCH;
  _arm(in_flight, left);
}

void ThreadedClient::_direct(InFlightPtr in_flight, const IncomingMessage &ping) {
  MultiplexerMessage request = in_flight->prototype;
  request.set_id(random64());
  request.set_to(ping.third->from());
  in_flight->direct_id = request.id();
  std::shared_ptr<const RawMessage> raw(RawMessage::FromMessage(request));
  // Through the connection the PING came on, which is known to reach that
  // backend; if it died meanwhile, any connection, since `to` is set. The
  // lane adopts it: what follows the query goes the same way.
  BasicClient::BasicScheduledMessageTracker tracker;
  ConnectionWrapper used = ping.second;
  if (BasicClient::Connection::pointer conn = ping.second.lock())
    if (conn->living())
      tracker = conn->schedule(raw);
  if (tracker && in_flight->lane)
    in_flight->lane->adopt(used);
  if (!tracker) {
    bool refused = false;
    tracker = _schedule(raw, in_flight->lane, &used, &refused);
    if (refused) {
      _finish(in_flight, NOT_CONNECTED, NULL);
      return;
    }
  }
  if (!tracker) {
    in_flight->stage = InFlight::WAITING;
    return;
  }
  in_flight->sent_via = used;
  _track(in_flight, in_flight->direct_id);
  in_flight->stage = InFlight::DIRECT;
  _arm(in_flight, _stage_timeout(in_flight));
}

void ThreadedClient::_arm(InFlightPtr in_flight, float timeout) {
  unsigned int generation = ++in_flight->generation;
  in_flight->timer->expires_after(std::chrono::microseconds(static_cast<long>(timeout * 1e6)));
  in_flight->timer->async_wait([this, in_flight, generation](const asio::error_code &error) {
    MX_DCHECK_RUN_ON(&io_thread_);
    _on_deadline(in_flight, generation, error);
  });
}

void ThreadedClient::_on_deadline(InFlightPtr in_flight, unsigned int generation, const asio::error_code &error) {
  if (error == asio::error::operation_aborted || generation != in_flight->generation || !in_flight->callback)
    return;
  if (in_flight->stage == InFlight::REQUEST)
    _search(in_flight);
  else if (in_flight->stage == InFlight::WAITING)
    _finish(in_flight, NOT_CONNECTED, NULL);
  else
    _finish(in_flight, TIMED_OUT, NULL);
}

void ThreadedClient::_finish(InFlightPtr in_flight, Outcome outcome, const IncomingMessage *reply) {
  asio::error_code ignored;
  in_flight->timer->cancel(ignored);
  ++in_flight->generation;
  by_id_.erase(in_flight->request_id);
  _remember_finished(in_flight->request_id);
  if (in_flight->search_id) {
    by_id_.erase(in_flight->search_id);
    _remember_finished(in_flight->search_id);
  }
  if (in_flight->direct_id) {
    by_id_.erase(in_flight->direct_id);
    _remember_finished(in_flight->direct_id);
  }
  in_flight_.erase(std::remove(in_flight_.begin(), in_flight_.end(), in_flight), in_flight_.end());
  Callback callback;
  callback.swap(in_flight->callback); // a finished query never reports twice
  Result result;
  result.outcome = outcome;
  if (reply) {
    result.reply = *reply;
    if (in_flight->lane)
      in_flight->lane->adopt(reply->second); // what follows the query goes where the answer came from
  }
  in_flight->lane.reset(); // the query held it only for its own duration
  if (callback)
    callback(result);
}

} // namespace multiplexer
