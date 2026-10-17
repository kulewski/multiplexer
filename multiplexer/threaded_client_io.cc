// ThreadedClient's io thread at work, besides the queries: the sends, with
// their deadlines; a connect() or a flush_all() ending; what arrives and is
// no reply to a query; and the shutdown's write-out and teardown. Part of
// ThreadedClient; see threaded_client.cc.
#include <algorithm>
#include <chrono>
#include <future>
#include <stdexcept>

#include "lib/logging/logging.h"
#include "lib/repr.h"
#include "lib/seconds.h"
#include "multiplexer/multiplexer.constants.h"
#include "multiplexer/threaded_client_core.h"

namespace multiplexer {

using mx::repr;

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

// A message as a log line names it.
static std::string describe(const MultiplexerMessage& msg) {
  return "message #" + repr(msg.id()) + " of type " + repr(msg.type()) + " from " + repr(msg.sender());
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

// A flush_all() call ends, flushed or out of time: BasicClient forgets its
// flush at once, and the caller is told in a handler of its own. A flush
// ends from wherever its last frame was written or given up on, a
// connection's handler or the outbox's bookkeeping maybe, which what the
// caller's callback does, a send say, must not run inside.
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
  _post([this, done, flushed] {
    MX_DCHECK_RUN_ON(&io_thread_);
    _guarded([&] { done(flushed); }, [] { return std::string("a flush_all() callback"); });
  });
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

// A caller's whole message framed, its empty id and sender filled
// (frame_stamped), marked for batching once anything was batched: a frame
// made before a set_batching() reached the io thread is read there.
std::shared_ptr<const RawMessage> ThreadedClient::Core::_stamped(const MultiplexerMessage& msg) {
  return frame_stamped(msg, instance_id_, [this] { return random64(); }, batching_.load(std::memory_order_relaxed));
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

void ThreadedClient::Core::send_serialized(std::string serialized, std::uint64_t id, std::uint32_t type, LanePtr lane,
                                           float timeout, SendCallback done) {
  if (lane && lane->closed()) {
    MXTHROW(NotConnected());  // as send() does
  }
  // Once anything is batched, a frame that may go in a batch is made
  // unsealed, its checksum computed on the io thread only if it goes alone.
  RawMessage* raw = !lane && batching_.load(std::memory_order_relaxed)
                        ? new RawMessage(RawMessage::Unsealed(), &serialized, id, type)
                        : new RawMessage(&serialized, id, type);
  _submit_send(std::shared_ptr<const RawMessage>(raw), false, false, timeout, done, lane);
}

void ThreadedClient::Core::send_all_serialized(std::string serialized, std::uint64_t id, std::uint32_t type,
                                               float timeout, SendCallback done) {
  _submit_send(std::shared_ptr<const RawMessage>(new RawMessage(&serialized, id, type)), true, false, timeout, done,
               LanePtr());
}

unsigned int ThreadedClient::Core::send_serialized_and_wait(std::string serialized, std::uint64_t id,
                                                            std::uint32_t type, bool all, float timeout, LanePtr lane,
                                                            bool* not_connected) {
  basic_client_->check_not_orphaned();
  if (io_thread_.is_current()) {
    throw std::logic_error("flushing ThreadedClient send called on the io thread, from a callback");
  }
  std::shared_ptr<std::promise<std::pair<unsigned int, bool>>> promise(
      new std::promise<std::pair<unsigned int, bool>>());
  std::future<std::pair<unsigned int, bool>> future = promise->get_future();
  _submit_send(std::shared_ptr<const RawMessage>(new RawMessage(&serialized, id, type)), all, true, timeout,
               SendCallback(), lane,
               [promise](unsigned int written, bool lost) { promise->set_value(std::make_pair(written, lost)); });
  const std::pair<unsigned int, bool> ended = future.get();
  if (not_connected) {
    *not_connected = ended.second;
  }
  return ended.first;
}

void ThreadedClient::Core::send_serialized_with_callback(std::string serialized, std::uint64_t id, std::uint32_t type,
                                                         bool all, float timeout, SendCallback done, LanePtr lane) {
  _submit_send(std::shared_ptr<const RawMessage>(new RawMessage(&serialized, id, type)), all, true, timeout, done,
               lane);
}

void ThreadedClient::Core::send_serialized_and_notify(std::string serialized, std::uint64_t id, std::uint32_t type,
                                                      bool all, float timeout, FlushedCallback done, LanePtr lane) {
  _submit_send(std::shared_ptr<const RawMessage>(new RawMessage(&serialized, id, type)), all, true, timeout,
               SendCallback(), lane, done);
}

// Hands a send to the io thread. Never blocks the caller: a plain post, so
// callbacks may send. A flushing send carries a completion the io thread
// calls once the message is written or the deadline passed.
void ThreadedClient::Core::_submit_send(std::shared_ptr<const RawMessage> raw, bool all, bool wait, float timeout,
                                        SendCallback done, LanePtr lane, FlushedCallback flushed) {
  basic_client_->check_not_orphaned();
  if (lane) {
    lane->check_not_inherited();  // here, not on the io thread, where it would throw into nobody
    basic_client_->check_ours(lane);
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
  if (!shut_down_) {
    // shutdown() began on another thread, and its handler still waits,
    // behind every send posted before it, this thread's earlier ones among
    // them: this one goes behind them too, so that the order of one
    // thread's sends holds on a connection. The write-out, begun there,
    // sees the number this one takes and writes it out too.
    _post([this, pending] {
      MX_DCHECK_RUN_ON(&io_thread_);
      if (torn_down_) {
        basic_client_->report_drop(pending->raw, DropReason::SHUT_DOWN);  // a shutdown with no write-out
        _settle(pending, 0, /*not_connected=*/true);
        return;
      }
      _place(pending);
    });
    return;
  }
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
  // A send nobody waits for may go in a batch (BasicClient::set_batching).
  const bool may_batch = !pending->wait && !pending->flushed;
  if (!basic_client_->send(pending->raw, pending->all, pending->lane, timeout, pending->number, NULL, NULL, followed,
                           may_batch)) {
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

// The end of a shutdown, once, after its drain: a send BasicClient still
// follows hears its end from there, written, or 0 for what it drops and
// reports, at the latest when the io thread ends (end_follows); a
// flush_all() still waiting ends; the connections close, and the io
// thread ends once its handlers are done.
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
    pong.set_to(msg.sender());
    pong.set_references(msg.id());
    if (pong.ByteSizeLong() > MAX_MESSAGE_SIZE) {
      pong = new_message(types::BACKEND_ERROR,
                         std::string("the echo of a ") + (msg.type() == types::PING ? "PING" : "search") + " of " +
                             repr(msg.message().size()) + " bytes would be over MAX_MESSAGE_SIZE");
      pong.set_to(msg.sender());
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

// A connection came (up) or went. A connect() waiting for it ends, and the
// queries hear of it (_queries_on_connection). The sends are BasicClient's:
// it places what waited for a connection, and hands over what waited for
// the room of one that went.
void ThreadedClient::Core::_on_connection(const ConnectionWrapper& connection, bool up) {
  MX_LOG(DEBUG, HIGHVERBOSITY,
         CTX("ThreadedClient") TEXT(std::string("connection ") + (up ? "up" : "down") +
                                    "; queries in flight: " + repr(in_flight_.size())));
  std::vector<std::shared_ptr<ConnectWaiter>> connects = connects_;
  for (const std::shared_ptr<ConnectWaiter>& waiter : connects) {
    if (waiter->connection.is_same_connection(connection)) {
      _end_connect(waiter, up);
    }
  }
  _queries_on_connection(connection, up);
}

}  // namespace multiplexer
