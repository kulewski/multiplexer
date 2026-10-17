// ThreadedClient's plain sends and the io thread's turn at its inbox
// (threaded_client_inbox.h): the callers' sends of plain events as their
// bytes (_send_plain, _send_plain_bytes), and the io thread's turn
// (_drain), which runs what was handed over in order, hands each plain
// send to the batching as its bytes (BasicClient::send_plain, send_bytes)
// and gives the bodies the batching is done with back to the callers.
//
// A calling thread keeps one body of its own (spare_body below), which a
// plain send is serialized into and which a body the io thread handed back
// replaces as the send goes into the inbox: once bodies come back, a
// sending thread allocates nothing per message, and the io thread frees
// nothing a sending thread allocated.
#include <algorithm>
#include <chrono>
#include <string>
#include <utility>
#include <vector>

#include "lib/seconds.h"
#include "multiplexer/basic_client.h"
#include "multiplexer/spent_bodies.h"
#include "multiplexer/threaded_client_core.h"
#include "multiplexer/threaded_client_inbox.h"

namespace multiplexer {

namespace {
// Whether this thread's spare body is gone, at the thread's end: trivially
// destructible, so it may be read until the thread's very end, from the
// destructor of a thread_local made before the spare, which C++ destroys
// after it.
thread_local bool spare_body_gone = false;

// The calling thread's body for its next plain send: a body an io thread
// handed back, or, before any came back, one this thread allocated. One per
// thread, whichever client it sends on: a body is only a string's room.
struct SpareBody {
  ~SpareBody() { spare_body_gone = true; }
  std::string body;
};
thread_local SpareBody spare;

// This thread's spare body, or `own` once it is gone: a send made then, from
// another thread_local's destructor, has a body of its own.
std::string* spare_body(std::string* own) { return spare_body_gone ? own : &spare.body; }
}  // namespace

// A fresh id for a plain send, without a lock: a counter from a random
// start, mixed by SplitMix64, a bijection, so that no id repeats within
// the client; never 0, which means none.
std::uint64_t ThreadedClient::Core::_plain_id() {
  for (;;) {
    std::uint64_t mixed = plain_id_start_ + plain_ids_.fetch_add(1, std::memory_order_relaxed) * 0x9e3779b97f4a7c15ULL;
    mixed = (mixed ^ (mixed >> 30)) * 0xbf58476d1ce4e5b9ULL;
    mixed = (mixed ^ (mixed >> 27)) * 0x94d049bb133111ebULL;
    mixed ^= mixed >> 31;
    if (mixed) {
      return mixed;
    }
  }
}

bool ThreadedClient::Core::_send_plain(const MultiplexerMessage& msg, float timeout) {
  basic_client_->check_not_orphaned();
  std::string own;
  std::string* body = spare_body(&own);
  PlainSend plain;
  // The key goes at the end of the body, with room for it taken at once,
  // not in a string of its own, which the io thread would free.
  plain.id = serialize_stamped(msg, instance_id_, [this] { return _plain_id(); }, body, msg.key().size());
  plain.type = msg.type();
  plain.read = true;
  plain.keyed = msg.has_key();
  if (plain.keyed) {
    body->append(msg.key());
    plain.key_bytes = msg.key().size();
  }
  if (_push_plain(&plain, body, timeout)) {
    return true;
  }
  if (body->capacity() > SpentBodies::BODY_BYTES_KEPT) {
    std::string().swap(*body);  // a whole message's room, not kept as the thread's spare
  }
  return false;
}

bool ThreadedClient::Core::_send_plain_bytes(std::string* serialized, std::uint64_t id, std::uint32_t type,
                                             float timeout) {
  basic_client_->check_not_orphaned();
  check_message_size(serialized->size());  // here, as a frame made of it would check
  std::string own;
  std::string* spare = spare_body(&own);
  // Copied into this thread's spare where it has the room, so that
  // `serialized` goes here; else `serialized` itself goes, rather than a
  // copy into a spare grown for it, which a body that big never comes back
  // as.
  std::string* body = serialized;
  if (serialized->size() <= spare->capacity()) {
    spare->assign(*serialized);
    body = spare;
  }
  PlainSend plain;
  plain.id = id;
  plain.type = type;
  if (!_push_plain(&plain, body, timeout)) {
    return false;  // `serialized` as it was, for the caller to send as any send
  }
  if (body == serialized && spare->capacity() < serialized->capacity()) {
    spare->swap(*serialized);  // the body handed back, roomier than the spare
  }
  return true;
}

// `plain`, its bytes in `body`, into the inbox, counted in posting_ as
// _post_unless_stopped() counts a post; `body` then takes a body handed
// back. False, nothing handed over and `body` as it was, when shutdown()
// came first.
bool ThreadedClient::Core::_push_plain(PlainSend* plain, std::string* body, float timeout) {
  plain->deadline = std::chrono::steady_clock::now() + mx::from_seconds(timeout);
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
  if (inbox_.push_plain(plain, body)) {
    _wake();
  }
  return true;
}

// The io thread's wake-up: a handler asio allocates in the Core's one
// block for it (WakeMemory), so that a wake allocates nothing.
struct ThreadedClient::Core::WakeHandler {
  typedef WakeAllocator<void> allocator_type;
  allocator_type get_allocator() const noexcept { return allocator_type(&core->wake_memory_); }
  void operator()() const {
    MX_DCHECK_RUN_ON(&core->io_thread_);
    core->_drain();
  }
  Core* core;
};

// The io thread is woken for what the inbox holds.
void ThreadedClient::Core::_wake() { io_service_.post(WakeHandler{this}); }

// The io thread's turn at the inbox: everything handed over so far, run in
// the order it came, and the bodies the batching was done with since the
// last turn given back. What throws is logged, as an io handler's
// exception is (_io_thread_main), and the rest runs. The clock is read at
// the first plain send, again after a call, which may have run the
// program's callback for long, and every CLOCK_EVERY_PLAIN plain sends, so
// that a send's time left is never counted from long before.
void ThreadedClient::Core::_drain() {
  inbox_.take(&taken_, spent_.get());
  spent_->clear();  // those the inbox did not keep, freed here, outside its lock
  std::chrono::steady_clock::time_point now;
  std::size_t since_clock = CLOCK_EVERY_PLAIN;  // read at the first plain send
  for (ThreadedClientInbox::Entry& entry : taken_) {
    try {
      if (entry.call) {
        std::function<void()> call;
        call.swap(entry.call);  // let go of as soon as it ran, as an asio handler is
        since_clock = CLOCK_EVERY_PLAIN;
        call();
      } else {
        if (since_clock >= CLOCK_EVERY_PLAIN) {
          now = std::chrono::steady_clock::now();
          since_clock = 0;
        }
        ++since_clock;
        _run_plain(&entry.plain, now);
      }
    } catch (const std::exception& e) {
      MX_LOG(ERROR, LOWVERBOSITY,
             CTX("ThreadedClient") TEXT(std::string("exception escaped an io handler: ") + e.what()));
    }
  }
  const std::size_t ran = taken_.size();
  taken_.clear();
  // The room a backlog grew goes once it is over, the inbox's two lists
  // taking turns here: kept while it is no more than TAKEN_ENTRIES_KEPT or
  // a few turns' worth.
  if (taken_.capacity() > TAKEN_ENTRIES_KEPT && taken_.capacity() > 4 * ran) {
    std::vector<ThreadedClientInbox::Entry>().swap(taken_);
  }
}

// A plain send from the inbox, its key, at the end of its body, split off
// into the io thread's own string, handed to the batching as its bytes, its
// time left counted from its send; dropped and reported when it came just
// before the shutdown, as a send's own post is (_place).
void ThreadedClient::Core::_run_plain(PlainSend* plain, std::chrono::steady_clock::time_point now) {
  std::string& body = plain->body;
  if (plain->key_bytes) {
    plain_key_.assign(body, body.size() - plain->key_bytes, plain->key_bytes);
    body.resize(body.size() - plain->key_bytes);  // its room kept
  } else {
    plain_key_.clear();
  }
  if (!shut_down_) {
    const float left = std::max(0.0f, std::chrono::duration<float>(plain->deadline - now).count());
    if (plain->read ? basic_client_->send_plain(&body, plain->id, plain->type, plain->keyed, plain_key_, left)
                    : basic_client_->send_bytes(&body, plain->id, plain->type, left)) {
      return;
    }
  }
  // Not taken, the body still here: reported as _refuse() and the shutdown
  // report a send's frame.
  basic_client_->report_drop(std::make_shared<RawMessage>(RawMessage::Unsealed(), &body, plain->id, plain->type),
                             shut_down_ ? DropReason::SHUT_DOWN : DropReason::CONNECTION_LOST);
}

}  // namespace multiplexer
