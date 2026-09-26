// BaseThreadedMultiplexerServer: a backend whose handlers run on worker
// threads behind a heartbeating io thread.
//
// BaseMultiplexerServer runs the loop and the handler on one thread, so
// while handle_message runs nothing heartbeats, and a backend built on it
// whose one request takes longer than the multiplexer's drop interval
// (docs/semantics.md) is dropped mid-work. This class puts the io on a
// ThreadedClient's thread, which heartbeats, reconnects, answers pings and
// the search clients use to find a backend, and hands every other message
// to a bounded queue that `workers` threads take from. With one worker
// handling is serial, in arrival order, as on BaseMultiplexerServer, and
// the peer stays registered under a request of any length. The workers
// start and the connections open in serve_forever(), so nothing reaches
// handle_message() before the subclass is built, and no multiplexer knows
// the backend until it serves. A request that
// arrives while the server is leaving, routed before the multiplexer saw
// the connection go, is refused with DELIVERY_ERROR, so its requester
// retries elsewhere at once rather than after its timeout; one that
// answers another is dropped, since nobody retries a reply and refusing
// one could start a loop with a peer that answers the refusal.
//
// The handler is handle_message(request): a Request carries the message
// and everything needed to answer it, held by shared_ptr so that a handler
// may keep it and answer later from another thread. A request dropped
// without a reply or no_response() is logged. The Python mirror is
// multiplexer/threaded_server.py; docs/api_cpp.md has the user's view.
#ifndef MX_MULTIPLEXER_BACKEND_BASE_THREADED_MULTIPLEXER_SERVER_H_
#define MX_MULTIPLEXER_BACKEND_BASE_THREADED_MULTIPLEXER_SERVER_H_

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <exception>
#include <memory>
#include <thread>
#include <vector>

#include "lib/mutex.h"
#include "multiplexer/backend/base_multiplexer_server.h"
#include "multiplexer/threaded_client.h"

namespace multiplexer {
namespace backend {

class BaseThreadedMultiplexerServer;

// What a BaseThreadedMultiplexerServer is built with.
struct ThreadedServerOptions {
  unsigned int workers = 1;                 // handler threads
  std::size_t queue_size = 1024;            // requests waiting for a worker; beyond it they are dropped
  bool decline_searches_when_full = false;  // leave a client's search unanswered while saturated
  float connect_timeout = DEFAULT_TIMEOUT;
  // What start_draining() tells the multiplexers: any and all off keeps
  // only addressed messages coming; all on keeps events; last_resort keeps
  // a lone backend serving through its drain.
  Routing drain_routing = direct_only_routing();
};

// One message being handled, and what answers it. reply() fills in `to`,
// `references`, `workflow` and the connection from the request and sends
// through its ThreadedClient, from whichever thread calls it.
class Request {
 public:
  ~Request();  // logs a warning when nobody answered or called no_response()

  const MultiplexerMessage& mxmsg() const { return *incoming_.third; }
  const ConnectionWrapper& connection() const { return incoming_.second; }
  const IncomingMessage& incoming() const { return incoming_; }
  bool answered() const { return answered_; }

  // The reply: `payload` as a message of `type`, or a message you built,
  // whose id and from are set and whose to, references and workflow are
  // filled in when empty. One reply per request: `references` means "this
  // is the reply", and a requester built on ThreadedClient drops what
  // references a query it has seen answered; a follow-up that is not the
  // reply goes through the server's client() with `to` set and no
  // `references`, correlated in the payload.
  void reply(const std::string& payload, std::uint32_t type);
  void reply(MultiplexerMessage msg);
  // The message needs no reply, as an event does.
  void no_response() { answered_ = true; }
  // BACKEND_ERROR carrying `message`: a Python requester's query() raises
  // BackendError, a C++ one gets the message itself.
  void report_error(const std::string& message);
  // REQUEST_RECEIVED to the requester at once, for a handler that takes long.
  void notify_start();
  template <typename Message>
  Message parse_message() const {
    Message message;
    message.ParseFromString(mxmsg().message());
    return message;
  }

 private:
  friend class BaseThreadedMultiplexerServer;
  Request(ThreadedClient* client, const IncomingMessage& incoming) : client_(client), incoming_(incoming) {}
  Request(const Request&) = delete;
  Request& operator=(const Request&) = delete;

  ThreadedClient* client_;
  IncomingMessage incoming_;
  std::atomic<bool> answered_{false};
  bool dropped_ = false;  // the server said why; no warning from the destructor
};
typedef std::shared_ptr<Request> RequestPtr;

class BaseThreadedMultiplexerServer {
 public:
  typedef ThreadedServerOptions Options;

 protected:
  // A backend of `type` for the multiplexers at `addresses`; a subclass
  // calls it. It only makes the instance id: the workers start and the
  // connections open in connect(), which serve_forever() calls first, so
  // nothing reaches handle_message() before the subclass is built, and
  // no multiplexer knows the backend until it can serve.
  BaseThreadedMultiplexerServer(const MultiplexerAddresses& addresses, PeerType type,
                                const Options& options = Options());

 public:
  virtual ~BaseThreadedMultiplexerServer();  // close()

  // connect(); then, until
  // stop() or a drain is over: every `poll` seconds, or sooner when
  // woken, periodic_task(); then take no new message, let the workers
  // finish the queue, close the connections and return. The calling thread
  // only polls; the handlers run on the workers. Rethrows what a handler
  // threw when on_handler_exception() returned false.
  void serve_forever(float poll = 1.0f, float drain_seconds = 0.0f);
  // Starts the workers and connects to every address, once;
  // serve_forever() calls it first, and a second call does nothing. Call
  // it yourself when something waits for a line you print before it
  // sends, so that the line means reachable, or in a test that wants the
  // backend connected without a thread serving it.
  void connect();
  // Take no more messages, let the workers finish what is queued, stop
  // them and close the connections. What still arrives, routed before
  // the multiplexers applied the drain routing or saw the connection go,
  // is refused with DELIVERY_ERROR, a reply dropped. Idempotent; the
  // destructor calls it.
  // Joins the workers, so from a handler, on a worker, it throws
  // std::logic_error: a handler that wants the server gone calls stop().
  void close();

  // Draining, as on BaseMultiplexerServer: tell every multiplexer the
  // drain_routing, nothing new by the rules by default, keep serving what
  // arrives, leave once drained().
  void start_draining();
  bool draining() const { return draining_.load(); }
  // Ask serve_forever() to return, from any thread.
  void stop();
  // Requests waiting for a worker plus those being handled, and the
  // number dropped by a full queue or refused while leaving.
  std::size_t pending() const;
  std::size_t dropped() const { return dropped_.load(); }

  std::uint64_t instance_id() const { return client_.instance_id(); }  // known from construction
  ThreadedClient& client() { return client_; }                         // for messages that are not replies

 protected:
  // Called on a worker thread with every message that is not the
  // protocol's own. Answer with request->reply(), or call
  // request->no_response() for an event; either may happen later, from
  // any thread, as long as it happens.
  virtual void handle_message(const RequestPtr& request) = 0;
  // Called from serve_forever() after every poll, on its thread.
  virtual void periodic_task() {}
  // Whether the drain is over: by default once `drain_seconds` have
  // passed since start_draining(), or, when the drain routing turns every
  // path off and asks for no last resort, once every connected
  // multiplexer has confirmed it and no request is queued or being
  // handled, since nothing more is on its way; a drain that keeps a path
  // open lasts the whole period. Override for a condition of your own.
  virtual bool drained() const;
  // Called on the worker thread when handle_message() threw, after
  // BACKEND_ERROR went to the requester. True (the default) keeps serving;
  // false makes serve_forever() return and rethrow.
  virtual bool on_handler_exception(const std::exception&) { return true; }
  // Whether to answer a client's search for a backend; on the io thread,
  // so quick. False with decline_searches_when_full while every worker is
  // busy and requests wait. A draining backend needs no policy here: the
  // multiplexers stop offering it (drain_routing).
  virtual bool should_respond_to_backend_for_packet_search() const;

  std::atomic<bool> working{true};

 private:
  void _on_message(const IncomingMessage& incoming);
  void _start_workers();
  void _work();
  void _handle(const RequestPtr& request);

  const Options options_;
  const MultiplexerAddresses addresses_;
  const PeerType type_;
  mutable mx::Mutex mutex_;
  std::condition_variable_any cond_;
  std::deque<RequestPtr> queue_ MX_GUARDED_BY(mutex_);
  unsigned int busy_ MX_GUARDED_BY(mutex_) = 0;
  bool accepting_ MX_GUARDED_BY(mutex_) = true;
  std::vector<std::thread> threads_ MX_GUARDED_BY(mutex_);
  std::atomic<bool> draining_{false};
  // When the drain started, as steady_clock ticks, written before
  // draining_ is published: drained() may run on another thread than
  // start_draining().
  std::atomic<std::chrono::steady_clock::rep> draining_since_ticks_{0};
  float drain_seconds_ = 0.0f;
  mx::Mutex wake_mutex_;
  std::condition_variable_any wake_;
  std::exception_ptr failure_;
  std::atomic<bool> connected_{false};
  std::atomic<bool> closed_{false};
  std::atomic<std::size_t> dropped_{0};
  mutable ThreadedClient client_;  // last: its callbacks reach the members above; asked from const drained()
};

}  // namespace backend
}  // namespace multiplexer

#endif  // MX_MULTIPLEXER_BACKEND_BASE_THREADED_MULTIPLEXER_SERVER_H_
