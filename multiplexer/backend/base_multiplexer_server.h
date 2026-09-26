// Base class for backends written in C++: connects to the multiplexers,
// runs the receive loop, answers the protocol's own messages, and calls
// handle_message() with everything else. The Python counterpart is
// BaseMultiplexerServer in clients.py; both behave the same way, including
// what happens when a handler throws. docs/api_cpp.md is the user's view.
//
// A BaseMultiplexerServer is driven by serve_forever(): it blocks in the
// client's read, so heartbeats and reconnects happen on their own and the
// peer type need not be passive. One message is handled at a time. After every
// iteration, message or poll timeout, periodic_task() runs: the place for
// work on the backend's own schedule and for noticing a request to leave.
// The library installs no signal handlers; a handler of your own should
// only set a sig_atomic_t that periodic_task() reads.
#ifndef MX_MULTIPLEXER_BACKEND_BASE_MULTIPLEXER_SERVER_H_
#define MX_MULTIPLEXER_BACKEND_BASE_MULTIPLEXER_SERVER_H_

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "lib/exception.h"
#include "lib/kwargs.h"
#include "multiplexer/Multiplexer.pb.h" /* generated */
#include "multiplexer/client.h"

namespace multiplexer {
namespace backend {

typedef std::pair<std::string, std::uint16_t> MultiplexerAddress;
typedef std::vector<MultiplexerAddress> MultiplexerAddresses;
typedef std::uint32_t PeerType;

using mx::util::kwargs::Kwargs;
using mx::util::kwargs::KwargsKeys;

// How long close() waits for the last replies to be written before it
// closes the connections, which takes CLOSE_READ_SECONDS more at most
// (Client::shutdown); a multiplexer that stopped reading cannot hold it
// longer than the two.
static const float CLOSE_FLUSH_SECONDS = 1.0f;

// The routing a draining backend asks for unless told otherwise: nothing
// new by the rules, only what is addressed to it (Multiplexer.proto's
// Routing; docs/leaving.md).
inline Routing direct_only_routing() {
  Routing routing;
  routing.set_any(false);
  routing.set_all(false);
  return routing;
}

// Whether a drain with this routing may end as soon as the multiplexers
// confirmed it: every path by the rules is off and the peer is no last
// resort, so only addressed messages can still come.
inline bool nothing_more_arrives(const Routing& routing) {
  return !routing.any() && !routing.all() && !routing.last_resort();
}

// See the file comment. Subclass, implement handle_message(), and call
// serve_forever(). Not thread-safe.
class BaseMultiplexerServer {
 public:
  // for use with send_message(..., multiplexer=(ONE|ALL|a ConnectionWrapper),
  // ...)
  static const int ONE = 1;
  static const int ALL = 2;

 protected:
  // A peer of `type` for the multiplexers at `addresses`: this makes the
  // instance id, and connect() or serve_forever() connects, so no
  // multiplexer knows the backend before it can serve. The second form
  // uses a Client the caller owns, keeps and connects.
  BaseMultiplexerServer(const MultiplexerAddresses& addresses, PeerType type);

  BaseMultiplexerServer(multiplexer::Client* conn, PeerType type);

 public:
  virtual ~BaseMultiplexerServer();

  // One step: wait up to `timeout` seconds for a message and handle it.
  // Throws Client::OperationTimedOut when the time passes.
  virtual void loop_iter(float timeout = DEFAULT_READ_TIMEOUT);

  // Connects to every address given to the constructor, once;
  // serve_forever() calls it first, and a second call does nothing. Call
  // it yourself when something waits for a line you print before it
  // sends, so that the line means reachable; when you drive loop_iter()
  // yourself; or in a test that wants the backend connected without a
  // thread serving it.
  void connect();

  // The loop: connect() unless already connected, then until `working` is
  // cleared or a drain is over, wait up to
  // `poll` seconds for a message, handle it if one came, call
  // periodic_task(); then close the connections. `drain_seconds` is how
  // long to keep serving after start_draining(), unless drained() is
  // overridden. The calling thread becomes the backend's thread: a backend
  // may be built on one thread and served from another, but from here on
  // only this thread may touch it.
  void serve_forever(float poll = 1.0f, float drain_seconds = 0.0f);

  // Draining: a backend about to exit tells every multiplexer to route it
  // nothing new by the rules, the drain routing, so that no request and
  // no search is sent to it, while it serves what still arrives. Call
  // start_draining() from periodic_task() when asked to leave; serve_forever
  // returns once drained().
  void start_draining();
  bool draining() const { return draining_; }
  // What start_draining() tells the multiplexers; set before it. The
  // default keeps only addressed messages coming; `all` on keeps events,
  // `last_resort` keeps a lone backend serving through its drain.
  void set_drain_routing(const Routing& routing) { drain_routing_ = routing; }
  const Routing& drain_routing() const { return drain_routing_; }
  // Asks serve_forever() to return, from any thread: it notices within one
  // poll, closes the connections and returns.
  void stop() { working = false; }

 protected:
  // Called with every message that is not the protocol's own. Reply with
  // send_message(); for a message that needs no reply call no_response(),
  // otherwise the missing reply is logged as a warning. While it runs,
  // last_mxmsg and last_connwrap are the message and its connection.
  virtual void handle_message(MultiplexerMessage&) = 0;

  // Called after every iteration of serve_forever(), message or not, so at
  // least once per `poll` seconds. Override for work on your own schedule
  // and for noticing a request to leave: call start_draining() or clear
  // `working`. Does nothing by default.
  virtual void periodic_task() {}

  // Whether the drain is over and serve_forever() may return; checked after
  // every iteration while draining, so between two messages. Default:
  // `drain_seconds` have passed since start_draining(), or, when the
  // drain routing turns every path off and asks for no last resort, every
  // connected multiplexer has confirmed it, so nothing more is on its way;
  // a drain that keeps a path open lasts the whole period, since work
  // keeps arriving. Override to wait for your own condition, for example
  // `BaseMultiplexerServer::drained() && in_flight_ == 0`.
  virtual bool drained() const;

  // Called when handle_message() threw, after BACKEND_ERROR went to the
  // requester. Return true to keep serving (the default); return false and
  // the exception propagates out of serve_forever().
  virtual bool on_handler_exception(const std::exception&) { return true; }

  // Whether to answer a client's search for a backend; true unless
  // overridden for a condition of your own. A draining backend needs no
  // policy here: the multiplexers stop offering it (the drain routing).
  virtual bool should_respond_to_backend_for_packet_search() const { return true; }

 protected:
  template <typename Message>
  static Message inline parse_message(const MultiplexerMessage& mxmsg) {
    return parse_message<Message>(mxmsg.message());
  }

  template <typename Message>
  static Message inline parse_message(const std::string& from) {
    Message message;
    message.ParseFromString(from);
    return message;
  }

 protected:
  // Tells the requester at once that its request is being worked on
  // (REQUEST_RECEIVED); call it first thing in handle_message.
  void notify_start();

  /*
   * required kwargs:
   *	    message:	const MultiplexerMessage* OR
   *			const std::string* OR
   *			std::string
   * possible kwargs:
   *	    to:		std::uint64_t
   *	    references: std::uint64_t
   *	    type:	std::uint32_t
   *	    workflow:	std::string OR
   *			const std::string*
   *	    multiplexer:    int OR
   *			    ConnectionWrapper
   * TODO flush, timeout
   * TODO other MultiplexerMessage keys ??
   *
   * returns
   *	    TODO add doc on return type
   */
  std::any send_message(Kwargs kwargs);

  void no_response() { _has_sent_response = true; }

  // Closes every connection; the server cannot be used afterwards. Safe to
  // call twice.
  void close();

  // Answers the current request with BACKEND_ERROR carrying `message`, so the
  // requester's query() fails at once instead of waiting out its timeout.
  void report_error(const std::string& message);

 private:
  void __handle_message();
  void __handle_internal_message();

 public:
  std::atomic<bool> working;  // cleared by stop(), from any thread, or by the loop thread directly

 protected:
  bool _has_sent_response;
  bool draining_ = false;
  float drain_seconds_ = 0.0f;
  std::chrono::steady_clock::time_point draining_since_;
  Routing drain_routing_ = direct_only_routing();

 private:
  std::unique_ptr<multiplexer::Client> __conn;
  const MultiplexerAddresses addresses_;
  bool connected_ = false;

 protected:
  multiplexer::Client* conn;
  std::shared_ptr<MultiplexerMessage> last_mxmsg;
  ConnectionWrapper last_connwrap;

 private:
};

};  // namespace backend
};  // namespace multiplexer

#endif  // MX_MULTIPLEXER_BACKEND_BASE_MULTIPLEXER_SERVER_H_
