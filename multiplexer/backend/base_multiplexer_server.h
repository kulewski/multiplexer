// Base class for backends written in C++: connects to the multiplexers,
// runs the receive loop, answers the protocol's own messages, and calls
// handle_message() with everything else. The Python counterpart is
// BaseMultiplexerServer in servers.py; both behave the same way, including
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
  // For send_message()'s `multiplexer`, besides a ConnectionWrapper.
  // constexpr, so inline: Kwargs::set() binds a reference to the one given,
  // which then needs the definition only an inline variable has unoptimized.
  static constexpr int ONE = 1;
  static constexpr int ALL = 2;

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
  // Throws Client::OperationTimedOut when the time passes; serve_forever()
  // takes the same two steps.
  virtual void loop_iter(float timeout = DEFAULT_READ_TIMEOUT);

  // Connects to every address given to the constructor, once; a second
  // call does nothing, nor does serve_forever() after it, which otherwise
  // starts the connections itself, waiting for none. Every connection
  // starts at once, and the call waits until each has its handshake done
  // or has failed, DEFAULT_TIMEOUT at most in all. Call it
  // yourself when something waits for a line you print before it sends,
  // so that the line means reachable; when you drive loop_iter()
  // yourself; or in a test that wants the backend connected without a
  // thread serving it.
  void connect();

  // The loop: a connection to every address started unless connect() did,
  // none waited for, then until `working` is cleared or a drain is over,
  // wait up to `poll` seconds for a message, handle it if one came, call
  // periodic_task(); then close the connections. The loop's waits finish
  // the handshakes: the backend serves what one multiplexer routes to it
  // while another has not welcomed it yet. `drain_seconds` is how
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
  // The client's dropped_while_closing(), kept past close(): the requests
  // that reached the server while it closed, and went unanswered.
  std::uint64_t dropped_while_closing() const { return conn ? conn->dropped_while_closing() : dropped_while_closing_; }

 protected:
  // Called with every message that is not the protocol's own. Reply with
  // send_message(); for a message that needs no reply call no_response(),
  // otherwise the missing reply is logged as a warning. While it runs,
  // last_mxmsg and last_connwrap are the message and its connection. A
  // DELIVERY_ERROR for a message of this server's, an event whose rule
  // reports delivery errors say, the class keeps to itself, logged at
  // DEBUG; BaseThreadedMultiplexerServer passes it to its handler.
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

  // Called when handle_message() threw, after the requester was sent
  // BACKEND_ERROR, unless a reply had gone out or the report failed. Return
  // true to keep serving (the default); return false and the exception
  // propagates out of serve_forever().
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

  // Queues a message. Required: `message`, a const MultiplexerMessage*
  // sent as it is, or a const std::string* or std::string payload, which
  // also needs `type` (std::uint32_t). Optional: `to` and `references`
  // (std::uint64_t), `workflow` (std::string or const std::string*), and
  // `multiplexer`, ONE, ALL (int) or a ConnectionWrapper; beside a whole
  // MultiplexerMessage only `multiplexer`, the rest being std::invalid_argument.
  // While a message is handled the defaults make it the reply: `to` the
  // requester, `references` and `workflow` the request's, `multiplexer`
  // the connection it came on, replaced when gone; a whole message gets
  // those of its fields that are empty filled in so, as Request::reply
  // fills a threaded server's. Outside a handler, from periodic_task()
  // say, there are none and the message is routed by its type through one
  // connection. Sent as every client sends
  // (SyncClient::queue): placed, or held while no connection is live, and
  // reported to the drop observer if given up on. Returns, in the
  // std::any, the message's ScheduledMessageTracker, the first copy's for
  // ALL, null when nothing took it. Not flushed: the loop writes it, and
  // close() flushes what is left.
  std::any send_message(Kwargs kwargs);

  void no_response() { _has_sent_response = true; }

  // Closes every connection as the client's shutdown(timeout) does, what
  // was sent before written first, `timeout` seconds at most; the server
  // cannot be used afterwards. Safe to call twice.
  void close(float timeout = CLOSE_FLUSH_SECONDS);

  // Answers the current request with BACKEND_ERROR carrying `message`, so the
  // requester's query() fails at once instead of waiting out its timeout.
  void report_error(const std::string& message);

 private:
  void __handle_message();
  void __handle_internal_message();
  // Answers the message being handled, a PING or a search, with a PING
  // carrying its payload back; `what` names it in the BACKEND_ERROR sent
  // instead when that echo would be over MAX_MESSAGE_SIZE.
  void _echo(const char* what);
  // Ends the reply defaults once a message has been handled.
  void _forget_request();
  // The two steps of loop_iter(): wait up to `timeout` seconds for a
  // message and keep it as the one to handle, false when the time passed;
  // then handle it, the reply defaults holding only meanwhile.
  // serve_forever() calls them apart, so that only the wait running out is
  // its poll's timeout.
  bool _receive_one(float timeout);
  void _handle_received();

 public:
  std::atomic<bool> working;  // cleared by stop(), from any thread, or by the loop thread directly

 protected:
  bool _has_sent_response;
  bool draining_ = false;
  float drain_seconds_ = 0.0f;
  std::chrono::steady_clock::time_point draining_since_;
  Routing drain_routing_ = direct_only_routing();

 private:
  // A connection to every address started, once, none waited for: the
  // ones to wait for, none when they were started before.
  std::vector<ConnectionWrapper> _start_connecting();

  std::unique_ptr<multiplexer::Client> __conn;
  const MultiplexerAddresses addresses_;
  bool connected_ = false;

 protected:
  multiplexer::Client* conn;
  std::uint64_t dropped_while_closing_ = 0;  // the closed client's, see dropped_while_closing()
  std::shared_ptr<MultiplexerMessage> last_mxmsg;
  ConnectionWrapper last_connwrap;

 private:
};

};  // namespace backend
};  // namespace multiplexer

#endif  // MX_MULTIPLEXER_BACKEND_BASE_MULTIPLEXER_SERVER_H_
