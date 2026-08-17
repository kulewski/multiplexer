// Base class for backends written in C++: connects to the multiplexers,
// runs the receive loop, answers the protocol's own messages, and calls
// handle_message() with everything else. The Python counterpart is
// BaseMultiplexerServer in clients.py; both behave the same way, including
// what happens when a handler throws. docs/api_cpp.md is the user's view.
//
// A backend is driven by serve_forever(): it blocks in the client's
// read, so heartbeats and reconnects happen on their own and the peer type
// need not be passive. One message is handled at a time. After every
// iteration, message or poll timeout, periodic_task() runs: the place for
// work on the backend's own schedule and for noticing a request to leave.
// The library installs no signal handlers; a handler of your own should
// only set a sig_atomic_t that periodic_task() reads.
#ifndef MX_MULTIPLEXER_BACKEND_BASE_MULTIPLEXER_SERVER_H_
#define MX_MULTIPLEXER_BACKEND_BASE_MULTIPLEXER_SERVER_H_

#include <atomic>
#include <chrono>
#include <string>
#include <utility>
#include <vector>

#include <boost/cstdint.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>

#include "lib/exception.h"
#include "lib/kwargs.h"
#include "multiplexer/Multiplexer.pb.h" /* generated */
#include "multiplexer/client.h"

namespace multiplexer {
namespace backend {

typedef std::pair<std::string, boost::uint16_t> MultiplexerAddress;
typedef std::vector<MultiplexerAddress> MultiplexerAddresses;
typedef boost::uint32_t PeerType;

using mx::util::kwargs::Kwargs;
using mx::util::kwargs::KwargsKeys;

// See the file comment. Subclass, implement handle_message(), and call
// serve_forever(). Not thread-safe.
class BaseMultiplexerServer {

public:
  // for use with send_message(..., multiplexer=(ONE|ALL|a ConnectionWrapper),
  // ...)
  static const int ONE = 1;
  static const int ALL = 2;

protected:
  // Connects to every address as a peer of `type`. The second form uses a
  // Client the caller owns and keeps.
  BaseMultiplexerServer(const MultiplexerAddresses &addresses, PeerType type);

  BaseMultiplexerServer(multiplexer::Client *conn, PeerType type);

public:
  virtual ~BaseMultiplexerServer();

  // One step: wait up to `timeout` seconds for a message and handle it.
  // Throws Client::OperationTimedOut when the time passes.
  virtual void loop_iter(float timeout = DEFAULT_READ_TIMEOUT);

  // The loop: until `working` is cleared or a drain is over, wait up to
  // `poll` seconds for a message, handle it if one came, call
  // periodic_task(); then close the connections. `drain_seconds` is how
  // long to keep serving after start_draining(), unless drained() is
  // overridden. The calling thread becomes the backend's thread: a backend
  // may be built on one thread and served from another, but from here on
  // only this thread may touch it.
  void serve_forever(float poll = 1.0f, float drain_seconds = 0.0f);

  // Draining: a backend about to exit stops answering the search clients
  // use to find a backend, so that no retried request is sent to it, while
  // it keeps serving what the multiplexer still routes to it. Call
  // start_draining() from periodic_task() when asked to leave; serve_forever
  // returns once drained().
  void start_draining();
  bool draining() const { return draining_; }
  // Asks serve_forever() to return, from any thread: it notices within one
  // poll, closes the connections and returns.
  void stop() { working = false; }

protected:
  // Called with every message that is not the protocol's own. Reply with
  // send_message(); for a message that needs no reply call no_response(),
  // otherwise the missing reply is logged as a warning. While it runs,
  // last_mxmsg and last_connwrap are the message and its connection.
  virtual void handle_message(MultiplexerMessage &) = 0;

  // Called after every iteration of serve_forever(), message or not, so at
  // least once per `poll` seconds. Override for work on your own schedule
  // and for noticing a request to leave: call start_draining() or clear
  // `working`. Does nothing by default.
  virtual void periodic_task() {}

  // Whether the drain is over and serve_forever() may return; checked after
  // every iteration while draining. Default: `drain_seconds` have passed
  // since start_draining(). Override to wait for your own condition, for
  // example `BaseMultiplexerServer::drained() && in_flight_ == 0`.
  virtual bool drained() const;

  // Called when handle_message() threw, after BACKEND_ERROR went to the
  // requester. Return true to keep serving (the default); return false and
  // the exception propagates out of serve_forever().
  virtual bool on_handler_exception(const std::exception &) { return true; }

  // Whether to answer a client's search for a backend; override for your
  // own condition. False while draining.
  virtual bool should_respond_to_backend_for_packet_search() const { return !draining_; }

protected:
  template <typename Message> static Message inline parse_message(const MultiplexerMessage &mxmsg) {
    return parse_message<Message>(mxmsg.message());
  }

  template <typename Message> static Message inline parse_message(const std::string &from) {
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
   *	    to:		boost::uint64_t
   *	    references: boost::uint64_t
   *	    type:	boost::uint32_t
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
  boost::any send_message(Kwargs kwargs);

  void no_response() { _has_sent_response = true; }

  // Closes every connection; the server cannot be used afterwards. Safe to
  // call twice.
  void close();

  // Answers the current request with BACKEND_ERROR carrying `message`, so the
  // requester's query() fails at once instead of waiting out its timeout.
  void report_error(const std::string &message);

private:
  void __handle_message();
  void __handle_internal_message();

public:
  std::atomic<bool> working; // cleared by stop(), from any thread, or by the loop thread directly

protected:
  bool _has_sent_response;
  bool draining_ = false;
  float drain_seconds_ = 0.0f;
  std::chrono::steady_clock::time_point draining_since_;

private:
  boost::scoped_ptr<multiplexer::Client> __conn;

protected:
  multiplexer::Client *conn;
  boost::shared_ptr<MultiplexerMessage> last_mxmsg;
  ConnectionWrapper last_connwrap;

private:
};

}; // namespace backend
}; // namespace multiplexer

#endif // MX_MULTIPLEXER_BACKEND_BASE_MULTIPLEXER_SERVER_H_
