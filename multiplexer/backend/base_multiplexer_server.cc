// BaseMultiplexerServer: the loop, the reply defaults and the protocol
// messages a backend answers itself. See the header for the design.
#include <boost/foreach.hpp>
#include <boost/scoped_ptr.hpp>

#include "multiplexer/backend/base_multiplexer_server.h"

namespace multiplexer {
namespace backend {

BaseMultiplexerServer::BaseMultiplexerServer(const MultiplexerAddresses &addresses, PeerType type)
    : working(true), _has_sent_response(false), __conn(new multiplexer::Client(type)), conn(__conn.get()) {
  BOOST_FOREACH (const MultiplexerAddress &address, addresses) {
    conn->connect(address.first, address.second);
  }
}

BaseMultiplexerServer::BaseMultiplexerServer(multiplexer::Client *conn_, PeerType type)
    : working(true), _has_sent_response(false), conn(conn_) {
  Assert(conn->client_type() == type);
}

BaseMultiplexerServer::~BaseMultiplexerServer() {}

void BaseMultiplexerServer::loop_iter(float timeout) {
  std::pair<boost::shared_ptr<MultiplexerMessage>, ConnectionWrapper> received = conn->receive_message(timeout);
  last_mxmsg = received.first;
  last_connwrap = received.second;
  __handle_message();
}

void BaseMultiplexerServer::serve_forever(float poll, float drain_seconds) {
  conn->bind_to_current_thread();
  drain_seconds_ = drain_seconds;
  try {
    while (working) {
      if (draining_ && drained())
        break;
      try {
        loop_iter(poll);
      } catch (Client::OperationTimedOut &) {
      }
      periodic_task();
    }
  } catch (...) {
    close();
    throw;
  }
  close();
}

void BaseMultiplexerServer::start_draining() {
  if (draining_)
    return;
  draining_ = true;
  draining_since_ = std::chrono::steady_clock::now();
}

bool BaseMultiplexerServer::drained() const {
  return draining_ &&
         std::chrono::duration<float>(std::chrono::steady_clock::now() - draining_since_).count() >= drain_seconds_;
}

// Builds and queues a message. While a request is being handled the
// defaults make it the reply: addressed to the requester, referencing the
// request, on the connection the request arrived on. Kwargs is typed by
// boost::any, so a key must hold exactly the type documented in the header.
boost::any BaseMultiplexerServer::send_message(Kwargs kwargs) {
  _has_sent_response = true;

  DbgAssert(kwargs.check_keys(KwargsKeys()("message")("to")("type")("references")("workflow")));
  Assert(kwargs.has_key("message"));
  DbgAssert(kwargs.unsafe_is<const MultiplexerMessage *>("message") ||
            kwargs.unsafe_is<const std::string *>("message") || kwargs.unsafe_is<std::string>("message"));
  DbgAssert(kwargs.empty_or<boost::uint32_t>("type"));
  DbgAssert(kwargs.empty_or<boost::uint64_t>("references"));
  DbgAssert(kwargs.empty_or<boost::uint64_t>("to"));
  DbgAssert(kwargs.empty_or<std::string>("workflow") || kwargs.unsafe_is<const std::string *>("workflow"));

  // defaults
  kwargs.set_default("workflow", last_mxmsg->workflow());
  kwargs.set_default("references", last_mxmsg->id());
  kwargs.set_default("to", last_mxmsg->from());
  kwargs.set_default("multiplexer", last_connwrap);

  boost::scoped_ptr<MultiplexerMessage> _mxmsg;
  const MultiplexerMessage *mxmsg;
  if (!kwargs.unsafe_is<const MultiplexerMessage *>("message")) {
    // Construct new MultiplexerMessage using some info from kwargs.
    _mxmsg.reset(new MultiplexerMessage());
    // id and from: the server drops messages without a sender, and replies
    // are matched by the id they reference (same as the Python client does)
    _mxmsg->set_id(conn->random64());
    _mxmsg->set_from(conn->instance_id());

    // set message
    if (kwargs.unsafe_is<std::string>("message"))
      _mxmsg->set_message(kwargs.get<const std::string &>("message"));
    else if (kwargs.unsafe_is<const std::string *>("message"))
      _mxmsg->set_message(*kwargs.get<const std::string *>("message"));
    else
      AssertMsg(false, "impossible");
    // type
    _mxmsg->set_type(kwargs.get<boost::uint32_t>("type"));
    // to
    _mxmsg->set_to(kwargs.get<boost::uint64_t>("to"));
    // references
    _mxmsg->set_references(kwargs.get<boost::uint64_t>("references"));
    // workflow
    if (kwargs.unsafe_is<const std::string *>("workflow"))
      _mxmsg->set_workflow(*kwargs.get<const std::string *>("workflow"));
    else if (kwargs.unsafe_is<std::string>("workflow"))
      _mxmsg->set_workflow(kwargs.get<const std::string &>("workflow"));

    mxmsg = _mxmsg.get();

  } else {
    mxmsg = kwargs.get<const MultiplexerMessage *>("message");
  }

  if (kwargs.unsafe_is<int>("multiplexer")) {
    switch (kwargs.get<int>("multiplexer")) {
    case ALL:
      return conn->schedule_all(*mxmsg);
    case ONE:
      return conn->schedule_one(*mxmsg);
    default:
      AssertMsg(false, "impossible");
    }
  } else if (kwargs.unsafe_is<ConnectionWrapper>("multiplexer")) {
    return conn->schedule_one(*mxmsg, kwargs.get<const ConnectionWrapper &>("multiplexer"));
  }
  AssertMsg(false, "impossible");
  return false; // unreachable
}

void BaseMultiplexerServer::notify_start() {
  DbgAssertMsg(!_has_sent_response, "If you use notify_start(), place it as a first function in "
                                    "your handle_message() code");
  send_message(Kwargs()
                   .set("message", std::string(""))
                   .set("type", types::REQUEST_RECEIVED)
                   .set("references", last_mxmsg->id()));
  _has_sent_response = false;
}

// One message: the protocol's own are answered here, the rest go to the
// subclass. A handler that throws is reported to the requester, then
// on_handler_exception() decides whether the loop goes on (the default) or
// the exception propagates.
void BaseMultiplexerServer::__handle_message() {
  _has_sent_response = false;
  try {
    if (last_mxmsg->type() <= types::MAX_MULTIPLEXER_META_PACKET) {
      __handle_internal_message();
      if (!_has_sent_response) {
        MX_LOG(WARNING, LOWVERBOSITY,
               TEXT("__handle_internal_message() finished w/o exception and "
                    "w/o any response"));
      }
    } else {
      handle_message(*last_mxmsg);
      if (!_has_sent_response) {
        MX_LOG(WARNING, LOWVERBOSITY,
               TEXT("handle_message() finished w/o exception and w/o any "
                    "response"));
      }
    }
  } catch (std::exception &error) {
    MX_LOG(ERROR, LOWVERBOSITY, TEXT(std::string("exception in handle_message: ") + error.what()));
    if (!_has_sent_response) {
      // Same as the Python backend: tell the requester instead of leaving it
      // to time out.
      report_error(error.what());
    }
    if (!on_handler_exception(error))
      throw;
  }
}

void BaseMultiplexerServer::report_error(const std::string &message) {
  send_message(Kwargs().set("message", message).set("type", types::BACKEND_ERROR));
}

// The protocol messages a backend must answer: a client's search for a
// backend gets a PING referencing it, which is how the client learns this
// backend is alive and where to send the request; a PING without
// references is an echo request and is answered with the same payload.
void BaseMultiplexerServer::__handle_internal_message() {
  const MultiplexerMessage &mxmsg = *last_mxmsg;
  switch (mxmsg.type()) {
  case types::BACKEND_FOR_PACKET_SEARCH:
    if (should_respond_to_backend_for_packet_search())
      send_message(Kwargs().set("message", std::string()).set("type", types::PING));
    else
      no_response(); // draining: let the client find another backend
    break;

  case types::PING:
    if (!mxmsg.references()) {
      DbgAssert(mxmsg.id());
      send_message(Kwargs()
                       .set("message", mxmsg.message())
                       //.set("flush", true)
                       .set("type", types::PING));
    } else {
      no_response();
    }
    break;

  default:
    MX_LOG(ERROR, LOWVERBOSITY, TEXT("received unknown meta-packet type=" + repr(mxmsg.type())));
  } // switch
}

void BaseMultiplexerServer::close() {
  if (conn == NULL)
    return;
  conn->shutdown();
  __conn.reset();
  conn = NULL;
}

}; // namespace backend
}; // namespace multiplexer
