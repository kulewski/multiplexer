// BaseMultiplexerServer: the loop, the reply defaults and the protocol
// messages a backend answers itself. See the header for the design.
#include "multiplexer/backend/base_multiplexer_server.h"

#include <memory>

namespace multiplexer {
namespace backend {

BaseMultiplexerServer::BaseMultiplexerServer(const MultiplexerAddresses& addresses, PeerType type)
    : working(true),
      _has_sent_response(false),
      __conn(new multiplexer::Client(type)),
      addresses_(addresses),
      conn(__conn.get()) {}

BaseMultiplexerServer::BaseMultiplexerServer(multiplexer::Client* conn_, PeerType type)
    : working(true), _has_sent_response(false), conn(conn_) {
  Assert(conn->client_type() == type);
}

BaseMultiplexerServer::~BaseMultiplexerServer() {}

void BaseMultiplexerServer::loop_iter(float timeout) {
  std::pair<std::shared_ptr<MultiplexerMessage>, ConnectionWrapper> received = conn->receive_message(timeout);
  last_mxmsg = received.first;
  last_connwrap = received.second;
  __handle_message();
}

void BaseMultiplexerServer::connect() {
  if (connected_) {
    return;
  }
  connected_ = true;
  for (const MultiplexerAddress& address : addresses_) {
    conn->connect(address.first, address.second);
  }
}

void BaseMultiplexerServer::serve_forever(float poll, float drain_seconds) {
  conn->bind_to_current_thread();
  drain_seconds_ = drain_seconds;
  try {
    connect();
    while (working) {
      if (draining_ && drained()) {
        break;
      }
      try {
        loop_iter(poll);
      } catch (Client::OperationTimedOut&) {
      }
      periodic_task();
    }
    // A drain serves what was already on its way: the requests the client
    // had read off the sockets when the drain ended are handled before the
    // connections close, whether the drain ended on the confirmation or
    // on its cap.
    while (draining_ && working && conn->has_incoming_messages()) {
      try {
        loop_iter(0);
      } catch (Client::OperationTimedOut&) {
        break;
      }
    }
  } catch (...) {
    close();
    throw;
  }
  close();
}

void BaseMultiplexerServer::start_draining() {
  if (draining_) {
    return;
  }
  draining_ = true;
  draining_since_ = std::chrono::steady_clock::now();
  conn->set_routing(drain_routing_);
}

bool BaseMultiplexerServer::drained() const {
  if (!draining_) {
    return false;
  }
  return std::chrono::duration<float>(std::chrono::steady_clock::now() - draining_since_).count() >= drain_seconds_ ||
         (nothing_more_arrives(drain_routing_) && conn->routing_acknowledged());
}

// Builds and queues a message. While a request is being handled the
// defaults make it the reply: addressed to the requester, referencing the
// request, on the connection the request arrived on. Kwargs is typed by
// std::any, so a key must hold exactly the type documented in the header.
std::any BaseMultiplexerServer::send_message(Kwargs kwargs) {
  _has_sent_response = true;

  DbgAssert(kwargs.check_keys(KwargsKeys()("message")("to")("type")("references")("workflow")));
  Assert(kwargs.has_key("message"));
  DbgAssert(kwargs.unsafe_is<const MultiplexerMessage*>("message") || kwargs.unsafe_is<const std::string*>("message") ||
            kwargs.unsafe_is<std::string>("message"));
  DbgAssert(kwargs.empty_or<std::uint32_t>("type"));
  DbgAssert(kwargs.empty_or<std::uint64_t>("references"));
  DbgAssert(kwargs.empty_or<std::uint64_t>("to"));
  DbgAssert(kwargs.empty_or<std::string>("workflow") || kwargs.unsafe_is<const std::string*>("workflow"));

  // defaults
  kwargs.set_default("workflow", last_mxmsg->workflow());
  kwargs.set_default("references", last_mxmsg->id());
  kwargs.set_default("to", last_mxmsg->from());
  kwargs.set_default("multiplexer", last_connwrap);

  std::unique_ptr<MultiplexerMessage> _mxmsg;
  const MultiplexerMessage* mxmsg;
  if (!kwargs.unsafe_is<const MultiplexerMessage*>("message")) {
    // Construct new MultiplexerMessage using some info from kwargs.
    _mxmsg.reset(new MultiplexerMessage());
    // id and from: the server drops messages without a sender, and replies
    // are matched by the id they reference (same as the Python client does)
    _mxmsg->set_id(conn->random64());
    _mxmsg->set_from(conn->instance_id());

    // set message
    if (kwargs.unsafe_is<std::string>("message")) {
      _mxmsg->set_message(kwargs.get<const std::string&>("message"));
    } else if (kwargs.unsafe_is<const std::string*>("message")) {
      _mxmsg->set_message(*kwargs.get<const std::string*>("message"));
    } else {
      AssertMsg(false, "impossible");
    }
    // type
    _mxmsg->set_type(kwargs.get<std::uint32_t>("type"));
    // to
    _mxmsg->set_to(kwargs.get<std::uint64_t>("to"));
    // references
    _mxmsg->set_references(kwargs.get<std::uint64_t>("references"));
    // workflow
    if (kwargs.unsafe_is<const std::string*>("workflow")) {
      _mxmsg->set_workflow(*kwargs.get<const std::string*>("workflow"));
    } else if (kwargs.unsafe_is<std::string>("workflow")) {
      _mxmsg->set_workflow(kwargs.get<const std::string&>("workflow"));
    }

    mxmsg = _mxmsg.get();

  } else {
    mxmsg = kwargs.get<const MultiplexerMessage*>("message");
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
    return conn->schedule_one(*mxmsg, kwargs.get<const ConnectionWrapper&>("multiplexer"));
  }
  AssertMsg(false, "impossible");
  return false;  // unreachable
}

void BaseMultiplexerServer::notify_start() {
  DbgAssertMsg(!_has_sent_response,
               "If you use notify_start(), place it as a first function in "
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
  } catch (std::exception& error) {
    MX_LOG(ERROR, LOWVERBOSITY, TEXT(std::string("exception in handle_message: ") + error.what()));
    if (!_has_sent_response) {
      // Same as the Python backend: tell the requester instead of leaving it
      // to time out.
      report_error(error.what());
    }
    if (!on_handler_exception(error)) {
      throw;
    }
  }
}

void BaseMultiplexerServer::report_error(const std::string& message) {
  send_message(Kwargs().set("message", message).set("type", types::BACKEND_ERROR));
}

// The protocol messages a backend must answer: a client's search for a
// backend gets a PING referencing it, which is how the client learns this
// backend is alive and where to send the request; a PING without
// references is an echo request and is answered with the same payload.
void BaseMultiplexerServer::__handle_internal_message() {
  const MultiplexerMessage& mxmsg = *last_mxmsg;
  switch (mxmsg.type()) {
    case types::BACKEND_FOR_PACKET_SEARCH:
      if (should_respond_to_backend_for_packet_search()) {
        send_message(Kwargs().set("message", std::string()).set("type", types::PING));
      } else {
        no_response();  // the policy declines: let the client find another backend
      }
      break;

    case types::PING:
      if (!mxmsg.references()) {
        DbgAssert(mxmsg.id());
        // The echo, built as send_message() builds a reply, goes back with
        // the payload as it came. One that would be over MAX_MESSAGE_SIZE is
        // answered with BACKEND_ERROR saying so, rather than not at all.
        MultiplexerMessage echo;
        echo.set_id(conn->random64());
        echo.set_from(conn->instance_id());
        echo.set_message(mxmsg.message());
        echo.set_type(types::PING);
        echo.set_to(mxmsg.from());
        echo.set_references(mxmsg.id());
        echo.set_workflow(mxmsg.workflow());
        if (echo.ByteSizeLong() > MAX_MESSAGE_SIZE) {
          echo.set_type(types::BACKEND_ERROR);
          echo.set_message("the echo of a PING of " + std::to_string(mxmsg.message().size()) +
                           " bytes would be over MAX_MESSAGE_SIZE");
          echo.clear_workflow();
        }
        send_message(Kwargs().set("message", static_cast<const MultiplexerMessage*>(&echo)));
      } else {
        no_response();
      }
      break;

    default:
      MX_LOG(ERROR, LOWVERBOSITY, TEXT("received unknown meta-packet type=" + repr(mxmsg.type())));
  }  // switch
}

void BaseMultiplexerServer::close() {
  if (conn == NULL) {
    return;
  }
  conn->flush_all(CLOSE_FLUSH_SECONDS);  // the last replies go out before the sockets close
  conn->shutdown();
  __conn.reset();
  conn = NULL;
}

};  // namespace backend
};  // namespace multiplexer
