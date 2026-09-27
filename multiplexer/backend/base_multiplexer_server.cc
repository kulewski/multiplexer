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
  try {
    __handle_message();
  } catch (...) {
    _forget_request();
    throw;
  }
  _forget_request();
}

// The reply defaults hold while a message is handled, and only then: what
// periodic_task() sends is no reply to the last request.
void BaseMultiplexerServer::_forget_request() {
  last_mxmsg.reset();
  last_connwrap = ConnectionWrapper();
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
    // The drain is over. What arrives from now on is refused at once, so
    // that its sender retries elsewhere, and what was already on its way,
    // read off the sockets when the drain ended, is handled before the
    // connections close, whether the drain ended on the confirmation or on
    // its cap: a number fixed here, however fast more comes, where every
    // reply's turn of the loop read more, handled in turn, so that under
    // steady load the drain never ended.
    if (draining_ && working) {
      conn->refuse_arrivals();
      while (working && conn->has_incoming_messages()) {
        try {
          loop_iter(0);
        } catch (Client::OperationTimedOut&) {
          break;
        }
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
// request, on the connection the request arrived on; outside a handler
// there are none, and the message is routed by its type. Kwargs is typed
// by std::any, so a key must hold exactly the type documented in the
// header.
std::any BaseMultiplexerServer::send_message(Kwargs kwargs) {
  DbgAssert(kwargs.check_keys(KwargsKeys()("message")("to")("type")("references")("workflow")("multiplexer")));
  Assert(kwargs.has_key("message"));
  DbgAssert(kwargs.unsafe_is<const MultiplexerMessage*>("message") || kwargs.unsafe_is<const std::string*>("message") ||
            kwargs.unsafe_is<std::string>("message"));
  DbgAssert(kwargs.empty_or<std::uint32_t>("type"));
  DbgAssert(kwargs.empty_or<std::uint64_t>("references"));
  DbgAssert(kwargs.empty_or<std::uint64_t>("to"));
  DbgAssert(kwargs.empty_or<std::string>("workflow") || kwargs.unsafe_is<const std::string*>("workflow"));

  // defaults
  if (last_mxmsg) {
    kwargs.set_default("workflow", last_mxmsg->workflow());
    kwargs.set_default("references", last_mxmsg->id());
    kwargs.set_default("to", last_mxmsg->from());
    kwargs.set_default("multiplexer", last_connwrap);
  } else {
    kwargs.set_default("workflow", std::string());  // read unchecked below, as every default is
    kwargs.set_default("references", std::uint64_t(0));
    kwargs.set_default("to", std::uint64_t(0));
    kwargs.set_default("multiplexer", ONE);
  }

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

  // Sent as every client sends (SyncClient::queue): placed, or held until
  // a connection comes up, and reported if given up on. A reply counts as
  // sent once something took it: one that threw leaves the request to
  // report_error().
  Client::ScheduledMessageTracker tracker = Client::ScheduledMessageTracker(Client::BasicScheduledMessageTracker());
  if (kwargs.unsafe_is<int>("multiplexer")) {
    switch (kwargs.get<int>("multiplexer")) {
      case ALL:
        tracker = conn->queue_all(*mxmsg);
        break;
      case ONE:
        tracker = conn->queue(*mxmsg);
        break;
      default:
        AssertMsg(false, "impossible");
    }
  } else if (kwargs.unsafe_is<ConnectionWrapper>("multiplexer")) {
    // that connection while it lives, another once it is gone
    tracker = conn->queue(*mxmsg, DEFAULT_TIMEOUT,
                          std::make_shared<Lane>(kwargs.get<const ConnectionWrapper&>("multiplexer")));
  } else {
    AssertMsg(false, "impossible");
  }
  _has_sent_response = _has_sent_response || static_cast<bool>(tracker);
  return tracker;
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
      // Same as the Python BaseMultiplexerServer: tell the requester instead
      // of leaving it to time out; a report that fails leaves it to its
      // timeout, and the handler's exception decides all the same.
      try {
        report_error(error.what());
      } catch (const std::exception& reporting) {
        MX_LOG(ERROR, LOWVERBOSITY,
               TEXT(std::string("could not report the exception to the requester: ") + reporting.what()));
      }
    }
    if (!on_handler_exception(error)) {
      throw;
    }
  }
}

// The echo, built as send_message() builds a reply, goes back with the
// payload as it came. One that would be over MAX_MESSAGE_SIZE is answered
// with BACKEND_ERROR saying so, rather than not at all.
void BaseMultiplexerServer::_echo(const char* what) {
  const MultiplexerMessage& mxmsg = *last_mxmsg;
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
    echo.set_message(std::string("the echo of a ") + what + " of " + std::to_string(mxmsg.message().size()) +
                     " bytes would be over MAX_MESSAGE_SIZE");
    echo.clear_workflow();
  }
  send_message(Kwargs().set("message", static_cast<const MultiplexerMessage*>(&echo)));
}

void BaseMultiplexerServer::report_error(const std::string& message) {
  send_message(Kwargs().set("message", message).set("type", types::BACKEND_ERROR));
}

// The protocol messages this class answers itself, each with a PING that
// references it and carries its payload back (_echo): a client's search
// for a backend, which is how the client learns this backend is alive and
// where to send the request, and a PING without references, an echo
// request.
void BaseMultiplexerServer::__handle_internal_message() {
  const MultiplexerMessage& mxmsg = *last_mxmsg;
  switch (mxmsg.type()) {
    case types::BACKEND_FOR_PACKET_SEARCH:
      if (should_respond_to_backend_for_packet_search()) {
        _echo("search");
      } else {
        no_response();  // the policy declines: let the client find another backend
      }
      break;

    case types::PING:
      if (!mxmsg.references()) {
        DbgAssert(mxmsg.id());
        _echo("PING");
      } else {
        no_response();
      }
      break;

    default:
      MX_LOG(ERROR, LOWVERBOSITY, TEXT("received unknown meta-packet type=" + repr(mxmsg.type())));
  }  // switch
}

void BaseMultiplexerServer::close(float timeout) {
  if (conn == NULL) {
    return;
  }
  if (!conn->orphaned()) {
    // What arrives meanwhile, and what was read and will not be handled
    // now, is refused, so that its sender retries elsewhere at once rather
    // than wait out its timeout.
    conn->refuse_arrivals();
    conn->refuse_unread();
  }
  conn->shutdown(timeout);  // the last replies go out first
  __conn.reset();
  conn = NULL;
}

};  // namespace backend
};  // namespace multiplexer
