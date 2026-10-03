// Server: construction, accepting, and the routing path. The design is in
// server.h; this file is the whole per-message path, kept in one
// translation unit so it inlines the way it did as header code.
#include "multiplexer/server.h"

#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <fstream>
#include <functional>
#include <iterator>
#include <limits>
#include <stdexcept>
#include <vector>

#include "lib/exception.h"
#include "lib/fingerprint.h"
#include "lib/logging/logging.h"
#include "lib/memory.h"
#include "lib/repr.h"
#include "lib/seconds.h"
#include "multiplexer/mxlog/type_id_constants.h" /* generated */

namespace multiplexer {

using mx::repr;

// `host` must be an IP address; names are not resolved. Port 0 lets the
// system choose, see local_port().
Server::Server(asio::io_service& io_service, const std::string& host, unsigned short port)
    : Base(io_service),
      acceptor_(io_service, asio::ip::tcp::endpoint(asio::ip::address::from_string(host), port)),
      io_service_(io_service),
      rules_timer_(io_service),
      session_timer_(io_service),
      flush_timer_(io_service),
      peers_timer_(io_service),
      accept_timer_(io_service),
      drain_timer_(io_service),
      drops_(io_service, "multiplexer.server") {}

void Server::start() {
  _start_accept();
  _arm_rules_check();
}

void Server::stop(float drain_seconds, std::function<void()> stopped) {
  MX_DCHECK_RUN_ON(&owner_thread());
  if (stopped) {
    on_stopped_ = std::move(stopped);
  }
  const bool again = stopping_;
  if (!again) {
    stopping_ = true;
    stop_started_ = std::chrono::steady_clock::now();
  }
  asio::error_code ignored;
  acceptor_.close(ignored);
  accept_timer_.cancel(ignored);
  rules_timer_.cancel(ignored);
  // A copy: every shutdown() below takes its connection out of accepted_.
  std::vector<Connection::pointer> live;
  for (const auto& entry : accepted_) {
    if (Connection::pointer connection = entry.second.lock()) {
      live.push_back(connection);
    }
  }
  const bool drain = !again && drain_seconds > 0;
  if (drain) {
    unsigned int registered = 0;
    for (const Connection::pointer& connection : live) {
      registered += connection->registered();
    }
    MX_LOG(INFO, LOWVERBOSITY,
           CTX("multiplexer.server") TEXT("stopping: " + repr(registered) +
                                          " connection(s) close once what is queued for them is written, within " +
                                          repr(drain_seconds) + " s"));
    drain_timer_.expires_after(mx::from_seconds(drain_seconds));
    drain_timer_.async_wait(
        [weak = weak_pointer(shared_from_this())](const asio::error_code& error) { _on_drain_deadline(weak, error); });
  } else {
    drain_timer_.cancel(ignored);
  }
  for (const Connection::pointer& connection : live) {
    if (connection->shuts_down()) {
      continue;
    }
    if (drain && connection->registered()) {
      connection->close_when_flushed(CLOSE_READ_SECONDS);
    } else {
      connection->shutdown();  // and one that never sent its welcome has nothing queued
    }
  }
  _stop_if_done();
}

// The drain's deadline: whatever still holds messages is shut down, its
// queue dropped; a connection already reading on to its peer's end keeps
// its own bound.
void Server::_on_drain_deadline(weak_pointer server, const asio::error_code& error) {
  pointer self = server.lock();
  if (error || !self) {
    return;
  }
  std::vector<Connection::pointer> late;
  for (const auto& entry : self->accepted_) {
    Connection::pointer connection = entry.second.lock();
    if (connection && connection->living()) {
      late.push_back(connection);
    }
  }
  if (!late.empty()) {
    MX_LOG(WARNING, LOWVERBOSITY,
           CTX("multiplexer.server") TEXT("stopping: " + repr(late.size()) +
                                          " connection(s) still held messages at the deadline; closing them"));
  }
  for (const Connection::pointer& connection : late) {
    if (!connection->shuts_down()) {
      connection->shutdown();
    }
  }
  self->_stop_if_done();
}

void Server::connection_closed(Connection* conn) {
  accepted_.erase(conn);
  if (stopping_) {
    stop_dropped_read_ += conn->dropped_while_closing();
    _stop_if_done();
  }
}

void Server::_stop_if_done() {
  if (!stopping_ || stop_done_ || !accepted_.empty()) {
    return;
  }
  stop_done_ = true;
  asio::error_code ignored;
  drain_timer_.cancel(ignored);
  const double seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - stop_started_).count();
  std::string line = "stopped: every connection closed in " + repr(static_cast<long>(seconds * 1000)) + " ms";
  if (stop_dropped_queued_ || stop_dropped_read_) {
    line += "; dropped " + repr(stop_dropped_queued_) + " message(s) still queued for peers and " +
            repr(stop_dropped_read_) + " that arrived while their connection closed";
  }
  MX_LOG(stop_dropped_queued_ || stop_dropped_read_ ? WARNING : INFO, LOWVERBOSITY,
         CTX("multiplexer.server") TEXT(line));
  drops_.flush();
  if (peers_write_pending_) {
    asio::error_code ignored;
    peers_timer_.cancel(ignored);
    _write_peers_file();  // the departures, before the loop may end
  }
  // After the peers left, so their departures are in the file; the
  // session's deadline timer would otherwise keep the loop alive.
  stop_recording("the multiplexer stopped");
  if (on_stopped_) {
    std::function<void()> stopped = std::move(on_stopped_);
    on_stopped_ = nullptr;
    stopped();
  }
}

namespace {
// Whether rules route to a peer type, ALL_TYPES included: never to a
// reserved one, the controllers mxcontrol connects as being the only such
// peers there are, which only answers and addressed messages reach.
bool routable(std::uint32_t peer_type) { return peer_type > peers::MAX_MULTIPLEXER_SPECIAL_PEER_TYPE; }

// TCP keepalive on an accepted connection: the kernel probes it once it
// has been silent NO_HEARTBIT_SO_PREPARE_DROP_INTERVAL seconds, every
// KEEPALIVE_PROBE_INTERVAL seconds, and closes it once the probes went
// unanswered NO_HEARTBIT_SO_REALLY_DROP_INTERVAL seconds. A peer whose host
// or network is gone gives its descriptor back after the 90 s the heartbeats
// give an active peer, also a passive one and one that has not sent its
// welcome, which nothing else asks; a live peer's kernel answers for it,
// however long its program stays away. Best effort: an option the system
// lacks keeps its default.
void keep_alive(asio::ip::tcp::socket& socket) {
  const int descriptor = socket.native_handle();
#if defined(TCP_KEEPIDLE)
  const int idle = static_cast<int>(NO_HEARTBIT_SO_PREPARE_DROP_INTERVAL);
  ::setsockopt(descriptor, IPPROTO_TCP, TCP_KEEPIDLE, &idle, sizeof idle);
#endif
#if defined(TCP_KEEPINTVL)
  const int interval = static_cast<int>(KEEPALIVE_PROBE_INTERVAL);
  ::setsockopt(descriptor, IPPROTO_TCP, TCP_KEEPINTVL, &interval, sizeof interval);
#endif
#if defined(TCP_KEEPCNT)
  const int probes = static_cast<int>(NO_HEARTBIT_SO_REALLY_DROP_INTERVAL / KEEPALIVE_PROBE_INTERVAL);
  ::setsockopt(descriptor, IPPROTO_TCP, TCP_KEEPCNT, &probes, sizeof probes);
#endif
  (void)descriptor;
  asio::error_code ignored;
  socket.set_option(asio::socket_base::keep_alive(true), ignored);  // last: the timer starts with the times above
}
}  // namespace

// The handler holds the Server weakly, as its timers do: one released
// after stop() while the loop runs has its aborted accept's handler find
// nothing, where it touched the freed Server.
void Server::_start_accept() {
  Connection::pointer new_connection = Connection::Create(io_service_, this->shared_from_this());
  acceptor_.async_accept(new_connection->socket(),
                         [weak = weak_pointer(shared_from_this()), new_connection](const asio::error_code& error) {
                           if (pointer self = weak.lock()) {
                             self->_handle_accept(new_connection, error);
                           }
                         });
}

void Server::_handle_accept(Connection::pointer new_connection, const asio::error_code& error) {
  MX_DCHECK_RUN_ON(&owner_thread());
  if (!acceptor_.is_open()) {
    // stop() closed the acceptor: do not re-arm, just drop the connection
    new_connection->shutdown();
    return;
  }
  if (error == asio::error::no_descriptors || error == asio::error::no_buffer_space ||
      error == asio::error::no_memory || error.value() == ENFILE) {
    if (drops_.first({ACCEPT_FAILED, ERROR, 0, 0}, [&] { return "cannot accept a connection: " + error.message(); })) {
      MX_LOG(ERROR, LogSummary::VERBOSITY,
             CTX("multiplexer.server") TEXT("cannot accept a connection: " + error.message() + "; trying again every " +
                                            repr(ACCEPT_RETRY_SECONDS) + " s"));
    }
    new_connection->shutdown();
    _accept_later();
    return;
  }
  _start_accept();
  if (!error) {
    // reading only, until the peer has introduced itself with
    // CONNECTION_WELCOME; routed traffic before that closes the connection
    accepted_[new_connection.get()] = new_connection;
    keep_alive(new_connection->socket());
    new_connection->start_only_read();
  } else {
    // the connection is dropped, or -- in fact -- has never been established
    new_connection->shutdown();
  }
}

void Server::_accept_later() {
  accept_timer_.expires_after(mx::from_seconds(ACCEPT_RETRY_SECONDS));
  accept_timer_.async_wait([weak = weak_pointer(shared_from_this())](const asio::error_code& error) {
    pointer self = weak.lock();
    if (!error && self && self->acceptor_.is_open()) {
      self->_start_accept();
    }
  });
}

void Server::handle_message(Connection::pointer conn, std::shared_ptr<const RawMessage> raw,
                            std::shared_ptr<MultiplexerMessage> msg) {
  MX_DCHECK_RUN_ON(&owner_thread());
  if (msg->sender() == instance_id_) {
    MX_LOG(ERROR, MEDIUMVERBOSITY,
           CTX("multiplexer.server") TEXT("received message from self") FLOW(msg->workflow())
               SKIPFILEIF(!(msg->logging_method() & multiplexer::LoggingMethod::FILE)));
    return;
  }
  return _handle_message(conn, *msg, raw);
}

// ---------------------------------------------------------------------------
// MessageMetaHandler

void Server::MessageMetaHandler::failed(const MultiplexerMessageDescription::RoutingRule& rule, std::uint32_t type) {
  __create_delivery_error_message(rule.include_original_packet_in_report());
  delivery_error_message->add_failed_type(type);
}

void Server::MessageMetaHandler::failed(const std::uint64_t to) {
  __create_delivery_error_message(msg.include_original_packet_in_report());
  delivery_error_message->set_failed_to(to);
}

void Server::MessageMetaHandler::unknown() {
  __create_delivery_error_message(msg.include_original_packet_in_report());
  delivery_error_message->set_is_known_type(false);
}

void Server::MessageMetaHandler::unroutable() {
  __create_delivery_error_message(msg.include_original_packet_in_report());
  delivery_error_message->set_is_known_type(true);
}

void Server::MessageMetaHandler::__create_delivery_error_message(bool include_original_packet_in_report) {
  if (delivery_error_message) {
    return;
  }
  delivery_error_message.reset(new DeliveryError());
  delivery_error_message->set_packet_id(this->msg.id());
  if (include_original_packet_in_report) {
    *delivery_error_message->mutable_original_message() = this->msg;
  }
}

// ---------------------------------------------------------------------------
// The routing path

void Server::_handle_message(Connection::pointer conn, const MultiplexerMessage& msg,
                             std::shared_ptr<const RawMessage> raw) {
  // Per message: off unless MX_LOG_VERBOSITY or --verbosity says CHATTERBOX.
  // The entry's data is the envelope without the payload.
  if (mx::logging::impl::should_log(DEBUG, CHATTERBOX)) {
    MultiplexerMessage envelope;
    envelope.set_id(msg.id());
    envelope.set_sender(msg.sender());
    envelope.set_to(msg.to());
    envelope.set_type(msg.type());
    envelope.set_timestamp(msg.timestamp());
    envelope.set_references(msg.references());
    envelope.set_workflow(msg.workflow());
    MX_LOG(DEBUG, CHATTERBOX,
           CTX("multiplexer.server") FLOW(msg.workflow())
               TEXT("handle_message(id=" + repr(msg.id()) + ", type=" + repr(msg.type()) + ")")
                   DATA(type_id_constants::MXSERVER_INCOMING_MULTIPLEXER_MESSAGE, envelope)
                       SKIPFILEIF(!(msg.logging_method() & multiplexer::LoggingMethod::FILE)));
  }

  if (memory_log_every_ && ++routed_messages_ % memory_log_every_ == 0) {
    MX_LOG(INFO, LOWVERBOSITY,
           CTX("multiplexer.server")
               TEXT("memory: heap_in_use=" + repr(mx::heap_in_use_bytes()) + " messages=" + repr(routed_messages_)));
  }

  MessageMetaHandler meta_handler(msg, conn, raw);
  do {
    if (_handle_message_inlined_rules(meta_handler)) {
      break;  // already handled
    }
    if (_handle_meta_message(meta_handler)) {
      break;  // already handled
    }

    // default message handler
    const Config::MessageDescriptionById& definitions = config_.message_description_by_id();
    Config::MessageDescriptionById::const_iterator definition = definitions.find(msg.type());
    if (definition == definitions.end()) {
      if (const std::string* line = drops_.first({UNKNOWN_TYPE, WARNING, msg.type(), 0}, [&] {
            return "message of unknown type " + repr(msg.type()) + "; dropping";
          })) {
        MX_LOG(WARNING, LogSummary::VERBOSITY, CTX("multiplexer.server") FLOW(msg.workflow()) TEXT(*line));
      }
      meta_handler.unknown();
      _record(meta_handler, 0, 0, RoutedMessage::UNKNOWN_TYPE, true);
      break;  // won't be handled at all
    }

    // it's known type
    const MultiplexerMessageDescription& description = definition->second;
    if (description.to().empty()) {
      // A reply type, or any other without a rule, sent without `to`: nobody
      // could ever receive it. Say so now rather than let the sender wait.
      if (const std::string* line = drops_.first({NO_RULE, WARNING, msg.type(), 0}, [&] {
            return "message of type " + repr(msg.type()) + " (" + description.name() +
                   ") has no routing rule and no `to`; dropping";
          })) {
        MX_LOG(WARNING, LogSummary::VERBOSITY, CTX("multiplexer.server") FLOW(msg.workflow()) TEXT(*line));
      }
      meta_handler.unroutable();
      _record(meta_handler, 0, 0, RoutedMessage::NO_RULE, true);
      break;
    }
    MX_LOG(DEBUG, CHATTERBOX,
           CTX("multiplexer.server") FLOW(msg.workflow())
               TEXT("received a message of type " + repr(msg.type()) + " (" + description.name() + "); scheduling...")
                   SKIPFILEIF(!(msg.logging_method() & multiplexer::LoggingMethod::FILE)));
    _schedule(meta_handler, description);

  } while (0);

  _handle_delivery_errors(meta_handler);
}

// Sends the collected DeliveryError to the message's `from`, as a message
// addressed by instance id, through _handle_message like any other. It
// asks for no delivery report itself, so a sender that is gone produces no
// second error.
void Server::_handle_delivery_errors(MessageMetaHandler& meta_handler) {
  if (!meta_handler.delivery_error_message) {
    return;  // no errors
  }

  // Per message, so off unless asked for: the line saying why, logged
  // where the failure was found, is the one that counts.
  MX_LOG(DEBUG, CHATTERBOX,
         CTX("multiplexer.server") FLOW(meta_handler.msg.workflow())
             TEXT("errors when delivering " + repr(meta_handler.msg.id()))
                 SKIPFILEIF(!(meta_handler.msg.logging_method() & multiplexer::LoggingMethod::FILE)));

  if (!meta_handler.msg.sender()) {  // sanity check
    return;
  }
  if (meta_handler.msg.sender() == instance_id_) {  // sanity check
    return;
  }

  MultiplexerMessage mxmsg;
  mxmsg.set_id(random_());
  mxmsg.set_sender(instance_id_);
  mxmsg.set_to(meta_handler.msg.sender());
  mxmsg.set_report_delivery_error(false);
  mxmsg.set_type(types::DELIVERY_ERROR);
  meta_handler.delivery_error_message->SerializeToString(mxmsg.mutable_message());
  mxmsg.set_references(meta_handler.msg.id());
  mxmsg.set_workflow(meta_handler.msg.workflow());
  // A report that would be over MAX_MESSAGE_SIZE, with the original a
  // message near the limit asked for, leaves the original out and says
  // so; one whose workflow alone is too big for it goes without the copy
  // of the workflow too. Never a frame the receiver would refuse.
  if (mxmsg.ByteSizeLong() > MAX_MESSAGE_SIZE && meta_handler.delivery_error_message->has_original_message()) {
    meta_handler.delivery_error_message->clear_original_message();
    meta_handler.delivery_error_message->set_original_message_omitted(true);
    meta_handler.delivery_error_message->SerializeToString(mxmsg.mutable_message());
  }
  if (mxmsg.ByteSizeLong() > MAX_MESSAGE_SIZE) {
    mxmsg.clear_workflow();
  }
  std::shared_ptr<const RawMessage> raw(RawMessage::FromMessage(mxmsg));
  _handle_message(meta_handler.conn, mxmsg, raw);
  meta_handler.delivery_error_message.reset();
}

// Routing carried by the message itself, which wins over the rules file:
// a `to` instance id, or override_rrules. True when the message was
// handled here.
bool Server::_handle_message_inlined_rules(MessageMetaHandler& meta_handler) {
  if (meta_handler.msg.to()) {
    ConnectionById::iterator entry = connection_by_id_.find(meta_handler.msg.to());
    Connection::pointer connection;
    if (entry == connection_by_id_.end() || !(connection = entry->second.lock())) {
      if (meta_handler.msg.report_delivery_error()) {
        meta_handler.failed(meta_handler.msg.to());
      }
      _record(meta_handler, meta_handler.msg.to(), 0, RoutedMessage::NO_RECIPIENT,
              meta_handler.msg.report_delivery_error());
      if (const std::string* line = drops_.first({NOT_CONNECTED, WARNING, 0, meta_handler.msg.to()}, [&] {
            return "message to " + repr(meta_handler.msg.to()) + " which is not connected; dropping";
          })) {
        MX_LOG(WARNING, LogSummary::VERBOSITY, CTX("multiplexer.server") FLOW(meta_handler.msg.workflow()) TEXT(*line));
      }
      return true;
    }
    // A full queue drops the message like an absent peer does, and the
    // sender is told the same way.
    if (connection->schedule(meta_handler.raw)) {
      _record(meta_handler, connection->peer_id(), connection->peer_type(), RoutedMessage::DELIVERED, false);
    } else {
      if (meta_handler.msg.report_delivery_error()) {
        meta_handler.failed(meta_handler.msg.to());
      }
      _record(meta_handler, connection->peer_id(), connection->peer_type(), RoutedMessage::QUEUE_FULL,
              meta_handler.msg.report_delivery_error());
      _dropped_queue_full(meta_handler, *connection);
    }
    return true;
  }

  if (meta_handler.msg.override_rrules().size()) {
    _schedule(meta_handler, meta_handler.msg.override_rrules());
    return true;
  }
  return false;
}

// Protocol messages that reach the multiplexer without a `to` field. Only
// BACKEND_FOR_PACKET_SEARCH does anything: it is forwarded to every peer
// named by the first rule of the searched-for type, so that each live
// backend can answer with a PING and the client can pick the first one.
// A second welcome is a protocol error; the rest is dropped.
bool Server::_handle_meta_message(MessageMetaHandler& meta_handler) {
  if (meta_handler.msg.type() > types::MAX_MULTIPLEXER_META_PACKET) {
    return false;  // this is not a meta message
  }

  switch (meta_handler.msg.type()) {
    case types::CONNECTION_WELCOME:
      MX_LOG(WARNING, HIGHVERBOSITY,
             CTX("multiplexer.server") TEXT("CONNECTION_WELCOME on an established connection; dropping it"));
      meta_handler.conn->shutdown();
      return true;

    case types::PING:
    case types::DELIVERY_ERROR:
    case RECORDING_STATUS:
    case RECORDING_RECORD:
    case RULES_STATUS:
    case PEER_STATUS:
      // Only meaningful with a `to` field, which was handled before this.
      return true;

    case PEER_CONTROL:
      _handle_peer_control(meta_handler);
      return true;

    case RECORDING_CONTROL:
      _handle_recording_control(meta_handler);
      return true;

    case RULES_CONTROL:
      _handle_rules_control(meta_handler);
      return true;

    case types::BACKEND_FOR_PACKET_SEARCH: {
      BackendForPacketSearch search;
      if (!search.ParseFromString(meta_handler.msg.message())) {
        if (const std::string* line =
                drops_.first({BAD_SEARCH, ERROR, 0, 0}, [] { return "garbled BACKEND_FOR_PACKET_SEARCH packet"; })) {
          MX_LOG(ERROR, LogSummary::VERBOSITY, CTX("multiplexer.server") FLOW(meta_handler.msg.workflow()) TEXT(*line));
        }
        meta_handler.unknown();
        return true;
      }
      const MultiplexerMessageDescription* description = config_.message_description(search.packet_type());
      if (!description || !description->to().size()) {
        if (const std::string* line = drops_.first({BAD_SEARCH, ERROR, search.packet_type(), 0}, [&] {
              return "BACKEND_FOR_PACKET_SEARCH: unknown packet type " + repr(search.packet_type()) +
                     " or type with no routing rules";
            })) {
          MX_LOG(ERROR, LogSummary::VERBOSITY, CTX("multiplexer.server") FLOW(meta_handler.msg.workflow()) TEXT(*line));
        }
        meta_handler.unknown();
        return true;
      }

      // The searched-for type's first rule, to every peer it names that
      // takes rule-routed requests (see MessageMetaHandler::search).
      MultiplexerMessageDescription::RoutingRule rule = description->to().Get(0);
      rule.set_whom(MultiplexerMessageDescription::RoutingRule::ALL);
      rule.set_report_delivery_error(true);
      rule.set_include_original_packet_in_report(false);
      meta_handler.search = true;

      if (rule.peer_type() == peers::ALL_TYPES) {
        for (ConnectionsByType::value_type& by_type : connections_by_type_) {
          if (routable(by_type.first)) {
            _schedule(meta_handler, by_type.second, rule, by_type.first);
          }
        }
      } else {
        ConnectionsByType::iterator found = connections_by_type_.find(rule.peer_type());
        ConnectionsList nobody;  // a type no peer has connected as stays out of the index
        _schedule(meta_handler, found != connections_by_type_.end() ? found->second : nobody, rule);
      }
    }
      return true;

    default:
      if (const std::string* line = drops_.first({UNKNOWN_PROTOCOL_TYPE, WARNING, meta_handler.msg.type(), 0}, [&] {
            return "unknown protocol message type " + repr(meta_handler.msg.type()) + "; dropping";
          })) {
        MX_LOG(WARNING, LogSummary::VERBOSITY, CTX("multiplexer.server") TEXT(*line));
      }
      return true;
  }
}

// ---------------------------------------------------------------------------
// Applying rules

unsigned int Server::_schedule(MessageMetaHandler& meta_handler, const MultiplexerMessageDescription& desc) {
  return _schedule(meta_handler, desc.to());
}

unsigned int Server::_schedule(
    MessageMetaHandler& meta_handler,
    const ::google::protobuf::RepeatedPtrField<MultiplexerMessageDescription::RoutingRule>& rules) {
  unsigned int scheduled = 0;
  for (const MultiplexerMessageDescription::RoutingRule& rule : rules) {
    scheduled += _schedule(meta_handler, rule);
  }
  return scheduled;
}

unsigned int Server::_schedule(MessageMetaHandler& meta_handler,
                               const MultiplexerMessageDescription::RoutingRule& rule) {
  unsigned int scheduled = 0;
  if (rule.peer_type() == peers::ALL_TYPES) {
    for (ConnectionsByType::value_type& by_type : connections_by_type_) {
      if (routable(by_type.first)) {
        scheduled += _schedule(meta_handler, by_type.second, rule, by_type.first);
      }
    }
  } else {
    // A rule, or a peer's override_rrules, may name any type: looking it up
    // must not add it to the index, which every ALL_TYPES rule and search
    // walks, and which a peer naming ever new types grew without bound.
    ConnectionsByType::iterator found = connections_by_type_.find(rule.peer_type());
    ConnectionsList nobody;
    scheduled += _schedule(meta_handler, found != connections_by_type_.end() ? found->second : nobody, rule);
  }

  if (!scheduled) {
    const unsigned int level = rule.delivery_error_is_error() ? ERROR : WARNING;
    const Unrouted why = _unrouted(rule, rule.whom() == MultiplexerMessageDescription::RoutingRule::ANY);
    if (rule.peer_type() == peers::ALL_TYPES && why == NONE_PRESENT) {
      return 0;  // nobody connected to tell: no failure for ALL_TYPES (see below)
    }
    if (const std::string* line =
            drops_.first({why, level, rule.peer_type(), 0}, [&] { return _unrouted_text(why, rule.peer_type()); })) {
      MX_LOG(level, LogSummary::VERBOSITY, CTX("multiplexer.server") FLOW(meta_handler.msg.workflow()) TEXT(*line));
    }
  }
  return scheduled;
}

Server::Unrouted Server::_unrouted(const MultiplexerMessageDescription::RoutingRule& rule, bool by_any) const {
  bool present = false;
  bool takes = false;
  const auto look_at = [&](const ConnectionsList& connections) {
    for (const auto& weak : connections) {
      if (Connection::pointer connection = weak.lock()) {
        if (connection->living()) {
          present = true;
          takes =
              takes || (by_any ? connection->accepts_any() : connection->accepts_all()) || connection->last_resort();
        }
      }
    }
  };
  if (rule.peer_type() == peers::ALL_TYPES) {
    for (const ConnectionsByType::value_type& by_type : connections_by_type_) {
      if (routable(by_type.first)) {
        look_at(by_type.second);
      }
    }
  } else {
    ConnectionsByType::const_iterator found = connections_by_type_.find(rule.peer_type());
    if (found != connections_by_type_.end()) {
      look_at(found->second);
    }
  }
  if (!present) {
    return NONE_PRESENT;
  }
  return takes ? ALL_FULL : ROUTING_OFF;
}

std::string Server::_unrouted_text(Unrouted why, std::uint32_t peer_type) const {
  const std::string type = repr(peer_type) + " (" + config_.peer_name_by_type(peer_type) + ")";
  switch (why) {
    case NONE_PRESENT:
      return "routing while none present of type " + type;
    case ROUTING_OFF:
      return "routing off on every peer of type " + type;
    case ALL_FULL:
      break;
  }
  return "queue full on every peer of type " + type + " that takes it";
}

void Server::_dropped_queue_full(const MessageMetaHandler& meta_handler, const Connection& connection) {
  if (const std::string* line = drops_.first({QUEUE_FULL, WARNING, connection.peer_type(), connection.peer_id()}, [&] {
        return "outgoing queue full, dropping message to peer " + repr(connection.peer_id()) + " of type " +
               repr(connection.peer_type()) + " (" + _peer_name(connection.peer_type()) + ")";
      })) {
    MX_LOG(WARNING, LogSummary::VERBOSITY, CTX("multiplexer.server") FLOW(meta_handler.msg.workflow()) TEXT(*line));
  }
}

unsigned int Server::_schedule(MessageMetaHandler& meta_handler, ConnectionsList& connections,
                               const MultiplexerMessageDescription::RoutingRule& rule) {
  return _schedule(meta_handler, connections, rule, rule.peer_type());
}

unsigned int Server::_schedule(MessageMetaHandler& meta_handler, ConnectionsList& connections,
                               const MultiplexerMessageDescription::RoutingRule& rule, std::uint32_t peer_type) {
  unsigned int scheduled = (unsigned int)-1;
  switch (rule.whom()) {
    case MultiplexerMessageDescription::RoutingRule::ALL:
      scheduled = send_to_all(meta_handler, connections);
      break;
    case MultiplexerMessageDescription::RoutingRule::ANY:
      scheduled = send_to_one(meta_handler, connections);
      break;
  }
  AssertMsg(scheduled != (unsigned int)-1, "unhandled Whom type " + mx::repr(rule.whom()));

  // A rule for ALL_TYPES reaches whoever is connected, a notice such as a
  // shutdown's: a type with nobody to take it is no failure, so it is
  // neither reported nor recorded, whatever the rule's flag says.
  if (!scheduled && rule.peer_type() != peers::ALL_TYPES) {
    if (rule.report_delivery_error()) {
      meta_handler.failed(rule, peer_type);
    }
    // One record for the rule, no recipient, saying why it queued the
    // message nowhere, as its log line does (_unrouted), and whether the
    // sender was told; whom: ALL recorded each peer it passed over besides.
    const bool by_any = meta_handler.search || rule.whom() == MultiplexerMessageDescription::RoutingRule::ANY;
    _record(meta_handler, 0, peer_type, _unrouted_disposition(_unrouted(rule, by_any)), rule.report_delivery_error());
  }
  return scheduled;
}

RoutedMessage::Disposition Server::_unrouted_disposition(Unrouted why) {
  switch (why) {
    case NONE_PRESENT:
      return RoutedMessage::NO_RECIPIENT;
    case ROUTING_OFF:
      return RoutedMessage::NOT_ACCEPTED;
    case ALL_FULL:
      break;
  }
  return RoutedMessage::QUEUE_FULL;
}

// whom: ALL. Every live connection of the type that takes the path gets
// the frame (Routing.all, or Routing.any for a search); one whose queue is
// full drops it, said by _dropped_queue_full, and is not counted. The
// peers that take nothing by the path are skipped, and recorded as such,
// unless no peer takes it: then the last resorts among them get it.
unsigned int Server::send_to_all(MessageMetaHandler& meta_handler, ConnectionsList& connections) {
  const bool by_any = meta_handler.search;
  bool somebody_takes = false;
  for (ConnectionsList::iterator current = connections.begin(); current != connections.end(); ++current) {
    if (Connection::pointer connection = current->lock()) {
      if (connection->living() && (by_any ? connection->accepts_any() : connection->accepts_all())) {
        somebody_takes = true;
        break;
      }
    }
  }
  unsigned int scheduled = 0;
  for (ConnectionsList::iterator current, next = connections.begin();
       next != connections.end() && (current = next++, true);) {
    if (Connection::pointer connection = current->lock()) {
      if (!connection->living()) {
        continue;
      }
      const bool takes = by_any ? connection->accepts_any() : connection->accepts_all();
      if (!takes && (somebody_takes || !connection->last_resort())) {
        _record(meta_handler, connection->peer_id(), connection->peer_type(), RoutedMessage::NOT_ACCEPTED, false);
        continue;
      }
      if (connection->schedule(meta_handler.raw)) {
        ++scheduled;
        _record(meta_handler, connection->peer_id(), connection->peer_type(), RoutedMessage::DELIVERED, false);
      } else {
        _record(meta_handler, connection->peer_id(), connection->peer_type(), RoutedMessage::QUEUE_FULL, false);
        _dropped_queue_full(meta_handler, *connection);
      }
    } else {
      connections.erase(current);  // dead connection
    }
  }
  return scheduled;
}

// whom: ANY. The first live connection with room that takes the path
// (Routing.any), starting from the front of the type's list, then moved
// to the back: round robin that skips busy peers and the ones taking no
// new requests. When there is none, the same over the last resorts.
unsigned int Server::send_to_one(MessageMetaHandler& meta_handler, ConnectionsList& connections) {
  for (int pass = 0; pass < 2; ++pass) {
    const bool last_resort = pass == 1;
    Connection::pointer connection;
    for (ConnectionsList::iterator current = connections.begin();
         (current = choose_free_connections(connections, current, last_resort)) != connections.end(); ++current) {
      if (!(connection = current->lock())) {
        continue;
      }
      if (!connection->schedule(meta_handler.raw)) {
        _record(meta_handler, connection->peer_id(), connection->peer_type(), RoutedMessage::QUEUE_FULL, false);
        continue;
      }
      _record(meta_handler, connection->peer_id(), connection->peer_type(), RoutedMessage::DELIVERED, false);
      connections.splice(connections.end(), connections, current);
      return 1;
    }
  }
  return 0;
}

// The peers file: written whole to a temporary name, then renamed, so a
// reader never sees a partial file. One line per registered peer.
void Server::_peers_changed() {
  if (peers_file_.empty() || peers_write_pending_) {
    return;
  }
  peers_write_pending_ = true;
  const std::chrono::steady_clock::duration since = std::chrono::steady_clock::now() - peers_written_at_;
  peers_timer_.expires_after(since >= PEERS_FILE_INTERVAL ? std::chrono::steady_clock::duration::zero()
                                                          : PEERS_FILE_INTERVAL - since);
  peers_timer_.async_wait([weak = weak_pointer(shared_from_this())](const asio::error_code& error) {
    pointer self = weak.lock();
    if (!error && self && self->peers_write_pending_) {
      self->_write_peers_file();
    }
  });
}

void Server::_write_peers_file() {
  peers_write_pending_ = false;
  peers_written_at_ = std::chrono::steady_clock::now();
  if (peers_file_.empty()) {
    return;
  }
  const std::string tmp = peers_file_ + ".tmp";
  unsigned int written = 0;
  {
    std::ofstream out(tmp.c_str(), std::ios::out | std::ios::trunc);
    if (!out.good()) {
      MX_LOG(ERROR, LOWVERBOSITY, CTX("multiplexer.server") TEXT("cannot write peers file " + tmp));
      return;
    }
    for (ConnectionById::const_iterator entry = connection_by_id_.begin(); entry != connection_by_id_.end(); ++entry) {
      Connection::pointer connection = entry->second.lock();
      if (!connection || !connection->living()) {
        continue;
      }
      out << connection->peer_id() << " " << _peer_name(connection->peer_type()) << " " << connection->peer_type()
          << "\n";
      ++written;
    }
  }
  if (std::rename(tmp.c_str(), peers_file_.c_str()) != 0) {
    MX_LOG(ERROR, LOWVERBOSITY, CTX("multiplexer.server") TEXT("cannot rename peers file to " + peers_file_));
    return;
  }
  MX_LOG(DEBUG, HIGHVERBOSITY, CTX("multiplexer.server") TEXT("peers file written: " + repr(written) + " peer(s)"));
}

std::string Server::_peer_name(std::uint32_t peer_type) const {
  if (peer_type == RECORDING_CONTROLLER) {
    return "RECORDING_CONTROLLER";
  }
  if (peer_type == RULES_CONTROLLER) {
    return "RULES_CONTROLLER";
  }
  return config_.peer_name_by_type(peer_type);
}

// The rules file.

Server::RulesLoad Server::load_rules(std::string* error) {
  MX_DCHECK_RUN_ON(&owner_thread());
  std::string text;
  if (!_read_rules_file(&text, error)) {
    return _rules_failed(*error, error);
  }
  return _apply_rules_text(text, error);
}

bool Server::_read_rules_file(std::string* text, std::string* error) {
  if (rules_file_.empty()) {
    *error = "no rules file was named";
    return false;
  }
  std::ifstream in(rules_file_.c_str(), std::ios::in | std::ios::binary);
  if (!in) {
    *error = "cannot read " + rules_file_;
    return false;
  }
  text->assign((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
  if (text->empty()) {
    *error = "empty rules file " + rules_file_;
    return false;
  }
  return true;
}

Server::RulesLoad Server::_apply_rules_text(const std::string& text, std::string* error) {
  const std::string fingerprint = mx::fingerprint(text);
  if (config_.initialized() && fingerprint == rules_fingerprint_) {
    // The file on disk is the rules in use again, if it ever was not.
    rules_last_error_.clear();
    rules_failed_fingerprint_.clear();
    return RulesLoad::UNCHANGED;
  }
  if (fingerprint == rules_failed_fingerprint_) {
    *error = rules_last_error_;  // the same bytes that failed last time: said already
    return RulesLoad::FAILED;
  }
  // Parsed apart from the rules in use, which nothing touches unless the
  // whole file is good.
  Config fresh;
  try {
    fresh.read_configuration_text(text, rules_file_);
  } catch (const mx::Exception& exception) {
    rules_failed_fingerprint_ = fingerprint;
    return _rules_failed(std::string(exception.what()) + " (content " + fingerprint + ")", error);
  }
  const std::string previous = rules_fingerprint_;
  config_ = std::move(fresh);
  rules_fingerprint_ = fingerprint;
  rules_loaded_us_ = recording::now_us();
  rules_last_error_.clear();
  rules_failed_fingerprint_.clear();

  // The connected peers follow the new file: a type's passive flag and
  // queue size are applied again, a controller kept passive as when it
  // registered. A peer whose type the file no longer names stays
  // connected, since dropping it would turn an edit into an outage; the
  // log counts them.
  unsigned int kept = 0;
  for (ConnectionById::const_iterator entry = connection_by_id_.begin(); entry != connection_by_id_.end(); ++entry) {
    Connection::pointer connection = entry->second.lock();
    if (!connection || !connection->living()) {
      continue;
    }
    const MultiplexerPeerDescription* peer = config_.peer_description(connection->peer_type());
    if (peer) {
      connection->set_is_passive(peer->is_passive() || controller(connection->peer_type()));
      connection->set_outgoing_queue_max_size(peer->queue_size());
    } else if (connection->peer_type() > peers::MAX_MULTIPLEXER_SPECIAL_PEER_TYPE) {
      ++kept;
    }
  }
  std::string line = (previous.empty() ? "rules loaded from " : "rules reloaded from ") + rules_file_ + ": " +
                     (previous.empty() ? "" : previous + " -> ") + fingerprint + ", " +
                     repr(config_.message_description_by_id().size()) + " message types, " +
                     repr(config_.peer_by_type().size()) + " peer types";
  if (kept) {
    line += "; " + repr(kept) + " connected peer(s) of types the file no longer names, kept";
  }
  MX_LOG(INFO, LOWVERBOSITY, CTX("multiplexer.server") TEXT(line));
  if (recorder_ || !taps_.empty()) {
    // A recording that spans the change says so, since its header names
    // the fingerprint the file had when the session began.
    Record record;
    recording::fill_rules(record, fingerprint, rules_file_, config_.message_description_by_id().size(),
                          config_.peer_by_type().size());
    _emit(record);
  }
  _peers_changed();  // the peers file names each peer's type as these rules do
  return RulesLoad::LOADED;
}

Server::RulesLoad Server::_rules_failed(const std::string& why, std::string* error) {
  *error = why;
  if (why != rules_last_error_) {
    rules_last_error_ = why;
    MX_LOG(ERROR, LOWVERBOSITY,
           CTX("multiplexer.server")
               TEXT("rules file not put in use: " + why +
                    (rules_fingerprint_.empty() ? "" : "; keeping the rules in use (" + rules_fingerprint_ + ")")));
  }
  return RulesLoad::FAILED;
}

void Server::set_rules_check_interval(float seconds) {
  const std::string refused = rules_check_interval_refused(seconds);
  if (!refused.empty()) {
    throw std::invalid_argument(refused);
  }
  rules_check_interval_ = seconds;
}

// Below the floor, mx::from_seconds truncating what is under a
// microsecond to 0, the timer would expire as soon as it is armed and the
// check run again and again, the io thread doing nothing else.
std::string Server::rules_check_interval_refused(float seconds) {
  if (seconds > 0 && seconds < MIN_RULES_CHECK_INTERVAL) {
    return repr(seconds) + " s is below the shortest rules check interval, " + repr(MIN_RULES_CHECK_INTERVAL) +
           " s; 0 turns the checks off";
  }
  return "";
}

// No check for 0, a negative interval or NaN: off.
void Server::_arm_rules_check() {
  if (!(rules_check_interval_ > 0) || rules_file_.empty()) {
    return;
  }
  rules_timer_.expires_after(mx::from_seconds(rules_check_interval_));
  rules_timer_.async_wait(
      [weak = weak_pointer(shared_from_this())](const asio::error_code& error) { _on_rules_check(weak, error); });
}

void Server::_on_rules_check(weak_pointer server, const asio::error_code& error) {
  if (error) {
    return;  // cancelled: stop()
  }
  pointer self = server.lock();
  if (!self || self->stopped()) {
    return;  // stop() came after the timer had expired: do not re-arm
  }
  self->_check_rules_file();
  self->_arm_rules_check();
}

// The timer's check. A file that differs from the rules in use is put in
// use once two checks in a row have read the same new bytes: a file caught
// between a truncate and its write, or half written, is never applied,
// at the price of one more interval. Failures are logged when they are news.
void Server::_check_rules_file() {
  std::string text;
  std::string error;
  if (!_read_rules_file(&text, &error)) {
    rules_pending_fingerprint_.clear();
    _rules_failed(error, &error);
    return;
  }
  const std::string fingerprint = mx::fingerprint(text);
  if (config_.initialized() && fingerprint == rules_fingerprint_) {
    rules_pending_fingerprint_.clear();
    rules_last_error_.clear();
    rules_failed_fingerprint_.clear();
    return;
  }
  if (fingerprint != rules_pending_fingerprint_) {
    rules_pending_fingerprint_ = fingerprint;  // seen once; put in use when seen again
    return;
  }
  rules_pending_fingerprint_.clear();
  _apply_rules_text(text, &error);
}

void Server::_handle_rules_control(MessageMetaHandler& meta_handler) {
  RulesControl control;
  RulesStatus status;
  if (!control.ParseFromString(meta_handler.msg.message())) {
    status.set_error("garbled RulesControl");
  } else if (control.action() == RulesControl::RELOAD) {
    std::string error;
    switch (load_rules(&error)) {
      case RulesLoad::LOADED:
        MX_LOG(INFO, LOWVERBOSITY,
               CTX("multiplexer.server")
                   TEXT("rules reloaded at the request of peer " + repr(meta_handler.msg.sender())));
        status.set_reloaded(true);
        break;
      case RulesLoad::UNCHANGED:
        status.set_reloaded(false);
        break;
      case RulesLoad::FAILED:
        status.set_error(error);
        break;
    }
  }
  _fill_rules_status(status);
  _reply(meta_handler, RULES_STATUS, status);
}

void Server::_handle_peer_control(MessageMetaHandler& meta_handler) {
  Connection::pointer conn = meta_handler.conn;
  PeerControl control;
  PeerStatus status;
  if (!control.ParseFromString(meta_handler.msg.message())) {
    status.set_error("garbled PeerControl");
  } else if (!same_routing(conn->routing(), control.routing())) {
    conn->set_routing(control.routing());
    MX_LOG(INFO, LOWVERBOSITY,
           CTX("multiplexer.server") TEXT("peer " + repr(conn->peer_id()) + " (" + _peer_name(conn->peer_type()) +
                                          ") takes " + routing_text(conn->routing())));
    _emit_peer_routing(*conn);
  }
  status.set_multiplexer_id(instance_id_);
  *status.mutable_routing() = conn->routing();
  _reply(meta_handler, PEER_STATUS, status);
}

void Server::_fill_rules_status(RulesStatus& status) {
  status.set_multiplexer_id(instance_id_);
  status.set_path(rules_file_);
  status.set_fingerprint(rules_fingerprint_);
  status.set_loaded_us(rules_loaded_us_);
  status.set_message_types(config_.message_description_by_id().size());
  status.set_peer_types(config_.peer_by_type().size());
  if (!rules_last_error_.empty()) {
    status.set_last_error(rules_last_error_);
  }
}

// Recording.

bool Server::start_recording(const std::string& path, const std::string& label, unsigned int payload_limit,
                             std::uint64_t max_bytes, unsigned int max_seconds, std::string* error) {
  MX_DCHECK_RUN_ON(&owner_thread());
  if (recorder_) {
    *error = "already recording " + recorder_->path();
    return false;
  }
  std::unique_ptr<Recorder> recorder(new Recorder(path, payload_limit));
  if (!recorder->ok()) {
    *error = "cannot open " + path;
    return false;
  }
  recorder->header(instance_id_, rules_fingerprint_, label);
  // The peers connected right now, so that the file stands on its own.
  const std::uint64_t now = recording::now_us();
  for (ConnectionById::const_iterator entry = connection_by_id_.begin(); entry != connection_by_id_.end(); ++entry) {
    Connection::pointer connection = entry->second.lock();
    if (!connection || !connection->living()) {
      continue;
    }
    Record record;
    recording::fill_peer(record, PeerEvent::CONNECTED, connection->peer_id(), connection->peer_type());
    record.set_timestamp_us(now);
    recorder->write(record);
    if (restricted(connection->routing())) {
      Record routing;
      recording::fill_peer_routing(routing, connection->peer_id(), connection->peer_type(), connection->routing());
      routing.set_timestamp_us(now);
      recorder->write(routing);
    }
  }
  recorder_ = std::move(recorder);
  session_ = Session();
  session_.label = label;
  session_.path = path;
  session_.started_us = now;
  session_.max_bytes = max_bytes;
  const std::uint64_t session = ++sessions_;
  if (max_seconds) {
    session_timer_.expires_after(std::chrono::seconds(max_seconds));
    session_timer_.async_wait([weak = weak_pointer(shared_from_this()), session](const asio::error_code& error) {
      _on_session_deadline(weak, session, error);
    });
  }
  _arm_recording_flush();
  MX_LOG(INFO, LOWVERBOSITY, CTX("multiplexer.server") TEXT("recording to " + path));
  return true;
}

void Server::stop_recording(const std::string& reason) {
  MX_DCHECK_RUN_ON(&owner_thread());
  if (!recorder_) {
    return;
  }
  session_.bytes = recorder_->bytes();
  session_.records = recorder_->records();
  session_.stopped = reason;
  recorder_.reset();
  session_timer_.cancel();
  flush_timer_.cancel();
  MX_LOG(INFO, LOWVERBOSITY,
         CTX("multiplexer.server") TEXT("recording to " + session_.path + " closed: " + reason + " (" +
                                        repr(session_.records) + " records, " + repr(session_.bytes) + " bytes)"));
}

void Server::_on_session_deadline(weak_pointer server, std::uint64_t session, const asio::error_code& error) {
  if (error) {
    return;  // cancelled: the session ended first
  }
  // The session may have ended, and another begun, between the expiry and
  // this call, the cancel then too late to recall it.
  pointer self = server.lock();
  if (self && self->sessions_ == session) {
    self->stop_recording("max_seconds reached");
  }
}

// The next tick, RECORDING_FLUSH_INTERVAL from now; a wait still pending
// is cancelled, so only one ever is.
void Server::_arm_recording_flush() {
  flush_timer_.expires_after(RECORDING_FLUSH_INTERVAL);
  flush_timer_.async_wait(
      [weak = weak_pointer(shared_from_this())](const asio::error_code& error) { _on_recording_flush(weak, error); });
}

// A tick: the buffer to the file, then the next tick. A flush that fails
// ends the session, as a record's write that fails does.
void Server::_on_recording_flush(weak_pointer server, const asio::error_code& error) {
  if (error) {
    return;  // cancelled: the session ended
  }
  pointer self = server.lock();
  if (!self || !self->recorder_) {
    return;  // the session ended after the timer had expired: no next tick
  }
  MX_DCHECK_RUN_ON(&self->owner_thread());
  self->recorder_->flush();
  if (!self->recorder_->ok()) {
    self->stop_recording("write failed");
    return;
  }
  self->_arm_recording_flush();
}

void Server::_emit_peer(PeerEvent::Kind kind, std::uint64_t peer_id, std::uint32_t peer_type) {
  if (!recorder_ && taps_.empty()) {
    return;
  }
  Record record;
  recording::fill_peer(record, kind, peer_id, peer_type);
  _emit(record);
}

void Server::_emit_peer_routing(const Connection& conn) {
  if (!recorder_ && taps_.empty()) {
    return;
  }
  Record record;
  recording::fill_peer_routing(record, conn.peer_id(), conn.peer_type(), conn.routing());
  _emit(record);
}

unsigned int Server::_payload_kept() const {
  unsigned int most = 0;
  if (recorder_) {
    if (!recorder_->payload_limit()) {
      return 0;
    }
    most = recorder_->payload_limit();
  }
  for (const Tap& tap : taps_) {
    if (!tap.payload_limit) {
      return 0;
    }
    most = std::max(most, tap.payload_limit);
  }
  return most;
}

// Every sink is given the length of the payload it keeps: its limit, or
// the whole payload for 0 or a limit the payload is within, and a tap no
// more than its frame can carry. The distinct lengths are served longest
// first, the payload cut in place from one to the next: the file session
// at its length, by its stream, and the taps at each length from one
// serialization. Only a tap's record carries the multiplexer's id.
void Server::_emit(Record& record) {
  record.set_timestamp_us(recording::now_us());
  for (Taps::size_type index = 0; index < taps_.size();) {
    Connection::pointer connection = taps_[index].conn.lock();
    if (connection && connection->living()) {
      ++index;
    } else {
      taps_.erase(taps_.begin() + index);  // its peer is gone
    }
  }
  const std::size_t payload = record.has_routed() ? record.routed().payload().size() : 0;
  const auto kept = [payload](unsigned int limit) -> std::size_t { return limit && limit < payload ? limit : payload; };
  std::vector<std::size_t> lengths;
  const std::size_t session_length = recorder_ ? kept(recorder_->payload_limit()) : 0;
  if (recorder_) {
    lengths.push_back(session_length);
  }
  std::vector<std::size_t> tap_lengths;
  for (const Tap& tap : taps_) {
    tap_lengths.push_back(kept(tap.payload_limit));
    lengths.push_back(tap_lengths.back());
  }
  std::sort(lengths.begin(), lengths.end(), std::greater<std::size_t>());
  lengths.erase(std::unique(lengths.begin(), lengths.end()), lengths.end());
  for (const std::size_t length : lengths) {
    recording::cut_payload(record, length);
    if (recorder_ && session_length == length) {
      record.clear_multiplexer_id();
      recorder_->write(record);
      if (!recorder_->ok()) {
        stop_recording("write failed");
      } else if (session_.max_bytes && recorder_->bytes() >= session_.max_bytes) {
        stop_recording("max_bytes reached");
      }
    }
    std::string serialized;  // the taps' record at this length, made for the first of them
    for (Taps::size_type index = 0; index < taps_.size(); ++index) {
      if (tap_lengths[index] != length) {
        continue;
      }
      if (serialized.empty()) {
        serialized = _tap_record(record);
      }
      _stream(taps_[index], serialized);
    }
  }
}

// The record of a message near MAX_MESSAGE_SIZE would be over it in a
// frame: its payload is cut to fit, and marked truncated, as a tap's own
// limit cuts it. The frame is measured, never reckoned: the record is
// serialized into the frame a tap gets, its id and addressee at their
// largest, and protobuf says how big that is. One over the limit is cut,
// a copy of it, so that a sink after the taps still has the record as it
// was, by what it is over, and measured again, the truncated mark and the
// shorter lengths changing the frame too, until it fits or has no payload
// left; one that cannot fit even so, its workflow too big, _stream drops.
std::string Server::_tap_record(Record& record) {
  record.set_multiplexer_id(instance_id_);
  MultiplexerMessage frame = _tap_frame(std::numeric_limits<std::uint64_t>::max());
  frame.set_id(std::numeric_limits<std::uint64_t>::max());
  record.SerializeToString(frame.mutable_message());
  if (frame.ByteSizeLong() <= MAX_MESSAGE_SIZE || !record.has_routed()) {
    return std::move(*frame.mutable_message());
  }
  Record cut(record);
  for (;;) {
    const std::size_t framed = frame.ByteSizeLong();
    const std::size_t payload = cut.routed().payload().size();
    if (framed <= MAX_MESSAGE_SIZE || !payload) {
      return std::move(*frame.mutable_message());
    }
    const std::size_t over = framed - MAX_MESSAGE_SIZE;
    recording::cut_payload(cut, payload > over ? payload - over : 0);
    cut.SerializeToString(frame.mutable_message());
  }
}

// The frame a tap's record goes out in, to `peer_id`, its message to be
// set: a RECORDING_RECORD from this multiplexer, with an id of its own.
MultiplexerMessage Server::_tap_frame(std::uint64_t peer_id) {
  MultiplexerMessage frame;
  frame.set_id(random_());
  frame.set_sender(instance_id_);
  frame.set_to(peer_id);
  frame.set_type(RECORDING_RECORD);
  return frame;
}

// One that cannot fit even with its payload cut, its workflow too big, is
// dropped, as one its queue refuses is.
void Server::_stream(Tap& tap, const std::string& serialized) {
  MultiplexerMessage mxmsg = _tap_frame(tap.peer_id);
  mxmsg.set_message(serialized);
  Connection::pointer connection = tap.conn.lock();
  if (!connection || mxmsg.ByteSizeLong() > MAX_MESSAGE_SIZE) {
    tap.dropped += 1;
    return;
  }
  std::shared_ptr<const RawMessage> raw(RawMessage::FromMessage(mxmsg));
  if (!connection->schedule(raw)) {
    tap.dropped += 1;
  }
}

Server::Taps::iterator Server::_find_tap(const Connection* conn) {
  for (Taps::iterator tap = taps_.begin(); tap != taps_.end(); ++tap) {
    if (tap->conn.lock().get() == conn) {
      return tap;
    }
  }
  return taps_.end();
}

void Server::_untap(const Connection* conn) {
  Taps::iterator tap = _find_tap(conn);
  if (tap != taps_.end()) {
    taps_.erase(tap);
  }
}

void Server::_handle_recording_control(MessageMetaHandler& meta_handler) {
  const MultiplexerMessage& msg = meta_handler.msg;
  Connection::pointer conn = meta_handler.conn;
  RecordingControl control;
  RecordingStatus status;
  if (!control.ParseFromString(msg.message())) {
    status.set_error("garbled RecordingControl");
  } else {
    switch (control.action()) {
      case RecordingControl::START: {
        std::string error;
        if (recording_dir_.empty()) {
          status.set_error("remote recording is off; start the multiplexer with --recording-dir");
        } else if (!recording::valid_label(control.label())) {
          status.set_error("label must be 1 to 64 letters, digits, '-' or '_'");
        } else {
          const std::uint64_t max_bytes =
              control.has_max_bytes() ? control.max_bytes() : DEFAULT_REMOTE_RECORDING_MAX_BYTES;
          const std::string path =
              recording::session_path(recording_dir_, control.label(), instance_id_, recording::now_us());
          if (!start_recording(path, control.label(), control.payload_limit(), max_bytes, control.max_seconds(),
                               &error)) {
            status.set_error(error);
          } else {
            MX_LOG(INFO, LOWVERBOSITY,
                   CTX("multiplexer.server") TEXT("recording started by peer " + repr(msg.sender())));
          }
        }
        break;
      }
      case RecordingControl::STOP:
        stop_recording("stopped by peer " + repr(msg.sender()));
        break;
      case RecordingControl::STATUS:
        break;
      case RecordingControl::TAP:
        if (!allow_tap_) {
          status.set_error("taps are off; start the multiplexer with --allow-tap");
        } else if (_find_tap(conn.get()) == taps_.end()) {
          Tap tap;
          tap.conn = conn;
          tap.peer_id = conn->peer_id();
          tap.payload_limit = control.payload_limit();
          tap.dropped = 0;
          taps_.push_back(tap);
          MX_LOG(INFO, LOWVERBOSITY,
                 CTX("multiplexer.server") TEXT("peer " + repr(msg.sender()) + " taps the recording"));
        }
        break;
      case RecordingControl::UNTAP:
        _untap(conn.get());
        break;
    }
  }
  _fill_status(status, conn.get());
  _reply(meta_handler, RECORDING_STATUS, status);
}

void Server::_fill_status(RecordingStatus& status, const Connection* requester) {
  status.set_multiplexer_id(instance_id_);
  status.set_recording(recorder_ != nullptr);
  if (recorder_ || !session_.path.empty()) {
    status.set_path(session_.path);
    if (!session_.label.empty()) {
      status.set_label(session_.label);
    }
    status.set_started_us(session_.started_us);
    status.set_bytes(recorder_ ? recorder_->bytes() : session_.bytes);
    status.set_records(recorder_ ? recorder_->records() : session_.records);
    if (!session_.stopped.empty()) {
      status.set_stopped(session_.stopped);
    }
  }
  status.set_taps(taps_.size());
  Taps::iterator tap = _find_tap(requester);
  if (tap != taps_.end()) {
    status.set_tapping(true);
    status.set_dropped(tap->dropped);
  }
}

void Server::_reply(const MessageMetaHandler& meta_handler, std::uint32_t type,
                    const ::google::protobuf::Message& payload) {
  MultiplexerMessage mxmsg;
  mxmsg.set_id(random_());
  mxmsg.set_sender(instance_id_);
  mxmsg.set_to(meta_handler.msg.sender());
  mxmsg.set_type(type);
  mxmsg.set_references(meta_handler.msg.id());
  mxmsg.set_workflow(meta_handler.msg.workflow());
  payload.SerializeToString(mxmsg.mutable_message());
  std::shared_ptr<const RawMessage> raw(RawMessage::FromMessage(mxmsg));
  // Forced past a full queue: the one answer to one frame received, which
  // a peer waits for; pushed at the back, behind everything routed to the
  // peer before it, as PEER_STATUS promises. A peer that sends control
  // frames faster than it reads loses the answers past the forced frames'
  // room (FORCED_FRAMES_PAST_FULL_QUEUE), logged, rather than growing its
  // queue for good.
  if (!meta_handler.conn->schedule(raw, /*force=*/true)) {
    _dropped_queue_full(meta_handler, *meta_handler.conn);
  }
}

}  // namespace multiplexer
