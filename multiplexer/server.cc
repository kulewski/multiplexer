// Server: construction, accepting, and the routing path. The design is in
// server.h; this file is the whole per-message path, kept in one
// translation unit so it inlines the way it did as header code.
#include "multiplexer/server.h"

#include <vector>

#include <boost/asio/placeholders.hpp>
#include <boost/bind/bind.hpp>
#include <boost/foreach.hpp>
#include <boost/lexical_cast.hpp>
#include <cstdio>
#include <fstream>

#include "lib/logging/logging.h"
#include "lib/memory.h"
#include "lib/repr.h"
#include "multiplexer/mxlog/type_id_constants.h" /* generated */

namespace multiplexer {

using mx::repr;

// `host` must be an IP address; names are not resolved. Port 0 lets the
// system choose, see local_port().
Server::Server(boost::asio::io_service &io_service, const std::string &host, unsigned short port)
    : Base(io_service),
      acceptor_(io_service, boost::asio::ip::tcp::endpoint(boost::asio::ip::address::from_string(host), port)),
      io_service_(io_service) {}

void Server::start() { _start_accept(); }

void Server::stop() {
  MX_DCHECK_RUN_ON(&owner_thread());
  boost::system::error_code ignored;
  acceptor_.close(ignored);
  std::vector<Connection::pointer> live;
  for (ConnectionById::iterator entry = connection_by_id_.begin(); entry != connection_by_id_.end(); ++entry)
    if (Connection::pointer connection = entry->second.lock())
      live.push_back(connection);
  for (size_t index = 0; index < live.size(); ++index)
    live[index]->shutdown();
}

void Server::_start_accept() {
  Connection::pointer new_connection = Connection::Create(io_service_, this->shared_from_this());
  acceptor_.async_accept(new_connection->socket(),
                         boost::bind(&Server::_handle_accept, this, new_connection, boost::asio::placeholders::error));
}

void Server::_handle_accept(Connection::pointer new_connection, const boost::system::error_code &error) {
  MX_DCHECK_RUN_ON(&owner_thread());
  if (!acceptor_.is_open()) {
    // stop() closed the acceptor: do not re-arm, just drop the connection
    new_connection->shutdown();
    return;
  }
  _start_accept();
  if (!error) {
    // reading only, until the peer has introduced itself with
    // CONNECTION_WELCOME; routed traffic before that closes the connection
    new_connection->start_only_read();
  } else {
    // the connection is dropped, or -- in fact -- has never been established
    new_connection->shutdown();
  }
}

void Server::handle_message(Connection::pointer conn, boost::shared_ptr<const RawMessage> raw,
                            boost::shared_ptr<MultiplexerMessage> msg) {
  MX_DCHECK_RUN_ON(&owner_thread());
  if (msg->from() == instance_id_) {
    MX_LOG(ERROR, MEDIUMVERBOSITY,
           CTX("multiplexer.server") TEXT("received message from self") FLOW(msg->workflow())
               SKIPFILEIF(!(msg->logging_method() & multiplexer::LoggingMethod::FILE)));
    return;
  }
  return _handle_message(conn, *msg, raw);
}

// ---------------------------------------------------------------------------
// MessageMetaHandler

void Server::MessageMetaHandler::failed(const MultiplexerMessageDescription::RoutingRule &rule, boost::uint32_t type) {
  __create_delivery_error_message(rule.include_original_packet_in_report());
  delivery_error_message->add_failed_type(type);
}

void Server::MessageMetaHandler::failed(const boost::uint64_t to) {
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
  if (delivery_error_message)
    return;
  delivery_error_message.reset(new DeliveryError());
  delivery_error_message->set_packet_id(this->msg.id());
  if (include_original_packet_in_report) {
    *delivery_error_message->mutable_original_message() = this->msg;
  }
}

// ---------------------------------------------------------------------------
// The routing path

void Server::_handle_message(Connection::pointer conn, const MultiplexerMessage &msg,
                             boost::shared_ptr<const RawMessage> raw) {
  MX_ENTER(VERBOSITY(HIGHVERBOSITY) DEFAULT);

  MX_LOG(DEBUG, HIGHVERBOSITY,
         CTX("multiplexer.server") FLOW(msg.workflow())
             TEXT("handle_message(id=" + repr(msg.id()) + ", type=" + repr(msg.type()) + ")")
                 DATA(type_id_constants::MXSERVER_INCOMING_MULTIPLEXER_MESSAGE, MultiplexerMessage,
                      (set_id(msg.id()))(set_from(msg.from()))(set_to(msg.to()))(set_type(msg.type()))(set_timestamp(
                          msg.timestamp()))(set_references(msg.references()))(set_workflow(msg.workflow())))
                     SKIPFILEIF(!(msg.logging_method() & multiplexer::LoggingMethod::FILE)));

  if (memory_log_every_ && ++routed_messages_ % memory_log_every_ == 0)
    MX_LOG(INFO, LOWVERBOSITY,
           CTX("multiplexer.server")
               TEXT("memory: heap_in_use=" + repr(mx::heap_in_use_bytes()) + " messages=" + repr(routed_messages_)));

  MessageMetaHandler meta_handler(msg, conn, raw);
  do {
    if (_handle_message_inlined_rules(meta_handler))
      break; // already handled
    if (_handle_meta_message(meta_handler))
      break; // already handled

    // default message handler
    const Config::MessageDescriptionById &definitions = config_.message_description_by_id();
    Config::MessageDescriptionById::const_iterator definition = definitions.find(msg.type());
    if (definition == definitions.end()) {
      MX_LOG(WARNING, HIGHVERBOSITY,
             CTX("multiplexer.server") TEXT("message of unknown type " + repr(msg.type()) + "; dropping"));
      meta_handler.unknown();
      _record(meta_handler, 0, 0, RoutedMessage::UNKNOWN_TYPE, true);
      break; // won't be handled at all
    }

    // it's known type
    const MultiplexerMessageDescription &description = definition->second;
    if (description.to().empty()) {
      // A reply type, or any other without a rule, sent without `to`: nobody
      // could ever receive it. Say so now rather than let the sender wait.
      MX_LOG(WARNING, HIGHVERBOSITY,
             CTX("multiplexer.server") TEXT("message of type " + repr(msg.type()) + " (" + description.name() +
                                            ") has no routing rule and no `to`; dropping"));
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

  MX_LEAVE();
}

// Sends the collected DeliveryError to the message's `from`, as a message
// addressed by instance id, through _handle_message like any other. It
// asks for no delivery report itself, so a sender that is gone produces no
// second error.
void Server::_handle_delivery_errors(MessageMetaHandler &meta_handler) {
  if (!meta_handler.delivery_error_message)
    return; // no errors

  MX_LOG(ERROR, HIGHVERBOSITY,
         CTX("multiplexer.server") FLOW(meta_handler.msg.workflow())
             TEXT("errors when delivering " + repr(meta_handler.msg.id()))
                 SKIPFILEIF(!(meta_handler.msg.logging_method() & multiplexer::LoggingMethod::FILE)));

  if (!meta_handler.msg.from()) // sanity check
    return;
  if (meta_handler.msg.from() == instance_id_) // sanity check
    return;

  MultiplexerMessage mxmsg;
  mxmsg.set_id(random_());
  mxmsg.set_from(instance_id_);
  mxmsg.set_to(meta_handler.msg.from());
  mxmsg.set_report_delivery_error(false);
  mxmsg.set_type(types::DELIVERY_ERROR);
  meta_handler.delivery_error_message->SerializeToString(mxmsg.mutable_message());
  mxmsg.set_references(meta_handler.msg.id());
  mxmsg.set_workflow(meta_handler.msg.workflow());
  boost::shared_ptr<const RawMessage> raw(RawMessage::FromMessage(mxmsg));
  _handle_message(meta_handler.conn, mxmsg, raw);
  meta_handler.delivery_error_message.reset();
}

// Routing carried by the message itself, which wins over the rules file:
// a `to` instance id, or override_rrules. True when the message was
// handled here.
bool Server::_handle_message_inlined_rules(MessageMetaHandler &meta_handler) {
  if (meta_handler.msg.to()) {
    ConnectionById::iterator entry = connection_by_id_.find(meta_handler.msg.to());
    Connection::pointer connection;
    if (entry == connection_by_id_.end() || !(connection = entry->second.lock())) {
      if (meta_handler.msg.report_delivery_error())
        meta_handler.failed(meta_handler.msg.to());
      _record(meta_handler, meta_handler.msg.to(), 0, RoutedMessage::NO_RECIPIENT,
              meta_handler.msg.report_delivery_error());
      MX_LOG(WARNING, HIGHVERBOSITY,
             CTX("multiplexer.server")
                 TEXT("message to " + repr(meta_handler.msg.to()) + " which is not connected; dropping"));
      return true;
    }
    // A full queue drops the message like an absent peer does, and the
    // sender is told the same way.
    if (connection->schedule(meta_handler.raw)) {
      _record(meta_handler, connection->peer_id(), connection->peer_type(), RoutedMessage::DELIVERED, false);
    } else {
      if (meta_handler.msg.report_delivery_error())
        meta_handler.failed(meta_handler.msg.to());
      _record(meta_handler, connection->peer_id(), connection->peer_type(), RoutedMessage::QUEUE_FULL,
              meta_handler.msg.report_delivery_error());
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
bool Server::_handle_meta_message(MessageMetaHandler &meta_handler) {
  if (meta_handler.msg.type() > types::MAX_MULTIPLEXER_META_PACKET)
    return false; // this is not a meta message

  switch (meta_handler.msg.type()) {
  case types::CONNECTION_WELCOME:
    MX_LOG(WARNING, HIGHVERBOSITY,
           CTX("multiplexer.server") TEXT("CONNECTION_WELCOME on an established connection; dropping it"));
    meta_handler.conn->shutdown();
    return true;

  case types::PING:
  case types::DELIVERY_ERROR:
    // Only meaningful with a `to` field, which was handled before this.
    return true;

  case types::BACKEND_FOR_PACKET_SEARCH: {
    BackendForPacketSearch search;
    if (!search.ParseFromString(meta_handler.msg.message())) {
      MX_LOG(ERROR, HIGHVERBOSITY,
             CTX("multiplexer.server") FLOW(meta_handler.msg.workflow())
                 TEXT("garbled BACKEND_FOR_PACKET_SEARCH packet"));
      meta_handler.unknown();
      return true;
    }
    const MultiplexerMessageDescription *description = config_.message_description(search.packet_type());
    if (!description || !description->to().size()) {
      MX_LOG(ERROR, HIGHVERBOSITY,
             CTX("multiplexer.server") FLOW(meta_handler.msg.workflow())
                 TEXT("BACKEND_FOR_PACKET_SEARCH: unknown packet type " + repr(search.packet_type()) +
                      " or type with no routing rules"));
      meta_handler.unknown();
      return true;
    }

    // The searched-for type's first rule, to every peer it names.
    MultiplexerMessageDescription::RoutingRule rule = description->to().Get(0);
    rule.set_whom(MultiplexerMessageDescription::RoutingRule::ALL);
    rule.set_report_delivery_error(true);
    rule.set_include_original_packet_in_report(false);

    if (rule.peer_type() == peers::ALL_TYPES) {
      BOOST_FOREACH (ConnectionsByType::value_type &by_type, connections_by_type_)
        _schedule(meta_handler, by_type.second, rule, by_type.first);
    } else {
      _schedule(meta_handler, connections_by_type_[rule.peer_type()], rule);
    }
  }
    return true;

  default:
    MX_LOG(WARNING, HIGHVERBOSITY,
           CTX("multiplexer.server")
               TEXT("unknown protocol message type " + repr(meta_handler.msg.type()) + "; dropping"));
    return true;
  }
}

// ---------------------------------------------------------------------------
// Applying rules

unsigned int Server::_schedule(MessageMetaHandler &meta_handler, const MultiplexerMessageDescription &desc) {
  return _schedule(meta_handler, desc.to());
}

unsigned int
Server::_schedule(MessageMetaHandler &meta_handler,
                  const ::google::protobuf::RepeatedPtrField<MultiplexerMessageDescription::RoutingRule> &rules) {
  unsigned int scheduled = 0;
  BOOST_FOREACH (const MultiplexerMessageDescription::RoutingRule &rule, rules)
    scheduled += _schedule(meta_handler, rule);
  return scheduled;
}

unsigned int Server::_schedule(MessageMetaHandler &meta_handler,
                               const MultiplexerMessageDescription::RoutingRule &rule) {
  unsigned int scheduled = 0;
  if (rule.peer_type() == peers::ALL_TYPES) {
    BOOST_FOREACH (ConnectionsByType::value_type &by_type, connections_by_type_)
      scheduled += _schedule(meta_handler, by_type.second, rule, by_type.first);
  } else {
    scheduled += _schedule(meta_handler, connections_by_type_[rule.peer_type()], rule);
  }

  if (!scheduled) {
    const unsigned int level = rule.delivery_error_is_error() ? ERROR : WARNING;
    MX_LOG(level, HIGHVERBOSITY,
           CTX("multiplexer.server") FLOW(meta_handler.msg.workflow())
               TEXT("routing while none present of type " + repr(rule.peer_type()) + " (" +
                    config_.peer_name_by_type(rule.peer_type()) + ")"));
  }
  return scheduled;
}

unsigned int Server::_schedule(MessageMetaHandler &meta_handler, ConnectionsList &connections,
                               const MultiplexerMessageDescription::RoutingRule &rule) {
  return _schedule(meta_handler, connections, rule, rule.peer_type());
}

unsigned int Server::_schedule(MessageMetaHandler &meta_handler, ConnectionsList &connections,
                               const MultiplexerMessageDescription::RoutingRule &rule, boost::uint32_t peer_type) {
  unsigned int scheduled = (unsigned int)-1;
  switch (rule.whom()) {
  case MultiplexerMessageDescription::RoutingRule::ALL:
    scheduled = send_to_all(meta_handler, connections);
    break;
  case MultiplexerMessageDescription::RoutingRule::ANY:
    scheduled = send_to_one(meta_handler, connections);
    break;
  }
  AssertMsg(scheduled != (unsigned int)-1, "unhandled Whom type " + boost::lexical_cast<std::string>(rule.whom()));

  if (!scheduled) {
    if (rule.report_delivery_error())
      meta_handler.failed(rule, peer_type);
    _record(meta_handler, 0, peer_type, RoutedMessage::NO_RECIPIENT, rule.report_delivery_error());
  }
  return scheduled;
}

// whom: ALL. Every live connection of the type gets the frame; one whose
// queue is full drops it (Connection::schedule logs that) and is not
// counted.
unsigned int Server::send_to_all(MessageMetaHandler &meta_handler, ConnectionsList &connections) {
  unsigned int scheduled = 0;
  for (ConnectionsList::iterator current, next = connections.begin();
       next != connections.end() && (current = next++, true);) {
    if (Connection::pointer connection = current->lock()) {
      if (!connection->living())
        continue;
      if (connection->schedule(meta_handler.raw)) {
        ++scheduled;
        _record(meta_handler, connection->peer_id(), connection->peer_type(), RoutedMessage::DELIVERED, false);
      } else {
        _record(meta_handler, connection->peer_id(), connection->peer_type(), RoutedMessage::QUEUE_FULL, false);
      }
    } else
      connections.erase(current); // dead connection
  }
  return scheduled;
}

// whom: ANY. The first live connection with room, starting from the front
// of the type's list, then moved to the back: round robin that skips busy
// peers.
unsigned int Server::send_to_one(MessageMetaHandler &meta_handler, ConnectionsList &connections) {
  Connection::pointer connection;
  for (ConnectionsList::iterator current = connections.begin();
       (current = choose_free_connections(connections, current)) != connections.end(); ++current) {
    if (!(connection = current->lock()))
      continue;
    if (!connection->schedule(meta_handler.raw)) {
      _record(meta_handler, connection->peer_id(), connection->peer_type(), RoutedMessage::QUEUE_FULL, false);
      continue;
    }
    _record(meta_handler, connection->peer_id(), connection->peer_type(), RoutedMessage::DELIVERED, false);
    connections.splice(connections.end(), connections, current);
    return 1;
  }
  return 0;
}

// The peers file: written whole to a temporary name, then renamed, so a
// reader never sees a partial file. One line per registered peer.
void Server::_write_peers_file(Connection *leaving) {
  if (peers_file_.empty())
    return;
  const std::string tmp = peers_file_ + ".tmp";
  {
    std::ofstream out(tmp.c_str(), std::ios::out | std::ios::trunc);
    if (!out.good()) {
      MX_LOG(ERROR, LOWVERBOSITY, CTX("multiplexer.server") TEXT("cannot write peers file " + tmp));
      return;
    }
    for (ConnectionById::const_iterator entry = connection_by_id_.begin(); entry != connection_by_id_.end(); ++entry) {
      Connection::pointer connection = entry->second.lock();
      if (!connection || connection.get() == leaving || !connection->living())
        continue;
      out << connection->peer_id() << " " << config_.peer_name_by_type(connection->peer_type()) << " "
          << connection->peer_type() << "\n";
    }
  }
  if (std::rename(tmp.c_str(), peers_file_.c_str()) != 0)
    MX_LOG(ERROR, LOWVERBOSITY, CTX("multiplexer.server") TEXT("cannot rename peers file to " + peers_file_));
}

}; // namespace multiplexer
