// BasicClient's routing and key filters: told to every connection with
// PEER_CONTROL and carried in every welcome, confirmed by PEER_STATUS, and
// acknowledged once every connection has confirmed and what was read
// before has been taken. Part of BasicClient; see basic_client.cc.

#include <stdexcept>
#include <string>

#include "lib/logging/logging.h"
#include "lib/repr.h"
#include "multiplexer/basic_client.h"
#include "multiplexer/multiplexer.constants.h"

using namespace mx;
using namespace multiplexer;
using mx::SimpleTimer;

void BasicClient::set_routing(const Routing& routing, AcknowledgedCallback acknowledged) {
  MX_DCHECK_RUN_ON(&owner_thread());
  routing_ = routing;
  MX_LOG(INFO, LOWVERBOSITY, CTX("BasicClient") TEXT("routing " + routing_text(routing)));
  if (acknowledged) {
    acknowledged_callbacks_.push_back(std::move(acknowledged));
  }
  _routing_changed();
}

void BasicClient::set_key_filters(const KeyFilters& filters, AcknowledgedCallback acknowledged) {
  MX_DCHECK_RUN_ON(&owner_thread());
  check_key_filters(filters);
  if (acknowledged) {
    acknowledged_callbacks_.push_back(std::move(acknowledged));
  }
  key_filters_ = filters;
  key_filters_message_.Clear();
  for (const KeyFilters::value_type& filter : filters) {
    KeyFilter* message = key_filters_message_.Add();
    message->set_type(filter.first);
    for (const std::string& pattern : filter.second) {
      message->add_pattern(pattern);
    }
  }
  MX_LOG(INFO, LOWVERBOSITY, CTX("BasicClient") TEXT("key filters " + key_filters_text(key_filters_message_)));
  _routing_changed();
}

// What a library can check without RE2; the multiplexer checks the rest.
void BasicClient::check_key_filters(const KeyFilters& filters) {
  for (const KeyFilters::value_type& filter : filters) {
    if (filter.first <= types::MAX_MULTIPLEXER_META_PACKET) {
      throw std::invalid_argument("a key filter for message type " + std::to_string(filter.first) +
                                  ", a protocol type");
    }
    for (const std::string& pattern : filter.second) {
      if (pattern.size() > MAX_KEY_PATTERN_SIZE) {
        throw std::invalid_argument("a key pattern of " + std::to_string(pattern.size()) +
                                    " bytes, over the limit of " + std::to_string(MAX_KEY_PATTERN_SIZE) +
                                    " (MAX_KEY_PATTERN_SIZE)");
      }
    }
  }
}

void BasicClient::_routing_changed() {
  ++routing_version_;
  confirmed_ = false;
  welcome_message_.reset();  // the next connection's welcome carries the new routing
  for (ConnectionById::iterator entry = connection_by_id_.begin(); entry != connection_by_id_.end(); ++entry) {
    if (Connection::pointer conn = entry->second.lock()) {
      if (conn->living()) {
        _send_routing(conn);
      }
    }
  }
  _update_confirmation();  // with no connection, at once
}

// Forced past a full outgoing queue, since the peer that steps out is
// often the saturated one, and pinned, so that a connection that dies
// does not hand the frame to another multiplexer: each gets its own.
void BasicClient::_send_routing(Connection::pointer conn) {
  PeerControl control;
  *control.mutable_routing() = routing_;
  *control.mutable_key_filter() = key_filters_message_;
  MultiplexerMessage mxmsg;
  mxmsg.set_id(random_());
  mxmsg.set_sender(instance_id_);
  mxmsg.set_type(PEER_CONTROL);
  control.SerializeToString(mxmsg.mutable_message());
  std::shared_ptr<const RawMessage> raw(RawMessage::FromMessage(mxmsg));
  raw->mark_pinned();
  raw->mark_own();  // a connection made since carries the routing in its welcome
  conn->managers_private_data().routing_request_id = mxmsg.id();
  if (!conn->schedule(raw, /*force=*/true)) {
    MX_LOG(WARNING, LOWVERBOSITY,
           CTX("BasicClient") TEXT("could not queue PEER_CONTROL on the connection to " + repr(conn->peer_id())));
  }
}

// The answer to the last PEER_CONTROL sent on this connection confirms the
// current routing and key filters; the answer to an older one, when two
// changes were made in a row, confirms nothing, whatever it carries. A
// refusal is logged as an error: a key pattern of the program's that does
// not compile, most likely, which the program should hear of.
void BasicClient::_on_peer_status(Connection::pointer conn, const MultiplexerMessage& mxmsg) {
  PeerStatus status;
  if (!status.ParseFromString(mxmsg.message()) || status.has_error()) {
    MX_LOG(ERROR, LOWVERBOSITY,
           CTX("BasicClient") TEXT("PEER_STATUS from " + repr(conn->peer_id()) + ": " +
                                   (status.has_error() ? status.error() : "garbled")));
    return;
  }
  auto& data = conn->managers_private_data();
  if (mxmsg.references() == data.routing_request_id && same_routing(status.routing(), routing_) &&
      same_key_filters(status.key_filter(), key_filters_message_)) {
    data.routing_acknowledged = routing_version_;
    _update_confirmation();
  }
}

bool BasicClient::_all_confirmed() const {
  for (ConnectionByTarget::const_iterator entry = connection_by_target_.begin(); entry != connection_by_target_.end();
       ++entry) {
    if (Connection::pointer conn = entry->second.lock()) {
      if (!conn->living()) {
        continue;
      }
      // A handshake in flight: the multiplexer may have indexed the
      // connection, with whatever routing its welcome carried, before we
      // hear back.
      if (!conn->registered() || conn->managers_private_data().routing_acknowledged != routing_version_) {
        return false;
      }
    }
  }
  return true;
}

// A message a multiplexer routed under the routing before reaches this
// client before that multiplexer's confirmation, both down one connection
// in order, so once every connection has confirmed, what came before is in
// the queue up to the count noted then, and nothing after it is old: a
// count where waiting for the queue to be empty would wait for as long as
// work keeps coming. A connection that registers with the current routing
// in its welcome brings nothing older, and one whose welcome was older is
// unconfirmed until its PEER_STATUS, which then moves the count on.
void BasicClient::_update_confirmation() {
  if (!_all_confirmed()) {
    confirmed_ = false;
    return;
  }
  if (!confirmed_) {
    confirmed_ = true;
    confirmed_enqueued_ = enqueued_;
  }
  if (routing_acknowledged() && (acknowledged_version_ != routing_version_ || !acknowledged_callbacks_.empty())) {
    acknowledged_version_ = routing_version_;
    _acknowledged();
  }
}

void BasicClient::_acknowledged_by_receive() {
  MX_DCHECK_RUN_ON(&owner_thread());
  if (acknowledged_version_ != routing_version_ && routing_acknowledged()) {
    acknowledged_version_ = routing_version_;
    _acknowledged();
  }
}

void BasicClient::_acknowledged() {
  acknowledged_wake_ = true;
  if (incoming_sink_) {
    run_acknowledged_callbacks();  // everything before went to the sink already
  }
}

bool BasicClient::routing_acknowledged() const {
  MX_DCHECK_RUN_ON(&owner_thread());
  return confirmed_ && dequeued_ >= confirmed_enqueued_ && _all_confirmed();
}

void BasicClient::run_acknowledged_callbacks() {
  MX_DCHECK_RUN_ON(&owner_thread());
  if (acknowledged_callbacks_.empty() || shuts_down_ || !routing_acknowledged()) {
    return;
  }
  std::vector<AcknowledgedCallback> due;
  due.swap(acknowledged_callbacks_);  // a callback's own change waits for its own acknowledgement
  for (AcknowledgedCallback& callback : due) {
    try {
      callback();
    } catch (const std::exception& error) {
      MX_LOG(ERROR, LOWVERBOSITY,
             CTX("BasicClient") TEXT(std::string("a callback of an acknowledged change threw: ") + error.what()));
    }
  }
}
