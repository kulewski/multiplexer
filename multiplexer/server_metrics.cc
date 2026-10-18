// Server's metrics: the drops counted where routing classifies them, their
// labels, and the snapshot the monitor's /metrics reads (Server::metrics,
// docs/metrics.md). Nothing here runs per message: a drop is counted on its
// failure path, and the snapshot walks the connections once a scrape.
#include <map>
#include <string>
#include <tuple>

#include "lib/repr.h"
#include "multiplexer/server.h"

namespace multiplexer {

// Two kinds of malformed protocol message are one reason.
const char* Server::_reason_label(unsigned int kind) {
  switch (kind) {
    case NONE_PRESENT:
      return "no_receiver";
    case ROUTING_OFF:
      return "routing_off";
    case ALL_FULL:
      return "all_full";
    case KEY_NOT_TAKEN:
      return "key_not_taken";
    case NOT_CONNECTED:
      return "not_connected";
    case QUEUE_FULL:
      return "queue_full";
    case UNKNOWN_TYPE:
      return "unknown_type";
    case NO_RULE:
      return "no_rule";
    case UNKNOWN_PROTOCOL_TYPE:
    case BAD_SEARCH:
      return "protocol_error";
    case KEY_TOO_LONG:
      return "key_too_long";
    case PEER_GONE:
      return "peer_gone";
    case CLOSING:
      return "closing";
    default:
      return "other";
  }
}

// The rules decide what a type is: one they do not have, from a peer's
// `to` message or a rule of its own, is OUTSIDE_RULES, whatever its number.
void Server::_count_drop(unsigned int kind, std::uint64_t type, std::uint64_t peer_type, std::uint64_t count) {
  if (type != NO_TYPE && !config_.message_description(static_cast<std::uint32_t>(type))) {
    type = OUTSIDE_RULES;
  }
  if (peer_type != NO_TYPE && !config_.peer_description(static_cast<std::uint32_t>(peer_type)) &&
      !controller(static_cast<std::uint32_t>(peer_type))) {
    peer_type = OUTSIDE_RULES;
  }
  dropped_[std::make_tuple(kind, type, peer_type)] += count;
}

std::string Server::_type_label(std::uint64_t type) const {
  if (type == NO_TYPE) {
    return "";
  }
  if (type == OUTSIDE_RULES) {
    return "unknown";
  }
  const MultiplexerMessageDescription* description = config_.message_description(static_cast<std::uint32_t>(type));
  return description ? description->name() : mx::repr(type);
}

std::string Server::_peer_label(std::uint64_t peer_type) const {
  if (peer_type == NO_TYPE) {
    return "";
  }
  if (peer_type == OUTSIDE_RULES) {
    return "unknown";
  }
  const std::uint32_t number = static_cast<std::uint32_t>(peer_type);
  return config_.peer_description(number) || controller(number) ? _peer_name(number) : mx::repr(peer_type);
}

// Every peer type of the rules a peer may connect as is listed, so that one
// nobody is connected as says 0 rather than nothing; the reserved ones,
// ALL_TYPES and MULTIPLEXER among them, are not, and a controller is while
// one is connected.
Server::Metrics Server::metrics() const {
  RoutingScope routing(*this);
  Metrics metrics;
  metrics.received_messages = ended_received_messages_;
  metrics.received_bytes = ended_received_bytes_;
  for (const auto& entry : accepted_) {
    if (Connection::pointer connection = entry.second.lock()) {
      metrics.received_messages += connection->received_messages();
      metrics.received_bytes += connection->received_bytes();
    }
  }
  metrics.accept_failures = accept_failures_;
  for (const auto& peer : config_.peer_by_type()) {
    if (peer.first > peers::MAX_MULTIPLEXER_SPECIAL_PEER_TYPE) {
      metrics.peers[peer.second.name()] = 0;
      metrics.queued[peer.second.name()] = 0;
    }
  }
  for (const ConnectionsByType::value_type& by_type : connections_by_type_) {
    const std::string name = _peer_label(by_type.first);
    for (const Connection::weak_pointer& weak : by_type.second) {
      if (Connection::pointer connection = weak.lock()) {
        if (connection->living()) {
          ++metrics.peers[name];
          metrics.queued[name] += connection->outgoing_queue_size();
        }
      }
    }
  }
  // Two kinds may share a reason, and two numbers a label once a reload
  // took their names away: summed under the labels a scrape sees.
  std::map<std::tuple<std::string, std::string, std::string>, std::uint64_t> labelled;
  for (const auto& drop : dropped_) {
    labelled[std::make_tuple(_reason_label(std::get<0>(drop.first)), _type_label(std::get<1>(drop.first)),
                             _peer_label(std::get<2>(drop.first)))] += drop.second;
  }
  for (const auto& drop : labelled) {
    metrics.dropped.push_back({std::get<0>(drop.first), std::get<1>(drop.first), std::get<2>(drop.first), drop.second});
  }
  metrics.rules_fingerprint = rules_fingerprint_;
  metrics.rules_failing = !rules_last_error_.empty();
  metrics.stopping = stopping_;
  return metrics;
}

}  // namespace multiplexer
