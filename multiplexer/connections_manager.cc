// The CONNECTION_WELCOME frame a manager sends after connecting: a
// WelcomeMessage with its peer type and instance id, serialized once and
// reused for every connection.
#include "multiplexer/connections_manager.h"

#include "multiplexer/multiplexer.constants.h" /* generated */

namespace multiplexer {
namespace impl {

namespace {

std::shared_ptr<const RawMessage> pack_welcome(const WelcomeMessage& welcome, std::uint64_t instance_id) {
  // pack it into MultiplexerMessage
  MultiplexerMessage msg;
  msg.set_id(0);
  msg.set_from(instance_id);
  msg.set_type(types::CONNECTION_WELCOME);
  welcome.SerializeToString(msg.mutable_message());

  // serialize
  return std::shared_ptr<const RawMessage>(RawMessage::FromMessage(msg));
}

}  // namespace

std::shared_ptr<const RawMessage> create_welcome_message(std::uint32_t peer_type, std::uint64_t instance_id) {
  WelcomeMessage welcome;
  welcome.set_type(peer_type);
  welcome.set_id(instance_id);
  return pack_welcome(welcome, instance_id);
}

std::shared_ptr<const RawMessage> create_welcome_message(std::uint32_t peer_type, std::uint64_t instance_id,
                                                         const Routing& routing) {
  WelcomeMessage welcome;
  welcome.set_type(peer_type);
  welcome.set_id(instance_id);
  *welcome.mutable_routing() = routing;
  return pack_welcome(welcome, instance_id);
}

};  // namespace impl
};  // namespace multiplexer
