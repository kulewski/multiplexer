// The CONNECTION_WELCOME frame a manager sends after connecting: a
// WelcomeMessage with its peer type and instance id, serialized once and
// reused for every connection.
#include "multiplexer/multiplexer.constants.h" /* generated */

#include "multiplexer/connections_manager.h"

namespace multiplexer {
namespace impl {

boost::shared_ptr<const RawMessage> create_welcome_message(boost::uint32_t peer_type, boost::uint64_t instance_id) {

  // prepare WelcomeMessage
  WelcomeMessage welcome;
  welcome.set_type(peer_type);
  welcome.set_id(instance_id);

  // pack it into MultiplexerMessage
  MultiplexerMessage msg;
  msg.set_id(0);
  msg.set_from(instance_id);
  msg.set_type(types::CONNECTION_WELCOME);
  welcome.SerializeToString(msg.mutable_message());

  // serialize
  return boost::shared_ptr<const RawMessage>(RawMessage::FromMessage(msg));
}

}; // namespace impl
}; // namespace multiplexer
