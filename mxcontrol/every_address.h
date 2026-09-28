// EveryAddress: the multiplexers behind the -M host:port addresses of the
// mxcontrol subcommands that must reach every replica, recording and rules.
// A name stands for every address it resolves to, one connection each; a
// subcommand that keeps running calls refresh() at every poll, which
// resolves the names again and connects to the addresses that are new, so
// that a replica that comes back under another address is reached too.
//
// The connections are made by address, since a client connected by name
// holds one connection and so reaches one replica, and the client
// reconnects each to its own address only: hence refresh(). A connection is
// never let go: one to an address that no name resolves to any more stays,
// retried by the client every AUTO_RECONNECT_TIME as any connection is,
// since the client has no call that drops one; a lookup that fails changes
// nothing. refresh() looks the names up on asio's resolver thread, so a
// slow name server never holds up the loop that reads, and costs nothing
// for an address given as such.
#ifndef MX_MXCONTROL_EVERY_ADDRESS_H_
#define MX_MXCONTROL_EVERY_ADDRESS_H_

#include <asio/io_service.hpp>
#include <memory>
#include <string>
#include <vector>

#include "multiplexer/client.h"

namespace mxcontrol {

// See the file comment. Used on the client's thread, and made after the
// client, which it needs alive: destroyed first, it lets go of a lookup
// still under way, whose answer then changes nothing.
class EveryAddress {
 public:
  // `addresses` as -M gives them, host:port, an empty host meaning
  // 127.0.0.1; `client` runs on `io_service`.
  EveryAddress(multiplexer::Client& client, asio::io_service& io_service, const std::vector<std::string>& addresses);
  ~EveryAddress();  // lets go of a lookup under way

  // Resolves every address and connects to each address found, waiting up
  // to `timeout` seconds for each handshake; returns how many of the -M
  // addresses reached at least one multiplexer. What could not be
  // resolved, connected or welcomed is said on stderr.
  unsigned int connect(float timeout);

  // Resolves every name again, in the background, and connects, without
  // waiting, to each address found that the client was never given; the
  // connections come up while the caller's next calls run the loop. A name
  // whose lookup is still under way is not looked up again until it ends;
  // one that does not resolve is said on stderr, once until it resolves
  // again, and so is every new address.
  void refresh();

 private:
  struct State;
  std::shared_ptr<State> state_;  // a lookup under way holds it weakly
};

}  // namespace mxcontrol

#endif  // MX_MXCONTROL_EVERY_ADDRESS_H_
