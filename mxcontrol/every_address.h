// EveryAddress: the multiplexers behind the -M host:port addresses of the
// mxcontrol subcommands that must reach every replica, recording and rules.
// A name stands for every address it resolves to, one connection each; a
// subcommand that keeps running calls refresh() at every poll, which
// resolves the names again, connects to the addresses that are new, so
// that a replica that comes back under another address is reached too,
// and drops the ones that are gone.
//
// The connections are made by address, since a client connected by name
// holds one connection and so reaches one replica, and the client
// reconnects each to its own address only: hence refresh(). An address no
// name resolves to any more is dropped (Client::disconnect) once its
// connection is down, so that its reconnect, every AUTO_RECONNECT_TIME,
// never reaches what listens there later, a multiplexer of another
// deployment that got the address, as pod addresses are reused; while its
// connection lives it stays, a replica on its way out still reached. Which
// connection is down is known from the one heard from last (heard()): the
// client makes a new one at each reconnect. A lookup that fails, or is
// still under way, drops nothing. refresh() looks the names up on asio's
// resolver thread, so a slow name server never holds up the loop that
// reads, and costs nothing for an address given as such.
#ifndef MX_MXCONTROL_EVERY_ADDRESS_H_
#define MX_MXCONTROL_EVERY_ADDRESS_H_

#include <asio/io_service.hpp>
#include <asio/ip/tcp.hpp>
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
  // `addresses` as -M gives them and Task::parse_options() checked them,
  // host:port or [IPv6 address]:port, an empty host meaning 127.0.0.1;
  // `client` runs on `io_service`.
  EveryAddress(multiplexer::Client& client, asio::io_service& io_service, const std::vector<std::string>& addresses);
  ~EveryAddress();  // lets go of a lookup under way

  // Resolves every address and connects to each address found, waiting up
  // to `timeout` seconds for each handshake; returns how many of the -M
  // addresses reached at least one multiplexer. What could not be
  // resolved, connected or welcomed is said on stderr.
  unsigned int connect(float timeout);

  // Resolves every name again, in the background, and connects, without
  // waiting, to each address found that the client was never given; the
  // connections come up while the caller's next calls run the loop. Once a
  // lookup has answered, every address no -M address resolves to now, by
  // the last answers, is dropped if its connection is down. A name whose
  // lookup is still under way is not looked up again until it ends; one
  // that does not resolve is said on stderr, once until it resolves again,
  // and so is every new address and every address dropped.
  void refresh();

  // A message came through `connection`: the connection to its address
  // now, which a reconnect made after the one connect() or refresh() made,
  // and the one whose end says the address's connection is down. For every
  // message the caller receives.
  void heard(const multiplexer::ConnectionWrapper& connection);

  // Whether a -M address resolves to `address` now, as its last lookup
  // that answered said; an address given as such resolves to itself.
  bool resolved(const asio::ip::tcp::endpoint& address) const;

 private:
  struct State;
  std::shared_ptr<State> state_;  // a lookup under way holds it weakly
};

}  // namespace mxcontrol

#endif  // MX_MXCONTROL_EVERY_ADDRESS_H_
