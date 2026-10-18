// multiplexer::Monitor: a multiplexer's health over HTTP, for a Kubernetes
// probe or anyone with curl, on an mx::HttpServer of its own thread:
// run_multiplexer --monitor-address serves it (docs/operations.md).
//
//   GET /healthz  200 when every event loop of the multiplexer has run a
//                 check posted to it within CHECK_SECONDS; 503 naming the
//                 loops that did not, stuck in a handler or a deadlock: a
//                 liveness probe, whose failures restart the process.
//   GET /readyz   the same, and 503 from the moment stop() began, a drain
//                 say: a readiness probe, which takes the pod out of its
//                 Service's endpoints, so that no new peer connects to a
//                 multiplexer about to go.
//   anything else 404; a query string is ignored.
//
// A TCP probe of the multiplexer's own port tells only that the kernel
// accepts for it: a multiplexer whose loops are stuck still completes
// every handshake. A check is a no-op posted to each loop, run in its turn
// after what the loop is running and what was posted there before, so
// that an answer says the loop runs.
//
// A loop that has not run the last check posted to it is not posted
// another, but waited for again: checks do not pile up in the queue of a
// loop stuck for good. What the loops answer into is shared with every
// check posted, which may run, or be let go of with its loop, after the
// monitor is gone.
#ifndef MX_MULTIPLEXER_MONITOR_H_
#define MX_MULTIPLEXER_MONITOR_H_

#include <asio/ip/tcp.hpp>
#include <cstdint>
#include <memory>
#include <string>

#include "lib/http_server.h"
#include "multiplexer/server.h"

namespace multiplexer {

class Monitor {
 public:
  // Serves on `endpoint`, port 0 for one the system picks, from now until
  // stop() or the destructor, asking `server`, whose set_threads() has run,
  // about its loops. Throws std::system_error when the address cannot be
  // bound.
  Monitor(Server::pointer server, const asio::ip::tcp::endpoint& endpoint);
  // stop().
  ~Monitor();
  Monitor(const Monitor&) = delete;
  Monitor& operator=(const Monitor&) = delete;

  // The port served on.
  std::uint16_t port() const { return http_.port(); }
  // Serving ends: a check under way is answered at once, the HTTP server's
  // thread joined. Before the loops are finished (Server::finish_loops),
  // which a check under way would wait for in vain; a second call does
  // nothing.
  void stop();

  // How long a check waits for every loop to run what it posted there, in
  // seconds: well past the longest a loop runs a handler, short of a
  // probe's timeout (operations.md sets 5 s).
  static constexpr float CHECK_SECONDS = 2;

 private:
  struct Answers;
  // The HttpServer's handler.
  int _answer(const std::string& target, std::string& body);
  // Empty when every loop ran a check within CHECK_SECONDS, else why not,
  // the body of a 503.
  std::string _check_loops();

  const Server::pointer server_;
  const std::shared_ptr<Answers> answers_;
  mx::HttpServer http_;  // last: it serves once the rest is in place, and stops first
};

}  // namespace multiplexer

#endif  // MX_MULTIPLEXER_MONITOR_H_
