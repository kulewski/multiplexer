// multiplexer::Monitor: a multiplexer's health and metrics over HTTP, for
// Kubernetes probes, a Prometheus scrape or anyone with curl, on an
// mx::HttpServer of its own thread: run_multiplexer --monitor-address
// serves it (docs/operations.md, docs/metrics.md).
//
//   GET /healthz  200 when every event loop of the multiplexer has run a
//                 check posted to it within CHECK_SECONDS; 503 naming the
//                 loops that did not, stuck in a handler or a deadlock: a
//                 liveness probe, whose failures restart the process.
//   GET /readyz   the same, and 503 from the moment stop() began, a drain
//                 say: a readiness probe, which takes the pod out of its
//                 Service's endpoints, so that no new peer connects to a
//                 multiplexer about to go.
//   GET /metrics  Prometheus' text format (docs/metrics.md): the routing's
//                 counts, read on the main loop (Server::metrics), how long
//                 a check posted to each loop waited, and the process's own
//                 figures; 503 when the main loop did not answer within
//                 CHECK_SECONDS.
//   anything else 404; a query string is ignored.
//
// A TCP probe of the multiplexer's own port tells only that the kernel
// accepts for it: a multiplexer whose loops are stuck still completes
// every handshake. A check is a no-op posted to each loop, run in its turn
// after what the loop is running and what was posted there before, so
// that an answer says the loop runs, and how long it waited how busy.
//
// A loop that has not run the last check posted to it is not posted
// another, but waited for again, nor is the main loop asked for a second
// snapshot while it owes one: nothing piles up in the queue of a loop
// stuck for good. What the loops answer into is shared with everything
// posted, which may run, or be let go of with its loop, after the monitor
// is gone. Nothing of the monitor runs between requests.
#ifndef MX_MULTIPLEXER_MONITOR_H_
#define MX_MULTIPLEXER_MONITOR_H_

#include <asio/ip/tcp.hpp>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

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
  // What /metrics answers with: Prometheus' text format, version 0.0.4.
  static constexpr const char* METRICS_CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8";

 private:
  struct Answers;
  // What a round of checks found: the loops that did not run theirs
  // within CHECK_SECONDS, how long each loop's last check waited, or has
  // waited so far, in seconds, and the snapshot, when one was asked for
  // and came.
  struct Checked {
    std::vector<unsigned int> unanswered;
    std::vector<double> lag;
    bool stopping = false;  // stop(): the round ended before its answers
    std::unique_ptr<Server::Metrics> snapshot;
  };
  // The HttpServer's handler.
  int _answer(const std::string& target, std::string& body, std::string& content_type);
  // A round: a check posted to every loop that ran its last one, and with
  // `snapshot` the main loop asked for the routing's counts, unless it
  // owes the last ones; then the answers waited for, CHECK_SECONDS at most.
  Checked _check(bool snapshot);
  // /metrics' body, from a round that brought its snapshot.
  std::string _metrics(const Checked& checked) const;

  const Server::pointer server_;
  const std::shared_ptr<Answers> answers_;
  const double started_at_;  // process_start_time_seconds, read once
  mx::HttpServer http_;      // last: it serves once the rest is in place, and stops first
};

}  // namespace multiplexer

#endif  // MX_MULTIPLEXER_MONITOR_H_
