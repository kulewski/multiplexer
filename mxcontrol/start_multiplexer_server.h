// The run_multiplexer subcommand; see the class comment.
#ifndef MX_MXCONTROL_START_MULTIPLEXER_SERVER_H_
#define MX_MXCONTROL_START_MULTIPLEXER_SERVER_H_

#include "mxcontrol/task.h"

namespace mxcontrol {

// run_multiplexer: run one multiplexer until SIGINT or SIGTERM. Options:
// --rules (the rules file) and --rules-check-interval (how often it is
// read again for a change; SIGHUP reads it now), --address host:port or
// [IPv6 address]:port (0.0.0.0:1980; port 0 picks a free port; an IP
// address, not a name), --port-file (where to write the bound address, in
// the same form), --record
// and --record-payload-bytes (a recording from the start), --recording-dir
// and --allow-tap (recording sessions and taps peers may ask for),
// --peers-file (the connected peers, rewritten on every change),
// --drain-seconds (how long a stop goes on sending what is queued; see
// Server::stop), --threads (the event loops the connections are dealt out
// over, each on a thread of its own, Server::set_threads; the
// environment's MX_SERVER_THREADS when it is not given, so that a test
// suite runs its multiplexers on several loops without a change to its
// harness), --monitor-address (where /healthz and /readyz are served over
// HTTP for Kubernetes probes, and /metrics for Prometheus,
// multiplexer/monitor.h; nothing is served without it) and
// --monitor-port-file (where to write the monitor's bound address, as
// --port-file does the multiplexer's). See docs/mxcontrol.md.
class StartMultiplexerServer : public Task {
 public:
  virtual int run();
  // The options, refused as a malformed line, with the usage, when the
  // rules check interval is one the server refuses
  // (Server::rules_check_interval_refused), --address is not an IP
  // address and a port (multiplexer/endpoint.h), --monitor-address, when
  // given, is not an IP address and a port, which it needs,
  // --monitor-port-file comes without --monitor-address, or the threads,
  // given or from MX_SERVER_THREADS, are not a number from 1 to
  // MAX_THREADS.
  virtual void parse_options(std::vector<std::string>& args);
  // The most event loops --threads asks for.
  static constexpr unsigned int MAX_THREADS = 256;
  virtual std::string short_description() const { return "run a multiplexer"; }
  virtual std::string short_synopsis(const std::string& commandname) {
    return "<" + commandname + "-options> [--address] address:port";
  }

 protected:
  virtual void _initialize_options(mx::options::Options& options) {
    options.add("rules", &rules_file_, "multiplexer.rules", "file from which routing rules will be read");
    options.add("rules-check-interval", &rules_check_interval_, 2.0,
                "seconds between checks of the rules file for a change, which is then put in use "
                "without a restart, 0.01 at least; 0 never checks (SIGHUP and `mxcontrol rules reload` "
                "still do)");
    options
        .add("address,M", &host_port_, "0.0.0.0:1980",
             "local address to listen on, host:port or [IPv6 address]:port, 1980 when the port is left out: "
             "0.0.0.0 is every IPv4 address, [::] every IPv6 one and, where the system maps IPv4 onto it as "
             "Linux does by default, every IPv4 one too")
        .positional("address");
    options.add("port-file", &port_file_,
                "once listening, write the bound address as host:port, or [IPv6 address]:port, to this file "
                "(use with --address host:0 to let the system pick a port)");
    options.add("memory-log-every", &memory_log_every_, 0,
                "log the C heap in use after every N routed messages (soak tests)");
    options.add("record", &record_file_,
                "append every peer event and delivery attempt to this file (Recording.proto records)");
    options.add("record-payload-bytes", &record_payload_bytes_, 0,
                "keep only the first N bytes of each recorded payload; 0 keeps all");
    options.add("recording-dir", &recording_dir_,
                "let peers start and stop recording sessions over the protocol, written to this directory");
    options.add_switch("allow-tap", &allow_tap_,
                       "let peers receive every record over their connection (RECORDING_CONTROL TAP)");
    options.add("peers-file", &peers_file_,
                "rewrite this file with the connected peers on every registration and unregistration");
    options.add("drain-seconds", &drain_seconds_, 5.0,
                "on SIGTERM or SIGINT, seconds to go on routing and sending what is queued before each "
                "connection closes, then exit; a second signal, or 0, stops at once");
    options.add("threads", &threads_, 0,
                "event loops the connections are dealt out over as they arrive, each on a thread of its own, "
                "the routing under one lock: 1 runs the multiplexer on one thread, more for a host where one "
                "thread is full (docs/operations.md); 0 takes MX_SERVER_THREADS from the environment, 1 when it "
                "is not set");
    options.add("monitor-address", &monitor_host_port_,
                "serve /healthz and /readyz over HTTP on this address, host:port or [IPv6 address]:port, for "
                "Kubernetes probes (docs/operations.md): 200 while every event loop runs, and /readyz 503 once "
                "the multiplexer stops; and /metrics for Prometheus (docs/metrics.md); nothing is served "
                "without it");
    options.add("monitor-port-file", &monitor_port_file_,
                "once the monitor listens, write its bound address, host:port or [IPv6 address]:port, to this "
                "file, before the --port-file (use with --monitor-address host:0 to let the system pick a port)");
  }

 private:
  std::string host_port_;  // --address as given
  std::string host_;       // and as parse_options() read it
  std::uint16_t port_ = 0;
  std::string rules_file_;
  float rules_check_interval_;
  std::string port_file_;
  unsigned int memory_log_every_;
  std::string record_file_;
  unsigned int record_payload_bytes_;
  std::string recording_dir_;
  bool allow_tap_ = false;
  std::string peers_file_;
  float drain_seconds_;
  unsigned int threads_ = 0;       // --threads as given, then as parse_options() settled it
  std::string monitor_host_port_;  // --monitor-address as given, empty for none
  std::string monitor_host_;       // and as parse_options() read it
  std::uint16_t monitor_port_ = 0;
  std::string monitor_port_file_;
};

};  // namespace mxcontrol

#endif  // MX_MXCONTROL_START_MULTIPLEXER_SERVER_H_
