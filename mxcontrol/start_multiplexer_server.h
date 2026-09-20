// The run_multiplexer subcommand; see the class comment.
#ifndef MX_MXCONTROL_START_MULTIPLEXER_SERVER_H_
#define MX_MXCONTROL_START_MULTIPLEXER_SERVER_H_

#include "mxcontrol/task.h"

namespace mxcontrol {

// run_multiplexer: run one multiplexer until SIGINT or SIGTERM. Options:
// --rules (the rules file), --address host:port (0.0.0.0:1980; port 0 picks
// a free port), --port-file (where to write the bound address), --record
// and --record-payload-bytes (a recording from the start), --recording-dir
// and --allow-tap (recording sessions and taps peers may ask for),
// --peers-file (the connected peers, rewritten on every change). See
// docs/mxcontrol.md.
class StartMultiplexerServer : public Task {
 public:
  virtual int run();
  virtual std::string short_description() const { return "run a multiplexer"; }
  virtual std::string short_synopsis(const std::string& commandname) {
    return "<" + commandname + "-options> [--address] address:port";
  }

 protected:
  virtual void _initialize_options(mx::options::Options& options) {
    options.add("rules", &rules_file_, "multiplexer.rules", "file from which routing rules will be read");
    options.add("address,M", &host_port_, "0.0.0.0:1980", "local address to listen on").positional("address");
    options.add("port-file", &port_file_,
                "once listening, write the bound address as host:port to this file "
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
  }

 private:
  std::string host_port_;
  std::string rules_file_;
  std::string port_file_;
  unsigned int memory_log_every_;
  std::string record_file_;
  unsigned int record_payload_bytes_;
  std::string recording_dir_;
  bool allow_tap_ = false;
  std::string peers_file_;
};

};  // namespace mxcontrol

#endif  // MX_MXCONTROL_START_MULTIPLEXER_SERVER_H_
