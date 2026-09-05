// The run_multiplexer subcommand; see the class comment.
#ifndef MX_MXCONTROL_START_MULTIPLEXER_SERVER_H_
#define MX_MXCONTROL_START_MULTIPLEXER_SERVER_H_

#include "mxcontrol/task.h"

namespace mxcontrol {

// run_multiplexer: run one multiplexer until SIGINT or SIGTERM. Options:
// --rules (the rules file), --address host:port (0.0.0.0:1980; port 0 picks
// a free port), --port-file (where to write the bound address), --record
// and --record-payload-bytes (the recording), --peers-file (the connected
// peers, rewritten on every change). See docs/mxcontrol.md.
class StartMultiplexerServer : public Task {
public:
  virtual int run();
  virtual std::string short_description() const { return "run a multiplexer"; }
  virtual std::string short_synopsis(const std::string &commandname) {
    return "<" + commandname + "-options> [--address] address:port";
  }

protected:
  virtual void _initialize_options_description(po::options_description &generic) {
    generic.add_options()("rules", po::value(&rules_file_)->default_value("multiplexer.rules"),
                          "file from which routing rules will be read")(
        "address,M", po::value(&host_port_)->default_value("0.0.0.0:1980"),
        "local address to listen on")("port-file", po::value(&port_file_),
                                      "once listening, write the bound address as host:port to this file "
                                      "(use with --address host:0 to let the system pick a port)")(
        "memory-log-every", po::value(&memory_log_every_)->default_value(0),
        "log the C heap in use after every N routed messages (soak tests)")(
        "record", po::value(&record_file_),
        "append every peer event and delivery attempt to this file (Recording.proto records)")(
        "record-payload-bytes", po::value(&record_payload_bytes_)->default_value(0),
        "keep only the first N bytes of each recorded payload; 0 keeps all")(
        "peers-file", po::value(&peers_file_),
        "rewrite this file with the connected peers on every registration and unregistration");
  }

  virtual void _initialize_positional_options_description(po::positional_options_description &positional) {
    positional.add("address", 1);
  }

private:
  std::string host_port_;
  std::string rules_file_;
  std::string port_file_;
  unsigned int memory_log_every_;
  std::string record_file_;
  unsigned int record_payload_bytes_;
  std::string peers_file_;
};

}; // namespace mxcontrol

#endif // MX_MXCONTROL_START_MULTIPLEXER_SERVER_H_
