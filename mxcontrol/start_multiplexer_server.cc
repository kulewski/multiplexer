// run_multiplexer: creates the Server, reads the rules, binds, optionally
// writes the port file, and runs the io_service until a signal stops it.
#include "mxcontrol/start_multiplexer_server.h"
#include "lib/repr.h"
#include "lib/sha1.h"
#include "multiplexer/recorder.h"
#include "multiplexer/server.h"
#include "mxcontrol/tasks_holder.h"
#include <boost/asio.hpp>
#include <boost/bind/bind.hpp>
#include <boost/lexical_cast.hpp>
#include <cstdio>
#include <fstream>
#include <iterator>
#include <memory>

namespace mxcontrol {
namespace {

// Writes `address` to `path` atomically (temporary file plus rename), so a
// reader never sees a partially written file.
void write_port_file(const std::string &path, const std::string &address) {
  const std::string tmp = path + ".tmp";
  {
    std::ofstream out(tmp.c_str(), std::ios::out | std::ios::trunc);
    AssertMsg(out.good(), "cannot write port file " + tmp);
    out << address << "\n";
  }
  AssertMsg(std::rename(tmp.c_str(), path.c_str()) == 0, "cannot rename port file to " + path);
}

void stop_on_signal(multiplexer::Server::pointer server, const boost::system::error_code &error, int signal_number) {
  if (error)
    return;
  MX_LOG(INFO, LOWVERBOSITY,
         TEXT("received signal " + boost::lexical_cast<std::string>(signal_number) + ", shutting down"));
  // Closing the acceptor and the connections lets io_service.run() return on
  // its own once every pending operation has completed.
  server->stop();
}

} // namespace

int StartMultiplexerServer::run() {
  using mx::repr;
  using std::string;

  string host = host_port_;
  boost::uint16_t port = 1980;

  string::size_type colonpos = host_port_.find(':');
  Assert(colonpos < host_port_.size() || colonpos == string::npos);

  if (colonpos < host_port_.size()) {
    // port specified
    Assert(host_port_[colonpos] == ':');
    string(&host_port_[0], &host_port_[colonpos]).swap(host);
    string portstring(&host_port_[0] + colonpos + 1, &host_port_[0] + host_port_.size());
    AssertMsg(portstring.find(':') == string::npos, "Invalid address spec: two colons");
    port = boost::lexical_cast<boost::uint16_t>(portstring);
  }

  // TODO support for name resolving (e.g. host = "localhost" by default)
  boost::asio::io_service io_service;
  multiplexer::Server::pointer server = multiplexer::Server::Create(io_service, host, port);
  server->clear_rules();
  server->read_rules(rules_file_);
  server->set_memory_log_every(memory_log_every_);
  {
    std::ifstream rules(rules_file_.c_str(), std::ios::binary);
    std::string rules_text((std::istreambuf_iterator<char>(rules)), std::istreambuf_iterator<char>());
    server->set_rules_sha1(mx::sha1_hex(rules_text));
  }
  server->set_recording_dir(recording_dir_);
  server->set_allow_tap(allow_tap_);
  if (!record_file_.empty()) {
    std::string error;
    if (!server->start_recording(record_file_, "", record_payload_bytes_, 0, 0, &error))
      MX_LOG(ERROR, LOWVERBOSITY, TEXT("--record: " + error));
  }
  if (!peers_file_.empty())
    server->set_peers_file(peers_file_);
  server->start();
  port = server->local_port();
  MX_LOG(INFO, LOWVERBOSITY, TEXT("starting MX server on " + host + ":" + repr(port)));
  if (!port_file_.empty())
    write_port_file(port_file_, host + ":" + repr(port));

  // SIGTERM and SIGINT shut the server down, so the process exits with 0.
  boost::asio::signal_set signals(io_service, SIGINT, SIGTERM);
  signals.async_wait(boost::bind(&stop_on_signal, server, boost::placeholders::_1, boost::placeholders::_2));
  // A bug in one connection's handler must not take the whole broker down:
  // log the exception and keep serving. Costs nothing while nothing throws.
  for (;;) {
    try {
      io_service.run();
      break;
    } catch (const std::exception &e) {
      MX_LOG(ERROR, LOWVERBOSITY, TEXT(std::string("exception escaped an io handler: ") + e.what()));
    }
  }
  MX_LOG(INFO, LOWVERBOSITY, TEXT("MX server stopped"));

  return 0;
}
}; // namespace mxcontrol

REGISTER_MXCONTROL_SUBCOMMAND(run_multiplexer, mxcontrol::StartMultiplexerServer);
