// run_multiplexer: creates the Server, reads the rules, binds, optionally
// writes the port file, and runs the io_service until a signal stops it;
// SIGHUP reloads the rules on the way.
#include "mxcontrol/start_multiplexer_server.h"

#include <asio.hpp>
#include <cstdio>
#include <fstream>
#include <iostream>
#include <memory>

#include "lib/repr.h"
#include "multiplexer/recorder.h"
#include "multiplexer/server.h"
#include "mxcontrol/tasks_holder.h"

namespace mxcontrol {
namespace {

// Writes `address` to `path` atomically (temporary file plus rename), so a
// reader never sees a partially written file.
void write_port_file(const std::string& path, const std::string& address) {
  const std::string tmp = path + ".tmp";
  {
    std::ofstream out(tmp.c_str(), std::ios::out | std::ios::trunc);
    AssertMsg(out.good(), "cannot write port file " + tmp);
    out << address << "\n";
  }
  AssertMsg(std::rename(tmp.c_str(), path.c_str()) == 0, "cannot rename port file to " + path);
}

void stop_on_signal(multiplexer::Server::pointer server, asio::signal_set& reload, const asio::error_code& error,
                    int signal_number) {
  if (error) {
    return;
  }
  MX_LOG(INFO, LOWVERBOSITY, TEXT("received signal " + mx::repr(signal_number) + ", shutting down"));
  // Closing the acceptor and the connections, and giving up the wait for
  // SIGHUP, lets io_service.run() return on its own once every pending
  // operation has completed.
  server->stop();
  reload.cancel();
}

// SIGHUP: the rules file is read again now, the Unix way to say a
// configuration changed (systemd's ExecReload); then wait for the next
// one. A signal delivered as the server stops does nothing, and does not
// re-arm the wait, which would keep io_service.run() from returning.
void reload_on_signal(multiplexer::Server::pointer server, asio::signal_set& signals, const asio::error_code& error) {
  if (error || server->stopped()) {
    return;
  }
  std::string reload_error;
  switch (server->load_rules(&reload_error)) {
    case multiplexer::Server::RulesLoad::LOADED:
      break;  // said by load_rules
    case multiplexer::Server::RulesLoad::UNCHANGED:
      MX_LOG(INFO, LOWVERBOSITY,
             TEXT("received SIGHUP; the rules file is the rules in use (" + server->rules_fingerprint() + ")"));
      break;
    case multiplexer::Server::RulesLoad::FAILED:
      MX_LOG(ERROR, LOWVERBOSITY, TEXT("received SIGHUP; the rules file is not in use: " + reload_error));
      break;
  }
  signals.async_wait(
      [server, &signals](const asio::error_code& next_error, int) { reload_on_signal(server, signals, next_error); });
}

}  // namespace

int StartMultiplexerServer::run() {
  using mx::repr;
  using std::string;

  string host = host_port_;
  std::uint16_t port = 1980;

  string::size_type colonpos = host_port_.find(':');
  Assert(colonpos < host_port_.size() || colonpos == string::npos);

  if (colonpos < host_port_.size()) {
    // port specified
    Assert(host_port_[colonpos] == ':');
    string(&host_port_[0], &host_port_[colonpos]).swap(host);
    string portstring(&host_port_[0] + colonpos + 1, &host_port_[0] + host_port_.size());
    AssertMsg(portstring.find(':') == string::npos, "Invalid address spec: two colons");
    port = mx::from_string<std::uint16_t>(portstring);
  }

  // TODO support for name resolving (e.g. host = "localhost" by default)
  asio::io_service io_service;
  multiplexer::Server::pointer server = multiplexer::Server::Create(io_service, host, port);
  // The signals first, before anything a supervisor might react to: SIGTERM
  // and SIGINT shut the server down, so the process exits with 0; SIGHUP
  // reloads the rules, and must not end the process from the moment it
  // exists (asio queues one that lands before run()).
  asio::signal_set reload(io_service, SIGHUP);
  reload.async_wait([server, &reload](const asio::error_code& error, int) { reload_on_signal(server, reload, error); });
  asio::signal_set signals(io_service, SIGINT, SIGTERM);
  signals.async_wait([server, &reload](const asio::error_code& error, int signal_number) {
    stop_on_signal(server, reload, error, signal_number);
  });
  server->set_rules_file(rules_file_);
  {
    // At start a bad file is fatal: better no multiplexer than one that
    // routes nothing. Later loads keep the rules in use instead.
    std::string error;
    if (server->load_rules(&error) != multiplexer::Server::RulesLoad::LOADED) {
      std::cerr << error << "\n";
      return 1;
    }
  }
  server->set_rules_check_interval(rules_check_interval_);
  server->set_memory_log_every(memory_log_every_);
  server->set_recording_dir(recording_dir_);
  server->set_allow_tap(allow_tap_);
  if (!record_file_.empty()) {
    std::string error;
    if (!server->start_recording(record_file_, "", record_payload_bytes_, 0, 0, &error)) {
      MX_LOG(ERROR, LOWVERBOSITY, TEXT("--record: " + error));
    }
  }
  if (!peers_file_.empty()) {
    server->set_peers_file(peers_file_);
  }
  server->start();
  port = server->local_port();
  MX_LOG(INFO, LOWVERBOSITY, TEXT("starting MX server on " + host + ":" + repr(port)));
  if (!port_file_.empty()) {
    write_port_file(port_file_, host + ":" + repr(port));
  }

  // A bug in one connection's handler must not take the whole broker down:
  // log the exception and keep serving. Costs nothing while nothing throws.
  for (;;) {
    try {
      io_service.run();
      break;
    } catch (const std::exception& e) {
      MX_LOG(ERROR, LOWVERBOSITY, TEXT(std::string("exception escaped an io handler: ") + e.what()));
    }
  }
  MX_LOG(INFO, LOWVERBOSITY, TEXT("MX server stopped"));

  return 0;
}
};  // namespace mxcontrol

REGISTER_MXCONTROL_SUBCOMMAND(run_multiplexer, mxcontrol::StartMultiplexerServer);
