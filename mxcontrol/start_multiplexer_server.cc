// run_multiplexer: creates the Server, reads the rules, binds, optionally
// writes the port file, and runs the io_service until a signal stops it;
// SIGHUP reloads the rules on the way. The first SIGTERM or SIGINT drains,
// a second one stops at once; SIGPIPE is ignored.
#include "mxcontrol/start_multiplexer_server.h"

#include <signal.h>  // sigaction

#include <asio.hpp>
#include <csignal>
#include <cstdio>
#include <fstream>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <tuple>

#include "lib/repr.h"
#include "multiplexer/endpoint.h"
#include "multiplexer/recorder.h"
#include "multiplexer/server.h"
#include "mxcontrol/tasks_holder.h"

namespace mxcontrol {
namespace {

// The port of an --address given without one.
constexpr std::uint16_t DEFAULT_PORT = 1980;

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

// SIGTERM or SIGINT: the server stops, sending what it holds within
// `drain_seconds`, while the next such signal stops it at once. Once every
// connection has ended, giving up the waits for signals lets
// io_service.run() return on its own.
void stop_on_signal(multiplexer::Server::pointer server, asio::signal_set& signals, asio::signal_set& reload,
                    float drain_seconds, const asio::error_code& error, int signal_number) {
  if (error) {
    return;
  }
  if (server->stopped()) {
    MX_LOG(INFO, LOWVERBOSITY, TEXT("received signal " + mx::repr(signal_number) + " again, stopping at once"));
    server->stop();
    return;
  }
  MX_LOG(INFO, LOWVERBOSITY, TEXT("received signal " + mx::repr(signal_number) + ", shutting down"));
  signals.async_wait([server, &signals, &reload](const asio::error_code& next_error, int next_signal) {
    stop_on_signal(server, signals, reload, 0, next_error, next_signal);
  });
  server->stop(drain_seconds, [&signals, &reload] {
    asio::error_code ignored;
    signals.cancel(ignored);
    reload.cancel(ignored);
  });
}

// SIGHUP: the rules file is read again now, the Unix way to say a
// configuration changed (systemd's ExecReload); then wait for the next
// one. A signal delivered as the server stops does nothing, and does not
// re-arm the wait, which would keep io_service.run() from returning.
void reload_on_signal(multiplexer::Server::pointer server, asio::signal_set& signals, const asio::error_code& error) {
  if (error || server->stopped()) {
    return;
  }
  // The next wait first: nothing the reload does can leave SIGHUP unheard.
  signals.async_wait(
      [server, &signals](const asio::error_code& next_error, int) { reload_on_signal(server, signals, next_error); });
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
}

// Adds `signal_number` to `signals`, its handler with SA_RESTART, so that a
// signal arriving during a call that blocks, a write to stderr say,
// restarts it rather than failing it with EINTR. Set with sigaction() on
// the handler asio installed: asio's own signal_set::flags::restart is
// newer than the asio Debian 12 and Ubuntu 22.04 ship.
void add_restarting(asio::signal_set& signals, int signal_number) {
  signals.add(signal_number);
  struct sigaction action;
  if (::sigaction(signal_number, nullptr, &action) == 0) {
    action.sa_flags |= SA_RESTART;
    ::sigaction(signal_number, &action, nullptr);
  }
}

}  // namespace

void StartMultiplexerServer::parse_options(std::vector<std::string>& args) {
  Task::parse_options(args);
  const std::string refused = multiplexer::Server::rules_check_interval_refused(rules_check_interval_);
  if (!refused.empty()) {
    throw mx::options::Error("--rules-check-interval: " + refused);
  }
  try {
    std::tie(host_, port_) = multiplexer::parse_endpoint(host_port_, DEFAULT_PORT);
  } catch (const std::invalid_argument& error) {
    throw mx::options::Error(std::string("--address: ") + error.what());
  }
  // The acceptor binds an address: a name would have to pick one of those
  // it resolves to, and the multiplexer would listen on that one only.
  asio::error_code not_an_address;
  asio::ip::make_address(host_, not_an_address);
  if (not_an_address) {
    throw mx::options::Error("--address: '" + host_port_ +
                             "': not an IP address; 0.0.0.0 listens on every IPv4 one, [::] on every one");
  }
}

int StartMultiplexerServer::run() {
  // A log reader that goes away, the other end of --logging-fd or of
  // stderr, must not take the broker down with it: its writes fail with
  // EPIPE instead, and the binary stream is dropped (lib/logging). The other
  // commands keep the default, so that `mxcontrol ... | head` ends quietly.
  std::signal(SIGPIPE, SIG_IGN);

  asio::io_service io_service;
  multiplexer::Server::pointer server = multiplexer::Server::Create(io_service, host_, port_);
  // The signals first, before anything a supervisor might react to: SIGTERM
  // and SIGINT shut the server down, so the process exits with 0; SIGHUP
  // reloads the rules, and must not end the process from the moment it
  // exists (asio queues one that lands before run()). Each with SA_RESTART
  // (add_restarting).
  asio::signal_set reload(io_service);
  add_restarting(reload, SIGHUP);
  reload.async_wait([server, &reload](const asio::error_code& error, int) { reload_on_signal(server, reload, error); });
  asio::signal_set signals(io_service);
  add_restarting(signals, SIGINT);
  add_restarting(signals, SIGTERM);
  const float drain_seconds = drain_seconds_ > 0 ? drain_seconds_ : 0;
  signals.async_wait([server, &signals, &reload, drain_seconds](const asio::error_code& error, int signal_number) {
    stop_on_signal(server, signals, reload, drain_seconds, error, signal_number);
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
  const std::string bound = multiplexer::format_endpoint(host_, server->local_port());
  MX_LOG(INFO, LOWVERBOSITY, TEXT("starting MX server on " + bound));
  if (!port_file_.empty()) {
    write_port_file(port_file_, bound);
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
