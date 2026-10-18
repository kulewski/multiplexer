// run_multiplexer: creates the Server, reads the rules, binds, optionally
// starts the monitor and writes the port file, and runs the io_service
// until a signal stops it; SIGHUP reloads the rules on the way. The first
// SIGTERM or SIGINT drains, a second one stops at once; SIGPIPE and
// SIGXFSZ are ignored.
#include "mxcontrol/start_multiplexer_server.h"

#include <signal.h>  // sigaction

#include <asio.hpp>
#include <cerrno>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <memory>
#include <optional>
#include <stdexcept>
#include <system_error>
#include <tuple>
#include <utility>

#include "lib/memory.h"
#include "lib/repr.h"
#include "multiplexer/endpoint.h"
#include "multiplexer/monitor.h"
#include "multiplexer/recorder.h"
#include "multiplexer/server.h"
#include "mxcontrol/tasks_holder.h"

namespace mxcontrol {
namespace {

// The port of an --address given without one.
constexpr std::uint16_t DEFAULT_PORT = 1980;

// `text`, the value of --`option`, as an IP address and a port to listen
// on, `default_port` when it has none, refused as a malformed line when it
// is not one (multiplexer/endpoint.h). An acceptor binds an address: a
// name would have to pick one of those it resolves to, and the socket
// would listen on that one only.
std::pair<std::string, std::uint16_t> listening_endpoint(const std::string& option, const std::string& text,
                                                         std::optional<std::uint16_t> default_port) {
  std::pair<std::string, std::uint16_t> endpoint;
  try {
    endpoint = multiplexer::parse_endpoint(text, default_port);
  } catch (const std::invalid_argument& error) {
    throw mx::options::Error("--" + option + ": " + error.what());
  }
  asio::error_code not_an_address;
  asio::ip::make_address(endpoint.first, not_an_address);
  if (not_an_address) {
    throw mx::options::Error("--" + option + ": '" + text +
                             "': not an IP address; 0.0.0.0 listens on every IPv4 one, [::] on every one");
  }
  return endpoint;
}

// Writes `address` to `path` atomically (temporary file plus rename), so a
// reader never sees a partially written file. A write that failed, a full
// disk say, ends the start, as a file that cannot be opened does, the
// temporary file removed and `path` left as it was.
void write_port_file(const std::string& path, const std::string& address) {
  const std::string tmp = path + ".tmp";
  {
    std::ofstream out(tmp.c_str(), std::ios::out | std::ios::trunc);
    AssertMsg(out.good(), "cannot write port file " + tmp);
    out << address << "\n";
    out.close();
    if (out.fail()) {
      const std::string why = std::strerror(errno);
      std::remove(tmp.c_str());
      AssertMsg(false, "cannot write port file " + tmp + ": " + why + "; " + path + " left as it was");
    }
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
  std::tie(host_, port_) = listening_endpoint("address", host_port_, DEFAULT_PORT);
  if (!monitor_host_port_.empty()) {
    std::tie(monitor_host_, monitor_port_) = listening_endpoint("monitor-address", monitor_host_port_, std::nullopt);
  } else if (!monitor_port_file_.empty()) {
    throw mx::options::Error("--monitor-port-file: there is no monitor without --monitor-address");
  }
  // --threads, else MX_SERVER_THREADS, else one loop.
  if (!threads_) {
    threads_ = 1;
    const char* environment = std::getenv("MX_SERVER_THREADS");
    if (environment && *environment) {
      char* end = nullptr;
      const unsigned long threads = std::strtoul(environment, &end, 10);
      if (*end || !threads || threads > MAX_THREADS) {
        throw mx::options::Error(std::string("MX_SERVER_THREADS: '") + environment + "': not a number from 1 to " +
                                 mx::repr(MAX_THREADS));
      }
      threads_ = static_cast<unsigned int>(threads);
    }
  }
  if (threads_ > MAX_THREADS) {
    throw mx::options::Error("--threads: " + mx::repr(threads_) + " is more than " + mx::repr(MAX_THREADS));
  }
}

int StartMultiplexerServer::run() {
  // A log reader that goes away, the other end of --logging-fd or of
  // stderr, must not take the broker down with it: its writes fail with
  // EPIPE instead, and the binary stream is dropped (lib/logging). The other
  // commands keep the default, so that `mxcontrol ... | head` ends quietly.
  std::signal(SIGPIPE, SIG_IGN);
  // Nor a file that reaches the process's size limit (RLIMIT_FSIZE: ulimit
  // -f, systemd's LimitFSIZE=), a recording or a log: its write fails with
  // EFBIG, which the recorder and the logger handle as they do a full disk,
  // where the signal's default action ended the process.
  std::signal(SIGXFSZ, SIG_IGN);
  // glibc's malloc keeps the memory of large frames for the next ones
  // (lib/memory.h), before anything is allocated; only a refusal is worth
  // a line.
  if (mx::keep_large_buffers() == mx::KeptBuffers::REFUSED) {
    MX_LOG(WARNING, LOWVERBOSITY,
           TEXT("glibc refused to keep large buffers in the heap: a frame over 128 KiB may cost page faults until "
                "glibc's own thresholds settle"));
  }

  asio::io_service io_service;
  // An address that cannot be bound, one in use or not this host's, ends
  // the start naming the option, as the monitor's does below; the
  // exception's own words, `bind: Address already in use`, said neither
  // which option nor which address.
  multiplexer::Server::pointer server;
  try {
    server = multiplexer::Server::Create(io_service, host_, port_);
  } catch (const std::system_error& error) {
    std::cerr << "--address " << multiplexer::format_endpoint(host_, port_) << ": " << error.what() << "\n";
    return 1;
  }
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
  server->set_threads(threads_);
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
  // The monitor before the server starts, so that an address it cannot
  // bind ends the start as the multiplexer's own does; a check posted
  // meanwhile waits in the loops' queues until they run.
  std::unique_ptr<multiplexer::Monitor> monitor;
  if (!monitor_host_port_.empty()) {
    try {
      monitor.reset(new multiplexer::Monitor(
          server, asio::ip::tcp::endpoint(asio::ip::make_address(monitor_host_), monitor_port_)));
    } catch (const std::system_error& error) {
      std::cerr << "--monitor-address " << multiplexer::format_endpoint(monitor_host_, monitor_port_) << ": "
                << error.what() << "\n";
      return 1;
    }
  }
  server->start();
  const std::string bound = multiplexer::format_endpoint(host_, server->local_port());
  MX_LOG(INFO, LOWVERBOSITY, TEXT("starting MX server on " + bound));
  if (monitor) {
    MX_LOG(INFO, LOWVERBOSITY,
           TEXT("serving /healthz, /readyz and /metrics on http://" +
                multiplexer::format_endpoint(monitor_host_, monitor->port())));
  }
  // The monitor's file first: once the multiplexer's says it listens, a
  // reader finds the monitor's too.
  if (monitor && !monitor_port_file_.empty()) {
    write_port_file(monitor_port_file_, multiplexer::format_endpoint(monitor_host_, monitor->port()));
  }
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
  if (monitor) {
    monitor->stop();  // before the loops finish, which a check under way would wait for in vain
  }
  server->finish_loops();  // the other loops, on several: every connection has ended by now
  MX_LOG(INFO, LOWVERBOSITY, TEXT("MX server stopped"));

  return 0;
}
};  // namespace mxcontrol

REGISTER_MXCONTROL_SUBCOMMAND(run_multiplexer, mxcontrol::StartMultiplexerServer);
