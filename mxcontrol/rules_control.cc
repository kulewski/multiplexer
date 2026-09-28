// rules: make running multiplexers read their rules file again, or ask
// which rules each has in use, over the protocol itself (RulesControl in
// Multiplexer.proto). Connects to every --multiplexer as a
// RULES_CONTROLLER, resolving a host name to all its addresses, so one
// command reaches every replica behind a name and the answers show
// whether they all run the same file. See docs/operations.md.
#include <chrono>
#include <ctime>
#include <iostream>
#include <set>

#include "lib/repr.h"
#include "lib/seconds.h"
#include "multiplexer/Multiplexer.pb.h" /* generated */
#include "multiplexer/client.h"
#include "mxcontrol/every_address.h"
#include "mxcontrol/task.h"
#include "mxcontrol/tasks_holder.h"

using multiplexer::Client;
using multiplexer::MultiplexerMessage;
using multiplexer::RulesControl;
using multiplexer::RulesStatus;
using mx::repr;

namespace mxcontrol {

namespace {

// Microseconds since the epoch as "YYYY-MM-DD HH:MM:SS UTC".
std::string when(std::uint64_t microseconds) {
  const std::time_t seconds = static_cast<std::time_t>(microseconds / 1000000);
  char text[32];
  std::strftime(text, sizeof text, "%Y-%m-%d %H:%M:%S UTC", std::gmtime(&seconds));
  return text;
}

// One status as a line: which multiplexer, what happened to the request,
// and the rules it has in use.
std::string describe(const RulesStatus& status, bool reload) {
  std::string line = "multiplexer " + repr(status.multiplexer_id()) + ": ";
  if (status.has_error()) {
    line += "error: " + status.error() + "; keeps ";
  } else if (reload) {
    line += status.reloaded() ? "reloaded; " : "unchanged; ";
  }
  line += "rules " + status.fingerprint() + " (" + repr(status.message_types()) + " message types, " +
          repr(status.peer_types()) + " peer types) from " + status.path() + ", loaded " + when(status.loaded_us());
  if (status.has_last_error() && !status.has_error()) {
    line += "; the file on disk is not in use: " + status.last_error();
  }
  return line;
}

}  // namespace

class RulesControlTask : public Task {
 public:
  virtual int run();
  virtual std::string short_description() const { return "reload or report the rules file of multiplexers"; }
  virtual std::string short_synopsis(const std::string& commandname) {
    return "<" + commandname + "-options> reload|status";
  }
  virtual void print_help(std::ostream& out) {
    out << "Ask every --multiplexer given about its rules file, over the protocol.\n"
        << "  reload  read the file again now and put it in use if it changed\n"
        << "  status  say which rules each has in use\n"
        << "A host name resolves to all its addresses, one connection each. The answer\n"
        << "names the fingerprint of the rules in use, the one the generated constants\n"
        << "carry, so the replicas can be compared.\n\n"
        << _options();
  }

 protected:
  virtual void _initialize_options(mx::options::Options& options) {
    options.add("action", &action_, "reload or status").positional("action");
    options.add("multiplexer,M", &multiplexers_,
                "multiplexer address as host:port; a name resolves to every address; may be repeated");
    options.add("timeout", &timeout_, 5.0, "seconds to wait for connections and answers");
  }

 private:
  std::string action_;
  std::vector<std::string> multiplexers_;
  float timeout_;
};

REGISTER_MXCONTROL_SUBCOMMAND(rules, mxcontrol::RulesControlTask);

// Sends the request on every connection and prints one answer per
// multiplexer; 0 when every one answered without an error.
int RulesControlTask::run() {
  if (action_ != "reload" && action_ != "status") {
    std::cerr << "action must be reload or status\n";
    return 2;
  }
  if (multiplexers_.empty()) {
    std::cerr << "give at least one --multiplexer host:port\n";
    return 2;
  }
  Client client(io_service(), multiplexer::RULES_CONTROLLER);
  const unsigned int reached = EveryAddress(client, io_service(), multiplexers_).connect(timeout_);
  const unsigned int expected = client.connections_count();
  if (!expected) {
    std::cerr << "no multiplexer reachable\n";
    return 1;
  }
  bool ok = reached == multiplexers_.size();
  if (!ok) {
    std::cerr << (multiplexers_.size() - reached) << " of " << multiplexers_.size()
              << " multiplexer address(es) could not be reached\n";
  }
  const bool reload = action_ == "reload";
  RulesControl control;
  control.set_action(reload ? RulesControl::RELOAD : RulesControl::STATUS);
  MultiplexerMessage request;
  request.set_id(client.random64());
  request.set_from(client.instance_id());
  request.set_type(multiplexer::RULES_CONTROL);
  control.SerializeToString(request.mutable_message());
  client.schedule_all(request);

  std::set<std::uint64_t> answered;
  const std::chrono::steady_clock::time_point deadline = std::chrono::steady_clock::now() + mx::from_seconds(timeout_);
  while (answered.size() < expected) {
    const float remaining = std::chrono::duration<float>(deadline - std::chrono::steady_clock::now()).count();
    std::pair<std::shared_ptr<MultiplexerMessage>, multiplexer::ConnectionWrapper> incoming;
    try {
      incoming = client.receive_message(remaining > 0 ? remaining : 0.01f);
    } catch (const Client::OperationTimedOut&) {
      break;
    } catch (const Client::NotConnected&) {
      break;
    }
    const MultiplexerMessage& mxmsg = *incoming.first;
    RulesStatus status;
    if (mxmsg.type() != multiplexer::RULES_STATUS || mxmsg.references() != request.id() ||
        !status.ParseFromString(mxmsg.message())) {
      continue;
    }
    answered.insert(status.multiplexer_id());
    std::cout << describe(status, reload) << "\n";
    if (status.has_error()) {
      ok = false;
    }
  }
  std::cout.flush();
  if (answered.size() < expected) {
    std::cerr << (expected - answered.size()) << " of " << expected << " multiplexer(s) did not answer\n";
    ok = false;
  }
  return ok ? 0 : 1;
}

}  // namespace mxcontrol
