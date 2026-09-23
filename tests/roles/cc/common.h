// Shared by every C++ test role: the Event lines the harness reads
// (multiplexer/events.proto), the options every role takes (--mx, --type,
// --name), the SIGTERM flag, and small parsers for the option formats.
// tests/README.md documents the events and options; the Python roles in
// tests/roles/py produce the same ones.
#ifndef MX_TESTS_ROLES_CC_COMMON_H_
#define MX_TESTS_ROLES_CC_COMMON_H_

#include <chrono>
#include <csignal>
#include <cstdint>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "lib/options.h"
#include "multiplexer/backend/base_multiplexer_server.h"
#include "multiplexer/client.h"
#include "multiplexer/events.pb.h"

namespace mxtestroles {

using multiplexer::Client;
using mxtesting::Event;

// An Event with its name set; fill in the fields, then emit().
Event event(const std::string& name);

// Writes `event` as one line of protocol buffer text format on stdout. Safe
// from any thread: roles report from workers and from a threaded client's
// io thread.
void emit(const Event& event);

// Sets `size` and, for short payloads, `payload`.
void set_payload(Event& event, const std::string& data);

double ms_since(const std::chrono::steady_clock::time_point& start);

// A "memory" event: the C heap in use after `after` messages.
Event memory_event(long after);

// SIGTERM / SIGINT set this; loop-driven roles check it between messages.
extern volatile std::sig_atomic_t stop_requested;
void install_signal_handlers();

// --mx (repeatable), --type and --name, and a Client connected to every --mx.
struct CommonOptions {
  std::vector<std::string> mx;
  unsigned type;
  std::string name;

  void add(mx::options::Options& options);
  std::unique_ptr<Client> connect() const;
  // The --mx addresses as (host, port) pairs, for a backend that connects itself.
  multiplexer::backend::MultiplexerAddresses addresses() const;
  // The "connected" event with instance_id, connections and name filled in.
  Event connected_event(Client& client) const;
};

// "201=203" pairs, as --serves takes them.
std::map<std::uint32_t, std::uint32_t> kv_ints(const std::vector<std::string>& items);
// "201:payload" pairs, as --query and --send take them.
std::vector<std::pair<std::uint32_t, std::string>> typed_payloads(const std::vector<std::string>& items);
std::string replace_all(std::string text, const std::string& from, const std::string& to);
std::string upper(std::string text);

// Runs `callable`; if it throws a client exception, emits `error_event` with
// `kind` set to the exception's name, the same names the Python roles report.
// --drain-routing's comma-separated flag names as a Routing.
inline multiplexer::Routing parse_drain_routing(const std::string& flags) {
  multiplexer::Routing routing;
  routing.set_any(false);
  routing.set_all(false);
  std::string::size_type start = 0;
  while (start <= flags.size()) {
    std::string::size_type comma = flags.find(',', start);
    const std::string flag = flags.substr(start, comma == std::string::npos ? std::string::npos : comma - start);
    if (flag == "any") {
      routing.set_any(true);
    } else if (flag == "all") {
      routing.set_all(true);
    } else if (flag == "last_resort") {
      routing.set_last_resort(true);
    } else if (!flag.empty()) {
      throw std::invalid_argument("unknown routing flag: " + flag);
    }
    if (comma == std::string::npos) {
      break;
    }
    start = comma + 1;
  }
  return routing;
}

template <typename Callable>
void report_client_errors(Callable callable, Event error_event) {
  try {
    callable();
    return;
  } catch (Client::NotConnected&) {
    error_event.set_kind("NotConnected");
  } catch (Client::OperationTimedOut&) {
    error_event.set_kind("OperationTimedOut");
  } catch (Client::OperationFailed&) {
    error_event.set_kind("OperationFailed");
  } catch (std::exception& error) {
    error_event.set_kind(typeid(error).name());
  }
  emit(error_event);
}

}  // namespace mxtestroles

#endif  // MX_TESTS_ROLES_CC_COMMON_H_
