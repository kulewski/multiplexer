// Shared by every C++ test role: the Event lines the harness reads
// (multiplexer/testing/events.proto), the options every role takes (--mx, --type,
// --name), the SIGTERM flag, and small parsers for the option formats.
// tests/README.md documents the events and options; the Python roles in
// tests/roles/py produce the same ones.
#ifndef MX_TESTS_ROLES_CC_COMMON_H_
#define MX_TESTS_ROLES_CC_COMMON_H_

#include <chrono>
#include <csignal>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <boost/cstdint.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/program_options.hpp>

#include "multiplexer/client.h"
#include "multiplexer/testing/events.pb.h"

namespace mxtestroles {

namespace po = boost::program_options;
using multiplexer::Client;
using mxtesting::Event;

// An Event with its name set; fill in the fields, then emit().
Event event(const std::string &name);

// Writes `event` as one line of protocol buffer text format on stdout. Safe
// from any thread: roles report from workers and from a threaded client's
// io thread.
void emit(const Event &event);

// Sets `size` and, for short payloads, `payload`.
void set_payload(Event &event, const std::string &data);

double ms_since(const std::chrono::steady_clock::time_point &start);

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

  void add(po::options_description &options);
  std::unique_ptr<Client> connect() const;
  // The "connected" event with instance_id, connections and name filled in.
  Event connected_event(Client &client) const;
};

// "201=203" pairs, as --serves takes them.
std::map<boost::uint32_t, boost::uint32_t> kv_ints(const std::vector<std::string> &items);
// "201:payload" pairs, as --query and --send take them.
std::vector<std::pair<boost::uint32_t, std::string>> typed_payloads(const std::vector<std::string> &items);
std::string replace_all(std::string text, const std::string &from, const std::string &to);
std::string upper(std::string text);

// Runs `callable`; if it throws a client exception, emits `error_event` with
// `kind` set to the exception's name, the same names the Python roles report.
template <typename Callable> void report_client_errors(Callable callable, Event error_event) {
  try {
    callable();
    return;
  } catch (Client::NotConnected &) {
    error_event.set_kind("NotConnected");
  } catch (Client::OperationTimedOut &) {
    error_event.set_kind("OperationTimedOut");
  } catch (Client::OperationFailed &) {
    error_event.set_kind("OperationFailed");
  } catch (std::exception &error) {
    error_event.set_kind(typeid(error).name());
  }
  emit(error_event);
}

} // namespace mxtestroles

#endif // MX_TESTS_ROLES_CC_COMMON_H_
