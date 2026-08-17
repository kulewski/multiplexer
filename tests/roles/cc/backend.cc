// The `backend` role: a backend that serves request types, with knobs for
// the failure scenarios. See tests/README.md for options and events.
#include <chrono>
#include <cstdlib>
#include <stdexcept>
#include <thread>
#include <unistd.h>

#include "multiplexer/backend/base_multiplexer_server.h"
#include "mxcontrol/task.h"
#include "mxcontrol/tasks_holder.h"
#include "tests/roles/cc/common.h"

namespace mxtestroles {

using multiplexer::MultiplexerMessage;
using mx::util::kwargs::Kwargs;

// How the role is asked to leave and what it does then; see the module
// comment of the Python role for the options.
struct LeaveOptions {
  double drain_seconds = 0.0;
  std::string drain_file;
  int drain_min_handled = 0;
  bool exit_on_exception = false;
};

// The backend that answers requests. --serves maps each request type to the
// reply type; --behaviour says what to do with the payload: upper (the
// default), echo, drop (no reply), raise (throw from the handler), sleep:MS
// (wait, then upper-case). --crash-after N exits with status 3 after N
// requests, mid-run, for the failover scenarios. Events: request,
// unexpected, crash, draining, stopped.
class BackendServer : public multiplexer::backend::BaseMultiplexerServer {
public:
  BackendServer(Client *client, unsigned type, const std::map<boost::uint32_t, boost::uint32_t> &serves,
                const std::string &behaviour, int crash_after, int memory_every, const LeaveOptions &leave)
      : BaseMultiplexerServer(client, type), serves_(serves), behaviour_(behaviour), crash_after_(crash_after),
        memory_every_(memory_every), leave_(leave), handled_(0) {}

  int handled() const { return handled_; }

protected:
  // Report the request, then answer, drop, raise or crash as configured.
  void handle_message(MultiplexerMessage &mxmsg) override {
    ++handled_;
    if (memory_every_ && handled_ % memory_every_ == 0)
      emit(memory_event(handled_));
    Event request = event("request");
    request.set_type(mxmsg.type());
    request.set_id(mxmsg.id());
    request.set_from_(mxmsg.from());
    request.set_size(mxmsg.message().size());
    emit(request);

    std::map<boost::uint32_t, boost::uint32_t>::const_iterator served = serves_.find(mxmsg.type());
    if (served == serves_.end()) {
      Event unexpected = event("unexpected");
      unexpected.set_type(mxmsg.type());
      unexpected.set_id(mxmsg.id());
      emit(unexpected);
      no_response();
      return;
    }
    if (behaviour_ == "drop") {
      no_response();
    } else if (behaviour_ == "raise") {
      throw std::runtime_error("handler failed on purpose");
    } else {
      if (behaviour_.compare(0, 6, "sleep:") == 0)
        std::this_thread::sleep_for(std::chrono::milliseconds(boost::lexical_cast<int>(behaviour_.substr(6))));
      std::string payload = behaviour_ == "upper" ? upper(mxmsg.message()) : mxmsg.message();
      send_message(Kwargs().set("message", payload).set("type", static_cast<boost::uint32_t>(served->second)));
    }
    if (crash_after_ && handled_ >= crash_after_) {
      Event crash = event("crash");
      crash.set_handled(handled_);
      emit(crash);
      std::_Exit(3);
    }
  }

  // Notice a request to leave, from SIGTERM or the drain file: stop at
  // once, or start draining when a drain was configured.
  void periodic_task() override {
    if (draining())
      return;
    bool asked = stop_requested || (!leave_.drain_file.empty() && access(leave_.drain_file.c_str(), F_OK) == 0);
    if (!asked)
      return;
    if (leave_.drain_seconds <= 0 && leave_.drain_min_handled <= 0) {
      working = false;
      return;
    }
    start_draining(); // keep serving, but no longer answer searches
    Event draining_event = event("draining");
    draining_event.set_drain_seconds(leave_.drain_seconds);
    emit(draining_event);
  }

  // The drain period is over and at least --drain-min-handled requests were served.
  bool drained() const override { return BaseMultiplexerServer::drained() && handled_ >= leave_.drain_min_handled; }

  // Keep serving, unless --exit-on-exception.
  bool on_handler_exception(const std::exception &) override { return !leave_.exit_on_exception; }

private:
  std::map<boost::uint32_t, boost::uint32_t> serves_;
  std::string behaviour_;
  int crash_after_;
  int memory_every_;
  LeaveOptions leave_;
  int handled_;
};

// The `backend` subcommand: runs a BackendServer until asked to leave.
class BackendRole : public mxcontrol::Task {
public:
  virtual std::string short_description() const { return "serve request types until stopped"; }
  virtual int run() {
    install_signal_handlers();
    std::unique_ptr<Client> client = common_.connect();
    BackendServer server(client.get(), common_.type, kv_ints(serves_), behaviour_, crash_after_, memory_every_, leave_);
    emit(common_.connected_event(*client));
    try {
      server.serve_forever(0.25f, static_cast<float>(leave_.drain_seconds));
    } catch (std::exception &error) { // the handler's exception, let through by on_handler_exception
      Event failed = event("handler_exception");
      failed.set_kind(error.what());
      failed.set_handled(server.handled());
      emit(failed);
      return 4;
    }
    Event stopped = event("stopped");
    stopped.set_handled(server.handled());
    emit(stopped);
    return 0;
  }

protected:
  virtual void _initialize_options_description(po::options_description &options) {
    common_.add(options);
    options.add_options()("serves", po::value(&serves_)->composing(), "REQUEST_TYPE=RESPONSE_TYPE, repeatable")(
        "behaviour", po::value(&behaviour_)->default_value("upper"), "upper | echo | drop | raise | sleep:MS")(
        "crash-after", po::value(&crash_after_)->default_value(0), "exit(3) after N handled requests")(
        "memory-every", po::value(&memory_every_)->default_value(0), "emit a memory event every N requests")(
        "drain-seconds", po::value(&leave_.drain_seconds)->default_value(0.0),
        "when asked to leave, decline searches but keep serving this long, then exit")(
        "drain-file", po::value(&leave_.drain_file)->default_value(""),
        "a file whose appearance asks the backend to leave")("drain-min-handled",
                                                             po::value(&leave_.drain_min_handled)->default_value(0),
                                                             "do not leave before N requests were served")(
        "exit-on-exception", po::bool_switch(&leave_.exit_on_exception), "a handler exception ends the process (4)");
  }

private:
  CommonOptions common_;
  std::vector<std::string> serves_;
  std::string behaviour_;
  int crash_after_;
  int memory_every_;
  LeaveOptions leave_;
};

REGISTER_MXCONTROL_SUBCOMMAND(backend, BackendRole);

} // namespace mxtestroles
