// The `backend` role: a backend that serves request types, with knobs for
// the failure scenarios. See tests/README.md for options and events.
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <stdexcept>
#include <thread>

#include "lib/repr.h"
#include "multiplexer/backend/base_multiplexer_server.h"
#include "multiplexer/backend/base_threaded_multiplexer_server.h"
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
  std::string drain_routing;  // the Routing flags kept on while draining: any,all,last_resort
  bool exit_on_exception = false;
};

// What both backends do about leaving, from periodic_task(): notice the
// request, drain or stop, report the multiplexers' acknowledgement of the
// drain routing once. `draining`, `start`, `acknowledged` and `stop` are
// the server's; returns nothing, emits `draining` and `acked`.
class Leaving {
 public:
  explicit Leaving(const LeaveOptions& options) : options_(options) {}

  template <typename IsDraining, typename Start, typename Acknowledged, typename Stop>
  void periodic(IsDraining draining, Start start, Acknowledged acknowledged, Stop stop) {
    if (draining()) {
      if (!acked_ && acknowledged()) {
        acked_ = true;
        Event acked = event("acked");
        acked.set_ms(std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - started_).count());
        emit(acked);
      }
      return;
    }
    bool asked = stop_requested || (!options_.drain_file.empty() && access(options_.drain_file.c_str(), F_OK) == 0);
    if (!asked) {
      return;
    }
    if (options_.drain_seconds <= 0 && options_.drain_min_handled <= 0) {
      stop();
      return;
    }
    started_ = std::chrono::steady_clock::now();
    start();  // the multiplexers route it nothing new; keep serving what arrives
    Event draining_event = event("draining");
    draining_event.set_drain_seconds(options_.drain_seconds);
    emit(draining_event);
  }

 private:
  LeaveOptions options_;
  bool acked_ = false;
  std::chrono::steady_clock::time_point started_;
};

// What a request gets: the reply, or nothing, or an exception, as
// --behaviour says; `handled` is the count so far, for --crash-after.
struct Behaviour {
  std::map<std::uint32_t, std::uint32_t> serves;
  std::string behaviour;
  int crash_after = 0;
  int memory_every = 0;

  // Reports the request; returns the reply type and payload, or no type
  // for a request that gets no reply; throws for "raise"; exits for a crash.
  template <typename Reply, typename NoResponse>
  void handle(const MultiplexerMessage& mxmsg, int handled, Reply reply, NoResponse no_response) {
    if (memory_every && handled % memory_every == 0) {
      emit(memory_event(handled));
    }
    Event request = event("request");
    request.set_type(mxmsg.type());
    request.set_id(mxmsg.id());
    request.set_sender(mxmsg.sender());
    request.set_size(mxmsg.message().size());
    emit(request);

    std::map<std::uint32_t, std::uint32_t>::const_iterator served = serves.find(mxmsg.type());
    if (served == serves.end()) {
      Event unexpected = event("unexpected");
      unexpected.set_type(mxmsg.type());
      unexpected.set_id(mxmsg.id());
      emit(unexpected);
      no_response();
      return;
    }
    if (behaviour == "drop") {
      no_response();
    } else if (behaviour == "raise") {
      throw std::runtime_error("handler failed on purpose");
    } else {
      if (behaviour.compare(0, 6, "sleep:") == 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(mx::from_string<int>(behaviour.substr(6))));
      }
      std::string payload = behaviour == "upper" ? upper(mxmsg.message()) : mxmsg.message();
      reply(payload, static_cast<std::uint32_t>(served->second));
    }
    if (crash_after && handled >= crash_after) {
      Event crash = event("crash");
      crash.set_handled(handled);
      emit(crash);
      std::_Exit(3);
    }
  }
};

// The backend that answers requests. --serves maps each request type to the
// reply type; --behaviour says what to do with the payload: upper (the
// default), echo, drop (no reply), raise (throw from the handler), sleep:MS
// (wait, then upper-case). --crash-after N exits with status 3 after N
// requests, mid-run, for the failover scenarios. Events: request,
// unexpected, crash, draining, acked, stopped.
class BackendServer : public multiplexer::backend::BaseMultiplexerServer {
 public:
  BackendServer(Client* client, unsigned type, const Behaviour& behaviour, const LeaveOptions& leave)
      : BaseMultiplexerServer(client, type), behaviour_(behaviour), leave_(leave), leaving_(leave), handled_(0) {
    set_drain_routing(parse_drain_routing(leave.drain_routing));
  }

  int handled() const { return handled_; }

 protected:
  void handle_message(MultiplexerMessage& mxmsg) override {
    ++handled_;
    behaviour_.handle(
        mxmsg, handled_,
        [this](const std::string& payload, std::uint32_t type) {
          send_message(Kwargs().set("message", payload).set("type", type));
        },
        [this] { no_response(); });
  }

  void periodic_task() override {
    leaving_.periodic([this] { return draining(); }, [this] { start_draining(); },
                      [this] { return conn->routing_acknowledged(); }, [this] { working = false; });
  }

  // The drain is over and at least --drain-min-handled requests were served.
  bool drained() const override { return BaseMultiplexerServer::drained() && handled_ >= leave_.drain_min_handled; }

  bool on_handler_exception(const std::exception&) override { return !leave_.exit_on_exception; }

 private:
  Behaviour behaviour_;
  LeaveOptions leave_;
  Leaving leaving_;
  int handled_;
};

// The same backend on the threaded class (--threaded): the handler runs on
// a worker thread and answers through the request.
class ThreadedBackendServer : public multiplexer::backend::BaseThreadedMultiplexerServer {
 public:
  ThreadedBackendServer(const multiplexer::backend::MultiplexerAddresses& addresses, unsigned type,
                        const Behaviour& behaviour, const LeaveOptions& leave,
                        const multiplexer::backend::ThreadedServerOptions& options)
      : BaseThreadedMultiplexerServer(addresses, type, options),
        behaviour_(behaviour),
        leave_(leave),
        leaving_(leave) {}

  int handled() const { return handled_.load(); }

 protected:
  void handle_message(const multiplexer::backend::RequestPtr& request) override {
    const int handled = ++handled_;
    behaviour_.handle(
        request->mxmsg(), handled,
        [&request](const std::string& payload, std::uint32_t type) { request->reply(payload, type); },
        [&request] { request->no_response(); });
  }

  void periodic_task() override {
    leaving_.periodic([this] { return draining(); }, [this] { start_draining(); },
                      [this] { return client().routing_acknowledged(); }, [this] { stop(); });
  }

  bool drained() const override {
    return BaseThreadedMultiplexerServer::drained() && handled_.load() >= leave_.drain_min_handled;
  }

  bool on_handler_exception(const std::exception&) override { return !leave_.exit_on_exception; }

 private:
  Behaviour behaviour_;
  LeaveOptions leave_;
  Leaving leaving_;
  std::atomic<int> handled_{0};
};

// The `backend` subcommand: runs a BackendServer until asked to leave.
class BackendRole : public mxcontrol::Task {
 public:
  virtual std::string short_description() const { return "serve request types until stopped"; }
  virtual int run() {
    install_signal_handlers();
    Behaviour behaviour;
    behaviour.serves = kv_ints(serves_);
    behaviour.behaviour = behaviour_;
    behaviour.crash_after = crash_after_;
    behaviour.memory_every = memory_every_;
    if (threaded_) {
      multiplexer::backend::ThreadedServerOptions options;
      options.drain_routing = parse_drain_routing(leave_.drain_routing);
      ThreadedBackendServer server(common_.addresses(), common_.type, behaviour, leave_, options);
      server.connect();
      Event connected = event("connected");
      connected.set_instance_id(server.instance_id());
      connected.set_connections(server.client().connections_count());
      connected.set_name(common_.name);
      emit(connected);
      return serve(server);
    }
    std::unique_ptr<Client> client = common_.connect();
    BackendServer server(client.get(), common_.type, behaviour, leave_);
    emit(common_.connected_event(*client));
    return serve(server);
  }

  // serve_forever() with the drain cap; the handler's exception, let
  // through by on_handler_exception, ends the process with 4.
  template <typename Server>
  int serve(Server& server) {
    try {
      server.serve_forever(0.25f, static_cast<float>(leave_.drain_seconds));
    } catch (std::exception& error) {
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
  virtual void _initialize_options(mx::options::Options& options) {
    common_.add(options);
    options.add("serves", &serves_, "REQUEST_TYPE=RESPONSE_TYPE, repeatable");
    options.add("behaviour", &behaviour_, "upper", "upper | echo | drop | raise | sleep:MS");
    options.add("crash-after", &crash_after_, 0, "exit(3) after N handled requests");
    options.add("memory-every", &memory_every_, 0, "emit a memory event every N requests");
    options.add(
        "drain-seconds", &leave_.drain_seconds, 0.0,
        "when asked to leave, drain: the multiplexers route it nothing new, it serves what arrives, at most this long");
    options.add("drain-file", &leave_.drain_file, "", "a file whose appearance asks the backend to leave");
    options.add("drain-min-handled", &leave_.drain_min_handled, 0, "do not leave before N requests were served");
    options.add("drain-routing", &leave_.drain_routing, "",
                "Routing flags kept on while draining: any,all,last_resort");
    options.add_switch("exit-on-exception", &leave_.exit_on_exception, "a handler exception ends the process (4)");
    options.add_switch("threaded", &threaded_, "serve on BaseThreadedMultiplexerServer instead");
  }

 private:
  CommonOptions common_;
  std::vector<std::string> serves_;
  std::string behaviour_;
  int crash_after_;
  int memory_every_;
  LeaveOptions leave_;
  bool threaded_ = false;
};

REGISTER_MXCONTROL_SUBCOMMAND(backend, BackendRole);

}  // namespace mxtestroles
