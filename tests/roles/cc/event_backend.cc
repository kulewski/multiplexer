// The `event_backend` role: receives events in a loop and reports them. See
// tests/README.md for options and events.
#include <unistd.h>

#include <chrono>

#include "multiplexer/backend/base_multiplexer_server.h"
#include "mxcontrol/task.h"
#include "mxcontrol/tasks_holder.h"
#include "tests/roles/cc/common.h"

namespace mxtestroles {

using multiplexer::MultiplexerMessage;

// A backend that receives events, reports each one and never answers.
class EventBackendServer : public multiplexer::backend::BaseMultiplexerServer {
 public:
  EventBackendServer(Client* client, unsigned type) : BaseMultiplexerServer(client, type), received_(0) {}
  int received() const { return received_; }

 protected:
  // Report one received event.
  virtual void handle_message(MultiplexerMessage& mxmsg) {
    ++received_;
    Event received = event("received");
    received.set_type(mxmsg.type());
    received.set_id(mxmsg.id());
    received.set_sender(mxmsg.sender());
    received.set_to(mxmsg.to());
    set_payload(received, mxmsg.message());
    emit(received);
    no_response();
  }

 private:
  int received_;
};

// The `event_backend` subcommand: runs an EventBackendServer until SIGTERM,
// --until N messages, or --for S seconds. With --drain-file it starts
// draining when the file appears, the --drain-routing flags kept on, and
// keeps looping. Events: received, draining, acked, done.
class EventBackendRole : public mxcontrol::Task {
 public:
  virtual std::string short_description() const { return "receive events in a loop and report them"; }
  virtual int run() {
    install_signal_handlers();
    std::unique_ptr<Client> client = common_.connect();
    EventBackendServer server(client.get(), common_.type);
    server.set_drain_routing(parse_drain_routing(drain_routing_));
    emit(common_.connected_event(*client));
    std::chrono::steady_clock::time_point deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(int(duration_ * 1000));
    bool acked = false;
    while (!stop_requested) {
      if (until_ && server.received() >= until_) {
        break;
      }
      if (duration_ > 0 && std::chrono::steady_clock::now() >= deadline) {
        break;
      }
      if (!drain_file_.empty() && !server.draining() && access(drain_file_.c_str(), F_OK) == 0) {
        server.start_draining();
        Event draining = event("draining");
        draining.set_drain_seconds(0.0);
        emit(draining);
      }
      if (server.draining() && !acked && client->routing_acknowledged()) {
        acked = true;
        Event acked_event = event("acked");
        acked_event.set_ms(0.0);
        emit(acked_event);
      }
      try {
        server.loop_iter(0.25f);
      } catch (Client::OperationTimedOut&) {
      }
    }
    Event done = event("done");
    done.set_received(server.received());
    emit(done);
    client->shutdown();
    return 0;
  }

 protected:
  virtual void _initialize_options(mx::options::Options& options) {
    common_.add(options);
    options.add("until", &until_, 0, "exit after N messages");
    options.add("for", &duration_, 0.0, "exit after S seconds");
    options.add("drain-file", &drain_file_, "", "a file whose appearance starts a drain");
    options.add("drain-routing", &drain_routing_, "", "Routing flags kept on while draining: any,all,last_resort");
  }

 private:
  CommonOptions common_;
  int until_;
  double duration_;
  std::string drain_file_;
  std::string drain_routing_;
};

REGISTER_MXCONTROL_SUBCOMMAND(event_backend, EventBackendRole);

}  // namespace mxtestroles
