// The `event_backend` role: receives events in a loop and reports them. See
// tests/README.md for options and events.
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
  EventBackendServer(Client *client, unsigned type) : BaseMultiplexerServer(client, type), received_(0) {}
  int received() const { return received_; }

protected:
  // Report one received event.
  virtual void handle_message(MultiplexerMessage &mxmsg) {
    ++received_;
    Event received = event("received");
    received.set_type(mxmsg.type());
    received.set_id(mxmsg.id());
    received.set_from_(mxmsg.from());
    received.set_to(mxmsg.to());
    set_payload(received, mxmsg.message());
    emit(received);
    no_response();
  }

private:
  int received_;
};

// The `event_backend` subcommand: runs an EventBackendServer until SIGTERM,
// --until N messages, or --for S seconds. Events: received, done.
class EventBackendRole : public mxcontrol::Task {
public:
  virtual std::string short_description() const { return "receive events in a loop and report them"; }
  virtual int run() {
    install_signal_handlers();
    std::unique_ptr<Client> client = common_.connect();
    EventBackendServer server(client.get(), common_.type);
    emit(common_.connected_event(*client));
    std::chrono::steady_clock::time_point deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(int(duration_ * 1000));
    while (!stop_requested) {
      if (until_ && server.received() >= until_)
        break;
      if (duration_ > 0 && std::chrono::steady_clock::now() >= deadline)
        break;
      try {
        server.loop_iter(0.25f);
      } catch (Client::OperationTimedOut &) {
      }
    }
    Event done = event("done");
    done.set_received(server.received());
    emit(done);
    client->shutdown();
    return 0;
  }

protected:
  virtual void _initialize_options_description(po::options_description &options) {
    common_.add(options);
    options.add_options()("until", po::value(&until_)->default_value(0), "exit after N messages")(
        "for", po::value(&duration_)->default_value(0.0), "exit after S seconds");
  }

private:
  CommonOptions common_;
  int until_;
  double duration_;
};

REGISTER_MXCONTROL_SUBCOMMAND(event_backend, EventBackendRole);

} // namespace mxtestroles
