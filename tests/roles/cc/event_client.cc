// The `event_client` role: sends events, never runs a loop. See
// tests/README.md for options and events.
#include <chrono>
#include <thread>

#include "multiplexer/Multiplexer.pb.h"
#include "mxcontrol/task.h"
#include "mxcontrol/tasks_holder.h"
#include "tests/roles/cc/common.h"

namespace mxtestroles {

using multiplexer::MultiplexerMessage;

// The `event_client` subcommand: a passive client that sends events and
// never waits for answers. --send TYPE:payload (repeatable) in order;
// --to ID addresses one peer directly; --all sends through every connection;
// --no-flush returns before the write; --interval pauses between sends;
// --linger keeps the process alive after the last one. Events: sent, done.
class EventClientRole : public mxcontrol::Task {
public:
  virtual std::string short_description() const { return "send events as a passive peer, never run a loop"; }
  virtual int run() {
    std::unique_ptr<Client> client = common_.connect();
    emit(common_.connected_event(*client));
    std::vector<std::pair<boost::uint32_t, std::string>> sends = typed_payloads(send_);
    int sent_count = 0;
    for (size_t index = 0; index < sends.size(); ++index) {
      MultiplexerMessage message;
      message.set_id(client->random64());
      message.set_from(client->instance_id());
      message.set_type(sends[index].first);
      message.set_message(sends[index].second);
      if (to_)
        message.set_to(to_);
      Event sent = event("sent");
      sent.set_type(sends[index].first);
      sent.set_id(message.id());
      Event error = event("error");
      error.set_type(sends[index].first);
      error.set_id(message.id());
      bool ok = false;
      try {
        if (all_) {
          sent.set_connections(client->schedule_all(message));
        } else if (no_flush_) {
          multiplexer::Client::ScheduledMessageTracker tracker = client->schedule_one(message);
          sent.set_in_queue(tracker.in_queue());
          sent.set_is_sent(tracker.is_sent());
          sent.set_is_lost(tracker.is_lost());
        } else {
          client->send(message, 10.0f); // written, through whatever connection is alive by then
          sent.set_in_queue(false);
          sent.set_is_sent(true);
          sent.set_is_lost(false);
        }
        ok = true;
      } catch (...) {
        report_client_errors([]() { throw; }, error);
      }
      if (!ok)
        continue;
      ++sent_count;
      emit(sent);
      if (interval_ > 0)
        std::this_thread::sleep_for(std::chrono::milliseconds(int(interval_ * 1000)));
    }
    if (linger_ > 0)
      std::this_thread::sleep_for(std::chrono::milliseconds(int(linger_ * 1000)));
    Event done = event("done");
    done.set_sent(sent_count);
    done.set_connections(client->connections_count());
    emit(done);
    client->shutdown();
    return 0;
  }

protected:
  virtual void _initialize_options_description(po::options_description &options) {
    common_.add(options);
    options.add_options()("send", po::value(&send_)->composing(), "TYPE:payload, repeatable, sent in order")(
        "to", po::value(&to_)->default_value(0), "direct to this instance id")(
        "all", po::bool_switch(&all_), "send through every connection")("no-flush", po::bool_switch(&no_flush_), "")(
        "interval", po::value(&interval_)->default_value(0.0), "pause between sends")(
        "linger", po::value(&linger_)->default_value(0.0), "stay connected this long after sending");
  }

private:
  CommonOptions common_;
  std::vector<std::string> send_;
  boost::uint64_t to_;
  bool all_;
  bool no_flush_;
  double interval_;
  double linger_;
};

REGISTER_MXCONTROL_SUBCOMMAND(event_client, EventClientRole);

} // namespace mxtestroles
