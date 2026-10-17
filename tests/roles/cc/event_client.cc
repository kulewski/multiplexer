// The `event_client` role: sends events, never runs a loop. See
// tests/README.md for options and events.
#include <chrono>
#include <set>
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
// --linger keeps the process alive after the last one. --batch-records N
// batches every type sent (set_batching), N messages a batch, --batch-delay
// S their max_delay: each send then returns at once, queue() putting the
// message in its type's batch, and flush_all() after the last lets the last
// batch go. Events: sent, error, flushed, done.
class EventClientRole : public mxcontrol::Task {
 public:
  virtual std::string short_description() const { return "send events as a passive peer, never run a loop"; }
  virtual int run() {
    std::unique_ptr<Client> client = common_.connect();
    emit(common_.connected_event(*client));
    std::vector<std::pair<std::uint32_t, std::string>> sends = typed_payloads(send_);
    if (batch_records_ > 0) {
      Client::Batching batching;
      batching.max_records = batch_records_;
      batching.max_delay = static_cast<float>(batch_delay_);
      std::set<std::uint32_t> types;
      for (const std::pair<std::uint32_t, std::string>& send : sends) {
        types.insert(send.first);
      }
      for (std::uint32_t type : types) {
        client->set_batching(type, batching);
      }
    }
    int sent_count = 0;
    for (size_t index = 0; index < sends.size(); ++index) {
      MultiplexerMessage message;
      message.set_id(client->random64());
      message.set_sender(client->instance_id());
      message.set_type(sends[index].first);
      message.set_message(sends[index].second);
      if (to_) {
        message.set_to(to_);
      }
      if (!key_.empty()) {
        message.set_key(key_);
      }
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
        } else if (batch_records_ > 0) {
          // Returns at once, the message in its type's batch, which goes
          // once full; the flush after the last lets the last go.
          if (!client->queue(message)) {
            throw Client::NotConnected();  // nothing may take it, as Python's and Rust's sends say
          }
        } else if (no_flush_) {
          multiplexer::Client::ScheduledMessageTracker tracker = client->schedule_one(message);
          sent.set_in_queue(tracker.in_queue());
          sent.set_is_sent(tracker.is_sent());
          sent.set_is_lost(tracker.is_lost());
        } else {
          client->send(message, 10.0f);  // written, through whatever connection is alive by then
          sent.set_in_queue(false);
          sent.set_is_sent(true);
          sent.set_is_lost(false);
        }
        ok = true;
      } catch (...) {
        report_client_errors([]() { throw; }, error);
      }
      if (!ok) {
        continue;
      }
      ++sent_count;
      emit(sent);
      if (interval_ > 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(int(interval_ * 1000)));
      }
    }
    if (batch_records_ > 0) {
      Event flushed = event("flushed");
      flushed.set_is_sent(client->flush_all(10.0f));  // the waiting send's 10 s
      emit(flushed);
    }
    if (linger_ > 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(int(linger_ * 1000)));
    }
    Event done = event("done");
    done.set_sent(sent_count);
    done.set_connections(client->connections_count());
    emit(done);
    client->shutdown();
    return 0;
  }

 protected:
  virtual void _initialize_options(mx::options::Options& options) {
    common_.add(options);
    options.add("send", &send_, "TYPE:payload, repeatable, sent in order");
    options.add("to", &to_, 0, "direct to this instance id");
    options.add_switch("all", &all_, "send through every connection");
    options.add_switch("no-flush", &no_flush_, "");
    options.add("interval", &interval_, 0.0, "pause between sends");
    options.add("linger", &linger_, 0.0, "stay connected this long after sending");
    options.add("key", &key_, "", "the key every message carries");
    options.add("batch-records", &batch_records_, 0u, "batch every type sent, N messages a batch, flushed at the end");
    options.add("batch-delay", &batch_delay_, 0.0, "the batches' max_delay, in seconds");
  }

 private:
  CommonOptions common_;
  std::vector<std::string> send_;
  std::uint64_t to_;
  bool all_;
  bool no_flush_;
  double interval_;
  double linger_;
  std::string key_;
  unsigned int batch_records_;
  double batch_delay_;
};

REGISTER_MXCONTROL_SUBCOMMAND(event_client, EventClientRole);

}  // namespace mxtestroles
