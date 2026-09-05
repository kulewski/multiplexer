// The `client` role: requests and their answers, in four modes. See the
// class comment and tests/README.md.
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <queue>
#include <thread>

#include "multiplexer/threaded_client.h"
#include "mxcontrol/task.h"
#include "mxcontrol/tasks_holder.h"
#include "tests/roles/cc/common.h"

namespace mxtestroles {

using multiplexer::MultiplexerMessage;
using multiplexer::ThreadedClient;

// The `client` subcommand: sends requests and reports the answers. Four
// modes, all producing the same response/error/done events:
//   default          a synchronous Client per worker (--parallel N), one
//                    query at a time;
//   --threaded       a ThreadedClient per worker, one query at a time;
//   --threaded --async N   up to N queries in flight through callbacks;
//   --workers N      N threads sharing one ThreadedClient, each with its own
//                    reply queue, the pattern of examples/echo/workers.py.
// --query TYPE:payload (repeatable) is the list each worker sends --count
// times; {worker}, {round} and {i} in a payload are filled in. --timeout,
// --payload-size, --sleep-before and --sleep-between shape the run.
class ClientRole : public mxcontrol::Task {
public:
  virtual std::string short_description() const { return "issue queries and report the answers"; }

  virtual int run() {
    if (workers_ > 0)
      return run_workers();
    std::vector<std::thread> threads;
    for (int worker_index = 0; worker_index < parallel_; ++worker_index)
      threads.push_back(std::thread(&ClientRole::run_worker, this, worker_index));
    for (size_t index = 0; index < threads.size(); ++index)
      threads[index].join();
    return 0;
  }

protected:
  virtual void _initialize_options_description(po::options_description &options) {
    common_.add(options);
    options.add_options()("query", po::value(&query_)->composing(), "TYPE:payload, repeatable, sent in order")(
        "count", po::value(&count_)->default_value(1), "repeat the list N times")(
        "parallel", po::value(&parallel_)->default_value(1),
        "workers, each with its own client")("timeout", po::value(&timeout_)->default_value(10.0), "")(
        "payload-size", po::value(&payload_size_)->default_value(0), "replace payloads with N bytes")(
        "sleep-before", po::value(&sleep_before_)->default_value(0.0), "idle after connecting")(
        "sleep-between", po::value(&sleep_between_)->default_value(0.0), "idle between queries")(
        "threaded", po::bool_switch(&threaded_), "use ThreadedClient (an io thread of its own)")(
        "async", po::value(&async_)->default_value(0),
        "with --threaded: keep up to N queries in flight at once, through callbacks")(
        "workers", po::value(&workers_)->default_value(0),
        "N threads sharing one ThreadedClient, each with its own reply queue")(
        "memory-every", po::value(&memory_every_)->default_value(0), "emit a memory event every N answered queries");
  }

private:
  // One query of a worker's list, with the payload already expanded.
  struct Query {
    int round, index;
    boost::uint32_t type;
    std::string payload;
  };

  // The kind name of a threaded client's outcome, as the Python roles report it.
  static const char *outcome_name(ThreadedClient::Outcome outcome) {
    switch (outcome) {
    case ThreadedClient::REPLIED:
      return "";
    case ThreadedClient::TIMED_OUT:
      return "OperationTimedOut";
    case ThreadedClient::FAILED:
      return "OperationFailed";
    case ThreadedClient::NOT_CONNECTED:
    case ThreadedClient::SHUT_DOWN:
      return "NotConnected";
    }
    return "";
  }

  // The worker's whole list: --count rounds of --query, payloads expanded.
  std::vector<Query> queries_for(int worker_index) const {
    std::vector<std::pair<boost::uint32_t, std::string>> list = typed_payloads(query_);
    std::vector<Query> out;
    for (int round = 0; round < count_; ++round)
      for (size_t query_index = 0; query_index < list.size(); ++query_index) {
        std::string payload = payload_size_ ? std::string(payload_size_, 'x') : list[query_index].second;
        payload = replace_all(payload, "{worker}", boost::lexical_cast<std::string>(worker_index));
        payload = replace_all(payload, "{round}", boost::lexical_cast<std::string>(round));
        payload = replace_all(payload, "{i}", boost::lexical_cast<std::string>(query_index));
        Query query = {round, (int)query_index, list[query_index].first, payload};
        out.push_back(query);
      }
    return out;
  }

  // The fields every response or error event carries.
  static Event query_event(const std::string &name, int worker_index, const Query &query) {
    Event result = event(name);
    result.set_worker(worker_index);
    result.set_round(query.round);
    result.set_index(query.index);
    result.set_query_type(query.type);
    return result;
  }

  static void report_reply(int worker_index, const Query &query, const MultiplexerMessage &reply, double ms) {
    Event response = query_event("response", worker_index, query);
    response.set_type(reply.type());
    response.set_from_(reply.from());
    response.set_references(reply.references());
    response.set_ms(ms);
    set_payload(response, reply.message());
    emit(response);
  }

  static void report_error(int worker_index, const Query &query, const std::string &kind, double ms) {
    Event error = query_event("error", worker_index, query);
    error.set_kind(kind);
    error.set_ms(ms);
    emit(error);
  }

  static void report_done(int worker_index, int responses, int errors, unsigned connections,
                          boost::uint64_t instance_id) {
    Event done = event("done");
    done.set_worker(worker_index);
    done.set_responses(responses);
    done.set_errors(errors);
    done.set_connections(connections);
    done.set_instance_id(instance_id);
    emit(done);
  }

  // Counts answered queries across workers and emits a memory event every
  // --memory-every of them.
  void count_answered() {
    if (!memory_every_)
      return;
    long answered = ++answered_;
    if (answered % memory_every_ == 0)
      emit(memory_event(answered));
  }

  // Report a threaded client's outcome; returns true for a reply.
  static bool report_outcome(int worker_index, const Query &query, const ThreadedClient::Result &result, double ms) {
    if (result.outcome == ThreadedClient::REPLIED) {
      report_reply(worker_index, query, *result.reply.third, ms);
      return true;
    }
    report_error(worker_index, query, outcome_name(result.outcome), ms);
    return false;
  }

  // A ThreadedClient connected to every --mx.
  std::unique_ptr<ThreadedClient> connect_threaded() const {
    std::unique_ptr<ThreadedClient> client(new ThreadedClient(common_.type));
    for (size_t index = 0; index < common_.mx.size(); ++index) {
      std::string::size_type colon = common_.mx[index].rfind(':');
      client->connect(common_.mx[index].substr(0, colon),
                      boost::lexical_cast<boost::uint16_t>(common_.mx[index].substr(colon + 1)));
    }
    return client;
  }

  Event connected_event(ThreadedClient &client, int worker_index) const {
    Event connected = event("connected");
    connected.set_instance_id(client.instance_id());
    connected.set_connections(client.connections_count());
    connected.set_name(common_.name);
    connected.set_worker(worker_index);
    connected.set_threaded(true);
    return connected;
  }

  // One worker of --parallel: its own client, then one of the modes.
  void run_worker(int worker_index) {
    if (threaded_) {
      run_worker_threaded(worker_index);
      return;
    }
    std::unique_ptr<Client> client = common_.connect();
    Event connected = common_.connected_event(*client);
    connected.set_worker(worker_index);
    connected.set_threaded(false);
    emit(connected);
    if (sleep_before_ > 0)
      std::this_thread::sleep_for(std::chrono::milliseconds(int(sleep_before_ * 1000)));

    int responses = 0, errors = 0;
    std::vector<Query> queries = queries_for(worker_index);
    for (size_t index = 0; index < queries.size(); ++index) {
      const Query &query = queries[index];
      if (sleep_between_ > 0 && index)
        std::this_thread::sleep_for(std::chrono::milliseconds(int(sleep_between_ * 1000)));
      std::chrono::steady_clock::time_point started = std::chrono::steady_clock::now();
      multiplexer::IncomingMessage reply;
      bool ok = false;
      try {
        reply = client->query(query.payload, query.type, timeout_);
        ok = true;
      } catch (...) {
        ++errors;
        Event error = query_event("error", worker_index, query);
        error.set_ms(ms_since(started));
        report_client_errors([]() { throw; }, error);
      }
      if (!ok)
        continue;
      ++responses;
      report_reply(worker_index, query, *reply.third, ms_since(started));
      count_answered();
    }
    report_done(worker_index, responses, errors, client->connections_count(), client->instance_id());
    client->shutdown();
  }

  // The same queries through a ThreadedClient: one at a time with the
  // blocking query(), or up to async_ in flight through callbacks.
  void run_worker_threaded(int worker_index) {
    std::unique_ptr<ThreadedClient> client = connect_threaded();
    emit(connected_event(*client, worker_index));
    if (sleep_before_ > 0)
      std::this_thread::sleep_for(std::chrono::milliseconds(int(sleep_before_ * 1000)));

    std::vector<Query> queries = queries_for(worker_index);
    std::mutex mutex;
    std::condition_variable done_cv;
    int in_flight = 0, responses = 0, errors = 0;
    for (size_t index = 0; index < queries.size(); ++index) {
      const Query &query = queries[index];
      if (sleep_between_ > 0 && index)
        std::this_thread::sleep_for(std::chrono::milliseconds(int(sleep_between_ * 1000)));
      std::chrono::steady_clock::time_point started = std::chrono::steady_clock::now();
      if (async_ <= 1) {
        // The query first, then the clock: argument evaluation order is
        // unspecified, and measuring before the call once reported 0 ms.
        ThreadedClient::Result result = client->query(query.payload, query.type, timeout_);
        double ms = ms_since(started);
        if (report_outcome(worker_index, query, result, ms))
          ++responses;
        else
          ++errors;
        count_answered();
        continue;
      }
      {
        std::unique_lock<std::mutex> lock(mutex);
        done_cv.wait(lock, [&] { return in_flight < async_; });
        ++in_flight;
      }
      client->query(
          query.payload, query.type,
          [&, query, started, worker_index](const ThreadedClient::Result &result) {
            // On the io thread: report, then let the worker issue the next one.
            bool replied = report_outcome(worker_index, query, result, ms_since(started));
            count_answered();
            std::lock_guard<std::mutex> lock(mutex);
            (replied ? responses : errors) += 1;
            --in_flight;
            done_cv.notify_all();
          },
          timeout_);
    }
    {
      std::unique_lock<std::mutex> lock(mutex);
      done_cv.wait(lock, [&] { return in_flight == 0; });
    }
    report_done(worker_index, responses, errors, client->connections_count(), client->instance_id());
    client->shutdown();
  }

  // --workers N: N threads share one ThreadedClient. Each issues its queries
  // with the callback form, does its own work meanwhile (sleep_between), and
  // takes the replies from its own queue, which the io thread fills.
  int run_workers() {
    std::unique_ptr<ThreadedClient> client = connect_threaded();
    Event connected = event("connected");
    connected.set_instance_id(client->instance_id());
    connected.set_connections(client->connections_count());
    connected.set_name(common_.name);
    connected.set_threaded(true);
    connected.set_workers(workers_);
    emit(connected);

    std::vector<std::thread> threads;
    for (int worker_index = 0; worker_index < workers_; ++worker_index)
      threads.push_back(std::thread([&, worker_index] {
        struct Item {
          Query query;
          std::chrono::steady_clock::time_point started;
          ThreadedClient::Result result;
        };
        std::mutex mutex;
        std::condition_variable cv;
        std::queue<Item> inbox; // replies land here, from the io thread
        int pending = 0, responses = 0, errors = 0;
        auto take = [&](const Item &item) {
          if (report_outcome(worker_index, item.query, item.result, ms_since(item.started)))
            ++responses;
          else
            ++errors;
        };
        std::vector<Query> queries = queries_for(worker_index);
        for (size_t index = 0; index < queries.size(); ++index) {
          const Query &query = queries[index];
          std::chrono::steady_clock::time_point started = std::chrono::steady_clock::now();
          client->query(
              query.payload, query.type,
              [&, query, started](const ThreadedClient::Result &result) {
                std::lock_guard<std::mutex> lock(mutex);
                inbox.push(Item{query, started, result});
                cv.notify_one();
              },
              timeout_);
          ++pending;
          if (sleep_between_ > 0)
            std::this_thread::sleep_for(std::chrono::milliseconds(int(sleep_between_ * 1000))); // own work
          std::lock_guard<std::mutex> lock(mutex);
          while (!inbox.empty()) { // whatever arrived meanwhile, without waiting
            take(inbox.front());
            inbox.pop();
            --pending;
          }
        }
        std::unique_lock<std::mutex> lock(mutex);
        while (pending) {
          cv.wait(lock, [&] { return !inbox.empty(); });
          take(inbox.front());
          inbox.pop();
          --pending;
        }
        report_done(worker_index, responses, errors, client->connections_count(), client->instance_id());
      }));
    for (size_t index = 0; index < threads.size(); ++index)
      threads[index].join();
    client->shutdown();
    return 0;
  }

  CommonOptions common_;
  std::vector<std::string> query_;
  int count_;
  int parallel_;
  double timeout_;
  int payload_size_;
  double sleep_before_;
  double sleep_between_;
  bool threaded_ = false;
  int async_ = 0;
  int workers_ = 0;
  int memory_every_ = 0;
  std::atomic<long> answered_{0};
};

REGISTER_MXCONTROL_SUBCOMMAND(client, ClientRole);

} // namespace mxtestroles
