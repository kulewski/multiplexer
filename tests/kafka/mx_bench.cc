// The multiplexer's side of the Kafka comparison (docs/kafka.md,
// tests/kafka/run.sh): `mx_bench produce` sends records as events of type
// BENCH_RECORD, which tests/kafka/bench.rules routes to one BENCH_CONSUMER
// in turn, as a Kafka consumer group shares a topic's partitions; `mx_bench
// consume` receives them and says how many came, how fast and how late.
// Each producer and each consumer is a thread with a SyncClient of its own,
// or, for a producer with --client threaded, a ThreadedClient, whose io
// thread batches and writes the records while the producer's thread makes
// them, as Kafka's producer library works on threads of its own;
// one connection to each multiplexer --address names, comma-separated, the
// library sending each message over one of them round robin and receiving
// from all, as a client of several multiplexers does. Without batching a producer's
// send() returns once its record is written to the socket, so nothing waits
// in the producer. With --batch-records, the producer batches the records
// (set_delivery's batch), --batch-bytes and --batch-delay its other limits, and
// queues each without waiting, with a flush_all() every FLUSH_EVERY records
// so that what waits in it stays bounded, as Kafka's producer stops taking
// records while its own queue is full. What does not reach a consumer in
// the end is lost, which the consumer says. Built in tests/kafka/Dockerfile
// against the library `make install` put there, its constants from
// bench.rules; common.h has the rest.
#include <cstdint>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <type_traits>
#include <vector>

#include "multiplexer/client.h"
#include "multiplexer/multiplexer.constants.h"
#include "multiplexer/threaded_client.h"
#include "tests/kafka/common.h"

namespace {

using multiplexer::MultiplexerMessage;
using multiplexer::SyncClient;
using multiplexer::ThreadedClient;

const float CONNECT_SECONDS = 30;
const float SEND_SECONDS = 60;             // a send that waits longer than this for room has failed
const std::uint64_t FLUSH_EVERY = 100000;  // records a batching producer queues between two flushes

// The multiplexers' hosts and ports from host:port,host:port...
std::vector<std::pair<std::string, std::uint16_t>> endpoints(const std::string& addresses) {
  std::vector<std::pair<std::string, std::uint16_t>> found;
  std::string::size_type start = 0;
  for (;;) {
    const std::string::size_type comma = addresses.find(',', start);
    const std::string address = addresses.substr(start, comma == std::string::npos ? comma : comma - start);
    const std::string::size_type colon = address.rfind(':');
    if (colon == std::string::npos) {
      throw std::invalid_argument("--address is host:port, comma-separated, not " + addresses);
    }
    found.emplace_back(address.substr(0, colon), static_cast<std::uint16_t>(std::stoi(address.substr(colon + 1))));
    if (comma == std::string::npos) {
      return found;
    }
    start = comma + 1;
  }
}

// `client` connected to every one of `to`.
template <typename Client>
void connect_all(Client& client, const std::vector<std::pair<std::string, std::uint16_t>>& to) {
  for (const std::pair<std::string, std::uint16_t>& endpoint : to) {
    if (!client.connect(endpoint.first, endpoint.second, CONNECT_SECONDS)) {
      throw std::runtime_error("no connection to " + endpoint.first + ":" + std::to_string(endpoint.second));
    }
  }
}

// One producer thread's records through `client`, a SyncClient or a
// ThreadedClient, connected and released at the barrier: each record
// handed over, with a flush_all() every FLUSH_EVERY records so that what
// waits in the client stays bounded, and the client shut down at the end,
// which writes whatever is left first.
template <typename Client>
void send_records(Client& client, const bench::Options& options, std::uint64_t mine) {
  if (options.batch_records) {
    SyncClient::Delivery batching;
    batching.batch = true;
    batching.max_records = options.batch_records;
    batching.max_bytes = options.batch_bytes;
    batching.max_delay = static_cast<float>(options.batch_delay);
    client.set_delivery(multiplexer::types::BENCH_RECORD, batching);
  }
  // A record is an event: the library gives it its id and sender.
  MultiplexerMessage record;
  record.set_type(multiplexer::types::BENCH_RECORD);
  std::string payload(options.size, 'r');
  for (std::uint64_t sent = 0; sent < options.records; ++sent) {
    bench::pace(options.rate, mine, sent);
    bench::stamp(&payload);
    record.set_message(payload);
    if constexpr (std::is_same<Client, ThreadedClient>::value) {
      client.send(record);  // returns at once: the io thread batches and writes it
    } else if (!options.batch_records) {
      client.send(record, SEND_SECONDS);
    } else if (!client.queue(record, SEND_SECONDS)) {
      throw std::runtime_error("a record nothing took");
    }
    if ((sent + 1) % FLUSH_EVERY == 0 && !client.flush_all(SEND_SECONDS)) {
      throw std::runtime_error("records not written");
    }
  }
  client.shutdown(SEND_SECONDS);
}

int produce(const bench::Options& options) {
  const std::vector<std::pair<std::string, std::uint16_t>> to = endpoints(options.address);
  bench::Barrier connected(options.threads);
  std::uint64_t start = 0;
  std::vector<std::thread> threads;
  for (int index = 0; index < options.threads; ++index) {
    threads.emplace_back([&options, &to, &connected, &start, index] {
      bench::or_exit("mx_bench", [&] {
        const auto run = [&](auto& client) {
          connect_all(client, to);
          connected.arrive_and_wait();
          if (index == 0) {
            start = bench::now_ns();
          }
          send_records(client, options, bench::now_ns());
        };
        if (options.client == "threaded") {
          ThreadedClient client(multiplexer::peers::BENCH_THREADED_PRODUCER);
          run(client);
        } else {
          SyncClient client(multiplexer::peers::BENCH_PRODUCER);
          run(client);
        }
      });
    });
  }
  for (std::thread& thread : threads) {
    thread.join();
  }
  std::cout << bench::produce_result("mx", options, bench::now_ns() - start) << std::endl;
  return 0;
}

int consume(const bench::Options& options) {
  const std::vector<std::pair<std::string, std::uint16_t>> from = endpoints(options.address);
  bench::Tally tally(options.expect);
  bench::Barrier connected(options.threads + 1);
  std::vector<std::thread> threads;
  for (int index = 0; index < options.threads; ++index) {
    threads.emplace_back([&options, &from, &tally, &connected] {
      bench::or_exit("mx_bench", [&] {
        SyncClient client(multiplexer::peers::BENCH_CONSUMER);
        connect_all(client, from);
        connected.arrive_and_wait();
        const std::uint64_t start = bench::now_ns();
        bench::Tally::Local mine;
        while (!tally.done(options.expect, options.quiet, start)) {
          try {
            const auto incoming = client.receive_message(0.1);
            const std::string& payload = incoming.first->message();
            tally.record(&mine, payload.data(), payload.size(), bench::now_ns());
          } catch (const SyncClient::OperationTimedOut&) {
            // nothing for a tenth of a second: done() decides
          }
        }
        tally.merge(std::move(mine));
      });
    });
  }
  connected.arrive_and_wait();
  std::cout << "ready" << std::endl;  // run.sh starts the producers now
  for (std::thread& thread : threads) {
    thread.join();
  }
  std::cout << tally.result("mx", options.threads, options.expect, options.size) << std::endl;
  return 0;
}

}  // namespace

int main(int argc, char** argv) {
  try {
    const bench::Options options = bench::parse(argc, argv);
    return options.role == "produce" ? produce(options) : consume(options);
  } catch (const std::exception& error) {
    std::cerr << "mx_bench: " << error.what() << std::endl;
    return 1;
  }
}
