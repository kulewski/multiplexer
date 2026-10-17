// What the two programs of the Kafka comparison share, mx_bench.cc and
// kafka_bench.cc (docs/kafka.md, tests/kafka/run.sh): their options, the
// clock that stamps each record as it is sent and measures how long it took
// to arrive, the tally a consumer keeps, and the one line of JSON each
// program prints when it ends. Every process of a run is on one machine, so
// the steady clock, CLOCK_MONOTONIC, is one clock for all of them, in
// containers too.
#ifndef MX_TESTS_KAFKA_COMMON_H_
#define MX_TESTS_KAFKA_COMMON_H_

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace bench {

// The command line: `produce` or `consume`, then --name value pairs.
struct Options {
  std::string role;
  std::string address;        // the multiplexers' host:port, comma-separated, or Kafka's bootstrap servers
  std::string topic;          // Kafka's topic; one partition for each consumer thread
  int threads = 1;            // producers, or consumers, one connection each
  std::uint64_t records = 0;  // a producer's records, each thread's
  std::size_t size = 100;     // a record's payload, 8 bytes at least: its stamp
  double rate = 0;            // a producer's records a second, each thread's; 0 is as fast as it goes
  std::uint64_t expect = 0;   // the records a consumer process waits for, from every producer
  double quiet = 10;          // seconds without a record after which a consumer stops waiting
  // Kafka's client settings beside the defaults, each --set name=value.
  std::vector<std::pair<std::string, std::string>> settings;
  // The multiplexer's batching (set_delivery's batch): records in a batch at most,
  // 0 for none, the batch's bytes at most, and its delay in seconds.
  unsigned int batch_records = 0;
  std::size_t batch_bytes = 1024 * 1024;
  double batch_delay = 0;
  // The multiplexer's producer client: "sync", a SyncClient on the
  // producer's thread, or "threaded", a ThreadedClient, whose io thread
  // batches and writes while the producer's thread makes the records.
  std::string client = "sync";
};

inline Options parse(int argc, char** argv) {
  Options options;
  if (argc < 2) {
    throw std::invalid_argument("usage: " + std::string(argv[0]) + " produce|consume --name value ...");
  }
  options.role = argv[1];
  for (int index = 2; index + 1 < argc; index += 2) {
    const std::string name = argv[index], value = argv[index + 1];
    if (name == "--address") {
      options.address = value;
    } else if (name == "--topic") {
      options.topic = value;
    } else if (name == "--threads") {
      options.threads = std::stoi(value);
    } else if (name == "--records") {
      options.records = std::stoull(value);
    } else if (name == "--size") {
      options.size = std::stoull(value);
    } else if (name == "--rate") {
      options.rate = std::stod(value);
    } else if (name == "--expect") {
      options.expect = std::stoull(value);
    } else if (name == "--quiet") {
      options.quiet = std::stod(value);
    } else if (name == "--batch-records") {
      options.batch_records = static_cast<unsigned int>(std::stoul(value));
    } else if (name == "--batch-bytes") {
      options.batch_bytes = std::stoull(value);
    } else if (name == "--batch-delay") {
      options.batch_delay = std::stod(value);
    } else if (name == "--client" && (value == "sync" || value == "threaded")) {
      options.client = value;
    } else if (name == "--set" && value.find('=') != std::string::npos) {
      options.settings.emplace_back(value.substr(0, value.find('=')), value.substr(value.find('=') + 1));
    } else {
      throw std::invalid_argument("unknown option " + name);
    }
  }
  if ((options.role != "produce" && options.role != "consume") || options.threads < 1 || options.size < 8) {
    throw std::invalid_argument("produce or consume, one thread at least, 8 bytes a record at least");
  }
  return options;
}

// The steady clock in nanoseconds.
inline std::uint64_t now_ns() {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

// A record's payload carries the moment it was sent in its first 8 bytes.
inline void stamp(std::string* payload) {
  const std::uint64_t now = now_ns();
  std::memcpy(&(*payload)[0], &now, sizeof now);
}
inline std::uint64_t stamp_of(const char* payload) {
  std::uint64_t sent = 0;
  std::memcpy(&sent, payload, sizeof sent);
  return sent;
}

// Holds a producer to `rate` records a second from `start`: record `index`
// waits for its moment.
inline void pace(double rate, std::uint64_t start, std::uint64_t index) {
  if (rate > 0) {
    const std::uint64_t due = start + static_cast<std::uint64_t>(index * 1e9 / rate);
    const std::uint64_t now = now_ns();
    if (due > now) {
      std::this_thread::sleep_for(std::chrono::nanoseconds(due - now));
    }
  }
}

// Runs a thread's `body`: an exception ends the process with its reason,
// since the run means nothing without the thread.
template <typename Body>
void or_exit(const char* program, Body body) {
  try {
    body();
  } catch (const std::exception& error) {
    std::cerr << program << ": " << error.what() << std::endl;
    std::_Exit(1);
  }
}

// Lets every thread of a process start at once.
class Barrier {
 public:
  explicit Barrier(int count) : left_(count) {}
  void arrive_and_wait() {
    left_.fetch_sub(1);
    while (left_.load() > 0) {
      std::this_thread::yield();
    }
  }

 private:
  std::atomic<int> left_;
};

// What a consumer process received, from all its threads. The rate is
// the middle of the run's: the records from the arrival of the tenth of
// them to the arrival of nine tenths, over the time between, so that
// neither the start, a client's first connection and the broker's first
// metadata, nor a consumer that started behind and caught up from a
// backlog, nor the end, counts for either side. The latency is sampled on
// the records after the first tenth, every record's, or every n-th once
// there are millions.
class Tally {
 public:
  explicit Tally(std::uint64_t expect)
      : warm_up_(std::max<std::uint64_t>(1, expect / 10)),
        cool_down_(std::max<std::uint64_t>(warm_up_ + 1, expect - expect / 10)),
        every_(std::max<std::uint64_t>(1, expect / 4000000)) {}

  // A thread's own, merged into the process's when the thread ends.
  struct Local {
    std::uint64_t last_received = 0;
    std::vector<std::uint32_t> latencies_us;
  };
  void record(Local* local, const char* payload, std::size_t size, std::uint64_t received) {
    if (size < 8) {
      return;
    }
    const std::uint64_t sent = stamp_of(payload);
    const std::uint64_t number = count_.fetch_add(1, std::memory_order_relaxed) + 1;
    last_arrival_.store(received, std::memory_order_relaxed);
    local->last_received = received;
    if (number == warm_up_) {
      warm_at_.store(received, std::memory_order_relaxed);
    } else if (number == cool_down_) {
      cool_at_.store(received, std::memory_order_relaxed);
    }
    if (number > warm_up_ && number % every_ == 0) {
      local->latencies_us.push_back(static_cast<std::uint32_t>(received > sent ? (received - sent) / 1000 : 0));
    }
  }
  void merge(Local&& local) {
    std::lock_guard<std::mutex> lock(mutex_);
    last_received_ = std::max(last_received_, local.last_received);
    latencies_us_.insert(latencies_us_.end(), local.latencies_us.begin(), local.latencies_us.end());
  }

  std::uint64_t count() const { return count_.load(std::memory_order_relaxed); }
  // When a consumer thread should stop: every record in, or `quiet` seconds
  // since the last one arrived, or since `start` when none did.
  bool done(std::uint64_t expect, double quiet, std::uint64_t start) const {
    if (count() >= expect) {
      return true;
    }
    const std::uint64_t last = std::max(start, last_arrival_.load(std::memory_order_relaxed));
    return now_ns() - last > static_cast<std::uint64_t>(quiet * 1e9);
  }

  // The line the consumer prints: the records the rate counts, the rate,
  // and the latency's percentiles. A run that lost records before nine
  // tenths arrived is measured to its last arrival.
  std::string result(const std::string& system, int threads, std::uint64_t expect, std::size_t size) {
    std::lock_guard<std::mutex> lock(mutex_);
    std::sort(latencies_us_.begin(), latencies_us_.end());
    const auto percentile = [this](double fraction) -> std::uint32_t {
      return latencies_us_.empty() ? 0
                                   : latencies_us_[std::min(latencies_us_.size() - 1,
                                                            static_cast<std::size_t>(fraction * latencies_us_.size()))];
    };
    const std::uint64_t received = count(), warm_at = warm_at_.load(std::memory_order_relaxed);
    const bool whole = received >= cool_down_;
    const std::uint64_t until = whole ? cool_at_.load(std::memory_order_relaxed) : last_received_;
    const std::uint64_t measured = whole ? cool_down_ - warm_up_ : received > warm_up_ ? received - warm_up_ : 0;
    const double seconds = measured && until > warm_at ? (until - warm_at) / 1e9 : 0;
    std::ostringstream line;
    line << "{\"system\": \"" << system << "\", \"role\": \"consume\", \"threads\": " << threads
         << ", \"size\": " << size << ", \"expected\": " << expect << ", \"received\": " << received
         << ", \"measured\": " << measured << ", \"seconds\": " << seconds
         << ", \"records_per_second\": " << (seconds > 0 ? measured / seconds : 0)
         << ", \"megabytes_per_second\": " << (seconds > 0 ? measured * size / seconds / 1e6 : 0)
         << ", \"latency_us\": {\"p50\": " << percentile(0.5) << ", \"p99\": " << percentile(0.99)
         << ", \"p999\": " << percentile(0.999) << ", \"max\": " << (latencies_us_.empty() ? 0 : latencies_us_.back())
         << "}}";
    return line.str();
  }

 private:
  const std::uint64_t warm_up_;
  const std::uint64_t cool_down_;
  const std::uint64_t every_;
  std::atomic<std::uint64_t> count_{0};
  std::atomic<std::uint64_t> last_arrival_{0};
  std::atomic<std::uint64_t> warm_at_{0};
  std::atomic<std::uint64_t> cool_at_{0};
  std::mutex mutex_;
  std::uint64_t last_received_ = 0;
  std::vector<std::uint32_t> latencies_us_;
};

// The line a producer process prints: its records, from the moment its
// threads started to the moment the last one had everything handed over.
inline std::string produce_result(const std::string& system, const Options& options, std::uint64_t seconds_ns) {
  const std::uint64_t records = options.records * options.threads;
  const double seconds = seconds_ns / 1e9;
  std::ostringstream line;
  line << "{\"system\": \"" << system << "\", \"role\": \"produce\", \"threads\": " << options.threads
       << ", \"size\": " << options.size << ", \"records\": " << records << ", \"seconds\": " << seconds
       << ", \"records_per_second\": " << (seconds > 0 ? records / seconds : 0)
       << ", \"megabytes_per_second\": " << (seconds > 0 ? records * options.size / seconds / 1e6 : 0) << "}";
  return line.str();
}

}  // namespace bench

#endif  // MX_TESTS_KAFKA_COMMON_H_
