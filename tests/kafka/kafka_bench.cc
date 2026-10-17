// Kafka's side of the comparison (docs/kafka.md, tests/kafka/run.sh), on
// librdkafka, the C/C++ library Kafka's clients in most languages but Java
// are built on: `kafka_bench produce` sends records to a topic, a producer
// of its own for each thread; `kafka_bench consume` reads them, each thread
// a consumer assigned one partition of the topic from its beginning, run.sh
// making the topic with one partition for each consumer thread, and says
// how many came, how fast and how late, as mx_bench does. The client's
// defaults hold (acks from every in-sync replica, records batched for up to
// 5 ms, no compression) but for what --set name=value changes, as run.sh
// does for the consumer's fetch backoff and, for latency, the batching. A
// producer ends once the broker has acknowledged every record it sent.
// Built in tests/kafka/Dockerfile; common.h has the rest.
#include <librdkafka/rdkafkacpp.h>

#include <cstdint>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "tests/kafka/common.h"

namespace {

const int FLUSH_MS = 60000;

// Counts the records the broker did not take, which the producer reports.
class Deliveries : public RdKafka::DeliveryReportCb {
 public:
  void dr_cb(RdKafka::Message& message) override {
    if (message.err() != RdKafka::ERR_NO_ERROR) {
      failed.fetch_add(1);
    }
  }
  std::atomic<std::uint64_t> failed{0};
};

// The configuration with `settings` set, then the command line's, or the
// reason it cannot be.
std::unique_ptr<RdKafka::Conf> configuration(const bench::Options& options,
                                             std::vector<std::pair<std::string, std::string>> settings) {
  std::unique_ptr<RdKafka::Conf> conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
  std::string error;
  settings.insert(settings.end(), options.settings.begin(), options.settings.end());
  for (const auto& setting : settings) {
    if (conf->set(setting.first, setting.second, error) != RdKafka::Conf::CONF_OK) {
      throw std::runtime_error(setting.first + ": " + error);
    }
  }
  return conf;
}

int produce(const bench::Options& options) {
  bench::Barrier ready(options.threads);
  std::uint64_t start = 0;
  std::atomic<std::uint64_t> failed{0};
  std::vector<std::thread> threads;
  for (int index = 0; index < options.threads; ++index) {
    threads.emplace_back([&options, &ready, &start, &failed, index] {
      bench::or_exit("kafka_bench", [&] {
        Deliveries deliveries;
        std::unique_ptr<RdKafka::Conf> conf = configuration(options, {{"bootstrap.servers", options.address}});
        std::string error;
        if (conf->set("dr_cb", &deliveries, error) != RdKafka::Conf::CONF_OK) {
          throw std::runtime_error(error);
        }
        std::unique_ptr<RdKafka::Producer> producer(RdKafka::Producer::create(conf.get(), error));
        if (!producer) {
          throw std::runtime_error(error);
        }
        ready.arrive_and_wait();
        if (index == 0) {
          start = bench::now_ns();
        }
        const std::uint64_t mine = bench::now_ns();
        std::string payload(options.size, 'r');
        for (std::uint64_t sent = 0; sent < options.records; ++sent) {
          bench::pace(options.rate, mine, sent);
          bench::stamp(&payload);
          for (;;) {
            const RdKafka::ErrorCode code =
                producer->produce(options.topic, RdKafka::Topic::PARTITION_UA, RdKafka::Producer::RK_MSG_COPY,
                                  &payload[0], payload.size(), nullptr, 0, 0, nullptr);
            if (code == RdKafka::ERR__QUEUE_FULL) {
              producer->poll(10);  // the client's queue is full: let it send
              continue;
            }
            if (code != RdKafka::ERR_NO_ERROR) {
              throw std::runtime_error(RdKafka::err2str(code));
            }
            break;
          }
          producer->poll(0);
        }
        if (producer->flush(FLUSH_MS) != RdKafka::ERR_NO_ERROR) {
          throw std::runtime_error("records still unacknowledged after the flush");
        }
        failed.fetch_add(deliveries.failed.load());
      });
    });
  }
  for (std::thread& thread : threads) {
    thread.join();
  }
  if (failed.load()) {
    std::cerr << "kafka_bench: " << failed.load() << " records failed" << std::endl;
    return 1;
  }
  std::cout << bench::produce_result("kafka", options, bench::now_ns() - start) << std::endl;
  return 0;
}

int consume(const bench::Options& options) {
  bench::Tally tally(options.expect);
  bench::Barrier assigned(options.threads + 1);
  std::vector<std::thread> threads;
  for (int index = 0; index < options.threads; ++index) {
    threads.emplace_back([&options, &tally, &assigned, index] {
      bench::or_exit("kafka_bench", [&] {
        std::unique_ptr<RdKafka::Conf> conf = configuration(options, {{"bootstrap.servers", options.address},
                                                                      {"group.id", "bench-" + options.topic},
                                                                      {"enable.auto.commit", "false"}});
        std::string error;
        std::unique_ptr<RdKafka::KafkaConsumer> consumer(RdKafka::KafkaConsumer::create(conf.get(), error));
        if (!consumer) {
          throw std::runtime_error(error);
        }
        std::vector<RdKafka::TopicPartition*> partitions = {
            RdKafka::TopicPartition::create(options.topic, index, RdKafka::Topic::OFFSET_BEGINNING)};
        const RdKafka::ErrorCode code = consumer->assign(partitions);
        RdKafka::TopicPartition::destroy(partitions);
        if (code != RdKafka::ERR_NO_ERROR) {
          throw std::runtime_error(RdKafka::err2str(code));
        }
        assigned.arrive_and_wait();
        const std::uint64_t start = bench::now_ns();
        bench::Tally::Local mine;
        while (!tally.done(options.expect, options.quiet, start)) {
          std::unique_ptr<RdKafka::Message> message(consumer->consume(100));
          if (message->err() == RdKafka::ERR_NO_ERROR) {
            tally.record(&mine, static_cast<const char*>(message->payload()), message->len(), bench::now_ns());
          } else if (message->err() != RdKafka::ERR__TIMED_OUT) {
            std::cerr << "kafka_bench: " << message->errstr() << std::endl;
          }
        }
        tally.merge(std::move(mine));
        consumer->close();
      });
    });
  }
  assigned.arrive_and_wait();
  std::cout << "ready" << std::endl;  // run.sh starts the producers now
  for (std::thread& thread : threads) {
    thread.join();
  }
  std::cout << tally.result("kafka", options.threads, options.expect, options.size) << std::endl;
  return 0;
}

}  // namespace

int main(int argc, char** argv) {
  try {
    const bench::Options options = bench::parse(argc, argv);
    if (options.topic.empty()) {
      throw std::invalid_argument("--topic is needed");
    }
    return options.role == "produce" ? produce(options) : consume(options);
  } catch (const std::exception& error) {
    std::cerr << "kafka_bench: " << error.what() << std::endl;
    return 1;
  }
}
