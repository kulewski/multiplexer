#!/usr/bin/env bash
# The Kafka comparison (docs/kafka.md): records from producers to consumers
# through one multiplexer and through one Kafka broker on this machine, in
# two flavours, each run three times and the median of each figure kept:
# both systems tuned for throughput, their producers batching, and both
# tuned for latency, neither batching. Tuned for throughput, two tables:
# the records a second each moves as fast as it can, and how late a record
# arrives at a steady rate; tuned for latency, the second alone, since as
# fast as it goes Kafka's producer with linger.ms=0 still gathers what
# piles up while a request is on its way, so it batches after all, which
# the first flavour compares. Every process is in a container on the host's
# network: the multiplexer and both systems' producers and consumers from
# the image tests/kafka/Dockerfile builds from the working tree, the broker
# from the official image, apache/kafka. Needs Docker and python3, and ports
# 9092 and 9093, Kafka's, and MX_PORT free. Takes about forty minutes.
#
#   tests/kafka/run.sh               every configuration
#   SCALE=0.1 tests/kafka/run.sh     a tenth of the records: a look at the scripts,
#                                    Kafka's start-up still in its figures
#   REPEAT=1 tests/kafka/run.sh      each run once
#   SYSTEMS=mx tests/kafka/run.sh    the multiplexer's side alone (or kafka)
#   MX_MULTIPLEXERS=2 tests/kafka/run.sh
#                                    the multiplexer's side as two multiplexers, on
#                                    MX_PORT and the port after it, every producer
#                                    and consumer connected to both, as a host
#                                    runs more than one
#   MX_ARGS="..." tests/kafka/run.sh run_multiplexer with these arguments as well
#   MX_CLIENT=sync tests/kafka/run.sh
#                                    the multiplexer's producers on that class in
#                                    both flavours (sync or threaded), rather than
#                                    on ThreadedClient tuned for throughput and on
#                                    SyncClient tuned for latency
#   RESULTS=tests/kafka/results.jsonl tests/kafka/run.sh
#                                    every run's line kept in that file too, which
#                                    docs/kafka_charts.py draws docs/kafka.md's
#                                    charts and writes its tables from
#
# The configurations: one producer and one consumer, four and four, and
# fewer producers than consumers, one and four, four and eight, so that one
# producer's records are shared out over several consumers, as a Kafka
# producer's are over the partitions of a topic.
# For throughput, the multiplexer's producer batches its records
# (set_delivery with batch, MX_THROUGHPUT) on a ThreadedClient, whose io thread batches
# and writes while the program sends, as Kafka's producer library does on
# threads of its own, and Kafka's producer runs with its client's
# defaults, records batched for up to 5 ms; Kafka's consumer runs without
# its pause of a second whenever 100,000 records wait in it unread
# (fetch.queue.backoff.ms), which held it to about 220,000 records a second,
# in both flavours. For latency, neither batches: the multiplexer's
# producer sends each record on its own from the program's thread, on a
# SyncClient, and Kafka's with linger.ms=0. The
# containers are named mx-kafka-bench-*, and removed at the end whatever
# happens; the image, mx-kafka-bench, stays for the next run.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."

KAFKA_IMAGE="${KAFKA_IMAGE:-apache/kafka:4.3.1}"
IMAGE=mx-kafka-bench
MX_PORT="${MX_PORT:-19800}"
MX_MULTIPLEXERS="${MX_MULTIPLEXERS:-1}"
MX_CLIENT="${MX_CLIENT:-}"
read -r -a MX_EXTRA <<< "${MX_ARGS:-}"
KAFKA_ADDRESS=localhost:9092
SCALE="${SCALE:-1}"
REPEAT="${REPEAT:-3}"
read -r -a systems <<< "${SYSTEMS:-mx kafka}"
# The multiplexers: their containers, ports and the addresses every peer
# connects to, comma-separated.
mx_names=() mx_ports=()
for ((index = 0; index < MX_MULTIPLEXERS; ++index)); do
  if ((index == 0)); then
    mx_names+=(mx-kafka-bench-mx)
  else
    mx_names+=("mx-kafka-bench-mx$((index + 1))")
  fi
  mx_ports+=($((MX_PORT + index)))
done
MX_ADDRESS="$(printf '127.0.0.1:%s,' "${mx_ports[@]}")"
MX_ADDRESS="${MX_ADDRESS%,}"
docker=(docker)
docker info > /dev/null 2>&1 || docker=(sudo docker)

# producers consumers records-each size rate: as fast as it goes, about ten
# seconds of sending for the faster system, then at a steady rate, which
# each system takes with room to spare, ten seconds.
fastest=(
  "1 1 30000000 100 0"
  "1 4 30000000 100 0"
  "4 4 25000000 100 0"
  "4 8 25000000 100 0"
  "1 1 20000000 1000 0"
  "1 4 20000000 1000 0"
  "4 4 10000000 1000 0"
  "4 8 10000000 1000 0"
)
steady=(
  "1 1 100000 100 10000"
  "1 1 1000000 100 100000"
)
# Each flavour's settings: the multiplexer's batches hold 1,000 records at
# most and wait 5 ms at most, as Kafka's producer gathers a batch for 5 ms.
MX_THROUGHPUT=(--batch-records 1000 --batch-delay 0.005 --client "${MX_CLIENT:-threaded}")
MX_LATENCY=(--client "${MX_CLIENT:-sync}")
KAFKA_THROUGHPUT=(--set fetch.queue.backoff.ms=0)
KAFKA_LATENCY=(--set fetch.queue.backoff.ms=0 --set linger.ms=0)

work="$(mktemp -d)"
containers=("${mx_names[@]}" mx-kafka-bench-kafka mx-kafka-bench-consumer mx-kafka-bench-producer)
cleanup() {
  "${docker[@]}" rm -f "${containers[@]}" > /dev/null 2>&1 || true
  rm -rf "$work"
}
trap cleanup EXIT
"${docker[@]}" rm -f "${containers[@]}" > /dev/null 2>&1 || true # a run killed before its cleanup

for port in 9092 9093 "${mx_ports[@]}"; do
  if (exec 3<> "/dev/tcp/127.0.0.1/$port") 2> /dev/null; then
    echo "run.sh: port $port is taken; free it, or give MX_PORT another" >&2 && exit 1
  fi
done

echo "== building $IMAGE from the working tree, pulling $KAFKA_IMAGE"
mkdir "$work/context"
git ls-files -z --cached --others --exclude-standard \
  | while IFS= read -r -d '' path; do [[ -e "$path" ]] && printf '%s\0' "$path"; done \
  | tar --null -T - -c | tar -x -C "$work/context"
"${docker[@]}" build -q -f tests/kafka/Dockerfile -t "$IMAGE" "$work/context" > /dev/null
"${docker[@]}" pull -q "$KAFKA_IMAGE" > /dev/null

# Waits until `command` succeeds, `seconds` at most.
wait_for() {
  local seconds=$1
  shift
  local deadline=$((SECONDS + seconds))
  until "$@" > /dev/null 2>&1; do
    ((SECONDS < deadline)) || { echo "run.sh: gave up waiting for: $*" >&2 && return 1; }
    sleep 0.2
  done
}
listening() { (exec 3<> "/dev/tcp/127.0.0.1/$1") 2> /dev/null; }
kafka_tool() { "${docker[@]}" exec mx-kafka-bench-kafka "/opt/kafka/bin/$1" --bootstrap-server "$KAFKA_ADDRESS" "${@:2}"; }

echo "== starting the multiplexer and the broker"
for ((index = 0; index < MX_MULTIPLEXERS; ++index)); do
  "${docker[@]}" run -d --name "${mx_names[index]}" --network host "$IMAGE" \
    mxcontrol run_multiplexer --rules /src/tests/kafka/bench.rules --address "127.0.0.1:${mx_ports[index]}" \
    "${MX_EXTRA[@]}" > /dev/null
done
"${docker[@]}" run -d --name mx-kafka-bench-kafka --network host "$KAFKA_IMAGE" > /dev/null
for port in "${mx_ports[@]}"; do
  wait_for 30 listening "$port"
done
wait_for 120 kafka_tool kafka-topics.sh --list

# The CPU seconds a system's broker has used so far: every multiplexer's
# together, or Kafka's broker's.
broker_seconds() {
  if [[ "$1" == mx ]]; then
    local name total=0
    for name in "${mx_names[@]}"; do
      total="$(awk -v a="$total" -v b="$(cpu_seconds "$name")" 'BEGIN {printf "%.3f", a + b}')"
    done
    echo "$total"
  else
    cpu_seconds mx-kafka-bench-kafka
  fi
}

# The CPU seconds a container has used so far, from its cgroup.
cpu_seconds() {
  local id
  id="$("${docker[@]}" inspect -f '{{.Id}}' "$1")"
  for dir in "/sys/fs/cgroup/system.slice/docker-$id.scope" "/sys/fs/cgroup/docker/$id"; do
    if [[ -r "$dir/cpu.stat" ]]; then
      awk '/^usage_usec/ {printf "%.3f", $2 / 1e6}' "$dir/cpu.stat"
      return
    fi
  done
  echo 0
}

# One run: FLAVOUR SYSTEM PRODUCERS CONSUMERS RECORDS SIZE RATE. The
# consumers first, ready once connected (assigned their partitions, for
# Kafka), then the producers; the run's line, the consumer's with the
# broker's CPU seconds from the producers' start to the consumers' end for
# each million records, goes to results.jsonl. A Kafka run's topic goes
# after it. The flavour's settings go to the producer; Kafka's consumer
# takes its own too.
run=0
one() {
  local flavour=$1 system=$2 producers=$3 consumers=$4 records=$5 size=$6 rate=$7
  local program address settings=() consumer_settings=() topic=()
  run=$((run + 1))
  if [[ "$system" == mx ]]; then
    program=mx_bench address="$MX_ADDRESS"
    if [[ "$flavour" == throughput ]]; then settings=("${MX_THROUGHPUT[@]}"); else settings=("${MX_LATENCY[@]}"); fi
  else
    program=kafka_bench address="$KAFKA_ADDRESS"
    topic=(--topic "bench-$run")
    if [[ "$flavour" == throughput ]]; then settings=("${KAFKA_THROUGHPUT[@]}"); else settings=("${KAFKA_LATENCY[@]}"); fi
    consumer_settings=("${settings[@]}")
    kafka_tool kafka-topics.sh --create --topic "bench-$run" --partitions "$consumers" --replication-factor 1 > /dev/null
  fi
  : > "$work/consume.out"
  "${docker[@]}" run --rm --name mx-kafka-bench-consumer --network host "$IMAGE" \
    "$program" consume --address "$address" "${topic[@]}" "${consumer_settings[@]}" --threads "$consumers" \
    --size "$size" --expect $((producers * records)) > "$work/consume.out" 2> "$work/consume.err" &
  local consumer=$!
  wait_for 60 grep -q ready "$work/consume.out"
  local before
  before="$(broker_seconds "$system")"
  "${docker[@]}" run --rm --name mx-kafka-bench-producer --network host "$IMAGE" \
    "$program" produce --address "$address" "${topic[@]}" "${settings[@]}" --threads "$producers" \
    --records "$records" --size "$size" --rate "$rate" > "$work/produce.out" 2> "$work/produce.err" ||
    { cat "$work/produce.err" >&2 && return 1; }
  wait "$consumer" || { cat "$work/consume.err" >&2 && return 1; }
  python3 - "$work" "$flavour" "$before" "$(broker_seconds "$system")" "$rate" << 'PY' >> "$work/results.jsonl"
import json, sys
work, flavour = sys.argv[1], sys.argv[2]
before, after, rate = map(float, sys.argv[3:])
consumed = json.loads(open(work + "/consume.out").read().split("ready", 1)[1].strip())
produced = json.loads(open(work + "/produce.out").read().strip())
consumed.update(flavour=flavour, producers=produced["threads"], rate=rate,
                sent_per_second=produced["records_per_second"],
                broker_cpu_per_million=(after - before) / max(1, consumed["received"]) * 1e6)
print(json.dumps(consumed))
PY
  if [[ "$system" == kafka ]]; then
    kafka_tool kafka-topics.sh --delete --topic "bench-$run" > /dev/null
  fi
  tail -1 "$work/results.jsonl" | python3 -c '
import json, sys
r = json.loads(sys.stdin.read())
print("   %-5s %9.0f records/s, %d lost, latency p50 %d us, p99 %d us" % (r["system"], r["records_per_second"],
      r["expected"] - r["received"], r["latency_us"]["p50"], r["latency_us"]["p99"]))'
}

for flavour in throughput latency; do
  configurations=("${fastest[@]}" "${steady[@]}")
  [[ "$flavour" == latency ]] && configurations=("${steady[@]}")
  for configuration in "${configurations[@]}"; do
    read -r producers consumers records size rate <<< "$configuration"
    records="$(awk -v n="$records" -v s="$SCALE" 'BEGIN {printf "%d", n * s}')"
    echo "== tuned for $flavour: $producers producer(s), $consumers consumer(s), $records records each of $size bytes$( ((rate)) && echo ", $rate a second each")"
    for ((repeat = 0; repeat < REPEAT; ++repeat)); do
      for system in "${systems[@]}"; do
        one "$flavour" "$system" "$producers" "$consumers" "$records" "$size" "$rate"
      done
    done
  done
done

if [[ -n "${RESULTS:-}" ]]; then
  cp "$work/results.jsonl" "$RESULTS"
fi
echo
python3 - "$work/results.jsonl" "$KAFKA_IMAGE" "$MX_MULTIPLEXERS" << 'PY'
import json, statistics, sys
runs = [json.loads(line) for line in open(sys.argv[1])]
groups = {}
for r in runs:
    groups.setdefault((r["flavour"], r["producers"], r["threads"], r["size"], r["rate"], r["system"]), []).append(r)
def median(key, group):
    return statistics.median(key(r) for r in group)
def number(value):
    return format(round(value), ",")
NAMES = {"mx": "multiplexer" if sys.argv[3] == "1" else sys.argv[3] + " multiplexers", "kafka": "Kafka"}
TITLES = {"throughput": "Tuned for throughput, both batching", "latency": "Tuned for latency, neither batching"}
order = sorted(groups, key=lambda k: (k[0] != "throughput", k[4], k[3], k[1], k[2], k[5] != "mx"))
for flavour in ("throughput", "latency"):
    if flavour == "throughput":
        print("%s, as fast as each goes:\n" % TITLES[flavour])
        print("| Producers → consumers | Record | System | Records/s | Sent/s | p50 latency | Broker CPU s per million records | Lost |")
        print("|---|---|---|---|---|---|---|---|")
        for key in order:
            if key[0] != flavour or key[4]:
                continue
            _, producers, consumers, size, rate, system = key
            group = groups[key]
            print("| %d → %d | %d B | %s | %s | %s | %s µs | %.2f | %s |" % (
                producers, consumers, size, NAMES[system], number(median(lambda r: r["records_per_second"], group)),
                number(median(lambda r: r["sent_per_second"], group)),
                number(median(lambda r: r["latency_us"]["p50"], group)),
                median(lambda r: r["broker_cpu_per_million"], group),
                number(median(lambda r: r["expected"] - r["received"], group))))
        print()
    print("%s, at a steady rate, how late a record arrives:\n" % TITLES[flavour])
    print("| Rate | Record | System | p50 | p99 | p99.9 | Max | Lost |")
    print("|---|---|---|---|---|---|---|---|")
    for key in order:
        if key[0] != flavour or not key[4]:
            continue
        _, producers, consumers, size, rate, system = key
        group = groups[key]
        print("| %s/s | %d B | %s | %s µs | %s µs | %s µs | %s µs | %s |" % (
            number(rate * producers), size, NAMES[system],
            *(number(median(lambda r, p=p: r["latency_us"][p], group)) for p in ("p50", "p99", "p999", "max")),
            number(median(lambda r: r["expected"] - r["received"], group))))
    print()
print("Kafka: %s; each figure the median of %d runs." % (sys.argv[2], len(runs) // len(groups)))
PY
