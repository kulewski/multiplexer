#!/usr/bin/env bash
# The numbers in the README: request/reply round trips through one
# multiplexer on this machine, with the C++ test roles as the peers, one
# client in sequence and then eight in parallel, 200-byte payloads. Prints
# throughput and the latency distribution; the multiplexer's CPU share
# says how much of one core the broker itself needed.
#
#   ./tests/bench.sh [queries per client, default 20000]
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
count="${1:-20000}"
bazel build -c opt //mxcontrol //tests/roles/cc:mxtestroles > /dev/null 2>&1
mxcontrol=bazel-bin/mxcontrol/mxcontrol
roles=bazel-bin/tests/roles/cc/mxtestroles
work="$(mktemp -d)"
trap 'kill $backend $mx 2>/dev/null; rm -rf "$work"' EXIT

$mxcontrol run_multiplexer --rules tests/testing.rules --address 127.0.0.1:0 --port-file "$work/port" > "$work/mx.log" 2>&1 &
mx=$!
until [[ -f "$work/port" ]]; do sleep 0.05; done
address="$(cat "$work/port")"
# TEST_BACKEND_A (201) answers TEST_REQUEST_A (201) with TEST_RESPONSE (203); TEST_CLIENT is 203.
$roles backend --mx "$address" --type 201 --serves 201=203 --behaviour echo > "$work/backend.log" 2>&1 &
backend=$!
until grep -q connected "$work/backend.log"; do sleep 0.05; done

for parallel in 1 8; do
  start=$(date +%s.%N)
  $roles client --mx "$address" --type 203 --query "201:hello" --count "$count" --parallel "$parallel" --payload-size 200 \
    > "$work/client.log" 2> /dev/null
  end=$(date +%s.%N)
  python3 - "$work/client.log" "$start" "$end" "$parallel" <<'PY'
import re, statistics, sys
ms = sorted(float(m.group(1)) for m in re.finditer(r"ms: ([0-9.]+)", open(sys.argv[1]).read()))
wall = float(sys.argv[3]) - float(sys.argv[2])
print("%s client(s): %d round trips in %.2f s = %.0f/s; median %.0f us, p99 %.0f us"
      % (sys.argv[4], len(ms), wall, len(ms) / wall, ms[len(ms) // 2] * 1000, ms[int(len(ms) * 0.99)] * 1000))
PY
done
echo "multiplexer: $(ps -o %cpu= -p $mx | tr -d ' ')% of one core over its life, $(($(ps -o rss= -p $mx) / 1024)) MB resident"
