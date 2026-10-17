#!/usr/bin/env bash
# The numbers in the README: request/reply round trips through one
# multiplexer on this machine, with the test roles of one language or two
# as the peers, the C++ ones by default, one client in sequence and then
# eight in parallel, 200-byte payloads. Prints throughput and the latency
# distribution; the multiplexer's CPU share says how much of one core the
# broker itself needed. The Python backend serves on
# BaseThreadedMultiplexerServer, its io thread handing every request to
# Python through a callback; its client is a SyncClient per worker.
#
#   ./tests/bench.sh [queries per client, default 20000] [backend: cc, rust or py, default cc] [client: default the backend's]
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
count="${1:-20000}"
backend_lang="${2:-cc}"
client_lang="${3:-$backend_lang}"
targets=(//mxcontrol)
for lang in "$backend_lang" "$client_lang"; do
  case "$lang" in
    cc | rust) targets+=("//tests/roles/$lang:mxtestroles") ;;
    py) targets+=(//tests/roles/py:backend //tests/roles/py:client) ;;
    *) echo "bench.sh: a language is cc, rust or py, not $lang" >&2 && exit 2 ;;
  esac
done
bazel build -c opt "${targets[@]}" > /dev/null 2>&1
mxcontrol=bazel-bin/mxcontrol/mxcontrol
# The command of `role` in `lang`: one binary of roles in C++ and Rust, one
# binary each in Python.
role() {
  local lang=$1 role=$2
  if [[ "$lang" == py ]]; then
    echo "bazel-bin/tests/roles/py/$role"
  else
    echo "bazel-bin/tests/roles/$lang/mxtestroles $role"
  fi
}
threaded=()
[[ "$backend_lang" == py ]] && threaded=(--threaded)
work="$(mktemp -d)"
trap 'kill $backend $mx 2>/dev/null; rm -rf "$work"' EXIT

$mxcontrol run_multiplexer --rules tests/testing.rules --address 127.0.0.1:0 --port-file "$work/port" > "$work/mx.log" 2>&1 &
mx=$!
until [[ -f "$work/port" ]]; do sleep 0.05; done
address="$(cat "$work/port")"
# TEST_BACKEND_A (201) answers TEST_REQUEST_A (201) with TEST_RESPONSE (203); TEST_CLIENT is 203.
$(role "$backend_lang" backend) --mx "$address" --type 201 --serves 201=203 --behaviour echo "${threaded[@]}" \
  > "$work/backend.log" 2>&1 &
backend=$!
until grep -q connected "$work/backend.log"; do sleep 0.05; done

echo "backend: $backend_lang, client: $client_lang"
for parallel in 1 8; do
  start=$(date +%s.%N)
  $(role "$client_lang" client) --mx "$address" --type 203 --query "201:hello" --count "$count" --parallel "$parallel" \
    --payload-size 200 \
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
