#!/usr/bin/env sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
EXE=${1:-"$ROOT/build/release/detlog-bench"}
OUT=${2:-"$ROOT/bench-results/$(date +%Y%m%d-%H%M%S)"}
REPETITIONS=${3:-3}
OPERATIONS=${4:-1000}
PYTHON=${PYTHON:-python3}

prepare_output_directory() {
  directory=$1
  if [ -e "$directory" ]; then
    if [ ! -d "$directory" ]; then
      echo "benchmark output path exists and is not a directory: $directory" >&2
      exit 2
    fi
    for existing in "$directory"/* "$directory"/.[!.]* "$directory"/..?*; do
      if [ -e "$existing" ] || [ -L "$existing" ]; then
        echo "benchmark output directory must be empty: $directory" >&2
        exit 2
      fi
    done
  else
    mkdir -p "$directory"
  fi
}

case "$REPETITIONS" in
  ''|*[!0-9]*|0|0[0-9]*) echo "repetitions must be a canonical positive integer" >&2; exit 2 ;;
esac
case "$OPERATIONS" in
  ''|*[!0-9]*|0|0[0-9]*) echo "operations must be a canonical positive integer" >&2; exit 2 ;;
esac
if [ "$OPERATIONS" -le 5 ]; then
  echo "operations must exceed the largest client count (5)" >&2
  exit 2
fi
if [ ! -x "$EXE" ]; then
  echo "benchmark executable not found or not executable: $EXE" >&2
  exit 2
fi
executable_json=$(
  "$PYTHON" - "$EXE" <<'PY'
import json
import sys

print(json.dumps(sys.argv[1], ensure_ascii=True))
PY
)
generated_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
expected_runs=$((144 * REPETITIONS))

prepare_output_directory "$OUT"
RAW="$OUT/raw.jsonl"
ERRORS="$OUT/stderr.log"
: >"$RAW"
: >"$ERRORS"
cat >"$OUT/matrix.json" <<EOF
{
  "schema": "detlog-bench-matrix/v1",
  "generated_at": "$generated_at",
  "executable": $executable_json,
  "repetitions": $REPETITIONS,
  "operations_per_run": $OPERATIONS,
  "expected_runs": $expected_runs,
  "nodes": [3, 5],
  "clients": [1, 3, 5],
  "payload_bytes": [64, 1024, 16384],
  "sim_scenarios": ["healthy", "leader-crash", "partition", "slow-follower", "slow-fsync"],
  "tcp_scenarios": ["healthy", "leader-crash", "partition"],
  "tcp_unsupported": ["slow-follower", "slow-fsync"],
  "tcp_partition_ms": 1000,
  "tcp_wal_flush_policy": "flush-every",
  "sim_wal_flush_policy": "simulated_flush_every",
  "note": "TCP nodes share one benchmark process but use real loopback sockets and distinct WALs; partition is a literal one-second bidirectional transport cut."
}
EOF

run_index=0
for mode in sim tcp; do
  if [ "$mode" = sim ]; then
    scenarios="healthy leader-crash partition slow-follower slow-fsync"
  else
    scenarios="healthy leader-crash partition"
  fi
  for nodes in 3 5; do
    for clients in 1 3 5; do
      for payload in 64 1024 16384; do
        for scenario in $scenarios; do
          trial=1
          while [ "$trial" -le "$REPETITIONS" ]; do
            run_index=$((run_index + 1))
            seed=$((1000000 * trial + run_index))
            echo "[$run_index] $mode $scenario n=$nodes c=$clients p=$payload trial=$trial"
            if ! "$EXE" --mode "$mode" --nodes "$nodes" --clients "$clients" \
                --payload "$payload" --operations "$OPERATIONS" \
                --trial "$trial" --seed "$seed" --scenario "$scenario" \
                --partition-ms 1000 \
                >>"$RAW" 2>>"$ERRORS"; then
              echo "benchmark failed; raw output was preserved in $RAW" >&2
              exit 1
            fi
            trial=$((trial + 1))
          done
        done
      done
    done
  done
done

"$PYTHON" "$SCRIPT_DIR/validate_benchmark_artifacts.py" "$OUT"

"$PYTHON" "$SCRIPT_DIR/plot_bench.py" "$RAW" \
  --csv "$OUT/summary.csv" --svg "$OUT/throughput.svg"
echo "Benchmark artifacts: $OUT"
