#!/usr/bin/env sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
EXE=${1:-"$ROOT/build/release/detlog-bench"}
OUT=${2:-"$ROOT/bench-results/$(date +%Y%m%d-%H%M%S)"}
REPETITIONS=${3:-3}
OPERATIONS=${4:-1000}
PYTHON=${PYTHON:-python3}

case "$REPETITIONS:$OPERATIONS" in
  *[!0-9:]*|0:*|*:0) echo "repetitions and operations must be positive integers" >&2; exit 2 ;;
esac
if [ ! -x "$EXE" ]; then
  echo "benchmark executable not found or not executable: $EXE" >&2
  exit 2
fi
case "$EXE" in
  *'"'*|*'\'*)
    escaped_executable=$(printf '%s' "$EXE" | sed 's/\\/\\\\/g; s/"/\\"/g')
    ;;
  *) escaped_executable=$EXE ;;
esac
generated_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)

mkdir -p "$OUT"
RAW="$OUT/raw.jsonl"
ERRORS="$OUT/stderr.log"
: >"$RAW"
: >"$ERRORS"
cat >"$OUT/matrix.json" <<EOF
{
  "generated_at": "$generated_at",
  "executable": "$escaped_executable",
  "repetitions": $REPETITIONS,
  "operations_per_run": $OPERATIONS,
  "nodes": [3, 5],
  "clients": [1, 3, 5],
  "payload_bytes": [64, 1024, 16384],
  "sim_scenarios": ["healthy", "leader-crash", "partition", "slow-follower", "slow-fsync"],
  "tcp_scenarios": ["healthy", "leader-crash"],
  "tcp_unsupported": ["partition", "slow-follower", "slow-fsync"],
  "note": "TCP nodes share one benchmark process but use real loopback sockets and distinct WALs."
}
EOF

run_index=0
for mode in sim tcp; do
  if [ "$mode" = sim ]; then
    scenarios="healthy leader-crash partition slow-follower slow-fsync"
  else
    scenarios="healthy leader-crash"
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

"$PYTHON" "$SCRIPT_DIR/plot_bench.py" "$RAW" \
  --csv "$OUT/summary.csv" --svg "$OUT/throughput.svg"
echo "Benchmark artifacts: $OUT"
