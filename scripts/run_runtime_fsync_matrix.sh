#!/usr/bin/env sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
EXE=${1:-"$ROOT/build/release/detlog-bench"}
OUT=${2:-"$ROOT/bench-results/runtime-fsync-$(date +%Y%m%d-%H%M%S)"}
REPETITIONS=${3:-3}
OPERATIONS=${4:-1000}
GROUP_SIZES=${GROUP_SIZES-"2 5"}
GROUP_DELAY_MS=${GROUP_DELAY_MS-2}
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
case "$GROUP_DELAY_MS" in
  ''|*[!0-9]*|0|0[0-9]*) echo "group delay must be a canonical positive integer" >&2; exit 2 ;;
esac
if [ "$GROUP_DELAY_MS" -gt 1000 ]; then
  echo "group delay must not exceed 1000 milliseconds" >&2
  exit 2
fi
if [ "$OPERATIONS" -le 5 ]; then
  echo "operations must exceed the largest client count (5) for fault runs" >&2
  exit 2
fi
if [ ! -x "$EXE" ]; then
  echo "benchmark executable not found or not executable: $EXE" >&2
  exit 2
fi
group_count=0
first_group=
for group in $GROUP_SIZES; do
  case "$group" in
    ''|*[!0-9]*|0|1|0[0-9]*) echo "group sizes must be canonical integers in 2..1024" >&2; exit 2 ;;
  esac
  if [ "$group" -lt 2 ] || [ "$group" -gt 1024 ]; then
    echo "group sizes must be integers in 2..1024" >&2
    exit 2
  fi
  group_count=$((group_count + 1))
  if [ -z "$first_group" ]; then first_group=$group; fi
done
if [ "$group_count" -eq 0 ]; then
  echo "GROUP_SIZES must be nonempty" >&2
  exit 2
fi

executable_json=$(
  "$PYTHON" - "$EXE" <<'PY'
import json
import sys

print(json.dumps(sys.argv[1], ensure_ascii=True))
PY
)
expected_runs=$((36 * (1 + group_count) * REPETITIONS))

prepare_output_directory "$OUT"
RAW="$OUT/raw.jsonl"
ERRORS="$OUT/stderr.log"
: >"$RAW"
: >"$ERRORS"
generated_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
group_json=$(printf '%s\n' $GROUP_SIZES | awk 'BEGIN { first=1; printf "[" } { if (!first) printf ", "; printf "%s", $1; first=0 } END { print "]" }')
cat >"$OUT/matrix.json" <<EOF
{
  "schema": "detlog-runtime-fsync-matrix/v1",
  "generated_at": "$generated_at",
  "executable": $executable_json,
  "repetitions": $REPETITIONS,
  "operations_per_run": $OPERATIONS,
  "expected_runs": $expected_runs,
  "mode": "tcp",
  "nodes": [3, 5],
  "clients": [1, 3, 5],
  "payload_bytes": [64, 1024, 16384],
  "scenarios": ["healthy", "leader-crash"],
  "policies": ["flush-every", "group"],
  "group_sizes": $group_json,
  "group_delay_ms": $GROUP_DELAY_MS,
  "note": "End-to-end NodeHost comparison over real loopback TCP and distinct WAL files."
}
EOF

run_index=0
for nodes in 3 5; do
  for clients in 1 3 5; do
    for payload in 64 1024 16384; do
      for scenario in healthy leader-crash; do
        for variant in flush-every $GROUP_SIZES; do
          if [ "$variant" = flush-every ]; then
            policy=flush-every
            group_size=$first_group
          else
            policy=group
            group_size=$variant
          fi
          trial=1
          while [ "$trial" -le "$REPETITIONS" ]; do
            run_index=$((run_index + 1))
            seed=$((2000000 * trial + run_index))
            echo "[$run_index] $scenario n=$nodes c=$clients p=$payload fsync=$policy g=$group_size trial=$trial"
            if ! "$EXE" --mode tcp --nodes "$nodes" --clients "$clients" \
                --payload "$payload" --operations "$OPERATIONS" \
                --trial "$trial" --seed "$seed" --scenario "$scenario" \
                --fsync-policy "$policy" --group-size "$group_size" \
                --group-delay-ms "$GROUP_DELAY_MS" >>"$RAW" 2>>"$ERRORS"; then
              echo "benchmark failed; raw output remains in $RAW" >&2
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
echo "Runtime fsync benchmark artifacts: $OUT"
