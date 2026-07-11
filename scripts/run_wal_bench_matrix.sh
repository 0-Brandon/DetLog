#!/usr/bin/env sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
EXE=${1:-"$ROOT/build/release/detlog-wal-bench"}
OUT=${2:-"$ROOT/bench-results/wal-$(date +%Y%m%d-%H%M%S)"}
REPETITIONS=${3:-3}
ENTRY_SIZES=${ENTRY_SIZES:-"100 1000 5000"}
PAYLOAD_BYTES=${PAYLOAD_BYTES:-"64 1024 8192"}
GROUP_SIZES=${GROUP_SIZES:-"8 32"}
PYTHON=${PYTHON:-python3}

case "$REPETITIONS" in
  ''|*[!0-9]*|0) echo "repetitions must be a positive integer" >&2; exit 2 ;;
esac
if [ "$REPETITIONS" -gt 1000 ]; then
  echo "repetitions must not exceed 1000" >&2
  exit 2
fi
if [ ! -x "$EXE" ]; then
  echo "WAL benchmark executable not found or not executable: $EXE" >&2
  exit 2
fi
case "$EXE" in
  *'"'*|*'\'*)
    escaped_executable=$(printf '%s' "$EXE" | sed 's/\\/\\\\/g; s/"/\\"/g')
    ;;
  *) escaped_executable=$EXE ;;
esac
generated_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)

entry_count=0
for entries in $ENTRY_SIZES; do
  case "$entries" in
    ''|*[!0-9]*|0) echo "entry sizes must be positive integers" >&2; exit 2 ;;
  esac
  if [ "$entries" -gt 1000000 ]; then
    echo "entry sizes must not exceed 1000000" >&2
    exit 2
  fi
  entry_count=$((entry_count + 1))
done
if [ "$entry_count" -eq 0 ]; then
  echo "ENTRY_SIZES must be nonempty" >&2
  exit 2
fi

payload_count=0
for payload in $PAYLOAD_BYTES; do
  case "$payload" in
    ''|*[!0-9]*) echo "payload sizes must be nonnegative integers" >&2; exit 2 ;;
  esac
  if [ "$payload" -gt 8388608 ]; then
    echo "payload sizes must not exceed 8388608" >&2
    exit 2
  fi
  payload_count=$((payload_count + 1))
done
if [ "$payload_count" -eq 0 ]; then
  echo "PAYLOAD_BYTES must be nonempty" >&2
  exit 2
fi

group_count=0
for group in $GROUP_SIZES; do
  case "$group" in
    ''|*[!0-9]*|0|1) echo "group sizes must be integers in 2..1024" >&2; exit 2 ;;
  esac
  if [ "$group" -gt 1024 ]; then
    echo "group sizes must be integers in 2..1024" >&2
    exit 2
  fi
  group_count=$((group_count + 1))
done
if [ "$group_count" -eq 0 ]; then
  echo "GROUP_SIZES must be nonempty" >&2
  exit 2
fi

number_array() {
  values=$1
  first=true
  printf '['
  for value in $values; do
    if [ "$first" = true ]; then
      first=false
    else
      printf ','
    fi
    printf '%s' "$value"
  done
  printf ']'
}

ENTRY_JSON=$(number_array "$ENTRY_SIZES")
PAYLOAD_JSON=$(number_array "$PAYLOAD_BYTES")
GROUP_JSON=$(number_array "$GROUP_SIZES")
expected_runs=$((entry_count * payload_count * REPETITIONS * (2 + group_count)))

mkdir -p "$OUT"
RAW="$OUT/raw-includes-nondurable.jsonl"
ERRORS="$OUT/stderr.log"
MATRIX="$OUT/matrix-manifest.json"
SUMMARY="$OUT/summary-includes-nondurable.csv"
FIGURES="$OUT/wal-figures-includes-nondurable.svg"
: >"$RAW"
: >"$ERRORS"
rm -f -- "$SUMMARY" "$FIGURES"
cat >"$MATRIX" <<EOF
{
  "schema": "detlog-wal-bench-matrix/v1",
  "generated_at": "$generated_at",
  "executable": "$escaped_executable",
  "repetitions": $REPETITIONS,
  "entry_sizes": $ENTRY_JSON,
  "payload_bytes": $PAYLOAD_JSON,
  "policies": ["flush-every", "group", "unsafe-no-flush"],
  "group_sizes": $GROUP_JSON,
  "expected_runs": $expected_runs,
  "raw_jsonl": "raw-includes-nondurable.jsonl",
  "diagnostics": "stderr.log",
  "derived_csv": "summary-includes-nondurable.csv",
  "derived_svg": "wal-figures-includes-nondurable.svg",
  "unsafe_policy_note": "UNSAFE: append acknowledgements are nondurable; each run performs and times an explicit final flush before reopen recovery."
}
EOF

run_index=0
run_benchmark() {
  entries=$1
  payload=$2
  trial=$3
  policy=$4
  group=${5:-}
  run_index=$((run_index + 1))
  if [ -n "$group" ]; then
    echo "[$run_index/$expected_runs] policy=$policy entries=$entries payload=$payload trial=$trial group=$group"
    if ! "$EXE" --entries "$entries" --payload "$payload" --trial "$trial" \
        --policy "$policy" --group-size "$group" >>"$RAW" 2>>"$ERRORS"; then
      echo "WAL benchmark failed; artifacts were preserved in $OUT" >&2
      exit 1
    fi
  else
    echo "[$run_index/$expected_runs] policy=$policy entries=$entries payload=$payload trial=$trial"
    if ! "$EXE" --entries "$entries" --payload "$payload" --trial "$trial" \
        --policy "$policy" >>"$RAW" 2>>"$ERRORS"; then
      echo "WAL benchmark failed; artifacts were preserved in $OUT" >&2
      exit 1
    fi
  fi
}

for entries in $ENTRY_SIZES; do
  for payload in $PAYLOAD_BYTES; do
    trial=1
    while [ "$trial" -le "$REPETITIONS" ]; do
      run_benchmark "$entries" "$payload" "$trial" flush-every
      for group in $GROUP_SIZES; do
        run_benchmark "$entries" "$payload" "$trial" group "$group"
      done
      run_benchmark "$entries" "$payload" "$trial" unsafe-no-flush
      trial=$((trial + 1))
    done
  done
done

if ! "$PYTHON" "$SCRIPT_DIR/plot_wal_bench.py" "$RAW" \
    --csv "$SUMMARY" --svg "$FIGURES"; then
  echo "WAL plot generation failed; raw results remain in $RAW" >&2
  exit 1
fi
echo "WAL benchmark artifacts: $OUT"
