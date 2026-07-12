#!/usr/bin/env sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
ARG_COUNT=$#
if [ "${RESUME:-0}" = "1" ] && [ "$ARG_COUNT" -lt 2 ]; then
  echo "RESUME=1 requires an explicit output directory argument" >&2
  exit 2
fi
EXE=${1:-"$ROOT/build/release/detlog-wal-bench"}
OUT=${2:-"$ROOT/bench-results/wal-$(date +%Y%m%d-%H%M%S)"}
REPETITIONS=${3:-3}
ENTRY_SIZES=${ENTRY_SIZES-"100 1000 5000"}
PAYLOAD_BYTES=${PAYLOAD_BYTES-"64 1024 8192"}
GROUP_SIZES=${GROUP_SIZES-"8 32"}
PYTHON=${PYTHON:-python3}
: "${ENVIRONMENT:?ENVIRONMENT must name the captured benchmark environment JSON}"

set -- "$PYTHON" "$SCRIPT_DIR/run_checkpointed_campaign.py" wal \
  --executable "$EXE" --output "$OUT" --repetitions "$REPETITIONS" \
  --entry-sizes
for entries in $ENTRY_SIZES; do
  set -- "$@" "$entries"
done
set -- "$@" --payload-bytes
for payload in $PAYLOAD_BYTES; do
  set -- "$@" "$payload"
done
set -- "$@" --group-sizes
for group in $GROUP_SIZES; do
  set -- "$@" "$group"
done
set -- "$@" --environment "$ENVIRONMENT"
if [ "${RESUME:-0}" = "1" ]; then
  set -- "$@" --resume
fi
if [ "${ACKNOWLEDGE_AMBIGUOUS_RUN:-0}" = "1" ]; then
  set -- "$@" --acknowledge-ambiguous-run
fi
if [ -n "${MAX_NEW_RUNS:-}" ]; then
  set -- "$@" --max-new-runs "$MAX_NEW_RUNS"
fi
exec "$@"
