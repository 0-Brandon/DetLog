#!/usr/bin/env sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
ARG_COUNT=$#
if [ "${RESUME:-0}" = "1" ] && [ "$ARG_COUNT" -lt 2 ]; then
  echo "RESUME=1 requires an explicit output directory argument" >&2
  exit 2
fi
EXE=${1:-"$ROOT/build/release/detlog-bench"}
OUT=${2:-"$ROOT/bench-results/runtime-fsync-$(date +%Y%m%d-%H%M%S)"}
REPETITIONS=${3:-3}
OPERATIONS=${4:-1000}
GROUP_SIZES=${GROUP_SIZES-"2 5"}
GROUP_DELAY_MS=${GROUP_DELAY_MS-2}
PYTHON=${PYTHON:-python3}
: "${ENVIRONMENT:?ENVIRONMENT must name the captured benchmark environment JSON}"

set -- "$PYTHON" "$SCRIPT_DIR/run_checkpointed_campaign.py" runtime-fsync \
  --executable "$EXE" --output "$OUT" --repetitions "$REPETITIONS" \
  --operations "$OPERATIONS" --group-delay-ms "$GROUP_DELAY_MS" \
  --group-sizes
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
