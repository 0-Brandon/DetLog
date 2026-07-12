#!/usr/bin/env sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
EXE=${1:-"$ROOT/build/release/detlog-sim"}
OUT=${2:-"$ROOT/artifacts/traces"}
PYTHON=${PYTHON:-python3}

if [ ! -x "$EXE" ]; then
  echo "simulator executable not found or not executable: $EXE" >&2
  exit 2
fi
if [ -e "$OUT" ] && [ ! -d "$OUT" ]; then
  echo "trace output path exists and is not a directory: $OUT" >&2
  exit 2
fi
if [ -L "$OUT" ]; then
  echo "trace output directory must not be a symbolic link: $OUT" >&2
  exit 2
fi
if [ -d "$OUT" ]; then
  for existing in "$OUT"/* "$OUT"/.[!.]* "$OUT"/..?*; do
    if [ ! -e "$existing" ] && [ ! -L "$existing" ]; then
      continue
    fi
    if [ ! -f "$existing" ] || [ -L "$existing" ]; then
      echo "trace output directory contains a non-regular artifact: $existing" >&2
      exit 2
    fi
    case "${existing##*/}" in
      manifest.json|*.jsonl) ;;
      *)
        echo "trace output directory contains an unrelated file: $existing" >&2
        exit 2
        ;;
    esac
  done
fi

OUT_PARENT=$(dirname -- "$OUT")
mkdir -p "$OUT_PARENT"
STAGING=$(
  "$PYTHON" - "$OUT_PARENT" "$OUT" <<'PY'
import pathlib
import sys
import tempfile

parent = pathlib.Path(sys.argv[1]).resolve()
name = pathlib.Path(sys.argv[2]).name
print(tempfile.mkdtemp(prefix=f".{name}.tmp-", dir=parent))
PY
)
trap 'rm -rf "$STAGING"' 0 HUP INT TERM

cat >"$STAGING/cases.txt" <<'EOF'
leader-crash 3 42 leader-crash-restart-3node-seed42.jsonl
leader-crash 5 84 leader-crash-restart-5node-seed84.jsonl
symmetric-partition 3 43 symmetric-partition-3node-seed43.jsonl
asymmetric-partition 5 44 asymmetric-partition-5node-seed44.jsonl
ambiguous-retry 3 45 ambiguous-retry-3node-seed45.jsonl
torn-wal 3 46 torn-wal-3node-seed46.jsonl
slow-follower 5 47 slow-follower-5node-seed47.jsonl
slow-disk 3 48 slow-disk-3node-seed48.jsonl
saturation 3 49 saturation-3node-seed49.jsonl
EOF

while read -r scenario nodes seed name; do
  [ -n "$scenario" ] || continue
  echo "Generating $name"
  "$EXE" --scenario "$scenario" --nodes "$nodes" --seed "$seed" \
    --trace "$STAGING/$name"
done <"$STAGING/cases.txt"

build_commit=${DETLOG_BUILD_COMMIT:-not_provided}
"$PYTHON" - "$STAGING" "$OUT" "$EXE" "$build_commit" <<'PY'
import datetime
import hashlib
import json
import os
import pathlib
import shutil
import sys
import uuid

staging = pathlib.Path(sys.argv[1]).resolve()
target = pathlib.Path(sys.argv[2]).resolve()
executable = sys.argv[3]
build_commit = sys.argv[4]

traces = []
cases_path = staging / "cases.txt"
for line_number, line in enumerate(
    cases_path.read_text(encoding="utf-8").splitlines(), 1
):
    if not line:
        continue
    fields = line.split()
    if len(fields) != 4:
        raise SystemExit(f"{cases_path}:{line_number}: malformed trace case")
    scenario, nodes_text, seed_text, name = fields
    path = staging / name
    if path.is_symlink() or not path.is_file() or path.stat().st_size == 0:
        raise SystemExit(f"trace generator did not produce a nonempty {path}")
    traces.append(
        {
            "scenario": scenario,
            "nodes": int(nodes_text),
            "seed": int(seed_text),
            "file": name,
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        }
    )

cases_path.unlink()
manifest = {
    "schema": "detlog-trace-corpus/v1",
    "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    "executable": executable,
    "build_commit": build_commit,
    "traces": traces,
}
(staging / "manifest.json").write_text(
    json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
)

backup = target.with_name(f".{target.name}.previous-{uuid.uuid4().hex}")
had_target = target.exists()
if had_target:
    os.replace(target, backup)
try:
    os.replace(staging, target)
except BaseException:
    if had_target and backup.exists() and not target.exists():
        os.replace(backup, target)
    raise
if had_target:
    shutil.rmtree(backup)
PY

trap - 0 HUP INT TERM
echo "Trace corpus: $OUT"
