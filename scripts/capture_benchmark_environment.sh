#!/usr/bin/env sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
OUT=${1:-"$ROOT/bench-results/environment.json"}
CXX=${CXX:-c++}
BUILD_FLAGS=${DETLOG_BUILD_FLAGS:-not_provided}
STORAGE=${DETLOG_STORAGE_DESCRIPTION:-not_provided}
NOTES=${DETLOG_BENCH_NOTES:-}
PYTHON=${PYTHON:-python3}
mkdir -p "$(dirname -- "$OUT")"

"$PYTHON" - "$ROOT" "$OUT" "$CXX" "$BUILD_FLAGS" "$STORAGE" "$NOTES" <<'PY'
import datetime
import json
import os
import pathlib
import platform
import shutil
import subprocess
import sys
import tempfile

root, output, compiler, flags, storage, notes = sys.argv[1:]

def capture(*command: str) -> str:
    try:
        value = subprocess.check_output(
            command, text=True, stderr=subprocess.STDOUT
        ).strip()
        return value or "unavailable"
    except Exception as error:
        return f"unavailable: {error}"


def os_release() -> dict[str, str]:
    values: dict[str, str] = {}
    path = pathlib.Path("/etc/os-release")
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            if "=" not in line or line.startswith("#"):
                continue
            key, value = line.split("=", 1)
            values[key] = value.strip().strip('"')
    except OSError:
        pass
    return values


def cpu_description() -> str:
    description = platform.processor().strip()
    if description:
        return description
    try:
        for line in pathlib.Path("/proc/cpuinfo").read_text(
            encoding="utf-8", errors="replace"
        ).splitlines():
            if line.lower().startswith("model name") and ":" in line:
                return line.split(":", 1)[1].strip()
    except OSError:
        pass
    return capture("uname", "-m")


def physical_memory_bytes():
    try:
        pages = os.sysconf("SC_PHYS_PAGES")
        page_size = os.sysconf("SC_PAGE_SIZE")
        if pages > 0 and page_size > 0:
            return int(pages * page_size)
    except (AttributeError, OSError, ValueError):
        pass
    return None


def power_scheme() -> str:
    governors: set[str] = set()
    for path in pathlib.Path("/sys/devices/system/cpu").glob(
        "cpu[0-9]*/cpufreq/scaling_governor"
    ):
        try:
            value = path.read_text(encoding="ascii").strip()
            if value:
                governors.add(value)
        except OSError:
            pass
    if governors:
        return ",".join(sorted(governors))
    if shutil.which("pmset"):
        return capture("pmset", "-g", "custom")
    return "unavailable"


def storage_inventory() -> str:
    if shutil.which("lsblk"):
        return capture(
            "lsblk", "-o", "NAME,MODEL,TYPE,SIZE,ROTA,MOUNTPOINTS"
        )
    if shutil.which("diskutil"):
        return capture("diskutil", "list")
    return "unavailable"


def worktree_status() -> str:
    try:
        completed = subprocess.run(
            ["git", "-C", root, "status", "--porcelain=v1"],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
    except Exception as error:
        return f"unavailable: {error}"
    if completed.returncode != 0:
        return f"unavailable: {completed.stdout.strip()}"
    return completed.stdout.strip() or "clean"


release = os_release()
now = datetime.datetime.now(datetime.timezone.utc)
local_now = datetime.datetime.now().astimezone()
record = {
    "schema": "detlog-benchmark-environment/v1",
    "captured_at": now.isoformat(),
    "machine_scope": "single local Unix-like host",
    "cpu": cpu_description(),
    "logical_processors": int(os.cpu_count() or 0),
    "physical_memory_bytes": physical_memory_bytes(),
    "os_product": release.get("NAME", platform.system()),
    "os_display_version": release.get(
        "VERSION_ID", release.get("PRETTY_NAME", platform.release())
    ),
    "os_build": platform.release(),
    "power_scheme": power_scheme(),
    "temp_directory": tempfile.gettempdir(),
    "storage_description": storage,
    "storage_inventory": storage_inventory(),
    "compiler_path": compiler,
    "compiler_version": capture(compiler, "--version"),
    "build_flags": flags,
    "build_commit": capture("git", "-C", root, "rev-parse", "HEAD"),
    "worktree": worktree_status(),
    "timezone": str(local_now.tzinfo or "unavailable"),
    "notes": notes,
}
path = pathlib.Path(output)
path.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
PY
echo "Benchmark environment: $OUT"
