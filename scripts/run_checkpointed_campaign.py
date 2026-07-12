#!/usr/bin/env python3
"""Run DetLog benchmark matrices with durable, interruption-safe checkpoints."""

from __future__ import annotations

import argparse
import contextlib
import ctypes
import hashlib
import json
import os
import shutil
import stat
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import subprocess
import sys
import tempfile
import time
from typing import Any, Iterator, Sequence
import uuid


ROOT = Path(__file__).resolve().parent.parent
SCRIPTS = ROOT / "scripts"
CHECKPOINT_SCHEMA = "detlog-benchmark-checkpoint/v1"
CHAIN_DOMAIN = b"detlog-benchmark-segment-chain/v1\0"
FILE_ATTRIBUTE_REPARSE_POINT = 0x400
MAX_JSONL_RECORD_BYTES = 16 * 1024 * 1024
CHILD_GATE_TIMEOUT_SECONDS = 60.0


class CampaignError(RuntimeError):
    pass


@dataclass(frozen=True)
class PlannedRun:
    ordinal: int
    arguments: tuple[str, ...]
    description: str
    manifest_fields: dict[str, Any]
    summary_fields: dict[str, Any]
    operation_fields: dict[str, Any]
    operations: int | None
    summary_status: str

    def plan_record(self) -> dict[str, Any]:
        return {
            "ordinal": self.ordinal,
            "arguments": list(self.arguments),
            "manifest_fields": self.manifest_fields,
            "summary_fields": self.summary_fields,
            "operation_fields": self.operation_fields,
            "operations": self.operations,
            "summary_status": self.summary_status,
        }


@dataclass(frozen=True)
class CampaignLayout:
    matrix_name: str
    raw_name: str
    stderr_name: str
    csv_name: str
    svg_name: str
    plot_script: str


@dataclass(frozen=True)
class CampaignDefinition:
    kind: str
    matrix: dict[str, Any]
    plan: tuple[PlannedRun, ...]
    layout: CampaignLayout


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def require_real_directory(path: Path, context: str) -> os.stat_result:
    try:
        status = path.lstat()
    except OSError as error:
        raise CampaignError(f"cannot inspect {context} {path}: {error}") from error
    if (
        stat.S_ISLNK(status.st_mode)
        or not stat.S_ISDIR(status.st_mode)
        or getattr(status, "st_file_attributes", 0)
        & FILE_ATTRIBUTE_REPARSE_POINT
    ):
        raise CampaignError(f"{context} must be a real directory: {path}")
    return status


def require_regular_file(path: Path, context: str) -> os.stat_result:
    try:
        status = path.lstat()
    except OSError as error:
        raise CampaignError(f"cannot inspect {context} {path}: {error}") from error
    if (
        stat.S_ISLNK(status.st_mode)
        or not stat.S_ISREG(status.st_mode)
        or getattr(status, "st_file_attributes", 0)
        & FILE_ATTRIBUTE_REPARSE_POINT
    ):
        raise CampaignError(f"{context} must be a non-link regular file: {path}")
    return status


def open_windows_nofollow(path: Path, flags: int, context: str) -> int:
    from ctypes import wintypes
    import msvcrt

    generic_read = 0x80000000
    generic_write = 0x40000000
    share_read = 0x00000001
    share_write = 0x00000002
    create_new = 1
    create_always = 2
    open_existing = 3
    open_always = 4
    truncate_existing = 5
    file_attribute_normal = 0x00000080
    file_flag_open_reparse_point = 0x00200000
    file_attribute_tag_info = 9

    access_mode = flags & (os.O_RDONLY | os.O_WRONLY | os.O_RDWR)
    if access_mode == os.O_RDWR:
        desired_access = generic_read | generic_write
    elif access_mode == os.O_WRONLY:
        desired_access = generic_write
    else:
        desired_access = generic_read
    if flags & os.O_CREAT:
        if flags & os.O_EXCL:
            disposition = create_new
        elif flags & os.O_TRUNC:
            disposition = create_always
        else:
            disposition = open_always
    elif flags & os.O_TRUNC:
        disposition = truncate_existing
    else:
        disposition = open_existing

    class FileAttributeTagInfo(ctypes.Structure):
        _fields_ = [
            ("file_attributes", wintypes.DWORD),
            ("reparse_tag", wintypes.DWORD),
        ]

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.CreateFileW.argtypes = (
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.LPVOID,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.HANDLE,
    )
    kernel32.CreateFileW.restype = wintypes.HANDLE
    kernel32.GetFileInformationByHandleEx.argtypes = (
        wintypes.HANDLE,
        ctypes.c_int,
        wintypes.LPVOID,
        wintypes.DWORD,
    )
    kernel32.GetFileInformationByHandleEx.restype = wintypes.BOOL
    kernel32.CloseHandle.argtypes = (wintypes.HANDLE,)
    kernel32.CloseHandle.restype = wintypes.BOOL
    handle = kernel32.CreateFileW(
        str(path),
        desired_access,
        share_read | share_write,
        None,
        disposition,
        file_attribute_normal | file_flag_open_reparse_point,
        None,
    )
    invalid_handle = ctypes.c_void_p(-1).value
    if handle == invalid_handle:
        error = ctypes.get_last_error()
        raise CampaignError(
            f"cannot open {context} {path}: Windows error {error}"
        )
    transferred = False
    try:
        attributes = FileAttributeTagInfo()
        ok = kernel32.GetFileInformationByHandleEx(
            handle,
            file_attribute_tag_info,
            ctypes.byref(attributes),
            ctypes.sizeof(attributes),
        )
        if not ok:
            error = ctypes.get_last_error()
            raise CampaignError(
                f"cannot inspect opened {context} {path}: Windows error {error}"
            )
        if attributes.file_attributes & FILE_ATTRIBUTE_REPARSE_POINT:
            raise CampaignError(f"{context} must not be a reparse point: {path}")
        descriptor = msvcrt.open_osfhandle(
            int(handle), flags | getattr(os, "O_BINARY", 0)
        )
        transferred = True
        return descriptor
    finally:
        if not transferred:
            kernel32.CloseHandle(handle)


def open_regular_fd(path: Path, flags: int, context: str) -> int:
    binary = getattr(os, "O_BINARY", 0)
    no_follow = getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = (
            open_windows_nofollow(path, flags, context)
            if os.name == "nt"
            else os.open(path, flags | binary | no_follow)
        )
    except OSError as error:
        raise CampaignError(f"cannot open {context} {path}: {error}") from error
    status = os.fstat(descriptor)
    if not stat.S_ISREG(status.st_mode):
        os.close(descriptor)
        raise CampaignError(f"{context} is not a regular file: {path}")
    return descriptor


def read_regular_bytes(path: Path, context: str) -> bytes:
    require_regular_file(path, context)
    descriptor = open_regular_fd(path, os.O_RDONLY, context)
    try:
        with os.fdopen(descriptor, "rb", closefd=False) as source:
            return source.read()
    finally:
        os.close(descriptor)


def regular_file_size(path: Path, context: str) -> int:
    return require_regular_file(path, context).st_size


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    descriptor = open_regular_fd(path, os.O_RDONLY, "hash input")
    try:
        with os.fdopen(descriptor, "rb", closefd=False) as source:
            for block in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(block)
    finally:
        os.close(descriptor)
    return digest.hexdigest()


def sha256_file_prefix(path: Path, length: int, context: str) -> str:
    digest = hashlib.sha256()
    descriptor = open_regular_fd(path, os.O_RDONLY, context)
    remaining = length
    try:
        with os.fdopen(descriptor, "rb", closefd=False) as source:
            while remaining:
                block = source.read(min(1024 * 1024, remaining))
                if not block:
                    raise CampaignError(f"{context} ended before its committed length")
                digest.update(block)
                remaining -= len(block)
    finally:
        os.close(descriptor)
    return digest.hexdigest()


def canonical_json(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=True, sort_keys=True, separators=(",", ":")
    ).encode("ascii")


def fsync_file(path: Path) -> None:
    # Windows' _commit rejects a read-only descriptor with EBADF.
    descriptor = open_regular_fd(path, os.O_RDWR, "durability target")
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def fsync_directory(path: Path) -> None:
    if os.name == "nt":
        return
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def replace_atomic(source: Path, destination: Path) -> None:
    if os.name != "nt":
        os.replace(source, destination)
        return

    movefile_replace_existing = 0x00000001
    movefile_write_through = 0x00000008
    transient_errors = {5, 32, 33}
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.MoveFileExW.argtypes = (
        ctypes.c_wchar_p,
        ctypes.c_wchar_p,
        ctypes.c_ulong,
    )
    kernel32.MoveFileExW.restype = ctypes.c_int
    delay = 0.01
    for attempt in range(50):
        if kernel32.MoveFileExW(
            str(source),
            str(destination),
            movefile_replace_existing | movefile_write_through,
        ):
            return
        error = ctypes.get_last_error()
        if error not in transient_errors or attempt == 49:
            raise ctypes.WinError(error)
        time.sleep(delay)
        delay = min(delay * 1.5, 0.2)
    raise CampaignError(f"cannot atomically replace {destination}")


def unlink_with_retry(path: Path, missing_ok: bool = False) -> None:
    delay = 0.01
    attempts = 50 if os.name == "nt" else 1
    for attempt in range(attempts):
        try:
            path.unlink(missing_ok=missing_ok)
            return
        except FileNotFoundError:
            if missing_ok:
                return
            raise
        except OSError as error:
            if (
                os.name != "nt"
                or getattr(error, "winerror", None) not in {5, 32, 33}
                or attempt == attempts - 1
            ):
                raise
            time.sleep(delay)
            delay = min(delay * 1.5, 0.2)


def rmtree_with_retry(path: Path) -> None:
    delay = 0.01
    attempts = 50 if os.name == "nt" else 1
    for attempt in range(attempts):
        if not path.exists():
            return
        try:
            shutil.rmtree(path)
            return
        except OSError as error:
            if (
                os.name != "nt"
                or getattr(error, "winerror", None) not in {5, 32, 33}
                or attempt == attempts - 1
            ):
                raise
            time.sleep(delay)
            delay = min(delay * 1.5, 0.2)


def write_atomic_bytes(path: Path, value: bytes) -> None:
    require_real_directory(path.parent, "atomic-write parent")
    descriptor, temporary_name = tempfile.mkstemp(
        dir=path.parent, prefix=f".detlog-write-{path.name}-", suffix=".tmp"
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as output:
            descriptor = -1
            output.write(value)
            output.flush()
            os.fsync(output.fileno())
        replace_atomic(temporary, path)
        fsync_directory(path.parent)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        unlink_with_retry(temporary, missing_ok=True)


def write_atomic_json(path: Path, value: dict[str, Any]) -> None:
    encoded = json.dumps(value, ensure_ascii=True, indent=2).encode("ascii") + b"\n"
    write_atomic_bytes(path, encoded)


def truncate_durably(path: Path, length: int) -> None:
    descriptor = open_regular_fd(path, os.O_RDWR, "truncate target")
    try:
        os.ftruncate(descriptor, length)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def append_durably(path: Path, value: bytes, context: str) -> int:
    descriptor = open_regular_fd(path, os.O_WRONLY | os.O_APPEND, context)
    try:
        view = memoryview(value)
        written = 0
        while written < len(view):
            written += os.write(descriptor, view[written:])
        os.fsync(descriptor)
        return os.fstat(descriptor).st_size
    finally:
        os.close(descriptor)


def require_plain_int(value: Any, context: str, minimum: int = 0) -> int:
    if type(value) is not int or value < minimum:
        raise CampaignError(f"{context} must be an integer >= {minimum}")
    return value


def require_exact_fields(
    record: dict[str, Any], expected: dict[str, Any], context: str
) -> None:
    for field, expected_value in expected.items():
        if field not in record:
            raise CampaignError(f"{context}: missing field {field!r}")
        actual = record[field]
        if type(actual) is not type(expected_value) or actual != expected_value:
            raise CampaignError(
                f"{context}: {field}={actual!r}, expected {expected_value!r}"
            )


def decode_records(segment: bytes, context: str) -> list[dict[str, Any]]:
    if not segment or not segment.endswith(b"\n"):
        raise CampaignError(f"{context}: every record must be LF-terminated")
    records: list[dict[str, Any]] = []
    for index, raw_line in enumerate(segment.splitlines(keepends=True), 1):
        if not raw_line.endswith(b"\n"):
            raise CampaignError(f"{context}: record {index} is not LF-terminated")
        line = raw_line[:-1]
        if line.endswith(b"\r"):
            line = line[:-1]
        if not line:
            raise CampaignError(f"{context}: blank records are invalid")
        try:
            decoded = line.decode("utf-8", errors="strict")
            record = json.loads(decoded)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise CampaignError(f"{context}: invalid record {index}: {error}") from error
        if not isinstance(record, dict):
            raise CampaignError(f"{context}: record {index} is not an object")
        records.append(record)
    return records


def validate_run_segment(
    segment: bytes,
    planned: PlannedRun,
    expected_build_commit: str,
    expected_build_flags: str,
) -> None:
    context = f"run {planned.ordinal}"
    records = decode_records(segment, context)
    expected_records = 2 if planned.operations is None else planned.operations + 2
    if len(records) != expected_records:
        raise CampaignError(
            f"{context}: {len(records)} records, expected {expected_records}"
        )
    manifest = records[0]
    summary = records[-1]
    if manifest.get("record") != "manifest":
        raise CampaignError(f"{context}: first record is not a manifest")
    if summary.get("record") != "summary":
        raise CampaignError(f"{context}: final record is not a summary")
    require_exact_fields(manifest, planned.manifest_fields, f"{context} manifest")
    require_exact_fields(summary, planned.summary_fields, f"{context} summary")
    expected_schema: Any = "detlog-wal-bench/v1" if planned.operations is None else 1
    if type(manifest.get("schema")) is not type(expected_schema) or manifest.get(
        "schema"
    ) != expected_schema:
        raise CampaignError(f"{context}: manifest schema is not {expected_schema!r}")
    if manifest.get("build_commit") != expected_build_commit:
        raise CampaignError(
            f"{context}: build_commit {manifest.get('build_commit')!r} does not "
            f"match {expected_build_commit!r}"
        )
    if manifest.get("build_flags") != expected_build_flags:
        raise CampaignError(f"{context}: build_flags do not match the campaign")
    if planned.operations is not None:
        if manifest.get("supported") is not True:
            raise CampaignError(f"{context}: manifest is not supported")
        operation_ids: set[int] = set()
        for offset, operation in enumerate(records[1:-1], 1):
            if operation.get("record") != "operation":
                raise CampaignError(f"{context}: record {offset + 1} is not an operation")
            require_exact_fields(
                operation, planned.operation_fields, f"{context} operation {offset}"
            )
            operation_id = require_plain_int(
                operation.get("operation"), f"{context} operation id", 1
            )
            if operation_id > planned.operations or operation_id in operation_ids:
                raise CampaignError(f"{context}: invalid/duplicate operation {operation_id}")
            operation_ids.add(operation_id)
            if operation.get("status") != "ok":
                raise CampaignError(f"{context}: operation {operation_id} is not successful")
        if operation_ids != set(range(1, planned.operations + 1)):
            raise CampaignError(f"{context}: operation IDs are not the exact expected set")
        if summary.get("status") != planned.summary_status:
            raise CampaignError(f"{context}: summary is not complete")
        if summary.get("failures") != 0 or summary.get("successes") != planned.operations:
            raise CampaignError(f"{context}: summary counts are incomplete")
    elif summary.get("status") != planned.summary_status:
        raise CampaignError(f"{context}: WAL summary status is not ok")


def split_committed_segments(
    data: bytes,
    plan: Sequence[PlannedRun],
    committed_runs: int,
    expected_build_commit: str,
    expected_build_flags: str,
) -> list[bytes]:
    if committed_runs > len(plan):
        raise CampaignError("checkpoint commits more runs than the plan contains")
    if data and not data.endswith(b"\n"):
        raise CampaignError("committed raw prefix is not LF-terminated")
    lines = data.splitlines(keepends=True)
    position = 0
    segments: list[bytes] = []
    for planned in plan[:committed_runs]:
        line_count = 2 if planned.operations is None else planned.operations + 2
        end = position + line_count
        if end > len(lines):
            raise CampaignError(
                f"committed prefix ends inside planned run {planned.ordinal}"
            )
        segment = b"".join(lines[position:end])
        validate_run_segment(
            segment, planned, expected_build_commit, expected_build_flags
        )
        segments.append(segment)
        position = end
    if position != len(lines):
        raise CampaignError("committed prefix contains records beyond its run count")
    return segments


def stream_committed_segments(
    path: Path,
    committed_bytes: int,
    plan: Sequence[PlannedRun],
    committed_runs: int,
    expected_build_commit: str,
    expected_build_flags: str,
) -> Iterator[tuple[PlannedRun, bytes]]:
    """Validate/yield one committed run at a time instead of loading the campaign."""
    if committed_runs > len(plan):
        raise CampaignError("checkpoint commits more runs than the plan contains")
    descriptor = open_regular_fd(path, os.O_RDONLY, "raw stream")
    remaining = committed_bytes
    try:
        with os.fdopen(descriptor, "rb", closefd=False) as source:
            for planned in plan[:committed_runs]:
                line_count = 2 if planned.operations is None else planned.operations + 2
                segment = bytearray()
                for record_number in range(1, line_count + 1):
                    if remaining <= 0:
                        raise CampaignError(
                            f"committed prefix ends inside planned run {planned.ordinal}"
                        )
                    limit = min(remaining, MAX_JSONL_RECORD_BYTES)
                    line = source.readline(limit)
                    if not line or not line.endswith(b"\n"):
                        raise CampaignError(
                            f"run {planned.ordinal}: record {record_number} is not "
                            "LF-terminated or exceeds the record-size limit"
                        )
                    segment.extend(line)
                    remaining -= len(line)
                encoded = bytes(segment)
                validate_run_segment(
                    encoded, planned, expected_build_commit, expected_build_flags
                )
                yield planned, encoded
            if remaining != 0:
                raise CampaignError(
                    "committed prefix contains records beyond its run count"
                )
    finally:
        os.close(descriptor)


def initial_chain(matrix_sha256: str, plan_sha256: str) -> bytes:
    return hashlib.sha256(
        CHAIN_DOMAIN + bytes.fromhex(matrix_sha256) + bytes.fromhex(plan_sha256)
    ).digest()


def advance_chain(chain: bytes, ordinal: int, segment: bytes) -> bytes:
    return hashlib.sha256(
        CHAIN_DOMAIN
        + b"run\0"
        + chain
        + ordinal.to_bytes(8, "big")
        + hashlib.sha256(segment).digest()
    ).digest()


def plan_hash(plan: Sequence[PlannedRun]) -> str:
    return sha256_bytes(canonical_json([run.plan_record() for run in plan]))


def runner_metadata() -> dict[str, Any]:
    try:
        commit = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        runner_state = subprocess.run(
            ["git", "status", "--porcelain=v1", "--", "scripts/run_checkpointed_campaign.py"],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError) as error:
        raise CampaignError("cannot capture checkpoint runner Git provenance") from error
    if len(commit) != 40 or any(character not in "0123456789abcdef" for character in commit):
        raise CampaignError("checkpoint runner Git commit is not a full SHA-1")
    if runner_state:
        raise CampaignError("checkpoint runner must be committed before a campaign starts")
    return {
        "runner": "scripts/run_checkpointed_campaign.py",
        "runner_commit": commit,
        "runner_sha256": sha256_file(Path(__file__).resolve()),
        "environment": "environment.json",
        "child_temp_policy": "unique_subdirectory_under_captured_temp",
    }


def process_token_windows(pid: int) -> tuple[bool, str | None]:
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.OpenProcess.argtypes = (ctypes.c_ulong, ctypes.c_int, ctypes.c_ulong)
    kernel32.OpenProcess.restype = ctypes.c_void_p
    kernel32.CloseHandle.argtypes = (ctypes.c_void_p,)
    kernel32.CloseHandle.restype = ctypes.c_int
    handle = kernel32.OpenProcess(0x1000, False, pid)
    if not handle:
        return False, None
    try:
        class FileTime(ctypes.Structure):
            _fields_ = (("low", ctypes.c_ulong), ("high", ctypes.c_ulong))

        kernel32.GetProcessTimes.argtypes = (
            ctypes.c_void_p,
            ctypes.POINTER(FileTime),
            ctypes.POINTER(FileTime),
            ctypes.POINTER(FileTime),
            ctypes.POINTER(FileTime),
        )
        kernel32.GetProcessTimes.restype = ctypes.c_int
        creation = FileTime()
        exit_time = FileTime()
        kernel = FileTime()
        user = FileTime()
        ok = kernel32.GetProcessTimes(
            handle,
            ctypes.byref(creation),
            ctypes.byref(exit_time),
            ctypes.byref(kernel),
            ctypes.byref(user),
        )
        token = (creation.high << 32) | creation.low
        exited = (exit_time.high << 32) | exit_time.low
        if not ok:
            return True, None
        return exited == 0, str(token)
    finally:
        kernel32.CloseHandle(handle)


def process_token_posix(pid: int) -> tuple[bool, str | None]:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False, None
    except PermissionError:
        return True, None
    stat = Path(f"/proc/{pid}/stat")
    if stat.is_file():
        try:
            fields = stat.read_text(encoding="ascii").split()
            return True, fields[21]
        except (OSError, IndexError, UnicodeError):
            pass
    return True, None


def process_token(pid: int) -> tuple[bool, str | None]:
    return process_token_windows(pid) if os.name == "nt" else process_token_posix(pid)


def active_child_is_alive(active: dict[str, Any]) -> bool:
    pid = active.get("child_pid")
    if type(pid) is not int or pid <= 0:
        raise CampaignError("active-run marker has an invalid child PID")
    alive, token = process_token(pid)
    if not alive:
        return False
    recorded = active.get("child_start_token")
    return recorded is None or token is None or str(recorded) == token


@contextlib.contextmanager
def exclusive_lock(path: Path) -> Iterator[None]:
    require_real_directory(path.parent, "lock parent")
    if path.exists():
        require_regular_file(path, "campaign lock")
    flags = os.O_CREAT | os.O_RDWR
    descriptor = open_regular_fd(path, flags, "campaign lock")
    lock_file = os.fdopen(descriptor, "r+b")
    if lock_file.seek(0, os.SEEK_END) == 0:
        lock_file.write(b"0")
        lock_file.flush()
    lock_file.seek(0)
    locked = False
    try:
        if os.name == "nt":
            import msvcrt

            try:
                msvcrt.locking(lock_file.fileno(), msvcrt.LK_NBLCK, 1)
            except OSError as error:
                raise CampaignError("another campaign process holds the lock") from error
        else:
            import fcntl

            try:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError as error:
                raise CampaignError("another campaign process holds the lock") from error
        locked = True
        yield
    finally:
        try:
            if locked:
                lock_file.seek(0)
                if os.name == "nt":
                    import msvcrt

                    msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
        finally:
            lock_file.close()


def cluster_definition(args: argparse.Namespace, executable: Path) -> CampaignDefinition:
    nodes_values = (3, 5)
    client_values = (1, 3, 5)
    payload_values = (64, 1024, 16384)
    sim_scenarios = ("healthy", "leader-crash", "partition", "slow-follower", "slow-fsync")
    tcp_scenarios = ("healthy", "leader-crash", "partition")
    runs: list[PlannedRun] = []
    ordinal = 0
    for mode, scenarios in (("sim", sim_scenarios), ("tcp", tcp_scenarios)):
        for nodes in nodes_values:
            for clients in client_values:
                for payload in payload_values:
                    for scenario in scenarios:
                        for trial in range(1, args.repetitions + 1):
                            ordinal += 1
                            seed = 1_000_000 * trial + ordinal
                            manifest = {
                                "mode": mode,
                                "scenario": scenario,
                                "nodes": nodes,
                                "clients": clients,
                                "payload_bytes": payload,
                                "operations": args.operations,
                                "trial": trial,
                                "seed": seed,
                            }
                            operation = {
                                key: manifest[key]
                                for key in ("mode", "scenario", "trial", "seed", "payload_bytes")
                            }
                            command = (
                                "--mode", mode, "--nodes", str(nodes), "--clients", str(clients),
                                "--payload", str(payload), "--operations", str(args.operations),
                                "--trial", str(trial), "--seed", str(seed), "--scenario", scenario,
                                "--partition-ms", "1000",
                            )
                            runs.append(
                                PlannedRun(
                                    ordinal, command,
                                    f"{mode} {scenario} n={nodes} c={clients} p={payload} trial={trial}",
                                    manifest, manifest.copy(), operation,
                                    args.operations, "complete",
                                )
                            )
    matrix = {
        "schema": "detlog-bench-matrix/v1",
        "generated_at": utc_now(),
        "executable": str(executable),
        "repetitions": args.repetitions,
        "operations_per_run": args.operations,
        "expected_runs": len(runs),
        "nodes": list(nodes_values),
        "clients": list(client_values),
        "payload_bytes": list(payload_values),
        "sim_scenarios": list(sim_scenarios),
        "tcp_scenarios": list(tcp_scenarios),
        "tcp_unsupported": ["slow-follower", "slow-fsync"],
        "tcp_partition_ms": 1000,
        "tcp_wal_flush_policy": "flush-every",
        "sim_wal_flush_policy": "simulated_flush_every",
        "note": "TCP nodes share one benchmark process but use real loopback sockets and distinct WALs; partition is a literal one-second bidirectional transport cut.",
    }
    matrix.update(runner_metadata())
    return CampaignDefinition(
        "cluster", matrix, tuple(runs),
        CampaignLayout("matrix.json", "raw.jsonl", "stderr.log", "summary.csv", "throughput.svg", "plot_bench.py"),
    )


def runtime_definition(args: argparse.Namespace, executable: Path) -> CampaignDefinition:
    groups = tuple(args.group_sizes)
    variants = (("flush-every", groups[0]),) + tuple(("group", group) for group in groups)
    runs: list[PlannedRun] = []
    ordinal = 0
    for nodes in (3, 5):
        for clients in (1, 3, 5):
            for payload in (64, 1024, 16384):
                for scenario in ("healthy", "leader-crash"):
                    for policy, group in variants:
                        for trial in range(1, args.repetitions + 1):
                            ordinal += 1
                            seed = 2_000_000 * trial + ordinal
                            manifest = {
                                "mode": "tcp", "scenario": scenario, "nodes": nodes,
                                "clients": clients, "payload_bytes": payload,
                                "operations": args.operations, "trial": trial, "seed": seed,
                                "wal_flush_policy": policy, "wal_group_size": group,
                                "wal_group_delay_ms": args.group_delay_ms,
                            }
                            summary = {
                                key: manifest[key]
                                for key in ("mode", "scenario", "nodes", "clients", "payload_bytes", "operations", "trial", "seed")
                            }
                            operation = {
                                key: manifest[key]
                                for key in ("mode", "scenario", "trial", "seed", "payload_bytes")
                            }
                            command = (
                                "--mode", "tcp", "--nodes", str(nodes), "--clients", str(clients),
                                "--payload", str(payload), "--operations", str(args.operations),
                                "--trial", str(trial), "--seed", str(seed), "--scenario", scenario,
                                "--fsync-policy", policy, "--group-size", str(group),
                                "--group-delay-ms", str(args.group_delay_ms),
                            )
                            runs.append(
                                PlannedRun(
                                    ordinal, command,
                                    f"{scenario} n={nodes} c={clients} p={payload} fsync={policy} g={group} trial={trial}",
                                    manifest, summary, operation, args.operations, "complete",
                                )
                            )
    matrix = {
        "schema": "detlog-runtime-fsync-matrix/v1",
        "generated_at": utc_now(), "executable": str(executable),
        "repetitions": args.repetitions, "operations_per_run": args.operations,
        "expected_runs": len(runs), "mode": "tcp", "nodes": [3, 5],
        "clients": [1, 3, 5], "payload_bytes": [64, 1024, 16384],
        "scenarios": ["healthy", "leader-crash"],
        "policies": ["flush-every", "group"], "group_sizes": list(groups),
        "group_delay_ms": args.group_delay_ms,
        "note": "End-to-end NodeHost comparison over real loopback TCP and distinct WAL files.",
    }
    matrix.update(runner_metadata())
    return CampaignDefinition(
        "runtime-fsync", matrix, tuple(runs),
        CampaignLayout("matrix.json", "raw.jsonl", "stderr.log", "summary.csv", "throughput.svg", "plot_bench.py"),
    )


def wal_definition(args: argparse.Namespace, executable: Path) -> CampaignDefinition:
    runs: list[PlannedRun] = []
    ordinal = 0
    variants = (("flush-every", 1),) + tuple(("group", group) for group in args.group_sizes) + (("unsafe-no-flush", 1),)
    for entries in args.entry_sizes:
        for payload in args.payload_bytes:
            for trial in range(1, args.repetitions + 1):
                for policy, group in variants:
                    ordinal += 1
                    manifest = {
                        "entries": entries, "payload_bytes": payload,
                        "trial": trial, "policy": policy, "group_size": group,
                    }
                    command: tuple[str, ...] = (
                        "--entries", str(entries), "--payload", str(payload),
                        "--trial", str(trial), "--policy", policy,
                    )
                    if policy == "group":
                        command += ("--group-size", str(group))
                    runs.append(
                        PlannedRun(
                            ordinal, command,
                            f"policy={policy} entries={entries} payload={payload} trial={trial} group={group}",
                            manifest, manifest.copy(), {}, None, "ok",
                        )
                    )
    matrix = {
        "schema": "detlog-wal-bench-matrix/v1", "generated_at": utc_now(),
        "executable": str(executable), "repetitions": args.repetitions,
        "entry_sizes": list(args.entry_sizes), "payload_bytes": list(args.payload_bytes),
        "policies": ["flush-every", "group", "unsafe-no-flush"],
        "group_sizes": list(args.group_sizes), "expected_runs": len(runs),
        "raw_jsonl": "raw-includes-nondurable.jsonl", "diagnostics": "stderr.log",
        "derived_csv": "summary-includes-nondurable.csv",
        "derived_svg": "wal-figures-includes-nondurable.svg",
        "unsafe_policy_note": "UNSAFE: append acknowledgements are nondurable; each run performs and times an explicit final flush before reopen recovery.",
    }
    matrix.update(runner_metadata())
    return CampaignDefinition(
        "wal", matrix, tuple(runs),
        CampaignLayout(
            "matrix-manifest.json", "raw-includes-nondurable.jsonl", "stderr.log",
            "summary-includes-nondurable.csv", "wal-figures-includes-nondurable.svg",
            "plot_wal_bench.py",
        ),
    )


def validate_cli(args: argparse.Namespace) -> None:
    if args.repetitions < 1:
        raise CampaignError("repetitions must be positive")
    if args.kind in ("cluster", "runtime-fsync") and not 5 < args.operations <= 100_000:
        raise CampaignError("operations must be in the range 6..100000")
    if args.kind == "runtime-fsync":
        if args.group_sizes is None:
            args.group_sizes = [2, 5]
        if not 1 <= args.group_delay_ms <= 1000:
            raise CampaignError("group delay must be in the range 1..1000")
        if not args.group_sizes or any(not 2 <= value <= 1024 for value in args.group_sizes):
            raise CampaignError("group sizes must all be in the range 2..1024")
        if len(set(args.group_sizes)) != len(args.group_sizes):
            raise CampaignError("group sizes must be unique")
    if args.kind == "wal":
        if args.group_sizes is None:
            args.group_sizes = [8, 32]
        if args.repetitions > 1000:
            raise CampaignError("WAL repetitions must not exceed 1000")
        if not args.entry_sizes or any(not 1 <= value <= 1_000_000 for value in args.entry_sizes):
            raise CampaignError("entry sizes must all be in the range 1..1000000")
        if not args.payload_bytes or any(not 0 <= value <= 8_388_608 for value in args.payload_bytes):
            raise CampaignError("payload sizes must all be in the range 0..8388608")
        if not args.group_sizes or any(not 2 <= value <= 1024 for value in args.group_sizes):
            raise CampaignError("group sizes must all be in the range 2..1024")
        if len(set(args.entry_sizes)) != len(args.entry_sizes):
            raise CampaignError("entry sizes must be unique")
        if len(set(args.payload_bytes)) != len(args.payload_bytes):
            raise CampaignError("payload sizes must be unique")
        if len(set(args.group_sizes)) != len(args.group_sizes):
            raise CampaignError("group sizes must be unique")
        for entries in args.entry_sizes:
            for payload in args.payload_bytes:
                estimated_entry_bytes = payload + 256
                if entries * estimated_entry_bytes > 256 * 1024 * 1024:
                    raise CampaignError(
                        "WAL matrix contains a workload above the 256 MiB bound"
                    )
                for group in args.group_sizes:
                    if group * estimated_entry_bytes > 64 * 1024 * 1024:
                        raise CampaignError(
                            "WAL matrix contains a group above the 64 MiB bound"
                        )
    if args.kind == "cluster" and args.group_sizes is None:
        args.group_sizes = []
    if args.max_new_runs is not None and args.max_new_runs < 1:
        raise CampaignError("max-new-runs must be positive")


def validate_environment_record(path: Path) -> dict[str, Any]:
    record = load_json_object(path, "benchmark environment")
    if record.get("schema") != "detlog-benchmark-environment/v1":
        raise CampaignError("environment record has the wrong schema")
    expected_commit = os.environ.get("DETLOG_BUILD_COMMIT") or "not_provided"
    expected_flags = os.environ.get("DETLOG_BUILD_FLAGS") or "not_provided"
    if len(expected_commit) != 40 or any(
        character not in "0123456789abcdef" for character in expected_commit
    ):
        raise CampaignError("DETLOG_BUILD_COMMIT must be a full lowercase Git SHA-1")
    if expected_flags == "not_provided":
        raise CampaignError("DETLOG_BUILD_FLAGS must describe the exact release build")
    if record.get("build_commit") != expected_commit:
        raise CampaignError("environment build_commit does not match DETLOG_BUILD_COMMIT")
    if record.get("build_flags") != expected_flags:
        raise CampaignError("environment build_flags do not match DETLOG_BUILD_FLAGS")
    if record.get("worktree") != "clean":
        raise CampaignError("reportable environment record must capture a clean worktree")
    recorded_temp = record.get("temp_directory")
    current_temp = str(Path(tempfile.gettempdir()).resolve())
    if not isinstance(recorded_temp, str) or Path(recorded_temp).resolve() != Path(current_temp):
        raise CampaignError("environment TEMP directory does not match the campaign")
    return record


def stable_matrix(matrix: dict[str, Any]) -> dict[str, Any]:
    value = dict(matrix)
    value.pop("generated_at", None)
    return value


def checkpoint_bindings(
    definition: CampaignDefinition,
    matrix_sha: str,
    executable: Path,
    executable_sha: str,
    environment_sha: str | None,
) -> dict[str, Any]:
    effective_temp = Path(tempfile.gettempdir()).resolve()
    return {
        "schema": CHECKPOINT_SCHEMA,
        "kind": definition.kind,
        "matrix_sha256": matrix_sha,
        "plan_sha256": plan_hash(definition.plan),
        "executable": str(executable),
        "executable_sha256": executable_sha,
        "environment_sha256": environment_sha,
        "temp_directory": str(effective_temp),
        "tmp_directory": str(effective_temp),
        "build_commit": os.environ.get("DETLOG_BUILD_COMMIT") or "not_provided",
        "build_flags": os.environ.get("DETLOG_BUILD_FLAGS") or "not_provided",
        "expected_runs": len(definition.plan),
        "raw_name": definition.layout.raw_name,
        "stderr_name": definition.layout.stderr_name,
    }


def checkpoint_value(
    bindings: dict[str, Any],
    definition: CampaignDefinition,
    committed_runs: int,
    committed_bytes: int,
    stderr_bytes: int,
    chain: bytes,
    stderr_sha: str,
) -> dict[str, Any]:
    result = dict(bindings)
    result.update(
        {
            "updated_at": utc_now(),
            "committed_runs": committed_runs,
            "committed_bytes": committed_bytes,
            "stderr_bytes": stderr_bytes,
            "segment_chain_sha256": chain.hex(),
            "stderr_prefix_sha256": stderr_sha,
            "complete": committed_runs == len(definition.plan),
            "last_ordinal": committed_runs if committed_runs else None,
            "last_key": (
                definition.plan[committed_runs - 1].manifest_fields
                if committed_runs
                else None
            ),
        }
    )
    return result


def load_json_object(path: Path, context: str) -> dict[str, Any]:
    try:
        value = json.loads(read_regular_bytes(path, context).decode("utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise CampaignError(f"cannot read {context} {path}: {error}") from error
    if not isinstance(value, dict):
        raise CampaignError(f"{context} must contain a JSON object")
    return value


def quarantine_uncommitted_tails(
    output: Path,
    raw: Path,
    raw_offset: int,
    errors: Path,
    error_offset: int,
    ordinal: int,
) -> Path | None:
    raw_size = regular_file_size(raw, "raw stream")
    error_size = regular_file_size(errors, "stderr stream")
    if raw_size == raw_offset and error_size == error_offset:
        return None
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    destination = (
        ROOT
        / "out"
        / "benchmark-interruptions"
        / f"{output.name}-{stamp}-{os.getpid()}-{uuid.uuid4().hex[:8]}"
    )
    temporary = destination.with_name(destination.name + ".tmp")
    temporary.mkdir(parents=True, exist_ok=False)
    if raw_size > raw_offset:
        write_atomic_bytes(
            temporary / "raw-tail.bin",
            read_regular_bytes(raw, "raw stream")[raw_offset:],
        )
    if error_size > error_offset:
        write_atomic_bytes(
            temporary / "stderr-tail.bin",
            read_regular_bytes(errors, "stderr stream")[error_offset:],
        )
    write_atomic_json(
        temporary / "metadata.json",
        {
            "schema": "detlog-benchmark-interrupted-tail/v1",
            "captured_at": utc_now(), "campaign": str(output),
            "next_ordinal": ordinal, "raw_committed_bytes": raw_offset,
            "raw_observed_bytes": raw_size, "stderr_committed_bytes": error_offset,
            "stderr_observed_bytes": error_size,
        },
    )
    replace_atomic(temporary, destination)
    fsync_directory(destination.parent)
    truncate_durably(raw, raw_offset)
    truncate_durably(errors, error_offset)
    return destination


def campaign_identity(output: Path) -> str:
    canonical = os.path.normcase(str(output.resolve()))
    return sha256_bytes(canonical.encode("utf-8"))


def transaction_root(output: Path) -> Path:
    campaign_id = campaign_identity(output)[:24]
    root = ROOT / "out" / "benchmark-transactions" / campaign_id
    root.mkdir(parents=True, exist_ok=True)
    require_real_directory(root, "transaction root")
    return root


def require_transaction_directory(output: Path, value: Any) -> Path:
    if not isinstance(value, str) or not value:
        raise CampaignError("active-run marker has no transaction directory")
    root = transaction_root(output).resolve()
    candidate = Path(value).absolute()
    try:
        common = Path(os.path.commonpath((root, candidate.resolve())))
    except (OSError, ValueError) as error:
        raise CampaignError("active transaction path is invalid") from error
    if common != root:
        raise CampaignError("active transaction escapes its campaign root")
    require_real_directory(candidate, "active transaction")
    return candidate


def safe_remove_transaction(output: Path, transaction: Path) -> None:
    root = transaction_root(output).resolve()
    resolved = transaction.resolve()
    if resolved.parent != root:
        raise CampaignError("refusing to remove a transaction outside its root")
    require_real_directory(transaction, "transaction cleanup target")
    rmtree_with_retry(transaction)


def quarantine_transaction(
    output: Path, transaction: Path, reason: str, ordinal: int
) -> Path:
    root = ROOT / "out" / "benchmark-interruptions"
    root.mkdir(parents=True, exist_ok=True)
    require_real_directory(root, "interruption quarantine")
    destination = root / (
        f"{output.name}-run-{ordinal}-{reason}-"
        f"{datetime.now().strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:8]}"
    )
    replace_atomic(transaction, destination)
    fsync_directory(root)
    return destination


def cleanup_orphan_transactions(
    output: Path, definition: CampaignDefinition, active_path: Path
) -> None:
    referenced: Path | None = None
    if active_path.exists():
        active = load_json_object(active_path, "active-run marker")
        if active.get("schema") != "detlog-benchmark-active-run/v2":
            raise CampaignError("active-run marker has an unsupported schema")
        ordinal = require_plain_int(active.get("ordinal"), "active ordinal", 1)
        if ordinal > len(definition.plan):
            raise CampaignError("active ordinal exceeds the ordered plan")
        if active.get("identity") != definition.plan[ordinal - 1].manifest_fields:
            raise CampaignError("active transaction identity disagrees with the plan")
        referenced = require_transaction_directory(
            output, active.get("transaction")
        ).resolve()
    root = transaction_root(output)
    for candidate in root.iterdir():
        require_real_directory(candidate, "transaction directory")
        if referenced is not None and candidate.resolve() == referenced:
            continue
        metadata_path = candidate / "transaction.json"
        if metadata_path.exists():
            metadata = load_json_object(metadata_path, "transaction metadata")
            cleanup_owned_temp(
                metadata.get("benchmark_temp_base"),
                metadata.get("benchmark_temp_directory"),
            )
        destination = quarantine_transaction(output, candidate, "orphan", 0)
        print(f"Quarantined orphan transaction: {destination}", flush=True)


def transaction_has_summary(stdout_path: Path) -> bool:
    data = read_regular_bytes(stdout_path, "transaction stdout")
    for raw_line in data.split(b"\n"):
        line = raw_line[:-1] if raw_line.endswith(b"\r") else raw_line
        if not line:
            continue
        try:
            record = json.loads(line.decode("utf-8", errors="strict"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            continue
        if isinstance(record, dict) and record.get("record") == "summary":
            return True
    return False


def cleanup_owned_temp(base_value: Any, owned_value: Any) -> None:
    if owned_value is None:
        return
    if not isinstance(base_value, str) or not isinstance(owned_value, str):
        raise CampaignError("transaction benchmark temp ownership is invalid")
    base = Path(base_value)
    owned = Path(owned_value)
    require_real_directory(base, "benchmark temp root")
    if not owned.exists():
        return
    require_real_directory(owned, "runner-owned benchmark temp directory")
    if owned.resolve().parent != base.resolve() or not owned.name.startswith(
        ".detlog-campaign-"
    ):
        raise CampaignError("runner-owned benchmark temp directory escapes its root")
    rmtree_with_retry(owned)


def cleanup_benchmark_temp(active: dict[str, Any]) -> None:
    cleanup_owned_temp(
        active.get("benchmark_temp_base"),
        active.get("benchmark_temp_directory"),
    )


def prepare_transaction(
    output: Path,
    active_path: Path,
    planned: PlannedRun,
    temp_directory: str,
) -> tuple[dict[str, Any], Path, Path, Path]:
    root = transaction_root(output)
    transaction = root / f"run-{planned.ordinal}-{uuid.uuid4().hex}"
    transaction.mkdir(exist_ok=False)
    stdout_path = transaction / "stdout.jsonl"
    stderr_path = transaction / "stderr.log"
    result_path = transaction / "result.json"
    worker_path = transaction / "worker.json"
    gate_path = transaction / "launch-gate.json"
    write_atomic_bytes(stdout_path, b"")
    write_atomic_bytes(stderr_path, b"")
    temp_root = Path(temp_directory)
    require_real_directory(temp_root, "benchmark temp root")
    campaign_id = campaign_identity(output)[:16]
    owned_temp = temp_root / (
        f".detlog-campaign-{campaign_id}-run-{planned.ordinal}-{uuid.uuid4().hex}"
    )
    write_atomic_json(
        transaction / "transaction.json",
        {
            "schema": "detlog-benchmark-transaction/v1",
            "ordinal": planned.ordinal,
            "benchmark_temp_base": str(temp_root),
            "benchmark_temp_directory": str(owned_temp),
        },
    )
    owned_temp.mkdir()
    launch_nonce = uuid.uuid4().hex
    write_atomic_json(
        worker_path,
        {
            "schema": "detlog-benchmark-child-worker/v1",
            "ordinal": planned.ordinal,
            "launch_nonce": launch_nonce,
            "gate": str(gate_path),
            "stdout": str(stdout_path),
            "stderr": str(stderr_path),
            "result": str(result_path),
        },
    )
    active = {
        "schema": "detlog-benchmark-active-run/v2",
        "prepared_at": utc_now(),
        "ordinal": planned.ordinal,
        "identity": planned.manifest_fields,
        "transaction": str(transaction),
        "state": "prepared",
        "parent_pid": os.getpid(),
        "child_pid": None,
        "child_start_token": None,
        "launch_nonce": launch_nonce,
        "benchmark_temp_base": str(temp_root),
        "benchmark_temp_directory": str(owned_temp),
    }
    write_atomic_json(active_path, active)
    return active, stdout_path, stderr_path, result_path


def require_worker_path(transaction: Path, value: Any, context: str) -> Path:
    if not isinstance(value, str) or not value:
        raise CampaignError(f"child worker has no {context} path")
    candidate = Path(value).absolute()
    if candidate.parent.resolve() != transaction.resolve():
        raise CampaignError(f"child worker {context} escapes its transaction")
    return candidate


def wait_for_launch_gate(gate_path: Path, launch_nonce: str) -> bool:
    deadline = time.monotonic() + CHILD_GATE_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if gate_path.exists():
            gate = load_json_object(gate_path, "child launch gate")
            if gate.get("schema") != "detlog-benchmark-launch-gate/v1":
                raise CampaignError("child launch gate has an unsupported schema")
            if gate.get("launch_nonce") != launch_nonce:
                raise CampaignError("child launch gate nonce does not match")
            return True
        time.sleep(0.05)
    return False


def run_child_worker(config_path: Path, executable: Path, arguments: Sequence[str]) -> int:
    """Supervise one benchmark only after the parent durably publishes our PID."""
    config_path = config_path.absolute()
    transaction = config_path.parent
    require_real_directory(transaction, "child worker transaction")
    config = load_json_object(config_path, "child worker configuration")
    if config.get("schema") != "detlog-benchmark-child-worker/v1":
        raise CampaignError("child worker configuration has an unsupported schema")
    ordinal = require_plain_int(config.get("ordinal"), "child worker ordinal", 1)
    launch_nonce = config.get("launch_nonce")
    if not isinstance(launch_nonce, str) or not launch_nonce:
        raise CampaignError("child worker launch nonce is invalid")
    gate_path = require_worker_path(transaction, config.get("gate"), "gate")
    stdout_path = require_worker_path(transaction, config.get("stdout"), "stdout")
    stderr_path = require_worker_path(transaction, config.get("stderr"), "stderr")
    result_path = require_worker_path(transaction, config.get("result"), "result")
    require_regular_file(executable, "child benchmark executable")
    require_regular_file(stdout_path, "child transaction stdout")
    require_regular_file(stderr_path, "child transaction stderr")
    if not wait_for_launch_gate(gate_path, launch_nonce):
        return 124

    stdout_descriptor = open_regular_fd(
        stdout_path, os.O_WRONLY | os.O_APPEND, "child transaction stdout"
    )
    stderr_descriptor = open_regular_fd(
        stderr_path, os.O_WRONLY | os.O_APPEND, "child transaction stderr"
    )
    process: subprocess.Popen[bytes] | None = None
    try:
        with os.fdopen(stdout_descriptor, "ab", buffering=0) as stdout_output:
            stdout_descriptor = -1
            with os.fdopen(stderr_descriptor, "ab", buffering=0) as stderr_output:
                stderr_descriptor = -1
                process = subprocess.Popen(
                    [str(executable), *arguments],
                    cwd=ROOT,
                    env=os.environ.copy(),
                    stdout=stdout_output,
                    stderr=stderr_output,
                )
                try:
                    exit_code = process.wait()
                except KeyboardInterrupt:
                    if process.poll() is None:
                        process.terminate()
                        try:
                            process.wait(timeout=5)
                        except subprocess.TimeoutExpired:
                            process.kill()
                            process.wait()
                    exit_code = 130
    finally:
        if stdout_descriptor >= 0:
            os.close(stdout_descriptor)
        if stderr_descriptor >= 0:
            os.close(stderr_descriptor)

    fsync_file(stdout_path)
    fsync_file(stderr_path)
    write_atomic_json(
        result_path,
        {
            "schema": "detlog-benchmark-child-result/v1",
            "recorded_at": utc_now(),
            "ordinal": ordinal,
            "exit_code": exit_code,
        },
    )
    return 0


def supervisor_process_options() -> dict[str, Any]:
    """Keep a parent Ctrl-C from converting a resumable pause into child failure."""
    if os.name == "nt":
        return {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP}
    return {"start_new_session": True}


def inspect_active_transaction(
    args: argparse.Namespace,
    output: Path,
    definition: CampaignDefinition,
    committed_runs: int,
    active_path: Path,
    failure_path: Path,
) -> dict[str, Any] | None:
    if not active_path.exists():
        return None
    active = load_json_object(active_path, "active-run marker")
    if active.get("schema") != "detlog-benchmark-active-run/v2":
        raise CampaignError("active-run marker has an unsupported schema")
    ordinal = require_plain_int(active.get("ordinal"), "active ordinal", 1)
    if ordinal > len(definition.plan):
        raise CampaignError("active ordinal exceeds the ordered plan")
    planned = definition.plan[ordinal - 1]
    if active.get("identity") != planned.manifest_fields:
        raise CampaignError("active transaction identity disagrees with the plan")
    transaction = require_transaction_directory(output, active.get("transaction"))
    stdout_path = transaction / "stdout.jsonl"
    stderr_path = transaction / "stderr.log"
    result_path = transaction / "result.json"
    require_regular_file(stdout_path, "transaction stdout")
    require_regular_file(stderr_path, "transaction stderr")

    if ordinal <= committed_runs:
        cleanup_benchmark_temp(active)
        unlink_with_retry(active_path, missing_ok=True)
        safe_remove_transaction(output, transaction)
        return None
    if ordinal != committed_runs + 1:
        raise CampaignError("active transaction is not the next ordered run")
    child_pid = active.get("child_pid")
    if child_pid is not None and active_child_is_alive(active):
        raise CampaignError(
            f"benchmark child PID {child_pid} is still alive; refusing concurrent resume"
        )

    if result_path.exists():
        result = load_json_object(result_path, "transaction result")
        if result.get("schema") != "detlog-benchmark-child-result/v1":
            raise CampaignError("transaction result has an unsupported schema")
        if result.get("ordinal") != ordinal:
            raise CampaignError("transaction result ordinal disagrees with active state")
        exit_code = result.get("exit_code")
        if type(exit_code) is not int:
            raise CampaignError("transaction exit code must be an integer")
        if exit_code != 0:
            write_atomic_json(
                failure_path,
                {
                    "schema": "detlog-benchmark-failed-run/v2",
                    "recorded_at": utc_now(),
                    "ordinal": ordinal,
                    "exit_code": exit_code,
                    "identity": planned.manifest_fields,
                    "transaction": str(transaction),
                },
            )
            raise CampaignError(
                f"campaign has a recorded child failure at run {ordinal}"
            )
        return active

    if not args.acknowledge_ambiguous_run:
        output_kind = "terminal" if transaction_has_summary(stdout_path) else "partial"
        raise CampaignError(
            f"run {ordinal} has an ambiguous dead transaction with {output_kind} "
            "output and no durable exit result; inspect it and pass "
            "--acknowledge-ambiguous-run to retry"
        )
    reason = "acknowledged-ambiguous"
    cleanup_benchmark_temp(active)
    destination = quarantine_transaction(output, transaction, reason, ordinal)
    unlink_with_retry(active_path, missing_ok=True)
    print(f"Quarantined {reason} transaction: {destination}", flush=True)
    return None


def verify_checkpoint_and_recover(
    output: Path,
    definition: CampaignDefinition,
    bindings: dict[str, Any],
    checkpoint: dict[str, Any],
    raw: Path,
    errors: Path,
) -> tuple[int, bytes]:
    for field, expected in bindings.items():
        actual = checkpoint.get(field)
        if type(actual) is not type(expected) or actual != expected:
            raise CampaignError(
                f"checkpoint binding {field}={actual!r}, expected {expected!r}"
            )
    committed_runs = require_plain_int(
        checkpoint.get("committed_runs"), "checkpoint committed_runs"
    )
    committed_bytes = require_plain_int(
        checkpoint.get("committed_bytes"), "checkpoint committed_bytes"
    )
    stderr_bytes = require_plain_int(
        checkpoint.get("stderr_bytes"), "checkpoint stderr_bytes"
    )
    if committed_runs > len(definition.plan):
        raise CampaignError("checkpoint run count exceeds the plan")
    expected_complete = committed_runs == len(definition.plan)
    if checkpoint.get("complete") is not expected_complete:
        raise CampaignError("checkpoint completion flag disagrees with its run count")
    expected_last = committed_runs if committed_runs else None
    if checkpoint.get("last_ordinal") != expected_last:
        raise CampaignError("checkpoint last ordinal disagrees with its run count")
    expected_key = (
        definition.plan[committed_runs - 1].manifest_fields
        if committed_runs
        else None
    )
    if checkpoint.get("last_key") != expected_key:
        raise CampaignError("checkpoint last key disagrees with the ordered plan")
    raw_size = regular_file_size(raw, "raw stream")
    error_size = regular_file_size(errors, "stderr stream")
    if raw_size < committed_bytes or error_size < stderr_bytes:
        raise CampaignError("artifact stream is shorter than its committed checkpoint")
    if sha256_file_prefix(
        errors, stderr_bytes, "stderr stream"
    ) != checkpoint.get("stderr_prefix_sha256"):
        raise CampaignError("committed stderr prefix hash does not match the checkpoint")
    segments = stream_committed_segments(
        raw,
        committed_bytes,
        definition.plan,
        committed_runs,
        bindings["build_commit"],
        bindings["build_flags"],
    )
    chain = initial_chain(bindings["matrix_sha256"], bindings["plan_sha256"])
    for planned, segment in segments:
        chain = advance_chain(chain, planned.ordinal, segment)
    if chain.hex() != checkpoint.get("segment_chain_sha256"):
        raise CampaignError("committed segment hash chain does not match the checkpoint")

    quarantined = quarantine_uncommitted_tails(
        output, raw, committed_bytes, errors, stderr_bytes, committed_runs + 1
    )
    if quarantined is not None:
        print(f"Quarantined interrupted tail: {quarantined}", flush=True)
    return committed_runs, chain


def run_finalizers(definition: CampaignDefinition, output: Path) -> None:
    raw = output / definition.layout.raw_name
    subprocess.run(
        [sys.executable, str(SCRIPTS / "validate_benchmark_artifacts.py"), str(output)],
        cwd=ROOT,
        check=True,
    )
    command = [
        sys.executable, str(SCRIPTS / definition.layout.plot_script), str(raw),
        "--csv", str(output / definition.layout.csv_name),
        "--svg", str(output / definition.layout.svg_name),
    ]
    subprocess.run(command, cwd=ROOT, check=True)


def bootstrap_value(
    definition: CampaignDefinition,
    executable: Path,
    executable_sha: str,
    environment_sha: str,
) -> dict[str, Any]:
    return {
        "schema": "detlog-benchmark-bootstrap/v1",
        "created_at": utc_now(),
        "kind": definition.kind,
        "stable_matrix_sha256": sha256_bytes(
            canonical_json(stable_matrix(definition.matrix))
        ),
        "plan_sha256": plan_hash(definition.plan),
        "executable": str(executable),
        "executable_sha256": executable_sha,
        "environment_sha256": environment_sha,
    }


def initialize_campaign(
    definition: CampaignDefinition,
    executable: Path,
    environment_source: Path,
    output: Path,
    matrix_path: Path,
    raw: Path,
    errors: Path,
    environment_destination: Path,
    checkpoint_path: Path,
    bootstrap_path: Path,
    recovering: bool,
) -> tuple[dict[str, Any], dict[str, Any], bytes]:
    for temporary in output.glob(".detlog-write-*.tmp"):
        require_regular_file(temporary, "stale bootstrap temporary")
        unlink_with_retry(temporary)
    executable_sha = sha256_file(executable)
    environment_sha = sha256_file(environment_source)
    expected_bootstrap = bootstrap_value(
        definition, executable, executable_sha, environment_sha
    )
    if bootstrap_path.exists():
        existing_bootstrap = load_json_object(bootstrap_path, "bootstrap marker")
        for field, expected in expected_bootstrap.items():
            if field == "created_at":
                continue
            if existing_bootstrap.get(field) != expected:
                raise CampaignError(f"bootstrap binding {field} changed")
    else:
        entries = list(output.iterdir())
        if recovering and entries:
            raise CampaignError(
                "partial initialization has no bootstrap marker; refusing reconstruction"
            )
        write_atomic_json(bootstrap_path, expected_bootstrap)

    if matrix_path.exists():
        existing_matrix = load_json_object(matrix_path, "matrix")
        if stable_matrix(existing_matrix) != stable_matrix(definition.matrix):
            raise CampaignError("bootstrap matrix does not match requested controls")
        matrix_bytes = read_regular_bytes(matrix_path, "matrix")
    else:
        matrix_bytes = json.dumps(
            definition.matrix, ensure_ascii=True, indent=4
        ).encode("ascii") + b"\n"
        write_atomic_bytes(matrix_path, matrix_bytes)

    for path, context in ((raw, "raw stream"), (errors, "stderr stream")):
        if path.exists():
            if regular_file_size(path, context) != 0:
                raise CampaignError(
                    f"zero-run bootstrap {context} unexpectedly contains data"
                )
        else:
            write_atomic_bytes(path, b"")

    if environment_destination.exists():
        if sha256_file(environment_destination) != environment_sha:
            raise CampaignError("bootstrap environment copy changed")
    else:
        write_atomic_bytes(
            environment_destination,
            read_regular_bytes(environment_source, "benchmark environment"),
        )

    matrix_sha = sha256_bytes(matrix_bytes)
    bindings = checkpoint_bindings(
        definition, matrix_sha, executable, executable_sha, environment_sha
    )
    chain = initial_chain(matrix_sha, bindings["plan_sha256"])
    if checkpoint_path.exists():
        checkpoint = load_json_object(checkpoint_path, "checkpoint")
        if checkpoint.get("committed_runs") != 0:
            raise CampaignError("bootstrap checkpoint is not a zero-run checkpoint")
    else:
        checkpoint = checkpoint_value(
            bindings,
            definition,
            0,
            0,
            0,
            chain,
            sha256_bytes(b""),
        )
        write_atomic_json(checkpoint_path, checkpoint)
    unlink_with_retry(bootstrap_path, missing_ok=True)
    return bindings, checkpoint, chain


def cleanup_stale_atomic_temporaries(output: Path) -> None:
    for temporary in output.glob(".detlog-write-*.tmp"):
        require_regular_file(temporary, "stale atomic temporary")
        unlink_with_retry(temporary)


def completion_value(
    definition: CampaignDefinition,
    bindings: dict[str, Any],
    checkpoint: dict[str, Any],
    output: Path,
) -> dict[str, Any]:
    names = (
        definition.layout.matrix_name,
        definition.layout.raw_name,
        definition.layout.stderr_name,
        "environment.json",
        definition.layout.csv_name,
        definition.layout.svg_name,
    )
    files: dict[str, Any] = {}
    for name in names:
        path = output / name
        files[name] = {
            "bytes": regular_file_size(path, f"completion file {name}"),
            "sha256": sha256_file(path),
        }
    return {
        "schema": "detlog-benchmark-campaign-complete/v1",
        "completed_at": utc_now(),
        "kind": definition.kind,
        "bindings": bindings,
        "committed_runs": checkpoint["committed_runs"],
        "segment_chain_sha256": checkpoint["segment_chain_sha256"],
        "files": files,
    }


def verify_completion(
    definition: CampaignDefinition,
    bindings: dict[str, Any],
    output: Path,
    complete_path: Path,
) -> None:
    complete = load_json_object(complete_path, "completion marker")
    if complete.get("schema") != "detlog-benchmark-campaign-complete/v1":
        raise CampaignError("completion marker has an unsupported schema")
    if complete.get("kind") != definition.kind:
        raise CampaignError("completion marker kind changed")
    if complete.get("bindings") != bindings:
        raise CampaignError("completion marker bindings changed")
    if complete.get("committed_runs") != len(definition.plan):
        raise CampaignError("completion marker run count is incomplete")
    files = complete.get("files")
    if not isinstance(files, dict):
        raise CampaignError("completion marker has no file inventory")
    expected_names = {
        definition.layout.matrix_name,
        definition.layout.raw_name,
        definition.layout.stderr_name,
        "environment.json",
        definition.layout.csv_name,
        definition.layout.svg_name,
    }
    if set(files) != expected_names:
        raise CampaignError("completion marker file inventory is not exact")
    for name, expected in files.items():
        if not isinstance(name, str) or not isinstance(expected, dict):
            raise CampaignError("completion marker file inventory is invalid")
        path = output / name
        if regular_file_size(path, f"completed file {name}") != expected.get(
            "bytes"
        ) or sha256_file(path) != expected.get("sha256"):
            raise CampaignError(f"completed artifact changed: {name}")
    subprocess.run(
        [sys.executable, str(SCRIPTS / "validate_benchmark_artifacts.py"), str(output)],
        cwd=ROOT,
        check=True,
    )


def commit_transaction(
    output: Path,
    definition: CampaignDefinition,
    bindings: dict[str, Any],
    planned: PlannedRun,
    active: dict[str, Any],
    raw: Path,
    errors: Path,
    checkpoint_path: Path,
    active_path: Path,
    failure_path: Path,
    chain: bytes,
) -> tuple[int, bytes, dict[str, Any]]:
    transaction = require_transaction_directory(output, active.get("transaction"))
    stdout_path = transaction / "stdout.jsonl"
    stderr_path = transaction / "stderr.log"
    segment = read_regular_bytes(stdout_path, "transaction stdout")
    diagnostic = read_regular_bytes(stderr_path, "transaction stderr")
    try:
        validate_run_segment(
            segment,
            planned,
            bindings["build_commit"],
            bindings["build_flags"],
        )
    except CampaignError as error:
        write_atomic_json(
            failure_path,
            {
                "schema": "detlog-benchmark-failed-run/v2",
                "recorded_at": utc_now(),
                "ordinal": planned.ordinal,
                "exit_code": 0,
                "validation_error": str(error),
                "identity": planned.manifest_fields,
                "transaction": str(transaction),
            },
        )
        raise

    raw_bytes = append_durably(raw, segment, "raw stream")
    stderr_bytes = append_durably(errors, diagnostic, "stderr stream")
    chain = advance_chain(chain, planned.ordinal, segment)
    checkpoint = checkpoint_value(
        bindings,
        definition,
        planned.ordinal,
        raw_bytes,
        stderr_bytes,
        chain,
        sha256_file(errors),
    )
    write_atomic_json(checkpoint_path, checkpoint)
    cleanup_benchmark_temp(active)
    unlink_with_retry(active_path, missing_ok=True)
    safe_remove_transaction(output, transaction)
    return planned.ordinal, chain, checkpoint


def execute_transaction(
    output: Path,
    active_path: Path,
    executable: Path,
    planned: PlannedRun,
    temp_directory: str,
) -> None:
    active, stdout_path, stderr_path, result_path = prepare_transaction(
        output, active_path, planned, temp_directory
    )
    transaction = require_transaction_directory(output, active.get("transaction"))
    worker_path = transaction / "worker.json"
    gate_path = transaction / "launch-gate.json"
    process: subprocess.Popen[bytes] | None = None
    stdout_descriptor = open_regular_fd(
        stdout_path, os.O_WRONLY | os.O_APPEND, "transaction stdout"
    )
    stderr_descriptor = open_regular_fd(
        stderr_path, os.O_WRONLY | os.O_APPEND, "transaction stderr"
    )
    try:
        with os.fdopen(stdout_descriptor, "ab", buffering=0) as stdout_output:
            stdout_descriptor = -1
            with os.fdopen(stderr_descriptor, "ab", buffering=0) as stderr_output:
                stderr_descriptor = -1
                child_environment = os.environ.copy()
                child_temp_directory = active["benchmark_temp_directory"]
                for variable in ("TMPDIR", "TEMP", "TMP"):
                    child_environment[variable] = child_temp_directory
                process = subprocess.Popen(
                    [
                        sys.executable,
                        str(Path(__file__).resolve()),
                        "--child-worker",
                        str(worker_path),
                        str(executable),
                        *planned.arguments,
                    ],
                    cwd=ROOT,
                    env=child_environment,
                    stdout=stdout_output,
                    stderr=stderr_output,
                    **supervisor_process_options(),
                )
                alive, token = process_token(process.pid)
                active.update(
                    {
                        "state": "running",
                        "child_pid": process.pid,
                        "child_start_token": token if alive else None,
                    }
                )
                write_atomic_json(active_path, active)
                write_atomic_json(
                    gate_path,
                    {
                        "schema": "detlog-benchmark-launch-gate/v1",
                        "published_at": utc_now(),
                        "launch_nonce": active["launch_nonce"],
                    },
                )
                process.wait()
    except BaseException:
        # The supervisor owns the benchmark once its launch gate is published.
        # Leaving it alive lets it durably record the outcome after this parent
        # is interrupted; resume will refuse overlap while its PID is live.
        raise
    finally:
        if stdout_descriptor >= 0:
            os.close(stdout_descriptor)
        if stderr_descriptor >= 0:
            os.close(stderr_descriptor)

    if not result_path.exists():
        raise CampaignError(
            f"run {planned.ordinal} supervisor exited without a durable result"
        )


def run_campaign(
    args: argparse.Namespace,
    definition: CampaignDefinition,
    executable: Path,
    output: Path,
) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    require_real_directory(output.parent, "campaign parent")
    output_existed = output.exists()
    if args.resume and not output_existed:
        raise CampaignError("--resume requires an existing campaign directory")
    if output_existed:
        require_real_directory(output, "campaign output")
    else:
        output.mkdir()
    lock_root = ROOT / "out" / "benchmark-locks"
    lock_root.mkdir(parents=True, exist_ok=True)
    require_real_directory(lock_root, "campaign lock root")
    lock_path = lock_root / (campaign_identity(output) + ".lock")
    with exclusive_lock(lock_path):
        cleanup_stale_atomic_temporaries(output)
        layout = definition.layout
        matrix_path = output / layout.matrix_name
        raw = output / layout.raw_name
        errors = output / layout.stderr_name
        checkpoint_path = output / ".checkpoint.json"
        bootstrap_path = output / ".bootstrap.json"
        active_path = output / ".active-run.json"
        failure_path = output / ".failed-run.json"
        complete_path = output / "campaign-complete.json"
        environment_destination = output / "environment.json"
        cleanup_orphan_transactions(output, definition, active_path)
        existing_entries = {path.name for path in output.iterdir()}

        environment_source = args.environment.resolve(strict=True)
        validate_environment_record(environment_source)
        environment_sha = sha256_file(environment_source)

        if not args.resume:
            if existing_entries:
                raise CampaignError(
                    f"refusing to overwrite nonempty output directory {output}; use --resume"
                )
            bindings, checkpoint, chain = initialize_campaign(
                definition,
                executable,
                environment_source,
                output,
                matrix_path,
                raw,
                errors,
                environment_destination,
                checkpoint_path,
                bootstrap_path,
                recovering=False,
            )
            committed_runs = 0
        else:
            if failure_path.exists():
                failure = load_json_object(failure_path, "failed-run marker")
                raise CampaignError(
                    f"campaign has a recorded child failure at run {failure.get('ordinal')}; "
                    "do not retry it as an interruption"
                )
            if complete_path.exists():
                for path, context in (
                    (matrix_path, "matrix"),
                    (environment_destination, "environment copy"),
                ):
                    require_regular_file(path, context)
                existing_matrix = load_json_object(matrix_path, "matrix")
                if stable_matrix(existing_matrix) != stable_matrix(definition.matrix):
                    raise CampaignError("resume arguments do not match completed matrix")
                if sha256_file(environment_destination) != environment_sha:
                    raise CampaignError("completed environment record changed")
                matrix_sha = sha256_file(matrix_path)
                bindings = checkpoint_bindings(
                    definition,
                    matrix_sha,
                    executable,
                    sha256_file(executable),
                    environment_sha,
                )
                verify_completion(definition, bindings, output, complete_path)
                unlink_with_retry(checkpoint_path, missing_ok=True)
                unlink_with_retry(bootstrap_path, missing_ok=True)
                print(f"Campaign is already complete: {output}", flush=True)
                return

            if not checkpoint_path.exists():
                bindings, checkpoint, chain = initialize_campaign(
                    definition,
                    executable,
                    environment_source,
                    output,
                    matrix_path,
                    raw,
                    errors,
                    environment_destination,
                    checkpoint_path,
                    bootstrap_path,
                    recovering=True,
                )
                committed_runs = 0
            else:
                required = (
                    (matrix_path, "matrix"),
                    (raw, "raw stream"),
                    (errors, "stderr stream"),
                    (checkpoint_path, "checkpoint"),
                    (environment_destination, "environment copy"),
                )
                for path, context in required:
                    require_regular_file(path, context)
                existing_matrix = load_json_object(matrix_path, "matrix")
                if stable_matrix(existing_matrix) != stable_matrix(definition.matrix):
                    raise CampaignError("resume arguments do not match the stored matrix")
                matrix_sha = sha256_file(matrix_path)
                if sha256_file(environment_destination) != environment_sha:
                    raise CampaignError(
                        "environment record changed since the campaign began"
                    )
                bindings = checkpoint_bindings(
                    definition,
                    matrix_sha,
                    executable,
                    sha256_file(executable),
                    environment_sha,
                )
                checkpoint = load_json_object(checkpoint_path, "checkpoint")
                committed_runs, chain = verify_checkpoint_and_recover(
                    output, definition, bindings, checkpoint, raw, errors
                )
                unlink_with_retry(bootstrap_path, missing_ok=True)

            recovered = inspect_active_transaction(
                args,
                output,
                definition,
                committed_runs,
                active_path,
                failure_path,
            )
            if recovered is not None:
                planned = definition.plan[committed_runs]
                committed_runs, chain, checkpoint = commit_transaction(
                    output,
                    definition,
                    bindings,
                    planned,
                    recovered,
                    raw,
                    errors,
                    checkpoint_path,
                    active_path,
                    failure_path,
                    chain,
                )
            print(
                f"Resuming {definition.kind} campaign after {committed_runs}/{len(definition.plan)} committed runs",
                flush=True,
            )

        if committed_runs == len(definition.plan):
            run_finalizers(definition, output)
            write_atomic_json(
                complete_path,
                completion_value(definition, bindings, checkpoint, output),
            )
            unlink_with_retry(checkpoint_path, missing_ok=True)
            unlink_with_retry(active_path, missing_ok=True)
        else:
            new_runs = 0
            for planned in definition.plan[committed_runs:]:
                print(
                    f"[{planned.ordinal}/{len(definition.plan)}] {planned.description}",
                    flush=True,
                )
                execute_transaction(
                    output,
                    active_path,
                    executable,
                    planned,
                    bindings["temp_directory"],
                )
                active = inspect_active_transaction(
                    args,
                    output,
                    definition,
                    committed_runs,
                    active_path,
                    failure_path,
                )
                if active is None:
                    raise CampaignError(
                        f"run {planned.ordinal} ended without a committed result"
                    )
                committed_runs, chain, checkpoint = commit_transaction(
                    output,
                    definition,
                    bindings,
                    planned,
                    active,
                    raw,
                    errors,
                    checkpoint_path,
                    active_path,
                    failure_path,
                    chain,
                )
                new_runs += 1
                if (
                    args.max_new_runs is not None
                    and new_runs >= args.max_new_runs
                    and committed_runs < len(definition.plan)
                ):
                    print(
                        f"Paused at durable checkpoint {committed_runs}/{len(definition.plan)}; resume with the same arguments",
                        flush=True,
                    )
                    break

            if committed_runs == len(definition.plan):
                run_finalizers(definition, output)
                write_atomic_json(
                    complete_path,
                    completion_value(definition, bindings, checkpoint, output),
                )
                unlink_with_retry(checkpoint_path, missing_ok=True)
                unlink_with_retry(active_path, missing_ok=True)
                print(f"Benchmark artifacts: {output}", flush=True)


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("kind", choices=("cluster", "runtime-fsync", "wal"))
    result.add_argument("--executable", required=True, type=Path)
    result.add_argument("--output", required=True, type=Path)
    result.add_argument("--environment", required=True, type=Path)
    result.add_argument("--resume", action="store_true")
    result.add_argument("--acknowledge-ambiguous-run", action="store_true")
    result.add_argument("--max-new-runs", type=int)
    result.add_argument("--repetitions", type=int, default=3)
    result.add_argument("--operations", type=int, default=1000)
    result.add_argument("--group-sizes", type=int, nargs="+")
    result.add_argument("--group-delay-ms", type=int, default=2)
    result.add_argument("--entry-sizes", type=int, nargs="+", default=[100, 1000, 5000])
    result.add_argument("--payload-bytes", type=int, nargs="+", default=[64, 1024, 8192])
    return result


def main() -> int:
    if len(sys.argv) > 1 and sys.argv[1] == "--child-worker":
        if len(sys.argv) < 4:
            print("error: child worker arguments are incomplete", file=sys.stderr)
            return 125
        try:
            return run_child_worker(
                Path(sys.argv[2]),
                Path(sys.argv[3]).resolve(strict=True),
                tuple(sys.argv[4:]),
            )
        except (CampaignError, OSError, subprocess.SubprocessError) as error:
            print(f"error: child worker: {error}", file=sys.stderr)
            return 125

    args = parser().parse_args()
    try:
        validate_cli(args)
        executable = args.executable.resolve(strict=True)
        require_regular_file(executable, "benchmark executable")
        if os.name != "nt" and not os.access(executable, os.X_OK):
            raise CampaignError(f"benchmark executable is not executable: {executable}")
        output = Path(os.path.abspath(args.output))
        if args.environment is not None:
            args.environment = args.environment.resolve(strict=True)
        if args.kind == "cluster":
            definition = cluster_definition(args, executable)
        elif args.kind == "runtime-fsync":
            definition = runtime_definition(args, executable)
        else:
            definition = wal_definition(args, executable)
        run_campaign(args, definition, executable, output)
    except (CampaignError, OSError, subprocess.CalledProcessError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("interrupted; committed checkpoint is unchanged", file=sys.stderr)
        return 130
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
