#!/usr/bin/env python3
"""Validate a completed DetLog benchmark directory before publication."""

from __future__ import annotations

import argparse
import gzip
import json
from pathlib import Path
from typing import Any


UINT64_MAX = (1 << 64) - 1


def _load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"{path}: invalid JSON: {error}") from error
    if not isinstance(value, dict):
        raise ValueError(f"{path}: manifest must be a JSON object")
    return value


def _matching_fields(manifest: dict[str, Any], summary: dict[str, Any]) -> None:
    fields = (
        "mode",
        "scenario",
        "nodes",
        "clients",
        "payload_bytes",
        "operations",
        "trial",
        "seed",
        "entries",
        "policy",
        "group_size",
    )
    for field in fields:
        if field in manifest and (
            manifest.get(field) != summary.get(field)
            or type(manifest.get(field)) is not type(summary.get(field))
        ):
            raise ValueError(
                f"run manifest/summary mismatch for {field}: "
                f"{manifest.get(field)!r} != {summary.get(field)!r}"
            )


def _values(matrix: dict[str, Any], field: str) -> list[Any]:
    value = matrix.get(field)
    if not isinstance(value, list) or not value:
        raise ValueError(f"matrix field {field!r} must be a nonempty array")
    return value


def _plain_integer(
    value: Any,
    location: str,
    minimum: int,
    maximum: int | None = None,
) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError(f"{location} must be an integer")
    if value < minimum or (maximum is not None and value > maximum):
        limit = (
            f"at least {minimum}"
            if maximum is None
            else f"in the range {minimum}..{maximum}"
        )
        raise ValueError(f"{location} must be {limit}")
    return value


def _integer_values(
    matrix: dict[str, Any],
    field: str,
    minimum: int,
    maximum: int | None = None,
) -> list[int]:
    values = _values(matrix, field)
    checked = [
        _plain_integer(value, f"matrix field {field!r} item", minimum, maximum)
        for value in values
    ]
    if len(set(checked)) != len(checked):
        raise ValueError(f"matrix field {field!r} contains duplicate values")
    return checked


def _require_exact_values(
    matrix: dict[str, Any], field: str, expected: list[Any]
) -> None:
    actual = _values(matrix, field)
    if actual != expected or any(
        type(value) is not type(expected_value)
        for value, expected_value in zip(actual, expected)
    ):
        raise ValueError(
            f"matrix field {field!r} must be exactly {expected!r}, got {actual!r}"
        )


def _validate_matrix_controls(matrix: dict[str, Any]) -> None:
    schema = matrix.get("schema")
    if schema == "detlog-bench-matrix/v1":
        _require_exact_values(matrix, "nodes", [3, 5])
        _require_exact_values(matrix, "clients", [1, 3, 5])
        _require_exact_values(matrix, "payload_bytes", [64, 1024, 16384])
        _require_exact_values(
            matrix,
            "sim_scenarios",
            [
                "healthy",
                "leader-crash",
                "partition",
                "slow-follower",
                "slow-fsync",
            ],
        )
        _require_exact_values(
            matrix,
            "tcp_scenarios",
            ["healthy", "leader-crash", "partition"],
        )
        _require_exact_values(
            matrix,
            "tcp_unsupported",
            ["slow-follower", "slow-fsync"],
        )
        partition_ms = _plain_integer(
            matrix.get("tcp_partition_ms"), "matrix tcp_partition_ms", 1
        )
        if partition_ms != 1000:
            raise ValueError("matrix tcp_partition_ms must be exactly 1000")
        if matrix.get("tcp_wal_flush_policy") != "flush-every":
            raise ValueError(
                "matrix tcp_wal_flush_policy must be 'flush-every'"
            )
        if matrix.get("sim_wal_flush_policy") != "simulated_flush_every":
            raise ValueError(
                "matrix sim_wal_flush_policy must be 'simulated_flush_every'"
            )
    elif schema == "detlog-runtime-fsync-matrix/v1":
        if matrix.get("mode") != "tcp":
            raise ValueError("runtime fsync matrix mode must be 'tcp'")
        _require_exact_values(matrix, "nodes", [3, 5])
        _require_exact_values(matrix, "clients", [1, 3, 5])
        _require_exact_values(matrix, "payload_bytes", [64, 1024, 16384])
        _require_exact_values(
            matrix, "scenarios", ["healthy", "leader-crash"]
        )
        _require_exact_values(matrix, "policies", ["flush-every", "group"])
        _integer_values(matrix, "group_sizes", 2, 1024)
        _plain_integer(
            matrix.get("group_delay_ms"), "matrix group_delay_ms", 1, 1000
        )
    elif schema == "detlog-wal-bench-matrix/v1":
        _require_exact_values(
            matrix,
            "policies",
            ["flush-every", "group", "unsafe-no-flush"],
        )
        _integer_values(matrix, "entry_sizes", 1, 1_000_000)
        _integer_values(matrix, "payload_bytes", 0, 8_388_608)
        _integer_values(matrix, "group_sizes", 2, 1024)


def _validate_manifest_identity(
    matrix: dict[str, Any], manifest: dict[str, Any], location: str
) -> None:
    schema = matrix.get("schema")
    _plain_integer(
        manifest.get("trial"),
        f"{location}: trial",
        1,
        _plain_integer(matrix.get("repetitions"), "matrix repetitions", 1),
    )
    if schema in (
        "detlog-bench-matrix/v1",
        "detlog-runtime-fsync-matrix/v1",
    ):
        if not isinstance(manifest.get("mode"), str):
            raise ValueError(f"{location}: mode must be a string")
        if not isinstance(manifest.get("scenario"), str):
            raise ValueError(f"{location}: scenario must be a string")
        for field in ("nodes", "clients", "payload_bytes", "operations"):
            _plain_integer(manifest.get(field), f"{location}: {field}", 1)
        _plain_integer(
            manifest.get("seed"), f"{location}: seed", 0, UINT64_MAX
        )
        if schema == "detlog-bench-matrix/v1":
            _plain_integer(
                manifest.get("tcp_partition_ms"),
                f"{location}: tcp_partition_ms",
                1,
            )
        else:
            _plain_integer(
                manifest.get("wal_group_size"),
                f"{location}: wal_group_size",
                2,
                1024,
            )
            _plain_integer(
                manifest.get("wal_group_delay_ms"),
                f"{location}: wal_group_delay_ms",
                1,
                1000,
            )
    elif schema == "detlog-wal-bench-matrix/v1":
        if not isinstance(manifest.get("policy"), str):
            raise ValueError(f"{location}: policy must be a string")
        _plain_integer(manifest.get("entries"), f"{location}: entries", 1)
        _plain_integer(
            manifest.get("payload_bytes"), f"{location}: payload_bytes", 0
        )
        _plain_integer(
            manifest.get("group_size"), f"{location}: group_size", 1
        )


def _validate_manifest_controls(
    matrix: dict[str, Any], manifest: dict[str, Any], location: str
) -> None:
    schema = matrix.get("schema")
    if schema == "detlog-bench-matrix/v1":
        if manifest.get("tcp_partition_ms") != matrix.get("tcp_partition_ms"):
            raise ValueError(
                f"{location}: tcp_partition_ms does not match the matrix"
            )
        mode = manifest.get("mode")
        policy_field = (
            "tcp_wal_flush_policy" if mode == "tcp" else "sim_wal_flush_policy"
        )
        if manifest.get("wal_flush_policy") != matrix.get(policy_field):
            raise ValueError(
                f"{location}: wal_flush_policy does not match {policy_field}"
            )
    elif schema == "detlog-runtime-fsync-matrix/v1":
        if manifest.get("mode") != matrix.get("mode"):
            raise ValueError(f"{location}: mode does not match the matrix")
        if manifest.get("wal_group_delay_ms") != matrix.get("group_delay_ms"):
            raise ValueError(
                f"{location}: wal_group_delay_ms does not match the matrix"
            )
        if manifest.get("wal_flush_policy") not in _values(matrix, "policies"):
            raise ValueError(
                f"{location}: wal_flush_policy is not declared by the matrix"
            )
    elif schema == "detlog-wal-bench-matrix/v1":
        if manifest.get("policy") not in _values(matrix, "policies"):
            raise ValueError(
                f"{location}: policy is not declared by the matrix"
            )


def _expected_run_keys(matrix: dict[str, Any]) -> set[tuple[Any, ...]]:
    schema = matrix.get("schema")
    repetitions = _plain_integer(
        matrix.get("repetitions"), "matrix repetitions", 1
    )
    trials = range(1, repetitions + 1)
    expected: set[tuple[Any, ...]] = set()
    if schema == "detlog-bench-matrix/v1":
        for mode, scenario_field in (
            ("sim", "sim_scenarios"),
            ("tcp", "tcp_scenarios"),
        ):
            for nodes in _values(matrix, "nodes"):
                for clients in _values(matrix, "clients"):
                    for payload in _values(matrix, "payload_bytes"):
                        for scenario in _values(matrix, scenario_field):
                            for trial in trials:
                                expected.add(
                                    (
                                        "cluster",
                                        mode,
                                        scenario,
                                        nodes,
                                        clients,
                                        payload,
                                        trial,
                                    )
                                )
    elif schema == "detlog-runtime-fsync-matrix/v1":
        groups = _values(matrix, "group_sizes")
        variants = [("flush-every", groups[0])] + [
            ("group", group) for group in groups
        ]
        for nodes in _values(matrix, "nodes"):
            for clients in _values(matrix, "clients"):
                for payload in _values(matrix, "payload_bytes"):
                    for scenario in _values(matrix, "scenarios"):
                        for policy, group in variants:
                            for trial in trials:
                                expected.add(
                                    (
                                        "runtime",
                                        "tcp",
                                        scenario,
                                        nodes,
                                        clients,
                                        payload,
                                        policy,
                                        group,
                                        trial,
                                    )
                                )
    elif schema == "detlog-wal-bench-matrix/v1":
        groups = _values(matrix, "group_sizes")
        variants = [("flush-every", 1), ("unsafe-no-flush", 1)] + [
            ("group", group) for group in groups
        ]
        for entries in _values(matrix, "entry_sizes"):
            for payload in _values(matrix, "payload_bytes"):
                for policy, group in variants:
                    for trial in trials:
                        expected.add(
                            ("wal", entries, payload, policy, group, trial)
                        )
    else:
        raise ValueError(f"unsupported matrix schema: {schema!r}")
    return expected


def _run_key(matrix: dict[str, Any], manifest: dict[str, Any]) -> tuple[Any, ...]:
    schema = matrix.get("schema")
    if schema == "detlog-bench-matrix/v1":
        return (
            "cluster",
            manifest.get("mode"),
            manifest.get("scenario"),
            manifest.get("nodes"),
            manifest.get("clients"),
            manifest.get("payload_bytes"),
            manifest.get("trial"),
        )
    if schema == "detlog-runtime-fsync-matrix/v1":
        return (
            "runtime",
            manifest.get("mode"),
            manifest.get("scenario"),
            manifest.get("nodes"),
            manifest.get("clients"),
            manifest.get("payload_bytes"),
            manifest.get("wal_flush_policy"),
            manifest.get("wal_group_size"),
            manifest.get("trial"),
        )
    return (
        "wal",
        manifest.get("entries"),
        manifest.get("payload_bytes"),
        manifest.get("policy"),
        manifest.get("group_size"),
        manifest.get("trial"),
    )


def validate_directory(directory: Path) -> dict[str, int]:
    if not directory.is_dir():
        raise ValueError(f"artifact directory does not exist: {directory}")
    matrix_candidates = [
        directory / "matrix.json",
        directory / "matrix-manifest.json",
    ]
    matrix_paths = [path for path in matrix_candidates if path.is_file()]
    if len(matrix_paths) != 1:
        raise ValueError(
            f"{directory}: expected exactly one matrix manifest, found "
            f"{len(matrix_paths)}"
        )
    matrix = _load_json(matrix_paths[0])
    _validate_matrix_controls(matrix)
    expected_runs = _plain_integer(
        matrix.get("expected_runs"),
        f"{matrix_paths[0]}: expected_runs",
        1,
    )
    operations_per_run = matrix.get("operations_per_run")
    if matrix.get("schema") in (
        "detlog-bench-matrix/v1",
        "detlog-runtime-fsync-matrix/v1",
    ):
        operations_per_run = _plain_integer(
            operations_per_run,
            f"{matrix_paths[0]}: operations_per_run",
            6,
        )
    elif operations_per_run is not None:
        raise ValueError(
            f"{matrix_paths[0]}: WAL matrix must not declare operations_per_run"
        )
    expected_keys = _expected_run_keys(matrix)
    if len(expected_keys) != expected_runs:
        raise ValueError(
            f"{matrix_paths[0]}: Cartesian matrix has {len(expected_keys)} "
            f"unique runs, expected_runs says {expected_runs}"
        )

    raw_candidates = sorted(directory.glob("raw*.jsonl"))
    compressed_candidates = sorted(directory.glob("raw*.jsonl.gz"))
    if len(raw_candidates) + len(compressed_candidates) != 1:
        raise ValueError(
            f"{directory}: expected exactly one raw JSONL representation, found "
            f"{len(raw_candidates)} uncompressed and "
            f"{len(compressed_candidates)} compressed candidates"
        )
    if raw_candidates:
        raw = raw_candidates[0]
    else:
        raw = compressed_candidates[0]
    declared_raw = matrix.get("raw_jsonl")
    if declared_raw is not None:
        if (
            not isinstance(declared_raw, str)
            or not declared_raw
            or "/" in declared_raw
            or "\\" in declared_raw
            or not declared_raw.startswith("raw")
            or not declared_raw.endswith(".jsonl")
        ):
            raise ValueError(
                f"{directory}: matrix raw_jsonl must be a safe JSONL basename"
            )
        if raw.name not in (declared_raw, f"{declared_raw}.gz"):
            raise ValueError(
                f"{directory}: raw stream {raw.name!r} does not match "
                f"matrix raw_jsonl {declared_raw!r}"
            )
    current: dict[str, Any] | None = None
    operation_count = 0
    operation_ids: set[int] = set()
    run_count = 0
    total_records = 0
    observed_keys: set[tuple[Any, ...]] = set()

    source_context = (
        gzip.open(raw, "rt", encoding="utf-8-sig")
        if raw.suffix == ".gz"
        else raw.open("r", encoding="utf-8-sig")
    )
    with source_context as source:
        for line_number, raw_line in enumerate(source, 1):
            line = raw_line.strip()
            if not line:
                raise ValueError(f"{raw}:{line_number}: blank records are invalid")
            try:
                record = json.loads(line)
            except json.JSONDecodeError as error:
                raise ValueError(f"{raw}:{line_number}: invalid JSON: {error}") from error
            if not isinstance(record, dict):
                raise ValueError(f"{raw}:{line_number}: record is not an object")
            total_records += 1
            kind = record.get("record")
            if kind == "manifest":
                if current is not None:
                    raise ValueError(
                        f"{raw}:{line_number}: new manifest before prior summary"
                    )
                if (
                    operations_per_run is not None
                    and record.get("supported") is not True
                ):
                    raise ValueError(f"{raw}:{line_number}: run is not supported")
                if (
                    operations_per_run is not None
                    and record.get("operations") != operations_per_run
                ):
                    raise ValueError(
                        f"{raw}:{line_number}: manifest operations does not "
                        "match operations_per_run"
                    )
                _validate_manifest_identity(
                    matrix, record, f"{raw}:{line_number}"
                )
                _validate_manifest_controls(
                    matrix, record, f"{raw}:{line_number}"
                )
                key = _run_key(matrix, record)
                if key not in expected_keys:
                    raise ValueError(
                        f"{raw}:{line_number}: run is outside requested matrix: {key!r}"
                    )
                if key in observed_keys:
                    raise ValueError(
                        f"{raw}:{line_number}: duplicate matrix run: {key!r}"
                    )
                observed_keys.add(key)
                current = record
                operation_count = 0
                operation_ids.clear()
            elif kind == "operation":
                if current is None or operations_per_run is None:
                    raise ValueError(
                        f"{raw}:{line_number}: operation outside a cluster run"
                    )
                operation_count += 1
                for field in (
                    "mode",
                    "scenario",
                    "trial",
                    "seed",
                    "payload_bytes",
                ):
                    if (
                        record.get(field) != current.get(field)
                        or type(record.get(field)) is not type(current.get(field))
                    ):
                        raise ValueError(
                            f"{raw}:{line_number}: operation {field} does not "
                            "match its manifest"
                        )
                operation = record.get("operation")
                if (
                    not isinstance(operation, int)
                    or isinstance(operation, bool)
                    or operation < 1
                    or operation > operations_per_run
                    or operation in operation_ids
                ):
                    raise ValueError(
                        f"{raw}:{line_number}: invalid/duplicate operation id "
                        f"{operation!r}"
                    )
                operation_ids.add(operation)
                if record.get("status") != "ok":
                    raise ValueError(
                        f"{raw}:{line_number}: non-success operation in completed run"
                    )
            elif kind == "summary":
                if current is None:
                    raise ValueError(f"{raw}:{line_number}: summary without manifest")
                _matching_fields(current, record)
                if operations_per_run is None:
                    if record.get("status") != "ok":
                        raise ValueError(f"{raw}:{line_number}: WAL run is not ok")
                else:
                    if operation_count != operations_per_run:
                        raise ValueError(
                            f"{raw}:{line_number}: {operation_count} operations, "
                            f"expected {operations_per_run}"
                        )
                    if record.get("status") != "complete" or record.get("failures") != 0:
                        raise ValueError(
                            f"{raw}:{line_number}: cluster run is not complete"
                        )
                    if record.get("successes") != operations_per_run:
                        raise ValueError(
                            f"{raw}:{line_number}: success count does not match "
                            "operations_per_run"
                        )
                current = None
                run_count += 1
            else:
                raise ValueError(
                    f"{raw}:{line_number}: unknown record type {kind!r}"
                )

    if current is not None:
        raise ValueError(f"{raw}: final run has no summary")
    if run_count != expected_runs:
        raise ValueError(
            f"{raw}: {run_count} completed runs, expected {expected_runs}"
        )
    if observed_keys != expected_keys:
        missing = sorted(expected_keys - observed_keys, key=repr)
        extra = sorted(observed_keys - expected_keys, key=repr)
        raise ValueError(
            f"{raw}: matrix mismatch; missing={missing[:3]!r} "
            f"extra={extra[:3]!r}"
        )
    expected_records = expected_runs * (
        2 if operations_per_run is None else operations_per_run + 2
    )
    if total_records != expected_records:
        raise ValueError(
            f"{raw}: {total_records} records, expected {expected_records}"
        )
    return {"runs": run_count, "records": total_records}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directories", nargs="+", type=Path)
    arguments = parser.parse_args()
    for directory in arguments.directories:
        result = validate_directory(directory)
        print(
            f"validated {directory}: runs={result['runs']} "
            f"records={result['records']}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
