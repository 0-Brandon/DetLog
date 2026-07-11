#!/usr/bin/env python3
"""Validate and derive CSV/SVG artifacts from DetLog WAL benchmark JSONL."""

from __future__ import annotations

import argparse
import csv
import html
import io
import json
import math
import os
import statistics
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, NoReturn


SCHEMA = "detlog-wal-bench/v1"
DERIVED_NAME_MARKER = "includes-nondurable"
POLICIES = ("flush-every", "group", "unsafe-no-flush")
DURABLE_LABEL = "DURABLE_ACKNOWLEDGEMENTS_DURING_APPEND"
UNSAFE_LABEL = "UNSAFE_NONDURABLE_DURING_APPEND_ACKNOWLEDGEMENTS"
UNSAFE_WARNING = (
    "UNSAFE / NONDURABLE APPEND ACKNOWLEDGEMENTS: unsafe-no-flush append "
    "throughput does not measure crash-safe commits"
)


CSV_FIELDS = [
    "source_file",
    "manifest_line",
    "summary_line",
    "provenance_id",
    "schema",
    "timestamp_epoch_ms",
    "os",
    "compiler",
    "cpp",
    "hardware_threads",
    "build_commit",
    "build_flags",
    "entries",
    "payload_bytes",
    "trial",
    "policy",
    "policy_display",
    "group_size",
    "durable_during_append",
    "durability_label",
    "reopen_precondition",
    "unsafe_warning",
    "status",
    "append_calls",
    "append_wall_ns",
    "append_cpu_ns",
    "append_ops_per_second",
    "final_explicit_flush_performed",
    "final_flush_wall_ns",
    "final_flush_cpu_ns",
    "file_bytes",
    "reopen_scan_wall_ns",
    "recovered_entry_count",
    "recovered_commit_index",
]


@dataclass
class Run:
    source_file: str
    manifest_line: int
    summary_line: int
    manifest: dict[str, Any]
    summary: dict[str, Any]
    provenance_id: str = ""

    def provenance_key(self) -> tuple[Any, ...]:
        # Source identity is intentional: separate experiment files are never
        # averaged together merely because their build strings happen to match.
        return (
            self.source_file,
            self.manifest["os"],
            self.manifest["compiler"],
            self.manifest["cpp"],
            self.manifest["hardware_threads"],
            self.manifest["build_commit"],
            self.manifest["build_flags"],
        )

    def aggregation_key(self) -> tuple[Any, ...]:
        return (
            self.provenance_key(),
            self.manifest["entries"],
            self.manifest["payload_bytes"],
            self.manifest["policy"],
            self.manifest["group_size"],
            self.manifest["durable_during_append"],
        )


@dataclass(frozen=True)
class MeanPoint:
    provenance_id: str
    entries: int
    payload_bytes: int
    policy: str
    group_size: int
    durable_during_append: bool
    trials: int
    append_ops_per_second: float
    append_ops_min: float
    append_ops_max: float
    file_bytes: float
    file_bytes_min: float
    file_bytes_max: float
    reopen_scan_wall_ns: float
    reopen_scan_min_ns: float
    reopen_scan_max_ns: float


def fail(path: Path, line: int, message: str) -> NoReturn:
    location = f"{path}:{line}" if line else str(path)
    raise ValueError(f"{location}: {message}")


def require_object(path: Path, line: int, value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        fail(path, line, "each JSONL record must be an object")
    return value


def require_string(
    path: Path, line: int, record: dict[str, Any], field: str
) -> str:
    value = record.get(field)
    if not isinstance(value, str) or not value:
        fail(path, line, f"{field!r} must be a nonempty string")
    return value


def require_integer(
    path: Path,
    line: int,
    record: dict[str, Any],
    field: str,
    minimum: int = 0,
) -> int:
    value = record.get(field)
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        fail(path, line, f"{field!r} must be an integer >= {minimum}")
    return value


def require_boolean(
    path: Path, line: int, record: dict[str, Any], field: str
) -> bool:
    value = record.get(field)
    if not isinstance(value, bool):
        fail(path, line, f"{field!r} must be a boolean")
    return value


def require_number(
    path: Path, line: int, record: dict[str, Any], field: str
) -> float:
    value = record.get(field)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        fail(path, line, f"{field!r} must be a number")
    result = float(value)
    if result < 0.0 or not math.isfinite(result):
        fail(path, line, f"{field!r} must be finite and nonnegative")
    return result


def validate_common(
    path: Path, line: int, record: dict[str, Any], expected_record: str
) -> None:
    if record.get("record") != expected_record:
        fail(path, line, f"expected a {expected_record!r} record")
    if record.get("schema") != SCHEMA:
        fail(path, line, f"schema must be exactly {SCHEMA!r}")
    require_integer(path, line, record, "entries", 1)
    require_integer(path, line, record, "payload_bytes")
    require_integer(path, line, record, "trial", 1)
    policy = require_string(path, line, record, "policy")
    if policy not in POLICIES:
        fail(path, line, f"unsupported policy {policy!r}")
    group_size = require_integer(path, line, record, "group_size", 1)
    if policy == "group" and not 2 <= group_size <= 1024:
        fail(path, line, "group policy requires group_size in 2..1024")
    if policy != "group" and group_size != 1:
        fail(path, line, f"{policy} requires group_size == 1")
    durable = require_boolean(path, line, record, "durable_during_append")
    if durable != (policy != "unsafe-no-flush"):
        fail(path, line, "durability flag contradicts policy")


def validate_manifest(path: Path, line: int, record: dict[str, Any]) -> None:
    validate_common(path, line, record, "manifest")
    require_integer(path, line, record, "timestamp_epoch_ms")
    require_string(path, line, record, "os")
    require_string(path, line, record, "compiler")
    require_integer(path, line, record, "cpp", 1)
    require_integer(path, line, record, "hardware_threads")
    require_string(path, line, record, "build_commit")
    require_string(path, line, record, "build_flags")
    durability_label = require_string(path, line, record, "durability_label")
    reopen_precondition = require_string(
        path, line, record, "reopen_precondition"
    )
    unsafe = record["policy"] == "unsafe-no-flush"
    expected_label = UNSAFE_LABEL if unsafe else DURABLE_LABEL
    expected_precondition = (
        "explicit-final-flush-required-and-performed"
        if unsafe
        else "append-policy-already-flushed"
    )
    if durability_label != expected_label:
        fail(path, line, "durability_label contradicts policy")
    if reopen_precondition != expected_precondition:
        fail(path, line, "reopen_precondition contradicts policy")


def validate_summary(path: Path, line: int, record: dict[str, Any]) -> None:
    # Refuse before aggregation. This deliberately does not have the generic
    # plotter's quiet "successful rows only" behavior.
    status = require_string(path, line, record, "status")
    if status != "ok":
        fail(
            path,
            line,
            f"run status is {status!r}; refusing to average a failed run",
        )
    validate_common(path, line, record, "summary")
    entries = record["entries"]
    policy = record["policy"]
    group_size = record["group_size"]
    append_calls = require_integer(path, line, record, "append_calls", 1)
    expected_calls = (
        (entries + group_size - 1) // group_size
        if policy == "group"
        else entries
    )
    if append_calls != expected_calls:
        fail(path, line, "append_calls does not match policy and entry count")
    append_wall_ns = require_integer(path, line, record, "append_wall_ns")
    require_integer(path, line, record, "append_cpu_ns")
    throughput = require_number(
        path, line, record, "append_ops_per_second"
    )
    if append_wall_ns > 0:
        expected_throughput = entries * 1_000_000_000.0 / append_wall_ns
        if not math.isclose(throughput, expected_throughput, rel_tol=2e-6, abs_tol=0.002):
            fail(path, line, "append_ops_per_second contradicts append_wall_ns")
    elif throughput != 0.0:
        fail(path, line, "zero append_wall_ns requires zero throughput")
    explicit_flush = require_boolean(
        path, line, record, "final_explicit_flush_performed"
    )
    unsafe = policy == "unsafe-no-flush"
    if explicit_flush != unsafe:
        fail(path, line, "final explicit flush flag contradicts policy")
    flush_wall = require_integer(path, line, record, "final_flush_wall_ns")
    flush_cpu = require_integer(path, line, record, "final_flush_cpu_ns")
    if not unsafe and (flush_wall != 0 or flush_cpu != 0):
        fail(path, line, "durable policy unexpectedly reports a final flush")
    require_integer(path, line, record, "file_bytes", 1)
    require_integer(path, line, record, "reopen_scan_wall_ns")
    recovered_entries = require_integer(
        path, line, record, "recovered_entry_count"
    )
    recovered_commit = require_integer(
        path, line, record, "recovered_commit_index"
    )
    if recovered_entries != entries or recovered_commit != entries:
        fail(path, line, "recovery result does not match requested entries")


def validate_pair(path: Path, manifest_line: int, manifest: dict[str, Any],
                  summary_line: int, summary: dict[str, Any]) -> None:
    for field in (
        "schema",
        "entries",
        "payload_bytes",
        "trial",
        "policy",
        "group_size",
        "durable_during_append",
    ):
        if manifest.get(field) != summary.get(field):
            fail(
                path,
                summary_line,
                f"summary {field!r} does not match manifest at line {manifest_line}",
            )


def load_runs(paths: Iterable[Path]) -> list[Run]:
    runs: list[Run] = []
    seen_sources: set[str] = set()
    for path in paths:
        resolved = str(path.resolve())
        if resolved in seen_sources:
            raise ValueError(f"duplicate input file would double-count runs: {path}")
        seen_sources.add(resolved)
        pending: tuple[int, dict[str, Any]] | None = None
        seen_run_keys: set[tuple[Any, ...]] = set()
        with path.open("r", encoding="utf-8") as source:
            for line_number, raw_line in enumerate(source, 1):
                line = raw_line.strip()
                if not line:
                    continue
                try:
                    parsed = json.loads(line)
                except json.JSONDecodeError as error:
                    fail(path, line_number, f"invalid JSON: {error}")
                record = require_object(path, line_number, parsed)
                kind = record.get("record")
                if kind == "manifest":
                    if pending is not None:
                        fail(
                            path,
                            line_number,
                            f"manifest at line {pending[0]} has no summary",
                        )
                    validate_manifest(path, line_number, record)
                    pending = (line_number, record)
                elif kind == "summary":
                    if pending is None:
                        fail(path, line_number, "summary has no preceding manifest")
                    validate_summary(path, line_number, record)
                    manifest_line, manifest = pending
                    validate_pair(
                        path, manifest_line, manifest, line_number, record
                    )
                    run_key = (
                        record["entries"],
                        record["payload_bytes"],
                        record["trial"],
                        record["policy"],
                        record["group_size"],
                    )
                    if run_key in seen_run_keys:
                        fail(path, line_number, "duplicate run coordinates in one source")
                    seen_run_keys.add(run_key)
                    runs.append(
                        Run(
                            source_file=resolved,
                            manifest_line=manifest_line,
                            summary_line=line_number,
                            manifest=manifest,
                            summary=record,
                        )
                    )
                    pending = None
                else:
                    fail(path, line_number, f"unknown record type {kind!r}")
        if pending is not None:
            fail(path, pending[0], "manifest has no following summary")
    if not runs:
        raise ValueError("no complete WAL manifest/summary pairs were found")
    return runs


def assign_provenance_ids(runs: list[Run]) -> dict[str, tuple[Any, ...]]:
    ids: dict[tuple[Any, ...], str] = {}
    descriptions: dict[str, tuple[Any, ...]] = {}
    for run in runs:
        key = run.provenance_key()
        if key not in ids:
            identifier = f"P{len(ids) + 1}"
            ids[key] = identifier
            descriptions[identifier] = key
        run.provenance_id = ids[key]
    return descriptions


def policy_display(policy: str, group_size: int) -> str:
    if policy == "group":
        return f"group({group_size})"
    if policy == "unsafe-no-flush":
        return "UNSAFE: no flush during append"
    return "flush every append"


def csv_text(runs: list[Run]) -> str:
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=CSV_FIELDS)
    writer.writeheader()
    for run in runs:
        manifest = run.manifest
        summary = run.summary
        unsafe = manifest["policy"] == "unsafe-no-flush"
        row = {
            "source_file": run.source_file,
            "manifest_line": run.manifest_line,
            "summary_line": run.summary_line,
            "provenance_id": run.provenance_id,
            "schema": manifest["schema"],
            "timestamp_epoch_ms": manifest["timestamp_epoch_ms"],
            "os": manifest["os"],
            "compiler": manifest["compiler"],
            "cpp": manifest["cpp"],
            "hardware_threads": manifest["hardware_threads"],
            "build_commit": manifest["build_commit"],
            "build_flags": manifest["build_flags"],
            "entries": summary["entries"],
            "payload_bytes": summary["payload_bytes"],
            "trial": summary["trial"],
            "policy": summary["policy"],
            "policy_display": policy_display(
                summary["policy"], summary["group_size"]
            ),
            "group_size": summary["group_size"],
            "durable_during_append": summary["durable_during_append"],
            "durability_label": manifest["durability_label"],
            "reopen_precondition": manifest["reopen_precondition"],
            "unsafe_warning": UNSAFE_WARNING if unsafe else "",
            "status": summary["status"],
        }
        for field in CSV_FIELDS:
            if field not in row and field in summary:
                row[field] = summary[field]
        writer.writerow(row)
    return output.getvalue()


def aggregate(runs: list[Run]) -> list[MeanPoint]:
    grouped: dict[tuple[Any, ...], list[Run]] = defaultdict(list)
    for run in runs:
        grouped[run.aggregation_key()].append(run)
    points: list[MeanPoint] = []
    policy_order = {policy: index for index, policy in enumerate(POLICIES)}
    for grouped_runs in grouped.values():
        first = grouped_runs[0]
        summary = first.summary
        append_values = [
            float(run.summary["append_ops_per_second"])
            for run in grouped_runs
        ]
        file_values = [
            float(run.summary["file_bytes"]) for run in grouped_runs
        ]
        scan_values = [
            float(run.summary["reopen_scan_wall_ns"])
            for run in grouped_runs
        ]
        points.append(
            MeanPoint(
                provenance_id=first.provenance_id,
                entries=summary["entries"],
                payload_bytes=summary["payload_bytes"],
                policy=summary["policy"],
                group_size=summary["group_size"],
                durable_during_append=summary["durable_during_append"],
                trials=len(grouped_runs),
                append_ops_per_second=statistics.fmean(append_values),
                append_ops_min=min(append_values),
                append_ops_max=max(append_values),
                file_bytes=statistics.fmean(file_values),
                file_bytes_min=min(file_values),
                file_bytes_max=max(file_values),
                reopen_scan_wall_ns=statistics.fmean(scan_values),
                reopen_scan_min_ns=min(scan_values),
                reopen_scan_max_ns=max(scan_values),
            )
        )
    return sorted(
        points,
        key=lambda point: (
            int(point.provenance_id[1:]),
            point.entries,
            point.payload_bytes,
            policy_order[point.policy],
            point.group_size,
        ),
    )


def compact_number(value: float) -> str:
    if value >= 1_000_000_000:
        return f"{value / 1_000_000_000:.3g}G"
    if value >= 1_000_000:
        return f"{value / 1_000_000:.3g}M"
    if value >= 1_000:
        return f"{value / 1_000:.3g}k"
    return f"{value:.3g}"


def svg_text(
    points: list[MeanPoint], provenance: dict[str, tuple[Any, ...]]
) -> str:
    if not points:
        raise ValueError("no valid WAL measurements are available to plot")
    left = 105
    right = 45
    bar_step = 55
    width = max(1200, left + right + len(points) * bar_step)
    provenance_top = 92
    provenance_line_height = 19
    provenance_bottom = provenance_top + len(provenance) * provenance_line_height
    panel_height = 375
    first_top = provenance_bottom + 30
    first_bottom = first_top + panel_height
    second_top = first_bottom + 95
    second_bottom = second_top + panel_height
    height = second_bottom + 70
    chart_left = left
    chart_right = width - right

    colors = {
        "flush-every": "#2563eb",
        "group": "#059669",
        "unsafe-no-flush": "url(#unsafeHatch)",
    }
    pieces = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}">',
        '<defs><pattern id="unsafeHatch" width="8" height="8" '
        'patternUnits="userSpaceOnUse" patternTransform="rotate(45)">'
        '<rect width="8" height="8" fill="#fee2e2"/>'
        '<line x1="0" y1="0" x2="0" y2="8" stroke="#b91c1c" '
        'stroke-width="4"/></pattern></defs>',
        '<rect width="100%" height="100%" fill="white"/>',
        '<style>text{font-family:system-ui,sans-serif;fill:#111827}'
        '.small{font-size:10px}.axis{stroke:#9ca3af;stroke-width:1}'
        '.grid{stroke:#d1d5db;stroke-width:1;opacity:.6}</style>',
        '<text x="20" y="30" font-size="21" font-weight="700">'
        'DetLog WAL benchmark — durable and nondurable policies kept distinct</text>',
        f'<rect x="20" y="42" width="{width - 40}" height="34" rx="4" '
        'fill="#fee2e2" stroke="#b91c1c" stroke-width="2"/>',
        f'<text x="32" y="64" font-size="14" font-weight="800" fill="#991b1b">'
        f'{html.escape(UNSAFE_WARNING)}</text>',
    ]

    for line_index, (identifier, key) in enumerate(provenance.items()):
        source, operating_system, compiler, cpp, threads, commit, flags = key
        display = (
            f"{identifier}: source={Path(source).name} | os={operating_system} | "
            f"compiler={compiler} | C++={cpp} | hw_threads={threads} | "
            f"commit={commit} | flags={flags}"
        )
        y = provenance_top + line_index * provenance_line_height
        pieces.append(
            f'<text x="24" y="{y}" font-size="11">'
            f'{html.escape(display)}<title>{html.escape(source)}</title></text>'
        )

    def axes(panel_top: int, panel_bottom: int, maximum: float,
             title: str, y_label: str) -> None:
        pieces.append(
            f'<text x="{chart_left}" y="{panel_top - 18}" font-size="17" '
            f'font-weight="700">{html.escape(title)}</text>'
        )
        pieces.append(
            f'<text transform="translate(22,{(panel_top + panel_bottom) / 2:.1f}) '
            f'rotate(-90)" text-anchor="middle" font-size="12">'
            f'{html.escape(y_label)}</text>'
        )
        pieces.append(
            f'<line class="axis" x1="{chart_left}" y1="{panel_top}" '
            f'x2="{chart_left}" y2="{panel_bottom}"/>'
        )
        pieces.append(
            f'<line class="axis" x1="{chart_left}" y1="{panel_bottom}" '
            f'x2="{chart_right}" y2="{panel_bottom}"/>'
        )
        scale_max = maximum if maximum > 0 else 1.0
        for tick in range(6):
            fraction = tick / 5
            y = panel_bottom - fraction * (panel_bottom - panel_top)
            pieces.append(
                f'<line class="grid" x1="{chart_left}" y1="{y:.1f}" '
                f'x2="{chart_right}" y2="{y:.1f}"/>'
            )
            pieces.append(
                f'<text class="small" text-anchor="end" x="{chart_left - 8}" '
                f'y="{y + 3:.1f}">{compact_number(scale_max * fraction)}</text>'
            )

    append_max = max(point.append_ops_max for point in points)
    axes(
        first_top,
        first_bottom,
        append_max,
        "Append throughput (trial means with min–max whiskers; final unsafe flush excluded)",
        "entries / second",
    )
    append_scale = append_max if append_max > 0 else 1.0
    for index, point in enumerate(points):
        x = chart_left + 12 + index * bar_step
        height_value = (
            (first_bottom - first_top)
            * point.append_ops_per_second
            / append_scale
        )
        y = first_bottom - height_value
        outline = "#991b1b" if not point.durable_during_append else "#065f46"
        label = policy_display(point.policy, point.group_size)
        title = (
            f"{point.provenance_id} | entries={point.entries} | "
            f"payload={point.payload_bytes} B | {label} | trials={point.trials} | "
            f"mean={point.append_ops_per_second:.6g} entries/s | "
            f"min={point.append_ops_min:.6g} | max={point.append_ops_max:.6g}"
        )
        pieces.append(
            f'<rect x="{x}" y="{y:.2f}" width="34" height="{height_value:.2f}" '
            f'fill="{colors[point.policy]}" stroke="{outline}" stroke-width="2">'
            f'<title>{html.escape(title)}</title></rect>'
        )
        whisker_top = (
            first_bottom
            - point.append_ops_max / append_scale * (first_bottom - first_top)
        )
        whisker_bottom = (
            first_bottom
            - point.append_ops_min / append_scale * (first_bottom - first_top)
        )
        whisker_x = x + 17
        pieces.append(
            f'<line x1="{whisker_x}" y1="{whisker_top:.2f}" '
            f'x2="{whisker_x}" y2="{whisker_bottom:.2f}" '
            'stroke="#111827" stroke-width="1.5"/>'
        )
        pieces.append(
            f'<line x1="{whisker_x - 6}" y1="{whisker_top:.2f}" '
            f'x2="{whisker_x + 6}" y2="{whisker_top:.2f}" '
            'stroke="#111827" stroke-width="1.5"/>'
        )
        pieces.append(
            f'<line x1="{whisker_x - 6}" y1="{whisker_bottom:.2f}" '
            f'x2="{whisker_x + 6}" y2="{whisker_bottom:.2f}" '
            'stroke="#111827" stroke-width="1.5"/>'
        )
        short_policy = (
            f"G{point.group_size}"
            if point.policy == "group"
            else ("UNSAFE" if point.policy == "unsafe-no-flush" else "F")
        )
        label_text = (
            f"{point.provenance_id} e{point.entries} p{point.payload_bytes} "
            f"{short_policy}"
        )
        pieces.append(
            f'<text class="small" transform="translate({x + 4},{first_bottom + 12}) '
            f'rotate(58)">{html.escape(label_text)}</text>'
        )

    file_max = max(point.file_bytes_max for point in points) or 1.0
    scan_ms_max = max(point.reopen_scan_max_ns / 1_000_000.0 for point in points)
    axes(
        second_top,
        second_bottom,
        scan_ms_max,
        "Reopen scan time vs WAL file size (trial means with min–max whiskers)",
        "reopen scan milliseconds",
    )
    scan_scale = scan_ms_max if scan_ms_max > 0 else 1.0
    for tick in range(6):
        fraction = tick / 5
        x = chart_left + fraction * (chart_right - chart_left)
        pieces.append(
            f'<line class="grid" x1="{x:.1f}" y1="{second_top}" '
            f'x2="{x:.1f}" y2="{second_bottom}"/>'
        )
        pieces.append(
            f'<text class="small" text-anchor="middle" x="{x:.1f}" '
            f'y="{second_bottom + 18}">{file_max * fraction / 1_048_576:.3g}</text>'
        )
    pieces.append(
        f'<text x="{(chart_left + chart_right) / 2:.1f}" '
        f'y="{second_bottom + 39}" text-anchor="middle" font-size="12">'
        'WAL file size (MiB)</text>'
    )
    for point in points:
        x = chart_left + point.file_bytes / file_max * (chart_right - chart_left)
        scan_ms = point.reopen_scan_wall_ns / 1_000_000.0
        y = second_bottom - scan_ms / scan_scale * (second_bottom - second_top)
        unsafe = not point.durable_during_append
        fill = colors[point.policy]
        stroke = "#991b1b" if unsafe else "#111827"
        label = policy_display(point.policy, point.group_size)
        title = (
            f"{point.provenance_id} | entries={point.entries} | "
            f"payload={point.payload_bytes} B | {label} | trials={point.trials} | "
            f"mean file={point.file_bytes:.6g} B "
            f"[{point.file_bytes_min:.6g}, {point.file_bytes_max:.6g}] | "
            f"mean scan={scan_ms:.6g} ms "
            f"[{point.reopen_scan_min_ns / 1_000_000.0:.6g}, "
            f"{point.reopen_scan_max_ns / 1_000_000.0:.6g}]"
        )
        x_min = chart_left + point.file_bytes_min / file_max * (chart_right - chart_left)
        x_max = chart_left + point.file_bytes_max / file_max * (chart_right - chart_left)
        y_min = second_bottom - (
            point.reopen_scan_min_ns / 1_000_000.0 / scan_scale
            * (second_bottom - second_top)
        )
        y_max = second_bottom - (
            point.reopen_scan_max_ns / 1_000_000.0 / scan_scale
            * (second_bottom - second_top)
        )
        pieces.append(
            f'<line x1="{x_min:.2f}" y1="{y:.2f}" x2="{x_max:.2f}" '
            f'y2="{y:.2f}" stroke="{stroke}" stroke-width="1.5"/>'
        )
        pieces.append(
            f'<line x1="{x:.2f}" y1="{y_max:.2f}" x2="{x:.2f}" '
            f'y2="{y_min:.2f}" stroke="{stroke}" stroke-width="1.5"/>'
        )
        pieces.append(
            f'<circle cx="{x:.2f}" cy="{y:.2f}" r="7" fill="{fill}" '
            f'stroke="{stroke}" stroke-width="2"><title>{html.escape(title)}</title></circle>'
        )
        pieces.append(
            f'<text class="small" x="{x + 9:.2f}" y="{y - 7:.2f}">'
            f'{html.escape(point.provenance_id)}</text>'
        )
        if unsafe:
            pieces.append(
                f'<line x1="{x - 6:.2f}" y1="{y - 6:.2f}" x2="{x + 6:.2f}" '
                f'y2="{y + 6:.2f}" stroke="#991b1b" stroke-width="2"/>'
            )
            pieces.append(
                f'<line x1="{x + 6:.2f}" y1="{y - 6:.2f}" x2="{x - 6:.2f}" '
                f'y2="{y + 6:.2f}" stroke="#991b1b" stroke-width="2"/>'
            )

    pieces.extend(
        [
            f'<rect x="{chart_left}" y="{height - 38}" width="16" height="12" '
            'fill="#2563eb"/><text x="{0}" y="{1}" font-size="11">'
            'flush-every (durable)</text>'.format(chart_left + 22, height - 28),
            f'<rect x="{chart_left + 190}" y="{height - 38}" width="16" height="12" '
            'fill="#059669"/><text x="{0}" y="{1}" font-size="11">'
            'group flush (durable)</text>'.format(chart_left + 212, height - 28),
            f'<rect x="{chart_left + 390}" y="{height - 38}" width="16" height="12" '
            'fill="url(#unsafeHatch)" stroke="#991b1b"/>'
            f'<text x="{chart_left + 412}" y="{height - 28}" font-size="11" '
            'font-weight="800" fill="#991b1b">UNSAFE no-flush during append</text>',
            "</svg>",
        ]
    )
    return "\n".join(pieces) + "\n"


def require_derived_name(path: Path, kind: str) -> None:
    if DERIVED_NAME_MARKER not in path.name:
        raise ValueError(
            f"{kind} filename must contain {DERIVED_NAME_MARKER!r}: {path}"
        )


def require_unsafe_source_names(runs: list[Run]) -> None:
    unsafe_sources = {
        run.source_file
        for run in runs
        if run.manifest["policy"] == "unsafe-no-flush"
    }
    for source in unsafe_sources:
        if DERIVED_NAME_MARKER not in Path(source).name:
            raise ValueError(
                "input containing unsafe-no-flush records must include "
                f"{DERIVED_NAME_MARKER!r} in its filename: {source}"
            )


def write_atomically(path: Path, content: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(content, encoding="utf-8", newline="")
    return temporary


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "jsonl", nargs="+", type=Path, help="raw WAL benchmark JSONL"
    )
    parser.add_argument("--csv", type=Path, required=True, help="derived CSV")
    parser.add_argument("--svg", type=Path, required=True, help="derived SVG")
    arguments = parser.parse_args()

    require_derived_name(arguments.csv, "CSV")
    require_derived_name(arguments.svg, "SVG")
    if arguments.csv.resolve() == arguments.svg.resolve():
        raise ValueError("CSV and SVG output paths must differ")

    runs = load_runs(arguments.jsonl)
    require_unsafe_source_names(runs)
    provenance = assign_provenance_ids(runs)
    points = aggregate(runs)
    csv_output = csv_text(runs)
    svg_output = svg_text(points, provenance)

    csv_temp: Path | None = None
    svg_temp: Path | None = None
    try:
        csv_temp = write_atomically(arguments.csv, csv_output)
        svg_temp = write_atomically(arguments.svg, svg_output)
        os.replace(csv_temp, arguments.csv)
        csv_temp = None
        os.replace(svg_temp, arguments.svg)
        svg_temp = None
    finally:
        for temporary in (csv_temp, svg_temp):
            if temporary is not None:
                temporary.unlink(missing_ok=True)

    print(
        f"validated {len(runs)} WAL runs across {len(provenance)} "
        f"source/provenance groups; wrote {arguments.csv} and {arguments.svg}"
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError) as error:
        raise SystemExit(f"plot_wal_bench.py: {error}") from error
