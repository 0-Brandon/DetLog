#!/usr/bin/env python3
"""Convert DetLog benchmark JSONL into a summary CSV and simple SVG chart."""

from __future__ import annotations

import argparse
import csv
import html
import json
import math
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable


CSV_FIELDS = [
    "source_file",
    "os",
    "compiler",
    "hardware_threads",
    "build_commit",
    "build_flags",
    "mode",
    "scenario",
    "nodes",
    "clients",
    "payload_bytes",
    "operations",
    "trial",
    "seed",
    "successes",
    "failures",
    "retries",
    "queue_rejections",
    "duration",
    "duration_unit",
    "wall_duration_ns",
    "process_cpu_ns",
    "process_cpu_scope",
    "peak_resident_bytes",
    "memory_scope",
    "throughput",
    "throughput_unit",
    "p50",
    "p95",
    "p99",
    "latency_unit",
    "elections",
    "transport_backpressure",
    "transport_down_drops",
    "storage_errors",
    "owner_queue_high_water",
    "client_queue_high_water",
    "sim_event_queue_high_water",
    "sim_network_bytes_high_water",
    "sim_storage_bytes_high_water",
    "fault_at",
    "replacement_leader_ready_duration",
    "recovery_to_first_success",
    "status",
    "safety_check",
]


def load_summaries(paths: Iterable[Path]) -> list[dict[str, Any]]:
    summaries: list[dict[str, Any]] = []
    for path in paths:
        manifest: dict[str, Any] | None = None
        with path.open("r", encoding="utf-8-sig") as source:
            for line_number, raw_line in enumerate(source, 1):
                line = raw_line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError as error:
                    raise ValueError(
                        f"{path}:{line_number}: invalid JSON: {error}"
                    ) from error
                if record.get("record") == "manifest":
                    manifest = record
                elif record.get("record") == "summary":
                    combined = dict(record)
                    combined["source_file"] = str(path)
                    if manifest is not None:
                        for field in (
                            "os",
                            "compiler",
                            "hardware_threads",
                            "build_commit",
                            "build_flags",
                        ):
                            combined[field] = manifest.get(field)
                    summaries.append(combined)
    if not summaries:
        raise ValueError("no summary records were found in the JSONL input")
    return summaries


def write_csv(path: Path, summaries: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for summary in summaries:
            writer.writerow({field: summary.get(field) for field in CSV_FIELDS})


def aggregate(
    summaries: list[dict[str, Any]],
) -> dict[str, list[tuple[str, float, str, float, float, int]]]:
    grouped: dict[tuple[Any, ...], list[float]] = defaultdict(list)
    for row in summaries:
        if row.get("status") != "complete" or row.get("failures") != 0:
            continue
        commit = row.get("build_commit")
        flags = row.get("build_flags")
        provenance = (
            row.get("source_file", "unknown"),
            row.get("os", "unknown"),
            row.get("compiler", "unknown"),
            row.get("hardware_threads", "unknown"),
            commit,
            flags,
        )
        key = (
            row.get("throughput_unit", "unknown"),
            provenance,
            row.get("mode", "?"),
            row.get("scenario", "?"),
            row.get("nodes", "?"),
            row.get("clients", "?"),
            row.get("payload_bytes", "?"),
        )
        value = row.get("throughput")
        if isinstance(value, (int, float)) and math.isfinite(float(value)):
            grouped[key].append(float(value))

    panels: dict[
        str, list[tuple[str, float, str, float, float, int]]
    ] = defaultdict(list)
    for key, values in sorted(grouped.items(), key=lambda item: item[0]):
        unit, provenance, mode, scenario, nodes, clients, payload = key
        provenance_label = Path(str(provenance[0])).name
        if provenance[4] not in (None, "not_provided"):
            provenance_label += f"@{str(provenance[4])[:12]}"
        label = (
            f"{mode}/{scenario} n{nodes} c{clients} p{payload} "
            f"build={provenance_label}"
        )
        panels[str(unit)].append(
            (
                label,
                sum(values) / len(values),
                str(scenario),
                min(values),
                max(values),
                len(values),
            )
        )
    return panels


def write_svg(path: Path, summaries: list[dict[str, Any]]) -> None:
    panels = aggregate(summaries)
    if not panels:
        raise ValueError(
            "summary records contain no successful finite throughput values"
        )

    margin_left = 90
    margin_right = 30
    panel_height = 340
    label_height = 110
    bar_step = 58
    widest_panel = max(len(values) for values in panels.values())
    width = max(900, margin_left + margin_right + widest_panel * bar_step)
    height = 50 + len(panels) * panel_height
    colors = {
        "healthy": "#2563eb",
        "leader-crash": "#dc2626",
        "partition": "#d97706",
        "slow-follower": "#7c3aed",
        "slow-fsync": "#059669",
    }

    pieces = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="white"/>',
        '<style>text{font-family:system-ui,sans-serif;fill:#111827}'
        '.small{font-size:10px}.axis{stroke:#9ca3af;stroke-width:1}</style>',
        '<text x="20" y="28" font-size="20" font-weight="600">'
        "DetLog benchmark throughput (mean bars, min-max whiskers)</text>",
    ]

    for panel_index, (unit, values) in enumerate(sorted(panels.items())):
        top = 50 + panel_index * panel_height
        chart_top = top + 35
        chart_bottom = top + panel_height - label_height
        chart_height = chart_bottom - chart_top
        maximum = max(high for _, _, _, _, high, _ in values) or 1.0
        pieces.append(
            f'<text x="{margin_left}" y="{top + 18}" font-size="14" '
            f'font-weight="600">{html.escape(unit)}</text>'
        )
        pieces.append(
            f'<line class="axis" x1="{margin_left}" y1="{chart_bottom}" '
            f'x2="{width - margin_right}" y2="{chart_bottom}"/>'
        )
        pieces.append(
            f'<line class="axis" x1="{margin_left}" y1="{chart_top}" '
            f'x2="{margin_left}" y2="{chart_bottom}"/>'
        )
        for tick in range(5):
            fraction = tick / 4
            y = chart_bottom - fraction * chart_height
            tick_value = maximum * fraction
            pieces.append(
                f'<line class="axis" x1="{margin_left - 4}" y1="{y:.1f}" '
                f'x2="{width - margin_right}" y2="{y:.1f}" opacity="0.25"/>'
            )
            pieces.append(
                f'<text class="small" text-anchor="end" x="{margin_left - 8}" '
                f'y="{y + 3:.1f}">{tick_value:.3g}</text>'
            )
        for index, (label, value, scenario, low, high, trials) in enumerate(values):
            x = margin_left + 12 + index * bar_step
            bar_height = chart_height * value / maximum
            y = chart_bottom - bar_height
            low_y = chart_bottom - chart_height * low / maximum
            high_y = chart_bottom - chart_height * high / maximum
            color = colors.get(scenario, "#4b5563")
            pieces.append(
                f'<rect x="{x}" y="{y:.1f}" width="34" height="{bar_height:.1f}" '
                f'fill="{color}"><title>{html.escape(label)}: mean={value:.6g}, '
                f'min={low:.6g}, max={high:.6g}, trials={trials}</title></rect>'
            )
            pieces.append(
                f'<line x1="{x + 17}" y1="{high_y:.1f}" x2="{x + 17}" '
                f'y2="{low_y:.1f}" stroke="#111827" stroke-width="1.5"/>'
            )
            for whisker_y in (high_y, low_y):
                pieces.append(
                    f'<line x1="{x + 11}" y1="{whisker_y:.1f}" '
                    f'x2="{x + 23}" y2="{whisker_y:.1f}" '
                    'stroke="#111827" stroke-width="1.5"/>'
                )
            pieces.append(
                f'<text class="small" transform="translate({x + 5},{chart_bottom + 12}) '
                f'rotate(55)" text-anchor="start">{html.escape(label)}</text>'
            )
    pieces.append("</svg>")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(pieces) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("jsonl", nargs="+", type=Path, help="raw benchmark JSONL")
    parser.add_argument("--csv", type=Path, required=True, help="summary CSV output")
    parser.add_argument("--svg", type=Path, required=True, help="throughput SVG output")
    arguments = parser.parse_args()

    summaries = load_summaries(arguments.jsonl)
    write_csv(arguments.csv, summaries)
    write_svg(arguments.svg, summaries)
    print(
        f"wrote {len(summaries)} summaries to {arguments.csv} and {arguments.svg}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
