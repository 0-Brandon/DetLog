#!/usr/bin/env python3
"""Deterministically compress raw benchmark streams and hash an artifact set."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
from pathlib import Path

from validate_benchmark_artifacts import validate_directory


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def artifact_files(directory: Path) -> list[Path]:
    if directory.is_symlink():
        raise ValueError(f"artifact directory must not be a symlink: {directory}")
    files: list[Path] = []
    for path in directory.iterdir():
        if path.is_symlink() or not path.is_file():
            raise ValueError(
                f"{directory}: artifact entries must be regular top-level "
                f"files: {path.name!r}"
            )
        if path.name.endswith(".tmp"):
            raise ValueError(
                f"{directory}: stale temporary artifact is present: {path.name!r}"
            )
        try:
            path.name.encode("ascii")
        except UnicodeEncodeError as error:
            raise ValueError(
                f"{directory}: checksum filenames must be ASCII: {path.name!r}"
            ) from error
        if "\n" in path.name or "\r" in path.name or "\\" in path.name:
            raise ValueError(
                f"{directory}: checksum-ambiguous filename: {path.name!r}"
            )
        files.append(path)
    return sorted(files)


def consume_completion_marker(directory: Path) -> None:
    marker = directory / "campaign-complete.json"
    if not marker.exists():
        return
    try:
        completion = json.loads(marker.read_text(encoding="ascii"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise ValueError(f"{marker}: invalid completion marker: {error}") from error
    if (
        not isinstance(completion, dict)
        or completion.get("schema")
        != "detlog-benchmark-campaign-complete/v1"
        or not isinstance(completion.get("files"), dict)
    ):
        raise ValueError(f"{marker}: unsupported completion marker")
    matrix_paths = [
        path
        for path in (directory / "matrix.json", directory / "matrix-manifest.json")
        if path.is_file() and not path.is_symlink()
    ]
    if len(matrix_paths) != 1:
        raise ValueError(f"{marker}: completion marker needs one matrix")
    matrix = json.loads(matrix_paths[0].read_text(encoding="utf-8"))
    if matrix.get("schema") == "detlog-wal-bench-matrix/v1":
        expected_names = {
            "matrix-manifest.json",
            matrix.get("raw_jsonl"),
            matrix.get("diagnostics"),
            matrix.get("derived_csv"),
            matrix.get("derived_svg"),
            "environment.json",
        }
    else:
        expected_names = {
            "matrix.json",
            "raw.jsonl",
            "stderr.log",
            "summary.csv",
            "throughput.svg",
            "environment.json",
        }
    if set(completion["files"]) != expected_names:
        raise ValueError(f"{marker}: completion inventory is not exact")
    for name, expected in completion["files"].items():
        if (
            not isinstance(name, str)
            or Path(name).name != name
            or not isinstance(expected, dict)
        ):
            raise ValueError(f"{marker}: unsafe completion inventory entry")
        path = directory / name
        if path.is_symlink() or not path.is_file():
            raise ValueError(f"{marker}: completed file is missing: {name!r}")
        if path.stat().st_size != expected.get("bytes") or sha256(path) != expected.get(
            "sha256"
        ):
            raise ValueError(f"{marker}: completed file changed: {name!r}")
    marker.unlink()
    print(f"verified and removed pre-package completion marker {marker}")


def compress_jsonl(path: Path) -> Path:
    destination = path.with_suffix(path.suffix + ".gz")
    temporary = destination.with_suffix(destination.suffix + ".tmp")
    line_count = 0
    try:
        with path.open("rb") as source, temporary.open("wb") as raw_output:
            with gzip.GzipFile(
                filename="", mode="wb", fileobj=raw_output, compresslevel=9, mtime=0
            ) as output:
                for line in source:
                    output.write(line)
                    line_count += 1
        recovered_lines = 0
        with gzip.open(temporary, "rb") as source:
            for _ in source:
                recovered_lines += 1
        if recovered_lines != line_count:
            raise ValueError(
                f"{path}: compressed line count {recovered_lines} != {line_count}"
            )
        temporary.replace(destination)
        return destination
    finally:
        temporary.unlink(missing_ok=True)


def package(directory: Path, remove_raw: bool) -> None:
    if not directory.is_dir():
        raise ValueError(f"artifact directory does not exist: {directory}")
    artifact_files(directory)
    result = validate_directory(directory)
    print(
        f"validated {directory}: runs={result['runs']} "
        f"records={result['records']}"
    )
    consume_completion_marker(directory)
    raw_files = sorted(directory.glob("raw*.jsonl"))
    if raw_files and not remove_raw:
        raise ValueError(
            f"{directory}: --remove-raw is required when packaging an "
            "uncompressed stream so the final artifact has one unambiguous "
            "raw representation"
        )
    for raw in raw_files:
        compressed = compress_jsonl(raw)
        print(f"compressed {raw} -> {compressed}")
        if remove_raw:
            raw.unlink()

    result = validate_directory(directory)
    print(
        f"validated packaged {directory}: runs={result['runs']} "
        f"records={result['records']}"
    )

    files = [
        path
        for path in artifact_files(directory)
        if path.name != "SHA256SUMS"
    ]
    if not files:
        raise ValueError(f"artifact directory is empty: {directory}")
    checksum = directory / "SHA256SUMS"
    checksum.write_text(
        "".join(f"{sha256(path)}  {path.name}\n" for path in files),
        encoding="ascii",
        newline="\n",
    )
    print(f"hashed {len(files)} files in {directory}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("directories", nargs="+", type=Path)
    parser.add_argument(
        "--remove-raw",
        action="store_true",
        help=(
            "remove raw JSONL only after deterministic gzip verification; "
            "required when packaging an uncompressed stream"
        ),
    )
    arguments = parser.parse_args()
    for directory in arguments.directories:
        package(directory, arguments.remove_raw)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
