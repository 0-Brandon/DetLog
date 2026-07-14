#!/usr/bin/env python3
"""Focused cross-platform tests for the benchmark checkpoint transaction."""

from __future__ import annotations

import gzip
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
import run_checkpointed_campaign as runner  # noqa: E402
import package_benchmark_artifacts as packager  # noqa: E402
import validate_benchmark_artifacts as artifact_validator  # noqa: E402


BUILD_COMMIT = "1" * 40
BUILD_FLAGS = "test-release-flags"
RUNNER_COMMIT = "2" * 40
RUNNER_SHA256 = "3" * 64


def encode_records(records: list[dict[str, object]]) -> bytes:
    return b"".join(
        json.dumps(record, separators=(",", ":")).encode("utf-8") + b"\n"
        for record in records
    )


def write_wal_artifact(directory: Path, publication: bool = False) -> bytes:
    matrix: dict[str, object] = {
        "schema": "detlog-wal-bench-matrix/v1",
        "repetitions": 1,
        "entry_sizes": [1],
        "payload_bytes": [0],
        "policies": ["flush-every", "group", "unsafe-no-flush"],
        "group_sizes": [2],
        "expected_runs": 3,
        "raw_jsonl": "raw-includes-nondurable.jsonl",
    }
    if publication:
        matrix.update(
            {
                "environment": "environment.json",
                "runner": "scripts/run_checkpointed_campaign.py",
                "runner_commit": RUNNER_COMMIT,
                "runner_sha256": RUNNER_SHA256,
            }
        )
        (directory / "environment.json").write_bytes(
            json.dumps(
                {
                    "schema": "detlog-benchmark-environment/v1",
                    "worktree": "clean",
                    "build_commit": BUILD_COMMIT,
                    "build_flags": BUILD_FLAGS,
                },
                separators=(",", ":"),
            ).encode("ascii")
            + b"\n"
        )
    (directory / "matrix-manifest.json").write_bytes(
        json.dumps(matrix, separators=(",", ":")).encode("ascii") + b"\n"
    )

    records: list[dict[str, object]] = []
    for policy, group in (
        ("flush-every", 1),
        ("group", 2),
        ("unsafe-no-flush", 1),
    ):
        identity: dict[str, object] = {
            "entries": 1,
            "payload_bytes": 0,
            "trial": 1,
            "policy": policy,
            "group_size": group,
        }
        manifest = {"record": "manifest", **identity}
        if publication:
            manifest.update(
                {"build_commit": BUILD_COMMIT, "build_flags": BUILD_FLAGS}
            )
        records.extend(
            (manifest, {"record": "summary", "status": "ok", **identity})
        )
    raw = encode_records(records)
    (directory / "raw-includes-nondurable.jsonl").write_bytes(raw)
    return raw


def write_cluster_artifact(directory: Path) -> list[bytes]:
    matrix = {
        "schema": "detlog-bench-matrix/v1",
        "repetitions": 1,
        "operations_per_run": 6,
        "expected_runs": 144,
        "nodes": [3, 5],
        "clients": [1, 3, 5],
        "payload_bytes": [64, 1024, 16384],
        "sim_scenarios": [
            "healthy",
            "leader-crash",
            "partition",
            "slow-follower",
            "slow-fsync",
        ],
        "tcp_scenarios": ["healthy", "leader-crash", "partition"],
        "tcp_unsupported": ["slow-follower", "slow-fsync"],
        "tcp_partition_ms": 1000,
        "tcp_wal_flush_policy": "flush-every",
        "sim_wal_flush_policy": "simulated_flush_every",
    }
    (directory / "matrix.json").write_bytes(
        json.dumps(matrix, separators=(",", ":")).encode("ascii") + b"\n"
    )
    segments: list[bytes] = []
    ordinal = 0
    for mode, scenarios in (
        (
            "sim",
            ("healthy", "leader-crash", "partition", "slow-follower", "slow-fsync"),
        ),
        ("tcp", ("healthy", "leader-crash", "partition")),
    ):
        for nodes in (3, 5):
            for clients in (1, 3, 5):
                for payload in (64, 1024, 16384):
                    for scenario in scenarios:
                        ordinal += 1
                        seed = 1_000_000 + ordinal
                        identity: dict[str, object] = {
                            "mode": mode,
                            "scenario": scenario,
                            "nodes": nodes,
                            "clients": clients,
                            "payload_bytes": payload,
                            "operations": 6,
                            "trial": 1,
                            "seed": seed,
                        }
                        records: list[dict[str, object]] = [
                            {
                                "record": "manifest",
                                **identity,
                                "supported": True,
                                "tcp_partition_ms": 1000,
                                "wal_flush_policy": (
                                    "simulated_flush_every"
                                    if mode == "sim"
                                    else "flush-every"
                                ),
                            }
                        ]
                        for operation in range(1, 7):
                            records.append(
                                {
                                    "record": "operation",
                                    "mode": mode,
                                    "scenario": scenario,
                                    "trial": 1,
                                    "seed": seed,
                                    "payload_bytes": payload,
                                    "operation": operation,
                                    "status": "ok",
                                }
                            )
                        records.append(
                            {
                                "record": "summary",
                                **identity,
                                "successes": 6,
                                "failures": 0,
                                "status": "complete",
                            }
                        )
                        segments.append(encode_records(records))
    (directory / "raw.jsonl").write_bytes(b"".join(segments))
    return segments


def planned_run(ordinal: int) -> runner.PlannedRun:
    seed = 1_000_000 + ordinal
    manifest = {
        "mode": "sim",
        "scenario": "healthy",
        "nodes": 3,
        "clients": 1,
        "payload_bytes": 64,
        "operations": 1,
        "trial": 1,
        "seed": seed,
    }
    operation = {
        key: manifest[key]
        for key in ("mode", "scenario", "trial", "seed", "payload_bytes")
    }
    return runner.PlannedRun(
        ordinal=ordinal,
        arguments=(),
        description=f"test run {ordinal}",
        manifest_fields=manifest,
        summary_fields=manifest.copy(),
        operation_fields=operation,
        operations=1,
        summary_status="complete",
    )


def encode_segment(planned: runner.PlannedRun, newline: bytes = b"\n") -> bytes:
    manifest = {
        "record": "manifest",
        "schema": 1,
        **planned.manifest_fields,
        "build_commit": BUILD_COMMIT,
        "build_flags": BUILD_FLAGS,
        "supported": True,
    }
    operation = {
        "record": "operation",
        **planned.operation_fields,
        "operation": 1,
        "status": "ok",
    }
    summary = {
        "record": "summary",
        **planned.summary_fields,
        "successes": 1,
        "failures": 0,
        "status": "complete",
    }
    return b"".join(
        json.dumps(record, separators=(",", ":")).encode("ascii") + newline
        for record in (manifest, operation, summary)
    )


def definition(plan: tuple[runner.PlannedRun, ...]) -> runner.CampaignDefinition:
    return runner.CampaignDefinition(
        kind="cluster",
        matrix={"schema": "test"},
        plan=plan,
        layout=runner.CampaignLayout(
            "matrix.json",
            "raw.jsonl",
            "stderr.log",
            "summary.csv",
            "throughput.svg",
            "plot_bench.py",
        ),
    )


def bindings(plan: tuple[runner.PlannedRun, ...]) -> dict[str, object]:
    matrix_hash = runner.sha256_bytes(b"matrix")
    return {
        "schema": runner.CHECKPOINT_SCHEMA,
        "kind": "cluster",
        "matrix_sha256": matrix_hash,
        "plan_sha256": runner.plan_hash(plan),
        "executable": "/test/detlog-bench",
        "executable_sha256": runner.sha256_bytes(b"executable"),
        "environment_sha256": runner.sha256_bytes(b"environment"),
        "temp_directory": "/test/temp",
        "tmp_directory": "/test/temp",
        "build_commit": BUILD_COMMIT,
        "build_flags": BUILD_FLAGS,
        "expected_runs": len(plan),
        "raw_name": "raw.jsonl",
        "stderr_name": "stderr.log",
    }


class CheckpointRunnerTests(unittest.TestCase):
    def test_atomic_state_update_waits_out_a_transient_reader(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            target = Path(temporary) / "state.json"
            runner.write_atomic_json(target, {"value": 1})
            descriptor = runner.open_regular_fd(target, os.O_RDONLY, "test state")

            def release() -> None:
                time.sleep(0.1)
                os.close(descriptor)

            closer = threading.Thread(target=release)
            closer.start()
            try:
                runner.write_atomic_json(target, {"value": 2})
            finally:
                closer.join()
            self.assertEqual(
                json.loads(target.read_text(encoding="ascii")), {"value": 2}
            )
            delete_descriptor = runner.open_regular_fd(
                target, os.O_RDONLY, "test state deletion"
            )

            def release_delete() -> None:
                time.sleep(0.1)
                os.close(delete_descriptor)

            delete_closer = threading.Thread(target=release_delete)
            delete_closer.start()
            try:
                runner.unlink_with_retry(target)
            finally:
                delete_closer.join()
            self.assertFalse(target.exists())

            tree = Path(temporary) / "tree"
            tree.mkdir()
            held = tree / "held.txt"
            held.write_bytes(b"held")
            tree_descriptor = runner.open_regular_fd(
                held, os.O_RDONLY, "test tree deletion"
            )

            def release_tree() -> None:
                time.sleep(0.1)
                os.close(tree_descriptor)

            tree_closer = threading.Thread(target=release_tree)
            tree_closer.start()
            try:
                runner.rmtree_with_retry(tree)
            finally:
                tree_closer.join()
            self.assertFalse(tree.exists())

    def test_artifact_validator_enforces_order_and_seed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            segments = write_cluster_artifact(directory)
            self.assertEqual(
                artifact_validator.validate_directory(directory)["runs"], 144
            )

            raw = directory / "raw.jsonl"
            raw.write_bytes(segments[1] + segments[0] + b"".join(segments[2:]))
            with self.assertRaisesRegex(ValueError, "out of order"):
                artifact_validator.validate_directory(directory)

            records = [json.loads(line) for line in segments[0].splitlines()]
            for record in records:
                record["seed"] += 1
            wrong_seed = encode_records(records) + b"".join(segments[1:])
            raw.write_bytes(wrong_seed)
            with self.assertRaisesRegex(ValueError, "seed"):
                artifact_validator.validate_directory(directory)

    def test_artifact_validator_requires_lf_for_raw_and_gzip(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            raw_bytes = write_wal_artifact(directory)
            artifact_validator.validate_directory(directory)

            raw = directory / "raw-includes-nondurable.jsonl"
            raw.write_bytes(raw_bytes[:-1])
            with self.assertRaisesRegex(ValueError, "LF-terminated"):
                artifact_validator.validate_directory(directory)

            raw.unlink()
            (directory / "raw-includes-nondurable.jsonl.gz").write_bytes(
                gzip.compress(raw_bytes[:-1], mtime=0)
            )
            with self.assertRaisesRegex(ValueError, "LF-terminated"):
                artifact_validator.validate_directory(directory)

    def test_publication_provenance_is_all_or_none(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            write_wal_artifact(directory)
            artifact_validator.validate_directory(directory)
            matrix_path = directory / "matrix-manifest.json"
            matrix = json.loads(matrix_path.read_text(encoding="ascii"))
            matrix["environment"] = "environment.json"
            matrix_path.write_bytes(
                json.dumps(matrix, separators=(",", ":")).encode("ascii")
                + b"\n"
            )
            with self.assertRaisesRegex(ValueError, "all provenance fields"):
                artifact_validator.validate_directory(directory)

    def test_publication_provenance_matches_environment(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            write_wal_artifact(directory, publication=True)
            artifact_validator.validate_directory(directory)

            environment = directory / "environment.json"
            environment.unlink()
            with self.assertRaisesRegex(ValueError, "environment file is missing"):
                artifact_validator.validate_directory(directory)

            write_wal_artifact(directory, publication=True)
            raw = directory / "raw-includes-nondurable.jsonl"
            records = [json.loads(line) for line in raw.read_bytes().splitlines()]
            records[0]["build_commit"] = "4" * 40
            raw.write_bytes(encode_records(records))
            with self.assertRaisesRegex(ValueError, "build_commit"):
                artifact_validator.validate_directory(directory)

    def test_segment_accepts_crlf_and_requires_final_lf(self) -> None:
        planned = planned_run(1)
        segment = encode_segment(planned, b"\r\n")
        runner.validate_run_segment(segment, planned, BUILD_COMMIT, BUILD_FLAGS)
        with self.assertRaises(runner.CampaignError):
            runner.validate_run_segment(
                segment[:-1], planned, BUILD_COMMIT, BUILD_FLAGS
            )

    def test_ordered_prefix_rejects_swapped_runs(self) -> None:
        plan = (planned_run(1), planned_run(2))
        swapped = encode_segment(plan[1]) + encode_segment(plan[0])
        with self.assertRaises(runner.CampaignError):
            runner.split_committed_segments(
                swapped, plan, 2, BUILD_COMMIT, BUILD_FLAGS
            )

    def test_complete_looking_uncommitted_tail_is_quarantined(self) -> None:
        plan = (planned_run(1), planned_run(2))
        first = encode_segment(plan[0])
        second = encode_segment(plan[1])
        committed_stderr = b"committed diagnostic\n"
        extra_stderr = b"uncommitted diagnostic\n"
        campaign = definition(plan)
        bound = bindings(plan)
        chain = runner.initial_chain(
            str(bound["matrix_sha256"]), str(bound["plan_sha256"])
        )
        chain = runner.advance_chain(chain, 1, first)

        with tempfile.TemporaryDirectory() as temporary:
            temp_root = Path(temporary)
            output = temp_root / "campaign"
            output.mkdir()
            raw = output / "raw.jsonl"
            errors = output / "stderr.log"
            raw.write_bytes(first + second)
            errors.write_bytes(committed_stderr + extra_stderr)
            checkpoint = runner.checkpoint_value(
                bound,
                campaign,
                1,
                len(first),
                len(committed_stderr),
                chain,
                runner.sha256_bytes(committed_stderr),
            )
            old_root = runner.ROOT
            runner.ROOT = temp_root
            try:
                committed, recovered_chain = runner.verify_checkpoint_and_recover(
                    output,
                    campaign,
                    bound,
                    checkpoint,
                    raw,
                    errors,
                )
            finally:
                runner.ROOT = old_root

            self.assertEqual(committed, 1)
            self.assertEqual(recovered_chain, chain)
            self.assertEqual(raw.read_bytes(), first)
            self.assertEqual(errors.read_bytes(), committed_stderr)
            quarantine = list(
                (temp_root / "out" / "benchmark-interruptions").iterdir()
            )
            self.assertEqual(len(quarantine), 1)
            self.assertEqual((quarantine[0] / "raw-tail.bin").read_bytes(), second)
            self.assertEqual(
                (quarantine[0] / "stderr-tail.bin").read_bytes(), extra_stderr
            )

    def test_corrupt_committed_prefix_is_never_truncated(self) -> None:
        plan = (planned_run(1), planned_run(2))
        first = encode_segment(plan[0])
        second = encode_segment(plan[1])
        campaign = definition(plan)
        bound = bindings(plan)
        chain = runner.initial_chain(
            str(bound["matrix_sha256"]), str(bound["plan_sha256"])
        )
        chain = runner.advance_chain(chain, 1, first)
        checkpoint = runner.checkpoint_value(
            bound,
            campaign,
            1,
            len(first),
            0,
            chain,
            runner.sha256_bytes(b""),
        )

        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "campaign"
            output.mkdir()
            raw = output / "raw.jsonl"
            errors = output / "stderr.log"
            corrupted = bytearray(first + second)
            corrupted[10] ^= 1
            raw.write_bytes(corrupted)
            errors.write_bytes(b"")
            before = raw.read_bytes()
            with self.assertRaises(runner.CampaignError):
                runner.verify_checkpoint_and_recover(
                    output,
                    campaign,
                    bound,
                    checkpoint,
                    raw,
                    errors,
                )
            self.assertEqual(raw.read_bytes(), before)

    def test_checkpoint_recovery_streams_the_committed_raw_file(self) -> None:
        plan = (planned_run(1), planned_run(2))
        segments = (encode_segment(plan[0]), encode_segment(plan[1]))
        campaign = definition(plan)
        bound = bindings(plan)
        chain = runner.initial_chain(
            str(bound["matrix_sha256"]), str(bound["plan_sha256"])
        )
        for planned, segment in zip(plan, segments):
            chain = runner.advance_chain(chain, planned.ordinal, segment)
        raw_bytes = b"".join(segments)
        checkpoint = runner.checkpoint_value(
            bound,
            campaign,
            2,
            len(raw_bytes),
            0,
            chain,
            runner.sha256_bytes(b""),
        )

        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "campaign"
            output.mkdir()
            raw = output / "raw.jsonl"
            errors = output / "stderr.log"
            raw.write_bytes(raw_bytes)
            errors.write_bytes(b"")
            with mock.patch.object(
                runner,
                "read_regular_bytes",
                side_effect=AssertionError("whole-file read is forbidden"),
            ):
                committed, recovered = runner.verify_checkpoint_and_recover(
                    output, campaign, bound, checkpoint, raw, errors
                )
            self.assertEqual(committed, 2)
            self.assertEqual(recovered, chain)

    def test_live_active_child_blocks_resume(self) -> None:
        plan = (planned_run(1),)
        campaign = definition(plan)
        alive, token = runner.process_token(__import__("os").getpid())
        self.assertTrue(alive)

        with tempfile.TemporaryDirectory() as temporary:
            temp_root = Path(temporary)
            output = temp_root / "campaign"
            output.mkdir()
            active = output / ".active-run.json"
            old_root = runner.ROOT
            runner.ROOT = temp_root
            try:
                state, _, _, _ = runner.prepare_transaction(
                    output, active, plan[0], str(temp_root)
                )
                state.update(
                    {
                        "state": "running",
                        "child_pid": __import__("os").getpid(),
                        "child_start_token": token,
                    }
                )
                runner.write_atomic_json(active, state)
                args = type("Args", (), {"acknowledge_ambiguous_run": False})()
                with self.assertRaises(runner.CampaignError):
                    runner.inspect_active_transaction(
                        args,
                        output,
                        campaign,
                        0,
                        active,
                        output / ".failed-run.json",
                    )
            finally:
                runner.ROOT = old_root

    def test_child_worker_cannot_launch_before_gate_publication(self) -> None:
        plan = (planned_run(1),)
        with tempfile.TemporaryDirectory() as temporary:
            temp_root = Path(temporary)
            output = temp_root / "campaign"
            output.mkdir()
            active_path = output / ".active-run.json"
            marker = temp_root / "benchmark-started.txt"
            old_root = runner.ROOT
            old_timeout = runner.CHILD_GATE_TIMEOUT_SECONDS
            runner.ROOT = temp_root
            runner.CHILD_GATE_TIMEOUT_SECONDS = 0.1
            try:
                active, _, _, result = runner.prepare_transaction(
                    output, active_path, plan[0], str(temp_root)
                )
                transaction = runner.require_transaction_directory(
                    output, active["transaction"]
                )
                exit_code = runner.run_child_worker(
                    transaction / "worker.json",
                    Path(sys.executable).resolve(strict=True),
                    ("-c", f"from pathlib import Path; Path({str(marker)!r}).touch()"),
                )
            finally:
                runner.CHILD_GATE_TIMEOUT_SECONDS = old_timeout
                runner.ROOT = old_root
            self.assertEqual(exit_code, 124)
            self.assertFalse(marker.exists())
            self.assertFalse(result.exists())

    def test_execute_transaction_uses_durable_supervisor_handoff(self) -> None:
        base = planned_run(1)
        planned = runner.PlannedRun(
            ordinal=base.ordinal,
            arguments=("-c", "print('supervised benchmark')"),
            description=base.description,
            manifest_fields=base.manifest_fields,
            summary_fields=base.summary_fields,
            operation_fields=base.operation_fields,
            operations=base.operations,
            summary_status=base.summary_status,
        )
        with tempfile.TemporaryDirectory() as temporary:
            temp_root = Path(temporary)
            output = temp_root / "campaign"
            output.mkdir()
            active_path = output / ".active-run.json"
            old_root = runner.ROOT
            runner.ROOT = temp_root
            try:
                runner.execute_transaction(
                    output,
                    active_path,
                    Path(sys.executable).resolve(strict=True),
                    planned,
                    str(temp_root),
                )
                active = json.loads(active_path.read_text(encoding="ascii"))
                transaction = runner.require_transaction_directory(
                    output, active["transaction"]
                )
                result = json.loads(
                    (transaction / "result.json").read_text(encoding="ascii")
                )
                stdout = (transaction / "stdout.jsonl").read_text(encoding="utf-8")
            finally:
                runner.ROOT = old_root
            self.assertEqual(active["state"], "running")
            self.assertIsInstance(active["child_pid"], int)
            self.assertEqual(result["exit_code"], 0)
            self.assertEqual(stdout, "supervised benchmark\n")

    def test_supervisor_isolated_from_parent_interrupt_group(self) -> None:
        options = runner.supervisor_process_options()
        if os.name == "nt":
            self.assertTrue(
                int(options.get("creationflags", 0))
                & subprocess.CREATE_NEW_PROCESS_GROUP
            )
        else:
            process = subprocess.run(
                [sys.executable, "-c", "import os; print(os.getpgrp())"],
                check=True,
                capture_output=True,
                text=True,
                **options,
            )
            self.assertNotEqual(int(process.stdout.strip()), os.getpgrp())

    def test_partial_bootstrap_is_reconstructed(self) -> None:
        plan = (planned_run(1),)
        campaign = definition(plan)
        with tempfile.TemporaryDirectory() as temporary:
            temp_root = Path(temporary)
            output = temp_root / "campaign"
            output.mkdir()
            executable = temp_root / "bench.exe"
            environment = temp_root / "environment.json"
            executable.write_bytes(b"executable")
            environment.write_bytes(b"environment")
            bootstrap = runner.bootstrap_value(
                campaign,
                executable,
                runner.sha256_file(executable),
                runner.sha256_file(environment),
            )
            runner.write_atomic_json(output / ".bootstrap.json", bootstrap)
            runner.write_atomic_bytes(
                output / "matrix.json",
                json.dumps(campaign.matrix).encode("ascii") + b"\n",
            )
            bindings_value, checkpoint, _ = runner.initialize_campaign(
                campaign,
                executable,
                environment,
                output,
                output / "matrix.json",
                output / "raw.jsonl",
                output / "stderr.log",
                output / "environment.json",
                output / ".checkpoint.json",
                output / ".bootstrap.json",
                recovering=True,
            )
            self.assertEqual(checkpoint["committed_runs"], 0)
            self.assertEqual(bindings_value["expected_runs"], 1)
            self.assertFalse((output / ".bootstrap.json").exists())
            self.assertEqual((output / "raw.jsonl").read_bytes(), b"")
            self.assertEqual((output / "environment.json").read_bytes(), b"environment")

    def test_success_result_replays_once_into_checkpoint(self) -> None:
        plan = (planned_run(1),)
        campaign = definition(plan)
        bound = bindings(plan)
        chain = runner.initial_chain(
            str(bound["matrix_sha256"]), str(bound["plan_sha256"])
        )
        with tempfile.TemporaryDirectory() as temporary:
            temp_root = Path(temporary)
            output = temp_root / "campaign"
            output.mkdir()
            raw = output / "raw.jsonl"
            errors = output / "stderr.log"
            checkpoint_path = output / ".checkpoint.json"
            active_path = output / ".active-run.json"
            raw.write_bytes(b"")
            errors.write_bytes(b"")
            old_root = runner.ROOT
            runner.ROOT = temp_root
            try:
                active, stdout, stderr, result = runner.prepare_transaction(
                    output, active_path, plan[0], str(temp_root)
                )
                stdout.write_bytes(encode_segment(plan[0]))
                stderr.write_bytes(b"")
                runner.write_atomic_json(
                    result,
                    {
                        "schema": "detlog-benchmark-child-result/v1",
                        "ordinal": 1,
                        "exit_code": 0,
                    },
                )
                recovered = runner.inspect_active_transaction(
                    SimpleNamespace(acknowledge_ambiguous_run=False),
                    output,
                    campaign,
                    0,
                    active_path,
                    output / ".failed-run.json",
                )
                self.assertIsNotNone(recovered)
                committed, _, checkpoint = runner.commit_transaction(
                    output,
                    campaign,
                    bound,
                    plan[0],
                    recovered or active,
                    raw,
                    errors,
                    checkpoint_path,
                    active_path,
                    output / ".failed-run.json",
                    chain,
                )
            finally:
                runner.ROOT = old_root
            self.assertEqual(committed, 1)
            self.assertEqual(checkpoint["committed_runs"], 1)
            self.assertEqual(raw.read_bytes(), encode_segment(plan[0]))
            self.assertFalse(active_path.exists())

    def test_nonzero_result_and_no_result_are_never_silent_retries(self) -> None:
        plan = (planned_run(1),)
        campaign = definition(plan)
        with tempfile.TemporaryDirectory() as temporary:
            temp_root = Path(temporary)
            output = temp_root / "campaign"
            output.mkdir()
            active_path = output / ".active-run.json"
            failure_path = output / ".failed-run.json"
            old_root = runner.ROOT
            runner.ROOT = temp_root
            try:
                _, _, _, result = runner.prepare_transaction(
                    output, active_path, plan[0], str(temp_root)
                )
                runner.write_atomic_json(
                    result,
                    {
                        "schema": "detlog-benchmark-child-result/v1",
                        "ordinal": 1,
                        "exit_code": -11,
                    },
                )
                with self.assertRaises(runner.CampaignError):
                    runner.inspect_active_transaction(
                        SimpleNamespace(acknowledge_ambiguous_run=False),
                        output,
                        campaign,
                        0,
                        active_path,
                        failure_path,
                    )
                self.assertEqual(
                    json.loads(failure_path.read_text(encoding="ascii"))["exit_code"],
                    -11,
                )

                failure_path.unlink()
                transaction = runner.require_transaction_directory(
                    output,
                    json.loads(active_path.read_text(encoding="ascii"))["transaction"],
                )
                (transaction / "result.json").unlink()
                with self.assertRaises(runner.CampaignError):
                    runner.inspect_active_transaction(
                        SimpleNamespace(acknowledge_ambiguous_run=False),
                        output,
                        campaign,
                        0,
                        active_path,
                        failure_path,
                    )
                recovered = runner.inspect_active_transaction(
                    SimpleNamespace(acknowledge_ambiguous_run=True),
                    output,
                    campaign,
                    0,
                    active_path,
                    failure_path,
                )
                self.assertIsNone(recovered)
                self.assertFalse(active_path.exists())
            finally:
                runner.ROOT = old_root

    def test_completion_marker_can_be_reverified_without_checkpoint(self) -> None:
        plan = (planned_run(1),)
        campaign = definition(plan)
        bound = bindings(plan)
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "campaign"
            output.mkdir()
            for name in (
                "matrix.json",
                "raw.jsonl",
                "stderr.log",
                "environment.json",
                "summary.csv",
                "throughput.svg",
            ):
                (output / name).write_bytes(name.encode("ascii"))
            checkpoint = {
                "committed_runs": 1,
                "segment_chain_sha256": "2" * 64,
            }
            marker = output / "campaign-complete.json"
            runner.write_atomic_json(
                marker,
                runner.completion_value(campaign, bound, checkpoint, output),
            )
            with mock.patch.object(runner.subprocess, "run") as invoked:
                runner.verify_completion(campaign, bound, output, marker)
            invoked.assert_called_once()

    def test_packager_consumes_only_a_matching_completion_marker(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary)
            artifacts = {
                "matrix.json": json.dumps(
                    {"schema": "detlog-bench-matrix/v1"}
                ).encode("ascii"),
                "raw.jsonl": b"record\n",
                "stderr.log": b"",
                "summary.csv": b"summary\n",
                "throughput.svg": b"<svg/>",
                "environment.json": b"{}",
            }
            for name, value in artifacts.items():
                (output / name).write_bytes(value)
            marker = output / "campaign-complete.json"
            inventory = {
                "schema": "detlog-benchmark-campaign-complete/v1",
                "files": {
                    name: {
                        "bytes": (output / name).stat().st_size,
                        "sha256": packager.sha256(output / name),
                    }
                    for name in artifacts
                },
            }
            marker.write_text(json.dumps(inventory), encoding="ascii")
            packager.consume_completion_marker(output)
            self.assertFalse(marker.exists())

            marker.write_text(json.dumps(inventory), encoding="ascii")
            (output / "raw.jsonl").write_bytes(b"changed\n")
            with self.assertRaises(ValueError):
                packager.consume_completion_marker(output)
            self.assertTrue(marker.exists())


if __name__ == "__main__":
    unittest.main()
