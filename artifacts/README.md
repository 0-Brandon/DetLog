# Representative generated evidence

These files are a small reproducibility corpus, not a performance report. They
were generated on 2026-07-11 on Windows with MinGW g++ 11.3, C++20, `-O2`, the
project's strict warning set, and an uncommitted worktree with no Git `HEAD`.
The benchmark manifests preserve that limitation as
`uncommitted-worktree-no-head`.
`SHA256SUMS` covers both traces and the two raw benchmark streams.

## Traces

The trace files capture the built-in leader-crash/replacement/restart scenario:

```powershell
detlog-sim.exe --seed 42 --nodes 3 `
  --trace artifacts\traces\leader-crash-restart-3node-seed42.jsonl
detlog-sim.exe --seed 84 --nodes 5 `
  --trace artifacts\traces\leader-crash-restart-5node-seed84.jsonl
```

They use `cluster_config(nodes, seed)` defaults. Replay identity is the same
executable/build, full defaults, action sequence, and seed; byte identity is not
promised across source, compiler, or standard-library changes.

## Cluster benchmark smoke matrix

`benchmarks/cluster-smoke` contains all supported simulator/TCP workload cells
with one trial and two operations per cell:

```powershell
scripts\run_bench_matrix.ps1 -Executable .\detlog-bench.exe `
  -OutputDirectory .\artifacts\benchmarks\cluster-smoke `
  -Repetitions 1 -Operations 2 -Python python
```

This is enough to exercise every harness path and generate raw JSONL, CSV, and
SVG, but one tiny trial cannot support throughput or variability conclusions.

## WAL benchmark smoke matrix

`benchmarks/wal-smoke` covers 10/100 entries, 64/1024-byte payloads,
flush-every/group-of-four/unsafe-no-flush, and one trial:

```powershell
scripts\run_wal_bench_matrix.ps1 -Executable .\detlog-wal-bench.exe `
  -OutputDirectory .\artifacts\benchmarks\wal-smoke -Repetitions 1 `
  -EntrySizes @(10,100) -PayloadBytes @(64,1024) -GroupSizes @(4) `
  -Python python
```

Files containing the unsafe policy say `includes-nondurable` in their names.
The unsafe append result is not crash-safe; the harness performs a separate
final flush only before reopening. Reopen timings use a warm page cache.
