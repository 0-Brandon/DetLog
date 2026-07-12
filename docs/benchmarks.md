# Benchmark and reproduction workflow

The benchmark harness measures DetLog against itself under controlled workload
and fault configurations. It is not a production-database comparison and its
numbers must not be presented as such.

Every run writes newline-delimited JSON to stdout. Redirect that stream to a
new file and retain it as the raw result; diagnostics go to stderr.

## Modes and supported scenarios

| Mode | healthy | leader-crash | partition | slow-follower | slow-fsync |
|---|---:|---:|---:|---:|---:|
| `sim` | yes | yes | yes | yes | yes |
| `tcp` | yes | yes | yes | unsupported | unsupported |

`sim` uses `DeterministicCluster`, virtual time, the production codec/WAL byte
format, and deterministic fault controls. `tcp` creates multiple `NodeHost`
objects in one benchmark process, but each node has a distinct temporary WAL
and communicates through real loopback TCP sockets. This avoids process-launch
noise while measuring the real runtime adapter. The `detlog-node` demo remains
the separate-process demonstration.

TCP slow-peer and slow-fsync runs are deliberately rejected with exit code 2
and an `unsupported` JSON record. The harness does not approximate controls it
does not possess.

Scenario behavior:

- `leader-crash` stops the ready leader while a closed-loop wave is in flight,
  retries the ambiguous commands with the same request IDs, and measures from
  fault injection to the first later success.
- `partition` isolates the simulated leader in both directions for 150 virtual
  ticks and then heals it. TCP mode uses a real bidirectional transport cut for
  `--partition-ms` wall-clock milliseconds (default 1000), records its measured
  duration, and keeps the run alive through the complete interval. Both modes
  require replacement election and a later successful commit while the cut is
  still active; otherwise the run exits nonzero.
- `slow-follower` adds deterministic bidirectional delay to one simulated
  follower while keeping election bounds large enough to avoid turning the
  scenario into continuous elections.
- `slow-fsync` raises simulated flush latency and records the setting in the
  run configuration.

## Run one configuration

Build benchmarks with optimization for measurement. Debug builds are useful
for correctness smoke tests but should not be published as performance data.

```sh
cmake --preset release
cmake --build --preset release --target detlog-bench detlog-wal-bench
```

```sh
build/release/detlog-bench \
  --mode sim \
  --nodes 3 \
  --clients 3 \
  --payload 1024 \
  --operations 1000 \
  --trial 1 \
  --seed 1001 \
  --scenario leader-crash \
  > bench-results/sim-crash-trial-1.jsonl
```

PowerShell uses the same arguments:

```powershell
build\release\detlog-bench.exe --mode tcp --nodes 3 --clients 3 `
  --payload 1024 --operations 1000 --trial 1 --seed 1001 `
  --scenario healthy > bench-results\tcp-healthy-trial-1.jsonl
```

Valid node counts are 3 and 5; client counts are 1, 3, and 5; payload values
are 64, 1024, and 16384 bytes. The payload is the command value size, so the
encoded command also includes its fixed fields and key.
Leader-crash and partition runs require more operations than clients so the
fault is guaranteed to interrupt a live closed-loop wave. A supported fault
run exits nonzero unless injection, replacement election, and a later success
were all observed; a partition must also complete its full heal interval.

Each client owns one session and has at most one unresolved logical command.
Its sequence advances only after an `ok` reply. A timeout, leader failure,
`busy`, or `not_leader` result retries the same command and request ID. This is
a closed-loop workload; it does not hide overload behind an unbounded producer
queue.

TCP accepts `--fsync-policy flush-every|group`. Safe group mode additionally
uses `--group-size` and `--group-delay-ms` to bound staged WAL frames and the
maximum time before their shared durability barrier. The default remains
flush-every. Simulator storage is a separate virtual model and rejects runtime
group mode.

## JSONL schema

Each run contains:

1. A `manifest` record with OS, compiler, C++ level, hardware concurrency,
   mode, scenario, seed/trial, workload dimensions, fsync/group bounds, timed
   partition configuration, and whether TCP nodes share a process. Set
   `DETLOG_BUILD_COMMIT` and `DETLOG_BUILD_FLAGS` in the
   environment to embed the exact revision and compiler flags; otherwise the
   manifest records those fields as `not_provided` rather than guessing.
2. One `operation` record per requested operation with client/session sequence,
   start/end, end-to-end latency, attempts, retries, queue rejections, and final
   status.
3. A `summary` record with successes/failures, p50/p95/p99, elections, aggregate
   retries/backpressure, throughput, coarse process CPU time, process peak
   resident memory, queue high-water marks, the measured `fault_duration`, and
   two separate recovery durations where a fault was injected:
   `replacement_leader_ready_duration` stops when a ready leader with a
   different node ID is observed, while `recovery_to_first_success` stops only
   after a later command has committed and replied successfully.

Simulator latency and duration use `virtual_ticks`, and throughput is
`ops_per_virtual_tick`. TCP uses nanoseconds and `ops_per_second`. These units
are intentionally distinct and must not be plotted on one numeric axis or
compared as if they represented the same clock.

Only the initial ready-leader election is warmed before timing; there are zero
excluded workload operations, so caches, WAL growth, and state-machine work are
cold at the first measured command. The manifest labels this policy and records
the simulator event bound or TCP wall-clock deadline that ends an incomplete
measurement. The TCP deadline is `30 s + 500 ms * operations`, capped at ten
minutes; this remains finite while accommodating sustained flush-per-command
storage rather than treating a slow but progressing durable run as failed.
Election counts still include the observed initial leader and any
replacements. Simulator runs make one final invariant check. TCP summaries
point to the runtime integration tests rather than claiming an online
independent safety oracle.

`process_cpu_ns` comes from standard C++ `std::clock()` over the timed workload
only. It is intentionally labelled `coarse_process_wide_timed_workload`: in TCP
mode it includes every in-process node and worker thread, and in simulator mode
it includes the simulator and benchmark adapter. Clock resolution and
multi-thread accounting are implementation-dependent, so this field is useful
for within-platform trials, not cross-platform normalization. It is not a
per-node CPU measurement.

`peak_resident_bytes` uses the process peak working set on Windows and
`getrusage` peak RSS on Linux/macOS. It covers the process lifetime, including
warmup, and includes all in-process TCP nodes; it is neither a timed-window
delta nor a per-node measurement. Unsupported platforms emit `null`.

TCP mode reports exact `owner_queue_high_water` and
`client_queue_high_water` gauges maintained by each host and takes the maximum
across nodes. Simulator mode reports the maximum scheduled-event count,
network bytes, and pending-storage bytes observed at deterministic event
boundaries. Queue families that do not apply to a mode are zero; rejection and
backpressure counters remain separate from depths.

`replacement_leader_ready_duration` is emitted for leader-crash and partition
runs in both modes and is `null` if no different ready leader was observed
before the bounded run ended. `recovery_to_first_success` remains separate and
normally includes election plus replication, WAL durability, application, and
reply delivery. Healthy and slow-path scenarios emit both fault fields as
`null`.

All loops have finite event/deadline and retry bounds. A run that cannot finish
emits `timeout`/`not_started` operation records, a `bounded_timeout` summary,
and a nonzero exit code while preserving everything already written.

## Reproduction matrices

The main matrix scripts run all documented supported scenario combinations
with flush-every runtime storage. With their defaults (three trials and 1000
operations), they run 270 simulator trials and 162 TCP trials. Use smaller
values for a smoke campaign, but keep operations above five because the fault
must interrupt a live wave even in the five-client cells.

Linux/macOS:

```sh
scripts/run_bench_matrix.sh build/release/detlog-bench \
  bench-results/smoke 1 20
```

PowerShell:

```powershell
scripts\run_bench_matrix.ps1 `
  -Executable build\release\detlog-bench.exe `
  -OutputDirectory bench-results\smoke `
  -Repetitions 1 -Operations 20
```

Each output directory contains:

- `raw.jsonl`: concatenated, unchanged benchmark records;
- `stderr.log`: diagnostics from every run;
- `matrix.json`: the exact requested matrix and unsupported TCP scenarios;
- `summary.csv`: one row per summary;
- `throughput.svg`: trial-mean throughput bars with min-max whiskers, separated
  by unit.

The scripts stop on the first failed supported run but retain all raw output
written before that point.

## End-to-end runtime fsync matrix

The runtime fsync runners compare actual `NodeHost` durability policies over
real loopback TCP and distinct file-backed WALs. Defaults cover healthy and
leader-crash histories, 3/5 nodes, 1/3/5 clients, all payload sizes,
flush-every, group-of-2, group-of-5, and three trials (324 runs):

```sh
scripts/run_runtime_fsync_matrix.sh build/release/detlog-bench \
  bench-results/runtime-fsync 3 1000
```

```powershell
scripts\run_runtime_fsync_matrix.ps1 `
  -Executable build\release\detlog-bench.exe `
  -OutputDirectory bench-results\runtime-fsync `
  -Repetitions 3 -Operations 1000 -GroupSizes @(2,5)
```

The manifest and chart labels retain policy, group bound, and group delay so
results with different durability contracts cannot be silently aggregated.

## WAL durability and recovery scaling

`detlog-wal-bench` is a separate storage experiment. It measures append wall
and process-CPU time, resulting file bytes, and the time to reopen and scan the
complete WAL as entry count and payload size grow. It validates the recovered
entry count and commit index before emitting a successful summary.

Reopen happens immediately in the same process after close, so
`reopen_scan_wall_ns` is a warm-page-cache scanner/startup measurement. A
cold-cache recovery claim requires privileged, OS-specific cache control and is
outside this portable harness.

The policies are deliberately precise:

- `flush-every` establishes a durability barrier for every frame;
- `group --group-size N` writes `N` independently recoverable frames and
  shares one flush across the group;
- `unsafe-no-flush` returns nondurable append results, is labelled unsafe in
  every record, and performs a separately timed explicit final flush solely so
  recovery scanning has a durable corpus. Its append numbers must never support
  a crash-safety claim.

Run one bounded recovery experiment:

```sh
build/release/detlog-wal-bench --entries 1000 --payload 1024 --trial 1 \
  --policy group --group-size 32 > wal-run.jsonl
```

Run repeated size/policy matrices:

```sh
scripts/run_wal_bench_matrix.sh build/release/detlog-wal-bench \
  bench-results/wal-smoke 3
```

```powershell
scripts\run_wal_bench_matrix.ps1 `
  -Executable build\release\detlog-wal-bench.exe `
  -OutputDirectory bench-results\wal-smoke -Repetitions 3
```

The runners preserve `raw-includes-nondurable.jsonl`, `stderr.log`, a matrix
manifest, `summary-includes-nondurable.csv`, and
`wal-figures-includes-nondurable.svg`. The raw stream and derived-output names
explicitly warn that safe results share those artifacts with the unsafe
policy; every raw record and the matrix manifest carry the precise durability
label or policy. The WAL
derivation tool validates manifest/summary pairs, keeps builds and source files
separate, and plots mean values with min-max trial whiskers. The WAL manifest
also records platform/compiler/build provenance. This remains a storage
microbenchmark; use the runtime fsync matrix above for end-to-end group-commit
claims.

## Plot cluster benchmark raw data

The plotting tool uses only the Python standard library:

```sh
python3 scripts/plot_bench.py bench-results/run-a/raw.jsonl \
  bench-results/run-b/raw.jsonl \
  --csv bench-results/combined.csv \
  --svg bench-results/combined.svg
```

This tool accepts `detlog-bench` simulator/TCP records. The WAL matrix runner
uses its dedicated WAL derivation tool and nondurable-labelled output names;
do not feed WAL records to `plot_bench.py`.

The CSV retains every summary, including failures, plus provenance copied from
the preceding run manifest. SVG aggregation includes only `complete` runs with
zero failures. Trials are aggregated only within one input file; separate raw
files are never silently averaged across machines or experiments, even when
their build metadata matches.

## Environment capture, validation, and packaging

Capture the machine while the code commit is clean and before writing tracked
evidence. On Windows, write the record under ignored `out` and supply the exact
benchmark volume/storage description plus background-load notes:

```powershell
scripts\capture_benchmark_environment.ps1 `
  -Output out\benchmark-environment.json `
  -BuildFlags $env:DETLOG_BUILD_FLAGS `
  -StorageDescription "D: Windows Storage Space (underlying devices listed)" `
  -Notes "Balanced power plan; ordinary desktop background services"
```

Unix-like hosts use the matching schema and environment variables:

```sh
DETLOG_STORAGE_DESCRIPTION='model, filesystem, and mount' \
DETLOG_BENCH_NOTES='power policy and background load' \
scripts/capture_benchmark_environment.sh out/benchmark-environment.json
```

Every matrix runner refuses a nonempty output directory, streams each child
record directly to the raw file, records `expected_runs`, and invokes the exact
cardinality/pair validator before plotting. Revalidate and losslessly package
completed directories with deterministic gzip plus per-directory checksums:

```sh
python3 scripts/validate_benchmark_artifacts.py bench-results/cluster \
  bench-results/runtime-fsync bench-results/wal
python3 scripts/package_benchmark_artifacts.py --remove-raw \
  bench-results/cluster bench-results/runtime-fsync bench-results/wal
```

The validator accepts either the original `raw*.jsonl` stream or its
deterministic `raw*.jsonl.gz` packaged form, so the published package can be
checked directly without an extraction step. A WAL matrix's `raw_jsonl` field
names the logical uncompressed stream; `.gz` is its lossless packaged encoding.
Packaging an uncompressed stream intentionally requires `--remove-raw`, which
prevents an ambiguous package from retaining both representations.

Publish the matrix manifest, losslessly compressed raw JSONL, captured
environment, derived CSV/SVG, diagnostics, and `SHA256SUMS` together. Repeated
trials and reported variability matter more than a single best result.
