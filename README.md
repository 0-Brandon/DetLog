# DetLog

DetLog is a C++20 replicated command log with a deterministic failure
simulator. It is intentionally a small, inspectable implementation of the
Raft safety protocol rather than a production database.

The project focuses on four things:

- explicit consensus and durability invariants;
- reproducible network, storage, and crash schedules;
- checksummed write-ahead-log recovery;
- evidence from invariant tests, traces, and failure/recovery measurements.

See [the architecture](docs/architecture.md), [protocol](docs/protocol.md),
[invariant protection map](docs/invariants.md), and
[deterministic fault semantics](docs/fault-semantics.md), plus the
[testing/experiment contract](docs/testing.md), before changing safety-critical
paths.

The executable paths below assume the `debug` preset. On Windows, append
`.exe`.

## Supported contract

DetLog uses a static three- or five-node Raft cluster. A successful command has
been stored durably by a majority, committed under Raft's current-term rule,
recorded in the leader's local commit watermark, and applied in order. Client
sessions use monotonically increasing sequence numbers so an ambiguous retry
does not execute the command twice.

The real runtime defaults to one durability barrier per WAL frame and also
supports bounded safe group commit. Group mode quarantines every dependent
effect until its shared flush succeeds; explicit no-flush remains unsafe and
is used only by clearly labelled storage experiments.

The deterministic environment uses integer virtual time and a pinned PRNG.
The exact replay key is the executable/build, full scenario configuration,
external action list, and seed. A lagging node may restart from its latest
intact WAL; rollback of storage that was already reported durable is outside
the fault model.

## Build

The reference build uses CMake 3.24+, Ninja, and a C++20 compiler:

```sh
cmake --preset debug
cmake --build --preset debug
ctest --preset debug
```

Sanitizer presets are separate:

```sh
cmake --preset asan-ubsan
cmake --build --preset asan-ubsan
ctest --preset asan-ubsan

cmake --preset tsan
cmake --build --preset tsan
ctest --preset tsan
```

Linux/Clang is the authoritative sanitizer, fuzzing, and durability-test
environment. The platform-independent core, simulator, codec, and WAL format
also build on Windows. No external library is required for the core test suite.

Without CMake, the standalone verification scripts compile every test directly:

```sh
scripts/verify.sh
```

```powershell
scripts\verify.ps1 -Compiler C:\msys64\mingw64\bin\g++.exe
```

## Run the demonstrations

Run a deterministic leader-crash/restart history and retain its JSONL trace:

```sh
build/debug/detlog-sim --seed 42 --nodes 3 --trace trace.jsonl
```

Use `--scenario` for leader crash, symmetric/asymmetric partition, ambiguous
retry, torn WAL, slow follower, per-node slow disk, or saturation histories.
The complete representative corpus can be regenerated with
`scripts/generate_trace_corpus.sh` or `scripts\generate_trace_corpus.ps1`.

`detlog-node` hosts one file-backed node. The
[real TCP walkthrough](docs/tcp-demo.md) gives complete three-process commands,
including restart from the same WAL.

The ordinary property test runs 50 pinned seeds. Increase the campaign without
changing the executable:

```sh
DETLOG_PROPERTY_SEEDS=2000 build/debug/property_tests
```

Benchmark methodology, supported fault controls, JSONL fields, matrix runners,
and figure generation are documented in [docs/benchmarks.md](docs/benchmarks.md).
The [reportable results](docs/benchmark-results.md) analyze the complete
three-trial cluster, runtime-fsync, and WAL campaigns. The
[published evidence corpus](artifacts/README.md) contains nine representative
failure traces plus compressed raw data, CSV/SVG outputs, environment captures,
and checksums for all three campaigns.
The [stale-candidate liveness postmortem](docs/postmortems/001-stale-candidate-election-deadlock.md)
records one bug found by the adversarial histories and the regression that now
protects it.

## Repository map

```text
include/detlog/    Public types and subsystem interfaces
src/               Consensus, reference model, state machine, storage, runtime
apps/              Demonstration executables
tests/             Unit and deterministic scenario tests
fuzz/              Engine-independent parser fuzz entry points
bench/             Raw-result benchmark harness
docs/              Architecture, protocol, invariants, and experiment rules
scripts/           Reproduction helpers
```

## Non-goals

There is no dynamic membership, snapshotting/compaction, Byzantine tolerance,
authentication, lease/read-index optimization, or claim of production
readiness. Unsafe no-flush experiments are labelled nondurable and are not
used to support crash-safety claims.
