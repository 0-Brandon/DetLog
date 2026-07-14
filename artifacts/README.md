# Published evidence corpus

This directory contains the representative deterministic traces and reportable
benchmark artifacts used by the project documentation. See
[`docs/benchmark-results.md`](../docs/benchmark-results.md) for analysis and
interpretation.

`SHA256SUMS` covers every published trace and benchmark file except itself,
including the per-campaign checksum manifests. Verify it from the repository
root with:

```powershell
Get-Content artifacts\SHA256SUMS | ForEach-Object {
  $hash, $path = $_ -split '  ', 2
  if ((Get-FileHash -Algorithm SHA256 $path).Hash.ToLowerInvariant() -ne $hash) {
    throw "checksum mismatch: $path"
  }
}
```

## Deterministic traces

[`traces/manifest.json`](traces/manifest.json) inventories nine seed-based
representative failures generated from build
`0a21224f245a2526b03140071eab6ce076add91a`:

| Scenario | Nodes | Seed |
|---|---:|---:|
| Leader crash/replacement/restart | 3 | 42 |
| Leader crash/replacement/restart | 5 | 84 |
| Symmetric partition | 3 | 43 |
| Asymmetric partition | 5 | 44 |
| Ambiguous client retry | 3 | 45 |
| Torn WAL tail | 3 | 46 |
| Slow follower | 5 | 47 |
| Slow disk | 3 | 48 |
| Queue saturation/backpressure | 3 | 49 |

The manifest records each filename and SHA-256 digest. Replay identity requires
the same executable/build, seed, cluster defaults, and action sequence. Byte
identity is not promised across compiler or standard-library changes.

## Reportable benchmarks

All three benchmark packages were produced from clean build commit
`25638317094f5a451d7715e301b375e37d87bbb6` with GCC 11.3.0, C++20,
`-O3 -DNDEBUG`, and the strict warning set. Their `environment.json` files
record the complete machine, OS, compiler, flags, storage, power plan, build,
and temporary-directory provenance.

| Directory | Coverage | Published data |
|---|---|---|
| [`benchmarks/cluster`](benchmarks/cluster) | 432 simulator/TCP runs; 200 operations each; three trials | matrix, environment, compressed raw JSONL, CSV, SVG, checksums |
| [`benchmarks/runtime-fsync`](benchmarks/runtime-fsync) | 324 loopback TCP runs comparing flush-every and groups 2/5; three trials | matrix, environment, compressed raw JSONL, CSV, SVG, checksums |
| [`benchmarks/wal`](benchmarks/wal) | 108 WAL append/reopen runs over 100/1,000/5,000 entries and 64 B/1 KiB/8 KiB payloads; three trials | matrix, environment, compressed raw JSONL, CSV, SVG, checksums |

The cluster and runtime raw streams contain a manifest, 200 operation records,
and a summary for every run. The WAL stream contains paired manifest/summary
records. Derived CSV/SVG files can be regenerated with the scripts documented
in [`docs/benchmarks.md`](../docs/benchmarks.md).

Files containing `includes-nondurable` deliberately include the WAL
`unsafe-no-flush` policy. Its append acknowledgements are not crash-safe; the
harness performs and separately times a final flush only before reopening.
Reopen timings use a warm page cache.

Validate the packaged artifacts with:

```powershell
python -B scripts\validate_benchmark_artifacts.py `
  artifacts\benchmarks\cluster `
  artifacts\benchmarks\runtime-fsync `
  artifacts\benchmarks\wal
```
