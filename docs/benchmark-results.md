# Reportable benchmark results

These results complete the benchmark campaign described in
[the benchmark workflow](benchmarks.md). They measure DetLog against itself;
they are not comparisons with a production database.

## Scope and provenance

The reportable build is commit
`25638317094f5a451d7715e301b375e37d87bbb6`, compiled with GCC 11.3.0 as
C++20 using `-O3 -DNDEBUG` and the project's strict warning flags. The recorded
host is a single local Windows workstation with an Intel Core i5-13500, 20
logical processors, 34,083,512,320 bytes of physical memory, the Balanced power
plan, and benchmark files on a `D:` Windows Storage Space. The captured OS
fields say Windows 10 Pro, 23H2, build 22631.3374; the product label and build
number are preserved as recorded even though that combination is unusual.

Campaigns ran sequentially, with `TEMP` and `TMP` pinned to the recorded
benchmark directory. Long campaigns were split into resumable, hash-checked
chunks; chunking did not change the executable, matrix, seed sequence, or raw
result stream.

| Campaign | Runs | Work per run | Raw records | Repetitions |
|---|---:|---:|---:|---:|
| Cluster simulator/TCP | 432 | 200 operations | 87,264 | 3 |
| Runtime fsync comparison | 324 | 200 operations | 65,448 | 3 |
| WAL append/reopen | 108 | 100, 1,000, or 5,000 entries | 216 | 3 |

Every one of the 151,200 cluster/runtime operations completed successfully;
there were no failed operations, queue rejections, or storage errors. All 108
WAL runs reported `ok`, and every reopen recovered the requested entry count
and commit index. These checks establish corpus integrity, not statistical
confidence: three trials support descriptive mean and min/max reporting, not
confidence intervals.

Published inputs and outputs are in
[`artifacts/benchmarks`](../artifacts/benchmarks). The CSV files are derived
views; the compressed JSONL, matrix, environment capture, and checksums are the
authoritative reproducibility record.

## Cluster campaign

The cluster matrix covers three and five nodes; one, three, and five clients;
64-byte, 1 KiB, and 16 KiB values; five simulator scenarios; and three TCP
scenarios. Simulator figures use virtual ticks. TCP figures use wall-clock
nanoseconds and real loopback sockets with distinct WALs, while all nodes share
one process. Simulator and TCP values therefore must not be numerically
compared with each other.

The following is one representative cell: three nodes, three clients, a 1 KiB
value, and 200 operations. Each value is the arithmetic mean with the observed
three-trial range in brackets.

| Mode | Scenario | Throughput | P99 latency | Replacement ready | First success | Partition held |
|---|---|---:|---:|---:|---:|---:|
| Simulator | healthy | 0.249066 [0.249066–0.249066] ops/tick | 12.0 [12–12] ticks | — | — | — |
| Simulator | leader crash | 0.242327 [0.242131–0.242718] ops/tick | 34.3 [33–35] ticks | 22.3 [21–23] ticks | 34.3 [33–35] ticks | — |
| Simulator | partition | 0.236102 [0.231214–0.240964] ops/tick | 53.7 [35–70] ticks | 41.7 [23–58] ticks | 53.7 [35–70] ticks | 150 ticks |
| Simulator | slow follower | 0.249066 [0.249066–0.249066] ops/tick | 12.0 [12–12] ticks | — | — | — |
| Simulator | slow fsync | 0.027976 [0.027976–0.027976] ops/tick | 107 [107–107] ticks | — | — | — |
| TCP | healthy | 8.858 [8.637–9.003] ops/s | 512.7 [498.5–529.3] ms | — | — | — |
| TCP | leader crash | 9.558 [9.111–9.914] ops/s | 1,222.1 [917.8–1,624.2] ms | 972.2 [654.4–1,405.6] ms | 1,086.5 [888.7–1,453.1] ms | — |
| TCP | partition | 8.766 [8.516–8.930] ops/s | 1,068.3 [839.6–1,305.1] ms | 731.3 [590.0–1,011.0] ms | 923.9 [839.6–1,073.4] ms | 1,012.3 [1,011.0–1,014.3] ms |

Across the full matrix:

- healthy TCP configuration means ranged from 3.841 to 13.340 ops/s, with
  mean per-run P99 values from 331.5 to 895.0 ms;
- every TCP leader-crash cell had a higher mean P99 than its matched healthy
  cell (1.017× to 2.384×), while its throughput ratio varied from 0.814× to
  1.124×;
- every TCP partition cell had lower mean throughput than its matched healthy
  cell (0.686× to 0.996×), and its P99 was 1.492× to 6.374× higher;
- simulated slow fsync produced only 3.54% to 14.93% of matched healthy
  throughput, with P99 6.7× to 122.6× higher; and
- the simulated slow-follower cells matched healthy cells because the
  available majority did not depend on the delayed follower. That is a result
  of this topology and deterministic delay model, not a general claim about
  slow replicas.

`replacement_leader_ready_duration` ends when a different ready leader is
observed. `recovery_to_first_success` additionally includes replication, WAL
durability, application, and the client reply. TCP leader-crash configuration
means ranged from 612.9 to 972.2 ms for replacement readiness and 897.8 to
1,124.7 ms for first success. TCP partition means ranged from 570.3 to 782.1 ms
and 883.6 to 1,110.7 ms respectively; the measured one-second cuts ranged from
1,001.6 to 1,020.7 ms across individual trials.

For the résumé-bullet hypothesis, the explicitly pooled nearest-rank P99 of
`recovery_to_first_success` across all 108 TCP crash/partition trials (108
distinct seeds) is **1,453.1061 ms**; the maximum is 1,453.7553 ms. Separately,
the 54-sample leader-crash and partition P99s are 1,453.7553 ms and
1,183.5641 ms. With only 54 samples, nearest-rank P99 is the maximum, so these
are evidence labels rather than robust tail estimates.

An evidence-backed rendering of the specification's résumé hypothesis is:

> Built a C++20 replicated log with leader election, durable WAL recovery,
> idempotent client retries, and deterministic network/crash simulation;
> validated safety across 2,000 seeded randomized histories and measured a
> 1.453 s nearest-rank pooled P99 time to resume successful commits across 108
> TCP leader-crash and partition trials.

## Runtime fsync comparison

The runtime matrix keeps the real loopback TCP path and distinct WAL files,
then compares flush-every with group sizes two and five at a fixed 2 ms group
delay. Closed-loop clients permit one unresolved operation per client, so
batching opportunity depends directly on client count.

Matched against flush-every across otherwise identical workloads:

Each throughput column below aggregates 12 matched ratios of three-trial cell
means for that client count.

| Policy | Clients | Throughput geometric ratio | Median throughput ratio | Flush-count geometric ratio | Mean observed group maximum |
|---|---:|---:|---:|---:|---:|
| Group 2 | 1 | 0.917 | 0.916 | 1.009 | 1.000 |
| Group 5 | 1 | 0.919 | 0.922 | 1.005 | 1.000 |
| Group 2 | 3 | 1.020 | 1.018 | 0.916 | 2.000 |
| Group 5 | 3 | 1.010 | 0.996 | 0.926 | 2.278 |
| Group 2 | 5 | 1.134 | 1.103 | 0.817 | 2.000 |
| Group 5 | 5 | 1.148 | 1.118 | 0.816 | 3.056 |

Thus grouping was about 8% slower with one client and no actual batching, but
delivered 13.4% to 14.8% higher geometric-mean throughput with five clients
while using about 18% fewer flushes. Group size five never filled; the largest
observed group was four.

The following 5-node, 5-client, 1 KiB cells show both the gain under healthy
load and the recovery tradeoff. Values are three-trial mean [min–max].

| Scenario | Policy | Throughput ops/s | P99 ms | Replacement ready ms | First success ms |
|---|---|---:|---:|---:|---:|
| Healthy | Flush every | 10.131 [9.115–10.858] | 1,117.2 [625.9–2,087.4] | — | — |
| Healthy | Group 2 | 12.513 [12.389–12.708] | 543.1 [497.7–590.7] | — | — |
| Healthy | Group 5 | 12.749 [11.758–13.522] | 520.4 [454.1–563.4] | — | — |
| Leader crash | Flush every | 10.876 [10.605–11.369] | 1,177.0 [1,118.9–1,245.1] | 761.9 [686.4–871.3] | 1,026.7 [871.4–1,121.1] |
| Leader crash | Group 2 | 11.375 [11.231–11.643] | 1,599.7 [1,366.9–2,019.9] | 960.9 [688.5–1,446.2] | 1,401.0 [1,142.1–1,818.8] |
| Leader crash | Group 5 | 11.835 [11.541–11.993] | 1,270.5 [1,217.2–1,348.6] | 877.3 [747.4–964.9] | 1,076.0 [998.9–1,169.5] |

Across leader-crash workloads, grouped policies did not show a recovery win:
median first-success recovery was 9.4% slower for group two and 10.7% slower
for group five. Only three trials per cell and election/storage noise prevent a
causal claim, but the campaign does show that healthy batching gains cannot be
assumed to improve failure latency.

## WAL append and reopen

The WAL campaign compares durable flush-every, durable groups of eight and 32,
and explicitly nondurable append acknowledgements. Throughput is entries per
second. Each durable value is a three-trial mean [min–max].

| Entries | Payload | Flush every | Group 8 | Group 32 |
|---:|---:|---:|---:|---:|
| 100 | 64 B | 31.6 [31.1–32.0] | 233.8 [226.5–243.4] | 715.6 [665.6–769.5] |
| 100 | 1 KiB | 29.9 [26.2–32.2] | 222.3 [203.5–231.9] | 624.3 [588.2–662.0] |
| 100 | 8 KiB | 31.4 [31.1–31.8] | 182.8 [148.9–207.6] | 530.2 [416.7–588.0] |
| 1,000 | 64 B | 29.1 [28.8–29.6] | 232.8 [223.5–238.0] | 903.8 [874.3–928.6] |
| 1,000 | 1 KiB | 30.2 [30.1–30.4] | 232.0 [227.7–236.6] | 829.7 [810.3–849.9] |
| 1,000 | 8 KiB | 30.6 [30.5–30.8] | 240.8 [234.8–249.2] | 774.5 [766.7–778.5] |
| 5,000 | 64 B | 29.7 [29.5–30.0] | 237.4 [236.3–238.8] | 899.2 [790.7–961.5] |
| 5,000 | 1 KiB | 30.9 [30.6–31.1] | 243.5 [242.0–244.6] | 931.5 [859.2–969.5] |
| 5,000 | 8 KiB | 31.2 [31.1–31.3] | 238.8 [234.9–245.1] | 795.2 [781.1–816.9] |

Across cells, group eight delivered 5.81× to 7.99× the flush-every throughput;
group 32 delivered 16.87× to 31.02×. These are entries/frame throughput
ratios: each group shares one durability barrier.

The unsafe policy's nine cell means ranged from 10,731 to 82,133 entries/s;
individual trials ranged from 8,285.897 to 95,274.390 entries/s. Its append
acknowledgements are not crash-safe and its throughput excludes the separately
timed final flush. For the largest 5,000×8 KiB cell, unsafe append throughput
was 10,731 [10,417–11,052] entries/s and the required final flush took 260.4
[126.9–359.1] ms. It must not be presented as a durable alternative.

Reopen scanning occurred immediately after close in the same process. The
following ranges span the four policy means for each workload; all policies had
the same file size and recovered count.

| Entries | Payload | WAL bytes | Reopen mean range |
|---:|---:|---:|---:|
| 100 | 64 B | 21,561 | 7.41–8.69 ms |
| 100 | 1 KiB | 117,561 | 8.30–10.07 ms |
| 100 | 8 KiB | 834,361 | 17.58–23.10 ms |
| 1,000 | 64 B | 215,061 | 25.97–33.65 ms |
| 1,000 | 1 KiB | 1,175,061 | 22.22–26.70 ms |
| 1,000 | 8 KiB | 8,343,061 | 80.69–82.55 ms |
| 5,000 | 64 B | 1,075,061 | 163.11–234.70 ms |
| 5,000 | 1 KiB | 5,875,061 | 151.88–161.58 ms |
| 5,000 | 8 KiB | 41,715,061 | 458.72–477.28 ms |

This demonstrates scanner cost as the WAL grows, but it is a warm-page-cache
startup benchmark, not a cold-disk recovery benchmark. Unsafe runs were
explicitly flushed before reopen, so their successful recovery says nothing
about whether earlier nondurable acknowledgements would survive a crash.

## Other required measurements

P50, P95, process CPU, peak resident memory, retries, elections, and queue
pressure are retained per run in the raw JSONL and CSV. The following are
individual-run ranges, not aggregates across unlike configurations.

| Campaign | P50 range | P95 range | Process CPU range | Peak RSS range | Total retries | Elections/run |
|---|---:|---:|---:|---:|---:|---:|
| Cluster simulator | 10–363 ticks | 10–1,201 ticks | 0.165–283.912 s | 6.86–93.65 MiB | 1,181 | 1–37 |
| Cluster TCP | 170.9–470.0 ms | 265.0–4,578.0 ms | 12.953–66.487 s | 6.11–42.26 MiB | 548 | 1–42 |
| Runtime TCP | 172.1–499.5 ms | 246.2–2,896.5 ms | 12.728–66.772 s | 6.12–42.49 MiB | 552 | 1–31 |

The maximum simulator event queue was 60 entries; its network and storage
high-water marks were 1,579,527 and 756,961 bytes. Across both TCP campaigns,
the maximum owner, client, and storage-task queue depths were 36, 5, and 1.
There were zero queue rejections and zero transport-backpressure events.
Process CPU is a coarse process-wide timed-workload measurement, and peak RSS
is process-wide and lifetime/warmup-inclusive; neither is a per-node resource
allocation.

## Interpretation limits

- This is one non-isolated workstation on a Balanced power plan and Storage
  Space. Scheduler, cache, antivirus, and storage-stack noise are visible in
  some three-trial ranges.
- TCP nodes share one process and communicate over loopback. The numbers are
  local end-to-end implementation measurements, not networked deployment
  capacity.
- Only leader readiness was warmed; the 200 workload operations were measured
  cold. Three trials and short runs make min/max descriptive, not inferential.
- Per-run P99 is the nearest-rank value over 200 successes (the third-largest
  latency). Grouped P99s are means of three per-run P99s, not a pooled
  600-operation percentile.
- Null recovery fields mean “not applicable,” never zero. Leader crash is a
  persistent fault and therefore has no timed fault duration.
- Simulator `safety_check=passed` records an end-of-run invariant check. TCP
  `covered_by_runtime_integration_tests` means safety comes from the separate
  integration suite, not an online benchmark oracle.

The full cell-level values and original fields remain in the published raw
JSONL and CSV files; the generated SVGs use the same derived summaries.
