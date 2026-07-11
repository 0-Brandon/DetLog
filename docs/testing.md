# Test and experiment strategy

## Layers

1. Codec and WAL unit tests validate canonical encodings, bounds, checksum
   behavior, logical truncation, and recovery at every final-frame cut.
2. Table-driven Raft tests use explicit messages and storage completions so
   persistence boundaries are visible.
3. Deterministic cluster scenarios inject elections, crashes, partitions,
   duplicate/reordered messages, queue pressure, and slow storage.
4. A domain action generator runs seeded histories. Failures print the seed,
   derived cluster size, complete generated action list, and a deterministically
   deletion-minimized action list that preserves the failure.
5. Parser fuzz targets cover protocol frames, WAL byte streams, and serialized
   simulator scenarios. All sizes are bounded before allocation.
6. A subprocess crash-consistency test exits immediately after a durable WAL
   append and midway through a raw frame, then reopens the file with the
   production recovery scanner. Node crash/restart histories are exercised by
   both the deterministic cluster and the real loopback runtime tests.

ASan+UBSan and TSan use separate builds. The TSan preset builds and runs the
whole suite; its concurrency-sensitive coverage comes from the TCP and storage
worker adapters, while the consensus simulator itself is single-threaded.

## Benchmark definitions

Real TCP benchmark results use distinct `NodeHost` instances and WAL files in
one benchmark process, communicating only through real loopback sockets. The
`detlog-node` demonstration is the separate-process path. A reportable trial
records the hardware, OS, compiler, flags, build revision, cluster
configuration, fsync policy, warmup and measurement windows, offered-load
model, and raw operations.

End-to-end client latency begins when a request is first submitted and ends at
its successful response; retry count and overload rejection are separate
fields. Internal commit/apply latency may also be recorded but must not be
substituted for end-to-end latency.

Simulator virtual-time latency, simulator engine wall-clock throughput, and
real TCP performance are separate metrics. The cluster harness reports
replacement-leader readiness separately from the first successful
post-failure commit. WAL reopen/scan cost is a separate storage benchmark;
follower catch-up is asserted in integration tests but is not currently a
timed benchmark metric.

Safe fsync modes are flush-per-frame and bounded group commit. No-flush is
labelled nondurable in every filename, manifest, and chart.
