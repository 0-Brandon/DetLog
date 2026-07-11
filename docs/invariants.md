# Safety invariants and protection map

The simulator's observer evaluates these properties after every event. It uses
the event history and durable replicas rather than trusting a node's own
`commitIndex` flag.

| ID | Precise property | Protecting implementation path | Primary tests |
| --- | --- | --- | --- |
| I1 Election safety | At most one candidate obtains a majority vote certificate in a term. | Durable hard-state update before granting or soliciting votes; unique voter counting. | Two-candidate election histories; crash between vote write and completion. |
| I2 Term monotonicity | A node's durable term never decreases and it grants at most one candidate a vote per term. | Hard-state WAL validation and higher-term transition. | WAL hard-state regression; delayed old-term RPC. |
| I3 Log matching | If two logs contain the same index and term, their prefixes through that index are identical. | `prevLogIndex`/`prevLogTerm` validation and atomic logical suffix replacement. | Conflicting follower suffix; reordered `AppendEntries`. |
| I4 Leader completeness | Every leader elected after an entry commits contains that entry. | Up-to-date vote comparison plus majority intersection. | Old leader rejoin and newer-term election. |
| I5 Commit certificate | Every newly committed index is justified by durable storage on a majority and the leader's current-term rule. | Durable `matchIndex`; commit scan restricted to current-term entries. | Minority partition; prior-term entry not committed directly. |
| I6 Committed entries are immutable | No valid log edit removes or changes an entry at or below a known commit watermark. | Core truncation guard and WAL recovery validation. | Attempted committed-prefix replacement. |
| I7 State-machine safety | No two nodes apply different commands at the same index. | Commit certificate, log matching, and ordered apply loop. | Random schedules with invariant observation. |
| I8 Apply discipline | `lastApplied` increases one at a time and never exceeds `commitIndex`. | Single ordered apply path. | Commit jump and restart replay. |
| I9 At-most-once client effect | A `(session, sequence, command digest)` changes the state machine at most once. | Replicated session table in deterministic apply. | Lost reply followed by retry; duplicate log commands. |
| I10 Tail recovery | Discarding an incomplete final WAL frame cannot alter any prior valid frame. | Framing, checksum, last-valid offset, and durable tail truncation. | Every byte cut of the last frame. |
| I11 Replay determinism | A fixed executable, configuration, workload, and seed yields the same semantic trace. | Pinned PRNG, integer time, total event ordering, canonical codec/JSON. | Repeat scenario and compare trace/hash. |
| I12 Resource bounds | A slow or partitioned peer cannot cause a configured queue or uncommitted-log bound to be exceeded. | Admission checks, per-peer bounded batches, event/fault caps. | Slow follower and prolonged minority partition. |

Convergence is not itself a safety oracle. Tests must check the relevant
property throughout the history, including histories that never regain
liveness.

