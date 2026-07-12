# Architecture

DetLog is a teaching and verification implementation of canonical Raft for a
static three- or five-node cluster. It deliberately omits membership changes,
snapshots, pre-vote, leader leases, and leadership transfer.

## Core boundary

Each node owns one `RaftNode` instance. The core is a single-owner state
machine: it receives a protocol, timer, client, or storage-completion event and
returns effects. It never reads a clock, performs file I/O, sleeps, opens a
socket, or mutates shared state.

Effects cover:

- durable storage batches and their operation identifiers;
- protocol messages;
- timer arm/reset requests with generation numbers;
- client responses;
- structured trace records.

Durability is asynchronous. An action that depends on stable state remains in
the node's pending continuation until the matching storage completion arrives.
In particular, a node does not send a granted vote, a successful
`AppendEntries` response, election requests for a new self-voted term, or a
successful client response before the relevant durability barrier.

## Consensus subset

The implementation follows the Raft safety rules:

1. `currentTerm`, `votedFor`, and log edits are persistent state.
2. A voter compares `(lastLogTerm, lastLogIndex)` lexicographically.
3. A leader counts only its own durable log and successful durable follower
   acknowledgements in `matchIndex`.
4. A leader directly advances `commitIndex` only to an entry from its current
   term that is stored on a majority. Earlier entries become committed as a
   consequence.
5. Entries are applied in increasing index order and only through
   `commitIndex`.
6. A new leader first appends and commits a current-term no-op. It does not
   accept normal client commands until that readiness entry is applied.

The last rule gives recovery a synchronization point and prevents a leader
from answering from an incompletely rebuilt deduplication table.

## State machine and retries

The replicated state machine is a deterministic key-value map. Reads, when
used, are serialized as log commands; there is no lease or follower-read path.

Every client command carries a 128-bit session ID and a monotonically
increasing sequence number. A session may have one unresolved command. The
replicated session table stores the last sequence, the command digest, and the
result:

- the next sequence executes and caches its result;
- the same sequence and digest returns the cached result without executing;
- the same sequence with a different digest is a request-ID conflict;
- older or skipped sequences are rejected deterministically.

Session records are retained for the life of this version. Snapshotting,
session expiry, and log compaction are intentionally out of scope.

## Persistence

Every node has a versioned, checksummed WAL bound to a cluster ID and node ID.
The physical file is append-only. A frame can atomically contain a hard-state
update, a logical suffix deletion, replacement entries, and a monotonic commit
watermark. Recovery replays complete valid frames to reconstruct the logical
state.

An incomplete final frame is an unacknowledged torn tail and may be removed.
A complete frame with a bad checksum, malformed interior data, identity
mismatch, or a semantic regression fails the node closed.

The default client contract is:

1. the entry has completed a durability barrier on a majority;
2. the leader records its local commit watermark durably;
3. the leader applies the entry in order;
4. the leader returns the deterministic result.

Flush-per-frame and bounded group commit preserve this contract. In grouped
runtime mode, WAL frames may be staged without a barrier, but every dependent
message, timer, trace, public consensus status, state-machine apply, and client
reply is operation-tagged and quarantined until the shared flush succeeds. A
group closes on its operation, elapsed-time, or configured WAL byte bound. A
group append/flush failure is fail-stop and discards the quarantine. A
no-flush mode is an explicitly unsafe benchmark mode and is never used for
safety claims.

## Failure model

Supported failures include message loss, delay, duplication, reordering,
directional partitions, process crash, per-node slow storage, rejected/short writes,
and a torn unflushed WAL tail. A restarted node may be far behind while keeping
its latest intact durable state.

Rolling back state that was previously reported durable is outside the Raft
fault model. In particular, restoring an older `currentTerm` or `votedFor`
could permit two votes in one term. Media corruption of flushed bytes is
detected and fails closed; repair from peers is not implemented.

## Bounded resources

Limits are expressed in bytes as well as item counts. The core never blocks.
Client admission returns a retryable overload result before appending if the
pending-client or uncommitted-log limit is reached. Replication keeps bounded
per-peer progress and sends bounded batches rather than queueing one message
per entry. Timer and heartbeat work is coalesced. The simulator caps scheduled
events and fault duplication. It retains a bounded trace with an explicit
saturation marker; callers can serialize that retained trace as JSONL.
The real TCP adapter also exposes a bounded test/benchmark partition control
that drops queued frames, disconnects the selected peer, suppresses reconnects,
and heals without retaining partition-period traffic.
