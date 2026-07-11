# Protocol and encoding

## Identities and indexes

- Node IDs are unsigned 32-bit integers fixed by static cluster configuration.
- Terms and log indexes are unsigned 64-bit integers. Index zero is the
  implicit empty-log sentinel and is never stored as a command entry.
- Client identity is a 128-bit session ID plus a 64-bit sequence number.
- RPC IDs correlate responses and prevent stale responses from regressing
  follower progress.

## Messages

`RequestVote` contains the candidate term, candidate ID, and its last log index
and term. `RequestVoteResponse` contains the responder term and grant decision.

`AppendEntries` contains the leader term and ID, RPC ID, previous index and
term, a bounded consecutive entry batch, the leader commit index, and no
implicit host pointers or timestamps. An empty entry batch is a heartbeat.

`AppendEntriesResponse` contains the responder term, RPC ID, success flag,
durable match index, and optional conflict term/index hints. Success means the
accepted mutation completed the selected durability barrier.

## Canonical binary representation

Wire integers are encoded explicitly in big-endian network order. WAL integers
use the storage format's explicit little-endian representation; neither format
serializes native C++ structs. Variable-length bytes are length-prefixed and
checked against configured limits before allocation.
Unknown message kinds, trailing bytes, invalid command kinds, nonconsecutive
entries, index-zero entries, and over-limit frames are rejected.

The deterministic transport moves immutable encoded frames through the same
decoder used by TCP. Network fault injection changes delivery behavior, not C++
objects, so framing and parser bugs remain observable in simulation.

Wire-version negotiation and cluster identity belong to the TCP connection
handshake. They are deliberately separate from Raft terms.

## Replay contract

The byte-for-byte replay guarantee covers the same executable, scenario
configuration, external client action list, and seed. Semantic JSONL fields are
kept stable across supported platforms, but byte-identical traces across
different standard libraries and compiler versions are not promised.
