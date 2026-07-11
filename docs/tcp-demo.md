# Real TCP cluster demo

`detlog-node` runs one file-backed Raft node. It is deliberately a demonstration
adapter, not a public client protocol: stdin commands are submitted only to the
local node, while Raft RPCs travel over the shared binary codec and TCP
transport.

The transport binds to loopback by default. Every node must use the same
128-bit cluster ID and membership list, while each node needs a distinct WAL
path and listening port.

## Build

Build the `detlog-node` target with a C++20 compiler. Windows links `ws2_32`;
POSIX builds link the platform thread library (`pthread` on Linux).

```sh
cmake -S . -B build -DBUILD_TESTING=ON
cmake --build build --target detlog-node
```

## Start three nodes

Open three terminals from the repository root. The examples use the same
membership string and cluster ID in every process:

```text
members = 1@127.0.0.1:7101,2@127.0.0.1:7102,3@127.0.0.1:7103
cluster = 4445544c4f470001:54435044454d4f31
```

Terminal 1:

```sh
build/detlog-node --id 1 --members 1@127.0.0.1:7101,2@127.0.0.1:7102,3@127.0.0.1:7103 --wal data/node-1.wal --cluster 4445544c4f470001:54435044454d4f31 --seed 101 --incarnation 1
```

Terminal 2:

```sh
build/detlog-node --id 2 --members 1@127.0.0.1:7101,2@127.0.0.1:7102,3@127.0.0.1:7103 --wal data/node-2.wal --cluster 4445544c4f470001:54435044454d4f31 --seed 202 --incarnation 1
```

Terminal 3:

```sh
build/detlog-node --id 3 --members 1@127.0.0.1:7101,2@127.0.0.1:7102,3@127.0.0.1:7103 --wal data/node-3.wal --cluster 4445544c4f470001:54435044454d4f31 --seed 303 --incarnation 1
```

On Windows, use `build\detlog-node.exe`. Ensure the `data` directory exists
before starting the processes.

Enter `status` in each terminal. Once one node reports `role=leader ready=1`,
submit commands in that terminal:

```text
put color blue
get color
erase color
retry
metrics
```

`put`, `get`, and `erase` are all replicated commands. `retry` resubmits the
last command with the same session/request ID, demonstrating the replicated
at-most-once result cache. A request sent to a follower returns `not_leader`
and, when known, prints the leader ID; the CLI does not automatically forward
it.

## Crash and restart

Stop a follower with `quit`, then restart it with the same node ID, membership,
cluster ID, port, and WAL path. Increase `--incarnation` on a process restart:

```sh
build/detlog-node --id 3 --members 1@127.0.0.1:7101,2@127.0.0.1:7102,3@127.0.0.1:7103 --wal data/node-3.wal --cluster 4445544c4f470001:54435044454d4f31 --seed 303 --incarnation 2
```

The node first reconstructs hard state, its logical log, commit watermark, KV
state, and request-deduplication state from the WAL. It then reconnects and
catches up missing entries from the current leader. A wrong cluster ID, unknown
node ID, duplicate connection, self-connection, or unsupported handshake
version is rejected before any Raft bytes are delivered.

For a leader-failure demonstration, stop the leader, wait for another node to
report `leader ready=1`, and continue submitting there. Two of three nodes (or
three of five) must remain available to commit.

## Scope and observability

- `metrics` reports queued/received messages, explicit TCP backpressure and
  disconnected-peer drops, and storage errors.
- WAL completion is delivered to Raft only after a durability barrier. A WAL
  error makes the node unavailable; it is never converted into a successful
  replication acknowledgment.
- Owner queues, storage queues, TCP queues, and client/reply slots are bounded.
- The TCP transport uses real steady-clock timers and OS scheduling. The seed
  makes election jitter reproducible, but this real-process demo does not claim
  the deterministic event ordering of the simulator.
- Non-loopback binding or dialing requires `--allow-non-loopback`. TLS and
  authentication are intentionally out of scope, so do not expose this demo to
  an untrusted network.
