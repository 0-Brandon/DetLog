#pragma once

#include "detlog/codec.hpp"
#include "detlog/raft.hpp"
#include "detlog/simulator.hpp"
#include "detlog/wal.hpp"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace detlog {

struct ClusterConfig {
  std::vector<NodeId> members{1, 2, 3};
  std::uint64_t seed{1};
  RaftConfig raft;
  codec::Limits codec;
  sim::SimulatorConfig simulator;
  WalOptions wal;
  std::uint64_t cluster_id_high{0x4445544c4f470001ULL};
  std::uint64_t cluster_id_low{0x53494d554c41544fULL};
  Tick election_jitter{10};
  Tick network_delay{1};
  Tick storage_write_latency{1};
  Tick storage_flush_latency{1};
  std::size_t max_semantic_trace_records{1'000'000U};
  std::size_t max_retained_client_replies{10'000U};
  bool automatic_invariant_checks{true};
};

[[nodiscard]] ClusterConfig cluster_config(std::size_t node_count,
                                           std::uint64_t seed = 1);

struct NodeSnapshot {
  NodeId id{};
  bool alive{};
  RaftRole role{RaftRole::unavailable};
  Term term{};
  bool leader_ready{};
  LogIndex last_log_index{};
  LogIndex durable_last_log_index{};
  LogIndex commit_index{};
  LogIndex last_applied{};
  std::size_t deferred_events{};
  std::vector<LogEntry> log;

  friend bool operator==(const NodeSnapshot&, const NodeSnapshot&) = default;
};

struct ClusterQueueDepth {
  std::size_t scheduled_events{};
  std::size_t network_bytes{};
  std::size_t storage_bytes{};

  friend bool operator==(const ClusterQueueDepth&,
                         const ClusterQueueDepth&) = default;
};

struct InvariantResult {
  bool ok{true};
  std::string detail;

  [[nodiscard]] explicit operator bool() const noexcept { return ok; }
};

enum class ClusterTraceSource : std::uint8_t {
  adapter = 1,
  raft = 2,
  simulator = 3,
};

struct ClusterTraceRecord {
  Tick time{};
  std::uint64_t sequence{};
  ClusterTraceSource source{ClusterTraceSource::adapter};
  std::string kind;
  NodeId node{};
  NodeId peer{};
  Term term{};
  LogIndex index{};
  std::uint64_t token{};
  std::size_t bytes{};
  std::string detail;

  friend bool operator==(const ClusterTraceRecord&,
                         const ClusterTraceRecord&) = default;
};

[[nodiscard]] std::string_view to_string(ClusterTraceSource source) noexcept;
[[nodiscard]] std::string render_cluster_jsonl(
    std::span<const ClusterTraceRecord> records);
[[nodiscard]] InvariantResult check_log_matching(
    std::span<const NodeSnapshot> snapshots);

// Whole-cluster single-threaded adapter for RaftNode. Protocol messages always
// traverse the canonical codec and simulated byte transport. A PersistEffect
// completes only after simulated write and flush completions.
//
// Persisted bytes use the production WAL encoder. Restart scans the simulated
// file through the production recovery scanner and removes an incomplete final
// frame before any later append, so the cluster exercises the same torn-tail
// semantics as the real file-backed path.
class DeterministicCluster {
 public:
  explicit DeterministicCluster(ClusterConfig config = {});
  ~DeterministicCluster();
  DeterministicCluster(DeterministicCluster&&) noexcept;
  DeterministicCluster& operator=(DeterministicCluster&&) noexcept;
  DeterministicCluster(const DeterministicCluster&) = delete;
  DeterministicCluster& operator=(const DeterministicCluster&) = delete;

  [[nodiscard]] const ClusterConfig& config() const noexcept;
  [[nodiscard]] const std::vector<NodeId>& members() const noexcept;
  [[nodiscard]] Tick now() const noexcept;
  [[nodiscard]] ClusterQueueDepth queue_depth() const noexcept;

  [[nodiscard]] std::optional<NodeSnapshot> snapshot(NodeId node) const;
  [[nodiscard]] std::vector<NodeSnapshot> snapshots() const;
  [[nodiscard]] std::vector<NodeId> leaders(bool require_ready = false) const;
  [[nodiscard]] std::optional<NodeId> leader(
      bool require_ready = false) const;

  [[nodiscard]] bool step();
  [[nodiscard]] std::size_t run(std::size_t max_events);
  [[nodiscard]] bool run_until(
      const std::function<bool(const DeterministicCluster&)>& predicate,
      std::size_t max_events);
  [[nodiscard]] std::optional<NodeId> run_until_leader(
      std::size_t max_events = 10'000U, bool require_ready = true);

  [[nodiscard]] ClientToken submit(NodeId node,
                                   const ClientCommand& command);
  [[nodiscard]] std::optional<ClientToken> submit_to_leader(
      const ClientCommand& command, bool require_ready = true);
  [[nodiscard]] bool run_until_reply(ClientToken token,
                                     std::size_t max_events = 10'000U);
  [[nodiscard]] std::optional<ClientReply> reply_for(ClientToken token) const;
  [[nodiscard]] const std::vector<ClientReply>& replies() const noexcept;
  [[nodiscard]] std::vector<ClientReply> take_replies();

  [[nodiscard]] bool crash(NodeId node,
                           sim::StorageCrashSpec storage = {});
  [[nodiscard]] bool restart(NodeId node);

  [[nodiscard]] bool set_partition(NodeId from, NodeId to,
                                   bool partitioned);
  [[nodiscard]] bool set_bidirectional_partition(NodeId first, NodeId second,
                                                 bool partitioned);
  [[nodiscard]] bool isolate(NodeId node, bool partitioned);
  [[nodiscard]] bool set_link_delay(NodeId from, NodeId to, Tick delay);
  [[nodiscard]] bool drop_next(NodeId from, NodeId to,
                               std::uint32_t count = 1);
  [[nodiscard]] bool duplicate_next(NodeId from, NodeId to,
                                    std::uint32_t extra_copies = 1);
  [[nodiscard]] bool fail_next_storage_write(NodeId node);
  [[nodiscard]] bool short_write_next(NodeId node,
                                      std::size_t prefix_bytes);
  [[nodiscard]] bool fail_next_storage_flush(NodeId node);

  [[nodiscard]] std::optional<RecoveredState> stable_state(NodeId node) const;
  [[nodiscard]] std::optional<std::string> value(NodeId node,
                                                 std::string_view key) const;
  [[nodiscard]] std::optional<LogEntry> committed_entry(LogIndex index) const;
  [[nodiscard]] bool command_is_committed(const SessionId& session,
                                          std::uint64_t sequence) const;

  // This observer is independent of the core's transition decisions. It also
  // records newly observed committed entries in an immutable ghost prefix.
  [[nodiscard]] InvariantResult check_invariants();
  [[nodiscard]] const InvariantResult& last_invariant_result() const noexcept;

  [[nodiscard]] const std::vector<ClusterTraceRecord>& trace() const noexcept;
  [[nodiscard]] bool trace_truncated() const noexcept;
  [[nodiscard]] std::string trace_jsonl() const;

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

using Cluster = DeterministicCluster;

}  // namespace detlog
