#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "detlog/codec.hpp"
#include "detlog/raft.hpp"
#include "detlog/tcp_transport.hpp"
#include "detlog/wal.hpp"

namespace detlog {

struct NodeHostLimits {
  std::size_t max_owner_events{1024};
  std::size_t max_client_queue{256};
  // Outstanding requests plus replies waiting to be polled share this bound.
  std::size_t max_client_slots{1024};
  std::size_t max_trace_records{4096};
  std::size_t max_storage_tasks{16};
  std::size_t max_storage_completions{16};
  std::size_t max_group_quarantined_effects{4096};
  std::size_t max_tcp_events_per_poll{128};
};

// Safe group commit is an explicit runtime policy layered above the WAL. When
// enabled, NodeHost may stage several WAL frames without flushing, but it does
// not expose any persistence-dependent Raft effect until one shared durability
// barrier succeeds. A batch that advances commit_index is never completed to
// Raft before that barrier, so state-machine apply also remains post-durable.
// The operation and delay limits bound both the batch size and acknowledgement
// latency. The default preserves one flush per append.
struct NodeHostGroupCommitConfig {
  bool enabled{};
  std::size_t max_operations{8};
  std::chrono::milliseconds max_delay{2};
};

struct NodeHostConfig {
  RaftConfig raft;
  WalIdentity identity;
  std::filesystem::path wal_path;
  WalOptions wal;
  TcpTransportConfig tcp;
  codec::Limits codec;
  std::chrono::milliseconds tick_duration{10};
  Tick election_jitter{10};
  std::uint64_t timer_seed{1};
  std::chrono::milliseconds idle_sleep{1};
  NodeHostLimits limits;
  NodeHostGroupCommitConfig group_commit;
};

struct NodeHostMetrics {
  std::uint64_t tcp_messages_received{};
  std::uint64_t tcp_decode_drops{};
  std::uint64_t tcp_messages_queued{};
  std::uint64_t tcp_backpressure_drops{};
  std::uint64_t tcp_down_drops{};
  std::uint64_t tcp_error_drops{};
  std::uint64_t tcp_peer_rejections{};
  std::uint64_t owner_queue_backpressure{};
  std::uint64_t storage_submitted{};
  std::uint64_t storage_completed{};
  std::uint64_t storage_errors{};
  // Physical durability barriers issued by the runtime. This is zero for the
  // explicitly unsafe WAL policy.
  std::uint64_t storage_flushes{};
  // Shared barriers used by the safe group-commit policy. Operations counts
  // frames made durable by those barriers, including size-one timeout groups.
  std::uint64_t storage_group_commits{};
  std::uint64_t storage_grouped_operations{};
  std::uint64_t storage_staged_operations{};
  std::size_t storage_group_max_size{};
  std::size_t storage_group_max_bytes{};
  std::size_t storage_task_queue_high_water{};
  std::size_t storage_quarantine_high_water{};
  std::uint64_t clients_submitted{};
  std::uint64_t client_replies{};
  std::uint64_t client_submit_backpressure{};
  std::uint64_t trace_drops{};
  std::size_t owner_queue_high_water{};
  std::size_t client_queue_high_water{};
};

enum class NodeHostSubmitStatus : std::uint8_t {
  accepted,
  would_block,
  stopped,
  error,
};

struct NodeHostSubmitResult {
  NodeHostSubmitStatus status{NodeHostSubmitStatus::error};
  ClientToken token{};
};

struct NodeHostStatus {
  NodeId node{};
  RaftRole role{RaftRole::unavailable};
  Term term{};
  std::optional<NodeId> leader_hint;
  bool leader_ready{};
  LogIndex last_log_index{};
  LogIndex durable_last_log_index{};
  LogIndex commit_index{};
  LogIndex last_applied{};
  bool storage_pending{};
  std::size_t owner_events{};
  std::size_t client_queue{};
};

// Caller-driven, single-owner adapter. start(), poll_once(), run_for(),
// submit(), status access, and reply/trace polling must all be called from the
// same owner thread. Worker threads only produce immutable TCP or storage
// completions and never access RaftNode.
class NodeHost final {
 public:
  explicit NodeHost(NodeHostConfig config);
  ~NodeHost();

  NodeHost(const NodeHost&) = delete;
  NodeHost& operator=(const NodeHost&) = delete;
  NodeHost(NodeHost&&) = delete;
  NodeHost& operator=(NodeHost&&) = delete;

  void start();
  [[nodiscard]] bool poll_once();
  [[nodiscard]] std::size_t run_for(std::chrono::milliseconds duration);
  void stop() noexcept;

  [[nodiscard]] NodeHostSubmitResult submit(ClientCommand command);
  [[nodiscard]] std::vector<ClientReply> poll_replies(
      std::size_t maximum = 1024);
  [[nodiscard]] std::vector<TraceRecord> poll_traces(
      std::size_t maximum = 1024);

  // Benchmark/test fault control for one directed real-TCP link. Call both
  // endpoints to model a bidirectional partition.
  [[nodiscard]] bool set_peer_partitioned(NodeId peer,
                                          bool partitioned) noexcept;

  [[nodiscard]] bool running() const noexcept;
  [[nodiscard]] std::uint16_t listening_port() const;
  [[nodiscard]] NodeHostStatus status() const;
  [[nodiscard]] NodeHostMetrics metrics() const noexcept;
  [[nodiscard]] std::optional<std::string> value_for_test(
      std::string_view key) const;
  [[nodiscard]] const std::vector<LogEntry>& log_for_test() const;

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace detlog
