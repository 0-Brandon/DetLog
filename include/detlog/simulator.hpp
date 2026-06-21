#pragma once

#include "detlog/model.hpp"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

namespace detlog::sim {

// PCG-XSH-RR with a pinned seeding and sampling procedure. In particular,
// bounded() does not use implementation-defined standard-library
// distributions, so a seed denotes the same sequence on every platform.
class Pcg32 {
 public:
  explicit Pcg32(std::uint64_t seed = 0,
                 std::uint64_t stream = 1) noexcept;

  [[nodiscard]] std::uint32_t next_u32() noexcept;
  [[nodiscard]] std::uint64_t next_u64() noexcept;
  [[nodiscard]] std::uint64_t bounded(std::uint64_t exclusive_bound) noexcept;
  [[nodiscard]] std::uint64_t inclusive(std::uint64_t minimum,
                                        std::uint64_t maximum) noexcept;
  [[nodiscard]] bool chance(std::uint64_t numerator,
                            std::uint64_t denominator) noexcept;

 private:
  std::uint64_t state_{};
  std::uint64_t increment_{};
};

struct EventQueueLimits {
  std::size_t max_pending_events{100'000U};
  std::uint64_t max_total_events{1'000'000U};
};

enum class StorageOperationKind : std::uint8_t { write = 1, flush = 2 };
enum class StorageCompletionStatus : std::uint8_t {
  success = 0,
  io_error,
  short_write,
};

[[nodiscard]] std::string_view to_string(
    StorageCompletionStatus status) noexcept;

struct TimerEvent {
  NodeId node{};
  TimerKind kind{TimerKind::election};
  std::uint64_t generation{};
  std::uint64_t incarnation{};

  friend bool operator==(const TimerEvent&, const TimerEvent&) = default;
};

struct NetworkEvent {
  std::uint64_t delivery_id{};
  NodeId from{};
  NodeId to{};
  std::uint64_t from_incarnation{};
  std::uint64_t to_incarnation{};
  std::uint32_t copy_index{};
  std::vector<std::byte> bytes;

  friend bool operator==(const NetworkEvent&, const NetworkEvent&) = default;
};

struct StorageEvent {
  NodeId node{};
  StorageOpId operation_id{};
  StorageOperationKind kind{StorageOperationKind::write};
  std::uint64_t incarnation{};
  std::size_t byte_count{};
  StorageCompletionStatus status{StorageCompletionStatus::success};

  friend bool operator==(const StorageEvent&, const StorageEvent&) = default;
};

struct UserEvent {
  NodeId node{};
  std::uint64_t tag{};
  std::uint64_t incarnation{};
  std::vector<std::byte> bytes;

  friend bool operator==(const UserEvent&, const UserEvent&) = default;
};

using EventPayload =
    std::variant<TimerEvent, NetworkEvent, StorageEvent, UserEvent>;

struct ScheduledEvent {
  Tick time{};
  std::uint64_t sequence{};
  EventPayload payload;

  friend bool operator==(const ScheduledEvent&, const ScheduledEvent&) =
      default;
};

enum class ScheduleStatus : std::uint8_t {
  scheduled = 0,
  unknown_node,
  node_down,
  payload_limit,
  time_in_past,
  time_overflow,
  pending_limit,
  total_limit,
  sequence_exhausted,
};

struct ScheduleResult {
  ScheduleStatus status{ScheduleStatus::scheduled};
  std::uint64_t sequence{};

  [[nodiscard]] explicit operator bool() const noexcept {
    return status == ScheduleStatus::scheduled;
  }
};

class EventQueue {
 public:
  explicit EventQueue(EventQueueLimits limits = {});

  [[nodiscard]] ScheduleResult schedule_at(Tick time,
                                           EventPayload payload);
  [[nodiscard]] ScheduleResult schedule_after(Tick delay,
                                              EventPayload payload);
  [[nodiscard]] std::optional<ScheduledEvent> pop_next();
  [[nodiscard]] std::vector<ScheduledEvent> extract_if(
      const std::function<bool(const ScheduledEvent&)>& predicate);

  [[nodiscard]] bool can_schedule(std::size_t count = 1) const noexcept;
  [[nodiscard]] Tick now() const noexcept { return now_; }
  [[nodiscard]] std::size_t pending() const noexcept { return heap_.size(); }
  [[nodiscard]] std::uint64_t total_scheduled() const noexcept {
    return total_scheduled_;
  }

 private:
  EventQueueLimits limits_;
  Tick now_{};
  std::uint64_t next_sequence_{1};
  std::uint64_t total_scheduled_{};
  std::vector<ScheduledEvent> heap_;
};

struct TransportLimits {
  std::size_t max_frame_bytes{1024U * 1024U};
  std::size_t max_queued_bytes_per_link{2U * 1024U * 1024U};
  std::size_t max_total_queued_bytes{16U * 1024U * 1024U};
  std::uint32_t max_extra_duplicates_per_send{8U};
  bool drop_in_flight_on_endpoint_crash{true};
};

struct StorageLimits {
  std::size_t max_file_bytes{64U * 1024U * 1024U};
  std::size_t max_write_bytes{1024U * 1024U};
  std::size_t max_pending_bytes{4U * 1024U * 1024U};
  std::size_t max_pending_operations{1024U};
};

struct SimulatorConfig {
  EventQueueLimits events;
  TransportLimits transport;
  StorageLimits storage;
  std::size_t max_trace_records{1'000'000U};
};

struct LinkFaultState {
  bool partitioned{};
  Tick additional_delay{};
  std::uint32_t drop_next{};
  std::uint32_t extra_copies_next{};

  friend bool operator==(const LinkFaultState&, const LinkFaultState&) =
      default;
};

enum class SendStatus : std::uint8_t {
  queued = 0,
  dropped,
  partitioned,
  unknown_node,
  endpoint_down,
  frame_limit,
  queue_limit,
  duplicate_limit,
  time_overflow,
  event_limit,
};

struct SendResult {
  SendStatus status{SendStatus::queued};
  std::uint32_t copies{};
  std::size_t queued_bytes{};

  [[nodiscard]] explicit operator bool() const noexcept {
    return status == SendStatus::queued;
  }
};

struct TimerToken {
  NodeId node{};
  TimerKind kind{TimerKind::election};
  std::uint64_t generation{};
  std::uint64_t incarnation{};
  std::uint64_t event_sequence{};

  friend bool operator==(const TimerToken&, const TimerToken&) = default;
};

enum class StorageSubmitStatus : std::uint8_t {
  queued = 0,
  unknown_node,
  node_down,
  write_limit,
  pending_limit,
  file_limit,
  time_overflow,
  event_limit,
  id_exhausted,
};

struct StorageSubmitResult {
  StorageSubmitStatus status{StorageSubmitStatus::queued};
  StorageOpId operation_id{};
  Tick completion_time{};

  [[nodiscard]] explicit operator bool() const noexcept {
    return status == StorageSubmitStatus::queued;
  }
};

struct StorageCrashSpec {
  // A deterministic prefix of completed-but-unflushed bytes that survives.
  // Values larger than the available tail preserve the entire tail.
  std::size_t surviving_unstable_bytes{};
  // A deterministic prefix of the currently active write that reaches disk.
  std::size_t torn_pending_write_bytes{};
};

enum class TraceKind : std::uint8_t {
  node_registered,
  node_crashed,
  node_restarted,
  timer_armed,
  timer_cancelled,
  timer_fired,
  stale_event,
  link_fault,
  network_queued,
  network_dropped,
  network_saturated,
  network_delivered,
  storage_queued,
  storage_completed,
  storage_crashed,
  storage_initialized,
  storage_repaired,
  storage_fault,
  storage_saturated,
  user_scheduled,
  user_delivered,
};

struct TraceRecord {
  Tick time{};
  std::uint64_t sequence{};
  TraceKind kind{TraceKind::stale_event};
  NodeId node{};
  NodeId peer{};
  std::uint64_t token{};
  std::uint64_t incarnation{};
  std::size_t bytes{};
  std::string detail;

  friend bool operator==(const TraceRecord&, const TraceRecord&) = default;
};

[[nodiscard]] std::string_view to_string(TraceKind kind) noexcept;
[[nodiscard]] std::string render_jsonl(
    std::span<const TraceRecord> records, bool truncated = false);

class Simulator {
 public:
  explicit Simulator(SimulatorConfig config = {});
  ~Simulator();
  Simulator(Simulator&&) noexcept;
  Simulator& operator=(Simulator&&) noexcept;
  Simulator(const Simulator&) = delete;
  Simulator& operator=(const Simulator&) = delete;

  [[nodiscard]] bool register_node(NodeId node);
  [[nodiscard]] bool is_alive(NodeId node) const noexcept;
  [[nodiscard]] std::uint64_t incarnation(NodeId node) const noexcept;
  [[nodiscard]] bool crash_node(NodeId node,
                                StorageCrashSpec storage = {});
  [[nodiscard]] bool restart_node(NodeId node);

  [[nodiscard]] std::optional<TimerToken> arm_timer(NodeId node,
                                                    TimerKind kind,
                                                    Tick delay);
  [[nodiscard]] bool cancel_timer(NodeId node, TimerKind kind);

  [[nodiscard]] bool set_partition(NodeId from, NodeId to,
                                   bool partitioned);
  [[nodiscard]] bool set_link_delay(NodeId from, NodeId to, Tick delay);
  [[nodiscard]] bool drop_next(NodeId from, NodeId to,
                               std::uint32_t count = 1);
  [[nodiscard]] bool duplicate_next(NodeId from, NodeId to,
                                    std::uint32_t extra_copies = 1);
  [[nodiscard]] LinkFaultState link_fault(NodeId from, NodeId to) const;

  [[nodiscard]] SendResult send(NodeId from, NodeId to,
                                std::span<const std::byte> bytes,
                                Tick base_delay = 0);

  [[nodiscard]] StorageSubmitResult submit_write(
      NodeId node, std::span<const std::byte> bytes, Tick latency);
  [[nodiscard]] StorageSubmitResult submit_flush(NodeId node, Tick latency);
  [[nodiscard]] bool fail_next_write(NodeId node);
  [[nodiscard]] bool short_write_next(NodeId node,
                                      std::size_t prefix_bytes);
  [[nodiscard]] bool fail_next_flush(NodeId node);

  // Bootstraps a new device with an already-durable file header. Repair is
  // restricted to a stopped node and only removes a suffix identified by a
  // higher-level recovery scanner.
  [[nodiscard]] bool initialize_storage(
      NodeId node, std::span<const std::byte> stable_bytes);
  [[nodiscard]] bool truncate_stable_storage(NodeId node,
                                             std::size_t valid_bytes);

  [[nodiscard]] ScheduleResult schedule_user(NodeId node, std::uint64_t tag,
                                             std::span<const std::byte> bytes,
                                             Tick delay);

  // Stale generation/incarnation events and deliveries blocked by a current
  // partition are consumed and traced internally; callers only see live work.
  [[nodiscard]] std::optional<ScheduledEvent> next();

  [[nodiscard]] Tick now() const noexcept;
  [[nodiscard]] std::size_t pending_events() const noexcept;
  [[nodiscard]] std::size_t queued_network_bytes() const noexcept;

  [[nodiscard]] std::span<const std::byte> stable_bytes(NodeId node) const;
  [[nodiscard]] std::span<const std::byte> unstable_bytes(NodeId node) const;
  [[nodiscard]] std::vector<std::byte> visible_bytes(NodeId node) const;
  [[nodiscard]] std::size_t pending_storage_bytes(NodeId node) const noexcept;

  [[nodiscard]] const std::vector<TraceRecord>& trace() const noexcept;
  [[nodiscard]] bool trace_truncated() const noexcept;
  [[nodiscard]] std::string trace_jsonl() const;

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace detlog::sim
