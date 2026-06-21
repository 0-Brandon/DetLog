#include "detlog/simulator.hpp"

#include <algorithm>
#include <array>
#include <charconv>
#include <limits>
#include <map>
#include <tuple>
#include <utility>

namespace detlog::sim {
namespace {

[[nodiscard]] bool add_overflows(Tick lhs, Tick rhs) noexcept {
  return rhs > std::numeric_limits<Tick>::max() - lhs;
}

[[nodiscard]] bool size_add_overflows(std::size_t lhs,
                                      std::size_t rhs) noexcept {
  return rhs > std::numeric_limits<std::size_t>::max() - lhs;
}

[[nodiscard]] bool size_multiply_overflows(std::size_t lhs,
                                           std::size_t rhs) noexcept {
  return lhs != 0 && rhs > std::numeric_limits<std::size_t>::max() / lhs;
}

struct EarlierEvent {
  [[nodiscard]] bool operator()(const ScheduledEvent& lhs,
                                const ScheduledEvent& rhs) const noexcept {
    // std::push_heap places the element for which the comparator is false at
    // the front. Reversing the natural order therefore creates a min-heap.
    return std::tie(lhs.time, lhs.sequence) >
           std::tie(rhs.time, rhs.sequence);
  }
};

void append_unsigned(std::string& output, std::uint64_t value) {
  std::array<char, 32> buffer{};
  const auto [end, error] =
      std::to_chars(buffer.data(), buffer.data() + buffer.size(), value);
  if (error == std::errc{}) {
    output.append(buffer.data(), end);
  }
}

void append_json_string(std::string& output, std::string_view value) {
  constexpr char kHex[] = "0123456789abcdef";
  output.push_back('"');
  for (const char raw_byte : value) {
    const auto byte = static_cast<unsigned char>(raw_byte);
    switch (byte) {
      case '"':
        output.append("\\\"");
        break;
      case '\\':
        output.append("\\\\");
        break;
      case '\b':
        output.append("\\b");
        break;
      case '\f':
        output.append("\\f");
        break;
      case '\n':
        output.append("\\n");
        break;
      case '\r':
        output.append("\\r");
        break;
      case '\t':
        output.append("\\t");
        break;
      default:
        if (byte < 0x20U) {
          output.append("\\u00");
          output.push_back(kHex[byte >> 4U]);
          output.push_back(kHex[byte & 0x0FU]);
        } else {
          output.push_back(static_cast<char>(byte));
        }
        break;
    }
  }
  output.push_back('"');
}

[[nodiscard]] std::uint64_t& timer_generation_for(
    std::uint64_t& election, std::uint64_t& heartbeat,
    TimerKind kind) noexcept {
  return kind == TimerKind::heartbeat ? heartbeat : election;
}

void advance_generation(std::uint64_t& generation) noexcept {
  // EventQueue's total-event cap makes wraparound unreachable in practice.
  // Keeping zero reserved also makes default-constructed tokens invalid.
  if (generation == std::numeric_limits<std::uint64_t>::max()) {
    generation = 1;
  } else {
    ++generation;
  }
}

}  // namespace

Pcg32::Pcg32(std::uint64_t seed, std::uint64_t stream) noexcept
    : increment_((stream << 1U) | 1U) {
  (void)next_u32();
  state_ += seed;
  (void)next_u32();
}

std::uint32_t Pcg32::next_u32() noexcept {
  const std::uint64_t previous = state_;
  state_ = previous * 6364136223846793005ULL + increment_;
  const auto xorshifted = static_cast<std::uint32_t>(
      ((previous >> 18U) ^ previous) >> 27U);
  const auto rotation = static_cast<std::uint32_t>(previous >> 59U);
  return static_cast<std::uint32_t>(
      (xorshifted >> rotation) |
      (xorshifted << ((0U - rotation) & 31U)));
}

std::uint64_t Pcg32::next_u64() noexcept {
  const auto high = static_cast<std::uint64_t>(next_u32()) << 32U;
  return high | next_u32();
}

std::uint64_t Pcg32::bounded(std::uint64_t exclusive_bound) noexcept {
  if (exclusive_bound == 0) {
    return 0;
  }
  const std::uint64_t threshold = (0U - exclusive_bound) % exclusive_bound;
  for (;;) {
    const auto value = next_u64();
    if (value >= threshold) {
      return value % exclusive_bound;
    }
  }
}

std::uint64_t Pcg32::inclusive(std::uint64_t minimum,
                               std::uint64_t maximum) noexcept {
  if (minimum > maximum) {
    return minimum;
  }
  const std::uint64_t distance = maximum - minimum;
  if (distance == std::numeric_limits<std::uint64_t>::max()) {
    return next_u64();
  }
  return minimum + bounded(distance + 1U);
}

bool Pcg32::chance(std::uint64_t numerator,
                   std::uint64_t denominator) noexcept {
  if (denominator == 0 || numerator == 0) {
    return false;
  }
  if (numerator >= denominator) {
    return true;
  }
  return bounded(denominator) < numerator;
}

EventQueue::EventQueue(EventQueueLimits limits) : limits_(limits) {}

ScheduleResult EventQueue::schedule_at(Tick time, EventPayload payload) {
  if (time < now_) {
    return {ScheduleStatus::time_in_past, 0};
  }
  if (heap_.size() >= limits_.max_pending_events) {
    return {ScheduleStatus::pending_limit, 0};
  }
  if (total_scheduled_ >= limits_.max_total_events) {
    return {ScheduleStatus::total_limit, 0};
  }
  if (next_sequence_ == 0) {
    return {ScheduleStatus::sequence_exhausted, 0};
  }

  const std::uint64_t sequence = next_sequence_;
  if (next_sequence_ == std::numeric_limits<std::uint64_t>::max()) {
    next_sequence_ = 0;
  } else {
    ++next_sequence_;
  }
  ++total_scheduled_;
  heap_.push_back(ScheduledEvent{time, sequence, std::move(payload)});
  std::push_heap(heap_.begin(), heap_.end(), EarlierEvent{});
  return {ScheduleStatus::scheduled, sequence};
}

ScheduleResult EventQueue::schedule_after(Tick delay, EventPayload payload) {
  if (add_overflows(now_, delay)) {
    return {ScheduleStatus::time_overflow, 0};
  }
  return schedule_at(now_ + delay, std::move(payload));
}

std::optional<ScheduledEvent> EventQueue::pop_next() {
  if (heap_.empty()) {
    return std::nullopt;
  }
  std::pop_heap(heap_.begin(), heap_.end(), EarlierEvent{});
  auto event = std::move(heap_.back());
  heap_.pop_back();
  now_ = event.time;
  return event;
}

std::vector<ScheduledEvent> EventQueue::extract_if(
    const std::function<bool(const ScheduledEvent&)>& predicate) {
  std::vector<ScheduledEvent> kept;
  std::vector<ScheduledEvent> removed;
  kept.reserve(heap_.size());
  removed.reserve(heap_.size());
  for (auto& event : heap_) {
    if (predicate(event)) {
      removed.push_back(std::move(event));
    } else {
      kept.push_back(std::move(event));
    }
  }
  heap_ = std::move(kept);
  std::make_heap(heap_.begin(), heap_.end(), EarlierEvent{});
  return removed;
}

bool EventQueue::can_schedule(std::size_t count) const noexcept {
  if (count > limits_.max_pending_events ||
      heap_.size() > limits_.max_pending_events - count) {
    return false;
  }
  if (count > limits_.max_total_events ||
      total_scheduled_ > limits_.max_total_events - count) {
    return false;
  }
  if (next_sequence_ == 0) {
    return false;
  }
  const auto remaining_sequences =
      std::numeric_limits<std::uint64_t>::max() - next_sequence_ + 1U;
  return count <= remaining_sequences;
}

std::string_view to_string(TraceKind kind) noexcept {
  switch (kind) {
    case TraceKind::node_registered:
      return "node_registered";
    case TraceKind::node_crashed:
      return "node_crashed";
    case TraceKind::node_restarted:
      return "node_restarted";
    case TraceKind::timer_armed:
      return "timer_armed";
    case TraceKind::timer_cancelled:
      return "timer_cancelled";
    case TraceKind::timer_fired:
      return "timer_fired";
    case TraceKind::stale_event:
      return "stale_event";
    case TraceKind::link_fault:
      return "link_fault";
    case TraceKind::network_queued:
      return "network_queued";
    case TraceKind::network_dropped:
      return "network_dropped";
    case TraceKind::network_saturated:
      return "network_saturated";
    case TraceKind::network_delivered:
      return "network_delivered";
    case TraceKind::storage_queued:
      return "storage_queued";
    case TraceKind::storage_completed:
      return "storage_completed";
    case TraceKind::storage_crashed:
      return "storage_crashed";
    case TraceKind::storage_initialized:
      return "storage_initialized";
    case TraceKind::storage_repaired:
      return "storage_repaired";
    case TraceKind::storage_fault:
      return "storage_fault";
    case TraceKind::storage_saturated:
      return "storage_saturated";
    case TraceKind::user_scheduled:
      return "user_scheduled";
    case TraceKind::user_delivered:
      return "user_delivered";
  }
  return "unknown";
}

std::string_view to_string(StorageCompletionStatus status) noexcept {
  switch (status) {
    case StorageCompletionStatus::success:
      return "success";
    case StorageCompletionStatus::io_error:
      return "io_error";
    case StorageCompletionStatus::short_write:
      return "short_write";
  }
  return "unknown";
}

std::string render_jsonl(std::span<const TraceRecord> records,
                         bool truncated) {
  std::string output;
  output.reserve(records.size() * 160U);
  for (const auto& record : records) {
    output.append("{\"time\":");
    append_unsigned(output, record.time);
    output.append(",\"sequence\":");
    append_unsigned(output, record.sequence);
    output.append(",\"kind\":");
    append_json_string(output, to_string(record.kind));
    output.append(",\"node\":");
    append_unsigned(output, record.node);
    output.append(",\"peer\":");
    append_unsigned(output, record.peer);
    output.append(",\"token\":");
    append_unsigned(output, record.token);
    output.append(",\"incarnation\":");
    append_unsigned(output, record.incarnation);
    output.append(",\"bytes\":");
    append_unsigned(output, record.bytes);
    output.append(",\"detail\":");
    append_json_string(output, record.detail);
    output.append("}\n");
  }
  if (truncated) {
    output.append("{\"trace_truncated\":true}\n");
  }
  return output;
}

struct Simulator::Impl {
  struct PendingStorageOperation {
    StorageOperationKind kind{StorageOperationKind::write};
    std::vector<std::byte> bytes;
    StorageCompletionStatus completion_status{
        StorageCompletionStatus::success};
    std::size_t actual_write_bytes{};
    Tick completion_time{};
    std::uint64_t event_sequence{};
  };

  struct StorageDevice {
    std::vector<std::byte> stable;
    std::vector<std::byte> unstable;
    std::map<StorageOpId, PendingStorageOperation> pending;
    std::size_t pending_bytes{};
    Tick available_at{};
    bool fail_next_write{};
    std::optional<std::size_t> short_write_prefix;
    bool fail_next_flush{};
  };

  struct NodeState {
    bool alive{true};
    std::uint64_t incarnation{1};
    std::uint64_t election_generation{};
    std::uint64_t heartbeat_generation{};
    StorageDevice storage;
  };

  struct LinkState {
    LinkFaultState fault;
    std::size_t queued_bytes{};
  };

  explicit Impl(SimulatorConfig initial_config)
      : config(std::move(initial_config)), events(config.events) {
    if (config.transport.max_extra_duplicates_per_send ==
        std::numeric_limits<std::uint32_t>::max()) {
      --config.transport.max_extra_duplicates_per_send;
    }
  }

  using LinkKey = std::pair<NodeId, NodeId>;

  [[nodiscard]] NodeState* node(NodeId id) noexcept {
    const auto found = nodes.find(id);
    return found == nodes.end() ? nullptr : &found->second;
  }

  [[nodiscard]] const NodeState* node(NodeId id) const noexcept {
    const auto found = nodes.find(id);
    return found == nodes.end() ? nullptr : &found->second;
  }

  [[nodiscard]] LinkState& link(NodeId from, NodeId to) {
    return links[{from, to}];
  }

  [[nodiscard]] const LinkState* find_link(NodeId from,
                                           NodeId to) const noexcept {
    const auto found = links.find({from, to});
    return found == links.end() ? nullptr : &found->second;
  }

  void record(TraceKind kind, NodeId node_id = 0, NodeId peer = 0,
              std::uint64_t token = 0, std::uint64_t incarnation = 0,
              std::size_t bytes = 0, std::string_view detail = {}) {
    if (trace_records.size() >= config.max_trace_records) {
      trace_was_truncated = true;
      return;
    }
    trace_records.push_back(TraceRecord{
        events.now(), next_trace_sequence++, kind, node_id, peer, token,
        incarnation, bytes, std::string(detail)});
  }

  [[nodiscard]] std::uint64_t& timer_generation(NodeState& state,
                                                TimerKind kind) noexcept {
    return timer_generation_for(state.election_generation,
                                state.heartbeat_generation, kind);
  }

  [[nodiscard]] Tick storage_start(const StorageDevice& storage) const {
    return std::max(events.now(), storage.available_at);
  }

  void release_network_bytes(const NetworkEvent& delivery) noexcept {
    const std::size_t bytes = delivery.bytes.size();
    const auto found = links.find({delivery.from, delivery.to});
    if (found != links.end()) {
      found->second.queued_bytes =
          bytes > found->second.queued_bytes
              ? 0
              : found->second.queued_bytes - bytes;
    }
    total_network_bytes =
        bytes > total_network_bytes ? 0 : total_network_bytes - bytes;
  }

  SimulatorConfig config;
  EventQueue events;
  std::map<NodeId, NodeState> nodes;
  std::map<LinkKey, LinkState> links;
  std::size_t total_network_bytes{};
  std::uint64_t next_delivery_id{1};
  StorageOpId next_storage_id{1};
  std::uint64_t next_trace_sequence{1};
  std::vector<TraceRecord> trace_records;
  bool trace_was_truncated{};
};

Simulator::Simulator(SimulatorConfig config)
    : impl_(std::make_unique<Impl>(std::move(config))) {}

Simulator::~Simulator() = default;
Simulator::Simulator(Simulator&&) noexcept = default;
Simulator& Simulator::operator=(Simulator&&) noexcept = default;

bool Simulator::register_node(NodeId node) {
  const auto [position, inserted] =
      impl_->nodes.emplace(node, Impl::NodeState{});
  if (inserted) {
    position->second.storage.available_at = impl_->events.now();
    impl_->record(TraceKind::node_registered, node, 0, 0,
                  position->second.incarnation);
  }
  return inserted;
}

bool Simulator::is_alive(NodeId node) const noexcept {
  const auto* state = impl_->node(node);
  return state != nullptr && state->alive;
}

std::uint64_t Simulator::incarnation(NodeId node) const noexcept {
  const auto* state = impl_->node(node);
  return state == nullptr ? 0 : state->incarnation;
}

bool Simulator::crash_node(NodeId node, StorageCrashSpec crash) {
  auto* state = impl_->node(node);
  if (state == nullptr || !state->alive) {
    return false;
  }

  auto& storage = state->storage;
  const std::size_t unstable_survivors =
      std::min(crash.surviving_unstable_bytes, storage.unstable.size());
  storage.stable.insert(storage.stable.end(), storage.unstable.begin(),
                        storage.unstable.begin() +
                            static_cast<std::ptrdiff_t>(unstable_survivors));

  std::size_t torn_survivors{};
  // A later in-flight write begins after the complete unstable tail. If only
  // a prefix of that tail survives, moving bytes from the later write left
  // across the lost gap would invent a compacted file image that no ordered
  // device could produce.
  if (unstable_survivors == storage.unstable.size() &&
      !storage.pending.empty()) {
    const auto& active = storage.pending.begin()->second;
    if (active.kind == StorageOperationKind::write) {
      torn_survivors =
          std::min(crash.torn_pending_write_bytes, active.bytes.size());
      storage.stable.insert(
          storage.stable.end(), active.bytes.begin(),
          active.bytes.begin() + static_cast<std::ptrdiff_t>(torn_survivors));
    }
  }

  storage.unstable.clear();
  storage.pending.clear();
  storage.pending_bytes = 0;
  storage.available_at = impl_->events.now();
  storage.fail_next_write = false;
  storage.short_write_prefix.reset();
  storage.fail_next_flush = false;
  state->alive = false;

  auto cancelled = impl_->events.extract_if(
      [&](const ScheduledEvent& event) {
        if (const auto* network = std::get_if<NetworkEvent>(&event.payload)) {
          return network->to == node ||
                 (impl_->config.transport.drop_in_flight_on_endpoint_crash &&
                  network->from == node);
        }
        if (const auto* timer = std::get_if<TimerEvent>(&event.payload)) {
          return timer->node == node;
        }
        if (const auto* pending_storage =
                std::get_if<StorageEvent>(&event.payload)) {
          return pending_storage->node == node;
        }
        const auto* user = std::get_if<UserEvent>(&event.payload);
        return user != nullptr && user->node == node;
      });
  for (const auto& event : cancelled) {
    if (const auto* network = std::get_if<NetworkEvent>(&event.payload)) {
      impl_->release_network_bytes(*network);
      impl_->record(TraceKind::stale_event, network->from, network->to,
                    network->delivery_id, network->to_incarnation,
                    network->bytes.size(), "network_cancelled_by_crash");
    } else {
      impl_->record(TraceKind::stale_event, node, 0, event.sequence,
                    state->incarnation, 0, "event_cancelled_by_crash");
    }
  }
  impl_->record(TraceKind::storage_crashed, node, 0, 0,
                state->incarnation, unstable_survivors + torn_survivors,
                "tail_survived");
  impl_->record(TraceKind::node_crashed, node, 0, 0, state->incarnation);
  return true;
}

bool Simulator::restart_node(NodeId node) {
  auto* state = impl_->node(node);
  if (state == nullptr || state->alive ||
      state->incarnation == std::numeric_limits<std::uint64_t>::max()) {
    return false;
  }
  ++state->incarnation;
  state->alive = true;
  state->election_generation = 0;
  state->heartbeat_generation = 0;
  state->storage.available_at = impl_->events.now();
  impl_->record(TraceKind::node_restarted, node, 0, 0,
                state->incarnation);
  return true;
}

std::optional<TimerToken> Simulator::arm_timer(NodeId node, TimerKind kind,
                                               Tick delay) {
  auto* state = impl_->node(node);
  if (state == nullptr || !state->alive) {
    return std::nullopt;
  }
  auto& generation = impl_->timer_generation(*state, kind);
  std::uint64_t candidate_generation = generation;
  advance_generation(candidate_generation);
  TimerEvent event{node, kind, candidate_generation, state->incarnation};
  auto scheduled = impl_->events.schedule_after(delay, EventPayload{event});
  if (!scheduled) {
    return std::nullopt;
  }
  generation = candidate_generation;
  impl_->record(TraceKind::timer_armed, node, 0, generation,
                state->incarnation, 0,
                kind == TimerKind::heartbeat ? "heartbeat" : "election");
  return TimerToken{node, kind, generation, state->incarnation,
                    scheduled.sequence};
}

bool Simulator::cancel_timer(NodeId node, TimerKind kind) {
  auto* state = impl_->node(node);
  if (state == nullptr || !state->alive) {
    return false;
  }
  auto& generation = impl_->timer_generation(*state, kind);
  advance_generation(generation);
  impl_->record(TraceKind::timer_cancelled, node, 0, generation,
                state->incarnation, 0,
                kind == TimerKind::heartbeat ? "heartbeat" : "election");
  return true;
}

bool Simulator::set_partition(NodeId from, NodeId to, bool partitioned) {
  if (impl_->node(from) == nullptr || impl_->node(to) == nullptr) {
    return false;
  }
  auto& fault = impl_->link(from, to).fault;
  fault.partitioned = partitioned;
  impl_->record(TraceKind::link_fault, from, to, partitioned ? 1U : 0U,
                incarnation(from), 0, "partition");
  return true;
}

bool Simulator::set_link_delay(NodeId from, NodeId to, Tick delay) {
  if (impl_->node(from) == nullptr || impl_->node(to) == nullptr) {
    return false;
  }
  impl_->link(from, to).fault.additional_delay = delay;
  impl_->record(TraceKind::link_fault, from, to, delay, incarnation(from), 0,
                "delay");
  return true;
}

bool Simulator::drop_next(NodeId from, NodeId to, std::uint32_t count) {
  if (impl_->node(from) == nullptr || impl_->node(to) == nullptr) {
    return false;
  }
  impl_->link(from, to).fault.drop_next = count;
  impl_->record(TraceKind::link_fault, from, to, count, incarnation(from), 0,
                "drop_next");
  return true;
}

bool Simulator::duplicate_next(NodeId from, NodeId to,
                               std::uint32_t extra_copies) {
  if (impl_->node(from) == nullptr || impl_->node(to) == nullptr ||
      extra_copies >
          impl_->config.transport.max_extra_duplicates_per_send) {
    return false;
  }
  impl_->link(from, to).fault.extra_copies_next = extra_copies;
  impl_->record(TraceKind::link_fault, from, to, extra_copies,
                incarnation(from), 0, "duplicate_next");
  return true;
}

LinkFaultState Simulator::link_fault(NodeId from, NodeId to) const {
  const auto* link = impl_->find_link(from, to);
  return link == nullptr ? LinkFaultState{} : link->fault;
}

SendResult Simulator::send(NodeId from, NodeId to,
                           std::span<const std::byte> bytes,
                           Tick base_delay) {
  auto* source = impl_->node(from);
  auto* target = impl_->node(to);
  if (source == nullptr || target == nullptr) {
    return {SendStatus::unknown_node, 0, 0};
  }
  if (!source->alive || !target->alive) {
    return {SendStatus::endpoint_down, 0, 0};
  }
  if (bytes.size() > impl_->config.transport.max_frame_bytes) {
    return {SendStatus::frame_limit, 0, 0};
  }

  auto& link = impl_->link(from, to);
  if (link.fault.partitioned) {
    impl_->record(TraceKind::network_dropped, from, to, 0,
                  source->incarnation, bytes.size(), "partition");
    return {SendStatus::partitioned, 0, 0};
  }
  if (link.fault.drop_next != 0) {
    --link.fault.drop_next;
    impl_->record(TraceKind::network_dropped, from, to, 0,
                  source->incarnation, bytes.size(), "drop_next");
    return {SendStatus::dropped, 0, 0};
  }
  const std::uint32_t extras = link.fault.extra_copies_next;
  if (extras > impl_->config.transport.max_extra_duplicates_per_send) {
    return {SendStatus::duplicate_limit, 0, 0};
  }
  const std::size_t copies = static_cast<std::size_t>(extras) + 1U;
  if (size_multiply_overflows(bytes.size(), copies)) {
    return {SendStatus::queue_limit, 0, 0};
  }
  const std::size_t queued_bytes = bytes.size() * copies;

  if (add_overflows(impl_->events.now(), base_delay) ||
      add_overflows(impl_->events.now() + base_delay,
                    link.fault.additional_delay)) {
    return {SendStatus::time_overflow, 0, 0};
  }
  const Tick delivery_time =
      impl_->events.now() + base_delay + link.fault.additional_delay;

  if (queued_bytes > impl_->config.transport.max_queued_bytes_per_link ||
      link.queued_bytes >
          impl_->config.transport.max_queued_bytes_per_link - queued_bytes ||
      queued_bytes > impl_->config.transport.max_total_queued_bytes ||
      impl_->total_network_bytes >
          impl_->config.transport.max_total_queued_bytes - queued_bytes) {
    impl_->record(TraceKind::network_saturated, from, to, 0,
                  source->incarnation, queued_bytes, "byte_limit");
    return {SendStatus::queue_limit, 0, 0};
  }
  if (!impl_->events.can_schedule(copies)) {
    impl_->record(TraceKind::network_saturated, from, to, 0,
                  source->incarnation, queued_bytes, "event_limit");
    return {SendStatus::event_limit, 0, 0};
  }
  if (impl_->next_delivery_id == 0) {
    return {SendStatus::event_limit, 0, 0};
  }

  const std::uint64_t delivery_id = impl_->next_delivery_id;
  if (impl_->next_delivery_id ==
      std::numeric_limits<std::uint64_t>::max()) {
    impl_->next_delivery_id = 0;
  } else {
    ++impl_->next_delivery_id;
  }

  for (std::size_t copy = 0; copy < copies; ++copy) {
    NetworkEvent delivery;
    delivery.delivery_id = delivery_id;
    delivery.from = from;
    delivery.to = to;
    delivery.from_incarnation = source->incarnation;
    delivery.to_incarnation = target->incarnation;
    delivery.copy_index = static_cast<std::uint32_t>(copy);
    delivery.bytes.assign(bytes.begin(), bytes.end());
    const auto scheduled = impl_->events.schedule_at(
        delivery_time, EventPayload{std::move(delivery)});
    (void)scheduled;
  }
  link.fault.extra_copies_next = 0;
  link.queued_bytes += queued_bytes;
  impl_->total_network_bytes += queued_bytes;
  impl_->record(TraceKind::network_queued, from, to, delivery_id,
                source->incarnation, queued_bytes,
                extras == 0 ? "single" : "duplicated");
  return {SendStatus::queued, static_cast<std::uint32_t>(copies),
          queued_bytes};
}

StorageSubmitResult Simulator::submit_write(
    NodeId node, std::span<const std::byte> bytes, Tick latency) {
  auto* state = impl_->node(node);
  if (state == nullptr) {
    return {StorageSubmitStatus::unknown_node, 0, 0};
  }
  if (!state->alive) {
    return {StorageSubmitStatus::node_down, 0, 0};
  }
  auto& storage = state->storage;
  const auto& limits = impl_->config.storage;
  if (bytes.size() > limits.max_write_bytes) {
    return {StorageSubmitStatus::write_limit, 0, 0};
  }
  if (storage.pending.size() >= limits.max_pending_operations ||
      bytes.size() > limits.max_pending_bytes ||
      storage.pending_bytes > limits.max_pending_bytes - bytes.size()) {
    impl_->record(TraceKind::storage_saturated, node, 0, 0,
                  state->incarnation, bytes.size(), "pending_limit");
    return {StorageSubmitStatus::pending_limit, 0, 0};
  }
  if (size_add_overflows(storage.stable.size(), storage.unstable.size()) ||
      size_add_overflows(storage.stable.size() + storage.unstable.size(),
                         storage.pending_bytes) ||
      size_add_overflows(storage.stable.size() + storage.unstable.size() +
                             storage.pending_bytes,
                         bytes.size()) ||
      storage.stable.size() + storage.unstable.size() +
              storage.pending_bytes + bytes.size() >
          limits.max_file_bytes) {
    impl_->record(TraceKind::storage_saturated, node, 0, 0,
                  state->incarnation, bytes.size(), "file_limit");
    return {StorageSubmitStatus::file_limit, 0, 0};
  }
  const Tick start = impl_->storage_start(storage);
  if (add_overflows(start, latency)) {
    return {StorageSubmitStatus::time_overflow, 0, 0};
  }
  if (!impl_->events.can_schedule()) {
    return {StorageSubmitStatus::event_limit, 0, 0};
  }
  if (impl_->next_storage_id == 0) {
    return {StorageSubmitStatus::id_exhausted, 0, 0};
  }

  const Tick completion = start + latency;
  StorageCompletionStatus completion_status =
      StorageCompletionStatus::success;
  std::size_t actual_write_bytes = bytes.size();
  if (storage.fail_next_write) {
    completion_status = StorageCompletionStatus::io_error;
    actual_write_bytes = 0;
  } else if (storage.short_write_prefix) {
    completion_status = StorageCompletionStatus::short_write;
    actual_write_bytes = bytes.empty()
                             ? 0
                             : std::min(*storage.short_write_prefix,
                                        bytes.size() - 1U);
  }
  const StorageOpId operation_id = impl_->next_storage_id;
  if (impl_->next_storage_id ==
      std::numeric_limits<StorageOpId>::max()) {
    impl_->next_storage_id = 0;
  } else {
    ++impl_->next_storage_id;
  }
  StorageEvent event{node, operation_id, StorageOperationKind::write,
                     state->incarnation, actual_write_bytes,
                     completion_status};
  const auto scheduled =
      impl_->events.schedule_at(completion, EventPayload{event});
  if (!scheduled) {
    return {StorageSubmitStatus::event_limit, 0, 0};
  }
  Impl::PendingStorageOperation pending;
  pending.kind = StorageOperationKind::write;
  pending.bytes.assign(bytes.begin(), bytes.end());
  pending.completion_status = completion_status;
  pending.actual_write_bytes = actual_write_bytes;
  pending.completion_time = completion;
  pending.event_sequence = scheduled.sequence;
  storage.pending.emplace(operation_id, std::move(pending));
  storage.fail_next_write = false;
  storage.short_write_prefix.reset();
  storage.pending_bytes += bytes.size();
  storage.available_at = completion;
  impl_->record(TraceKind::storage_queued, node, 0, operation_id,
                state->incarnation, bytes.size(), "write");
  return {StorageSubmitStatus::queued, operation_id, completion};
}

StorageSubmitResult Simulator::submit_flush(NodeId node, Tick latency) {
  auto* state = impl_->node(node);
  if (state == nullptr) {
    return {StorageSubmitStatus::unknown_node, 0, 0};
  }
  if (!state->alive) {
    return {StorageSubmitStatus::node_down, 0, 0};
  }
  auto& storage = state->storage;
  if (storage.pending.size() >=
      impl_->config.storage.max_pending_operations) {
    impl_->record(TraceKind::storage_saturated, node, 0, 0,
                  state->incarnation, 0, "pending_limit");
    return {StorageSubmitStatus::pending_limit, 0, 0};
  }
  const Tick start = impl_->storage_start(storage);
  if (add_overflows(start, latency)) {
    return {StorageSubmitStatus::time_overflow, 0, 0};
  }
  if (!impl_->events.can_schedule()) {
    return {StorageSubmitStatus::event_limit, 0, 0};
  }
  if (impl_->next_storage_id == 0) {
    return {StorageSubmitStatus::id_exhausted, 0, 0};
  }

  const Tick completion = start + latency;
  const StorageCompletionStatus completion_status =
      storage.fail_next_flush ? StorageCompletionStatus::io_error
                              : StorageCompletionStatus::success;
  const StorageOpId operation_id = impl_->next_storage_id;
  if (impl_->next_storage_id ==
      std::numeric_limits<StorageOpId>::max()) {
    impl_->next_storage_id = 0;
  } else {
    ++impl_->next_storage_id;
  }
  StorageEvent event{node, operation_id, StorageOperationKind::flush,
                     state->incarnation, 0, completion_status};
  const auto scheduled =
      impl_->events.schedule_at(completion, EventPayload{event});
  if (!scheduled) {
    return {StorageSubmitStatus::event_limit, 0, 0};
  }
  Impl::PendingStorageOperation pending;
  pending.kind = StorageOperationKind::flush;
  pending.completion_status = completion_status;
  pending.completion_time = completion;
  pending.event_sequence = scheduled.sequence;
  storage.pending.emplace(operation_id, std::move(pending));
  storage.fail_next_flush = false;
  storage.available_at = completion;
  impl_->record(TraceKind::storage_queued, node, 0, operation_id,
                state->incarnation, 0, "flush");
  return {StorageSubmitStatus::queued, operation_id, completion};
}

bool Simulator::fail_next_write(NodeId node) {
  auto* state = impl_->node(node);
  if (state == nullptr || !state->alive) {
    return false;
  }
  state->storage.fail_next_write = true;
  state->storage.short_write_prefix.reset();
  impl_->record(TraceKind::storage_fault, node, 0, 0,
                state->incarnation, 0, "fail_next_write");
  return true;
}

bool Simulator::short_write_next(NodeId node, std::size_t prefix_bytes) {
  auto* state = impl_->node(node);
  if (state == nullptr || !state->alive) {
    return false;
  }
  state->storage.fail_next_write = false;
  state->storage.short_write_prefix = prefix_bytes;
  impl_->record(TraceKind::storage_fault, node, 0, 0,
                state->incarnation, prefix_bytes, "short_write_next");
  return true;
}

bool Simulator::fail_next_flush(NodeId node) {
  auto* state = impl_->node(node);
  if (state == nullptr || !state->alive) {
    return false;
  }
  state->storage.fail_next_flush = true;
  impl_->record(TraceKind::storage_fault, node, 0, 0,
                state->incarnation, 0, "fail_next_flush");
  return true;
}

bool Simulator::initialize_storage(
    NodeId node, std::span<const std::byte> stable_bytes) {
  auto* state = impl_->node(node);
  if (state == nullptr || !state->alive ||
      stable_bytes.size() > impl_->config.storage.max_file_bytes) {
    return false;
  }
  auto& storage = state->storage;
  if (!storage.stable.empty() || !storage.unstable.empty() ||
      !storage.pending.empty()) {
    return false;
  }
  storage.stable.assign(stable_bytes.begin(), stable_bytes.end());
  storage.available_at = impl_->events.now();
  impl_->record(TraceKind::storage_initialized, node, 0, 0,
                state->incarnation, stable_bytes.size(), "durable_header");
  return true;
}

bool Simulator::truncate_stable_storage(NodeId node,
                                        std::size_t valid_bytes) {
  auto* state = impl_->node(node);
  if (state == nullptr || state->alive) {
    return false;
  }
  auto& storage = state->storage;
  if (!storage.pending.empty() || !storage.unstable.empty() ||
      valid_bytes > storage.stable.size()) {
    return false;
  }
  const std::size_t removed = storage.stable.size() - valid_bytes;
  storage.stable.resize(valid_bytes);
  impl_->record(TraceKind::storage_repaired, node, 0, 0,
                state->incarnation, removed, "truncate_incomplete_tail");
  return true;
}

ScheduleResult Simulator::schedule_user(NodeId node, std::uint64_t tag,
                                        std::span<const std::byte> bytes,
                                        Tick delay) {
  auto* state = impl_->node(node);
  if (state == nullptr) {
    return {ScheduleStatus::unknown_node, 0};
  }
  if (!state->alive) {
    return {ScheduleStatus::node_down, 0};
  }
  if (bytes.size() > impl_->config.transport.max_frame_bytes) {
    return {ScheduleStatus::payload_limit, 0};
  }
  UserEvent event;
  event.node = node;
  event.tag = tag;
  event.incarnation = state->incarnation;
  event.bytes.assign(bytes.begin(), bytes.end());
  auto result =
      impl_->events.schedule_after(delay, EventPayload{std::move(event)});
  if (result) {
    impl_->record(TraceKind::user_scheduled, node, 0, tag,
                  state->incarnation, bytes.size());
  }
  return result;
}

std::optional<ScheduledEvent> Simulator::next() {
  while (auto scheduled = impl_->events.pop_next()) {
    if (auto* timer = std::get_if<TimerEvent>(&scheduled->payload)) {
      auto* state = impl_->node(timer->node);
      const bool valid = state != nullptr && state->alive &&
                         state->incarnation == timer->incarnation &&
                         impl_->timer_generation(*state, timer->kind) ==
                             timer->generation;
      if (!valid) {
        impl_->record(TraceKind::stale_event, timer->node, 0,
                      timer->generation, timer->incarnation, 0, "timer");
        continue;
      }
      impl_->record(TraceKind::timer_fired, timer->node, 0,
                    timer->generation, timer->incarnation, 0,
                    timer->kind == TimerKind::heartbeat ? "heartbeat"
                                                        : "election");
      return scheduled;
    }

    if (auto* delivery = std::get_if<NetworkEvent>(&scheduled->payload)) {
      impl_->release_network_bytes(*delivery);
      const auto* source = impl_->node(delivery->from);
      const auto* target = impl_->node(delivery->to);
      const auto* link = impl_->find_link(delivery->from, delivery->to);
      if (link != nullptr && link->fault.partitioned) {
        impl_->record(TraceKind::network_dropped, delivery->from,
                      delivery->to, delivery->delivery_id,
                      delivery->from_incarnation, delivery->bytes.size(),
                      "partition_at_delivery");
        continue;
      }
      const bool target_valid =
          target != nullptr && target->alive &&
          target->incarnation == delivery->to_incarnation;
      const bool source_valid =
          source != nullptr && source->alive &&
          source->incarnation == delivery->from_incarnation;
      if (!target_valid ||
          (impl_->config.transport.drop_in_flight_on_endpoint_crash &&
           !source_valid)) {
        impl_->record(TraceKind::stale_event, delivery->from, delivery->to,
                      delivery->delivery_id, delivery->to_incarnation,
                      delivery->bytes.size(), "network");
        continue;
      }
      impl_->record(TraceKind::network_delivered, delivery->from,
                    delivery->to, delivery->delivery_id,
                    delivery->to_incarnation, delivery->bytes.size(),
                    delivery->copy_index == 0 ? "original" : "duplicate");
      return scheduled;
    }

    if (auto* storage_event =
            std::get_if<StorageEvent>(&scheduled->payload)) {
      auto* state = impl_->node(storage_event->node);
      if (state == nullptr || !state->alive ||
          state->incarnation != storage_event->incarnation) {
        impl_->record(TraceKind::stale_event, storage_event->node, 0,
                      storage_event->operation_id,
                      storage_event->incarnation, 0, "storage");
        continue;
      }
      auto& device = state->storage;
      const auto found = device.pending.find(storage_event->operation_id);
      if (found == device.pending.end()) {
        impl_->record(TraceKind::stale_event, storage_event->node, 0,
                      storage_event->operation_id,
                      storage_event->incarnation, 0, "storage_cancelled");
        continue;
      }
      storage_event->status = found->second.completion_status;
      if (found->second.kind == StorageOperationKind::write) {
        storage_event->byte_count = found->second.actual_write_bytes;
        device.unstable.insert(
            device.unstable.end(), found->second.bytes.begin(),
            found->second.bytes.begin() + static_cast<std::ptrdiff_t>(
                                              found->second.actual_write_bytes));
        device.pending_bytes -= found->second.bytes.size();
      } else if (found->second.completion_status ==
                 StorageCompletionStatus::success) {
        storage_event->byte_count = device.unstable.size();
        device.stable.insert(device.stable.end(), device.unstable.begin(),
                             device.unstable.end());
        device.unstable.clear();
      } else {
        storage_event->byte_count = 0;
      }
      const auto detail = to_string(found->second.completion_status);
      device.pending.erase(found);
      if (device.pending.empty()) {
        device.available_at = impl_->events.now();
      }
      impl_->record(TraceKind::storage_completed, storage_event->node, 0,
                    storage_event->operation_id,
                    storage_event->incarnation, storage_event->byte_count,
                    detail);
      return scheduled;
    }

    auto* user = std::get_if<UserEvent>(&scheduled->payload);
    if (user != nullptr) {
      const auto* state = impl_->node(user->node);
      if (state == nullptr || !state->alive ||
          state->incarnation != user->incarnation) {
        impl_->record(TraceKind::stale_event, user->node, 0, user->tag,
                      user->incarnation, user->bytes.size(), "user");
        continue;
      }
      impl_->record(TraceKind::user_delivered, user->node, 0, user->tag,
                    user->incarnation, user->bytes.size());
      return scheduled;
    }
  }
  return std::nullopt;
}

Tick Simulator::now() const noexcept { return impl_->events.now(); }

std::size_t Simulator::pending_events() const noexcept {
  return impl_->events.pending();
}

std::size_t Simulator::queued_network_bytes() const noexcept {
  return impl_->total_network_bytes;
}

std::span<const std::byte> Simulator::stable_bytes(NodeId node) const {
  const auto* state = impl_->node(node);
  return state == nullptr ? std::span<const std::byte>{}
                          : std::span<const std::byte>{state->storage.stable};
}

std::span<const std::byte> Simulator::unstable_bytes(NodeId node) const {
  const auto* state = impl_->node(node);
  return state == nullptr ? std::span<const std::byte>{}
                          : std::span<const std::byte>{state->storage.unstable};
}

std::vector<std::byte> Simulator::visible_bytes(NodeId node) const {
  const auto* state = impl_->node(node);
  if (state == nullptr) {
    return {};
  }
  std::vector<std::byte> bytes;
  bytes.reserve(state->storage.stable.size() +
                state->storage.unstable.size());
  bytes.insert(bytes.end(), state->storage.stable.begin(),
               state->storage.stable.end());
  bytes.insert(bytes.end(), state->storage.unstable.begin(),
               state->storage.unstable.end());
  return bytes;
}

std::size_t Simulator::pending_storage_bytes(NodeId node) const noexcept {
  const auto* state = impl_->node(node);
  return state == nullptr ? 0 : state->storage.pending_bytes;
}

const std::vector<TraceRecord>& Simulator::trace() const noexcept {
  return impl_->trace_records;
}

bool Simulator::trace_truncated() const noexcept {
  return impl_->trace_was_truncated;
}

std::string Simulator::trace_jsonl() const {
  return render_jsonl(impl_->trace_records, impl_->trace_was_truncated);
}

}  // namespace detlog::sim
