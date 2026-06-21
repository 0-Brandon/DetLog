#include "detlog/cluster.hpp"

#include <algorithm>
#include <array>
#include <charconv>
#include <deque>
#include <limits>
#include <map>
#include <set>
#include <stdexcept>
#include <tuple>
#include <utility>

namespace detlog {
namespace {

[[nodiscard]] std::uint64_t mix_seed(std::uint64_t value) noexcept {
  value += 0x9e3779b97f4a7c15ULL;
  value = (value ^ (value >> 30U)) * 0xbf58476d1ce4e5b9ULL;
  value = (value ^ (value >> 27U)) * 0x94d049bb133111ebULL;
  return value ^ (value >> 31U);
}

[[nodiscard]] bool tick_add_overflows(Tick lhs, Tick rhs) noexcept {
  return rhs > std::numeric_limits<Tick>::max() - lhs;
}

[[nodiscard]] std::string_view raft_trace_name(TraceKind kind) noexcept {
  switch (kind) {
    case TraceKind::role_change:
      return "role_change";
    case TraceKind::term_change:
      return "term_change";
    case TraceKind::vote:
      return "vote";
    case TraceKind::send:
      return "send";
    case TraceKind::receive:
      return "receive";
    case TraceKind::storage_begin:
      return "storage_begin";
    case TraceKind::storage_complete:
      return "storage_complete";
    case TraceKind::log_change:
      return "log_change";
    case TraceKind::commit:
      return "commit";
    case TraceKind::apply:
      return "apply";
    case TraceKind::client_reply:
      return "client_reply";
    case TraceKind::queue_saturated:
      return "queue_saturated";
    case TraceKind::fault:
      return "fault";
  }
  return "unknown";
}

[[nodiscard]] std::string_view send_status_name(sim::SendStatus status) {
  switch (status) {
    case sim::SendStatus::queued:
      return "queued";
    case sim::SendStatus::dropped:
      return "dropped";
    case sim::SendStatus::partitioned:
      return "partitioned";
    case sim::SendStatus::unknown_node:
      return "unknown_node";
    case sim::SendStatus::endpoint_down:
      return "endpoint_down";
    case sim::SendStatus::frame_limit:
      return "frame_limit";
    case sim::SendStatus::queue_limit:
      return "queue_limit";
    case sim::SendStatus::duplicate_limit:
      return "duplicate_limit";
    case sim::SendStatus::time_overflow:
      return "time_overflow";
    case sim::SendStatus::event_limit:
      return "event_limit";
  }
  return "unknown";
}

[[nodiscard]] std::string_view storage_status_name(
    sim::StorageSubmitStatus status) {
  switch (status) {
    case sim::StorageSubmitStatus::queued:
      return "queued";
    case sim::StorageSubmitStatus::unknown_node:
      return "unknown_node";
    case sim::StorageSubmitStatus::node_down:
      return "node_down";
    case sim::StorageSubmitStatus::write_limit:
      return "write_limit";
    case sim::StorageSubmitStatus::pending_limit:
      return "pending_limit";
    case sim::StorageSubmitStatus::file_limit:
      return "file_limit";
    case sim::StorageSubmitStatus::time_overflow:
      return "time_overflow";
    case sim::StorageSubmitStatus::event_limit:
      return "event_limit";
    case sim::StorageSubmitStatus::id_exhausted:
      return "id_exhausted";
  }
  return "unknown";
}

[[nodiscard]] RecoveredState recovered_from(const WalState& state) {
  return RecoveredState{state.hard_state, state.entries, state.commit_index};
}

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
  for (const char raw : value) {
    const auto byte = static_cast<unsigned char>(raw);
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
          output.push_back(kHex[byte & 0x0fU]);
        } else {
          output.push_back(static_cast<char>(byte));
        }
        break;
    }
  }
  output.push_back('"');
}

[[nodiscard]] std::optional<std::string> compare_applied_prefix(
    const RaftNode& node) {
  if (node.last_applied() > node.commit_index()) {
    return "lastApplied exceeds commitIndex";
  }
  if (node.last_applied() > node.log().size()) {
    return "lastApplied exceeds the local log";
  }

  constexpr std::uint64_t kFnvOffset = 14695981039346656037ULL;
  constexpr std::uint64_t kFnvPrime = 1099511628211ULL;
  auto digest = [&](const ClientCommand& command) {
    std::uint64_t hash = kFnvOffset;
    auto byte = [&](std::uint8_t value) {
      hash ^= value;
      hash *= kFnvPrime;
    };
    auto u64 = [&](std::uint64_t value) {
      for (unsigned shift = 0; shift < 64; shift += 8) {
        byte(static_cast<std::uint8_t>(value >> shift));
      }
    };
    auto string = [&](std::string_view value) {
      u64(static_cast<std::uint64_t>(value.size()));
      for (const char character : value) {
        byte(static_cast<std::uint8_t>(
            static_cast<unsigned char>(character)));
      }
    };
    byte(static_cast<std::uint8_t>(command.kind));
    string(command.key);
    string(command.value);
    return hash;
  };

  std::map<std::string, std::string, std::less<>> values;
  std::map<SessionId, SessionRecord> sessions;
  for (LogIndex index = 1; index <= node.last_applied(); ++index) {
    const auto& entry = node.log()[static_cast<std::size_t>(index - 1U)];
    if (!entry.command) {
      continue;
    }
    const auto& command = *entry.command;
    const std::uint64_t command_digest = digest(command);
    const auto existing = sessions.find(command.session);
    if (existing != sessions.end()) {
      if (command.sequence < existing->second.sequence) {
        continue;
      }
      if (command.sequence == existing->second.sequence) {
        if (command_digest != existing->second.command_digest) {
          continue;
        }
        continue;
      }
      if (existing->second.sequence ==
              std::numeric_limits<std::uint64_t>::max() ||
          command.sequence != existing->second.sequence + 1U) {
        continue;
      }
    } else if (command.sequence != 1) {
      continue;
    }

    std::string result;
    switch (command.kind) {
      case CommandKind::put:
        values[command.key] = command.value;
        result = command.value;
        break;
      case CommandKind::erase: {
        const auto value = values.find(command.key);
        if (value != values.end()) {
          result = value->second;
          values.erase(value);
        }
        break;
      }
      case CommandKind::get: {
        const auto value = values.find(command.key);
        if (value != values.end()) {
          result = value->second;
        }
        break;
      }
    }
    sessions[command.session] = SessionRecord{
        command.sequence, command_digest, ClientStatus::ok,
        std::move(result)};
  }
  if (values.size() != node.state_machine().key_count() ||
      sessions.size() != node.state_machine().session_count()) {
    return "state-machine cardinality differs from applied log prefix";
  }
  for (const auto& [session, expected] : sessions) {
    if (std::optional<SessionRecord>{expected} !=
        node.state_machine().session(session)) {
      return "state-machine session differs from applied log prefix";
    }
  }
  for (const auto& [key, expected] : values) {
    if (std::optional<std::string>{expected} !=
        node.state_machine().value_for_test(key)) {
      return "state-machine value differs from applied log prefix";
    }
  }
  return std::nullopt;
}

}  // namespace

ClusterConfig cluster_config(std::size_t node_count, std::uint64_t seed) {
  if (node_count != 3 && node_count != 5) {
    throw std::invalid_argument("cluster size must be three or five");
  }
  ClusterConfig config;
  config.members.clear();
  config.members.reserve(node_count);
  for (std::size_t index = 0; index < node_count; ++index) {
    config.members.push_back(static_cast<NodeId>(index + 1U));
  }
  config.seed = seed;
  return config;
}

std::string_view to_string(ClusterTraceSource source) noexcept {
  switch (source) {
    case ClusterTraceSource::adapter:
      return "adapter";
    case ClusterTraceSource::raft:
      return "raft";
    case ClusterTraceSource::simulator:
      return "simulator";
  }
  return "unknown";
}

std::string render_cluster_jsonl(
    std::span<const ClusterTraceRecord> records) {
  std::string output;
  output.reserve(records.size() * 192U);
  for (const auto& record : records) {
    output.append("{\"time\":");
    append_unsigned(output, record.time);
    output.append(",\"sequence\":");
    append_unsigned(output, record.sequence);
    output.append(",\"source\":");
    append_json_string(output, to_string(record.source));
    output.append(",\"kind\":");
    append_json_string(output, record.kind);
    output.append(",\"node\":");
    append_unsigned(output, record.node);
    output.append(",\"peer\":");
    append_unsigned(output, record.peer);
    output.append(",\"term\":");
    append_unsigned(output, record.term);
    output.append(",\"index\":");
    append_unsigned(output, record.index);
    output.append(",\"token\":");
    append_unsigned(output, record.token);
    output.append(",\"bytes\":");
    append_unsigned(output, record.bytes);
    output.append(",\"detail\":");
    append_json_string(output, record.detail);
    output.append("}\n");
  }
  return output;
}

InvariantResult check_log_matching(std::span<const NodeSnapshot> snapshots) {
  for (const auto& node : snapshots) {
    for (std::size_t offset = 0; offset < node.log.size(); ++offset) {
      if (node.log[offset].index != static_cast<LogIndex>(offset) + 1U) {
        return {false, "snapshot log is not consecutive on node " +
                           std::to_string(node.id)};
      }
    }
  }
  for (std::size_t first = 0; first < snapshots.size(); ++first) {
    for (std::size_t second = first + 1U; second < snapshots.size();
         ++second) {
      const auto& left = snapshots[first];
      const auto& right = snapshots[second];
      const std::size_t shared = std::min(left.log.size(), right.log.size());
      bool prefix_equal = true;
      for (std::size_t offset = 0; offset < shared; ++offset) {
        if (left.log[offset] != right.log[offset]) {
          prefix_equal = false;
        }
        if (left.log[offset].term == right.log[offset].term &&
            !prefix_equal) {
          return {false,
                  "Log Matching violated between nodes " +
                      std::to_string(left.id) + " and " +
                      std::to_string(right.id) + " at index " +
                      std::to_string(offset + 1U)};
        }
      }
    }
  }
  return {};
}

struct DeterministicCluster::Impl {
  struct TimerBinding {
    std::uint64_t simulator_generation{};
    std::uint64_t raft_generation{};
    std::uint64_t incarnation{};
  };

  struct NodeRuntime {
    NodeRuntime(RaftConfig node_config, WalIdentity node_identity,
                std::uint64_t random_seed, std::uint64_t random_stream)
        : raft_config(std::move(node_config)),
          identity(node_identity),
          election_random(random_seed, random_stream) {}

    RaftConfig raft_config;
    WalIdentity identity;
    WalState wal_state;
    std::unique_ptr<RaftNode> raft;
    sim::Pcg32 election_random;
    std::optional<TimerBinding> election_timer;
    std::optional<TimerBinding> heartbeat_timer;
  };

  enum class PersistStage : std::uint8_t { write, flush };

  struct PendingPersist {
    NodeId node{};
    std::uint64_t incarnation{};
    StorageOpId raft_operation{};
    std::uint64_t expected_frame_sequence{};
    PersistStage stage{PersistStage::write};
  };

  explicit Impl(ClusterConfig initial_config)
      : config(std::move(initial_config)), simulator(config.simulator) {}

  void initialize();
  void sync_simulator_trace();
  void record(ClusterTraceSource source, std::string_view kind,
              NodeId node = 0, NodeId peer = 0, Term term = 0,
              LogIndex index = 0, std::uint64_t token = 0,
              std::size_t bytes = 0, std::string_view detail = {});
  void retain_reply(ClientReply reply, NodeId node);
  void register_vote(NodeId voter, Term term, NodeId candidate);
  void register_leader(NodeId leader, Term term);
  [[nodiscard]] NodeRuntime* runtime(NodeId node) noexcept;
  [[nodiscard]] const NodeRuntime* runtime(NodeId node) const noexcept;
  [[nodiscard]] Term term_for(NodeId node) const noexcept;
  [[nodiscard]] std::optional<TimerBinding>& timer_binding(
      NodeRuntime& runtime, TimerKind kind) noexcept;

  void enqueue_effects(NodeId node, std::vector<RaftEffect> effects);
  void drain_effects();
  void deliver(NodeId node, RaftEvent event);
  void handle_effect(NodeId node, RaftEffect effect);
  void handle_persist(NodeId node, const PersistEffect& effect);
  void handle_send(NodeId node, const SendEffect& effect);
  void handle_timer(NodeId node, const ScheduleTimerEffect& effect);
  void handle_storage_event(const sim::StorageEvent& event);
  void fail_storage(NodeId node, StorageOpId operation,
                    std::string_view reason);
  [[nodiscard]] bool recover_node(NodeRuntime& runtime,
                                  bool repair_incomplete_tail);

  [[nodiscard]] InvariantResult inspect_invariants();
  void verify_if_enabled();

  ClusterConfig config;
  sim::Simulator simulator;
  std::map<NodeId, NodeRuntime> nodes;
  std::map<StorageOpId, PendingPersist> pending_persists;
  std::deque<std::pair<NodeId, RaftEffect>> effect_queue;
  bool draining_effects{};
  ClientToken next_client_token{1};
  std::vector<ClientReply> client_replies;
  std::map<LogIndex, LogEntry> committed_ghost;
  std::map<LogIndex, Term> commit_certificate_terms;
  std::map<Term, NodeId> elected_leaders;
  std::map<std::pair<NodeId, Term>, NodeId> durable_votes;
  std::map<NodeId, LogIndex> observed_commit_indexes;
  std::map<NodeId, Term> observed_durable_terms;
  std::optional<std::string> observer_failure;
  std::optional<std::string> adapter_failure;
  InvariantResult last_invariant;
  std::vector<ClusterTraceRecord> semantic_trace;
  bool semantic_trace_truncated{};
  bool reply_retention_saturated{};
  std::size_t simulator_trace_offset{};
  std::uint64_t next_trace_sequence{1};
};

void DeterministicCluster::Impl::initialize() {
  std::sort(config.members.begin(), config.members.end());
  if ((config.members.size() != 3 && config.members.size() != 5) ||
      std::adjacent_find(config.members.begin(), config.members.end()) !=
          config.members.end()) {
    throw std::invalid_argument(
        "cluster membership must contain three or five unique node IDs");
  }
  if (config.election_jitter >
      std::numeric_limits<Tick>::max() - config.raft.election_timeout) {
    throw std::invalid_argument("election timeout plus jitter overflows");
  }
  if (config.raft.max_append_entries_per_rpc >
      config.codec.max_entries_per_append) {
    throw std::invalid_argument(
        "wire entry bound is smaller than the Raft append bound");
  }
  if (config.raft.max_append_entries_per_rpc >
      config.wal.max_entries_per_frame) {
    throw std::invalid_argument(
        "WAL entry bound is smaller than the Raft append bound");
  }
  if (config.raft.max_key_bytes > config.codec.max_key_bytes ||
      config.raft.max_value_bytes > config.codec.max_value_bytes ||
      config.raft.max_key_bytes > config.wal.max_key_bytes ||
      config.raft.max_value_bytes > config.wal.max_value_bytes ||
      config.raft.max_append_bytes_per_rpc > config.codec.max_frame_bytes ||
      config.raft.max_append_bytes_per_rpc > config.wal.max_frame_bytes) {
    throw std::invalid_argument(
        "Raft payload bounds exceed codec or WAL bounds");
  }
  constexpr std::size_t kWalFrameMargin = 256;
  const std::size_t max_raft_frame = config.raft.max_append_bytes_per_rpc;
  if (max_raft_frame > config.simulator.transport.max_frame_bytes ||
      max_raft_frame >
          config.simulator.transport.max_queued_bytes_per_link ||
      max_raft_frame >
          config.simulator.transport.max_total_queued_bytes ||
      max_raft_frame >
          std::numeric_limits<std::size_t>::max() - kWalFrameMargin ||
      config.simulator.storage.max_write_bytes <
          max_raft_frame + kWalFrameMargin ||
      config.simulator.storage.max_pending_bytes <
          max_raft_frame + kWalFrameMargin ||
      config.simulator.storage.max_pending_operations == 0 ||
      config.simulator.storage.max_file_bytes <
          max_raft_frame + kWalFrameMargin ||
      config.simulator.events.max_pending_events < config.members.size() ||
      config.simulator.events.max_total_events < config.members.size()) {
    throw std::invalid_argument(
        "simulator bounds cannot carry one maximum Raft/WAL operation");
  }
  if (config.max_retained_client_replies == 0) {
    throw std::invalid_argument("client reply retention bound must be nonzero");
  }
  for (const NodeId id : config.members) {
    RaftConfig raft_config = config.raft;
    raft_config.node_id = id;
    raft_config.members = config.members;
    const WalIdentity identity{config.cluster_id_high, config.cluster_id_low,
                               id};
    const auto [position, inserted] = nodes.try_emplace(
        id, std::move(raft_config), identity,
        mix_seed(config.seed ^ static_cast<std::uint64_t>(id)),
        mix_seed(config.seed + static_cast<std::uint64_t>(id)));
    if (!inserted || !simulator.register_node(id)) {
      throw std::logic_error("failed to register cluster node");
    }
    sync_simulator_trace();

    auto header = encode_wal_file_header(identity);
    if (!simulator.initialize_storage(id, header)) {
      throw std::invalid_argument(
          "simulated storage cannot hold the WAL file header");
    }
    sync_simulator_trace();
    auto scanned = scan_wal_bytes(simulator.stable_bytes(id), identity,
                                  config.wal);
    position->second.wal_state = std::move(scanned.state);
    position->second.raft = std::make_unique<RaftNode>(
        position->second.raft_config,
        recovered_from(position->second.wal_state));
  }

  for (auto& [id, node] : nodes) {
    enqueue_effects(id, node.raft->start());
  }
  verify_if_enabled();
}

void DeterministicCluster::Impl::sync_simulator_trace() {
  const auto& source = simulator.trace();
  while (simulator_trace_offset < source.size()) {
    const auto& event = source[simulator_trace_offset++];
    record(ClusterTraceSource::simulator, sim::to_string(event.kind),
           event.node, event.peer, 0, 0, event.token, event.bytes,
           event.detail);
  }
}

void DeterministicCluster::Impl::record(
    ClusterTraceSource source, std::string_view kind, NodeId node_id,
    NodeId peer, Term term, LogIndex index, std::uint64_t token,
    std::size_t bytes, std::string_view detail) {
  if (semantic_trace_truncated) {
    return;
  }
  const std::size_t limit = config.max_semantic_trace_records;
  if (limit == 0) {
    semantic_trace_truncated = true;
    return;
  }
  if (semantic_trace.size() >= limit - 1U) {
    semantic_trace.push_back(ClusterTraceRecord{
        simulator.now(), next_trace_sequence++, ClusterTraceSource::adapter,
        "trace_saturated", 0, 0, 0, 0, 0, 0,
        "semantic trace retention bound reached"});
    semantic_trace_truncated = true;
    return;
  }
  semantic_trace.push_back(ClusterTraceRecord{
      simulator.now(), next_trace_sequence++, source, std::string(kind),
      node_id, peer, term, index, token, bytes, std::string(detail)});
}

void DeterministicCluster::Impl::retain_reply(ClientReply reply,
                                              NodeId node) {
  if (client_replies.size() >= config.max_retained_client_replies) {
    client_replies.erase(client_replies.begin());
    if (!reply_retention_saturated) {
      reply_retention_saturated = true;
      record(ClusterTraceSource::adapter, "reply_retention_saturated", node,
             0, term_for(node), 0, reply.token, 0,
             "oldest retained reply evicted");
    }
  }
  client_replies.push_back(std::move(reply));
}

void DeterministicCluster::Impl::register_vote(NodeId voter, Term term,
                                               NodeId candidate) {
  const auto [position, inserted] =
      durable_votes.emplace(std::make_pair(voter, term), candidate);
  if (!inserted && position->second != candidate && !observer_failure) {
    observer_failure = "node " + std::to_string(voter) +
                       " voted for two candidates in term " +
                       std::to_string(term);
  }
}

void DeterministicCluster::Impl::register_leader(NodeId leader, Term term) {
  const auto [position, inserted] = elected_leaders.emplace(term, leader);
  if (!inserted) {
    if (position->second != leader && !observer_failure) {
      observer_failure = "two leaders were observed in term " +
                         std::to_string(term);
    }
    return;
  }
  std::size_t votes{};
  for (const auto& [voter_term, candidate] : durable_votes) {
    if (voter_term.second == term && candidate == leader) {
      ++votes;
    }
  }
  const auto* node = runtime(leader);
  if (node != nullptr &&
      votes < node->raft_config.members.size() / 2U + 1U &&
      !observer_failure) {
    observer_failure = "leader lacks an observed durable vote certificate in term " +
                       std::to_string(term);
  }
  if (node != nullptr && node->raft && !observer_failure) {
    for (const auto& [index, committed] : committed_ghost) {
      if (node->raft->log().size() < index ||
          node->raft->log()[static_cast<std::size_t>(index - 1U)] !=
              committed) {
        observer_failure =
            "new leader is missing committed index " +
            std::to_string(index) + " in term " + std::to_string(term);
        break;
      }
    }
  }
}

DeterministicCluster::Impl::NodeRuntime*
DeterministicCluster::Impl::runtime(NodeId node) noexcept {
  const auto found = nodes.find(node);
  return found == nodes.end() ? nullptr : &found->second;
}

const DeterministicCluster::Impl::NodeRuntime*
DeterministicCluster::Impl::runtime(NodeId node) const noexcept {
  const auto found = nodes.find(node);
  return found == nodes.end() ? nullptr : &found->second;
}

Term DeterministicCluster::Impl::term_for(NodeId node) const noexcept {
  const auto* found = runtime(node);
  if (found == nullptr) {
    return 0;
  }
  return found->raft ? found->raft->current_term()
                     : found->wal_state.hard_state.current_term;
}

std::optional<DeterministicCluster::Impl::TimerBinding>&
DeterministicCluster::Impl::timer_binding(NodeRuntime& node,
                                          TimerKind kind) noexcept {
  return kind == TimerKind::heartbeat ? node.heartbeat_timer
                                      : node.election_timer;
}

void DeterministicCluster::Impl::enqueue_effects(
    NodeId node, std::vector<RaftEffect> effects) {
  for (auto& effect : effects) {
    effect_queue.emplace_back(node, std::move(effect));
  }
  if (!draining_effects) {
    drain_effects();
  }
}

void DeterministicCluster::Impl::drain_effects() {
  draining_effects = true;
  while (!effect_queue.empty()) {
    auto [node, effect] = std::move(effect_queue.front());
    effect_queue.pop_front();
    sync_simulator_trace();
    handle_effect(node, std::move(effect));
  }
  sync_simulator_trace();
  draining_effects = false;
}

void DeterministicCluster::Impl::deliver(NodeId node, RaftEvent event) {
  auto* found = runtime(node);
  if (found == nullptr || !found->raft || !simulator.is_alive(node)) {
    return;
  }
  enqueue_effects(node, found->raft->step(std::move(event)));
}

void DeterministicCluster::Impl::handle_effect(NodeId node,
                                               RaftEffect effect) {
  if (const auto* trace_effect = std::get_if<TraceEffect>(&effect)) {
    const auto& trace = trace_effect->record;
    if (trace.kind == TraceKind::role_change &&
        trace.detail == "became leader") {
      register_leader(trace.node, trace.term);
    }
    record(ClusterTraceSource::raft, raft_trace_name(trace.kind), trace.node,
           0, trace.term, trace.index, 0, 0, trace.detail);
    return;
  }
  if (const auto* reply = std::get_if<ClientReplyEffect>(&effect)) {
    retain_reply(reply->reply, node);
    record(ClusterTraceSource::adapter, "client_reply", node, 0,
           term_for(node), reply->reply.log_index, reply->reply.token, 0,
           std::to_string(static_cast<unsigned>(reply->reply.status)));
    return;
  }
  if (const auto* persist = std::get_if<PersistEffect>(&effect)) {
    handle_persist(node, *persist);
    return;
  }
  if (const auto* send = std::get_if<SendEffect>(&effect)) {
    handle_send(node, *send);
    return;
  }
  if (const auto* timer = std::get_if<ScheduleTimerEffect>(&effect)) {
    handle_timer(node, *timer);
  }
}

void DeterministicCluster::Impl::handle_persist(
    NodeId node, const PersistEffect& effect) {
  auto* found = runtime(node);
  if (found == nullptr || !found->raft || !simulator.is_alive(node)) {
    return;
  }

  try {
    auto encoded = encode_wal_frame(found->wal_state, effect.batch, config.wal);
    const auto submitted = simulator.submit_write(
        node, encoded.bytes, config.storage_write_latency);
    sync_simulator_trace();
    if (!submitted) {
      fail_storage(node, effect.op_id, storage_status_name(submitted.status));
      return;
    }
    pending_persists.emplace(
        submitted.operation_id,
        PendingPersist{node, simulator.incarnation(node), effect.op_id,
                       encoded.state_after.last_frame_sequence,
                       PersistStage::write});
    record(ClusterTraceSource::adapter, "persist_write_queued", node, 0,
           found->raft->current_term(), 0, effect.op_id,
           encoded.bytes.size());
  } catch (const WalError& error) {
    fail_storage(node, effect.op_id, error.what());
  } catch (const std::exception& error) {
    fail_storage(node, effect.op_id, error.what());
  }
}

void DeterministicCluster::Impl::handle_send(NodeId node,
                                             const SendEffect& effect) {
  if (const auto* request =
          std::get_if<RequestVote>(&effect.envelope.message)) {
    register_vote(effect.envelope.from, request->term,
                  request->candidate_id);
  } else if (const auto* response =
                 std::get_if<RequestVoteResponse>(
                     &effect.envelope.message);
             response != nullptr && response->vote_granted) {
    register_vote(effect.envelope.from, response->term,
                  effect.envelope.to);
  }
  const auto encoded = codec::encode_message(effect.envelope.message,
                                             config.codec);
  if (!encoded) {
    if (!adapter_failure) {
      adapter_failure = "core emitted an unencodable protocol message: " +
                        std::string(codec::to_string(encoded.error));
    }
    record(ClusterTraceSource::adapter, "codec_rejected", node,
           effect.envelope.to, term_for(node), 0, 0, 0,
           codec::to_string(encoded.error));
    return;
  }
  const auto sent = simulator.send(effect.envelope.from, effect.envelope.to,
                                   *encoded, config.network_delay);
  sync_simulator_trace();
  record(ClusterTraceSource::adapter, "transport_send", effect.envelope.from,
         effect.envelope.to, term_for(node), 0, 0, encoded->size(),
         send_status_name(sent.status));
}

void DeterministicCluster::Impl::handle_timer(
    NodeId node, const ScheduleTimerEffect& effect) {
  auto* found = runtime(node);
  if (found == nullptr || !found->raft || !simulator.is_alive(node)) {
    return;
  }
  Tick delay = effect.timer.delay;
  if (effect.timer.kind == TimerKind::election &&
      config.election_jitter != 0) {
    const Tick jitter = found->election_random.inclusive(
        0, config.election_jitter);
    if (tick_add_overflows(delay, jitter)) {
      record(ClusterTraceSource::adapter, "timer_rejected", node, 0,
             found->raft->current_term(), 0, effect.timer.generation, 0,
             "time_overflow");
      return;
    }
    delay += jitter;
  }
  const auto token = simulator.arm_timer(node, effect.timer.kind, delay);
  sync_simulator_trace();
  if (!token) {
    auto& previous = timer_binding(*found, effect.timer.kind);
    if (previous &&
        previous->incarnation == simulator.incarnation(node)) {
      previous->raft_generation = effect.timer.generation;
      record(ClusterTraceSource::adapter, "timer_rearm_coalesced", node, 0,
             found->raft->current_term(), 0, effect.timer.generation, 0,
             "existing simulator deadline retained");
    } else {
      if (!adapter_failure) {
        adapter_failure =
            "timer re-arm failed without an existing live timer";
      }
      record(ClusterTraceSource::adapter, "timer_rejected", node, 0,
             found->raft->current_term(), 0, effect.timer.generation, 0,
             "simulator_limit");
    }
    return;
  }
  timer_binding(*found, effect.timer.kind) = TimerBinding{
      token->generation, effect.timer.generation, token->incarnation};
  record(ClusterTraceSource::adapter, "timer_mapped", node, 0,
         found->raft->current_term(), 0, effect.timer.generation, 0,
         effect.timer.kind == TimerKind::heartbeat ? "heartbeat"
                                                   : "election");
}

void DeterministicCluster::Impl::fail_storage(
    NodeId node, StorageOpId operation, std::string_view reason) {
  record(ClusterTraceSource::adapter, "persist_failed", node, 0,
         term_for(node), 0, operation, 0, reason);
  deliver(node, StorageComplete{operation, false, std::string(reason)});
}

bool DeterministicCluster::Impl::recover_node(NodeRuntime& node,
                                              bool repair_incomplete_tail) {
  try {
    auto scanned = scan_wal_bytes(simulator.stable_bytes(node.identity.node_id),
                                  node.identity, config.wal);
    if (scanned.has_incomplete_tail) {
      if (!repair_incomplete_tail ||
          scanned.valid_bytes >
              std::numeric_limits<std::size_t>::max() ||
          !simulator.truncate_stable_storage(
              node.identity.node_id,
              static_cast<std::size_t>(scanned.valid_bytes))) {
        return false;
      }
      sync_simulator_trace();
      record(ClusterTraceSource::adapter, "wal_tail_repaired",
             node.identity.node_id, 0, scanned.state.hard_state.current_term,
             scanned.state.commit_index, 0,
             static_cast<std::size_t>(scanned.valid_bytes));
    }
    node.wal_state = std::move(scanned.state);
    return true;
  } catch (const WalError& error) {
    record(ClusterTraceSource::adapter, "wal_recovery_failed",
           node.identity.node_id, 0, node.wal_state.hard_state.current_term,
           node.wal_state.commit_index, 0, 0, error.what());
    return false;
  }
}

void DeterministicCluster::Impl::handle_storage_event(
    const sim::StorageEvent& event) {
  const auto found = pending_persists.find(event.operation_id);
  if (found == pending_persists.end()) {
    record(ClusterTraceSource::adapter, "orphan_storage_completion",
           event.node, 0, term_for(event.node), 0, event.operation_id,
           event.byte_count);
    return;
  }
  PendingPersist bridge = found->second;
  pending_persists.erase(found);
  if (bridge.node != event.node ||
      bridge.incarnation != simulator.incarnation(event.node)) {
    record(ClusterTraceSource::adapter, "stale_storage_bridge", event.node,
           0, term_for(event.node), 0, bridge.raft_operation,
           event.byte_count);
    return;
  }
  if (event.status != sim::StorageCompletionStatus::success) {
    fail_storage(event.node, bridge.raft_operation,
                 sim::to_string(event.status));
    return;
  }

  if (bridge.stage == PersistStage::write) {
    const auto flush =
        simulator.submit_flush(event.node, config.storage_flush_latency);
    sync_simulator_trace();
    if (!flush) {
      fail_storage(event.node, bridge.raft_operation,
                   storage_status_name(flush.status));
      return;
    }
    bridge.stage = PersistStage::flush;
    pending_persists.emplace(flush.operation_id, bridge);
    record(ClusterTraceSource::adapter, "persist_flush_queued", event.node,
           0, term_for(event.node), 0, bridge.raft_operation,
           event.byte_count);
    return;
  }

  auto* node = runtime(event.node);
  if (node == nullptr || !node->raft) {
    return;
  }
  try {
    auto scanned = scan_wal_bytes(simulator.stable_bytes(event.node),
                                  node->identity, config.wal);
    if (scanned.has_incomplete_tail ||
        scanned.state.last_frame_sequence !=
            bridge.expected_frame_sequence) {
      fail_storage(event.node, bridge.raft_operation,
                   "flush did not produce the expected complete WAL frame");
      return;
    }
    node->wal_state = std::move(scanned.state);
    record(ClusterTraceSource::adapter, "persist_durable", event.node, 0,
           node->wal_state.hard_state.current_term,
           node->wal_state.commit_index, bridge.raft_operation,
           event.byte_count);
    deliver(event.node,
            StorageComplete{bridge.raft_operation, true, std::string{}});
  } catch (const WalError& error) {
    fail_storage(event.node, bridge.raft_operation, error.what());
  }
}

InvariantResult DeterministicCluster::Impl::inspect_invariants() {
  auto fail = [](std::string detail) {
    return InvariantResult{false, std::move(detail)};
  };
  if (adapter_failure) {
    return fail(*adapter_failure);
  }
  if (observer_failure) {
    return fail(*observer_failure);
  }

  if (simulator.pending_events() > config.simulator.events.max_pending_events) {
    return fail("simulator pending-event bound exceeded");
  }
  if (simulator.queued_network_bytes() >
      config.simulator.transport.max_total_queued_bytes) {
    return fail("simulator network-byte bound exceeded");
  }
  if (semantic_trace.size() > config.max_semantic_trace_records) {
    return fail("cluster semantic-trace bound exceeded");
  }
  if (client_replies.size() > config.max_retained_client_replies) {
    return fail("cluster client-reply bound exceeded");
  }

  std::vector<NodeSnapshot> effective_snapshots;
  effective_snapshots.reserve(nodes.size());
  for (const auto& [id, node] : nodes) {
    if (simulator.pending_storage_bytes(id) >
        config.simulator.storage.max_pending_bytes) {
      return fail("simulator storage-byte bound exceeded on node " +
                  std::to_string(id));
    }
    if (node.wal_state.commit_index > node.wal_state.entries.size()) {
      return fail("durable commit index exceeds durable log on node " +
                  std::to_string(id));
    }
    const Term durable_term = node.wal_state.hard_state.current_term;
    const LogIndex durable_commit = node.wal_state.commit_index;
    if (durable_term < observed_durable_terms[id]) {
      return fail("durable term decreased on node " + std::to_string(id));
    }
    if (durable_commit < observed_commit_indexes[id]) {
      return fail("durable commit index decreased on node " +
                  std::to_string(id));
    }
    observed_durable_terms[id] = durable_term;
    observed_commit_indexes[id] = durable_commit;
    if (node.wal_state.hard_state.voted_for) {
      register_vote(id, node.wal_state.hard_state.current_term,
                    *node.wal_state.hard_state.voted_for);
    }

    if (!node.raft || !simulator.is_alive(id)) {
      effective_snapshots.push_back(NodeSnapshot{
          id,
          false,
          RaftRole::unavailable,
          node.wal_state.hard_state.current_term,
          false,
          static_cast<LogIndex>(node.wal_state.entries.size()),
          static_cast<LogIndex>(node.wal_state.entries.size()),
          node.wal_state.commit_index,
          0,
          0,
          node.wal_state.entries});
      continue;
    }
    const RaftNode& raft = *node.raft;
    if (raft.last_applied() > raft.commit_index()) {
      return fail("lastApplied exceeds commitIndex on node " +
                  std::to_string(id));
    }
    if (raft.deferred_event_count() >
        node.raft_config.max_deferred_events) {
      return fail("deferred event bound exceeded on node " +
                  std::to_string(id));
    }
    for (std::size_t offset = 0; offset < raft.log().size(); ++offset) {
      if (raft.log()[offset].index !=
          static_cast<LogIndex>(offset) + 1U) {
        return fail("volatile log is not consecutive on node " +
                    std::to_string(id));
      }
    }
    if (const auto mismatch = compare_applied_prefix(raft)) {
      return fail(*mismatch + " on node " + std::to_string(id));
    }
    effective_snapshots.push_back(NodeSnapshot{
        id,
        true,
        raft.role(),
        raft.current_term(),
        raft.leader_ready(),
        raft.last_log_index(),
        raft.durable_last_log_index(),
        raft.commit_index(),
        raft.last_applied(),
        raft.deferred_event_count(),
        raft.log()});
  }

  if (observer_failure) {
    return fail(*observer_failure);
  }
  const auto matching = check_log_matching(effective_snapshots);
  if (!matching) {
    return matching;
  }

  auto highest_certified = [&]() -> LogIndex {
    return committed_ghost.empty() ? 0 : committed_ghost.rbegin()->first;
  };

  // A leader is the only role allowed to introduce a newly committed prefix.
  // The target must be from its current term and already durable on a quorum;
  // preceding entries become committed only as a consequence of that target.
  for (const auto& [id, node] : nodes) {
    if (!node.raft || !simulator.is_alive(id) ||
        node.raft->role() != RaftRole::leader) {
      continue;
    }
    register_leader(id, node.raft->current_term());
    if (observer_failure) {
      return fail(*observer_failure);
    }
    const LogIndex target = node.raft->commit_index();
    if (target <= highest_certified()) {
      continue;
    }
    if (target == 0 || target > node.raft->log().size()) {
      return fail("leader commit target is outside its log");
    }
    const LogEntry& target_entry =
        node.raft->log()[static_cast<std::size_t>(target - 1U)];
    if (target_entry.term != node.raft->current_term()) {
      return fail("leader directly committed an entry from an older term");
    }
    std::size_t durable_replicas{};
    for (const auto& [replica_id, replica] : nodes) {
      static_cast<void>(replica_id);
      if (replica.wal_state.entries.size() >= target &&
          replica.wal_state.entries[
              static_cast<std::size_t>(target - 1U)] == target_entry) {
        ++durable_replicas;
      }
    }
    if (durable_replicas < node.raft->quorum_size()) {
      return fail("new commit lacks a durable quorum certificate at index " +
                  std::to_string(target));
    }
    for (LogIndex index = highest_certified() + 1U; index <= target;
         ++index) {
      const LogEntry& entry =
          node.raft->log()[static_cast<std::size_t>(index - 1U)];
      const auto [position, inserted] = committed_ghost.emplace(index, entry);
      if (!inserted && position->second != entry) {
        return fail("committed entry changed at index " +
                    std::to_string(index));
      }
      commit_certificate_terms.try_emplace(index,
                                           node.raft->current_term());
    }
  }

  const LogIndex certified = highest_certified();
  for (const auto& snapshot : effective_snapshots) {
    if (snapshot.commit_index > certified) {
      return fail("node exposed an uncertified commit index " +
                  std::to_string(snapshot.commit_index));
    }
    for (LogIndex index = 1; index <= snapshot.commit_index; ++index) {
      const auto ghost = committed_ghost.find(index);
      if (ghost == committed_ghost.end() ||
          snapshot.log[static_cast<std::size_t>(index - 1U)] !=
              ghost->second) {
        return fail("committed-prefix disagreement on node " +
                    std::to_string(snapshot.id) + " at index " +
                    std::to_string(index));
      }
    }
  }

  // Leader Completeness applies to leaders elected in or after the term that
  // supplied a commit certificate; an isolated older-term leader may still
  // believe it is leader until communication resumes.
  for (const auto& snapshot : effective_snapshots) {
    if (!snapshot.alive || snapshot.role != RaftRole::leader) {
      continue;
    }
    for (const auto& [index, committed] : committed_ghost) {
      const auto certificate = commit_certificate_terms.find(index);
      if (certificate != commit_certificate_terms.end() &&
          snapshot.term < certificate->second) {
        continue;
      }
      if (snapshot.log.size() < index ||
          snapshot.log[static_cast<std::size_t>(index - 1U)] != committed) {
        return fail("leader completeness violated by node " +
                    std::to_string(snapshot.id) + " at index " +
                    std::to_string(index));
      }
    }
  }
  return {};
}

void DeterministicCluster::Impl::verify_if_enabled() {
  last_invariant = inspect_invariants();
  if (config.automatic_invariant_checks && !last_invariant) {
    throw std::logic_error("cluster invariant failed: " +
                           last_invariant.detail);
  }
}

DeterministicCluster::DeterministicCluster(ClusterConfig config)
    : impl_(std::make_unique<Impl>(std::move(config))) {
  impl_->initialize();
}

DeterministicCluster::~DeterministicCluster() = default;
DeterministicCluster::DeterministicCluster(DeterministicCluster&&) noexcept =
    default;
DeterministicCluster& DeterministicCluster::operator=(
    DeterministicCluster&&) noexcept = default;

const ClusterConfig& DeterministicCluster::config() const noexcept {
  return impl_->config;
}

const std::vector<NodeId>& DeterministicCluster::members() const noexcept {
  return impl_->config.members;
}

Tick DeterministicCluster::now() const noexcept {
  return impl_->simulator.now();
}

ClusterQueueDepth DeterministicCluster::queue_depth() const noexcept {
  std::size_t storage_bytes = 0;
  for (const NodeId node : impl_->config.members) {
    const std::size_t pending = impl_->simulator.pending_storage_bytes(node);
    if (pending > std::numeric_limits<std::size_t>::max() - storage_bytes) {
      storage_bytes = std::numeric_limits<std::size_t>::max();
      break;
    }
    storage_bytes += pending;
  }
  return ClusterQueueDepth{impl_->simulator.pending_events(),
                           impl_->simulator.queued_network_bytes(),
                           storage_bytes};
}

std::optional<NodeSnapshot> DeterministicCluster::snapshot(NodeId node) const {
  const auto* found = impl_->runtime(node);
  if (found == nullptr) {
    return std::nullopt;
  }
  if (!found->raft || !impl_->simulator.is_alive(node)) {
    return NodeSnapshot{node,
                        false,
                        RaftRole::unavailable,
                        found->wal_state.hard_state.current_term,
                        false,
                        static_cast<LogIndex>(found->wal_state.entries.size()),
                        static_cast<LogIndex>(found->wal_state.entries.size()),
                        found->wal_state.commit_index,
                        0,
                        0,
                        found->wal_state.entries};
  }
  const auto& raft = *found->raft;
  return NodeSnapshot{node,
                      true,
                      raft.role(),
                      raft.current_term(),
                      raft.leader_ready(),
                      raft.last_log_index(),
                      raft.durable_last_log_index(),
                      raft.commit_index(),
                      raft.last_applied(),
                      raft.deferred_event_count(),
                      raft.log()};
}

std::vector<NodeSnapshot> DeterministicCluster::snapshots() const {
  std::vector<NodeSnapshot> result;
  result.reserve(impl_->config.members.size());
  for (const NodeId node : impl_->config.members) {
    if (auto value = snapshot(node)) {
      result.push_back(std::move(*value));
    }
  }
  return result;
}

std::vector<NodeId> DeterministicCluster::leaders(bool require_ready) const {
  std::vector<NodeId> result;
  for (const auto& [id, node] : impl_->nodes) {
    if (node.raft && impl_->simulator.is_alive(id) &&
        node.raft->role() == RaftRole::leader &&
        (!require_ready || node.raft->leader_ready())) {
      result.push_back(id);
    }
  }
  return result;
}

std::optional<NodeId> DeterministicCluster::leader(bool require_ready) const {
  const auto found = leaders(require_ready);
  return found.size() == 1 ? std::optional<NodeId>{found.front()}
                           : std::nullopt;
}

bool DeterministicCluster::step() {
  auto scheduled = impl_->simulator.next();
  impl_->sync_simulator_trace();
  if (!scheduled) {
    return false;
  }

  if (const auto* timer =
          std::get_if<sim::TimerEvent>(&scheduled->payload)) {
    auto* node = impl_->runtime(timer->node);
    if (node != nullptr && node->raft) {
      auto& binding = impl_->timer_binding(*node, timer->kind);
      if (binding &&
          binding->simulator_generation == timer->generation &&
          binding->incarnation == timer->incarnation) {
        const std::uint64_t raft_generation = binding->raft_generation;
        binding.reset();
        impl_->record(ClusterTraceSource::adapter, "timer_deliver",
                      timer->node, 0, node->raft->current_term(), 0,
                      raft_generation);
        impl_->deliver(timer->node,
                       TimerFired{timer->kind, raft_generation});
      } else {
        impl_->record(ClusterTraceSource::adapter, "timer_mapping_stale",
                      timer->node, 0, impl_->term_for(timer->node), 0,
                      timer->generation);
      }
    }
  } else if (const auto* network =
                 std::get_if<sim::NetworkEvent>(&scheduled->payload)) {
    auto decoded = codec::decode_message(network->bytes, impl_->config.codec);
    if (!decoded) {
      impl_->record(ClusterTraceSource::adapter, "codec_decode_failed",
                    network->to, network->from,
                    impl_->term_for(network->to), 0, network->delivery_id,
                    network->bytes.size(), codec::to_string(decoded.error));
    } else {
      impl_->record(ClusterTraceSource::adapter, "protocol_deliver",
                    network->to, network->from,
                    impl_->term_for(network->to), 0, network->delivery_id,
                    network->bytes.size());
      impl_->deliver(network->to,
                     MessageReceived{MessageEnvelope{
                         network->from, network->to, std::move(*decoded)}});
    }
  } else if (const auto* storage =
                 std::get_if<sim::StorageEvent>(&scheduled->payload)) {
    impl_->handle_storage_event(*storage);
  } else if (const auto* user =
                 std::get_if<sim::UserEvent>(&scheduled->payload)) {
    impl_->record(ClusterTraceSource::adapter, "user_event", user->node, 0,
                  impl_->term_for(user->node), 0, user->tag,
                  user->bytes.size());
  }
  impl_->sync_simulator_trace();
  impl_->verify_if_enabled();
  return true;
}

std::size_t DeterministicCluster::run(std::size_t max_events) {
  std::size_t processed{};
  while (processed < max_events && step()) {
    ++processed;
  }
  return processed;
}

bool DeterministicCluster::run_until(
    const std::function<bool(const DeterministicCluster&)>& predicate,
    std::size_t max_events) {
  if (predicate(*this)) {
    return true;
  }
  for (std::size_t processed = 0; processed < max_events; ++processed) {
    if (!step()) {
      return predicate(*this);
    }
    if (predicate(*this)) {
      return true;
    }
  }
  return predicate(*this);
}

std::optional<NodeId> DeterministicCluster::run_until_leader(
    std::size_t max_events, bool require_ready) {
  (void)run_until(
      [require_ready](const DeterministicCluster& cluster) {
        return cluster.leader(require_ready).has_value();
      },
      max_events);
  return leader(require_ready);
}

ClientToken DeterministicCluster::submit(NodeId node,
                                         const ClientCommand& command) {
  if (impl_->next_client_token == 0) {
    throw std::overflow_error("client token space is exhausted");
  }
  const ClientToken token = impl_->next_client_token++;
  impl_->record(ClusterTraceSource::adapter, "client_submit", node, 0,
                impl_->term_for(node), 0, token,
                command.key.size() + command.value.size());
  auto* found = impl_->runtime(node);
  if (found == nullptr || !found->raft ||
      !impl_->simulator.is_alive(node)) {
    impl_->retain_reply(
        ClientReply{token, ClientStatus::unavailable, std::nullopt, 0, {}},
        node);
    impl_->record(ClusterTraceSource::adapter, "client_reply", node, 0,
                  impl_->term_for(node), 0, token, 0, "unavailable");
  } else {
    impl_->deliver(node, ClientRequest{token, command});
  }
  impl_->verify_if_enabled();
  return token;
}

std::optional<ClientToken> DeterministicCluster::submit_to_leader(
    const ClientCommand& command, bool require_ready) {
  const auto target = leader(require_ready);
  if (!target) {
    return std::nullopt;
  }
  return submit(*target, command);
}

bool DeterministicCluster::run_until_reply(ClientToken token,
                                           std::size_t max_events) {
  return run_until(
      [token](const DeterministicCluster& cluster) {
        return cluster.reply_for(token).has_value();
      },
      max_events);
}

std::optional<ClientReply> DeterministicCluster::reply_for(
    ClientToken token) const {
  const auto found = std::find_if(
      impl_->client_replies.rbegin(), impl_->client_replies.rend(),
      [token](const ClientReply& reply) { return reply.token == token; });
  return found == impl_->client_replies.rend()
             ? std::nullopt
             : std::optional<ClientReply>{*found};
}

const std::vector<ClientReply>& DeterministicCluster::replies() const noexcept {
  return impl_->client_replies;
}

std::vector<ClientReply> DeterministicCluster::take_replies() {
  auto result = std::move(impl_->client_replies);
  impl_->client_replies.clear();
  return result;
}

bool DeterministicCluster::crash(NodeId node,
                                 sim::StorageCrashSpec storage) {
  auto* found = impl_->runtime(node);
  if (found == nullptr || !found->raft ||
      !impl_->simulator.crash_node(node, storage)) {
    return false;
  }
  impl_->sync_simulator_trace();
  found->raft.reset();
  found->election_timer.reset();
  found->heartbeat_timer.reset();
  for (auto pending = impl_->pending_persists.begin();
       pending != impl_->pending_persists.end();) {
    if (pending->second.node == node) {
      pending = impl_->pending_persists.erase(pending);
    } else {
      ++pending;
    }
  }
  impl_->record(ClusterTraceSource::adapter, "process_crash", node, 0,
                found->wal_state.hard_state.current_term,
                found->wal_state.commit_index);
  impl_->verify_if_enabled();
  return true;
}

bool DeterministicCluster::restart(NodeId node) {
  auto* found = impl_->runtime(node);
  if (found == nullptr || found->raft || impl_->simulator.is_alive(node)) {
    return false;
  }
  if (!impl_->recover_node(*found, true)) {
    return false;
  }
  if (!impl_->simulator.restart_node(node)) {
    return false;
  }
  impl_->sync_simulator_trace();
  found->raft = std::make_unique<RaftNode>(
      found->raft_config, recovered_from(found->wal_state));
  impl_->record(ClusterTraceSource::adapter, "process_restart", node, 0,
                found->raft->current_term(), found->raft->commit_index());
  impl_->enqueue_effects(node, found->raft->start());
  impl_->verify_if_enabled();
  return true;
}

bool DeterministicCluster::set_partition(NodeId from, NodeId to,
                                         bool partitioned) {
  const bool changed =
      impl_->simulator.set_partition(from, to, partitioned);
  impl_->sync_simulator_trace();
  if (changed) {
    impl_->verify_if_enabled();
  }
  return changed;
}

bool DeterministicCluster::set_bidirectional_partition(
    NodeId first, NodeId second, bool partitioned) {
  if (!set_partition(first, second, partitioned)) {
    return false;
  }
  return set_partition(second, first, partitioned);
}

bool DeterministicCluster::isolate(NodeId node, bool partitioned) {
  if (impl_->runtime(node) == nullptr) {
    return false;
  }
  for (const NodeId peer : impl_->config.members) {
    if (peer == node) {
      continue;
    }
    if (!set_partition(node, peer, partitioned) ||
        !set_partition(peer, node, partitioned)) {
      return false;
    }
  }
  return true;
}

bool DeterministicCluster::set_link_delay(NodeId from, NodeId to,
                                          Tick delay) {
  const bool changed = impl_->simulator.set_link_delay(from, to, delay);
  impl_->sync_simulator_trace();
  if (changed) {
    impl_->verify_if_enabled();
  }
  return changed;
}

bool DeterministicCluster::drop_next(NodeId from, NodeId to,
                                     std::uint32_t count) {
  const bool changed = impl_->simulator.drop_next(from, to, count);
  impl_->sync_simulator_trace();
  if (changed) {
    impl_->verify_if_enabled();
  }
  return changed;
}

bool DeterministicCluster::duplicate_next(NodeId from, NodeId to,
                                          std::uint32_t extra_copies) {
  const bool changed =
      impl_->simulator.duplicate_next(from, to, extra_copies);
  impl_->sync_simulator_trace();
  if (changed) {
    impl_->verify_if_enabled();
  }
  return changed;
}

bool DeterministicCluster::fail_next_storage_write(NodeId node) {
  const bool changed = impl_->simulator.fail_next_write(node);
  impl_->sync_simulator_trace();
  if (changed) {
    impl_->verify_if_enabled();
  }
  return changed;
}

bool DeterministicCluster::short_write_next(NodeId node,
                                            std::size_t prefix_bytes) {
  const bool changed =
      impl_->simulator.short_write_next(node, prefix_bytes);
  impl_->sync_simulator_trace();
  if (changed) {
    impl_->verify_if_enabled();
  }
  return changed;
}

bool DeterministicCluster::fail_next_storage_flush(NodeId node) {
  const bool changed = impl_->simulator.fail_next_flush(node);
  impl_->sync_simulator_trace();
  if (changed) {
    impl_->verify_if_enabled();
  }
  return changed;
}

std::optional<RecoveredState> DeterministicCluster::stable_state(
    NodeId node) const {
  const auto* found = impl_->runtime(node);
  return found == nullptr
             ? std::nullopt
             : std::optional<RecoveredState>{recovered_from(
                   found->wal_state)};
}

std::optional<std::string> DeterministicCluster::value(
    NodeId node, std::string_view key) const {
  const auto* found = impl_->runtime(node);
  if (found == nullptr || !found->raft ||
      !impl_->simulator.is_alive(node)) {
    return std::nullopt;
  }
  return found->raft->state_machine().value_for_test(key);
}

std::optional<LogEntry> DeterministicCluster::committed_entry(
    LogIndex index) const {
  const auto found = impl_->committed_ghost.find(index);
  return found == impl_->committed_ghost.end()
             ? std::nullopt
             : std::optional<LogEntry>{found->second};
}

bool DeterministicCluster::command_is_committed(
    const SessionId& session, std::uint64_t sequence) const {
  return std::any_of(
      impl_->committed_ghost.begin(), impl_->committed_ghost.end(),
      [&](const auto& indexed_entry) {
        const auto& command = indexed_entry.second.command;
        return command && command->session == session &&
               command->sequence == sequence;
      });
}

InvariantResult DeterministicCluster::check_invariants() {
  impl_->last_invariant = impl_->inspect_invariants();
  return impl_->last_invariant;
}

const InvariantResult& DeterministicCluster::last_invariant_result()
    const noexcept {
  return impl_->last_invariant;
}

const std::vector<ClusterTraceRecord>& DeterministicCluster::trace()
    const noexcept {
  return impl_->semantic_trace;
}

bool DeterministicCluster::trace_truncated() const noexcept {
  return impl_->semantic_trace_truncated;
}

std::string DeterministicCluster::trace_jsonl() const {
  return render_cluster_jsonl(impl_->semantic_trace);
}

}  // namespace detlog
