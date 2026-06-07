#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <variant>
#include <vector>

namespace detlog {

using NodeId = std::uint32_t;
using Term = std::uint64_t;
using LogIndex = std::uint64_t;
using RpcId = std::uint64_t;
using StorageOpId = std::uint64_t;
using ClientToken = std::uint64_t;
using Tick = std::uint64_t;

struct SessionId {
  std::uint64_t high{};
  std::uint64_t low{};

  friend bool operator==(const SessionId&, const SessionId&) = default;
  friend bool operator<(const SessionId& lhs, const SessionId& rhs) noexcept {
    return lhs.high < rhs.high ||
           (lhs.high == rhs.high && lhs.low < rhs.low);
  }
};

enum class CommandKind : std::uint8_t {
  put = 1,
  erase = 2,
  get = 3,
};

struct ClientCommand {
  SessionId session;
  std::uint64_t sequence{};
  CommandKind kind{CommandKind::put};
  std::string key;
  std::string value;

  friend bool operator==(const ClientCommand&, const ClientCommand&) = default;
};

struct LogEntry {
  LogIndex index{};
  Term term{};
  // No command denotes the leader's current-term readiness no-op.
  std::optional<ClientCommand> command;

  friend bool operator==(const LogEntry&, const LogEntry&) = default;
};

struct HardState {
  Term current_term{};
  std::optional<NodeId> voted_for;

  friend bool operator==(const HardState&, const HardState&) = default;
};

struct PersistBatch {
  std::optional<HardState> hard_state;
  // Entries at and after truncate_from become logically dead. A batch may then
  // append a consecutive replacement suffix. The WAL file itself stays append-only.
  std::optional<LogIndex> truncate_from;
  std::vector<LogEntry> entries;
  std::optional<LogIndex> commit_index;

  [[nodiscard]] bool empty() const noexcept {
    return !hard_state && !truncate_from && entries.empty() && !commit_index;
  }

  friend bool operator==(const PersistBatch&, const PersistBatch&) = default;
};

struct RequestVote {
  Term term{};
  NodeId candidate_id{};
  LogIndex last_log_index{};
  Term last_log_term{};

  friend bool operator==(const RequestVote&, const RequestVote&) = default;
};

struct RequestVoteResponse {
  Term term{};
  bool vote_granted{};

  friend bool operator==(const RequestVoteResponse&,
                         const RequestVoteResponse&) = default;
};

struct AppendEntries {
  Term term{};
  NodeId leader_id{};
  RpcId rpc_id{};
  LogIndex prev_log_index{};
  Term prev_log_term{};
  std::vector<LogEntry> entries;
  LogIndex leader_commit{};

  friend bool operator==(const AppendEntries&, const AppendEntries&) = default;
};

struct AppendEntriesResponse {
  Term term{};
  RpcId rpc_id{};
  bool success{};
  LogIndex match_index{};
  LogIndex conflict_index{};
  Term conflict_term{};

  friend bool operator==(const AppendEntriesResponse&,
                         const AppendEntriesResponse&) = default;
};

using Message = std::variant<RequestVote, RequestVoteResponse, AppendEntries,
                             AppendEntriesResponse>;

struct MessageEnvelope {
  NodeId from{};
  NodeId to{};
  Message message;

  friend bool operator==(const MessageEnvelope&,
                         const MessageEnvelope&) = default;
};

enum class ClientStatus : std::uint8_t {
  ok = 0,
  not_leader,
  leader_not_ready,
  busy,
  unavailable,
  stale_sequence,
  sequence_gap,
  request_id_conflict,
  invalid_request,
  storage_error,
};

struct ClientReply {
  ClientToken token{};
  ClientStatus status{ClientStatus::ok};
  std::optional<NodeId> leader_hint;
  LogIndex log_index{};
  std::string value;

  friend bool operator==(const ClientReply&, const ClientReply&) = default;
};

enum class TimerKind : std::uint8_t { election = 1, heartbeat = 2 };

struct TimerRequest {
  TimerKind kind{TimerKind::election};
  Tick delay{};
  std::uint64_t generation{};

  friend bool operator==(const TimerRequest&, const TimerRequest&) = default;
};

enum class TraceKind : std::uint8_t {
  role_change,
  term_change,
  vote,
  send,
  receive,
  storage_begin,
  storage_complete,
  log_change,
  commit,
  apply,
  client_reply,
  queue_saturated,
  fault,
};

struct TraceRecord {
  TraceKind kind{TraceKind::receive};
  NodeId node{};
  Term term{};
  LogIndex index{};
  std::string detail;

  friend bool operator==(const TraceRecord&, const TraceRecord&) = default;
};

}  // namespace detlog
