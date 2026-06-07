#pragma once

#include "detlog/model.hpp"
#include "detlog/state_machine.hpp"

#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <variant>
#include <vector>

namespace detlog {

enum class RaftRole : std::uint8_t {
  follower = 1,
  candidate = 2,
  leader = 3,
  unavailable = 4,
};

struct RaftConfig {
  NodeId node_id{};
  std::vector<NodeId> members;
  Tick election_timeout{10};
  Tick heartbeat_interval{2};
  std::size_t max_deferred_events{128};
  std::size_t max_uncommitted_entries{1024};
  std::size_t max_uncommitted_bytes{4U * 1024U * 1024U};
  std::size_t max_command_bytes{64U * 1024U};
  std::size_t max_key_bytes{4U * 1024U};
  std::size_t max_value_bytes{64U * 1024U};
  std::size_t max_append_entries_per_rpc{64};
  std::size_t max_append_bytes_per_rpc{512U * 1024U};
  std::size_t append_retry_heartbeats{3};
  std::size_t max_waiters_per_request{8};
};

struct RecoveredState {
  HardState hard_state;
  std::vector<LogEntry> log;
  LogIndex commit_index{};
};

struct PersistEffect {
  StorageOpId op_id{};
  PersistBatch batch;
};

struct SendEffect {
  MessageEnvelope envelope;
};

struct ScheduleTimerEffect {
  TimerRequest timer;
};

struct ClientReplyEffect {
  ClientReply reply;
};

struct TraceEffect {
  TraceRecord record;
};

using RaftEffect =
    std::variant<PersistEffect, SendEffect, ScheduleTimerEffect,
                 ClientReplyEffect, TraceEffect>;

struct TimerFired {
  TimerKind kind{TimerKind::election};
  std::uint64_t generation{};
};

struct MessageReceived {
  MessageEnvelope envelope;
};

struct StorageComplete {
  StorageOpId op_id{};
  bool success{true};
  std::string error;
};

struct ClientRequest {
  ClientToken token{};
  ClientCommand command;
};

using RaftEvent =
    std::variant<TimerFired, MessageReceived, StorageComplete, ClientRequest>;

// RaftNode is a single-threaded transition engine. It mutates speculative
// in-memory state while a PersistEffect is outstanding, but defers every other
// event and emits no persistence-dependent RPC or client success until the
// matching StorageComplete arrives.
class RaftNode {
 public:
  explicit RaftNode(RaftConfig config,
                    RecoveredState recovered = RecoveredState{});

  std::vector<RaftEffect> start();
  std::vector<RaftEffect> step(RaftEvent event);

  [[nodiscard]] NodeId id() const noexcept { return config_.node_id; }
  [[nodiscard]] RaftRole role() const noexcept { return role_; }
  [[nodiscard]] Term current_term() const noexcept {
    return hard_state_.current_term;
  }
  [[nodiscard]] std::optional<NodeId> voted_for() const noexcept {
    return hard_state_.voted_for;
  }
  [[nodiscard]] std::optional<NodeId> leader_hint() const noexcept {
    return leader_hint_;
  }
  [[nodiscard]] LogIndex last_log_index() const noexcept;
  [[nodiscard]] LogIndex durable_last_log_index() const noexcept {
    return durable_last_log_index_;
  }
  [[nodiscard]] LogIndex commit_index() const noexcept { return commit_index_; }
  [[nodiscard]] LogIndex last_applied() const noexcept { return last_applied_; }
  [[nodiscard]] bool leader_ready() const noexcept { return leader_ready_; }
  [[nodiscard]] bool storage_pending() const noexcept {
    return pending_storage_.has_value();
  }
  [[nodiscard]] std::size_t deferred_event_count() const noexcept {
    return deferred_events_.size();
  }
  [[nodiscard]] std::size_t quorum_size() const noexcept {
    return config_.members.size() / 2U + 1U;
  }
  [[nodiscard]] const std::vector<LogEntry>& log() const noexcept {
    return log_;
  }
  [[nodiscard]] const KvStateMachine& state_machine() const noexcept {
    return state_machine_;
  }

 private:
  struct InflightAppend {
    RpcId rpc_id{};
    LogIndex prev_index{};
    LogIndex last_index{};
  };

  struct PeerProgress {
    LogIndex next_index{1};
    LogIndex match_index{};
    std::optional<InflightAppend> inflight;
    std::size_t inflight_heartbeat_age{};
  };

  struct PendingClientEntry {
    ClientCommand command;
    std::uint64_t digest{};
    std::vector<ClientToken> waiters;
  };

  struct PendingStorage {
    StorageOpId op_id{};
    PersistBatch batch;
    std::function<void(std::vector<RaftEffect>&)> after_success;
  };

  void dispatch(RaftEvent event, std::vector<RaftEffect>& effects);
  void on_timer(const TimerFired& timer, std::vector<RaftEffect>& effects);
  void on_message(const MessageEnvelope& envelope,
                  std::vector<RaftEffect>& effects);
  void on_storage_complete(const StorageComplete& complete,
                           std::vector<RaftEffect>& effects);
  void on_client_request(ClientRequest request,
                         std::vector<RaftEffect>& effects);

  void on_request_vote(NodeId from, const RequestVote& request,
                       std::vector<RaftEffect>& effects);
  void on_request_vote_response(NodeId from,
                                const RequestVoteResponse& response,
                                std::vector<RaftEffect>& effects);
  void on_append_entries(NodeId from, const AppendEntries& request,
                         std::vector<RaftEffect>& effects);
  void on_append_entries_response(NodeId from,
                                  const AppendEntriesResponse& response,
                                  std::vector<RaftEffect>& effects);

  void begin_election(std::vector<RaftEffect>& effects);
  void become_leader(std::vector<RaftEffect>& effects);
  void step_down(Term term, std::optional<NodeId> leader_hint,
                 std::vector<RaftEffect>& effects, bool emit_timer);
  void begin_persist(
      PersistBatch batch,
      std::function<void(std::vector<RaftEffect>&)> after_success,
      std::vector<RaftEffect>& effects);
  void fail_storage(const std::string& detail,
                    std::vector<RaftEffect>& effects);

  void schedule_election(std::vector<RaftEffect>& effects);
  void schedule_heartbeat(std::vector<RaftEffect>& effects);
  void send_message(NodeId to, Message message,
                    std::vector<RaftEffect>& effects);
  void send_append(NodeId peer, std::vector<RaftEffect>& effects);
  void send_all_appends(std::vector<RaftEffect>& effects);
  void maybe_advance_commit(std::vector<RaftEffect>& effects);
  void apply_committed(std::vector<RaftEffect>& effects);

  [[nodiscard]] bool is_member(NodeId id) const noexcept;
  [[nodiscard]] const LogEntry* entry_at(LogIndex index) const noexcept;
  [[nodiscard]] Term term_at(LogIndex index) const noexcept;
  [[nodiscard]] bool candidate_is_up_to_date(LogIndex index,
                                              Term term) const noexcept;
  [[nodiscard]] std::size_t uncommitted_entry_count() const noexcept;
  [[nodiscard]] std::size_t uncommitted_bytes() const noexcept;
  [[nodiscard]] static std::size_t entry_bytes(const LogEntry& entry) noexcept;
  [[nodiscard]] bool can_append(const LogEntry& entry) const noexcept;
  [[nodiscard]] bool can_append_client(const LogEntry& entry) const noexcept;
  [[nodiscard]] std::optional<LogIndex> last_index_of_term(Term term) const;
  [[nodiscard]] LogIndex first_index_of_term(LogIndex at) const noexcept;

  void remove_pending_from(LogIndex index,
                           std::vector<RaftEffect>& effects);
  void emit_reply(ClientToken token, ClientStatus status,
                  std::vector<RaftEffect>& effects,
                  std::string value = {}, LogIndex index = 0);
  void trace(TraceKind kind, std::vector<RaftEffect>& effects,
             std::string detail = {}, LogIndex index = 0) const;

  RaftConfig config_;
  RaftRole role_{RaftRole::follower};
  HardState hard_state_;
  HardState durable_hard_state_;
  std::vector<LogEntry> log_;
  LogIndex durable_last_log_index_{};
  LogIndex commit_index_{};
  LogIndex durable_commit_index_{};
  LogIndex last_applied_{};
  KvStateMachine state_machine_;

  std::optional<NodeId> leader_hint_;
  bool leader_ready_{};
  LogIndex readiness_index_{};
  std::set<NodeId> votes_received_;
  std::map<NodeId, PeerProgress> peers_;

  std::map<LogIndex, PendingClientEntry> pending_clients_;
  std::map<SessionId, LogIndex> pending_sessions_;

  std::optional<PendingStorage> pending_storage_;
  std::deque<RaftEvent> deferred_events_;
  StorageOpId next_storage_op_id_{1};
  RpcId next_rpc_id_{1};
  std::uint64_t election_generation_{};
  std::uint64_t heartbeat_generation_{};
  bool started_{};
};

}  // namespace detlog
