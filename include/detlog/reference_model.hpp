#pragma once

#include "detlog/model.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

namespace detlog::reference {

// This is intentionally a serial, abstract model rather than a second Raft
// implementation. A history supplies quorum witnesses explicitly; the model
// checks those witnesses, maintains replica prefixes, and interprets committed
// commands without calling RaftNode or KvStateMachine.
struct Elect {
  NodeId candidate{};
  Term term{};
  std::vector<NodeId> voters;
};

struct AppendNoOp {
  NodeId leader{};
};

struct Propose {
  NodeId leader{};
  ClientCommand command;
};

// Abstracts one or more successful AppendEntries exchanges. The follower is
// made identical to the leader through `through`; a conflicting, uncommitted
// suffix is discarded.
struct ReplicatePrefix {
  NodeId leader{};
  NodeId follower{};
  LogIndex through{};
};

// `acknowledgers` is the quorum certificate for this commit. Every listed
// replica must contain the leader's prefix through `through`.
struct CommitPrefix {
  NodeId leader{};
  LogIndex through{};
  std::vector<NodeId> acknowledgers;
};

// Abstracts delivery of a leaderCommit value after the leader has committed.
struct LearnCommit {
  NodeId leader{};
  NodeId follower{};
  LogIndex through{};
};

struct Crash {
  NodeId node{};
};

struct Restart {
  NodeId node{};
};

using Action =
    std::variant<Elect, AppendNoOp, Propose, ReplicatePrefix, CommitPrefix,
                 LearnCommit, Crash, Restart>;

struct ProposalOutcome {
  ClientStatus status{ClientStatus::ok};
  std::string value;
  LogIndex index{};
  bool appended{};
  bool duplicate{};

  friend bool operator==(const ProposalOutcome&,
                         const ProposalOutcome&) = default;
};

struct AppliedOutcome {
  LogIndex index{};
  ClientStatus status{ClientStatus::ok};
  std::string value;
  bool executed{};
  bool duplicate{};

  friend bool operator==(const AppliedOutcome&, const AppliedOutcome&) =
      default;
};

struct ActionResult {
  bool ok{true};
  std::string error;
  std::optional<LogIndex> appended_index;
  std::optional<ProposalOutcome> proposal;

  [[nodiscard]] explicit operator bool() const noexcept { return ok; }
};

struct HistoryResult {
  bool ok{true};
  std::size_t applied_actions{};
  std::string error;

  [[nodiscard]] explicit operator bool() const noexcept { return ok; }
};

struct ReplicaView {
  NodeId id{};
  bool alive{};
  Term current_term{};
  std::optional<NodeId> voted_for;
  LogIndex commit_index{};
  LogIndex last_applied{};
  bool leader_ready{};
  std::vector<LogEntry> log;

  friend bool operator==(const ReplicaView&, const ReplicaView&) = default;
};

class Model {
 public:
  explicit Model(std::vector<NodeId> members);
  ~Model();
  Model(Model&&) noexcept;
  Model& operator=(Model&&) noexcept;
  Model(const Model&) = delete;
  Model& operator=(const Model&) = delete;

  [[nodiscard]] const std::vector<NodeId>& members() const noexcept;
  [[nodiscard]] std::size_t quorum_size() const noexcept;

  [[nodiscard]] ActionResult apply(const Action& action);
  [[nodiscard]] HistoryResult apply(std::span<const Action> history);

  [[nodiscard]] std::optional<ReplicaView> replica(NodeId node) const;
  [[nodiscard]] std::vector<ReplicaView> replicas() const;
  [[nodiscard]] std::optional<std::string> value(
      NodeId node, std::string_view key) const;
  [[nodiscard]] std::optional<AppliedOutcome> applied_outcome(
      NodeId node, LogIndex index) const;
  [[nodiscard]] bool is_leader(NodeId node) const;

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace detlog::reference
