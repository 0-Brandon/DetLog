#include "detlog/reference_model.hpp"

#include <algorithm>
#include <limits>
#include <map>
#include <set>
#include <stdexcept>
#include <type_traits>
#include <utility>

namespace detlog::reference {
namespace {

struct SessionState {
  ClientCommand command;
  ClientStatus status{ClientStatus::ok};
  std::string result;
};

struct ReplicaState {
  NodeId id{};
  bool alive{true};
  // Leadership is volatile: the term -> leader map below is a historical
  // uniqueness witness, not a substitute for the role a restarted replica
  // actually has. Crash/restart and observing a higher term clear this bit.
  bool active_leader{};
  Term current_term{};
  std::optional<NodeId> voted_for;
  LogIndex commit_index{};
  LogIndex last_applied{};
  Term ready_term{};
  std::vector<LogEntry> log;
  std::map<std::string, std::string, std::less<>> values;
  std::map<SessionId, SessionState> sessions;
  std::map<LogIndex, AppliedOutcome> outcomes;
};

[[nodiscard]] bool structurally_valid(const ClientCommand& command) {
  if (command.sequence == 0 || command.key.empty()) {
    return false;
  }
  switch (command.kind) {
    case CommandKind::put:
      return true;
    case CommandKind::erase:
    case CommandKind::get:
      return command.value.empty();
  }
  return false;
}

[[nodiscard]] bool same_request(const ClientCommand& lhs,
                                const ClientCommand& rhs) {
  return lhs.session == rhs.session && lhs.sequence == rhs.sequence &&
         lhs.kind == rhs.kind && lhs.key == rhs.key && lhs.value == rhs.value;
}

[[nodiscard]] Term last_term(const ReplicaState& replica) {
  return replica.log.empty() ? 0 : replica.log.back().term;
}

[[nodiscard]] bool candidate_is_up_to_date(const ReplicaState& candidate,
                                            const ReplicaState& voter) {
  const Term candidate_term = last_term(candidate);
  const Term voter_term = last_term(voter);
  if (candidate_term != voter_term) {
    return candidate_term > voter_term;
  }
  return candidate.log.size() >= voter.log.size();
}

[[nodiscard]] bool equal_prefix(const ReplicaState& lhs,
                                const ReplicaState& rhs,
                                LogIndex through) {
  if (through > lhs.log.size() || through > rhs.log.size()) {
    return false;
  }
  for (LogIndex index = 1; index <= through; ++index) {
    const std::size_t offset = static_cast<std::size_t>(index - 1U);
    if (lhs.log[offset] != rhs.log[offset]) {
      return false;
    }
  }
  return true;
}

[[nodiscard]] AppliedOutcome interpret(ReplicaState& replica, LogIndex index,
                                       const ClientCommand& command) {
  AppliedOutcome result;
  result.index = index;
  if (!structurally_valid(command)) {
    result.status = ClientStatus::invalid_request;
    return result;
  }

  const auto existing = replica.sessions.find(command.session);
  if (existing != replica.sessions.end()) {
    const SessionState& session = existing->second;
    if (command.sequence < session.command.sequence) {
      result.status = ClientStatus::stale_sequence;
      return result;
    }
    if (command.sequence == session.command.sequence) {
      if (!same_request(command, session.command)) {
        result.status = ClientStatus::request_id_conflict;
        return result;
      }
      result.status = session.status;
      result.value = session.result;
      result.duplicate = true;
      return result;
    }
    if (session.command.sequence ==
            std::numeric_limits<std::uint64_t>::max() ||
        command.sequence != session.command.sequence + 1U) {
      result.status = ClientStatus::sequence_gap;
      return result;
    }
  } else if (command.sequence != 1) {
    result.status = ClientStatus::sequence_gap;
    return result;
  }

  switch (command.kind) {
    case CommandKind::put:
      replica.values[command.key] = command.value;
      result.value = command.value;
      break;
    case CommandKind::erase: {
      const auto value = replica.values.find(command.key);
      if (value != replica.values.end()) {
        result.value = value->second;
        replica.values.erase(value);
      }
      break;
    }
    case CommandKind::get: {
      const auto value = replica.values.find(command.key);
      if (value != replica.values.end()) {
        result.value = value->second;
      }
      break;
    }
  }

  result.status = ClientStatus::ok;
  result.executed = true;
  replica.sessions[command.session] =
      SessionState{command, result.status, result.value};
  return result;
}

void apply_through(ReplicaState& replica, LogIndex through) {
  while (replica.last_applied < through) {
    ++replica.last_applied;
    const LogEntry& entry =
        replica.log[static_cast<std::size_t>(replica.last_applied - 1U)];
    if (entry.command) {
      replica.outcomes[replica.last_applied] =
          interpret(replica, replica.last_applied, *entry.command);
    }
  }
}

[[nodiscard]] ActionResult reject(std::string error) {
  ActionResult result;
  result.ok = false;
  result.error = std::move(error);
  return result;
}

[[nodiscard]] ActionResult proposal_result(ProposalOutcome outcome) {
  ActionResult result;
  if (outcome.appended) {
    result.appended_index = outcome.index;
  }
  result.proposal = std::move(outcome);
  return result;
}

}  // namespace

struct Model::Impl {
  explicit Impl(std::vector<NodeId> configured_members)
      : members(std::move(configured_members)) {
    if (members.empty()) {
      throw std::invalid_argument("reference model requires members");
    }
    std::set<NodeId> unique;
    for (const NodeId member : members) {
      if (member == 0 || !unique.insert(member).second) {
        throw std::invalid_argument(
            "reference model members must be unique and nonzero");
      }
      ReplicaState replica;
      replica.id = member;
      replicas.emplace(member, std::move(replica));
    }
  }

  [[nodiscard]] ReplicaState* find(NodeId node) {
    const auto found = replicas.find(node);
    return found == replicas.end() ? nullptr : &found->second;
  }

  [[nodiscard]] const ReplicaState* find(NodeId node) const {
    const auto found = replicas.find(node);
    return found == replicas.end() ? nullptr : &found->second;
  }

  [[nodiscard]] bool leader(const ReplicaState& replica) const {
    const auto found = leaders.find(replica.current_term);
    return replica.active_leader && found != leaders.end() &&
           found->second == replica.id;
  }

  [[nodiscard]] ActionResult elect(const Elect& action) {
    ReplicaState* candidate = find(action.candidate);
    if (candidate == nullptr) {
      return reject("election candidate is not a member");
    }
    if (!candidate->alive) {
      return reject("crashed candidate cannot be elected");
    }
    if (action.term == 0 || action.term <= candidate->current_term) {
      return reject("election term must advance the candidate term");
    }

    std::set<NodeId> unique_voters;
    for (const NodeId voter_id : action.voters) {
      ReplicaState* voter = find(voter_id);
      if (voter == nullptr) {
        return reject("election voter is not a member");
      }
      if (!unique_voters.insert(voter_id).second) {
        return reject("election voter is duplicated");
      }
      if (!voter->alive) {
        return reject("crashed replica cannot vote");
      }
      if (voter->current_term > action.term) {
        return reject("voter has already observed a higher term");
      }
      if (voter->current_term == action.term && voter->voted_for &&
          *voter->voted_for != action.candidate) {
        return reject("voter already voted for another candidate");
      }
      if (!candidate_is_up_to_date(*candidate, *voter)) {
        return reject("candidate log is not up to date for a voter");
      }
    }
    if (!unique_voters.contains(action.candidate)) {
      return reject("candidate's self-vote is missing");
    }
    if (unique_voters.size() < members.size() / 2U + 1U) {
      return reject("election has no quorum");
    }
    const auto existing_leader = leaders.find(action.term);
    if (existing_leader != leaders.end() &&
        existing_leader->second != action.candidate) {
      return reject("term already has a different leader");
    }

    for (const NodeId voter_id : unique_voters) {
      ReplicaState& voter = replicas.at(voter_id);
      if (voter.current_term < action.term) {
        voter.active_leader = false;
        voter.current_term = action.term;
        voter.voted_for.reset();
      }
      voter.voted_for = action.candidate;
    }
    candidate->current_term = action.term;
    candidate->voted_for = action.candidate;
    candidate->active_leader = true;
    leaders[action.term] = action.candidate;
    return {};
  }

  [[nodiscard]] ActionResult append_noop(const AppendNoOp& action) {
    ReplicaState* replica = find(action.leader);
    if (replica == nullptr) {
      return reject("no-op leader is not a member");
    }
    if (!replica->alive || !leader(*replica)) {
      return reject("only a live elected leader may append a no-op");
    }
    const LogIndex index = static_cast<LogIndex>(replica->log.size()) + 1U;
    replica->log.push_back(LogEntry{index, replica->current_term, std::nullopt});
    ActionResult result;
    result.appended_index = index;
    return result;
  }

  [[nodiscard]] ActionResult propose(const Propose& action) {
    ReplicaState* replica = find(action.leader);
    if (replica == nullptr) {
      return reject("proposal target is not a member");
    }
    if (!replica->alive || !leader(*replica)) {
      return reject("proposal target is not a live elected leader");
    }
    if (replica->ready_term != replica->current_term) {
      return reject("leader has not committed an entry in its current term");
    }

    ProposalOutcome outcome;
    if (!structurally_valid(action.command)) {
      outcome.status = ClientStatus::invalid_request;
      return proposal_result(std::move(outcome));
    }
    const auto existing = replica->sessions.find(action.command.session);
    if (existing != replica->sessions.end()) {
      const SessionState& session = existing->second;
      if (action.command.sequence < session.command.sequence) {
        outcome.status = ClientStatus::stale_sequence;
        return proposal_result(std::move(outcome));
      }
      if (action.command.sequence == session.command.sequence) {
        if (!same_request(action.command, session.command)) {
          outcome.status = ClientStatus::request_id_conflict;
          return proposal_result(std::move(outcome));
        }
        outcome.status = session.status;
        outcome.value = session.result;
        outcome.duplicate = true;
        return proposal_result(std::move(outcome));
      }
      if (session.command.sequence ==
              std::numeric_limits<std::uint64_t>::max() ||
          action.command.sequence != session.command.sequence + 1U) {
        outcome.status = ClientStatus::sequence_gap;
        return proposal_result(std::move(outcome));
      }
    } else if (action.command.sequence != 1) {
      outcome.status = ClientStatus::sequence_gap;
      return proposal_result(std::move(outcome));
    }

    const LogIndex index = static_cast<LogIndex>(replica->log.size()) + 1U;
    replica->log.push_back(
        LogEntry{index, replica->current_term, action.command});
    outcome.index = index;
    outcome.appended = true;
    return proposal_result(std::move(outcome));
  }

  [[nodiscard]] ActionResult replicate(const ReplicatePrefix& action) {
    ReplicaState* source = find(action.leader);
    ReplicaState* follower = find(action.follower);
    if (source == nullptr || follower == nullptr) {
      return reject("replication endpoint is not a member");
    }
    if (source == follower) {
      return reject("replication endpoints must differ");
    }
    if (!source->alive || !follower->alive || !leader(*source)) {
      return reject("replication requires a live leader and follower");
    }
    if (follower->current_term > source->current_term) {
      return reject("higher-term follower rejects replication");
    }
    if (action.through > source->log.size()) {
      return reject("replication exceeds the leader log");
    }

    LogIndex divergence{};
    const LogIndex common_limit = std::min<LogIndex>(
        action.through, static_cast<LogIndex>(follower->log.size()));
    for (LogIndex index = 1; index <= common_limit; ++index) {
      const std::size_t offset = static_cast<std::size_t>(index - 1U);
      if (follower->log[offset] != source->log[offset]) {
        divergence = index;
        break;
      }
    }
    if (divergence == 0 && follower->log.size() < action.through) {
      divergence = static_cast<LogIndex>(follower->log.size()) + 1U;
    }
    if (divergence != 0) {
      if (divergence <= follower->commit_index) {
        return reject("replication would overwrite a committed entry");
      }
      follower->log.resize(static_cast<std::size_t>(divergence - 1U));
      const auto begin = source->log.begin() +
                         static_cast<std::ptrdiff_t>(divergence - 1U);
      const auto end = source->log.begin() +
                       static_cast<std::ptrdiff_t>(action.through);
      follower->log.insert(follower->log.end(), begin, end);
    }
    if (follower->current_term < source->current_term) {
      follower->active_leader = false;
      follower->current_term = source->current_term;
      follower->voted_for.reset();
    }
    return {};
  }

  [[nodiscard]] ActionResult commit(const CommitPrefix& action) {
    ReplicaState* source = find(action.leader);
    if (source == nullptr) {
      return reject("commit leader is not a member");
    }
    if (!source->alive || !leader(*source)) {
      return reject("only a live elected leader may commit");
    }
    if (action.through < source->commit_index ||
        action.through > source->log.size()) {
      return reject("commit index is outside the leader's legal range");
    }

    std::set<NodeId> unique;
    for (const NodeId acknowledger : action.acknowledgers) {
      const ReplicaState* replica = find(acknowledger);
      if (replica == nullptr) {
        return reject("commit acknowledger is not a member");
      }
      if (!unique.insert(acknowledger).second) {
        return reject("commit acknowledger is duplicated");
      }
      if (!replica->alive) {
        return reject("crashed replica cannot acknowledge a commit");
      }
      if (!equal_prefix(*source, *replica, action.through)) {
        return reject("commit acknowledger lacks the leader prefix");
      }
    }
    if (unique.size() < members.size() / 2U + 1U) {
      return reject("commit has no quorum");
    }
    if (!unique.contains(action.leader)) {
      return reject("commit certificate omits the leader's durable copy");
    }
    if (action.through > source->commit_index && action.through != 0) {
      const LogEntry& entry =
          source->log[static_cast<std::size_t>(action.through - 1U)];
      if (entry.term != source->current_term) {
        return reject("leader may advance commit only through current-term entry");
      }
    }

    source->commit_index = action.through;
    apply_through(*source, action.through);
    if (action.through != 0) {
      const LogEntry& entry =
          source->log[static_cast<std::size_t>(action.through - 1U)];
      if (entry.term == source->current_term) {
        source->ready_term = source->current_term;
      }
    }
    return {};
  }

  [[nodiscard]] ActionResult learn(const LearnCommit& action) {
    ReplicaState* source = find(action.leader);
    ReplicaState* follower = find(action.follower);
    if (source == nullptr || follower == nullptr) {
      return reject("commit propagation endpoint is not a member");
    }
    if (!source->alive || !follower->alive || !leader(*source)) {
      return reject("commit propagation requires a live leader and follower");
    }
    if (follower->current_term > source->current_term) {
      return reject("higher-term follower rejects commit propagation");
    }
    if (action.through < follower->commit_index ||
        action.through > source->commit_index ||
        !equal_prefix(*source, *follower, action.through)) {
      return reject("follower cannot learn the requested commit prefix");
    }
    follower->commit_index = action.through;
    apply_through(*follower, action.through);
    if (follower->current_term < source->current_term) {
      follower->active_leader = false;
      follower->current_term = source->current_term;
      follower->voted_for.reset();
    }
    return {};
  }

  [[nodiscard]] ActionResult crash(const Crash& action) {
    ReplicaState* replica = find(action.node);
    if (replica == nullptr) {
      return reject("crash target is not a member");
    }
    if (!replica->alive) {
      return reject("replica is already crashed");
    }
    replica->alive = false;
    replica->active_leader = false;
    return {};
  }

  [[nodiscard]] ActionResult restart(const Restart& action) {
    ReplicaState* replica = find(action.node);
    if (replica == nullptr) {
      return reject("restart target is not a member");
    }
    if (replica->alive) {
      return reject("replica is already alive");
    }
    replica->alive = true;
    // Raft role is not durable. A recovered node always rejoins as a follower
    // and must win a fresh election before it can act as leader again.
    replica->active_leader = false;
    return {};
  }

  std::vector<NodeId> members;
  std::map<NodeId, ReplicaState> replicas;
  std::map<Term, NodeId> leaders;
};

Model::Model(std::vector<NodeId> members)
    : impl_(std::make_unique<Impl>(std::move(members))) {}

Model::~Model() = default;
Model::Model(Model&&) noexcept = default;
Model& Model::operator=(Model&&) noexcept = default;

const std::vector<NodeId>& Model::members() const noexcept {
  return impl_->members;
}

std::size_t Model::quorum_size() const noexcept {
  return impl_->members.size() / 2U + 1U;
}

ActionResult Model::apply(const Action& action) {
  return std::visit(
      [this](const auto& concrete) -> ActionResult {
        using T = std::decay_t<decltype(concrete)>;
        if constexpr (std::is_same_v<T, Elect>) {
          return impl_->elect(concrete);
        } else if constexpr (std::is_same_v<T, AppendNoOp>) {
          return impl_->append_noop(concrete);
        } else if constexpr (std::is_same_v<T, Propose>) {
          return impl_->propose(concrete);
        } else if constexpr (std::is_same_v<T, ReplicatePrefix>) {
          return impl_->replicate(concrete);
        } else if constexpr (std::is_same_v<T, CommitPrefix>) {
          return impl_->commit(concrete);
        } else if constexpr (std::is_same_v<T, LearnCommit>) {
          return impl_->learn(concrete);
        } else if constexpr (std::is_same_v<T, Crash>) {
          return impl_->crash(concrete);
        } else {
          return impl_->restart(concrete);
        }
      },
      action);
}

HistoryResult Model::apply(std::span<const Action> history) {
  for (std::size_t index = 0; index < history.size(); ++index) {
    ActionResult result = apply(history[index]);
    if (!result) {
      return HistoryResult{.ok = false,
                           .applied_actions = index,
                           .error = std::move(result.error)};
    }
  }
  HistoryResult result;
  result.applied_actions = history.size();
  return result;
}

std::optional<ReplicaView> Model::replica(NodeId node) const {
  const ReplicaState* replica = impl_->find(node);
  if (replica == nullptr) {
    return std::nullopt;
  }
  return ReplicaView{.id = replica->id,
                     .alive = replica->alive,
                     .current_term = replica->current_term,
                     .voted_for = replica->voted_for,
                     .commit_index = replica->commit_index,
                     .last_applied = replica->last_applied,
                     .leader_ready = impl_->leader(*replica) &&
                                     replica->ready_term != 0 &&
                                     replica->ready_term == replica->current_term,
                     .log = replica->log};
}

std::vector<ReplicaView> Model::replicas() const {
  std::vector<ReplicaView> result;
  result.reserve(impl_->members.size());
  for (const NodeId member : impl_->members) {
    result.push_back(*replica(member));
  }
  return result;
}

std::optional<std::string> Model::value(NodeId node,
                                        std::string_view key) const {
  const ReplicaState* replica = impl_->find(node);
  if (replica == nullptr) {
    return std::nullopt;
  }
  const auto value = replica->values.find(key);
  if (value == replica->values.end()) {
    return std::nullopt;
  }
  return value->second;
}

std::optional<AppliedOutcome> Model::applied_outcome(NodeId node,
                                                     LogIndex index) const {
  const ReplicaState* replica = impl_->find(node);
  if (replica == nullptr) {
    return std::nullopt;
  }
  const auto outcome = replica->outcomes.find(index);
  return outcome == replica->outcomes.end()
             ? std::nullopt
             : std::optional<AppliedOutcome>{outcome->second};
}

bool Model::is_leader(NodeId node) const {
  const ReplicaState* replica = impl_->find(node);
  return replica != nullptr && replica->alive && impl_->leader(*replica);
}

}  // namespace detlog::reference
