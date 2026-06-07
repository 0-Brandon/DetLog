#include "detlog/raft.hpp"

#include <algorithm>
#include <limits>
#include <stdexcept>
#include <utility>

namespace detlog {
namespace {

// Conservative upper bounds for the shared wire codec: a command entry has
// 50 fixed bytes before key/value data, and an AppendEntries frame has 60
// fixed bytes. Keeping a little margin makes the core's cap safely smaller
// than the transport codec's cap.
constexpr std::size_t kEntryOverhead = 64;
constexpr std::size_t kAppendRpcOverhead = 64;

bool command_size_within(const ClientCommand& command, std::size_t maximum,
                         std::size_t max_key,
                         std::size_t max_value) noexcept {
  return command.key.size() <= max_key && command.value.size() <= max_value &&
         command.key.size() <= maximum &&
         command.value.size() <= maximum - command.key.size();
}

}  // namespace

RaftNode::RaftNode(RaftConfig config, RecoveredState recovered)
    : config_(std::move(config)),
      hard_state_(recovered.hard_state),
      durable_hard_state_(recovered.hard_state),
      log_(std::move(recovered.log)),
      durable_last_log_index_(static_cast<LogIndex>(log_.size())),
      commit_index_(recovered.commit_index),
      durable_commit_index_(recovered.commit_index) {
  std::sort(config_.members.begin(), config_.members.end());
  if ((config_.members.size() != 3 && config_.members.size() != 5) ||
      std::adjacent_find(config_.members.begin(), config_.members.end()) !=
          config_.members.end() ||
      !std::binary_search(config_.members.begin(), config_.members.end(),
                          config_.node_id)) {
    throw std::invalid_argument(
        "Raft membership must contain this node and exactly 3 or 5 unique nodes");
  }
  if (hard_state_.voted_for &&
      (hard_state_.current_term == 0 ||
       !std::binary_search(config_.members.begin(), config_.members.end(),
                           *hard_state_.voted_for))) {
    throw std::invalid_argument("Recovered vote is not valid membership state");
  }
  if (config_.election_timeout == 0 || config_.heartbeat_interval == 0 ||
      config_.max_deferred_events == 0 ||
      config_.max_uncommitted_entries == 0 ||
      config_.max_uncommitted_bytes < kEntryOverhead ||
      config_.max_command_bytes == 0 || config_.max_key_bytes == 0 ||
      config_.max_append_entries_per_rpc == 0 ||
      config_.max_append_bytes_per_rpc <= kAppendRpcOverhead ||
      config_.append_retry_heartbeats == 0 ||
      config_.max_waiters_per_request == 0) {
    throw std::invalid_argument("Raft bounds and timer durations must be non-zero");
  }
  if (log_.size() >
      static_cast<std::size_t>(std::numeric_limits<LogIndex>::max())) {
    throw std::invalid_argument("Recovered log is too large");
  }
  if (hard_state_.current_term == 0 && hard_state_.voted_for) {
    throw std::invalid_argument("term-zero hard state cannot contain a vote");
  }
  for (std::size_t offset = 0; offset < log_.size(); ++offset) {
    const LogIndex expected = static_cast<LogIndex>(offset) + 1;
    const LogEntry& entry = log_[offset];
    if (entry.index != expected || entry.term == 0 ||
        entry.term > hard_state_.current_term ||
        (entry.command &&
         (!KvStateMachine::structurally_valid(*entry.command) ||
          !command_size_within(*entry.command, config_.max_command_bytes,
                               config_.max_key_bytes,
                               config_.max_value_bytes))) ||
        entry_bytes(entry) >
            config_.max_append_bytes_per_rpc - kAppendRpcOverhead) {
      throw std::invalid_argument("Recovered log is not a valid consecutive Raft log");
    }
  }
  if (commit_index_ > last_log_index()) {
    throw std::invalid_argument("Recovered commit index is beyond the log");
  }

  while (last_applied_ < commit_index_) {
    ++last_applied_;
    const LogEntry* entry = entry_at(last_applied_);
    if (entry != nullptr && entry->command) {
      static_cast<void>(state_machine_.apply(*entry->command));
    }
  }
}

std::vector<RaftEffect> RaftNode::start() {
  std::vector<RaftEffect> effects;
  if (started_ || role_ == RaftRole::unavailable) {
    return effects;
  }
  started_ = true;
  schedule_election(effects);
  return effects;
}

std::vector<RaftEffect> RaftNode::step(RaftEvent event) {
  std::vector<RaftEffect> effects;

  if (pending_storage_ && !std::holds_alternative<StorageComplete>(event)) {
    if (const auto* request = std::get_if<ClientRequest>(&event);
        request != nullptr &&
        (!KvStateMachine::structurally_valid(request->command) ||
         !command_size_within(request->command, config_.max_command_bytes,
                              config_.max_key_bytes,
                              config_.max_value_bytes))) {
      emit_reply(request->token, ClientStatus::invalid_request, effects);
      return effects;
    }
    if (const auto* received = std::get_if<MessageReceived>(&event)) {
      if (const auto* append =
              std::get_if<AppendEntries>(&received->envelope.message)) {
        bool bounded = append->entries.size() <=
                       config_.max_append_entries_per_rpc;
        std::size_t bytes = kAppendRpcOverhead;
        for (const LogEntry& entry : append->entries) {
          const std::size_t added = entry_bytes(entry);
          if (!bounded || added > config_.max_append_bytes_per_rpc -
                                      std::min(config_.max_append_bytes_per_rpc,
                                               bytes)) {
            bounded = false;
            break;
          }
          bytes += added;
          if (entry.command &&
              (!KvStateMachine::structurally_valid(*entry.command) ||
               !command_size_within(*entry.command,
                                    config_.max_command_bytes,
                                    config_.max_key_bytes,
                                    config_.max_value_bytes))) {
            bounded = false;
            break;
          }
        }
        if (!bounded) {
          trace(TraceKind::queue_saturated, effects,
                "rejected oversized deferred protocol event");
          return effects;
        }
      }
    }
    if (deferred_events_.size() >= config_.max_deferred_events) {
      trace(TraceKind::queue_saturated, effects,
            "deferred event queue is full");
      if (const auto* request = std::get_if<ClientRequest>(&event)) {
        emit_reply(request->token, ClientStatus::busy, effects);
      }
      return effects;
    }
    deferred_events_.push_back(std::move(event));
    return effects;
  }

  if (role_ == RaftRole::unavailable &&
      !std::holds_alternative<StorageComplete>(event)) {
    if (const auto* request = std::get_if<ClientRequest>(&event)) {
      emit_reply(request->token, ClientStatus::unavailable, effects);
    }
    return effects;
  }

  dispatch(std::move(event), effects);
  while (!pending_storage_ && role_ != RaftRole::unavailable &&
         !deferred_events_.empty()) {
    RaftEvent deferred = std::move(deferred_events_.front());
    deferred_events_.pop_front();
    dispatch(std::move(deferred), effects);
  }
  return effects;
}

void RaftNode::dispatch(RaftEvent event, std::vector<RaftEffect>& effects) {
  if (const auto* timer = std::get_if<TimerFired>(&event)) {
    on_timer(*timer, effects);
  } else if (const auto* message = std::get_if<MessageReceived>(&event)) {
    on_message(message->envelope, effects);
  } else if (const auto* complete = std::get_if<StorageComplete>(&event)) {
    on_storage_complete(*complete, effects);
  } else if (auto* request = std::get_if<ClientRequest>(&event)) {
    on_client_request(std::move(*request), effects);
  }
}

void RaftNode::on_timer(const TimerFired& timer,
                        std::vector<RaftEffect>& effects) {
  if (timer.kind == TimerKind::election) {
    if (timer.generation != election_generation_ ||
        role_ == RaftRole::leader || role_ == RaftRole::unavailable) {
      return;
    }
    begin_election(effects);
    return;
  }

  if (timer.generation != heartbeat_generation_ ||
      role_ != RaftRole::leader) {
    return;
  }
  for (auto& [peer, progress] : peers_) {
    static_cast<void>(peer);
    if (!progress.inflight) {
      continue;
    }
    ++progress.inflight_heartbeat_age;
    if (progress.inflight_heartbeat_age >= config_.append_retry_heartbeats) {
      progress.inflight.reset();
      progress.inflight_heartbeat_age = 0;
    }
  }
  send_all_appends(effects);
  schedule_heartbeat(effects);
}

void RaftNode::on_message(const MessageEnvelope& envelope,
                          std::vector<RaftEffect>& effects) {
  if (envelope.to != config_.node_id || envelope.from == config_.node_id ||
      !is_member(envelope.from)) {
    return;
  }
  trace(TraceKind::receive, effects, "protocol message received");
  if (const auto* vote_request = std::get_if<RequestVote>(&envelope.message)) {
    on_request_vote(envelope.from, *vote_request, effects);
  } else if (const auto* vote_response =
                 std::get_if<RequestVoteResponse>(&envelope.message)) {
    on_request_vote_response(envelope.from, *vote_response, effects);
  } else if (const auto* append_request =
                 std::get_if<AppendEntries>(&envelope.message)) {
    on_append_entries(envelope.from, *append_request, effects);
  } else if (const auto* append_response =
                 std::get_if<AppendEntriesResponse>(&envelope.message)) {
    on_append_entries_response(envelope.from, *append_response, effects);
  }
}

void RaftNode::on_storage_complete(const StorageComplete& complete,
                                   std::vector<RaftEffect>& effects) {
  if (!pending_storage_ || pending_storage_->op_id != complete.op_id) {
    trace(TraceKind::fault, effects, "unexpected storage completion");
    return;
  }

  PendingStorage pending = std::move(*pending_storage_);
  pending_storage_.reset();
  if (!complete.success) {
    fail_storage(complete.error.empty() ? "storage operation failed"
                                        : complete.error,
                 effects);
    return;
  }

  if (pending.batch.hard_state) {
    durable_hard_state_ = *pending.batch.hard_state;
  }
  if (pending.batch.truncate_from || !pending.batch.entries.empty()) {
    durable_last_log_index_ = last_log_index();
  }
  if (pending.batch.commit_index) {
    durable_commit_index_ = *pending.batch.commit_index;
  }
  trace(TraceKind::storage_complete, effects, "storage operation completed");
  if (pending.after_success) {
    pending.after_success(effects);
  }
}

void RaftNode::begin_election(std::vector<RaftEffect>& effects) {
  if (hard_state_.current_term == std::numeric_limits<Term>::max()) {
    fail_storage("term counter overflow", effects);
    return;
  }
  role_ = RaftRole::candidate;
  ++hard_state_.current_term;
  hard_state_.voted_for = config_.node_id;
  leader_hint_.reset();
  leader_ready_ = false;
  readiness_index_ = 0;
  peers_.clear();
  votes_received_.clear();
  votes_received_.insert(config_.node_id);
  ++heartbeat_generation_;
  trace(TraceKind::term_change, effects, "started election");
  trace(TraceKind::role_change, effects, "became candidate");
  trace(TraceKind::vote, effects, "recorded self vote");

  PersistBatch batch;
  batch.hard_state = hard_state_;
  begin_persist(
      std::move(batch),
      [this](std::vector<RaftEffect>& next_effects) {
        if (role_ != RaftRole::candidate) {
          return;
        }
        schedule_election(next_effects);
        const RequestVote request{
            .term = hard_state_.current_term,
            .candidate_id = config_.node_id,
            .last_log_index = durable_last_log_index_,
            .last_log_term = term_at(durable_last_log_index_),
        };
        for (const NodeId member : config_.members) {
          if (member != config_.node_id) {
            send_message(member, request, next_effects);
          }
        }
      },
      effects);
}

void RaftNode::on_request_vote(NodeId from, const RequestVote& request,
                               std::vector<RaftEffect>& effects) {
  if (request.term == 0 || request.candidate_id != from) {
    return;
  }
  if (request.term < hard_state_.current_term) {
    send_message(from,
                 RequestVoteResponse{.term = hard_state_.current_term,
                                     .vote_granted = false},
                 effects);
    return;
  }

  PersistBatch batch;
  const bool was_leader = role_ == RaftRole::leader;
  const bool observed_higher_term = request.term > hard_state_.current_term;
  if (observed_higher_term) {
    step_down(request.term, std::nullopt, effects, false);
    batch.hard_state = hard_state_;
  }

  const bool can_vote = !hard_state_.voted_for ||
                        *hard_state_.voted_for == request.candidate_id;
  const bool grant = can_vote && candidate_is_up_to_date(
                                     request.last_log_index,
                                     request.last_log_term);
  if (grant && hard_state_.voted_for != request.candidate_id) {
    hard_state_.voted_for = request.candidate_id;
    batch.hard_state = hard_state_;
  }

  auto reply = [this, from, grant, was_leader](
                   std::vector<RaftEffect>& next_effects) {
    if (grant) {
      trace(TraceKind::vote, next_effects, "granted vote");
    }
    // A leader invalidates its election timer. If a higher-term but stale
    // candidate makes it step down, denying that candidate must still arm a
    // new timer or the surviving quorum can deadlock without any campaigner.
    if (grant || was_leader) {
      schedule_election(next_effects);
    }
    send_message(from,
                 RequestVoteResponse{.term = hard_state_.current_term,
                                     .vote_granted = grant},
                 next_effects);
  };
  if (batch.empty()) {
    reply(effects);
  } else {
    begin_persist(std::move(batch), std::move(reply), effects);
  }
}

void RaftNode::on_request_vote_response(
    NodeId from, const RequestVoteResponse& response,
    std::vector<RaftEffect>& effects) {
  if (response.term > hard_state_.current_term) {
    step_down(response.term, std::nullopt, effects, false);
    PersistBatch batch;
    batch.hard_state = hard_state_;
    begin_persist(
        std::move(batch),
        [this](std::vector<RaftEffect>& next_effects) {
          schedule_election(next_effects);
        },
        effects);
    return;
  }
  if (role_ != RaftRole::candidate ||
      response.term != hard_state_.current_term || !response.vote_granted) {
    return;
  }
  if (votes_received_.insert(from).second) {
    trace(TraceKind::vote, effects, "received vote");
  }
  if (votes_received_.size() >= quorum_size()) {
    become_leader(effects);
  }
}

void RaftNode::become_leader(std::vector<RaftEffect>& effects) {
  role_ = RaftRole::leader;
  leader_hint_ = config_.node_id;
  leader_ready_ = false;
  readiness_index_ = 0;
  votes_received_.clear();
  ++election_generation_;
  peers_.clear();
  for (const NodeId member : config_.members) {
    if (member != config_.node_id) {
      peers_.emplace(member, PeerProgress{
                                   .next_index = durable_last_log_index_ + 1,
                                   .match_index = 0,
                                   .inflight = std::nullopt,
                                   .inflight_heartbeat_age = 0,
                               });
    }
  }
  trace(TraceKind::role_change, effects, "became leader");

  if (last_log_index() == std::numeric_limits<LogIndex>::max()) {
    fail_storage("log index overflow", effects);
    return;
  }
  LogEntry no_op{.index = last_log_index() + 1,
                 .term = hard_state_.current_term,
                 .command = std::nullopt};
  if (!can_append(no_op)) {
    trace(TraceKind::queue_saturated, effects,
          "no room for leader readiness entry");
    role_ = RaftRole::follower;
    leader_hint_.reset();
    peers_.clear();
    schedule_election(effects);
    return;
  }

  log_.push_back(no_op);
  readiness_index_ = no_op.index;
  trace(TraceKind::log_change, effects, "appended leader readiness no-op",
        no_op.index);
  PersistBatch batch;
  batch.entries.push_back(no_op);
  begin_persist(
      std::move(batch),
      [this](std::vector<RaftEffect>& next_effects) {
        if (role_ != RaftRole::leader) {
          return;
        }
        send_all_appends(next_effects);
        schedule_heartbeat(next_effects);
        maybe_advance_commit(next_effects);
      },
      effects);
}

void RaftNode::on_append_entries(NodeId from, const AppendEntries& request,
                                 std::vector<RaftEffect>& effects) {
  if (request.term == 0 || request.leader_id != from) {
    return;
  }
  if (request.term < hard_state_.current_term) {
    send_message(from,
                 AppendEntriesResponse{
                     .term = hard_state_.current_term,
                     .rpc_id = request.rpc_id,
                     .success = false,
                     .match_index = 0,
                     .conflict_index = last_log_index() + 1,
                     .conflict_term = 0,
                 },
                 effects);
    return;
  }

  PersistBatch batch;
  if (request.term > hard_state_.current_term) {
    step_down(request.term, from, effects, false);
    batch.hard_state = hard_state_;
  } else {
    if (role_ != RaftRole::follower) {
      step_down(request.term, from, effects, false);
    } else {
      leader_hint_ = from;
    }
  }

  bool entries_valid = request.entries.size() <=
                       config_.max_append_entries_per_rpc;
  std::size_t append_bytes = kAppendRpcOverhead;
  LogIndex expected_index = request.prev_log_index;
  for (const LogEntry& entry : request.entries) {
    if (expected_index == std::numeric_limits<LogIndex>::max()) {
      entries_valid = false;
      break;
    }
    ++expected_index;
    const std::size_t bytes = entry_bytes(entry);
    if (bytes > config_.max_append_bytes_per_rpc - append_bytes) {
      entries_valid = false;
      break;
    }
    append_bytes += bytes;
    if (entry.index != expected_index || entry.term == 0 ||
        entry.term > request.term ||
        (entry.command &&
         (!KvStateMachine::structurally_valid(*entry.command) ||
          !command_size_within(*entry.command, config_.max_command_bytes,
                               config_.max_key_bytes,
                               config_.max_value_bytes)))) {
      entries_valid = false;
      break;
    }
  }

  auto reject = [this, from, rpc_id = request.rpc_id,
                 conflict_index = last_log_index() + 1,
                 conflict_term = Term{0}](
                    std::vector<RaftEffect>& next_effects) {
    schedule_election(next_effects);
    send_message(from,
                 AppendEntriesResponse{
                     .term = hard_state_.current_term,
                     .rpc_id = rpc_id,
                     .success = false,
                     .match_index = 0,
                     .conflict_index = conflict_index,
                     .conflict_term = conflict_term,
                 },
                 next_effects);
  };

  if (!entries_valid) {
    if (batch.empty()) {
      reject(effects);
    } else {
      begin_persist(std::move(batch), std::move(reject), effects);
    }
    return;
  }

  if (request.prev_log_index > last_log_index()) {
    const LogIndex conflict = last_log_index() + 1;
    auto missing = [this, from, rpc_id = request.rpc_id, conflict](
                       std::vector<RaftEffect>& next_effects) {
      schedule_election(next_effects);
      send_message(from,
                   AppendEntriesResponse{
                       .term = hard_state_.current_term,
                       .rpc_id = rpc_id,
                       .success = false,
                       .match_index = 0,
                       .conflict_index = conflict,
                       .conflict_term = 0,
                   },
                   next_effects);
    };
    if (batch.empty()) {
      missing(effects);
    } else {
      begin_persist(std::move(batch), std::move(missing), effects);
    }
    return;
  }

  if (term_at(request.prev_log_index) != request.prev_log_term) {
    const Term conflict_term = term_at(request.prev_log_index);
    const LogIndex conflict_index =
        first_index_of_term(request.prev_log_index);
    auto mismatch = [this, from, rpc_id = request.rpc_id, conflict_index,
                     conflict_term](std::vector<RaftEffect>& next_effects) {
      schedule_election(next_effects);
      send_message(from,
                   AppendEntriesResponse{
                       .term = hard_state_.current_term,
                       .rpc_id = rpc_id,
                       .success = false,
                       .match_index = 0,
                       .conflict_index = conflict_index,
                       .conflict_term = conflict_term,
                   },
                   next_effects);
    };
    if (batch.empty()) {
      mismatch(effects);
    } else {
      begin_persist(std::move(batch), std::move(mismatch), effects);
    }
    return;
  }

  std::size_t incoming = 0;
  while (incoming < request.entries.size()) {
    const LogEntry& entry = request.entries[incoming];
    const LogEntry* local = entry_at(entry.index);
    if (local == nullptr || *local != entry) {
      break;
    }
    ++incoming;
  }

  if (incoming < request.entries.size()) {
    const LogIndex replacement_index = request.entries[incoming].index;
    if (replacement_index <= commit_index_) {
      fail_storage("leader attempted to overwrite a committed entry", effects);
      return;
    }
    if (replacement_index <= last_log_index()) {
      batch.truncate_from = replacement_index;
      remove_pending_from(replacement_index, effects);
      log_.resize(static_cast<std::size_t>(replacement_index - 1));
    }
    batch.entries.assign(request.entries.begin() +
                             static_cast<std::ptrdiff_t>(incoming),
                         request.entries.end());
    log_.insert(log_.end(), batch.entries.begin(), batch.entries.end());
    trace(TraceKind::log_change, effects, "accepted replicated log suffix",
          replacement_index);
  }

  const LogIndex match_index =
      request.entries.empty() ? request.prev_log_index
                              : request.entries.back().index;
  const LogIndex new_commit = std::min(request.leader_commit, match_index);
  if (new_commit > commit_index_) {
    commit_index_ = new_commit;
    batch.commit_index = new_commit;
  }
  auto accept = [this, from, rpc_id = request.rpc_id, match_index,
                 apply = batch.commit_index.has_value()](
                    std::vector<RaftEffect>& next_effects) {
    if (apply) {
      trace(TraceKind::commit, next_effects,
            "learned committed prefix from leader", commit_index_);
      apply_committed(next_effects);
    }
    schedule_election(next_effects);
    send_message(from,
                 AppendEntriesResponse{
                     .term = hard_state_.current_term,
                     .rpc_id = rpc_id,
                     .success = true,
                     .match_index = match_index,
                     .conflict_index = 0,
                     .conflict_term = 0,
                 },
                 next_effects);
  };
  if (batch.empty()) {
    accept(effects);
  } else {
    begin_persist(std::move(batch), std::move(accept), effects);
  }
}

void RaftNode::on_append_entries_response(
    NodeId from, const AppendEntriesResponse& response,
    std::vector<RaftEffect>& effects) {
  if (response.term > hard_state_.current_term) {
    step_down(response.term, std::nullopt, effects, false);
    PersistBatch batch;
    batch.hard_state = hard_state_;
    begin_persist(
        std::move(batch),
        [this](std::vector<RaftEffect>& next_effects) {
          schedule_election(next_effects);
        },
        effects);
    return;
  }
  if (role_ != RaftRole::leader ||
      response.term != hard_state_.current_term) {
    return;
  }
  const auto progress_it = peers_.find(from);
  if (progress_it == peers_.end() || !progress_it->second.inflight ||
      progress_it->second.inflight->rpc_id != response.rpc_id) {
    return;
  }

  PeerProgress& progress = progress_it->second;
  const InflightAppend sent = *progress.inflight;
  progress.inflight.reset();
  progress.inflight_heartbeat_age = 0;
  if (response.success) {
    if (response.match_index < sent.last_index) {
      return;
    }
    progress.match_index = std::max(progress.match_index, sent.last_index);
    progress.next_index = progress.match_index + 1;
    maybe_advance_commit(effects);
    if (role_ == RaftRole::leader && !pending_storage_) {
      const auto current = peers_.find(from);
      if (current != peers_.end() &&
          current->second.match_index < durable_last_log_index_) {
        send_append(from, effects);
      }
    }
    return;
  }

  LogIndex next = response.conflict_index;
  if (response.conflict_term != 0) {
    const auto matching = last_index_of_term(response.conflict_term);
    if (matching) {
      next = *matching + 1;
    }
  }
  if (next == 0) {
    next = progress.next_index > 1 ? progress.next_index - 1 : 1;
  }
  next = std::min(next, durable_last_log_index_ + 1);
  next = std::max(next, progress.match_index + 1);
  progress.next_index = next;
  send_append(from, effects);
}

void RaftNode::on_client_request(ClientRequest request,
                                 std::vector<RaftEffect>& effects) {
  if (role_ == RaftRole::unavailable) {
    emit_reply(request.token, ClientStatus::unavailable, effects);
    return;
  }
  if (role_ != RaftRole::leader) {
    emit_reply(request.token, ClientStatus::not_leader, effects);
    return;
  }
  if (!leader_ready_) {
    emit_reply(request.token, ClientStatus::leader_not_ready, effects);
    return;
  }
  if (!KvStateMachine::structurally_valid(request.command) ||
      !command_size_within(request.command, config_.max_command_bytes,
                           config_.max_key_bytes,
                           config_.max_value_bytes)) {
    emit_reply(request.token, ClientStatus::invalid_request, effects);
    return;
  }

  const std::uint64_t digest =
      KvStateMachine::command_digest(request.command);
  const auto applied = state_machine_.session(request.command.session);
  if (applied) {
    if (request.command.sequence < applied->sequence) {
      emit_reply(request.token, ClientStatus::stale_sequence, effects);
      return;
    }
    if (request.command.sequence == applied->sequence) {
      if (digest != applied->command_digest) {
        emit_reply(request.token, ClientStatus::request_id_conflict, effects);
      } else {
        emit_reply(request.token, applied->status, effects, applied->result,
                   last_applied_);
      }
      return;
    }
    if (applied->sequence == std::numeric_limits<std::uint64_t>::max() ||
        request.command.sequence != applied->sequence + 1) {
      emit_reply(request.token, ClientStatus::sequence_gap, effects);
      return;
    }
  } else if (request.command.sequence != 1) {
    emit_reply(request.token, ClientStatus::sequence_gap, effects);
    return;
  }

  const auto pending_session = pending_sessions_.find(request.command.session);
  if (pending_session != pending_sessions_.end()) {
    auto pending = pending_clients_.find(pending_session->second);
    if (pending == pending_clients_.end()) {
      pending_sessions_.erase(pending_session);
    } else {
      if (pending->second.command.sequence != request.command.sequence) {
        emit_reply(request.token, ClientStatus::sequence_gap, effects);
      } else if (pending->second.digest != digest) {
        emit_reply(request.token, ClientStatus::request_id_conflict, effects);
      } else if (pending->second.waiters.size() >=
                 config_.max_waiters_per_request) {
        emit_reply(request.token, ClientStatus::busy, effects);
      } else {
        pending->second.waiters.push_back(request.token);
      }
      return;
    }
  }

  if (last_log_index() == std::numeric_limits<LogIndex>::max()) {
    emit_reply(request.token, ClientStatus::busy, effects);
    return;
  }
  LogEntry entry{
      .index = last_log_index() + 1,
      .term = hard_state_.current_term,
      .command = request.command,
  };
  if (!can_append_client(entry)) {
    trace(TraceKind::queue_saturated, effects,
          "uncommitted log admission bound reached");
    emit_reply(request.token, ClientStatus::busy, effects);
    return;
  }

  log_.push_back(entry);
  pending_sessions_[request.command.session] = entry.index;
  pending_clients_[entry.index] = PendingClientEntry{
      .command = std::move(request.command),
      .digest = digest,
      .waiters = {request.token},
  };
  trace(TraceKind::log_change, effects, "appended client command",
        entry.index);
  PersistBatch batch;
  batch.entries.push_back(std::move(entry));
  begin_persist(
      std::move(batch),
      [this](std::vector<RaftEffect>& next_effects) {
        if (role_ != RaftRole::leader) {
          return;
        }
        send_all_appends(next_effects);
        maybe_advance_commit(next_effects);
      },
      effects);
}

void RaftNode::step_down(Term term, std::optional<NodeId> leader_hint,
                         std::vector<RaftEffect>& effects,
                         bool emit_timer) {
  const RaftRole old_role = role_;
  if (term > hard_state_.current_term) {
    hard_state_.current_term = term;
    hard_state_.voted_for.reset();
    trace(TraceKind::term_change, effects, "observed a higher term");
  }
  role_ = RaftRole::follower;
  leader_hint_ = leader_hint;
  leader_ready_ = false;
  readiness_index_ = 0;
  votes_received_.clear();
  peers_.clear();
  ++heartbeat_generation_;
  if (old_role != RaftRole::follower) {
    trace(TraceKind::role_change, effects, "became follower");
  }
  if (emit_timer) {
    schedule_election(effects);
  }
}

void RaftNode::begin_persist(
    PersistBatch batch,
    std::function<void(std::vector<RaftEffect>&)> after_success,
    std::vector<RaftEffect>& effects) {
  if (batch.empty()) {
    if (after_success) {
      after_success(effects);
    }
    return;
  }
  if (pending_storage_) {
    throw std::logic_error("only one Raft storage operation may be outstanding");
  }
  if (next_storage_op_id_ == 0) {
    fail_storage("storage operation id overflow", effects);
    return;
  }
  const StorageOpId op_id = next_storage_op_id_++;
  pending_storage_ = PendingStorage{
      .op_id = op_id,
      .batch = std::move(batch),
      .after_success = std::move(after_success),
  };
  trace(TraceKind::storage_begin, effects, "storage operation started");
  effects.emplace_back(
      PersistEffect{.op_id = op_id, .batch = pending_storage_->batch});
}

void RaftNode::fail_storage(const std::string& detail,
                            std::vector<RaftEffect>& effects) {
  role_ = RaftRole::unavailable;
  leader_hint_.reset();
  leader_ready_ = false;
  peers_.clear();
  votes_received_.clear();
  pending_storage_.reset();
  ++election_generation_;
  ++heartbeat_generation_;
  trace(TraceKind::fault, effects, detail);

  for (const auto& [index, pending] : pending_clients_) {
    for (const ClientToken token : pending.waiters) {
      emit_reply(token, ClientStatus::storage_error, effects, {}, index);
    }
  }
  pending_clients_.clear();
  pending_sessions_.clear();
  for (const RaftEvent& event : deferred_events_) {
    if (const auto* request = std::get_if<ClientRequest>(&event)) {
      emit_reply(request->token, ClientStatus::storage_error, effects);
    }
  }
  deferred_events_.clear();
}

void RaftNode::schedule_election(std::vector<RaftEffect>& effects) {
  ++election_generation_;
  effects.emplace_back(ScheduleTimerEffect{.timer = TimerRequest{
                                               .kind = TimerKind::election,
                                               .delay = config_.election_timeout,
                                               .generation =
                                                   election_generation_}});
}

void RaftNode::schedule_heartbeat(std::vector<RaftEffect>& effects) {
  ++heartbeat_generation_;
  effects.emplace_back(ScheduleTimerEffect{.timer = TimerRequest{
                                               .kind = TimerKind::heartbeat,
                                               .delay =
                                                   config_.heartbeat_interval,
                                               .generation =
                                                   heartbeat_generation_}});
}

void RaftNode::send_message(NodeId to, Message message,
                            std::vector<RaftEffect>& effects) {
  trace(TraceKind::send, effects, "protocol message emitted");
  effects.emplace_back(SendEffect{.envelope = MessageEnvelope{
                                      .from = config_.node_id,
                                      .to = to,
                                      .message = std::move(message)}});
}

void RaftNode::send_append(NodeId peer,
                           std::vector<RaftEffect>& effects) {
  if (role_ != RaftRole::leader || pending_storage_) {
    return;
  }
  const auto found = peers_.find(peer);
  if (found == peers_.end()) {
    return;
  }
  PeerProgress& progress = found->second;
  if (progress.inflight) {
    return;
  }
  progress.next_index =
      std::clamp(progress.next_index, LogIndex{1},
                 durable_last_log_index_ + 1);
  const LogIndex prev = progress.next_index - 1;
  const LogIndex available = durable_last_log_index_ - prev;
  const LogIndex maximum =
      static_cast<LogIndex>(config_.max_append_entries_per_rpc);
  const LogIndex candidate_count = std::min(available, maximum);
  std::vector<LogEntry> entries;
  entries.reserve(static_cast<std::size_t>(candidate_count));
  std::size_t encoded_bytes = kAppendRpcOverhead;
  for (LogIndex offset = 1; offset <= candidate_count; ++offset) {
    const LogEntry* entry = entry_at(prev + offset);
    if (entry == nullptr) {
      break;
    }
    const std::size_t bytes = entry_bytes(*entry);
    if (bytes > config_.max_append_bytes_per_rpc - encoded_bytes) {
      break;
    }
    encoded_bytes += bytes;
    entries.push_back(*entry);
  }
  if (next_rpc_id_ == 0) {
    fail_storage("RPC id overflow", effects);
    return;
  }
  const RpcId rpc_id = next_rpc_id_++;
  const LogIndex sent_last =
      prev + static_cast<LogIndex>(entries.size());
  progress.inflight = InflightAppend{
      .rpc_id = rpc_id, .prev_index = prev, .last_index = sent_last};
  progress.inflight_heartbeat_age = 0;
  send_message(peer,
               AppendEntries{
                   .term = hard_state_.current_term,
                   .leader_id = config_.node_id,
                   .rpc_id = rpc_id,
                   .prev_log_index = prev,
                   .prev_log_term = term_at(prev),
                   .entries = std::move(entries),
                   .leader_commit = commit_index_,
               },
               effects);
}

void RaftNode::send_all_appends(std::vector<RaftEffect>& effects) {
  if (role_ != RaftRole::leader || pending_storage_) {
    return;
  }
  std::vector<NodeId> peer_ids;
  peer_ids.reserve(peers_.size());
  for (const auto& [peer, progress] : peers_) {
    static_cast<void>(progress);
    peer_ids.push_back(peer);
  }
  for (const NodeId peer : peer_ids) {
    send_append(peer, effects);
  }
}

void RaftNode::maybe_advance_commit(std::vector<RaftEffect>& effects) {
  if (role_ != RaftRole::leader || pending_storage_) {
    return;
  }
  LogIndex target = commit_index_;
  for (LogIndex candidate = durable_last_log_index_;
       candidate > commit_index_; --candidate) {
    const LogEntry* entry = entry_at(candidate);
    if (entry == nullptr || entry->term != hard_state_.current_term) {
      continue;
    }
    std::size_t replicas = 1;  // The leader's durable log.
    for (const auto& [peer, progress] : peers_) {
      static_cast<void>(peer);
      if (progress.match_index >= candidate) {
        ++replicas;
      }
    }
    if (replicas >= quorum_size()) {
      target = candidate;
      break;
    }
  }
  if (target == commit_index_) {
    return;
  }

  commit_index_ = target;
  PersistBatch batch;
  batch.commit_index = target;
  begin_persist(
      std::move(batch),
      [this](std::vector<RaftEffect>& next_effects) {
        trace(TraceKind::commit, next_effects,
              "leader durably advanced commit index", commit_index_);
        apply_committed(next_effects);
        if (role_ == RaftRole::leader && readiness_index_ != 0 &&
            last_applied_ >= readiness_index_) {
          leader_ready_ = true;
        }
        send_all_appends(next_effects);
      },
      effects);
}

void RaftNode::apply_committed(std::vector<RaftEffect>& effects) {
  while (last_applied_ < commit_index_) {
    ++last_applied_;
    const LogEntry* entry = entry_at(last_applied_);
    if (entry == nullptr) {
      fail_storage("commit index references a missing log entry", effects);
      return;
    }

    ApplyResult result;
    if (entry->command) {
      result = state_machine_.apply(*entry->command);
    }
    trace(TraceKind::apply, effects,
          entry->command ? "applied client command" : "applied no-op",
          last_applied_);

    const auto pending = pending_clients_.find(last_applied_);
    if (pending == pending_clients_.end()) {
      continue;
    }
    for (const ClientToken token : pending->second.waiters) {
      emit_reply(token, result.status, effects, result.value, last_applied_);
    }
    const auto session =
        pending_sessions_.find(pending->second.command.session);
    if (session != pending_sessions_.end() &&
        session->second == last_applied_) {
      pending_sessions_.erase(session);
    }
    pending_clients_.erase(pending);
  }
}

LogIndex RaftNode::last_log_index() const noexcept {
  return static_cast<LogIndex>(log_.size());
}

bool RaftNode::is_member(NodeId id) const noexcept {
  return std::binary_search(config_.members.begin(), config_.members.end(), id);
}

const LogEntry* RaftNode::entry_at(LogIndex index) const noexcept {
  if (index == 0 || index > last_log_index()) {
    return nullptr;
  }
  return &log_[static_cast<std::size_t>(index - 1)];
}

Term RaftNode::term_at(LogIndex index) const noexcept {
  const LogEntry* entry = entry_at(index);
  return entry == nullptr ? Term{0} : entry->term;
}

bool RaftNode::candidate_is_up_to_date(LogIndex index, Term term) const noexcept {
  const Term local_term = term_at(last_log_index());
  return term > local_term || (term == local_term && index >= last_log_index());
}

std::size_t RaftNode::uncommitted_entry_count() const noexcept {
  return static_cast<std::size_t>(last_log_index() - commit_index_);
}

std::size_t RaftNode::entry_bytes(const LogEntry& entry) noexcept {
  if (!entry.command) {
    return kEntryOverhead;
  }
  const std::size_t key_size = entry.command->key.size();
  const std::size_t value_size = entry.command->value.size();
  if (key_size > std::numeric_limits<std::size_t>::max() - value_size ||
      kEntryOverhead >
          std::numeric_limits<std::size_t>::max() - key_size - value_size) {
    return std::numeric_limits<std::size_t>::max();
  }
  return kEntryOverhead + key_size + value_size;
}

std::size_t RaftNode::uncommitted_bytes() const noexcept {
  std::size_t total = 0;
  for (LogIndex index = commit_index_ + 1; index <= last_log_index(); ++index) {
    const LogEntry* entry = entry_at(index);
    if (entry == nullptr) {
      break;
    }
    const std::size_t bytes = entry_bytes(*entry);
    if (bytes > std::numeric_limits<std::size_t>::max() - total) {
      return std::numeric_limits<std::size_t>::max();
    }
    total += bytes;
  }
  return total;
}

bool RaftNode::can_append(const LogEntry& entry) const noexcept {
  if (entry_bytes(entry) >
      config_.max_append_bytes_per_rpc - kAppendRpcOverhead) {
    return false;
  }
  if (uncommitted_entry_count() >= config_.max_uncommitted_entries) {
    return false;
  }
  const std::size_t current = uncommitted_bytes();
  const std::size_t added = entry_bytes(entry);
  return current <= config_.max_uncommitted_bytes &&
         added <= config_.max_uncommitted_bytes - current;
}

bool RaftNode::can_append_client(const LogEntry& entry) const noexcept {
  // Keep one entry and one no-op's worth of bytes available so a future leader
  // can establish a current-term commit barrier for this suffix.
  const std::size_t count = uncommitted_entry_count();
  if (count >= config_.max_uncommitted_entries ||
      config_.max_uncommitted_entries - count <= 1) {
    return false;
  }
  const std::size_t current = uncommitted_bytes();
  const std::size_t added = entry_bytes(entry);
  if (current > config_.max_uncommitted_bytes ||
      added > config_.max_uncommitted_bytes - current) {
    return false;
  }
  const std::size_t after_entry = current + added;
  return after_entry <= config_.max_uncommitted_bytes &&
         kEntryOverhead <= config_.max_uncommitted_bytes - after_entry;
}

std::optional<LogIndex> RaftNode::last_index_of_term(Term term) const {
  for (auto entry = log_.rbegin(); entry != log_.rend(); ++entry) {
    if (entry->term == term) {
      return entry->index;
    }
  }
  return std::nullopt;
}

LogIndex RaftNode::first_index_of_term(LogIndex at) const noexcept {
  if (at == 0 || at > last_log_index()) {
    return last_log_index() + 1;
  }
  const Term term = term_at(at);
  while (at > 1 && term_at(at - 1) == term) {
    --at;
  }
  return at;
}

void RaftNode::remove_pending_from(LogIndex index,
                                   std::vector<RaftEffect>& effects) {
  auto pending = pending_clients_.lower_bound(index);
  while (pending != pending_clients_.end()) {
    for (const ClientToken token : pending->second.waiters) {
      emit_reply(token, ClientStatus::unavailable, effects, {}, pending->first);
    }
    const auto session =
        pending_sessions_.find(pending->second.command.session);
    if (session != pending_sessions_.end() && session->second == pending->first) {
      pending_sessions_.erase(session);
    }
    pending = pending_clients_.erase(pending);
  }
}

void RaftNode::emit_reply(ClientToken token, ClientStatus status,
                          std::vector<RaftEffect>& effects, std::string value,
                          LogIndex index) {
  std::optional<NodeId> hint = leader_hint_;
  if (role_ == RaftRole::leader) {
    hint = config_.node_id;
  }
  effects.emplace_back(ClientReplyEffect{.reply = ClientReply{
                                             .token = token,
                                             .status = status,
                                             .leader_hint = hint,
                                             .log_index = index,
                                             .value = std::move(value)}});
  trace(TraceKind::client_reply, effects, "client reply emitted", index);
}

void RaftNode::trace(TraceKind kind, std::vector<RaftEffect>& effects,
                     std::string detail, LogIndex index) const {
  effects.emplace_back(TraceEffect{.record = TraceRecord{
                                       .kind = kind,
                                       .node = config_.node_id,
                                       .term = hard_state_.current_term,
                                       .index = index,
                                       .detail = std::move(detail)}});
}

}  // namespace detlog
