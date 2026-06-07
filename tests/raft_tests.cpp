#include "detlog/raft.hpp"

#include <cstdlib>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace {

using namespace detlog;

[[noreturn]] void fail(const char* expression, int line) {
  throw std::runtime_error("check failed at line " + std::to_string(line) +
                           ": " + expression);
}

#define CHECK(expression)                  \
  do {                                     \
    if (!(expression)) {                   \
      fail(#expression, __LINE__);         \
    }                                      \
  } while (false)

template <class EffectType>
std::vector<EffectType> effects_of(const std::vector<RaftEffect>& effects) {
  std::vector<EffectType> selected;
  for (const RaftEffect& effect : effects) {
    if (const auto* value = std::get_if<EffectType>(&effect)) {
      selected.push_back(*value);
    }
  }
  return selected;
}

RaftConfig config_for(NodeId node_id) {
  return RaftConfig{.node_id = node_id, .members = {1, 2, 3}};
}

MessageReceived received(NodeId from, NodeId to, Message message) {
  return MessageReceived{.envelope = MessageEnvelope{
                             .from = from,
                             .to = to,
                             .message = std::move(message)}};
}

PersistEffect only_persist(const std::vector<RaftEffect>& effects) {
  const auto persists = effects_of<PersistEffect>(effects);
  CHECK(persists.size() == 1);
  return persists.front();
}

std::uint64_t election_generation(const std::vector<RaftEffect>& effects) {
  for (const ScheduleTimerEffect& timer :
       effects_of<ScheduleTimerEffect>(effects)) {
    if (timer.timer.kind == TimerKind::election) {
      return timer.timer.generation;
    }
  }
  fail("election timer exists", __LINE__);
}

std::vector<AppendEntries> appends_to(const std::vector<RaftEffect>& effects,
                                      NodeId peer) {
  std::vector<AppendEntries> appends;
  for (const SendEffect& send : effects_of<SendEffect>(effects)) {
    if (send.envelope.to == peer) {
      if (const auto* append =
              std::get_if<AppendEntries>(&send.envelope.message)) {
        appends.push_back(*append);
      }
    }
  }
  return appends;
}

AppendEntries only_append_to(const std::vector<RaftEffect>& effects,
                             NodeId peer) {
  const auto appends = appends_to(effects, peer);
  CHECK(appends.size() == 1);
  return appends.front();
}

LogIndex matched_end(const AppendEntries& append) {
  return append.entries.empty() ? append.prev_log_index
                                : append.entries.back().index;
}

std::vector<RaftEffect> acknowledge(RaftNode& leader, NodeId peer,
                                    const AppendEntries& append,
                                    bool success = true) {
  return leader.step(received(
      peer, leader.id(),
      AppendEntriesResponse{
          .term = leader.current_term(),
          .rpc_id = append.rpc_id,
          .success = success,
          .match_index = success ? matched_end(append) : LogIndex{0},
          .conflict_index = success ? LogIndex{0} : LogIndex{1},
          .conflict_term = 0,
      }));
}

std::vector<RaftEffect> elect_and_ready(RaftNode& node) {
  const auto initial = node.start();
  const auto election = node.step(TimerFired{
      .kind = TimerKind::election,
      .generation = election_generation(initial),
  });
  const PersistEffect term_write = only_persist(election);
  CHECK(effects_of<SendEffect>(election).empty());

  const auto term_durable = node.step(StorageComplete{
      .op_id = term_write.op_id, .success = true, .error = {}});
  CHECK(effects_of<SendEffect>(term_durable).size() == 2);

  const auto elected = node.step(received(
      2, node.id(),
      RequestVoteResponse{.term = node.current_term(), .vote_granted = true}));
  const PersistEffect no_op_write = only_persist(elected);
  CHECK(node.role() == RaftRole::leader);
  CHECK(!node.leader_ready());
  CHECK(effects_of<SendEffect>(elected).empty());

  const auto no_op_durable = node.step(StorageComplete{
      .op_id = no_op_write.op_id, .success = true, .error = {}});
  const AppendEntries append = only_append_to(no_op_durable, 2);
  CHECK(append.entries.size() == 1);
  CHECK(!append.entries.front().command);

  const auto replicated = acknowledge(node, 2, append);
  const PersistEffect commit_write = only_persist(replicated);
  CHECK(!node.leader_ready());
  CHECK(effects_of<ClientReplyEffect>(replicated).empty());

  const auto ready = node.step(StorageComplete{
      .op_id = commit_write.op_id, .success = true, .error = {}});
  CHECK(node.role() == RaftRole::leader);
  CHECK(node.leader_ready());
  CHECK(node.commit_index() == 1);
  CHECK(node.last_applied() == 1);
  return ready;
}

ClientCommand put(SessionId session, std::uint64_t sequence,
                  std::string key, std::string value) {
  return ClientCommand{
      .session = session,
      .sequence = sequence,
      .kind = CommandKind::put,
      .key = std::move(key),
      .value = std::move(value),
  };
}

void test_membership_and_storage_gated_election() {
  RaftNode five(RaftConfig{.node_id = 3, .members = {5, 4, 3, 2, 1}});
  CHECK(five.quorum_size() == 3);
  bool rejected = false;
  try {
    RaftNode invalid(RaftConfig{.node_id = 1, .members = {1, 2, 3, 4}});
    static_cast<void>(invalid);
  } catch (const std::invalid_argument&) {
    rejected = true;
  }
  CHECK(rejected);

  RaftConfig config = config_for(1);
  config.max_deferred_events = 1;
  RaftNode node(config);
  const auto initial = node.start();
  const auto election = node.step(TimerFired{
      .kind = TimerKind::election,
      .generation = election_generation(initial),
  });
  const PersistEffect hard_state = only_persist(election);
  CHECK(hard_state.batch.hard_state.has_value());
  CHECK(hard_state.batch.hard_state->current_term == 1);
  CHECK(hard_state.batch.hard_state->voted_for == 1);
  CHECK(effects_of<SendEffect>(election).empty());

  const auto deferred = node.step(ClientRequest{
      .token = 10, .command = put({1, 1}, 1, "a", "b")});
  CHECK(deferred.empty());
  CHECK(node.deferred_event_count() == 1);
  const auto saturated = node.step(ClientRequest{
      .token = 11, .command = put({1, 2}, 1, "c", "d")});
  const auto busy = effects_of<ClientReplyEffect>(saturated);
  CHECK(busy.size() == 1);
  CHECK(busy.front().reply.status == ClientStatus::busy);

  const auto completed = node.step(StorageComplete{
      .op_id = hard_state.op_id, .success = true, .error = {}});
  CHECK(effects_of<SendEffect>(completed).size() == 2);
  const auto replies = effects_of<ClientReplyEffect>(completed);
  CHECK(replies.size() == 1);
  CHECK(replies.front().reply.token == 10);
  CHECK(replies.front().reply.status == ClientStatus::not_leader);
}

void test_term_zero_vote_is_never_persisted() {
  RaftNode node(config_for(1));
  (void)node.start();
  const auto effects = node.step(received(
      2, 1,
      RequestVote{.term = 0,
                  .candidate_id = 2,
                  .last_log_index = 0,
                  .last_log_term = 0}));
  CHECK(effects_of<PersistEffect>(effects).empty());
  CHECK(effects_of<SendEffect>(effects).empty());
  CHECK(node.current_term() == 0);
  CHECK(!node.voted_for());

  bool rejected_recovery = false;
  try {
    RaftNode invalid(config_for(1),
                     RecoveredState{HardState{0, 2}, {}, 0});
    (void)invalid;
  } catch (const std::invalid_argument&) {
    rejected_recovery = true;
  }
  CHECK(rejected_recovery);
}

void test_vote_freshness_and_durable_grant() {
  RecoveredState recovered{
      .hard_state = HardState{.current_term = 2, .voted_for = std::nullopt},
      .log = {LogEntry{.index = 1, .term = 2, .command = std::nullopt}},
      .commit_index = 0,
  };
  RaftNode node(config_for(1), recovered);

  const auto stale = node.step(received(
      2, 1,
      RequestVote{.term = 3,
                  .candidate_id = 2,
                  .last_log_index = 100,
                  .last_log_term = 1}));
  const PersistEffect term_write = only_persist(stale);
  CHECK(effects_of<SendEffect>(stale).empty());
  const auto denied = node.step(StorageComplete{
      .op_id = term_write.op_id, .success = true, .error = {}});
  const auto denied_sends = effects_of<SendEffect>(denied);
  CHECK(denied_sends.size() == 1);
  const auto* denial =
      std::get_if<RequestVoteResponse>(&denied_sends.front().envelope.message);
  CHECK(denial != nullptr);
  CHECK(!denial->vote_granted);

  const auto fresh = node.step(received(
      3, 1,
      RequestVote{.term = 4,
                  .candidate_id = 3,
                  .last_log_index = 1,
                  .last_log_term = 2}));
  const PersistEffect vote_write = only_persist(fresh);
  CHECK(effects_of<SendEffect>(fresh).empty());
  CHECK(vote_write.batch.hard_state->voted_for == 3);
  const auto granted = node.step(StorageComplete{
      .op_id = vote_write.op_id, .success = true, .error = {}});
  const auto granted_sends = effects_of<SendEffect>(granted);
  CHECK(granted_sends.size() == 1);
  const auto* grant =
      std::get_if<RequestVoteResponse>(&granted_sends.front().envelope.message);
  CHECK(grant != nullptr);
  CHECK(grant->vote_granted);

  const auto second_candidate = node.step(received(
      2, 1,
      RequestVote{.term = 4,
                  .candidate_id = 2,
                  .last_log_index = 1,
                  .last_log_term = 2}));
  CHECK(effects_of<PersistEffect>(second_candidate).empty());
  const auto response = effects_of<SendEffect>(second_candidate);
  CHECK(response.size() == 1);
  CHECK(!std::get<RequestVoteResponse>(response.front().envelope.message)
             .vote_granted);
}

void test_higher_term_stale_candidate_rearms_follower() {
  RaftNode node(config_for(1));
  (void)elect_and_ready(node);
  CHECK(node.role() == RaftRole::leader);

  const Term higher_term = node.current_term() + 1;
  const auto stepped_down = node.step(received(
      2, 1,
      RequestVote{.term = higher_term,
                  .candidate_id = 2,
                  .last_log_index = 0,
                  .last_log_term = 0}));
  CHECK(node.role() == RaftRole::follower);
  const PersistEffect hard_state = only_persist(stepped_down);
  CHECK(effects_of<SendEffect>(stepped_down).empty());

  const auto denied = node.step(StorageComplete{
      .op_id = hard_state.op_id, .success = true, .error = {}});
  const auto timers = effects_of<ScheduleTimerEffect>(denied);
  CHECK(timers.size() == 1);
  CHECK(timers.front().timer.kind == TimerKind::election);
  const std::uint64_t surviving_timer_generation =
      timers.front().timer.generation;
  const auto sends = effects_of<SendEffect>(denied);
  CHECK(sends.size() == 1);
  const auto* response =
      std::get_if<RequestVoteResponse>(&sends.front().envelope.message);
  CHECK(response != nullptr);
  CHECK(response->term == higher_term);
  CHECK(!response->vote_granted);

  // Further stale candidates can raise the term, but must not continually
  // postpone the follower's already-armed election timer.
  const auto denied_again = node.step(received(
      2, 1,
      RequestVote{.term = higher_term + 1,
                  .candidate_id = 2,
                  .last_log_index = 0,
                  .last_log_term = 0}));
  const PersistEffect next_hard_state = only_persist(denied_again);
  const auto next_response = node.step(StorageComplete{
      .op_id = next_hard_state.op_id, .success = true, .error = {}});
  CHECK(effects_of<ScheduleTimerEffect>(next_response).empty());

  const auto campaign = node.step(TimerFired{
      .kind = TimerKind::election,
      .generation = surviving_timer_generation,
  });
  CHECK(node.role() == RaftRole::candidate);
  CHECK(node.current_term() == higher_term + 2);
  (void)only_persist(campaign);
}

void test_current_term_only_direct_commit() {
  RaftConfig config = config_for(1);
  config.max_append_entries_per_rpc = 1;
  RecoveredState recovered{
      .hard_state = HardState{.current_term = 1, .voted_for = std::nullopt},
      .log = {LogEntry{.index = 1, .term = 1, .command = std::nullopt}},
      .commit_index = 0,
  };
  RaftNode node(config, recovered);
  const auto initial = node.start();
  const auto election = node.step(TimerFired{
      .kind = TimerKind::election,
      .generation = election_generation(initial),
  });
  const PersistEffect term_write = only_persist(election);
  static_cast<void>(node.step(StorageComplete{
      .op_id = term_write.op_id, .success = true, .error = {}}));
  const auto elected = node.step(received(
      2, 1,
      RequestVoteResponse{.term = node.current_term(), .vote_granted = true}));
  const PersistEffect no_op_write = only_persist(elected);
  const auto no_op_durable = node.step(StorageComplete{
      .op_id = no_op_write.op_id, .success = true, .error = {}});
  const AppendEntries optimistic_batch = only_append_to(no_op_durable, 2);
  CHECK(optimistic_batch.entries.size() == 1);
  CHECK(optimistic_batch.entries.front().term == 2);
  const auto backed_up = acknowledge(node, 2, optimistic_batch, false);
  const AppendEntries old_term_batch = only_append_to(backed_up, 2);
  CHECK(old_term_batch.entries.size() == 1);
  CHECK(old_term_batch.entries.front().term == 1);

  const auto old_replicated = acknowledge(node, 2, old_term_batch);
  CHECK(node.commit_index() == 0);
  CHECK(effects_of<PersistEffect>(old_replicated).empty());
  const AppendEntries current_term_batch = only_append_to(old_replicated, 2);
  CHECK(current_term_batch.entries.size() == 1);
  CHECK(current_term_batch.entries.front().term == 2);

  const auto current_replicated = acknowledge(node, 2, current_term_batch);
  const PersistEffect commit_write = only_persist(current_replicated);
  CHECK(commit_write.batch.commit_index == 2);
  static_cast<void>(node.step(StorageComplete{
      .op_id = commit_write.op_id, .success = true, .error = {}}));
  CHECK(node.commit_index() == 2);
  CHECK(node.last_applied() == 2);
  CHECK(node.leader_ready());
}

void test_follower_append_ack_and_apply_are_storage_gated() {
  RaftNode follower(config_for(2));
  const ClientCommand command = put({9, 9}, 1, "key", "value");
  const auto received_append = follower.step(received(
      1, 2,
      AppendEntries{
          .term = 1,
          .leader_id = 1,
          .rpc_id = 55,
          .prev_log_index = 0,
          .prev_log_term = 0,
          .entries = {LogEntry{.index = 1, .term = 1, .command = command}},
          .leader_commit = 1,
      }));
  const PersistEffect write = only_persist(received_append);
  CHECK(effects_of<SendEffect>(received_append).empty());
  CHECK(follower.durable_last_log_index() == 0);
  CHECK(follower.last_applied() == 0);

  const auto durable = follower.step(StorageComplete{
      .op_id = write.op_id, .success = true, .error = {}});
  CHECK(follower.durable_last_log_index() == 1);
  CHECK(follower.commit_index() == 1);
  CHECK(follower.last_applied() == 1);
  CHECK(follower.state_machine().value_for_test("key") == "value");
  const auto responses = effects_of<SendEffect>(durable);
  CHECK(responses.size() == 1);
  CHECK(std::get<AppendEntriesResponse>(responses.front().envelope.message)
            .success);
}

void test_heartbeat_cannot_commit_unmatched_local_suffix() {
  const ClientCommand uncommitted = put({7, 7}, 1, "unsafe", "value");
  RecoveredState recovered{
      .hard_state = HardState{.current_term = 2, .voted_for = std::nullopt},
      .log = {
          LogEntry{.index = 1, .term = 1, .command = std::nullopt},
          LogEntry{.index = 2, .term = 1, .command = uncommitted},
      },
      .commit_index = 0,
  };
  RaftNode follower(config_for(2), recovered);
  const auto heartbeat = follower.step(received(
      1, 2,
      AppendEntries{
          .term = 2,
          .leader_id = 1,
          .rpc_id = 77,
          .prev_log_index = 1,
          .prev_log_term = 1,
          .entries = {},
          .leader_commit = 2,
      }));
  const PersistEffect commit_write = only_persist(heartbeat);
  CHECK(commit_write.batch.commit_index == 1);
  const auto complete = follower.step(StorageComplete{
      .op_id = commit_write.op_id, .success = true, .error = {}});
  CHECK(follower.commit_index() == 1);
  CHECK(follower.last_applied() == 1);
  CHECK(!follower.state_machine().value_for_test("unsafe"));
  const auto response = effects_of<SendEffect>(complete);
  CHECK(response.size() == 1);
  CHECK(std::get<AppendEntriesResponse>(response.front().envelope.message)
            .match_index == 1);
}

void test_replicated_dedup_and_client_reply_after_apply() {
  RaftNode leader(config_for(1));
  const auto ready_effects = elect_and_ready(leader);
  const AppendEntries ready_heartbeat = only_append_to(ready_effects, 2);
  const auto heartbeat_ack = acknowledge(leader, 2, ready_heartbeat);
  CHECK(effects_of<PersistEffect>(heartbeat_ack).empty());

  const ClientCommand command = put({100, 200}, 1, "name", "detlog");
  const auto accepted =
      leader.step(ClientRequest{.token = 1000, .command = command});
  const PersistEffect local_write = only_persist(accepted);
  CHECK(effects_of<SendEffect>(accepted).empty());
  CHECK(effects_of<ClientReplyEffect>(accepted).empty());

  const auto retry_while_writing =
      leader.step(ClientRequest{.token = 1001, .command = command});
  CHECK(retry_while_writing.empty());
  CHECK(leader.deferred_event_count() == 1);

  const auto local_durable = leader.step(StorageComplete{
      .op_id = local_write.op_id, .success = true, .error = {}});
  CHECK(effects_of<ClientReplyEffect>(local_durable).empty());
  const AppendEntries append = only_append_to(local_durable, 2);
  CHECK(append.entries.size() == 1);
  CHECK(append.entries.front().command == command);

  const auto replicated = acknowledge(leader, 2, append);
  const PersistEffect commit_write = only_persist(replicated);
  CHECK(effects_of<ClientReplyEffect>(replicated).empty());
  const auto applied = leader.step(StorageComplete{
      .op_id = commit_write.op_id, .success = true, .error = {}});
  const auto replies = effects_of<ClientReplyEffect>(applied);
  CHECK(replies.size() == 2);
  CHECK(replies[0].reply.token == 1000);
  CHECK(replies[1].reply.token == 1001);
  CHECK(replies[0].reply.status == ClientStatus::ok);
  CHECK(replies[0].reply.value == "detlog");
  CHECK(leader.state_machine().value_for_test("name") == "detlog");
  CHECK(leader.log().size() == 2);

  const auto completed_retry =
      leader.step(ClientRequest{.token = 1002, .command = command});
  const auto cached = effects_of<ClientReplyEffect>(completed_retry);
  CHECK(cached.size() == 1);
  CHECK(cached.front().reply.status == ClientStatus::ok);
  CHECK(cached.front().reply.value == "detlog");
  CHECK(effects_of<PersistEffect>(completed_retry).empty());
  CHECK(leader.log().size() == 2);

  ClientCommand conflicting = command;
  conflicting.value = "different";
  const auto conflict =
      leader.step(ClientRequest{.token = 1003, .command = conflicting});
  CHECK(effects_of<ClientReplyEffect>(conflict).front().reply.status ==
        ClientStatus::request_id_conflict);

  ClientCommand gap = command;
  gap.sequence = 3;
  const auto sequence_gap =
      leader.step(ClientRequest{.token = 1004, .command = gap});
  CHECK(effects_of<ClientReplyEffect>(sequence_gap).front().reply.status ==
        ClientStatus::sequence_gap);
}

void test_uncommitted_admission_is_bounded_and_retryable() {
  RaftConfig config = config_for(1);
  config.max_uncommitted_entries = 2;  // One client entry plus readiness reserve.
  RaftNode leader(config);
  const auto ready_effects = elect_and_ready(leader);
  const AppendEntries heartbeat = only_append_to(ready_effects, 2);
  static_cast<void>(acknowledge(leader, 2, heartbeat));

  const auto first = leader.step(ClientRequest{
      .token = 2000, .command = put({2, 1}, 1, "one", "1")});
  const PersistEffect write = only_persist(first);
  const auto queued = leader.step(ClientRequest{
      .token = 2001, .command = put({2, 2}, 1, "two", "2")});
  CHECK(queued.empty());
  const auto durable = leader.step(StorageComplete{
      .op_id = write.op_id, .success = true, .error = {}});
  const auto replies = effects_of<ClientReplyEffect>(durable);
  CHECK(replies.size() == 1);
  CHECK(replies.front().reply.token == 2001);
  CHECK(replies.front().reply.status == ClientStatus::busy);
  CHECK(effects_of<ClientReplyEffect>(first).empty());
}

void test_append_batch_has_byte_bound() {
  RaftConfig config = config_for(1);
  config.max_append_entries_per_rpc = 64;
  config.max_append_bytes_per_rpc = 191;
  RecoveredState recovered{
      .hard_state = HardState{.current_term = 1, .voted_for = std::nullopt},
      .log = {
          LogEntry{.index = 1, .term = 1, .command = std::nullopt},
          LogEntry{.index = 2, .term = 1, .command = std::nullopt},
      },
      .commit_index = 0,
  };
  RaftNode node(config, recovered);
  const auto initial = node.start();
  const auto election = node.step(TimerFired{
      .kind = TimerKind::election,
      .generation = election_generation(initial),
  });
  const PersistEffect term_write = only_persist(election);
  static_cast<void>(node.step(StorageComplete{
      .op_id = term_write.op_id, .success = true, .error = {}}));
  const auto elected = node.step(received(
      2, 1,
      RequestVoteResponse{.term = node.current_term(), .vote_granted = true}));
  const PersistEffect no_op_write = only_persist(elected);
  const auto durable = node.step(StorageComplete{
      .op_id = no_op_write.op_id, .success = true, .error = {}});
  const AppendEntries append = only_append_to(durable, 2);
  CHECK(append.entries.size() == 1);
}

void test_command_field_bounds_match_wire_defaults() {
  RaftNode node(config_for(1));
  (void)elect_and_ready(node);

  const SessionId session{33, 44};
  const auto rejected = node.step(ClientRequest{
      .token = 700,
      .command = put(session, 1, std::string(4097, 'k'), "value"),
  });
  CHECK(effects_of<PersistEffect>(rejected).empty());
  const auto rejected_replies = effects_of<ClientReplyEffect>(rejected);
  CHECK(rejected_replies.size() == 1);
  CHECK(rejected_replies.front().reply.status ==
        ClientStatus::invalid_request);

  const auto accepted = node.step(ClientRequest{
      .token = 701,
      .command = put(session, 1, std::string(4096, 'k'), "value"),
  });
  (void)only_persist(accepted);
  CHECK(effects_of<ClientReplyEffect>(accepted).empty());
}

void test_oversized_events_are_not_retained_behind_storage() {
  RaftNode node(config_for(1));
  const auto initial = node.start();
  const auto election = node.step(TimerFired{
      .kind = TimerKind::election,
      .generation = election_generation(initial),
  });
  (void)only_persist(election);
  CHECK(node.storage_pending());

  const auto rejected = node.step(ClientRequest{
      .token = 801,
      .command = put(SessionId{5, 6}, 1, std::string(4097, 'k'), "value"),
  });
  CHECK(node.deferred_event_count() == 0);
  const auto replies = effects_of<ClientReplyEffect>(rejected);
  CHECK(replies.size() == 1);
  CHECK(replies.front().reply.status == ClientStatus::invalid_request);

  LogEntry oversized_entry{
      .index = 1,
      .term = 1,
      .command = put(SessionId{7, 8}, 1, "key",
                     std::string(65U * 1024U, 'v')),
  };
  const auto dropped = node.step(received(
      2, 1,
      AppendEntries{.term = 1,
                    .leader_id = 2,
                    .rpc_id = 1,
                    .prev_log_index = 0,
                    .prev_log_term = 0,
                    .entries = {std::move(oversized_entry)},
                    .leader_commit = 0}));
  CHECK(node.deferred_event_count() == 0);
  CHECK(!effects_of<TraceEffect>(dropped).empty());
}

void test_state_machine_retry_contract() {
  KvStateMachine state;
  const ClientCommand command = put({8, 8}, 1, "k", "v");
  const ApplyResult first = state.apply(command);
  CHECK(first.status == ClientStatus::ok);
  CHECK(first.executed);
  const ApplyResult duplicate = state.apply(command);
  CHECK(duplicate.status == ClientStatus::ok);
  CHECK(duplicate.duplicate);
  CHECK(!duplicate.executed);
  ClientCommand conflict = command;
  conflict.value = "other";
  CHECK(state.apply(conflict).status == ClientStatus::request_id_conflict);
  ClientCommand gap = command;
  gap.sequence = 3;
  CHECK(state.apply(gap).status == ClientStatus::sequence_gap);
}

}  // namespace

int main() {
  try {
    test_membership_and_storage_gated_election();
    test_term_zero_vote_is_never_persisted();
    test_vote_freshness_and_durable_grant();
    test_higher_term_stale_candidate_rearms_follower();
    test_current_term_only_direct_commit();
    test_follower_append_ack_and_apply_are_storage_gated();
    test_heartbeat_cannot_commit_unmatched_local_suffix();
    test_replicated_dedup_and_client_reply_after_apply();
    test_uncommitted_admission_is_bounded_and_retryable();
    test_append_batch_has_byte_bound();
    test_command_field_bounds_match_wire_defaults();
    test_oversized_events_are_not_retained_behind_storage();
    test_state_machine_retry_contract();
    std::cout << "raft_tests: all checks passed\n";
    return EXIT_SUCCESS;
  } catch (const std::exception& error) {
    std::cerr << "raft_tests: " << error.what() << '\n';
    return EXIT_FAILURE;
  }
}
