#include "detlog/cluster.hpp"
#include "detlog/reference_model.hpp"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace {

[[noreturn]] void fail(std::string message, int line) {
  throw std::runtime_error("line " + std::to_string(line) + ": " + message);
}

#define REQUIRE(condition)                                                   \
  do {                                                                       \
    if (!(condition)) fail("requirement failed: " #condition, __LINE__);    \
  } while (false)

using ReferenceModel = detlog::reference::Model;

[[nodiscard]] detlog::ClientCommand command(
    detlog::SessionId session, std::uint64_t sequence,
    detlog::CommandKind kind, std::string key, std::string value = {}) {
  return detlog::ClientCommand{session, sequence, kind, std::move(key),
                               std::move(value)};
}

detlog::reference::ActionResult apply(
    ReferenceModel& model, detlog::reference::Action action) {
  auto result = model.apply(action);
  if (!result) {
    fail("reference action rejected: " + result.error, __LINE__);
  }
  return result;
}

[[nodiscard]] bool prefix_equal(const std::vector<detlog::LogEntry>& lhs,
                                const std::vector<detlog::LogEntry>& rhs,
                                detlog::LogIndex through) {
  if (through > lhs.size() || through > rhs.size()) {
    return false;
  }
  for (detlog::LogIndex index = 1; index <= through; ++index) {
    const std::size_t offset = static_cast<std::size_t>(index - 1U);
    if (lhs[offset] != rhs[offset]) {
      return false;
    }
  }
  return true;
}

void wait_for_committed_prefix(detlog::Cluster& cluster,
                               detlog::NodeId leader,
                               detlog::LogIndex through,
                               bool require_every_member = true) {
  REQUIRE(cluster.run_until(
      [=](const detlog::Cluster& value) {
        const auto source = value.snapshot(leader);
        if (!source || !source->alive || source->commit_index < through) {
          return false;
        }
        for (const detlog::NodeId member : value.members()) {
          const auto replica = value.snapshot(member);
          if (!replica) {
            return false;
          }
          if (!replica->alive) {
            if (require_every_member) {
              return false;
            }
            continue;
          }
          if (replica->commit_index < through ||
              replica->last_applied < through ||
              !prefix_equal(source->log, replica->log, through)) {
            return false;
          }
        }
        return true;
      },
      20'000));
}

[[nodiscard]] detlog::NodeId ready_leader(detlog::Cluster& cluster) {
  const auto leader = cluster.run_until_leader(20'000, true);
  REQUIRE(leader.has_value());
  return *leader;
}

[[nodiscard]] detlog::ClientReply submit_and_wait(
    detlog::Cluster& cluster, detlog::NodeId leader,
    const detlog::ClientCommand& request) {
  const detlog::ClientToken token = cluster.submit(leader, request);
  REQUIRE(cluster.run_until_reply(token, 20'000));
  const auto reply = cluster.reply_for(token);
  REQUIRE(reply.has_value());
  return *reply;
}

void mirror_first_election(detlog::Cluster& cluster, ReferenceModel& model,
                           detlog::NodeId leader) {
  const auto leader_snapshot = cluster.snapshot(leader);
  REQUIRE(leader_snapshot.has_value());
  REQUIRE(leader_snapshot->term != 0);
  REQUIRE(leader_snapshot->log.size() == 1);
  REQUIRE(!leader_snapshot->log.front().command.has_value());
  wait_for_committed_prefix(cluster, leader, 1);

  apply(model, detlog::reference::Elect{leader, leader_snapshot->term,
                                        cluster.members()});
  const auto append = apply(model, detlog::reference::AppendNoOp{leader});
  REQUIRE(append.appended_index == std::optional<detlog::LogIndex>{1});
  for (const detlog::NodeId member : cluster.members()) {
    if (member != leader) {
      apply(model, detlog::reference::ReplicatePrefix{leader, member, 1});
    }
  }
  apply(model,
        detlog::reference::CommitPrefix{leader, 1, cluster.members()});
  for (const detlog::NodeId member : cluster.members()) {
    if (member != leader) {
      apply(model, detlog::reference::LearnCommit{leader, member, 1});
    }
  }
}

void require_same_replicas(
    const detlog::Cluster& cluster, const ReferenceModel& model,
    const std::vector<std::string_view>& observed_keys) {
  for (const detlog::NodeId member : cluster.members()) {
    const auto actual = cluster.snapshot(member);
    const auto expected = model.replica(member);
    REQUIRE(actual.has_value());
    REQUIRE(expected.has_value());
    REQUIRE(actual->alive == expected->alive);
    REQUIRE(actual->term == expected->current_term);
    REQUIRE(actual->log == expected->log);
    REQUIRE(actual->commit_index == expected->commit_index);
    if (actual->alive) {
      REQUIRE(actual->last_applied == expected->last_applied);
      REQUIRE(actual->leader_ready == expected->leader_ready);
    }
    for (const std::string_view key : observed_keys) {
      REQUIRE(cluster.value(member, key) == model.value(member, key));
    }
  }
}

[[nodiscard]] detlog::ClientReply commit_command(
    detlog::Cluster& cluster, ReferenceModel& model, detlog::NodeId leader,
    const detlog::ClientCommand& request) {
  const auto actual_reply = submit_and_wait(cluster, leader, request);
  const auto proposal =
      apply(model, detlog::reference::Propose{leader, request});
  REQUIRE(proposal.proposal.has_value());
  REQUIRE(proposal.proposal->appended);
  REQUIRE(proposal.appended_index.has_value());
  REQUIRE(actual_reply.log_index == *proposal.appended_index);

  for (const detlog::NodeId member : cluster.members()) {
    if (member != leader) {
      apply(model, detlog::reference::ReplicatePrefix{
                       leader, member, *proposal.appended_index});
    }
  }
  apply(model, detlog::reference::CommitPrefix{
                   leader, *proposal.appended_index, cluster.members()});
  for (const detlog::NodeId member : cluster.members()) {
    if (member != leader) {
      apply(model, detlog::reference::LearnCommit{
                       leader, member, *proposal.appended_index});
    }
  }
  wait_for_committed_prefix(cluster, leader, *proposal.appended_index);

  const auto expected = model.applied_outcome(leader, *proposal.appended_index);
  REQUIRE(expected.has_value());
  REQUIRE(actual_reply.status == expected->status);
  REQUIRE(actual_reply.value == expected->value);
  return actual_reply;
}

void abstract_history_rejects_missing_quorums() {
  ReferenceModel model({1, 2, 3});
  const std::vector<detlog::reference::Action> invalid_history{
      detlog::reference::Elect{1, 1, {1}}};
  const auto history = model.apply(invalid_history);
  REQUIRE(!history);
  REQUIRE(history.applied_actions == 0);
  REQUIRE(history.error.find("quorum") != std::string::npos);

  apply(model, detlog::reference::Elect{1, 1, {1, 2}});
  apply(model, detlog::reference::AppendNoOp{1});
  apply(model, detlog::reference::ReplicatePrefix{1, 2, 1});
  const auto bad_commit = model.apply(
      detlog::reference::CommitPrefix{1, 1, {1, 3}});
  REQUIRE(!bad_commit);
  REQUIRE(bad_commit.error.find("lacks") != std::string::npos);
  REQUIRE(model.replica(1)->commit_index == 0);

  apply(model, detlog::reference::CommitPrefix{1, 1, {1, 2}});
  REQUIRE(model.replica(1)->leader_ready);
}

void abstract_restart_does_not_restore_volatile_leadership() {
  ReferenceModel model({1, 2, 3});
  apply(model, detlog::reference::Elect{1, 1, {1, 2}});
  apply(model, detlog::reference::AppendNoOp{1});
  apply(model, detlog::reference::ReplicatePrefix{1, 2, 1});
  apply(model, detlog::reference::CommitPrefix{1, 1, {1, 2}});
  REQUIRE(model.is_leader(1));
  REQUIRE(model.replica(1)->leader_ready);

  apply(model, detlog::reference::Crash{1});
  REQUIRE(!model.is_leader(1));
  apply(model, detlog::reference::Restart{1});
  REQUIRE(!model.is_leader(1));
  REQUIRE(!model.replica(1)->leader_ready);

  const auto rejected =
      model.apply(detlog::reference::AppendNoOp{1});
  REQUIRE(!rejected);
  REQUIRE(rejected.error.find("elected leader") != std::string::npos);

  apply(model, detlog::reference::Elect{1, 2, {1, 2}});
  REQUIRE(model.is_leader(1));
  REQUIRE(!model.replica(1)->leader_ready);
}

void abstract_partitioned_old_leader_steps_down_on_higher_term() {
  ReferenceModel model({1, 2, 3});
  apply(model, detlog::reference::Elect{1, 1, {1, 2}});
  apply(model, detlog::reference::AppendNoOp{1});
  apply(model, detlog::reference::ReplicatePrefix{1, 2, 1});
  apply(model, detlog::reference::ReplicatePrefix{1, 3, 1});
  apply(model, detlog::reference::CommitPrefix{1, 1, {1, 2}});

  // Node 1 is outside the term-2 election quorum, so it may still believe it
  // is the term-1 leader while the majority elects node 3 in a newer term.
  apply(model, detlog::reference::Elect{3, 2, {2, 3}});
  REQUIRE(model.is_leader(1));
  REQUIRE(model.is_leader(3));
  REQUIRE(model.replica(1)->current_term == 1);
  REQUIRE(model.replica(3)->current_term == 2);

  // Receiving a term-2 replication request is the observation that forces the
  // isolated old leader to become a follower.
  apply(model, detlog::reference::ReplicatePrefix{3, 1, 1});
  REQUIRE(!model.is_leader(1));
  REQUIRE(model.is_leader(3));
  REQUIRE(model.replica(1)->current_term == 2);
  REQUIRE(!model.replica(1)->leader_ready);
}

void production_matches_reference_replication_and_dedup() {
  detlog::Cluster cluster(detlog::cluster_config(3, 0xd1ffU));
  ReferenceModel model(cluster.members());
  const detlog::NodeId leader = ready_leader(cluster);
  mirror_first_election(cluster, model, leader);

  const detlog::SessionId session{0x5245464552454e43ULL,
                                  0x444946464552454eULL};
  const auto put = command(session, 1, detlog::CommandKind::put,
                           "reference-key", "reference-value");
  const auto first = commit_command(cluster, model, leader, put);
  REQUIRE(first.status == detlog::ClientStatus::ok);
  REQUIRE(first.value == "reference-value");
  require_same_replicas(cluster, model, {"reference-key"});

  const auto before = cluster.snapshot(leader);
  REQUIRE(before.has_value());
  const auto retry = submit_and_wait(cluster, leader, put);
  const auto expected_retry =
      apply(model, detlog::reference::Propose{leader, put});
  REQUIRE(expected_retry.proposal.has_value());
  REQUIRE(!expected_retry.proposal->appended);
  REQUIRE(expected_retry.proposal->duplicate);
  REQUIRE(retry.status == expected_retry.proposal->status);
  REQUIRE(retry.value == expected_retry.proposal->value);
  REQUIRE(cluster.snapshot(leader)->last_log_index == before->last_log_index);
  REQUIRE(model.replica(leader)->log.size() == before->log.size());

  const auto conflicting = command(session, 1, detlog::CommandKind::put,
                                   "reference-key", "other-value");
  const auto conflict_reply = submit_and_wait(cluster, leader, conflicting);
  const auto expected_conflict =
      apply(model, detlog::reference::Propose{leader, conflicting});
  REQUIRE(expected_conflict.proposal.has_value());
  REQUIRE(conflict_reply.status == expected_conflict.proposal->status);
  REQUIRE(conflict_reply.status == detlog::ClientStatus::request_id_conflict);
  REQUIRE(cluster.snapshot(leader)->last_log_index == before->last_log_index);

  const auto get = command(session, 2, detlog::CommandKind::get,
                           "reference-key");
  const auto get_reply = commit_command(cluster, model, leader, get);
  REQUIRE(get_reply.value == "reference-value");
  const auto erase = command(session, 3, detlog::CommandKind::erase,
                             "reference-key");
  const auto erase_reply = commit_command(cluster, model, leader, erase);
  REQUIRE(erase_reply.value == "reference-value");
  require_same_replicas(cluster, model, {"reference-key"});
  REQUIRE(!model.value(leader, "reference-key"));
  REQUIRE(cluster.check_invariants());
}

void production_matches_reference_divergent_tail_repair() {
  detlog::Cluster cluster(detlog::cluster_config(3, 0x7a11U));
  ReferenceModel model(cluster.members());
  const detlog::NodeId old_leader = ready_leader(cluster);
  mirror_first_election(cluster, model, old_leader);

  REQUIRE(cluster.isolate(old_leader, true));
  const detlog::SessionId session{0x554e434f4d4d4954ULL,
                                  0x5445442d5441494cULL};
  const auto abandoned = command(session, 1, detlog::CommandKind::put,
                                 "abandoned", "must-not-apply");
  const detlog::ClientToken token = cluster.submit(old_leader, abandoned);
  REQUIRE(cluster.run_until(
      [=](const detlog::Cluster& value) {
        const auto snapshot = value.snapshot(old_leader);
        return snapshot && snapshot->durable_last_log_index == 2 &&
               snapshot->commit_index == 1;
      },
      100));
  const auto old_tail = cluster.snapshot(old_leader);
  REQUIRE(old_tail.has_value());
  REQUIRE(old_tail->log[1].command == std::optional{abandoned});
  const auto reference_tail =
      apply(model, detlog::reference::Propose{old_leader, abandoned});
  REQUIRE(reference_tail.appended_index ==
          std::optional<detlog::LogIndex>{2});

  REQUIRE(cluster.crash(old_leader));
  apply(model, detlog::reference::Crash{old_leader});
  const detlog::NodeId replacement = ready_leader(cluster);
  REQUIRE(replacement != old_leader);
  const auto replacement_snapshot = cluster.snapshot(replacement);
  REQUIRE(replacement_snapshot.has_value());
  REQUIRE(replacement_snapshot->log.size() == 2);
  REQUIRE(!replacement_snapshot->log[1].command.has_value());

  std::vector<detlog::NodeId> live_voters;
  for (const detlog::NodeId member : cluster.members()) {
    if (member != old_leader) {
      live_voters.push_back(member);
    }
  }
  apply(model, detlog::reference::Elect{replacement,
                                        replacement_snapshot->term,
                                        live_voters});
  const auto replacement_noop =
      apply(model, detlog::reference::AppendNoOp{replacement});
  REQUIRE(replacement_noop.appended_index ==
          std::optional<detlog::LogIndex>{2});
  for (const detlog::NodeId member : live_voters) {
    if (member != replacement) {
      apply(model,
            detlog::reference::ReplicatePrefix{replacement, member, 2});
    }
  }
  apply(model,
        detlog::reference::CommitPrefix{replacement, 2, live_voters});
  for (const detlog::NodeId member : live_voters) {
    if (member != replacement) {
      apply(model, detlog::reference::LearnCommit{replacement, member, 2});
    }
  }
  wait_for_committed_prefix(cluster, replacement, 2, false);
  REQUIRE(!cluster.reply_for(token).has_value() ||
          cluster.reply_for(token)->status != detlog::ClientStatus::ok);
  REQUIRE(!cluster.command_is_committed(session, 1));

  REQUIRE(cluster.isolate(old_leader, false));
  REQUIRE(cluster.restart(old_leader));
  apply(model, detlog::reference::Restart{old_leader});
  wait_for_committed_prefix(cluster, replacement, 2);
  apply(model,
        detlog::reference::ReplicatePrefix{replacement, old_leader, 2});
  apply(model,
        detlog::reference::LearnCommit{replacement, old_leader, 2});

  require_same_replicas(cluster, model, {"abandoned"});
  REQUIRE(!cluster.value(old_leader, "abandoned"));
  REQUIRE(!model.value(old_leader, "abandoned"));
  REQUIRE(cluster.snapshot(old_leader)->log[1].command == std::nullopt);
  REQUIRE(cluster.check_invariants());
}

}  // namespace

int main() {
  const std::vector<std::pair<std::string_view, std::function<void()>>> tests{
      {"abstract quorum validation", abstract_history_rejects_missing_quorums},
      {"abstract restart clears leadership",
       abstract_restart_does_not_restore_volatile_leadership},
      {"abstract higher-term observation clears old leadership",
       abstract_partitioned_old_leader_steps_down_on_higher_term},
      {"differential replication and dedup",
       production_matches_reference_replication_and_dedup},
      {"differential divergent tail repair",
       production_matches_reference_divergent_tail_repair},
  };
  for (const auto& [name, test] : tests) {
    try {
      test();
      std::cout << "[pass] " << name << '\n';
    } catch (const std::exception& error) {
      std::cerr << "[fail] " << name << ": " << error.what() << '\n';
      return 1;
    }
  }
  return 0;
}
