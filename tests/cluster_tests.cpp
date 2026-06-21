#include "detlog/cluster.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <iostream>
#include <limits>
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

[[nodiscard]] detlog::ClientCommand put(detlog::SessionId session,
                                        std::uint64_t sequence,
                                        std::string key,
                                        std::string value) {
  return detlog::ClientCommand{session, sequence, detlog::CommandKind::put,
                               std::move(key), std::move(value)};
}

[[nodiscard]] detlog::NodeId elect_ready(detlog::Cluster& cluster) {
  const auto leader = cluster.run_until_leader(20'000, true);
  REQUIRE(leader.has_value());
  return *leader;
}

[[nodiscard]] detlog::ClientReply submit_and_wait(
    detlog::Cluster& cluster, detlog::NodeId node,
    const detlog::ClientCommand& command) {
  const auto token = cluster.submit(node, command);
  REQUIRE(cluster.run_until_reply(token, 20'000));
  const auto reply = cluster.reply_for(token);
  REQUIRE(reply.has_value());
  return *reply;
}

void healthy_three_and_five_node_elections() {
  for (const std::size_t size : {std::size_t{3}, std::size_t{5}}) {
    detlog::Cluster cluster(detlog::cluster_config(size, 101 + size));
    const auto leader = elect_ready(cluster);
    const auto snapshot = cluster.snapshot(leader);
    REQUIRE(snapshot.has_value());
    REQUIRE(snapshot->alive);
    REQUIRE(snapshot->role == detlog::RaftRole::leader);
    REQUIRE(snapshot->leader_ready);
    REQUIRE(snapshot->term != 0);
    REQUIRE(snapshot->commit_index >= 1);
    REQUIRE(snapshot->last_applied == snapshot->commit_index);
    REQUIRE(snapshot->log.front().command == std::nullopt);
    REQUIRE(cluster.check_invariants());
  }
}

void put_commits_on_a_durable_quorum() {
  detlog::Cluster cluster(detlog::cluster_config(3, 202));
  const auto leader = elect_ready(cluster);
  const detlog::SessionId session{0x10, 0x20};
  const auto command = put(session, 1, "color", "blue");
  const auto reply = submit_and_wait(cluster, leader, command);
  REQUIRE(reply.status == detlog::ClientStatus::ok);
  REQUIRE(reply.value == "blue");
  REQUIRE(reply.log_index != 0);
  REQUIRE(cluster.command_is_committed(session, 1));
  REQUIRE(cluster.value(leader, "color") ==
          std::optional<std::string>{"blue"});

  std::size_t durable_replicas{};
  for (const auto node : cluster.members()) {
    const auto stable = cluster.stable_state(node);
    REQUIRE(stable.has_value());
    if (stable->log.size() >= reply.log_index &&
        stable->log[static_cast<std::size_t>(reply.log_index - 1U)] ==
            *cluster.committed_entry(reply.log_index)) {
      ++durable_replicas;
    }
  }
  REQUIRE(durable_replicas >= 2);
  REQUIRE(cluster.check_invariants());
}

void retry_is_deduplicated_without_another_log_entry() {
  detlog::Cluster cluster(detlog::cluster_config(3, 303));
  const auto leader = elect_ready(cluster);
  const detlog::SessionId session{0x30, 0x40};
  const auto command = put(session, 1, "retry", "once");
  const auto first = submit_and_wait(cluster, leader, command);
  REQUIRE(first.status == detlog::ClientStatus::ok);
  const auto before = cluster.snapshot(leader);
  REQUIRE(before.has_value());

  const auto second = submit_and_wait(cluster, leader, command);
  REQUIRE(second.status == detlog::ClientStatus::ok);
  REQUIRE(second.value == first.value);
  const auto after = cluster.snapshot(leader);
  REQUIRE(after.has_value());
  REQUIRE(after->last_log_index == before->last_log_index);
  REQUIRE(cluster.value(leader, "retry") ==
          std::optional<std::string>{"once"});
}

void leader_crash_elects_replacement_and_continues_commits() {
  detlog::Cluster cluster(detlog::cluster_config(3, 404));
  const auto first_leader = elect_ready(cluster);
  const detlog::SessionId session{0x50, 0x60};
  const auto first = submit_and_wait(
      cluster, first_leader, put(session, 1, "epoch", "one"));
  REQUIRE(first.status == detlog::ClientStatus::ok);

  REQUIRE(cluster.crash(first_leader));
  const auto replacement = elect_ready(cluster);
  REQUIRE(replacement != first_leader);
  const auto second = submit_and_wait(
      cluster, replacement, put(session, 2, "epoch", "two"));
  REQUIRE(second.status == detlog::ClientStatus::ok);
  REQUIRE(cluster.command_is_committed(session, 2));
  REQUIRE(cluster.value(replacement, "epoch") ==
          std::optional<std::string>{"two"});

  REQUIRE(cluster.restart(first_leader));
  REQUIRE(cluster.run_until(
      [first_leader](const detlog::Cluster& value) {
        return value.value(first_leader, "epoch") ==
               std::optional<std::string>{"two"};
      },
      20'000));
  REQUIRE(cluster.check_invariants());
}

void isolated_minority_cannot_commit_a_new_command() {
  detlog::Cluster cluster(detlog::cluster_config(3, 505));
  const auto isolated_leader = elect_ready(cluster);
  REQUIRE(cluster.isolate(isolated_leader, true));

  const detlog::SessionId session{0x70, 0x80};
  const auto command = put(session, 1, "minority", "forbidden");
  const auto token = cluster.submit(isolated_leader, command);
  REQUIRE(cluster.run(2'000) == 2'000);
  REQUIRE(!cluster.command_is_committed(session, 1));
  const auto reply = cluster.reply_for(token);
  REQUIRE(!reply.has_value() || reply->status != detlog::ClientStatus::ok);
  REQUIRE(cluster.check_invariants());
}

void incomplete_wal_tail_is_repaired_before_restart_append() {
  detlog::Cluster cluster(detlog::cluster_config(3, 606));
  const auto leader = elect_ready(cluster);
  const auto stable_before = cluster.stable_state(leader);
  REQUIRE(stable_before.has_value());

  const detlog::SessionId session{0x90, 0xa0};
  (void)cluster.submit(leader, put(session, 1, "torn", "tail"));
  REQUIRE(cluster.crash(leader, detlog::sim::StorageCrashSpec{0, 20}));
  REQUIRE(cluster.restart(leader));
  const auto stable_after = cluster.stable_state(leader);
  REQUIRE(stable_after.has_value());
  REQUIRE(stable_after->hard_state == stable_before->hard_state);
  REQUIRE(stable_after->log == stable_before->log);
  REQUIRE(stable_after->commit_index == stable_before->commit_index);
  REQUIRE(!cluster.command_is_committed(session, 1));
  REQUIRE(cluster.trace_jsonl().find("wal_tail_repaired") !=
          std::string::npos);
}

[[nodiscard]] std::string replay_trace(std::uint64_t seed) {
  detlog::Cluster cluster(detlog::cluster_config(3, seed));
  const auto leader = elect_ready(cluster);
  detlog::NodeId peer = 0;
  for (const auto candidate : cluster.members()) {
    if (candidate != leader) {
      peer = candidate;
      break;
    }
  }
  REQUIRE(peer != 0);
  REQUIRE(cluster.duplicate_next(leader, peer, 1));
  const detlog::SessionId session{0xb0, 0xc0};
  const auto reply = submit_and_wait(
      cluster, leader, put(session, 1, "replay", "stable"));
  REQUIRE(reply.status == detlog::ClientStatus::ok);
  REQUIRE(cluster.drop_next(peer, leader, 1));
  REQUIRE(cluster.run(100) == 100);
  return cluster.trace_jsonl();
}

void same_seed_and_actions_produce_identical_trace() {
  const auto first = replay_trace(707);
  const auto second = replay_trace(707);
  REQUIRE(first == second);
  REQUIRE(first.find("protocol_deliver") != std::string::npos);
  REQUIRE(first.find("persist_durable") != std::string::npos);
}

void failed_cluster_timer_rearm_keeps_the_live_deadline() {
  auto config = detlog::cluster_config(3, 5);
  config.simulator.events.max_total_events = 13;
  detlog::Cluster cluster(config);
  REQUIRE(cluster.run(100) != 0);
  REQUIRE(cluster.trace_jsonl().find("timer_rearm_coalesced") !=
          std::string::npos);
  REQUIRE(cluster.check_invariants());
}

void cluster_retention_is_bounded() {
  auto trace_config = detlog::cluster_config(3, 808);
  trace_config.max_semantic_trace_records = 10;
  detlog::Cluster trace_limited(trace_config);
  REQUIRE(trace_limited.trace().size() == 10);
  REQUIRE(trace_limited.trace_truncated());
  REQUIRE(trace_limited.trace().back().kind == "trace_saturated");

  auto reply_config = detlog::cluster_config(3, 809);
  reply_config.max_retained_client_replies = 3;
  detlog::Cluster reply_limited(reply_config);
  const auto leader = elect_ready(reply_limited);
  detlog::NodeId stopped = 0;
  for (const auto node : reply_limited.members()) {
    if (node != leader) {
      stopped = node;
      break;
    }
  }
  REQUIRE(stopped != 0);
  REQUIRE(reply_limited.crash(stopped));
  detlog::ClientToken first{};
  detlog::ClientToken last{};
  for (std::uint64_t sequence = 1; sequence <= 6; ++sequence) {
    const auto token = reply_limited.submit(
        stopped, put({0xd0, sequence}, 1, "bounded", "reply"));
    if (sequence == 1) {
      first = token;
    }
    last = token;
  }
  REQUIRE(reply_limited.replies().size() == 3);
  REQUIRE(!reply_limited.reply_for(first));
  REQUIRE(reply_limited.reply_for(last));
  REQUIRE(reply_limited.trace_jsonl().find("reply_retention_saturated") !=
          std::string::npos);
}

void handcrafted_log_matching_violation_is_rejected() {
  const auto left_command = put({1, 1}, 1, "key", "left");
  const auto right_command = put({2, 2}, 1, "key", "right");
  detlog::NodeSnapshot left;
  left.id = 1;
  left.log = {detlog::LogEntry{1, 1, left_command},
              detlog::LogEntry{2, 2, std::nullopt}};
  detlog::NodeSnapshot right;
  right.id = 2;
  right.log = {detlog::LogEntry{1, 1, right_command},
               detlog::LogEntry{2, 2, std::nullopt}};
  const std::array snapshots{left, right};
  const auto result = detlog::check_log_matching(snapshots);
  REQUIRE(!result);
  REQUIRE(result.detail.find("Log Matching violated") != std::string::npos);
}

void storage_failures_cannot_release_client_success() {
  const auto exercise = [](std::uint64_t seed, int mode) {
    detlog::Cluster cluster(detlog::cluster_config(3, seed));
    const auto leader = elect_ready(cluster);
    const detlog::SessionId session{0xe0, seed};
    if (mode == 0) {
      REQUIRE(cluster.fail_next_storage_write(leader));
    } else if (mode == 1) {
      REQUIRE(cluster.short_write_next(leader, 16));
    } else {
      REQUIRE(cluster.fail_next_storage_flush(leader));
    }
    const auto token = cluster.submit(
        leader, put(session, 1, "storage-fault", "not-acknowledged"));
    REQUIRE(cluster.run_until_reply(token, 10'000));
    const auto reply = cluster.reply_for(token);
    REQUIRE(reply.has_value());
    REQUIRE(reply->status == detlog::ClientStatus::storage_error);
    REQUIRE(!cluster.command_is_committed(session, 1));
    REQUIRE(cluster.check_invariants());

    if (mode == 1) {
      REQUIRE(cluster.crash(
          leader,
          detlog::sim::StorageCrashSpec{
              std::numeric_limits<std::size_t>::max(),
              std::numeric_limits<std::size_t>::max()}));
      REQUIRE(cluster.restart(leader));
      REQUIRE(cluster.trace_jsonl().find("wal_tail_repaired") !=
              std::string::npos);
    }
  };
  exercise(911, 0);
  exercise(912, 1);
  exercise(913, 2);

  auto invalid = detlog::cluster_config(3, 914);
  invalid.simulator.storage.max_pending_operations = 0;
  bool rejected = false;
  try {
    detlog::Cluster ignored(invalid);
  } catch (const std::invalid_argument&) {
    rejected = true;
  }
  REQUIRE(rejected);
}

void incompatible_simulator_bounds_fail_fast() {
  auto transport_limited = detlog::cluster_config(3, 901);
  transport_limited.simulator.transport.max_frame_bytes =
      transport_limited.raft.max_append_bytes_per_rpc - 1U;
  bool rejected_transport = false;
  try {
    detlog::Cluster invalid(std::move(transport_limited));
    (void)invalid;
  } catch (const std::invalid_argument&) {
    rejected_transport = true;
  }
  REQUIRE(rejected_transport);

  auto storage_limited = detlog::cluster_config(3, 902);
  storage_limited.simulator.storage.max_write_bytes =
      storage_limited.raft.max_append_bytes_per_rpc;
  bool rejected_storage = false;
  try {
    detlog::Cluster invalid(std::move(storage_limited));
    (void)invalid;
  } catch (const std::invalid_argument&) {
    rejected_storage = true;
  }
  REQUIRE(rejected_storage);
}

}  // namespace

int main() {
  const std::vector<std::pair<std::string_view, std::function<void()>>> tests{
      {"healthy elections", healthy_three_and_five_node_elections},
      {"put durable quorum", put_commits_on_a_durable_quorum},
      {"retry dedup", retry_is_deduplicated_without_another_log_entry},
      {"leader crash", leader_crash_elects_replacement_and_continues_commits},
      {"minority cannot commit", isolated_minority_cannot_commit_a_new_command},
      {"WAL tail repair", incomplete_wal_tail_is_repaired_before_restart_append},
      {"seed replay", same_seed_and_actions_produce_identical_trace},
      {"timer rearm saturation",
       failed_cluster_timer_rearm_keeps_the_live_deadline},
      {"bounded retention", cluster_retention_is_bounded},
      {"negative Log Matching", handcrafted_log_matching_violation_is_rejected},
      {"storage failure barriers",
       storage_failures_cannot_release_client_success},
      {"incompatible simulator bounds", incompatible_simulator_bounds_fail_fast},
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
