#include "detlog/cluster.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <iostream>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace {

[[noreturn]] void fail(std::string message, int line) {
  throw std::runtime_error("line " + std::to_string(line) + ": " +
                           std::move(message));
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

class CheckedHistory {
 public:
  explicit CheckedHistory(detlog::ClusterConfig config)
      : cluster_(std::move(config)) {
    require_safe("after construction");
  }

  [[nodiscard]] detlog::Cluster& cluster() noexcept { return cluster_; }
  [[nodiscard]] const detlog::Cluster& cluster() const noexcept {
    return cluster_;
  }

  void require_safe(std::string_view where) {
    const auto result = cluster_.check_invariants();
    if (!result) {
      throw std::runtime_error(std::string(where) + ": " + result.detail);
    }
  }

  [[nodiscard]] bool step() {
    const bool progressed = cluster_.step();
    require_safe("after simulator event");
    return progressed;
  }

  template <typename Predicate>
  void drive_until(Predicate predicate, std::size_t max_events,
                   std::string_view description) {
    require_safe("before drive");
    if (predicate(cluster_)) {
      return;
    }
    for (std::size_t event = 0; event < max_events; ++event) {
      if (!step()) {
        break;
      }
      if (predicate(cluster_)) {
        return;
      }
    }
    throw std::runtime_error("event bound reached while waiting for " +
                             std::string(description));
  }

  void drive(std::size_t events) {
    for (std::size_t event = 0; event < events; ++event) {
      if (!step()) {
        break;
      }
    }
  }

  [[nodiscard]] detlog::NodeId elect_ready(
      std::size_t max_events = 50'000U) {
    drive_until(
        [](const detlog::Cluster& cluster) {
          return cluster.leader(true).has_value();
        },
        max_events, "a ready leader");
    return *cluster_.leader(true);
  }

  [[nodiscard]] detlog::ClientReply wait_for_reply(
      detlog::ClientToken token, std::size_t max_events = 50'000U) {
    drive_until(
        [token](const detlog::Cluster& cluster) {
          return cluster.reply_for(token).has_value();
        },
        max_events, "a client reply");
    return *cluster_.reply_for(token);
  }

 private:
  detlog::Cluster cluster_;
};

[[nodiscard]] std::vector<detlog::NodeId> followers(
    const detlog::Cluster& cluster, detlog::NodeId leader) {
  std::vector<detlog::NodeId> result;
  for (const detlog::NodeId node : cluster.members()) {
    if (node != leader) {
      result.push_back(node);
    }
  }
  REQUIRE(result.size() == cluster.members().size() - 1U);
  return result;
}

[[nodiscard]] bool is_command(const detlog::LogEntry& entry,
                              const detlog::ClientCommand& command) {
  return entry.command && *entry.command == command;
}

[[nodiscard]] bool durable_contains(const detlog::Cluster& cluster,
                                    detlog::NodeId node,
                                    const detlog::ClientCommand& command) {
  const auto state = cluster.stable_state(node);
  if (!state) {
    return false;
  }
  return std::ranges::any_of(
      state->log, [&command](const detlog::LogEntry& entry) {
        return is_command(entry, command);
      });
}

[[nodiscard]] std::size_t durable_replica_count(
    const detlog::Cluster& cluster, const detlog::ClientCommand& command) {
  return static_cast<std::size_t>(std::ranges::count_if(
      cluster.members(), [&cluster, &command](detlog::NodeId node) {
        return durable_contains(cluster, node, command);
      }));
}

[[nodiscard]] bool trace_since(
    const detlog::Cluster& cluster, std::size_t begin,
    const std::function<bool(const detlog::ClusterTraceRecord&)>& predicate) {
  const auto& trace = cluster.trace();
  if (begin > trace.size()) {
    return false;
  }
  return std::any_of(trace.begin() + static_cast<std::ptrdiff_t>(begin),
                     trace.end(), predicate);
}

void require_one_logical_entry(const detlog::Cluster& cluster,
                               const detlog::ClientCommand& command) {
  std::set<detlog::LogIndex> indexes;
  for (const auto& snapshot : cluster.snapshots()) {
    std::size_t copies{};
    for (const auto& entry : snapshot.log) {
      if (is_command(entry, command)) {
        ++copies;
        indexes.insert(entry.index);
      }
    }
    REQUIRE(copies <= 1U);
  }
  REQUIRE(indexes.size() == 1U);
}

void wait_for_all_values(CheckedHistory& history, std::string_view key,
                         std::string_view expected) {
  const std::string key_text{key};
  const std::string expected_text{expected};
  history.drive_until(
      [key_text, expected_text](const detlog::Cluster& cluster) {
        return std::ranges::all_of(
            cluster.members(),
            [&cluster, &key_text, &expected_text](detlog::NodeId node) {
              return cluster.value(node, key_text) ==
                     std::optional<std::string>{expected_text};
            });
      },
      50'000U, "all replicas to apply the expected value");
}

void crash_before_append_send() {
  CheckedHistory history(detlog::cluster_config(3, 1'001));
  auto& cluster = history.cluster();
  const detlog::NodeId leader = history.elect_ready();
  const auto command = put({0x1001, 0xaaaa}, 1, "before-send", "once");

  const std::size_t trace_begin = cluster.trace().size();
  const detlog::ClientToken abandoned = cluster.submit(leader, command);
  history.require_safe("after client submission");
  REQUIRE(trace_since(
      cluster, trace_begin,
      [leader](const detlog::ClusterTraceRecord& record) {
        return record.node == leader && record.kind == "persist_write_queued";
      }));
  REQUIRE(!trace_since(
      cluster, trace_begin,
      [leader](const detlog::ClusterTraceRecord& record) {
        return record.node == leader && record.kind == "transport_send";
      }));

  REQUIRE(cluster.crash(leader));
  history.require_safe("after pre-send leader crash");
  REQUIRE(!cluster.command_is_committed(command.session, command.sequence));
  REQUIRE(!cluster.reply_for(abandoned));

  const detlog::NodeId replacement = history.elect_ready();
  REQUIRE(replacement != leader);
  const detlog::ClientToken retry = cluster.submit(replacement, command);
  history.require_safe("after retry on replacement leader");
  const auto reply = history.wait_for_reply(retry);
  REQUIRE(reply.status == detlog::ClientStatus::ok);
  REQUIRE(cluster.command_is_committed(command.session, command.sequence));

  REQUIRE(cluster.restart(leader));
  history.require_safe("after old leader restart");
  wait_for_all_values(history, command.key, command.value);
  require_one_logical_entry(cluster, command);
}

void crash_after_quorum_replication_before_reply() {
  CheckedHistory history(detlog::cluster_config(3, 1'002));
  auto& cluster = history.cluster();
  const detlog::NodeId leader = history.elect_ready();
  const auto peers = followers(cluster, leader);
  const detlog::NodeId quorum_peer = peers.front();
  const detlog::NodeId lagging_peer = peers.back();
  const auto command = put({0x1002, 0xbbbb}, 1, "quorum-no-reply", "once");

  REQUIRE(cluster.set_bidirectional_partition(leader, lagging_peer, true));
  history.require_safe("after isolating the lagging follower");
  const detlog::ClientToken abandoned = cluster.submit(leader, command);
  history.require_safe("after client submission");
  history.drive_until(
      [leader, quorum_peer, abandoned,
       &command](const detlog::Cluster& running) {
        return durable_contains(running, leader, command) &&
               durable_contains(running, quorum_peer, command) &&
               !running.reply_for(abandoned);
      },
      50'000U, "durable quorum replication before a reply");

  REQUIRE(durable_replica_count(cluster, command) == 2U);
  REQUIRE(!durable_contains(cluster, lagging_peer, command));
  REQUIRE(!cluster.reply_for(abandoned));
  REQUIRE(!cluster.command_is_committed(command.session, command.sequence));
  REQUIRE(cluster.crash(leader));
  history.require_safe("after quorum-replicated leader crash");

  const detlog::NodeId replacement = history.elect_ready();
  REQUIRE(replacement == quorum_peer);
  REQUIRE(cluster.command_is_committed(command.session, command.sequence));
  const detlog::ClientToken retry = cluster.submit(replacement, command);
  history.require_safe("after ambiguous retry");
  const auto reply = history.wait_for_reply(retry);
  REQUIRE(reply.status == detlog::ClientStatus::ok);
  REQUIRE(reply.value == command.value);
  require_one_logical_entry(cluster, command);

  REQUIRE(cluster.set_bidirectional_partition(leader, lagging_peer, false));
  REQUIRE(cluster.restart(leader));
  history.require_safe("after healing and restarting the old leader");
  wait_for_all_values(history, command.key, command.value);
  require_one_logical_entry(cluster, command);
}

void crash_after_reply_before_all_followers_receive() {
  CheckedHistory history(detlog::cluster_config(3, 1'003));
  auto& cluster = history.cluster();
  const detlog::NodeId leader = history.elect_ready();
  const auto peers = followers(cluster, leader);
  const detlog::NodeId lagging_peer = peers.back();
  const auto command =
      put({0x1003, 0xcccc}, 1, "reply-before-all", "committed");

  REQUIRE(cluster.set_bidirectional_partition(leader, lagging_peer, true));
  history.require_safe("after isolating one follower");
  const detlog::ClientToken token = cluster.submit(leader, command);
  history.require_safe("after client submission");
  const auto reply = history.wait_for_reply(token);
  REQUIRE(reply.status == detlog::ClientStatus::ok);
  REQUIRE(cluster.command_is_committed(command.session, command.sequence));
  REQUIRE(!durable_contains(cluster, lagging_peer, command));

  REQUIRE(cluster.crash(leader));
  REQUIRE(cluster.set_bidirectional_partition(leader, lagging_peer, false));
  history.require_safe("after post-reply leader crash and heal");
  const detlog::NodeId replacement = history.elect_ready();
  REQUIRE(replacement != leader);
  history.drive_until(
      [lagging_peer, &command](const detlog::Cluster& running) {
        return running.value(lagging_peer, command.key) ==
               std::optional<std::string>{command.value};
      },
      50'000U, "the lagging follower to receive the committed entry");

  REQUIRE(cluster.restart(leader));
  history.require_safe("after former leader restart");
  wait_for_all_values(history, command.key, command.value);
  require_one_logical_entry(cluster, command);
}

void follower_crash_during_wal_write() {
  auto config = detlog::cluster_config(3, 1'004);
  config.storage_write_latency = 4;
  config.storage_flush_latency = 2;
  CheckedHistory history(std::move(config));
  auto& cluster = history.cluster();
  const detlog::NodeId leader = history.elect_ready();

  history.drive_until(
      [leader](const detlog::Cluster& running) {
        const auto leader_state = running.stable_state(leader);
        if (!leader_state) {
          return false;
        }
        return std::ranges::all_of(
            running.members(), [&running, &leader_state](detlog::NodeId node) {
              const auto state = running.stable_state(node);
              return state && state->log == leader_state->log &&
                     state->commit_index == leader_state->commit_index;
            });
      },
      50'000U, "followers to durably learn the readiness commit");

  const auto peers = followers(cluster, leader);
  const detlog::NodeId victim = peers.front();
  const detlog::NodeId survivor = peers.back();
  const auto command = put({0x1004, 0xdddd}, 1, "torn-follower", "safe");
  REQUIRE(cluster.set_link_delay(leader, survivor, 5));
  history.require_safe("after delaying the surviving follower");
  const detlog::ClientToken token = cluster.submit(leader, command);
  history.require_safe("after client submission");

  history.drive_until(
      [leader, &command](const detlog::Cluster& running) {
        return durable_contains(running, leader, command);
      },
      50'000U, "the leader command WAL write to become durable");
  const std::size_t trace_begin = cluster.trace().size();
  history.drive_until(
      [trace_begin, victim](const detlog::Cluster& running) {
        return trace_since(
            running, trace_begin,
            [victim](const detlog::ClusterTraceRecord& record) {
              return record.node == victim &&
                     record.kind == "persist_write_queued";
            });
      },
      50'000U, "the follower WAL write to be in flight");
  REQUIRE(!durable_contains(cluster, victim, command));

  REQUIRE(cluster.crash(victim, detlog::sim::StorageCrashSpec{0, 20}));
  REQUIRE(cluster.set_link_delay(leader, survivor, 0));
  history.require_safe("after tearing an in-flight follower WAL write");
  const auto reply = history.wait_for_reply(token);
  REQUIRE(reply.status == detlog::ClientStatus::ok);
  REQUIRE(cluster.command_is_committed(command.session, command.sequence));

  const std::size_t restart_trace = cluster.trace().size();
  REQUIRE(cluster.restart(victim));
  history.require_safe("after torn follower restart");
  REQUIRE(trace_since(
      cluster, restart_trace,
      [victim](const detlog::ClusterTraceRecord& record) {
        return record.node == victim && record.kind == "wal_tail_repaired";
      }));
  wait_for_all_values(history, command.key, command.value);
  require_one_logical_entry(cluster, command);
}

void simultaneous_election_timeouts_remain_safe() {
  auto config = detlog::cluster_config(3, 1'005);
  config.raft.election_timeout = 5;
  config.raft.heartbeat_interval = 1;
  config.election_jitter = 0;
  CheckedHistory history(std::move(config));
  auto& cluster = history.cluster();

  history.drive_until(
      [](const detlog::Cluster& running) {
        std::set<detlog::NodeId> candidates;
        std::optional<detlog::Tick> common_time;
        for (const auto& record : running.trace()) {
          if (record.source != detlog::ClusterTraceSource::raft ||
              record.kind != "role_change" ||
              record.detail != "became candidate") {
            continue;
          }
          if (!common_time) {
            common_time = record.time;
          }
          if (record.time == *common_time) {
            candidates.insert(record.node);
          }
        }
        return candidates.size() == running.members().size();
      },
      1'000U, "all election timers to fire simultaneously");

  const detlog::Tick timeout_time = cluster.now();
  std::set<detlog::NodeId> simultaneous_candidates;
  for (const auto& record : cluster.trace()) {
    if (record.time == timeout_time &&
        record.source == detlog::ClusterTraceSource::raft &&
        record.kind == "role_change" &&
        record.detail == "became candidate") {
      simultaneous_candidates.insert(record.node);
    }
  }
  REQUIRE(simultaneous_candidates.size() == cluster.members().size());
  REQUIRE(cluster.leaders().empty());
  history.drive(200U);
  history.require_safe("after repeated simultaneous election rounds");
}

void delayed_heartbeat_causes_safe_reelection() {
  auto config = detlog::cluster_config(3, 1'006);
  config.raft.election_timeout = 12;
  config.raft.heartbeat_interval = 2;
  config.election_jitter = 2;
  CheckedHistory history(std::move(config));
  auto& cluster = history.cluster();
  const detlog::NodeId leader = history.elect_ready();
  const detlog::Term starting_term = cluster.snapshot(leader)->term;
  const detlog::NodeId delayed = followers(cluster, leader).back();
  const std::size_t trace_begin = cluster.trace().size();

  REQUIRE(cluster.set_link_delay(leader, delayed, 40));
  history.require_safe("after delaying leader heartbeats");
  history.drive_until(
      [trace_begin, delayed](const detlog::Cluster& running) {
        return trace_since(
            running, trace_begin,
            [delayed](const detlog::ClusterTraceRecord& record) {
              return record.node == delayed &&
                     record.source == detlog::ClusterTraceSource::raft &&
                     record.kind == "role_change" &&
                     record.detail == "became candidate";
            });
      },
      10'000U, "the delayed follower election timeout");
  REQUIRE(cluster.set_link_delay(leader, delayed, 0));
  history.require_safe("after restoring heartbeat latency");

  history.drive_until(
      [starting_term](const detlog::Cluster& running) {
        const auto ready = running.leader(true);
        if (!ready) {
          return false;
        }
        const auto snapshot = running.snapshot(*ready);
        return snapshot && snapshot->term > starting_term;
      },
      50'000U, "a higher-term ready leader");
  const detlog::NodeId current = *cluster.leader(true);
  const auto command = put({0x1006, 0xeeee}, 1, "delayed-heartbeat", "safe");
  const detlog::ClientToken token = cluster.submit(current, command);
  history.require_safe("after post-reelection client submission");
  REQUIRE(history.wait_for_reply(token).status == detlog::ClientStatus::ok);
  REQUIRE(cluster.command_is_committed(command.session, command.sequence));
}

void ambiguous_success_reply_is_deduplicated() {
  CheckedHistory history(detlog::cluster_config(3, 1'007));
  auto& cluster = history.cluster();
  const detlog::NodeId leader = history.elect_ready();
  const auto command = put({0x1007, 0xffff}, 1, "lost-reply", "once");

  const detlog::ClientToken first_token = cluster.submit(leader, command);
  history.require_safe("after original client submission");
  const auto first_reply = history.wait_for_reply(first_token);
  REQUIRE(first_reply.status == detlog::ClientStatus::ok);
  REQUIRE(cluster.command_is_committed(command.session, command.sequence));
  const auto discarded_replies = cluster.take_replies();
  REQUIRE(std::ranges::any_of(
      discarded_replies, [first_token](const detlog::ClientReply& reply) {
        return reply.token == first_token;
      }));
  REQUIRE(!cluster.reply_for(first_token));

  REQUIRE(cluster.crash(leader));
  history.require_safe("after losing the success reply and crashing leader");
  const detlog::NodeId replacement = history.elect_ready();
  const detlog::ClientToken retry = cluster.submit(replacement, command);
  history.require_safe("after retrying the ambiguous request");
  const auto retry_reply = history.wait_for_reply(retry);
  REQUIRE(retry_reply.status == detlog::ClientStatus::ok);
  REQUIRE(retry_reply.value == first_reply.value);
  require_one_logical_entry(cluster, command);

  REQUIRE(cluster.restart(leader));
  history.require_safe("after old leader restart");
  wait_for_all_values(history, command.key, command.value);
  require_one_logical_entry(cluster, command);
}

void slow_follower_does_not_block_quorum() {
  auto config = detlog::cluster_config(3, 1'008);
  config.raft.election_timeout = 30;
  config.raft.heartbeat_interval = 2;
  config.election_jitter = 3;
  CheckedHistory history(std::move(config));
  auto& cluster = history.cluster();
  const detlog::NodeId leader = history.elect_ready();
  const detlog::NodeId slow = followers(cluster, leader).back();
  const auto command = put({0x1008, 0x1111}, 1, "slow-follower", "live");

  REQUIRE(cluster.set_link_delay(leader, slow, 8));
  REQUIRE(cluster.set_link_delay(slow, leader, 8));
  history.require_safe("after slowing one follower");
  const detlog::ClientToken token = cluster.submit(leader, command);
  history.require_safe("after client submission with slow follower");
  const auto reply = history.wait_for_reply(token);
  REQUIRE(reply.status == detlog::ClientStatus::ok);
  REQUIRE(cluster.command_is_committed(command.session, command.sequence));
  REQUIRE(!durable_contains(cluster, slow, command));

  REQUIRE(cluster.set_link_delay(leader, slow, 0));
  REQUIRE(cluster.set_link_delay(slow, leader, 0));
  history.require_safe("after restoring follower latency");
  history.drive_until(
      [slow, &command](const detlog::Cluster& running) {
        return running.value(slow, command.key) ==
               std::optional<std::string>{command.value};
      },
      50'000U, "the slow follower to catch up");
  require_one_logical_entry(cluster, command);
}

void slow_disk_preserves_durability_before_reply() {
  auto config = detlog::cluster_config(3, 1'009);
  config.raft.election_timeout = 100;
  config.raft.heartbeat_interval = 5;
  config.election_jitter = 5;
  CheckedHistory history(std::move(config));
  auto& cluster = history.cluster();
  const detlog::NodeId leader = history.elect_ready();
  const auto peers = followers(cluster, leader);
  const detlog::NodeId healthy = peers.front();
  const detlog::NodeId slow = peers.back();

  history.drive_until(
      [leader](const detlog::Cluster& running) {
        const auto leader_state = running.stable_state(leader);
        if (!leader_state) {
          return false;
        }
        return std::ranges::all_of(
            running.members(), [&running, &leader_state](detlog::NodeId node) {
              const auto state = running.stable_state(node);
              return state && state->log == leader_state->log &&
                     state->commit_index == leader_state->commit_index;
            });
      },
      50'000U, "all replicas to durably learn the readiness commit");

  constexpr detlog::Tick kSlowWriteTicks = 20;
  constexpr detlog::Tick kSlowFlushTicks = 20;
  REQUIRE(!cluster.set_storage_latency(0, kSlowWriteTicks,
                                       kSlowFlushTicks));
  history.require_safe("after rejecting an unknown storage node");
  const std::size_t control_trace = cluster.trace().size();
  REQUIRE(cluster.set_storage_latency(slow, kSlowWriteTicks,
                                      kSlowFlushTicks));
  history.require_safe("after slowing exactly one follower disk");
  REQUIRE(trace_since(
      cluster, control_trace,
      [slow](const detlog::ClusterTraceRecord& record) {
        return record.source == detlog::ClusterTraceSource::adapter &&
               record.kind == "storage_latency_set" &&
               record.node == slow && record.token == kSlowWriteTicks &&
               record.detail == "write_ticks=20 flush_ticks=20";
      }));

  const auto command = put({0x1009, 0x2222}, 1, "slow-disk", "durable");
  const detlog::Tick submitted_at = cluster.now();

  const detlog::ClientToken token = cluster.submit(leader, command);
  history.require_safe("after slow-disk client submission");
  REQUIRE(!cluster.reply_for(token));
  REQUIRE(cluster.queue_depth().storage_bytes > 0U);
  const auto reply = history.wait_for_reply(token);
  REQUIRE(reply.status == detlog::ClientStatus::ok);
  REQUIRE(durable_contains(cluster, leader, command));
  REQUIRE(durable_contains(cluster, healthy, command));
  REQUIRE(!durable_contains(cluster, slow, command));
  REQUIRE(durable_replica_count(cluster, command) == 2U);
  REQUIRE(cluster.command_is_committed(command.session, command.sequence));

  history.drive_until(
      [slow, &command](const detlog::Cluster& running) {
        return running.value(slow, command.key) ==
               std::optional<std::string>{command.value};
      },
      50'000U, "the slow disk follower to durably catch up and apply");
  REQUIRE(cluster.now() >=
          submitted_at + kSlowWriteTicks + kSlowFlushTicks);
  require_one_logical_entry(cluster, command);
}

struct NamedHistory {
  std::string_view name;
  void (*run)();
};

}  // namespace

int main() {
  const std::vector<NamedHistory> histories{
      {"leader crash before AppendEntries send", crash_before_append_send},
      {"leader crash after quorum replication before reply",
       crash_after_quorum_replication_before_reply},
      {"leader crash after reply before every follower receives",
       crash_after_reply_before_all_followers_receive},
      {"follower crash during WAL write", follower_crash_during_wal_write},
      {"simultaneous election timeouts",
       simultaneous_election_timeouts_remain_safe},
      {"delayed heartbeat reelection", delayed_heartbeat_causes_safe_reelection},
      {"ambiguous client retry", ambiguous_success_reply_is_deduplicated},
      {"slow follower", slow_follower_does_not_block_quorum},
      {"slow disk", slow_disk_preserves_durability_before_reply},
  };

  for (const auto& history : histories) {
    try {
      history.run();
      std::cout << "[pass] " << history.name << '\n';
    } catch (const std::exception& error) {
      std::cerr << "[fail] " << history.name << ": " << error.what()
                << '\n';
      return 1;
    }
  }
  return 0;
}
