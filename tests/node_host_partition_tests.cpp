#include "detlog/node_host.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

namespace {

using detlog::ClientCommand;
using detlog::ClientReply;
using detlog::ClientStatus;
using detlog::ClientToken;
using detlog::CommandKind;
using detlog::LogIndex;
using detlog::NodeHost;
using detlog::NodeHostConfig;
using detlog::NodeHostSubmitStatus;
using detlog::NodeId;
using detlog::RaftRole;
using detlog::SessionId;
using detlog::TcpPeerEndpoint;

[[noreturn]] void fail(std::string message, int line) {
  throw std::runtime_error("line " + std::to_string(line) + ": " +
                           std::move(message));
}

#define REQUIRE(condition)                                                 \
  do {                                                                     \
    if (!(condition)) fail("requirement failed: " #condition, __LINE__); \
  } while (false)

class TempDirectory {
 public:
  TempDirectory() {
    const auto stamp =
        std::chrono::steady_clock::now().time_since_epoch().count();
    for (std::uint32_t attempt = 0; attempt < 100; ++attempt) {
      path_ = std::filesystem::temp_directory_path() /
              ("detlog-node-host-partition-" + std::to_string(stamp) + "-" +
               std::to_string(attempt));
      std::error_code error;
      if (std::filesystem::create_directory(path_, error)) return;
      if (error && error != std::errc::file_exists) {
        throw std::filesystem::filesystem_error(
            "create partition-test directory", path_, error);
      }
    }
    throw std::runtime_error("could not allocate a unique temporary directory");
  }

  ~TempDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path_, ignored);
  }

  [[nodiscard]] std::filesystem::path wal(NodeId node) const {
    return path_ / ("node-" + std::to_string(node) + ".wal");
  }

 private:
  std::filesystem::path path_;
};

constexpr std::uint64_t kClusterHigh = 0x4445544c4f470001ULL;
constexpr std::uint64_t kClusterLow = 0x504152544954494fULL;
constexpr std::chrono::milliseconds kMinimumPartition{200};
constexpr std::chrono::seconds kOperationTimeout{10};

[[nodiscard]] NodeHostConfig host_config(
    NodeId node, std::filesystem::path wal,
    std::vector<TcpPeerEndpoint> peers, detlog::Tick election_timeout,
    std::uint64_t incarnation) {
  NodeHostConfig config;
  config.raft.node_id = node;
  config.raft.members = {1, 2, 3};
  config.raft.election_timeout = election_timeout;
  config.raft.heartbeat_interval = 2;
  config.identity = {kClusterHigh, kClusterLow, node};
  config.wal_path = std::move(wal);
  config.tcp.identity = {kClusterHigh, kClusterLow, node};
  config.tcp.incarnation = incarnation;
  config.tcp.peers = std::move(peers);
  config.tcp.limits.max_outbound_bytes_per_peer = 1024U * 1024U;
  config.tcp.limits.max_total_outbound_bytes = 2U * 1024U * 1024U;
  config.tcp.limits.max_inbound_events = 128;
  config.tcp.limits.max_inbound_event_bytes = 2U * 1024U * 1024U;
  config.tcp.limits.max_pending_connections = 16;
  config.tcp.limits.handshake_timeout_ms = 1000;
  config.tcp.limits.reconnect_delay_ms = 20;
  config.tcp.limits.io_poll_interval_ms = 2;
  config.tick_duration = std::chrono::milliseconds(5);
  config.election_jitter = 2;
  config.timer_seed = 0xdef000U + node;
  config.idle_sleep = std::chrono::milliseconds(0);
  config.limits.max_owner_events = 128;
  config.limits.max_client_queue = 32;
  config.limits.max_client_slots = 64;
  config.limits.max_trace_records = 512;
  config.limits.max_storage_tasks = 8;
  config.limits.max_storage_completions = 8;
  config.limits.max_group_quarantined_effects = 512;
  config.limits.max_tcp_events_per_poll = 32;
  return config;
}

class ReplyCollector {
 public:
  void collect(NodeId node, NodeHost& host) {
    for (ClientReply& reply : host.poll_replies()) {
      replies_[node][reply.token].push_back(std::move(reply));
    }
  }

  [[nodiscard]] const std::vector<ClientReply>& replies(
      NodeId node, ClientToken token) const {
    const auto node_replies = replies_.find(node);
    if (node_replies == replies_.end()) return empty_;
    const auto token_replies = node_replies->second.find(token);
    return token_replies == node_replies->second.end() ? empty_
                                                       : token_replies->second;
  }

  [[nodiscard]] bool has_reply(NodeId node, ClientToken token) const {
    return !replies(node, token).empty();
  }

  [[nodiscard]] bool has_status(NodeId node, ClientToken token,
                                ClientStatus status) const {
    for (const ClientReply& reply : replies(node, token)) {
      if (reply.status == status) return true;
    }
    return false;
  }

 private:
  std::map<NodeId, std::map<ClientToken, std::vector<ClientReply>>> replies_;
  std::vector<ClientReply> empty_;
};

using Hosts = std::array<std::unique_ptr<NodeHost>, 3>;

void require_runtime_bounds(const Hosts& hosts,
                            const std::array<NodeHostConfig, 3>& configs) {
  for (std::size_t index = 0; index < hosts.size(); ++index) {
    const auto status = hosts[index]->status();
    const auto metrics = hosts[index]->metrics();
    REQUIRE(status.owner_events <= configs[index].limits.max_owner_events);
    REQUIRE(status.client_queue <= configs[index].limits.max_client_queue);
    REQUIRE(metrics.owner_queue_high_water <=
            configs[index].limits.max_owner_events);
    REQUIRE(metrics.client_queue_high_water <=
            configs[index].limits.max_client_queue);
    REQUIRE(metrics.storage_task_queue_high_water <=
            configs[index].limits.max_storage_tasks);
    REQUIRE(metrics.storage_quarantine_high_water <=
            configs[index].limits.max_group_quarantined_effects);
  }
}

class RuntimeSafetyObserver {
 public:
  void observe(const Hosts& hosts) {
    for (std::size_t host_index = 0; host_index < hosts.size(); ++host_index) {
      const auto status = hosts[host_index]->status();
      const auto& log = hosts[host_index]->log_for_test();
      REQUIRE(status.term >= previous_terms_[host_index]);
      REQUIRE(status.commit_index >= previous_commits_[host_index]);
      REQUIRE(status.last_applied >= previous_applied_[host_index]);
      REQUIRE(status.last_applied <= status.commit_index);
      REQUIRE(status.last_applied <= status.durable_last_log_index);
      if (!status.storage_pending) {
        REQUIRE(status.commit_index <= status.durable_last_log_index);
      }
      REQUIRE(status.durable_last_log_index <= status.last_log_index);
      REQUIRE(status.last_log_index == log.size());
      previous_terms_[host_index] = status.term;
      previous_commits_[host_index] = status.commit_index;
      previous_applied_[host_index] = status.last_applied;

      if (status.role == RaftRole::leader) {
        const auto [found, inserted] =
            ready_leaders_.emplace(status.term, status.node);
        REQUIRE(inserted || found->second == status.node);
      }
      const LogIndex durable_commit =
          std::min(status.commit_index, status.durable_last_log_index);
      for (LogIndex index = 1; index <= durable_commit; ++index) {
        const auto& entry = log[static_cast<std::size_t>(index - 1U)];
        const auto [found, inserted] = committed_entries_.emplace(index, entry);
        REQUIRE(inserted || found->second == entry);
      }
    }

    for (std::size_t left = 0; left < hosts.size(); ++left) {
      const auto left_status = hosts[left]->status();
      const auto left_commit = std::min(left_status.commit_index,
                                        left_status.durable_last_log_index);
      const auto& left_log = hosts[left]->log_for_test();
      for (std::size_t right = left + 1U; right < hosts.size(); ++right) {
        const auto right_status = hosts[right]->status();
        const auto right_commit = std::min(right_status.commit_index,
                                           right_status.durable_last_log_index);
        const auto& right_log = hosts[right]->log_for_test();
        const LogIndex shared_commit = std::min(left_commit, right_commit);
        for (LogIndex index = 0; index < shared_commit; ++index) {
          const std::size_t offset = static_cast<std::size_t>(index);
          REQUIRE(left_log[offset] == right_log[offset]);
        }
      }
    }
  }

 private:
  std::array<detlog::Term, 3> previous_terms_{};
  std::array<LogIndex, 3> previous_commits_{};
  std::array<LogIndex, 3> previous_applied_{};
  std::map<detlog::Term, NodeId> ready_leaders_;
  std::map<LogIndex, detlog::LogEntry> committed_entries_;
};

template <typename Predicate, typename Observer>
[[nodiscard]] bool pump_until_checked(
    Hosts& hosts, const std::array<NodeHostConfig, 3>& configs,
    RuntimeSafetyObserver& safety, ReplyCollector& replies,
    Predicate predicate, Observer observer,
    std::chrono::milliseconds timeout = kOperationTimeout) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    bool activity = false;
    for (std::size_t index = 0; index < hosts.size(); ++index) {
      for (int iteration = 0; iteration < 8; ++iteration) {
        activity = hosts[index]->poll_once() || activity;
        replies.collect(configs[index].raft.node_id, *hosts[index]);
        require_runtime_bounds(hosts, configs);
        safety.observe(hosts);
        observer();
      }
    }
    if (predicate()) return true;
    if (!activity) std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  return false;
}

template <typename Predicate>
[[nodiscard]] bool pump_until(
    Hosts& hosts, const std::array<NodeHostConfig, 3>& configs,
    RuntimeSafetyObserver& safety, ReplyCollector& replies,
    Predicate predicate,
    std::chrono::milliseconds timeout = kOperationTimeout) {
  return pump_until_checked(hosts, configs, safety, replies,
                            std::move(predicate), [] {}, timeout);
}

[[nodiscard]] std::optional<std::size_t> ready_leader(
    const Hosts& hosts, std::optional<std::size_t> excluded = std::nullopt) {
  std::optional<std::size_t> result;
  for (std::size_t index = 0; index < hosts.size(); ++index) {
    if (excluded && index == *excluded) continue;
    const auto status = hosts[index]->status();
    if (status.role != RaftRole::leader || !status.leader_ready) continue;
    if (result) return std::nullopt;
    result = index;
  }
  return result;
}

[[nodiscard]] ClientCommand put(SessionId session, std::uint64_t sequence,
                                std::string value) {
  return ClientCommand{session, sequence, CommandKind::put, "partition-key",
                       std::move(value)};
}

[[nodiscard]] std::size_t command_occurrences(const NodeHost& host,
                                              const SessionId& session,
                                              std::uint64_t sequence) {
  std::size_t result = 0;
  for (const auto& entry : host.log_for_test()) {
    if (entry.command && entry.command->session == session &&
        entry.command->sequence == sequence) {
      ++result;
    }
  }
  return result;
}

[[nodiscard]] bool committed_prefixes_equal(const Hosts& hosts) {
  LogIndex target = 0;
  for (const auto& host : hosts) {
    const auto status = host->status();
    if (status.storage_pending ||
        status.commit_index > status.durable_last_log_index) {
      return false;
    }
    target = std::max(target, status.commit_index);
  }
  const auto& expected = hosts.front()->log_for_test();
  if (expected.size() < target) return false;
  for (const auto& host : hosts) {
    if (host->status().commit_index != target) return false;
    const auto& log = host->log_for_test();
    if (log.size() < target) return false;
    for (LogIndex index = 0; index < target; ++index) {
      const std::size_t offset = static_cast<std::size_t>(index);
      if (log[offset] != expected[offset]) return false;
    }
  }
  return true;
}

void set_leader_partition(Hosts& hosts, std::size_t leader,
                          bool partitioned) {
  const NodeId leader_id = hosts[leader]->status().node;
  for (std::size_t index = 0; index < hosts.size(); ++index) {
    if (index == leader) continue;
    const NodeId peer_id = hosts[index]->status().node;
    REQUIRE(hosts[leader]->set_peer_partitioned(peer_id, partitioned));
    REQUIRE(hosts[index]->set_peer_partitioned(leader_id, partitioned));
  }
}

void test_literal_tcp_partition_recovery_and_ambiguous_retry() {
  TempDirectory directory;
  std::array<NodeHostConfig, 3> configs;
  Hosts hosts;

  configs[0] = host_config(
      1, directory.wal(1),
      {TcpPeerEndpoint{2, "127.0.0.1", 0},
       TcpPeerEndpoint{3, "127.0.0.1", 0}},
      20, 501);
  hosts[0] = std::make_unique<NodeHost>(configs[0]);
  hosts[0]->start();
  const std::uint16_t port_one = hosts[0]->listening_port();

  configs[1] = host_config(
      2, directory.wal(2),
      {TcpPeerEndpoint{1, "127.0.0.1", port_one},
       TcpPeerEndpoint{3, "127.0.0.1", 0}},
      40, 502);
  hosts[1] = std::make_unique<NodeHost>(configs[1]);
  hosts[1]->start();
  const std::uint16_t port_two = hosts[1]->listening_port();

  configs[2] = host_config(
      3, directory.wal(3),
      {TcpPeerEndpoint{1, "127.0.0.1", port_one},
       TcpPeerEndpoint{2, "127.0.0.1", port_two}},
      60, 503);
  hosts[2] = std::make_unique<NodeHost>(configs[2]);
  hosts[2]->start();

  RuntimeSafetyObserver safety;
  ReplyCollector replies;
  std::optional<std::size_t> initial_leader;
  REQUIRE(pump_until(hosts, configs, safety, replies, [&] {
    initial_leader = ready_leader(hosts);
    return initial_leader.has_value();
  }));
  std::size_t old_leader = *initial_leader;
  NodeId old_leader_id = hosts[old_leader]->status().node;

  const SessionId session{0xabcddcba, 0x12344321};
  const ClientCommand baseline = put(session, 1, "baseline");
  const auto baseline_submit = hosts[old_leader]->submit(baseline);
  REQUIRE(baseline_submit.status == NodeHostSubmitStatus::accepted);
  REQUIRE(pump_until(hosts, configs, safety, replies, [&] {
    return replies.has_reply(old_leader_id, baseline_submit.token);
  }));
  const auto& baseline_replies =
      replies.replies(old_leader_id, baseline_submit.token);
  REQUIRE(baseline_replies.size() == 1);
  REQUIRE(baseline_replies.front().status == ClientStatus::ok);
  const LogIndex baseline_index = baseline_replies.front().log_index;
  REQUIRE(baseline_index != 0);
  REQUIRE(pump_until(hosts, configs, safety, replies, [&] {
    for (const auto& host : hosts) {
      if (host->value_for_test("partition-key") !=
              std::optional<std::string>{"baseline"} ||
          host->status().last_applied < baseline_index) {
        return false;
      }
    }
    return true;
  }));

  // Wall-clock scheduling can legitimately advance the term while the
  // baseline is propagating. Cut whichever node is the unique ready leader at
  // the actual fault boundary so the isolated request is guaranteed to enter
  // a leader's log rather than a stale host queue.
  REQUIRE(pump_until(hosts, configs, safety, replies, [&] {
    initial_leader = ready_leader(hosts);
    return initial_leader.has_value();
  }));
  old_leader = *initial_leader;
  old_leader_id = hosts[old_leader]->status().node;

  const auto isolated_baseline = hosts[old_leader]->status();
  const auto require_isolated_safe = [&] {
    const auto status = hosts[old_leader]->status();
    REQUIRE(status.commit_index == isolated_baseline.commit_index);
    REQUIRE(status.last_applied == isolated_baseline.last_applied);
    REQUIRE(hosts[old_leader]->value_for_test("partition-key") ==
            std::optional<std::string>{"baseline"});
  };

  set_leader_partition(hosts, old_leader, true);
  const auto partition_started = std::chrono::steady_clock::now();

  const ClientCommand ambiguous = put(session, 2, "after-partition");
  const auto isolated_submit = hosts[old_leader]->submit(ambiguous);
  REQUIRE(isolated_submit.status == NodeHostSubmitStatus::accepted);
  const auto require_partition_safe = [&] {
    require_isolated_safe();
    REQUIRE(!replies.has_status(old_leader_id, isolated_submit.token,
                                ClientStatus::ok));
  };
  REQUIRE(pump_until_checked(
      hosts, configs, safety, replies,
      [&] { return command_occurrences(*hosts[old_leader], session, 2) == 1; },
      require_partition_safe));
  REQUIRE(!replies.has_status(old_leader_id, isolated_submit.token,
                              ClientStatus::ok));

  std::optional<std::size_t> majority_leader;
  REQUIRE(pump_until_checked(
      hosts, configs, safety, replies,
      [&] {
        majority_leader = ready_leader(hosts, old_leader);
        return majority_leader.has_value();
      },
      require_partition_safe));
  REQUIRE(*majority_leader != old_leader);
  const NodeId majority_leader_id = hosts[*majority_leader]->status().node;

  const auto majority_submit = hosts[*majority_leader]->submit(ambiguous);
  REQUIRE(majority_submit.status == NodeHostSubmitStatus::accepted);
  REQUIRE(pump_until_checked(
      hosts, configs, safety, replies,
      [&] {
        return replies.has_reply(majority_leader_id, majority_submit.token);
      },
      require_partition_safe));
  const auto& majority_replies =
      replies.replies(majority_leader_id, majority_submit.token);
  REQUIRE(majority_replies.size() == 1);
  REQUIRE(majority_replies.front().status == ClientStatus::ok);
  REQUIRE(majority_replies.front().value == "after-partition");
  const LogIndex majority_commit = majority_replies.front().log_index;
  REQUIRE(majority_commit > baseline_index);

  REQUIRE(pump_until_checked(
      hosts, configs, safety, replies,
      [&] {
        return std::chrono::steady_clock::now() - partition_started >=
               kMinimumPartition;
      },
      require_partition_safe, kMinimumPartition + std::chrono::seconds(2)));
  const auto measured_partition =
      std::chrono::steady_clock::now() - partition_started;
  REQUIRE(measured_partition >= kMinimumPartition);
  REQUIRE(!replies.has_status(old_leader_id, isolated_submit.token,
                              ClientStatus::ok));

  REQUIRE(pump_until_checked(
      hosts, configs, safety, replies,
      [&] {
        std::size_t majority_applied = 0;
        for (std::size_t index = 0; index < hosts.size(); ++index) {
          if (index == old_leader) continue;
          if (hosts[index]->value_for_test("partition-key") ==
                  std::optional<std::string>{"after-partition"} &&
              hosts[index]->status().last_applied >= majority_commit) {
            ++majority_applied;
          }
        }
        return majority_applied == 2;
      },
      require_partition_safe));

  set_leader_partition(hosts, old_leader, false);
  REQUIRE(pump_until(hosts, configs, safety, replies, [&] {
    for (const auto& host : hosts) {
      const auto status = host->status();
      if (host->value_for_test("partition-key") !=
              std::optional<std::string>{"after-partition"} ||
          status.commit_index < majority_commit ||
          status.last_applied < majority_commit ||
          command_occurrences(*host, session, 2) != 1) {
        return false;
      }
    }
    return committed_prefixes_equal(hosts);
  }));
  REQUIRE(committed_prefixes_equal(hosts));
  REQUIRE(!replies.has_status(old_leader_id, isolated_submit.token,
                              ClientStatus::ok));

  std::optional<std::size_t> healed_leader;
  REQUIRE(pump_until(hosts, configs, safety, replies, [&] {
    healed_leader = ready_leader(hosts);
    return healed_leader.has_value();
  }));
  const NodeId healed_leader_id = hosts[*healed_leader]->status().node;
  std::array<std::size_t, 3> log_sizes{};
  for (std::size_t index = 0; index < hosts.size(); ++index) {
    log_sizes[index] = hosts[index]->log_for_test().size();
  }

  const auto dedup_submit = hosts[*healed_leader]->submit(ambiguous);
  REQUIRE(dedup_submit.status == NodeHostSubmitStatus::accepted);
  REQUIRE(pump_until(hosts, configs, safety, replies, [&] {
    return replies.has_reply(healed_leader_id, dedup_submit.token);
  }));
  const auto& dedup_replies =
      replies.replies(healed_leader_id, dedup_submit.token);
  REQUIRE(dedup_replies.size() == 1);
  REQUIRE(dedup_replies.front().status == ClientStatus::ok);
  REQUIRE(dedup_replies.front().value == "after-partition");
  REQUIRE(pump_until(hosts, configs, safety, replies, [] { return false; },
                     std::chrono::milliseconds(75)) == false);
  REQUIRE(replies.replies(healed_leader_id, dedup_submit.token).size() == 1);
  for (std::size_t index = 0; index < hosts.size(); ++index) {
    REQUIRE(hosts[index]->log_for_test().size() == log_sizes[index]);
    REQUIRE(command_occurrences(*hosts[index], session, 1) == 1);
    REQUIRE(command_occurrences(*hosts[index], session, 2) == 1);
  }
  require_runtime_bounds(hosts, configs);

  for (auto& host : hosts) host->stop();
}

}  // namespace

int main() {
  try {
    test_literal_tcp_partition_recovery_and_ambiguous_retry();
    std::cout << "node_host_partition_tests: all checks passed\n";
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "node_host_partition_tests: " << error.what() << '\n';
    return 1;
  }
}
