#include "detlog/node_host.hpp"

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
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace {

using detlog::ClientCommand;
using detlog::ClientReply;
using detlog::ClientStatus;
using detlog::CommandKind;
using detlog::NodeHost;
using detlog::NodeHostConfig;
using detlog::NodeHostSubmitStatus;
using detlog::NodeId;
using detlog::RaftRole;
using detlog::SessionId;
using detlog::TcpPeerEndpoint;

[[noreturn]] void fail(std::string message, int line) {
  throw std::runtime_error("line " + std::to_string(line) + ": " + message);
}

#define REQUIRE(condition)                                                   \
  do {                                                                       \
    if (!(condition)) fail("requirement failed: " #condition, __LINE__);    \
  } while (false)

class TempDirectory {
 public:
  explicit TempDirectory(std::string_view label) {
    const auto stamp = std::chrono::steady_clock::now()
                           .time_since_epoch()
                           .count();
    path_ = std::filesystem::temp_directory_path() /
            ("detlog-group-" + std::string(label) + "-" +
             std::to_string(stamp));
    std::filesystem::create_directories(path_);
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

[[nodiscard]] NodeHostConfig config_for(
    NodeId node, std::filesystem::path wal,
    std::vector<TcpPeerEndpoint> peers, detlog::Tick election_timeout,
    bool grouped) {
  NodeHostConfig config;
  config.raft.node_id = node;
  config.raft.members = {1, 2, 3};
  config.raft.election_timeout = election_timeout;
  config.raft.heartbeat_interval = 2;
  config.identity = {0x47524f5550434d54ULL, 0x54455354434c5354ULL, node};
  config.wal_path = std::move(wal);
  config.tcp.identity = {config.identity.cluster_id_high,
                         config.identity.cluster_id_low, node};
  config.tcp.incarnation = 10'000U + node;
  config.tcp.peers = std::move(peers);
  config.tcp.limits.handshake_timeout_ms = 1000;
  config.tcp.limits.reconnect_delay_ms = 10;
  config.tcp.limits.io_poll_interval_ms = 1;
  config.tick_duration = std::chrono::milliseconds(5);
  config.election_jitter = 1;
  config.timer_seed = 0x47524f55ULL + node;
  config.idle_sleep = std::chrono::milliseconds(0);
  config.group_commit.enabled = grouped;
  config.group_commit.max_operations = 8;
  config.group_commit.max_delay =
      grouped ? std::chrono::milliseconds(100)
              : std::chrono::milliseconds(2);
  return config;
}

struct Hosts {
  std::array<NodeHostConfig, 3> configs;
  std::array<std::unique_ptr<NodeHost>, 3> nodes;
};

[[nodiscard]] Hosts start_hosts(
    const TempDirectory& directory, bool grouped,
    std::chrono::milliseconds group_delay = std::chrono::milliseconds(100)) {
  Hosts result;
  const bool long_group_delay = group_delay > std::chrono::milliseconds(100);
  const detlog::Tick first_timeout = long_group_delay ? 600U : 40U;
  const detlog::Tick second_timeout = long_group_delay ? 1200U : 90U;
  const detlog::Tick third_timeout = long_group_delay ? 1800U : 140U;
  result.configs[0] = config_for(
      1, directory.wal(1),
      {TcpPeerEndpoint{2, "127.0.0.1", 0},
       TcpPeerEndpoint{3, "127.0.0.1", 0}},
      first_timeout, grouped);
  result.configs[0].group_commit.max_delay = group_delay;
  result.nodes[0] = std::make_unique<NodeHost>(result.configs[0]);
  result.nodes[0]->start();
  const std::uint16_t port_one = result.nodes[0]->listening_port();

  result.configs[1] = config_for(
      2, directory.wal(2),
      {TcpPeerEndpoint{1, "127.0.0.1", port_one},
       TcpPeerEndpoint{3, "127.0.0.1", 0}},
      second_timeout, grouped);
  result.configs[1].group_commit.max_delay = group_delay;
  result.nodes[1] = std::make_unique<NodeHost>(result.configs[1]);
  result.nodes[1]->start();
  const std::uint16_t port_two = result.nodes[1]->listening_port();

  result.configs[2] = config_for(
      3, directory.wal(3),
      {TcpPeerEndpoint{1, "127.0.0.1", port_one},
       TcpPeerEndpoint{2, "127.0.0.1", port_two}},
      third_timeout, grouped);
  result.configs[2].group_commit.max_delay = group_delay;
  result.nodes[2] = std::make_unique<NodeHost>(result.configs[2]);
  result.nodes[2]->start();
  return result;
}

struct Replies {
  std::map<detlog::ClientToken, ClientReply> values;

  void collect(NodeHost& host) {
    for (ClientReply& reply : host.poll_replies()) {
      values.emplace(reply.token, std::move(reply));
    }
  }
};

template <typename Predicate>
[[nodiscard]] bool pump(Hosts& hosts, Replies& replies, Predicate predicate,
                        std::chrono::milliseconds timeout) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    bool activity = false;
    for (auto& host : hosts.nodes) {
      if (!host) continue;
      for (int iteration = 0; iteration < 8; ++iteration) {
        activity = host->poll_once() || activity;
      }
      replies.collect(*host);
    }
    if (predicate()) return true;
    if (!activity) std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  return false;
}

[[nodiscard]] NodeHost* wait_for_leader(Hosts& hosts, Replies& replies) {
  NodeHost* leader = nullptr;
  REQUIRE(pump(
      hosts, replies,
      [&] {
        leader = nullptr;
        for (auto& host : hosts.nodes) {
          const auto status = host->status();
          if (status.role == RaftRole::leader && status.leader_ready) {
            if (leader != nullptr) return false;
            leader = host.get();
          }
        }
        return leader != nullptr;
      },
      std::chrono::seconds(20)));
  return leader;
}

[[nodiscard]] ClientCommand command(std::uint64_t client,
                                    std::string value) {
  return ClientCommand{SessionId{0x47524f5550544553ULL, client}, 1,
                       CommandKind::put, "group-key", std::move(value)};
}

void stop_hosts(Hosts& hosts) {
  for (auto& host : hosts.nodes) {
    if (host) host->stop();
  }
}

void safe_group_commit_quarantines_and_batches() {
  TempDirectory directory("safe");
  Hosts hosts = start_hosts(directory, true);
  Replies replies;
  NodeHost* leader = wait_for_leader(hosts, replies);
  const auto before = leader->status();

  std::vector<detlog::ClientToken> tokens;
  for (std::uint64_t client = 1; client <= 6; ++client) {
    const auto submitted = leader->submit(
        command(client, "value-" + std::to_string(client)));
    REQUIRE(submitted.status == NodeHostSubmitStatus::accepted);
    tokens.push_back(submitted.token);
  }

  // The group delay is 100 ms. Drain staged write completions for a shorter
  // interval and prove no commit/apply/reply or published status crossed the
  // missing durability barrier.
  REQUIRE(!pump(hosts, replies, [] { return false; },
                std::chrono::milliseconds(25)));
  for (const auto token : tokens) REQUIRE(!replies.values.contains(token));
  const auto during = leader->status();
  REQUIRE(during.commit_index == before.commit_index);
  REQUIRE(during.last_applied == before.last_applied);
  REQUIRE(!leader->value_for_test("group-key"));

  REQUIRE(pump(
      hosts, replies,
      [&] {
        for (const auto token : tokens) {
          const auto found = replies.values.find(token);
          if (found == replies.values.end() ||
              found->second.status != ClientStatus::ok) {
            return false;
          }
        }
        return true;
      },
      std::chrono::seconds(10)));

  const auto metrics = leader->metrics();
  REQUIRE(metrics.storage_group_commits > 0);
  REQUIRE(metrics.storage_grouped_operations >= metrics.storage_group_commits);
  REQUIRE(metrics.storage_group_max_size >= 2);
  REQUIRE(metrics.storage_flushes == metrics.storage_group_commits);
  REQUIRE(metrics.storage_flushes < metrics.storage_grouped_operations);
  const std::size_t leader_offset =
      static_cast<std::size_t>(leader->status().node - 1U);
  REQUIRE(metrics.storage_group_max_bytes > 0);
  REQUIRE(metrics.storage_group_max_bytes <=
          hosts.configs[leader_offset].wal.max_group_bytes);
  REQUIRE(metrics.storage_task_queue_high_water <= 16U);
  REQUIRE(metrics.storage_quarantine_high_water > 0);

  stop_hosts(hosts);
  for (auto& host : hosts.nodes) host.reset();

  // Every successful client reply preceded a shared flush. Reconstructing from
  // the same WAL must therefore retain the applied state without peer help.
  std::size_t recovered_values = 0;
  std::array<std::size_t, 6> durable_command_copies{};
  for (std::size_t index = 0; index < hosts.configs.size(); ++index) {
    hosts.configs[index].tcp.incarnation += 1000;
    NodeHost recovered(hosts.configs[index]);
    REQUIRE(recovered.status().last_applied == recovered.status().commit_index);
    if (recovered.value_for_test("group-key")) ++recovered_values;
    for (const auto& entry : recovered.log_for_test()) {
      if (!entry.command ||
          entry.command->session.high != 0x47524f5550544553ULL ||
          entry.command->session.low == 0 ||
          entry.command->session.low > durable_command_copies.size()) {
        continue;
      }
      ++durable_command_copies[static_cast<std::size_t>(
          entry.command->session.low - 1U)];
    }
  }
  REQUIRE(recovered_values >= 1U);
  for (const std::size_t copies : durable_command_copies) {
    REQUIRE(copies >= 2U);
  }
}

void stop_before_barrier_discards_speculative_effects() {
  TempDirectory directory("pre-barrier-stop");
  Hosts hosts = start_hosts(directory, true, std::chrono::seconds(1));
  Replies replies;
  NodeHost* leader = wait_for_leader(hosts, replies);
  const NodeId leader_id = leader->status().node;
  const auto baseline = leader->status();
  const auto baseline_metrics = leader->metrics();

  const auto submitted = leader->submit(command(77, "must-not-apply"));
  REQUIRE(submitted.status == NodeHostSubmitStatus::accepted);
  REQUIRE(pump(
      hosts, replies,
      [&] {
        return leader->metrics().storage_staged_operations >
               baseline_metrics.storage_staged_operations;
      },
      std::chrono::milliseconds(500)));
  REQUIRE(!replies.values.contains(submitted.token));
  REQUIRE(leader->metrics().storage_staged_operations >
          baseline_metrics.storage_staged_operations);
  REQUIRE(leader->metrics().storage_group_commits ==
          baseline_metrics.storage_group_commits);
  REQUIRE(leader->status().commit_index == baseline.commit_index);
  REQUIRE(leader->status().last_applied == baseline.last_applied);
  REQUIRE(!leader->value_for_test("group-key"));

  stop_hosts(hosts);
  REQUIRE(!replies.values.contains(submitted.token));
  for (auto& host : hosts.nodes) host.reset();

  const std::size_t leader_offset =
      static_cast<std::size_t>(leader_id - 1U);
  hosts.configs[leader_offset].tcp.incarnation += 1000;
  NodeHost recovered(hosts.configs[leader_offset]);
  REQUIRE(recovered.status().commit_index == baseline.commit_index);
  REQUIRE(recovered.status().last_applied == baseline.last_applied);
  REQUIRE(!recovered.value_for_test("group-key"));
}

void quarantine_overflow_preserves_durable_status() {
  TempDirectory directory("quarantine-overflow");
  auto config = config_for(
      1, directory.wal(1),
      {TcpPeerEndpoint{2, "127.0.0.1", 0},
       TcpPeerEndpoint{3, "127.0.0.1", 0}},
      1, true);
  config.group_commit.max_delay = std::chrono::milliseconds(500);
  config.limits.max_group_quarantined_effects = 6;
  NodeHost host(config);
  const auto baseline = host.status();
  host.start();
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(3);
  while (host.running() && std::chrono::steady_clock::now() < deadline) {
    (void)host.poll_once();
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  REQUIRE(!host.running());
  REQUIRE(host.metrics().storage_staged_operations > 0);
  REQUIRE(host.metrics().storage_errors > 0);
  const auto failed = host.status();
  REQUIRE(failed.role == RaftRole::unavailable);
  REQUIRE(failed.term == baseline.term);
  REQUIRE(failed.last_log_index == baseline.last_log_index);
  REQUIRE(failed.durable_last_log_index == baseline.durable_last_log_index);
  REQUIRE(failed.commit_index == baseline.commit_index);
  REQUIRE(failed.last_applied == baseline.last_applied);
}

void flush_every_remains_the_default() {
  TempDirectory directory("default");
  Hosts hosts = start_hosts(directory, false);
  Replies replies;
  NodeHost* leader = wait_for_leader(hosts, replies);
  const auto submitted = leader->submit(command(101, "default-safe"));
  REQUIRE(submitted.status == NodeHostSubmitStatus::accepted);
  REQUIRE(pump(
      hosts, replies,
      [&] {
        return replies.values.contains(submitted.token) &&
               replies.values.at(submitted.token).status == ClientStatus::ok;
      },
      std::chrono::seconds(8)));
  const auto metrics = leader->metrics();
  REQUIRE(metrics.storage_flushes > 0);
  REQUIRE(metrics.storage_group_commits == 0);
  REQUIRE(metrics.storage_grouped_operations == 0);
  stop_hosts(hosts);
}

void invalid_group_contracts_are_rejected() {
  TempDirectory directory("invalid");
  auto config = config_for(
      1, directory.wal(1),
      {TcpPeerEndpoint{2, "127.0.0.1", 0},
       TcpPeerEndpoint{3, "127.0.0.1", 0}},
      10, true);
  config.wal.flush_policy = detlog::WalFlushPolicy::unsafe_no_flush;
  bool rejected = false;
  try {
    NodeHost invalid(std::move(config));
    (void)invalid;
  } catch (const std::invalid_argument&) {
    rejected = true;
  }
  REQUIRE(rejected);
}

}  // namespace

int main() {
  try {
    invalid_group_contracts_are_rejected();
    flush_every_remains_the_default();
    quarantine_overflow_preserves_durable_status();
    stop_before_barrier_discards_speculative_effects();
    safe_group_commit_quarantines_and_batches();
    std::cout << "node_host_group_commit_tests: all checks passed\n";
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "node_host_group_commit_tests: " << error.what() << '\n';
    return 1;
  }
}
