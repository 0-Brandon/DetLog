#include "detlog/node_host.hpp"

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
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
  TempDirectory() {
    const auto stamp = std::chrono::steady_clock::now()
                           .time_since_epoch()
                           .count();
    path_ = std::filesystem::temp_directory_path() /
            ("detlog-node-host-" + std::to_string(stamp));
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

constexpr std::uint64_t kClusterHigh = 0x4445544c4f470001ULL;
constexpr std::uint64_t kClusterLow = 0x484f535454455354ULL;

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
  config.tcp.limits.handshake_timeout_ms = 1000;
  config.tcp.limits.reconnect_delay_ms = 20;
  config.tcp.limits.io_poll_interval_ms = 2;
  config.tick_duration = std::chrono::milliseconds(5);
  config.election_jitter = 2;
  config.timer_seed = 0xabc000U + node;
  config.idle_sleep = std::chrono::milliseconds(0);
  return config;
}

struct ReplyCollector {
  std::map<detlog::ClientToken, std::vector<ClientReply>> by_token;

  void collect(NodeHost& host) {
    for (ClientReply& reply : host.poll_replies()) {
      by_token[reply.token].push_back(std::move(reply));
    }
  }
};

template <typename Predicate>
[[nodiscard]] bool pump_until(
    std::array<std::unique_ptr<NodeHost>, 3>& hosts,
    ReplyCollector& replies, Predicate&& predicate,
    std::chrono::milliseconds timeout = std::chrono::seconds(8)) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    bool activity = false;
    for (auto& host : hosts) {
      if (!host) continue;
      for (int iteration = 0; iteration < 8; ++iteration) {
        activity = host->poll_once() || activity;
      }
      replies.collect(*host);
    }
    if (std::forward<Predicate>(predicate)()) return true;
    if (!activity) std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  return false;
}

[[nodiscard]] ClientCommand put(SessionId session, std::uint64_t sequence,
                                std::string value) {
  return ClientCommand{session, sequence, CommandKind::put, "shared-key",
                       std::move(value)};
}

void test_three_host_runtime_recovery_and_dedup() {
  TempDirectory directory;
  std::array<NodeHostConfig, 3> configs;
  std::array<std::unique_ptr<NodeHost>, 3> hosts;

  // This is a wall-clock TCP/fsync integration test. Keep election windows
  // comfortably above ordinary Windows scheduler and flush variance while
  // retaining a deterministic stagger between the three nodes.
  configs[0] = host_config(
      1, directory.wal(1),
      {TcpPeerEndpoint{2, "127.0.0.1", 0},
       TcpPeerEndpoint{3, "127.0.0.1", 0}},
      50, 101);
  hosts[0] = std::make_unique<NodeHost>(configs[0]);
  hosts[0]->start();
  const std::uint16_t port_one = hosts[0]->listening_port();

  configs[1] = host_config(
      2, directory.wal(2),
      {TcpPeerEndpoint{1, "127.0.0.1", port_one},
       TcpPeerEndpoint{3, "127.0.0.1", 0}},
      100, 102);
  hosts[1] = std::make_unique<NodeHost>(configs[1]);
  hosts[1]->start();
  const std::uint16_t port_two = hosts[1]->listening_port();

  configs[2] = host_config(
      3, directory.wal(3),
      {TcpPeerEndpoint{1, "127.0.0.1", port_one},
       TcpPeerEndpoint{2, "127.0.0.1", port_two}},
      150, 103);
  hosts[2] = std::make_unique<NodeHost>(configs[2]);
  hosts[2]->start();

  ReplyCollector replies;
  NodeHost* leader = nullptr;
  REQUIRE(pump_until(hosts, replies, [&] {
    leader = nullptr;
    for (auto& host : hosts) {
      const auto status = host->status();
      if (status.role == RaftRole::leader && status.leader_ready) {
        if (leader != nullptr) return false;
        leader = host.get();
      }
    }
    return leader != nullptr;
  }));

  const SessionId session{0x1111, 0x2222};
  const ClientCommand first_command = put(session, 1, "value-one");
  const auto first_submit = leader->submit(first_command);
  REQUIRE(first_submit.status == NodeHostSubmitStatus::accepted);
  REQUIRE(pump_until(hosts, replies, [&] {
    return replies.by_token.contains(first_submit.token) &&
           !replies.by_token[first_submit.token].empty();
  }));
  REQUIRE(replies.by_token[first_submit.token].size() == 1);
  REQUIRE(replies.by_token[first_submit.token].front().status ==
          ClientStatus::ok);
  REQUIRE(replies.by_token[first_submit.token].front().value == "value-one");

  const std::size_t log_size_after_first = leader->log_for_test().size();
  const auto duplicate_submit = leader->submit(first_command);
  REQUIRE(duplicate_submit.status == NodeHostSubmitStatus::accepted);
  REQUIRE(pump_until(hosts, replies, [&] {
    return replies.by_token.contains(duplicate_submit.token) &&
           !replies.by_token[duplicate_submit.token].empty();
  }));
  REQUIRE(replies.by_token[duplicate_submit.token].size() == 1);
  REQUIRE(replies.by_token[duplicate_submit.token].front().status ==
          ClientStatus::ok);
  REQUIRE(leader->log_for_test().size() == log_size_after_first);
  REQUIRE(pump_until(hosts, replies, [] { return false; },
                     std::chrono::milliseconds(75)) == false);
  REQUIRE(replies.by_token[duplicate_submit.token].size() == 1);

  std::size_t leader_index = 0;
  for (std::size_t index = 0; index < hosts.size(); ++index) {
    if (hosts[index].get() == leader) leader_index = index;
  }
  const std::size_t restart_index = leader_index == 2 ? 1 : 2;
  const std::uint16_t restart_port = hosts[restart_index]->listening_port();
  hosts[restart_index]->stop();
  hosts[restart_index].reset();

  const ClientCommand second_command = put(session, 2, "value-two");
  const auto second_submit = leader->submit(second_command);
  REQUIRE(second_submit.status == NodeHostSubmitStatus::accepted);
  REQUIRE(pump_until(hosts, replies, [&] {
    return replies.by_token.contains(second_submit.token) &&
           !replies.by_token[second_submit.token].empty();
  }));
  REQUIRE(replies.by_token[second_submit.token].front().status ==
          ClientStatus::ok);

  configs[restart_index].tcp.listen_port = restart_port;
  configs[restart_index].tcp.incarnation += 1000;
  hosts[restart_index] =
      std::make_unique<NodeHost>(configs[restart_index]);
  hosts[restart_index]->start();
  REQUIRE(pump_until(hosts, replies, [&] {
    return hosts[restart_index]->value_for_test("shared-key") ==
               std::optional<std::string>{"value-two"} &&
           hosts[restart_index]->status().commit_index >=
               leader->status().commit_index;
  }));

  for (auto& host : hosts) {
    if (host) host->stop();
  }
}

void test_host_rejects_unsendable_and_oversized_admission() {
  TempDirectory directory;
  auto config = host_config(
      1, directory.wal(1),
      {TcpPeerEndpoint{2, "127.0.0.1", 0},
       TcpPeerEndpoint{3, "127.0.0.1", 0}},
      10, 9001);
  config.tcp.limits.max_outbound_bytes_per_peer =
      config.raft.max_append_bytes_per_rpc;
  bool rejected = false;
  try {
    NodeHost invalid(config);
    (void)invalid;
  } catch (const std::invalid_argument&) {
    rejected = true;
  }
  REQUIRE(rejected);

  config.tcp.limits.max_outbound_bytes_per_peer = 4U * 1024U * 1024U;
  NodeHost host(std::move(config));
  host.start();
  ClientCommand oversized{SessionId{1, 2}, 1, CommandKind::put,
                            std::string(4097, 'k'), "value"};
  REQUIRE(host.submit(std::move(oversized)).status ==
          detlog::NodeHostSubmitStatus::error);
  host.stop();
}

}  // namespace

int main() {
  try {
    test_host_rejects_unsendable_and_oversized_admission();
    test_three_host_runtime_recovery_and_dedup();
    std::cout << "node_host_tests: all checks passed\n";
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "node_host_tests: " << error.what() << '\n';
    return 1;
  }
}
