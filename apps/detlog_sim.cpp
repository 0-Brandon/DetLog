#include "detlog/cluster.hpp"

#include <charconv>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace {

[[nodiscard]] std::uint64_t parse_unsigned(std::string_view text,
                                           std::string_view option) {
  std::uint64_t value{};
  const auto [end, error] =
      std::from_chars(text.data(), text.data() + text.size(), value);
  if (error != std::errc{} || end != text.data() + text.size()) {
    throw std::invalid_argument(std::string(option) +
                                " requires an unsigned integer");
  }
  return value;
}

void print_usage(std::ostream& output) {
  output << "usage: detlog-sim [--seed N] [--nodes 3|5] [--trace FILE] "
            "[--scenario leader-crash|symmetric-partition|"
            "asymmetric-partition|ambiguous-retry|torn-wal|slow-follower|"
            "slow-disk|saturation]\n";
}

[[nodiscard]] detlog::ClientCommand put(detlog::SessionId session,
                                        std::uint64_t sequence,
                                        std::string value) {
  return detlog::ClientCommand{session, sequence, detlog::CommandKind::put,
                               "demo-key", std::move(value)};
}

[[nodiscard]] detlog::ClientReply commit(
    detlog::Cluster& cluster, detlog::NodeId leader,
    const detlog::ClientCommand& command) {
  const auto token = cluster.submit(leader, command);
  if (!cluster.run_until_reply(token, 50'000)) {
    throw std::runtime_error("client request did not complete");
  }
  const auto reply = cluster.reply_for(token);
  if (!reply || reply->status != detlog::ClientStatus::ok) {
    throw std::runtime_error("client request did not commit successfully");
  }
  return *reply;
}

struct ScenarioSummary {
  detlog::NodeId first_leader{};
  std::optional<detlog::NodeId> replacement_leader;
  detlog::LogIndex first_commit{};
  detlog::LogIndex second_commit{};
};

[[nodiscard]] detlog::NodeId wait_replacement(detlog::Cluster& cluster,
                                              detlog::NodeId old_leader) {
  const bool found = cluster.run_until(
      [old_leader](const detlog::Cluster& running) {
        for (const detlog::NodeId leader : running.leaders(true)) {
          if (leader != old_leader) return true;
        }
        return false;
      },
      50'000);
  if (!found) throw std::runtime_error("cluster did not elect a replacement");
  for (const detlog::NodeId leader : cluster.leaders(true)) {
    if (leader != old_leader) return leader;
  }
  throw std::runtime_error("replacement disappeared after election");
}

void wait_value(detlog::Cluster& cluster, detlog::NodeId node,
                std::string_view expected) {
  if (!cluster.run_until(
          [node, expected](const detlog::Cluster& running) {
            return running.value(node, "demo-key") ==
                   std::optional<std::string>{expected};
          },
          50'000)) {
    throw std::runtime_error("node did not catch up to the expected value");
  }
}

[[nodiscard]] ScenarioSummary run_leader_crash(
    detlog::Cluster& cluster, detlog::NodeId first_leader,
    detlog::SessionId session) {
  ScenarioSummary result;
  result.first_leader = first_leader;
  result.first_commit =
      commit(cluster, first_leader, put(session, 1, "before-crash")).log_index;
  if (!cluster.crash(first_leader)) {
    throw std::runtime_error("could not crash the first leader");
  }
  const detlog::NodeId replacement = wait_replacement(cluster, first_leader);
  result.replacement_leader = replacement;
  result.second_commit =
      commit(cluster, replacement, put(session, 2, "after-crash")).log_index;
  if (!cluster.restart(first_leader)) {
    throw std::runtime_error("could not restart the old leader");
  }
  wait_value(cluster, first_leader, "after-crash");
  return result;
}

[[nodiscard]] ScenarioSummary run_partition(
    detlog::Cluster& cluster, detlog::NodeId first_leader,
    detlog::SessionId session, bool symmetric) {
  ScenarioSummary result;
  result.first_leader = first_leader;
  result.first_commit =
      commit(cluster, first_leader, put(session, 1, "before-partition"))
          .log_index;
  if (symmetric) {
    if (!cluster.isolate(first_leader, true)) {
      throw std::runtime_error("could not isolate leader");
    }
  } else {
    for (const detlog::NodeId peer : cluster.members()) {
      if (peer != first_leader &&
          !cluster.set_partition(first_leader, peer, true)) {
        throw std::runtime_error("could not inject asymmetric partition");
      }
    }
  }
  const auto ambiguous = put(session, 2, "ambiguous-partition-write");
  (void)cluster.submit(first_leader, ambiguous);
  const detlog::NodeId replacement = wait_replacement(cluster, first_leader);
  result.replacement_leader = replacement;
  result.second_commit =
      commit(cluster, replacement, put(session, 2, "after-partition"))
          .log_index;
  if (symmetric) {
    (void)cluster.isolate(first_leader, false);
  } else {
    for (const detlog::NodeId peer : cluster.members()) {
      if (peer != first_leader) {
        (void)cluster.set_partition(first_leader, peer, false);
      }
    }
  }
  wait_value(cluster, first_leader, "after-partition");
  return result;
}

[[nodiscard]] ScenarioSummary run_ambiguous_retry(
    detlog::Cluster& cluster, detlog::NodeId leader,
    detlog::SessionId session) {
  ScenarioSummary result;
  result.first_leader = leader;
  const auto command = put(session, 1, "applied-once");
  const auto first = commit(cluster, leader, command);
  result.first_commit = first.log_index;
  (void)cluster.take_replies();  // The client times out after losing its reply.
  const auto before = cluster.snapshot(leader);
  const auto retry = commit(cluster, leader, command);
  const auto after = cluster.snapshot(leader);
  if (!before || !after || before->last_log_index != after->last_log_index ||
      retry.log_index != first.log_index) {
    throw std::runtime_error("ambiguous retry appended or applied twice");
  }
  result.second_commit = retry.log_index;
  return result;
}

[[nodiscard]] ScenarioSummary run_torn_wal(
    detlog::Cluster& cluster, detlog::NodeId leader,
    detlog::SessionId session) {
  ScenarioSummary result;
  result.first_leader = leader;
  result.first_commit =
      commit(cluster, leader, put(session, 1, "before-torn-tail")).log_index;
  (void)cluster.submit(leader, put(session, 2, "torn-unacknowledged"));
  if (!cluster.crash(leader, detlog::sim::StorageCrashSpec{0, 20}) ||
      !cluster.restart(leader)) {
    throw std::runtime_error("torn-WAL crash/restart failed");
  }
  const auto ready = cluster.run_until_leader(50'000, true);
  if (!ready) throw std::runtime_error("cluster did not recover from torn WAL");
  result.replacement_leader = *ready;
  result.second_commit =
      commit(cluster, *ready, put(session, 2, "after-torn-tail")).log_index;
  wait_value(cluster, leader, "after-torn-tail");
  return result;
}

[[nodiscard]] ScenarioSummary run_slow_follower(
    detlog::Cluster& cluster, detlog::NodeId leader,
    detlog::SessionId session) {
  detlog::NodeId slow{};
  for (const detlog::NodeId node : cluster.members()) {
    if (node != leader) slow = node;
  }
  if (slow == 0 || !cluster.set_link_delay(leader, slow, 25) ||
      !cluster.set_link_delay(slow, leader, 25)) {
    throw std::runtime_error("could not configure slow follower");
  }
  ScenarioSummary result;
  result.first_leader = leader;
  result.first_commit =
      commit(cluster, leader, put(session, 1, "with-slow-follower")).log_index;
  (void)cluster.run(250);
  (void)cluster.set_link_delay(leader, slow, 1);
  (void)cluster.set_link_delay(slow, leader, 1);
  wait_value(cluster, slow, "with-slow-follower");
  result.second_commit = result.first_commit;
  return result;
}

[[nodiscard]] ScenarioSummary run_slow_disk(
    detlog::Cluster& cluster, detlog::NodeId leader,
    detlog::SessionId session) {
  detlog::NodeId slow{};
  for (const detlog::NodeId node : cluster.members()) {
    if (node != leader) slow = node;
  }
  if (slow == 0 || !cluster.set_storage_latency(slow, 10, 25)) {
    throw std::runtime_error("could not configure per-node slow disk");
  }
  ScenarioSummary result;
  result.first_leader = leader;
  result.first_commit =
      commit(cluster, leader, put(session, 1, "with-slow-disk")).log_index;
  wait_value(cluster, slow, "with-slow-disk");
  result.second_commit = result.first_commit;
  return result;
}

[[nodiscard]] ScenarioSummary run_saturation(
    detlog::Cluster& cluster, detlog::NodeId leader,
    detlog::SessionId session) {
  if (!cluster.isolate(leader, true)) {
    throw std::runtime_error("could not isolate leader for saturation");
  }
  for (std::uint64_t index = 1; index <= 12; ++index) {
    (void)cluster.submit(
        leader, put({session.high, session.low + index}, 1,
                    "saturated-" + std::to_string(index)));
  }
  (void)cluster.run(2'000);
  const auto depth = cluster.queue_depth();
  if (depth.scheduled_events >
          cluster.config().simulator.events.max_pending_events ||
      !cluster.check_invariants()) {
    throw std::runtime_error("saturation exceeded a bound or invariant");
  }
  (void)cluster.isolate(leader, false);
  const auto ready = cluster.run_until_leader(50'000, true);
  if (!ready) throw std::runtime_error("cluster did not recover after saturation");
  ScenarioSummary result;
  result.first_leader = leader;
  result.replacement_leader = *ready;
  result.first_commit = cluster.snapshot(*ready)->commit_index;
  result.second_commit =
      commit(cluster, *ready, put(session, 1, "after-saturation")).log_index;
  return result;
}

}  // namespace

int main(int argc, char** argv) {
  try {
    std::uint64_t seed = 1;
    std::size_t node_count = 3;
    std::string scenario{"leader-crash"};
    std::optional<std::string> trace_path;
    for (int index = 1; index < argc; ++index) {
      const std::string_view argument{argv[index]};
      if (argument == "--help" || argument == "-h") {
        print_usage(std::cout);
        return 0;
      }
      if (index + 1 >= argc) {
        throw std::invalid_argument(std::string(argument) +
                                    " requires a value");
      }
      const std::string_view value{argv[++index]};
      if (argument == "--seed") {
        seed = parse_unsigned(value, argument);
      } else if (argument == "--nodes") {
        const auto parsed = parse_unsigned(value, argument);
        if (parsed != 3 && parsed != 5) {
          throw std::invalid_argument("--nodes must be 3 or 5");
        }
        node_count = static_cast<std::size_t>(parsed);
      } else if (argument == "--trace") {
        trace_path = std::string(value);
      } else if (argument == "--scenario") {
        scenario = value;
      } else {
        throw std::invalid_argument("unknown option: " +
                                    std::string(argument));
      }
    }

    const bool known_scenario =
        scenario == "leader-crash" || scenario == "symmetric-partition" ||
        scenario == "asymmetric-partition" ||
        scenario == "ambiguous-retry" || scenario == "torn-wal" ||
        scenario == "slow-follower" || scenario == "slow-disk" ||
        scenario == "saturation";
    if (!known_scenario) {
      throw std::invalid_argument("unknown --scenario value");
    }

    auto config = detlog::cluster_config(node_count, seed);
    if (scenario == "saturation") {
      config.raft.max_uncommitted_entries = 2;
      config.raft.max_uncommitted_bytes = 4096;
    }
    detlog::Cluster cluster(std::move(config));
    const auto first_leader = cluster.run_until_leader(50'000, true);
    if (!first_leader) {
      throw std::runtime_error("cluster did not elect a ready leader");
    }

    const detlog::SessionId session{0x4445544c4f47ULL, seed};
    ScenarioSummary summary;
    if (scenario == "leader-crash") {
      summary = run_leader_crash(cluster, *first_leader, session);
    } else if (scenario == "symmetric-partition") {
      summary = run_partition(cluster, *first_leader, session, true);
    } else if (scenario == "asymmetric-partition") {
      summary = run_partition(cluster, *first_leader, session, false);
    } else if (scenario == "ambiguous-retry") {
      summary = run_ambiguous_retry(cluster, *first_leader, session);
    } else if (scenario == "torn-wal") {
      summary = run_torn_wal(cluster, *first_leader, session);
    } else if (scenario == "slow-follower") {
      summary = run_slow_follower(cluster, *first_leader, session);
    } else if (scenario == "saturation") {
      summary = run_saturation(cluster, *first_leader, session);
    } else {
      summary = run_slow_disk(cluster, *first_leader, session);
    }

    const auto invariant = cluster.check_invariants();
    if (!invariant) {
      throw std::runtime_error("invariant failure: " + invariant.detail);
    }

    const std::string trace = cluster.trace_jsonl();
    if (trace_path) {
      std::ofstream output(*trace_path, std::ios::binary | std::ios::trunc);
      if (!output) {
        throw std::runtime_error("could not open trace output: " +
                                 *trace_path);
      }
      output.write(trace.data(), static_cast<std::streamsize>(trace.size()));
      if (!output) {
        throw std::runtime_error("could not write trace output: " +
                                 *trace_path);
      }
    }

    std::cout << "scenario=" << scenario << " seed=" << seed
              << " nodes=" << node_count
              << " first_leader=" << summary.first_leader
              << " replacement_leader=";
    if (summary.replacement_leader) {
      std::cout << *summary.replacement_leader;
    } else {
      std::cout << "none";
    }
    std::cout << " first_commit=" << summary.first_commit
              << " second_commit=" << summary.second_commit
              << " virtual_time=" << cluster.now()
              << " trace_records=" << cluster.trace().size() << '\n';
    if (trace_path) {
      std::cout << "trace=" << *trace_path << '\n';
    }
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "detlog-sim: " << error.what() << '\n';
    print_usage(std::cerr);
    return 1;
  }
}
