#include "detlog/cluster.hpp"

#include <algorithm>
#include <array>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace {

enum class ActionKind : std::uint8_t {
  step_burst,
  client_put,
  client_retry,
  directed_partition,
  symmetric_partition,
  drop_next,
  duplicate_next,
  link_delay,
  crash_intact,
  crash_torn,
  restart,
  fail_write,
  short_write,
  fail_flush,
};

struct Action {
  ActionKind kind{ActionKind::step_burst};
  std::uint8_t first{};
  std::uint8_t second{};
  std::uint16_t amount{};
  bool enabled{};
};

struct ScenarioResult {
  bool ok{true};
  std::string error;
};

[[nodiscard]] std::string_view action_name(ActionKind kind) {
  switch (kind) {
    case ActionKind::step_burst:
      return "step";
    case ActionKind::client_put:
      return "put";
    case ActionKind::client_retry:
      return "retry";
    case ActionKind::directed_partition:
      return "partition1";
    case ActionKind::symmetric_partition:
      return "partition2";
    case ActionKind::drop_next:
      return "drop";
    case ActionKind::duplicate_next:
      return "duplicate";
    case ActionKind::link_delay:
      return "delay";
    case ActionKind::crash_intact:
      return "crash_intact";
    case ActionKind::crash_torn:
      return "crash_torn";
    case ActionKind::restart:
      return "restart";
    case ActionKind::fail_write:
      return "fail_write";
    case ActionKind::short_write:
      return "short_write";
    case ActionKind::fail_flush:
      return "fail_flush";
  }
  return "unknown";
}

[[nodiscard]] std::string serialize_actions(
    const std::vector<Action>& actions) {
  std::string result;
  for (std::size_t index = 0; index < actions.size(); ++index) {
    if (index != 0) {
      result.push_back(';');
    }
    const auto& action = actions[index];
    result.append(action_name(action.kind));
    result.push_back('(');
    result.append(std::to_string(action.first));
    result.push_back(',');
    result.append(std::to_string(action.second));
    result.push_back(',');
    result.append(std::to_string(action.amount));
    result.push_back(',');
    result.push_back(action.enabled ? '1' : '0');
    result.push_back(')');
  }
  return result;
}

[[nodiscard]] std::vector<Action> generate_actions(std::uint64_t seed,
                                                   std::size_t count) {
  detlog::sim::Pcg32 random(seed, 0x50524f5045525459ULL);
  std::vector<Action> actions;
  actions.reserve(count);
  constexpr std::uint64_t kKinds = 14;
  for (std::size_t index = 0; index < count; ++index) {
    actions.push_back(Action{
        static_cast<ActionKind>(random.bounded(kKinds)),
        static_cast<std::uint8_t>(random.bounded(5)),
        static_cast<std::uint8_t>(random.bounded(5)),
        static_cast<std::uint16_t>(random.bounded(32)),
        random.chance(1, 2),
    });
  }
  return actions;
}

class Scenario {
 public:
  Scenario(std::uint64_t seed, std::size_t node_count)
      : seed_(seed), cluster_(detlog::cluster_config(node_count, seed)) {}

  void apply(const Action& action) {
    const detlog::NodeId first = node(action.first);
    detlog::NodeId second = node(action.second);
    if (second == first) {
      const auto& members = cluster_.members();
      const auto position = std::find(members.begin(), members.end(), first);
      const std::size_t offset =
          static_cast<std::size_t>(position - members.begin());
      second = members[(offset + 1U) % members.size()];
    }

    switch (action.kind) {
      case ActionKind::step_burst:
        drive(1U + action.amount % 8U);
        break;
      case ActionKind::client_put:
        submit_new(action.first, second, action.amount);
        break;
      case ActionKind::client_retry:
        retry(action.first, second);
        break;
      case ActionKind::directed_partition:
        (void)cluster_.set_partition(first, second, action.enabled);
        break;
      case ActionKind::symmetric_partition:
        (void)cluster_.set_bidirectional_partition(first, second,
                                                   action.enabled);
        break;
      case ActionKind::drop_next:
        (void)cluster_.drop_next(first, second,
                                 1U + action.amount % 3U);
        break;
      case ActionKind::duplicate_next:
        (void)cluster_.duplicate_next(first, second, action.amount % 4U);
        break;
      case ActionKind::link_delay:
        (void)cluster_.set_link_delay(first, second, action.amount % 9U);
        break;
      case ActionKind::crash_intact:
        (void)cluster_.crash(
            first,
            detlog::sim::StorageCrashSpec{
                std::numeric_limits<std::size_t>::max(),
                std::numeric_limits<std::size_t>::max()});
        break;
      case ActionKind::crash_torn:
        (void)cluster_.crash(
            first, detlog::sim::StorageCrashSpec{
                       action.amount % 8U, 1U + action.amount % 24U});
        break;
      case ActionKind::restart:
        (void)cluster_.restart(first);
        break;
      case ActionKind::fail_write:
        (void)cluster_.fail_next_storage_write(first);
        break;
      case ActionKind::short_write:
        (void)cluster_.short_write_next(first, 1U + action.amount % 24U);
        break;
      case ActionKind::fail_flush:
        (void)cluster_.fail_next_storage_flush(first);
        break;
    }
    require_invariants("after action");
  }

  void settle_and_commit() {
    // A final full process cycle clears unused one-shot storage injections and
    // forces every incomplete durable tail through production recovery.
    for (const auto member : cluster_.members()) {
      const auto snapshot = cluster_.snapshot(member);
      if (snapshot && snapshot->alive) {
        (void)cluster_.crash(member);
        require_invariants("while stopping nodes for final recovery");
      }
    }
    for (const auto member : cluster_.members()) {
      const auto snapshot = cluster_.snapshot(member);
      if (snapshot && !snapshot->alive && !cluster_.restart(member)) {
        throw std::runtime_error("a node failed deterministic WAL recovery");
      }
      require_invariants("while restarting nodes for final recovery");
    }
    for (const auto from : cluster_.members()) {
      for (const auto to : cluster_.members()) {
        if (from == to) {
          continue;
        }
        (void)cluster_.set_partition(from, to, false);
        (void)cluster_.set_link_delay(from, to, 1);
        (void)cluster_.drop_next(from, to, 0);
        (void)cluster_.duplicate_next(from, to, 0);
      }
    }
    require_invariants("after healing all links");

    const detlog::ClientCommand command{
        {0x50524f5045525459ULL, seed_}, 1, detlog::CommandKind::put,
        "post-heal", "committed"};
    for (std::size_t attempt = 0; attempt < 5; ++attempt) {
      const auto leader = wait_for_ready_leader(20'000);
      if (!leader) {
        continue;
      }
      const auto token = cluster_.submit(*leader, command);
      for (std::size_t event = 0; event < 10'000; ++event) {
        if (const auto reply = cluster_.reply_for(token)) {
          if (reply->status == detlog::ClientStatus::ok &&
              cluster_.command_is_committed(command.session,
                                            command.sequence)) {
            require_invariants("after post-heal commit");
            return;
          }
          break;
        }
        if (!step()) {
          break;
        }
      }
    }
    throw std::runtime_error(
        "healed live cluster did not resume successful commits");
  }

 private:
  [[nodiscard]] detlog::NodeId node(std::uint8_t selector) const {
    const auto& members = cluster_.members();
    return members[static_cast<std::size_t>(selector) % members.size()];
  }

  void submit_new(std::uint8_t client_selector, detlog::NodeId target,
                  std::uint16_t value) {
    const std::size_t slot = client_selector % clients_.size();
    const std::uint64_t sequence = ++next_sequences_[slot];
    detlog::ClientCommand command{
        {seed_ ^ 0x434c49454e540000ULL,
         static_cast<std::uint64_t>(slot + 1U)},
        sequence,
        detlog::CommandKind::put,
        "key-" + std::to_string(slot),
        "value-" + std::to_string(value)};
    clients_[slot] = command;
    (void)cluster_.submit(target, command);
  }

  void retry(std::uint8_t client_selector, detlog::NodeId target) {
    const std::size_t slot = client_selector % clients_.size();
    if (clients_[slot]) {
      (void)cluster_.submit(target, *clients_[slot]);
    }
  }

  [[nodiscard]] bool step() {
    const bool progressed = cluster_.step();
    require_invariants("after simulator event");
    return progressed;
  }

  void drive(std::size_t events) {
    for (std::size_t index = 0; index < events; ++index) {
      if (!step()) {
        break;
      }
    }
  }

  [[nodiscard]] std::optional<detlog::NodeId> wait_for_ready_leader(
      std::size_t max_events) {
    for (std::size_t index = 0; index < max_events; ++index) {
      if (const auto leader = cluster_.leader(true)) {
        return leader;
      }
      if (!step()) {
        break;
      }
    }
    return cluster_.leader(true);
  }

  void require_invariants(std::string_view where) {
    const auto result = cluster_.check_invariants();
    if (!result) {
      throw std::runtime_error(std::string(where) + ": " + result.detail);
    }
  }

  std::uint64_t seed_{};
  detlog::Cluster cluster_;
  std::array<std::uint64_t, 4> next_sequences_{};
  std::array<std::optional<detlog::ClientCommand>, 4> clients_{};
};

[[nodiscard]] ScenarioResult run_scenario(
    std::uint64_t seed, std::size_t node_count,
    const std::vector<Action>& actions) {
  try {
    Scenario scenario(seed, node_count);
    for (const auto& action : actions) {
      scenario.apply(action);
    }
    scenario.settle_and_commit();
    return {};
  } catch (const std::exception& error) {
    return {false, error.what()};
  } catch (...) {
    return {false, "non-standard exception"};
  }
}

[[nodiscard]] std::vector<Action> minimize_failure(
    std::uint64_t seed, std::size_t node_count,
    std::vector<Action> current) {
  std::size_t partitions = 2;
  while (current.size() >= 2) {
    const std::size_t chunk =
        (current.size() + partitions - 1U) / partitions;
    bool reduced = false;
    for (std::size_t begin = 0; begin < current.size(); begin += chunk) {
      const std::size_t end = std::min(current.size(), begin + chunk);
      std::vector<Action> candidate;
      candidate.reserve(current.size() - (end - begin));
      candidate.insert(candidate.end(), current.begin(),
                       current.begin() + static_cast<std::ptrdiff_t>(begin));
      candidate.insert(candidate.end(),
                       current.begin() + static_cast<std::ptrdiff_t>(end),
                       current.end());
      if (!run_scenario(seed, node_count, candidate).ok) {
        current = std::move(candidate);
        partitions = std::max<std::size_t>(2, partitions - 1U);
        reduced = true;
        break;
      }
    }
    if (reduced) {
      continue;
    }
    if (partitions >= current.size()) {
      break;
    }
    partitions = std::min(current.size(), partitions * 2U);
  }
  return current;
}

[[nodiscard]] std::size_t configured_seed_count() {
  constexpr std::size_t kDefaultSeeds = 50;
  constexpr std::size_t kMaximumSeeds = 10'000;
  const char* value = std::getenv("DETLOG_PROPERTY_SEEDS");
  if (value == nullptr || *value == '\0') {
    return kDefaultSeeds;
  }
  std::size_t parsed{};
  const std::string_view text{value};
  const auto [end, error] =
      std::from_chars(text.data(), text.data() + text.size(), parsed);
  if (error != std::errc{} || end != text.data() + text.size() || parsed == 0 ||
      parsed > kMaximumSeeds) {
    throw std::invalid_argument(
        "DETLOG_PROPERTY_SEEDS must be in the range 1..10000");
  }
  return parsed;
}

}  // namespace

int main() {
  try {
    const std::size_t seed_count = configured_seed_count();
    constexpr std::size_t kActionsPerSeed = 48;
    for (std::size_t offset = 0; offset < seed_count; ++offset) {
      const std::uint64_t seed = static_cast<std::uint64_t>(offset + 1U);
      const std::size_t node_count = (seed & 1U) == 0 ? 5U : 3U;
      const auto actions = generate_actions(seed, kActionsPerSeed);
      const auto result = run_scenario(seed, node_count, actions);
      if (!result.ok) {
        std::cerr << "property failure: seed=" << seed
                  << " nodes=" << node_count << " error=" << result.error
                  << "\nactions=" << serialize_actions(actions) << '\n';
        const auto minimized = minimize_failure(seed, node_count, actions);
        const auto minimized_result =
            run_scenario(seed, node_count, minimized);
        std::cerr << "minimized_actions=" << serialize_actions(minimized)
                  << "\nminimized_error=" << minimized_result.error << '\n';
        return 1;
      }
    }
    std::cout << "property campaign passed: seeds=" << seed_count
              << " actions_per_seed=" << kActionsPerSeed << '\n';
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "property harness error: " << error.what() << '\n';
    return 1;
  }
}
