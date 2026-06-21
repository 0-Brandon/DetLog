#include "detlog/cluster.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <optional>
#include <span>
#include <string>
#include <vector>

namespace {

constexpr std::size_t kMaximumInputBytes = 256;
constexpr std::size_t kMaximumActions = 32;

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
  ActionKind kind{};
  std::uint8_t first{};
  std::uint8_t second{};
  std::uint16_t amount{};
  bool enabled{};
};

[[noreturn]] void fail_fuzz_case() { std::abort(); }

[[nodiscard]] std::uint64_t hash_input(std::span<const std::uint8_t> bytes) {
  std::uint64_t hash = 14695981039346656037ULL;
  for (const auto byte : bytes) {
    hash ^= byte;
    hash *= 1099511628211ULL;
  }
  return hash;
}

[[nodiscard]] std::vector<Action> decode_actions(
    std::span<const std::uint8_t> bytes) {
  std::vector<Action> actions;
  if (bytes.size() <= 1) {
    return actions;
  }
  const std::size_t available = bytes.size() - 1U;
  const std::size_t count = std::min(kMaximumActions, available / 5U);
  actions.reserve(count);
  for (std::size_t index = 0; index < count; ++index) {
    const std::size_t offset = 1U + index * 5U;
    const std::uint16_t amount = static_cast<std::uint16_t>(
        (static_cast<std::uint16_t>(bytes[offset + 3U]) << 8U) |
        bytes[offset + 4U]);
    actions.push_back(Action{
        static_cast<ActionKind>(bytes[offset] % 14U),
        bytes[offset + 1U],
        bytes[offset + 2U],
        amount,
        (bytes[offset + 4U] & 0x80U) != 0,
    });
  }
  return actions;
}

class FuzzScenario {
 public:
  FuzzScenario(std::uint64_t seed, std::size_t node_count)
      : seed_(seed), cluster_(configuration(seed, node_count)) {}

  void apply(const Action& action) {
    const auto first = node(action.first);
    auto second = node(action.second);
    if (first == second) {
      const auto& members = cluster_.members();
      const auto position = std::find(members.begin(), members.end(), first);
      const auto offset =
          static_cast<std::size_t>(position - members.begin());
      second = members[(offset + 1U) % members.size()];
    }
    switch (action.kind) {
      case ActionKind::step_burst:
        drive(1U + action.amount % 4U);
        break;
      case ActionKind::client_put:
        submit(action.first, second, action.amount);
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
        (void)cluster_.set_link_delay(first, second, action.amount % 7U);
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
    check();
  }

  void recover_and_prove_progress() {
    for (const auto member : cluster_.members()) {
      const auto snapshot = cluster_.snapshot(member);
      if (snapshot && snapshot->alive) {
        (void)cluster_.crash(member);
        check();
      }
    }
    for (const auto member : cluster_.members()) {
      const auto snapshot = cluster_.snapshot(member);
      if (snapshot && !snapshot->alive && !cluster_.restart(member)) {
        fail_fuzz_case();
      }
      check();
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
    check();

    const detlog::ClientCommand command{
        {0x46555a5a5343454eULL, seed_}, 1, detlog::CommandKind::put,
        "fuzz-progress", "ok"};
    for (std::size_t attempt = 0; attempt < 4; ++attempt) {
      const auto leader = wait_for_leader(4'000);
      if (!leader) {
        continue;
      }
      const auto token = cluster_.submit(*leader, command);
      for (std::size_t event = 0; event < 4'000; ++event) {
        if (const auto reply = cluster_.reply_for(token)) {
          if (reply->status == detlog::ClientStatus::ok &&
              cluster_.command_is_committed(command.session,
                                            command.sequence)) {
            check();
            return;
          }
          break;
        }
        if (!step()) {
          break;
        }
      }
    }
    fail_fuzz_case();
  }

 private:
  [[nodiscard]] static detlog::ClusterConfig configuration(
      std::uint64_t seed, std::size_t node_count) {
    auto config = detlog::cluster_config(node_count, seed);
    config.max_semantic_trace_records = 20'000;
    config.max_retained_client_replies = 256;
    config.simulator.max_trace_records = 20'000;
    config.simulator.events.max_pending_events = 10'000;
    config.simulator.events.max_total_events = 100'000;
    return config;
  }

  [[nodiscard]] detlog::NodeId node(std::uint8_t selector) const {
    const auto& members = cluster_.members();
    return members[static_cast<std::size_t>(selector) % members.size()];
  }

  void submit(std::uint8_t selector, detlog::NodeId target,
              std::uint16_t value) {
    const std::size_t slot = selector % commands_.size();
    detlog::ClientCommand command{
        {seed_ ^ 0x46555a5a434c4e54ULL,
         static_cast<std::uint64_t>(slot + 1U)},
        ++sequences_[slot],
        detlog::CommandKind::put,
        "fuzz-key-" + std::to_string(slot),
        "v-" + std::to_string(value)};
    commands_[slot] = command;
    (void)cluster_.submit(target, command);
  }

  void retry(std::uint8_t selector, detlog::NodeId target) {
    const std::size_t slot = selector % commands_.size();
    if (commands_[slot]) {
      (void)cluster_.submit(target, *commands_[slot]);
    }
  }

  void check() {
    if (!cluster_.check_invariants()) {
      fail_fuzz_case();
    }
  }

  [[nodiscard]] bool step() {
    const bool progressed = cluster_.step();
    check();
    return progressed;
  }

  void drive(std::size_t count) {
    for (std::size_t index = 0; index < count; ++index) {
      if (!step()) {
        break;
      }
    }
  }

  [[nodiscard]] std::optional<detlog::NodeId> wait_for_leader(
      std::size_t max_events) {
    for (std::size_t event = 0; event < max_events; ++event) {
      if (const auto leader = cluster_.leader(true)) {
        return leader;
      }
      if (!step()) {
        break;
      }
    }
    return cluster_.leader(true);
  }

  std::uint64_t seed_{};
  detlog::Cluster cluster_;
  std::array<std::uint64_t, 4> sequences_{};
  std::array<std::optional<detlog::ClientCommand>, 4> commands_{};
};

void run_input(std::span<const std::uint8_t> input) {
  const auto bounded = input.first(std::min(input.size(), kMaximumInputBytes));
  const std::uint64_t seed = hash_input(bounded);
  const std::size_t nodes = !bounded.empty() && (bounded.front() & 1U) != 0
                                ? 5U
                                : 3U;
  FuzzScenario scenario(seed, nodes);
  for (const auto& action : decode_actions(bounded)) {
    scenario.apply(action);
  }
  scenario.recover_and_prove_progress();
}

}  // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data,
                                       std::size_t size) {
  try {
    run_input(std::span<const std::uint8_t>{data, size});
  } catch (...) {
    fail_fuzz_case();
  }
  return 0;
}

#ifdef DETLOG_SCENARIO_FUZZ_STANDALONE
int main() {
  const std::array<std::uint8_t, 1> emptyish{0};
  const std::array<std::uint8_t, 26> faults{
      1, 1, 0, 1, 0, 1, 4, 1, 2, 0, 2, 9, 2,
      1, 0, 3, 12, 1, 0, 5, 0, 13, 2, 0, 1, 0};
  (void)LLVMFuzzerTestOneInput(emptyish.data(), emptyish.size());
  (void)LLVMFuzzerTestOneInput(faults.data(), faults.size());
  return 0;
}
#endif
