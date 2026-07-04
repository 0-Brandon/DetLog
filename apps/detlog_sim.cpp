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
  output << "usage: detlog-sim [--seed N] [--nodes 3|5] [--trace FILE]\n";
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

}  // namespace

int main(int argc, char** argv) {
  try {
    std::uint64_t seed = 1;
    std::size_t node_count = 3;
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
      } else {
        throw std::invalid_argument("unknown option: " +
                                    std::string(argument));
      }
    }

    detlog::Cluster cluster(detlog::cluster_config(node_count, seed));
    const auto first_leader = cluster.run_until_leader(50'000, true);
    if (!first_leader) {
      throw std::runtime_error("cluster did not elect a ready leader");
    }

    const detlog::SessionId session{0x4445544c4f47ULL, seed};
    const auto first = commit(cluster, *first_leader,
                              put(session, 1, "before-crash"));
    if (!cluster.crash(*first_leader)) {
      throw std::runtime_error("could not crash the first leader");
    }
    const auto replacement = cluster.run_until_leader(50'000, true);
    if (!replacement || *replacement == *first_leader) {
      throw std::runtime_error("cluster did not elect a replacement leader");
    }
    const auto second = commit(cluster, *replacement,
                               put(session, 2, "after-crash"));
    if (!cluster.restart(*first_leader)) {
      throw std::runtime_error("could not restart the old leader");
    }
    const bool caught_up = cluster.run_until(
        [old_leader = *first_leader](const detlog::Cluster& running) {
          return running.value(old_leader, "demo-key") ==
                 std::optional<std::string>{"after-crash"};
        },
        50'000);
    if (!caught_up) {
      throw std::runtime_error(
          "restarted former leader did not catch up within the event bound");
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

    std::cout << "seed=" << seed << " nodes=" << node_count
              << " first_leader=" << *first_leader
              << " replacement_leader=" << *replacement
              << " first_commit=" << first.log_index
              << " second_commit=" << second.log_index
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
