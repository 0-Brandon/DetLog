#include "detlog/node_host.hpp"

#include <algorithm>
#include <atomic>
#include <charconv>
#include <chrono>
#include <cctype>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <deque>
#include <filesystem>
#include <iostream>
#include <limits>
#include <map>
#include <mutex>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace {

struct MemberAddress {
  detlog::NodeId node{};
  std::string address;
  std::uint16_t port{};
};

struct Arguments {
  detlog::NodeId node{};
  std::vector<MemberAddress> members;
  std::filesystem::path wal_path;
  std::uint64_t cluster_high{};
  std::uint64_t cluster_low{};
  std::uint64_t seed{1};
  std::uint64_t incarnation{};
  std::uint64_t tick_ms{10};
  bool allow_non_loopback{};
};

[[nodiscard]] std::uint64_t parse_unsigned(std::string_view text,
                                           std::string_view option,
                                           int base = 10) {
  std::uint64_t value{};
  const auto [end, error] = std::from_chars(
      text.data(), text.data() + text.size(), value, base);
  if (error != std::errc{} || end != text.data() + text.size()) {
    throw std::invalid_argument(std::string(option) +
                                " has an invalid unsigned value");
  }
  return value;
}

[[nodiscard]] std::pair<std::uint64_t, std::uint64_t> parse_cluster(
    std::string_view text) {
  const std::size_t colon = text.find(':');
  std::string_view high;
  std::string_view low;
  if (colon != std::string_view::npos) {
    high = text.substr(0, colon);
    low = text.substr(colon + 1U);
  } else if (text.size() == 32) {
    high = text.substr(0, 16);
    low = text.substr(16);
  } else {
    throw std::invalid_argument(
        "--cluster requires 32 hex digits or HIGH:LOW");
  }
  if (high.empty() || low.empty() || high.size() > 16 || low.size() > 16) {
    throw std::invalid_argument("--cluster halves must contain 1-16 hex digits");
  }
  return {parse_unsigned(high, "--cluster", 16),
          parse_unsigned(low, "--cluster", 16)};
}

[[nodiscard]] std::vector<MemberAddress> parse_members(
    std::string_view specification) {
  std::vector<MemberAddress> members;
  std::size_t begin = 0;
  while (begin <= specification.size()) {
    const std::size_t comma = specification.find(',', begin);
    const std::string_view item = specification.substr(
        begin, comma == std::string_view::npos ? specification.size() - begin
                                                : comma - begin);
    const std::size_t at = item.find('@');
    const std::size_t colon = item.rfind(':');
    if (at == std::string_view::npos || colon == std::string_view::npos ||
        at == 0 || colon <= at + 1U || colon + 1U >= item.size()) {
      throw std::invalid_argument(
          "--members entries must be ID@IPv4:PORT");
    }
    const std::uint64_t node =
        parse_unsigned(item.substr(0, at), "--members");
    const std::uint64_t port =
        parse_unsigned(item.substr(colon + 1U), "--members");
    if (node == 0 || node > std::numeric_limits<detlog::NodeId>::max() ||
        port == 0 || port > std::numeric_limits<std::uint16_t>::max()) {
      throw std::invalid_argument("--members ID or port is out of range");
    }
    members.push_back(MemberAddress{
        static_cast<detlog::NodeId>(node),
        std::string(item.substr(at + 1U, colon - at - 1U)),
        static_cast<std::uint16_t>(port)});
    if (comma == std::string_view::npos) break;
    begin = comma + 1U;
  }
  std::sort(members.begin(), members.end(),
            [](const MemberAddress& lhs, const MemberAddress& rhs) {
              return lhs.node < rhs.node;
            });
  if ((members.size() != 3 && members.size() != 5) ||
      std::adjacent_find(
          members.begin(), members.end(),
          [](const MemberAddress& lhs, const MemberAddress& rhs) {
            return lhs.node == rhs.node;
          }) != members.end()) {
    throw std::invalid_argument("--members requires 3 or 5 unique nodes");
  }
  return members;
}

void print_usage(std::ostream& output) {
  output
      << "usage: detlog-node --id N --members "
         "1@127.0.0.1:7101,2@127.0.0.1:7102,3@127.0.0.1:7103 "
         "--wal FILE --cluster HIGH:LOW [--seed N] [--incarnation N] "
         "[--tick-ms N] [--allow-non-loopback]\n";
}

[[nodiscard]] Arguments parse_arguments(int argc, char** argv) {
  Arguments result;
  bool has_node = false;
  bool has_members = false;
  bool has_wal = false;
  bool has_cluster = false;
  for (int index = 1; index < argc; ++index) {
    const std::string_view argument{argv[index]};
    if (argument == "--help" || argument == "-h") {
      print_usage(std::cout);
      std::exit(0);
    }
    if (argument == "--allow-non-loopback") {
      result.allow_non_loopback = true;
      continue;
    }
    if (index + 1 >= argc) {
      throw std::invalid_argument(std::string(argument) + " requires a value");
    }
    const std::string_view value{argv[++index]};
    if (argument == "--id") {
      const std::uint64_t parsed = parse_unsigned(value, argument);
      if (parsed == 0 ||
          parsed > std::numeric_limits<detlog::NodeId>::max()) {
        throw std::invalid_argument("--id is out of range");
      }
      result.node = static_cast<detlog::NodeId>(parsed);
      has_node = true;
    } else if (argument == "--members") {
      result.members = parse_members(value);
      has_members = true;
    } else if (argument == "--wal") {
      result.wal_path = std::string(value);
      has_wal = true;
    } else if (argument == "--cluster") {
      const auto [high, low] = parse_cluster(value);
      result.cluster_high = high;
      result.cluster_low = low;
      has_cluster = true;
    } else if (argument == "--seed") {
      result.seed = parse_unsigned(value, argument);
    } else if (argument == "--incarnation") {
      result.incarnation = parse_unsigned(value, argument);
    } else if (argument == "--tick-ms") {
      result.tick_ms = parse_unsigned(value, argument);
      if (result.tick_ms == 0 ||
          result.tick_ms >
              static_cast<std::uint64_t>(
                  std::numeric_limits<std::int64_t>::max())) {
        throw std::invalid_argument("--tick-ms is out of range");
      }
    } else {
      throw std::invalid_argument("unknown option: " + std::string(argument));
    }
  }
  if (!has_node || !has_members || !has_wal || !has_cluster) {
    throw std::invalid_argument("--id, --members, --wal, and --cluster are required");
  }
  if (std::none_of(result.members.begin(), result.members.end(),
                   [&](const MemberAddress& member) {
                     return member.node == result.node;
                   })) {
    throw std::invalid_argument("--id is not present in --members");
  }
  return result;
}

[[nodiscard]] detlog::NodeHostConfig host_config(const Arguments& arguments) {
  detlog::NodeHostConfig config;
  config.raft.node_id = arguments.node;
  for (const MemberAddress& member : arguments.members) {
    config.raft.members.push_back(member.node);
  }
  config.identity = {arguments.cluster_high, arguments.cluster_low,
                     arguments.node};
  config.wal_path = arguments.wal_path;
  config.tcp.identity = {arguments.cluster_high, arguments.cluster_low,
                         arguments.node};
  config.tcp.incarnation = arguments.incarnation;
  config.tcp.allow_non_loopback = arguments.allow_non_loopback;
  for (const MemberAddress& member : arguments.members) {
    if (member.node == arguments.node) {
      config.tcp.listen_address = member.address;
      config.tcp.listen_port = member.port;
    } else {
      config.tcp.peers.push_back(
          detlog::TcpPeerEndpoint{member.node, member.address, member.port});
    }
  }
  config.tick_duration =
      std::chrono::milliseconds(static_cast<std::int64_t>(arguments.tick_ms));
  config.timer_seed = arguments.seed;
  return config;
}

[[nodiscard]] std::string_view client_status(detlog::ClientStatus status) {
  using detlog::ClientStatus;
  switch (status) {
    case ClientStatus::ok:
      return "ok";
    case ClientStatus::not_leader:
      return "not_leader";
    case ClientStatus::leader_not_ready:
      return "leader_not_ready";
    case ClientStatus::busy:
      return "busy";
    case ClientStatus::unavailable:
      return "unavailable";
    case ClientStatus::stale_sequence:
      return "stale_sequence";
    case ClientStatus::sequence_gap:
      return "sequence_gap";
    case ClientStatus::request_id_conflict:
      return "request_id_conflict";
    case ClientStatus::invalid_request:
      return "invalid_request";
    case ClientStatus::storage_error:
      return "storage_error";
  }
  return "unknown";
}

void print_status(const detlog::NodeHostStatus& status) {
  std::cout << "node=" << status.node << " role=";
  switch (status.role) {
    case detlog::RaftRole::follower:
      std::cout << "follower";
      break;
    case detlog::RaftRole::candidate:
      std::cout << "candidate";
      break;
    case detlog::RaftRole::leader:
      std::cout << "leader";
      break;
    case detlog::RaftRole::unavailable:
      std::cout << "unavailable";
      break;
  }
  std::cout << " term=" << status.term << " ready=" << status.leader_ready
            << " commit=" << status.commit_index
            << " applied=" << status.last_applied;
  if (status.leader_hint) std::cout << " leader=" << *status.leader_hint;
  std::cout << '\n';
}

[[nodiscard]] std::string trim_left(std::string value) {
  value.erase(value.begin(),
              std::find_if(value.begin(), value.end(), [](char character) {
                return !std::isspace(static_cast<unsigned char>(character));
              }));
  return value;
}

}  // namespace

int main(int argc, char** argv) {
  try {
    const Arguments arguments = parse_arguments(argc, argv);
    detlog::NodeHost host(host_config(arguments));
    host.start();
    std::cout << "detlog-node " << arguments.node << " listening on port "
              << host.listening_port()
              << "; commands: put KEY VALUE | get KEY | erase KEY | retry | "
                 "status | metrics | quit\n";

    constexpr std::size_t kInputQueueLimit = 64;
    std::mutex input_mutex;
    std::deque<std::string> input_lines;
    std::atomic<bool> input_done{false};
    std::atomic<std::size_t> input_drops{0};
    std::jthread input_thread([&] {
      std::string line;
      while (std::getline(std::cin, line)) {
        const bool quitting = line == "quit" || line == "exit";
        {
          std::lock_guard lock(input_mutex);
          if (input_lines.size() < kInputQueueLimit) {
            input_lines.push_back(std::move(line));
          } else {
            input_drops.fetch_add(1, std::memory_order_relaxed);
          }
        }
        if (quitting) break;
      }
      input_done.store(true, std::memory_order_release);
    });

    std::optional<detlog::ClientCommand> last_command;
    std::uint64_t next_request_id = 1;
    const std::uint64_t session_epoch =
        static_cast<std::uint64_t>(
            std::chrono::system_clock::now().time_since_epoch().count()) ^
        (static_cast<std::uint64_t>(arguments.node) << 32U) ^
        arguments.incarnation;
    bool quit = false;
    while (!quit) {
      bool activity = false;
      for (int iteration = 0; iteration < 16; ++iteration) {
        activity = host.poll_once() || activity;
      }
      for (const detlog::ClientReply& reply : host.poll_replies()) {
        std::cout << "reply token=" << reply.token
                  << " status=" << client_status(reply.status)
                  << " index=" << reply.log_index;
        if (!reply.value.empty()) std::cout << " value=" << reply.value;
        if (reply.leader_hint) std::cout << " leader=" << *reply.leader_hint;
        std::cout << '\n';
      }

      std::optional<std::string> line;
      {
        std::lock_guard lock(input_mutex);
        if (!input_lines.empty()) {
          line = std::move(input_lines.front());
          input_lines.pop_front();
        }
      }
      const std::size_t dropped =
          input_drops.exchange(0, std::memory_order_relaxed);
      if (dropped != 0) {
        std::cerr << "dropped " << dropped << " input commands: queue full\n";
      }
      if (!line) {
        if (input_done.load(std::memory_order_acquire)) break;
        if (!activity) std::this_thread::sleep_for(std::chrono::milliseconds(1));
        continue;
      }

      std::istringstream input(*line);
      std::string operation;
      input >> operation;
      if (operation == "quit" || operation == "exit") {
        quit = true;
      } else if (operation == "status") {
        print_status(host.status());
      } else if (operation == "metrics") {
        const auto metrics = host.metrics();
        std::cout << "tcp_sent=" << metrics.tcp_messages_queued
                  << " tcp_received=" << metrics.tcp_messages_received
                  << " tcp_backpressure=" << metrics.tcp_backpressure_drops
                  << " tcp_down=" << metrics.tcp_down_drops
                  << " storage_errors=" << metrics.storage_errors << '\n';
      } else if (operation == "retry") {
        if (!last_command) {
          std::cerr << "no command is available to retry\n";
          continue;
        }
        const auto submitted = host.submit(*last_command);
        std::cout << "retry status="
                  << static_cast<unsigned>(submitted.status)
                  << " token=" << submitted.token << '\n';
      } else if (operation == "put" || operation == "get" ||
                 operation == "erase") {
        std::string key;
        input >> key;
        if (key.empty()) {
          std::cerr << operation << " requires KEY";
          if (operation == "put") std::cerr << " VALUE";
          std::cerr << '\n';
          continue;
        }
        std::string value;
        if (operation == "put") {
          std::getline(input, value);
          value = trim_left(std::move(value));
          if (value.empty()) {
            std::cerr << "put requires a nonempty VALUE\n";
            continue;
          }
        }
        const detlog::CommandKind kind =
            operation == "put"    ? detlog::CommandKind::put
            : operation == "get"  ? detlog::CommandKind::get
                                    : detlog::CommandKind::erase;
        detlog::ClientCommand command{
            detlog::SessionId{session_epoch,
                              next_request_id++},
            1, kind, std::move(key), std::move(value)};
        const auto submitted = host.submit(command);
        if (submitted.status == detlog::NodeHostSubmitStatus::accepted) {
          last_command = std::move(command);
          std::cout << "submitted token=" << submitted.token << '\n';
        } else {
          std::cerr << "submit rejected status="
                    << static_cast<unsigned>(submitted.status) << '\n';
        }
      } else if (!operation.empty()) {
        std::cerr << "unknown command: " << operation << '\n';
      }
    }

    host.stop();
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "detlog-node: " << error.what() << '\n';
    print_usage(std::cerr);
    return 1;
  }
}
