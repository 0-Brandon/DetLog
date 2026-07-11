#include "detlog/cluster.hpp"
#include "detlog/node_host.hpp"

#include <algorithm>
#include <array>
#include <charconv>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#include <psapi.h>
#elif defined(__unix__) || defined(__APPLE__)
#include <sys/resource.h>
#endif

namespace {

using Clock = std::chrono::steady_clock;

struct Options {
  std::string mode{"sim"};
  std::string scenario{"healthy"};
  std::size_t nodes{3};
  std::size_t clients{1};
  std::size_t payload{64};
  std::size_t operations{100};
  std::uint64_t trial{1};
  std::uint64_t seed{1};
};

struct OperationRecord {
  std::uint64_t operation{};
  std::size_t client{};
  std::uint64_t sequence{};
  std::uint64_t start{};
  std::uint64_t end{};
  std::uint64_t latency{};
  std::uint64_t attempts{};
  std::uint64_t retries{};
  std::uint64_t queue_rejections{};
  std::string status;
};

struct RunSummary {
  std::size_t successes{};
  std::size_t failures{};
  std::uint64_t retries{};
  std::uint64_t queue_rejections{};
  std::uint64_t elections{};
  std::uint64_t duration{};
  std::uint64_t wall_duration_ns{};
  std::vector<std::uint64_t> successful_latencies;
  std::optional<std::uint64_t> fault_at;
  std::optional<std::uint64_t> replacement_leader_ready_duration;
  std::optional<std::uint64_t> recovery_to_first_success;
  std::optional<std::uint64_t> process_cpu_ns;
  std::optional<std::uint64_t> peak_resident_bytes;
  std::string status{"complete"};
  std::string safety_check{"not_applicable"};
  std::uint64_t transport_backpressure{};
  std::uint64_t transport_down_drops{};
  std::uint64_t storage_errors{};
  std::size_t owner_queue_high_water{};
  std::size_t client_queue_high_water{};
  std::size_t sim_event_queue_high_water{};
  std::size_t sim_network_bytes_high_water{};
  std::size_t sim_storage_bytes_high_water{};
};

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
  output
      << "usage: detlog-bench [--mode sim|tcp] [--nodes 3|5] "
         "[--clients 1|3|5] [--payload 64|1024|16384] [--operations N] "
         "[--trial N] [--seed N] "
         "[--scenario healthy|leader-crash|partition|slow-follower|slow-fsync]\n";
}

[[nodiscard]] Options parse_options(int argc, char** argv) {
  Options options;
  for (int index = 1; index < argc; ++index) {
    const std::string_view argument{argv[index]};
    if (argument == "--help" || argument == "-h") {
      print_usage(std::cout);
      std::exit(0);
    }
    if (index + 1 >= argc) {
      throw std::invalid_argument(std::string(argument) + " requires a value");
    }
    const std::string_view value{argv[++index]};
    if (argument == "--mode") {
      options.mode = value;
    } else if (argument == "--nodes") {
      options.nodes = static_cast<std::size_t>(parse_unsigned(value, argument));
    } else if (argument == "--clients") {
      options.clients =
          static_cast<std::size_t>(parse_unsigned(value, argument));
    } else if (argument == "--payload") {
      options.payload =
          static_cast<std::size_t>(parse_unsigned(value, argument));
    } else if (argument == "--operations") {
      options.operations =
          static_cast<std::size_t>(parse_unsigned(value, argument));
    } else if (argument == "--trial") {
      options.trial = parse_unsigned(value, argument);
    } else if (argument == "--seed") {
      options.seed = parse_unsigned(value, argument);
    } else if (argument == "--scenario") {
      options.scenario = value;
    } else {
      throw std::invalid_argument("unknown option: " + std::string(argument));
    }
  }
  const bool known_scenario =
      options.scenario == "healthy" || options.scenario == "leader-crash" ||
      options.scenario == "partition" ||
      options.scenario == "slow-follower" ||
      options.scenario == "slow-fsync";
  if ((options.mode != "sim" && options.mode != "tcp") ||
      (options.nodes != 3 && options.nodes != 5) ||
      (options.clients != 1 && options.clients != 3 && options.clients != 5) ||
      (options.payload != 64 && options.payload != 1024 &&
       options.payload != 16384) ||
      options.operations == 0 || options.operations > 1'000'000U ||
      !known_scenario) {
    throw std::invalid_argument("benchmark option is outside the supported matrix");
  }
  return options;
}

void json_string(std::ostream& output, std::string_view value) {
  static constexpr char hex[] = "0123456789abcdef";
  output.put('"');
  for (const char raw_character : value) {
    const auto character = static_cast<unsigned char>(raw_character);
    switch (character) {
      case '"':
        output << "\\\"";
        break;
      case '\\':
        output << "\\\\";
        break;
      case '\b':
        output << "\\b";
        break;
      case '\f':
        output << "\\f";
        break;
      case '\n':
        output << "\\n";
        break;
      case '\r':
        output << "\\r";
        break;
      case '\t':
        output << "\\t";
        break;
      default:
        if (character < 0x20U) {
          output << "\\u00" << hex[character >> 4U]
                 << hex[character & 0x0fU];
        } else {
          output.put(static_cast<char>(character));
        }
    }
  }
  output.put('"');
}

[[nodiscard]] std::string_view operating_system() noexcept {
#ifdef _WIN32
  return "windows";
#elif defined(__APPLE__)
  return "macos";
#elif defined(__linux__)
  return "linux";
#else
  return "unknown";
#endif
}

[[nodiscard]] std::string compiler_description() {
#if defined(__clang__)
  return std::string("clang ") + __clang_version__;
#elif defined(__GNUC__)
  return std::string("gcc ") + __VERSION__;
#elif defined(_MSC_VER)
  return "msvc " + std::to_string(_MSC_VER);
#else
  return "unknown";
#endif
}

[[nodiscard]] std::string environment_value(const char* name) {
  const char* value = std::getenv(name);
  return value == nullptr || *value == '\0' ? "not_provided" : value;
}

[[nodiscard]] std::optional<std::uint64_t> process_cpu_elapsed_ns(
    std::clock_t started) noexcept {
  const std::clock_t finished = std::clock();
  if (started == static_cast<std::clock_t>(-1) ||
      finished == static_cast<std::clock_t>(-1) || finished < started ||
      CLOCKS_PER_SEC <= 0) {
    return std::nullopt;
  }
  const long double nanoseconds =
      static_cast<long double>(finished - started) * 1'000'000'000.0L /
      static_cast<long double>(CLOCKS_PER_SEC);
  if (nanoseconds < 0.0L ||
      nanoseconds >
          static_cast<long double>(std::numeric_limits<std::uint64_t>::max())) {
    return std::nullopt;
  }
  return static_cast<std::uint64_t>(nanoseconds);
}

[[nodiscard]] std::optional<std::uint64_t> peak_resident_bytes() noexcept {
#ifdef _WIN32
  PROCESS_MEMORY_COUNTERS counters{};
  if (GetProcessMemoryInfo(GetCurrentProcess(), &counters,
                           static_cast<DWORD>(sizeof(counters))) == FALSE) {
    return std::nullopt;
  }
  return static_cast<std::uint64_t>(counters.PeakWorkingSetSize);
#elif defined(__unix__) || defined(__APPLE__)
  rusage usage{};
  if (getrusage(RUSAGE_SELF, &usage) != 0 || usage.ru_maxrss < 0) {
    return std::nullopt;
  }
  const auto resident = static_cast<std::uint64_t>(usage.ru_maxrss);
#ifdef __APPLE__
  return resident;
#else
  if (resident > std::numeric_limits<std::uint64_t>::max() / 1024U) {
    return std::nullopt;
  }
  return resident * 1024U;
#endif
#else
  return std::nullopt;
#endif
}

void emit_manifest(std::ostream& output, const Options& options,
                   bool supported, std::string_view unsupported_reason = {}) {
  const auto timestamp =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count();
  const std::uint64_t sim_event_bound =
      options.operations > 5'000U
          ? 100'000'000ULL
          : 100'000ULL +
                static_cast<std::uint64_t>(options.operations) * 20'000ULL;
  const std::uint64_t tcp_deadline_ms =
      std::min<std::uint64_t>(
          300'000ULL,
          30'000ULL +
              static_cast<std::uint64_t>(options.operations) * 100ULL);
  output << "{\"record\":\"manifest\",\"schema\":1,\"timestamp_epoch_ms\":"
         << timestamp << ",\"os\":";
  json_string(output, operating_system());
  output << ",\"compiler\":";
  json_string(output, compiler_description());
  output << ",\"cpp\":" << __cplusplus
         << ",\"hardware_threads\":" << std::thread::hardware_concurrency()
         << ",\"mode\":";
  json_string(output, options.mode);
  output << ",\"scenario\":";
  json_string(output, options.scenario);
  output << ",\"nodes\":" << options.nodes << ",\"clients\":"
         << options.clients << ",\"payload_bytes\":" << options.payload
         << ",\"operations\":" << options.operations << ",\"trial\":"
         << options.trial << ",\"seed\":" << options.seed
         << ",\"closed_loop\":true,\"max_unresolved_per_session\":1"
         << ",\"warmup_policy\":\"ready_leader_election_only\""
         << ",\"workload_warmup_operations\":0"
         << ",\"measurement_policy\":\"cold_closed_loop_operations_or_bound\""
         << ",\"sim_measurement_event_bound\":";
  if (options.mode == "sim") {
    output << sim_event_bound;
  } else {
    output << "null";
  }
  output << ",\"tcp_measurement_deadline_ms\":";
  if (options.mode == "tcp") {
    output << tcp_deadline_ms;
  } else {
    output << "null";
  }
  output
         << ",\"same_process_nodes\":"
         << (options.mode == "tcp" ? "true" : "false")
         << ",\"node_execution\":";
  json_string(output, options.mode == "tcp" ? "same_process_real_loopback_tcp"
                                             : "single_threaded_simulation");
  output << ",\"wal_flush_policy\":\"flush_every_append\""
         << ",\"sim_network_delay_ticks\":1"
         << ",\"sim_storage_write_ticks\":1"
         << ",\"sim_storage_flush_ticks\":"
         << (options.scenario == "slow-fsync" ? 20 : 1)
         << ",\"sim_slow_link_ticks\":15,\"sim_partition_ticks\":50"
         << ",\"build_commit\":";
  json_string(output, environment_value("DETLOG_BUILD_COMMIT"));
  output << ",\"build_flags\":";
  json_string(output, environment_value("DETLOG_BUILD_FLAGS"));
  output << ",\"cpu_measurement\":";
  json_string(output, "std_clock_coarse_process_wide_timed_workload");
  output << ",\"memory_measurement\":";
  json_string(output, "process_peak_resident_set_platform_api");
  output << ",\"supported\":" << (supported ? "true" : "false");
  if (!supported) {
    output << ",\"unsupported_reason\":";
    json_string(output, unsupported_reason);
  }
  output << "}\n";
}

void emit_operation(std::ostream& output, const Options& options,
                    const OperationRecord& record, std::string_view unit) {
  output << "{\"record\":\"operation\",\"mode\":";
  json_string(output, options.mode);
  output << ",\"scenario\":";
  json_string(output, options.scenario);
  output << ",\"trial\":" << options.trial << ",\"seed\":"
         << options.seed << ",\"operation\":" << record.operation
         << ",\"client\":" << record.client << ",\"sequence\":"
         << record.sequence << ",\"payload_bytes\":" << options.payload
         << ",\"start\":" << record.start << ",\"end\":" << record.end
         << ",\"latency\":" << record.latency << ",\"latency_unit\":";
  json_string(output, unit);
  output << ",\"attempts\":" << record.attempts << ",\"retries\":"
         << record.retries << ",\"queue_rejections\":"
         << record.queue_rejections << ",\"status\":";
  json_string(output, record.status);
  output << "}\n";
}

[[nodiscard]] std::uint64_t percentile(std::vector<std::uint64_t> values,
                                       std::size_t percentage) {
  if (values.empty()) return 0;
  std::sort(values.begin(), values.end());
  const std::size_t rank =
      (percentage * values.size() + 99U) / 100U;
  return values[std::max<std::size_t>(1, rank) - 1U];
}

void emit_summary(std::ostream& output, const Options& options,
                  const RunSummary& summary, std::string_view duration_unit,
                  std::string_view throughput_unit) {
  const double throughput =
      summary.duration == 0
          ? 0.0
          : (throughput_unit == "ops_per_second"
                 ? static_cast<double>(summary.successes) * 1'000'000'000.0 /
                       static_cast<double>(summary.duration)
                 : static_cast<double>(summary.successes) /
                       static_cast<double>(summary.duration));
  output << "{\"record\":\"summary\",\"mode\":";
  json_string(output, options.mode);
  output << ",\"scenario\":";
  json_string(output, options.scenario);
  output << ",\"nodes\":" << options.nodes << ",\"clients\":"
         << options.clients << ",\"payload_bytes\":" << options.payload
         << ",\"operations\":" << options.operations << ",\"trial\":"
         << options.trial << ",\"seed\":" << options.seed
         << ",\"successes\":" << summary.successes << ",\"failures\":"
         << summary.failures << ",\"retries\":" << summary.retries
         << ",\"queue_rejections\":" << summary.queue_rejections
         << ",\"duration\":" << summary.duration << ",\"duration_unit\":";
  json_string(output, duration_unit);
  output << ",\"wall_duration_ns\":" << summary.wall_duration_ns
         << ",\"process_cpu_ns\":";
  if (summary.process_cpu_ns) {
    output << *summary.process_cpu_ns;
  } else {
    output << "null";
  }
  output << ",\"process_cpu_scope\":";
  json_string(output, "coarse_process_wide_timed_workload");
  output << ",\"peak_resident_bytes\":";
  if (summary.peak_resident_bytes) {
    output << *summary.peak_resident_bytes;
  } else {
    output << "null";
  }
  output << ",\"memory_scope\":";
  json_string(output, "process_peak_resident_set");
  output
         << ",\"throughput\":" << std::setprecision(12) << throughput
         << ",\"throughput_unit\":";
  json_string(output, throughput_unit);
  output << ",\"p50\":"
         << percentile(summary.successful_latencies, 50)
         << ",\"p95\":" << percentile(summary.successful_latencies, 95)
         << ",\"p99\":" << percentile(summary.successful_latencies, 99)
         << ",\"latency_unit\":";
  json_string(output, duration_unit);
  output << ",\"elections\":" << summary.elections
         << ",\"transport_backpressure\":"
         << summary.transport_backpressure << ",\"transport_down_drops\":"
         << summary.transport_down_drops << ",\"storage_errors\":"
         << summary.storage_errors << ",\"owner_queue_high_water\":"
         << summary.owner_queue_high_water
         << ",\"client_queue_high_water\":"
         << summary.client_queue_high_water
         << ",\"sim_event_queue_high_water\":"
         << summary.sim_event_queue_high_water
         << ",\"sim_network_bytes_high_water\":"
         << summary.sim_network_bytes_high_water
         << ",\"sim_storage_bytes_high_water\":"
         << summary.sim_storage_bytes_high_water << ",\"status\":";
  json_string(output, summary.status);
  output << ",\"safety_check\":";
  json_string(output, summary.safety_check);
  output << ",\"fault_at\":";
  if (summary.fault_at) {
    output << *summary.fault_at;
  } else {
    output << "null";
  }
  output << ",\"replacement_leader_ready_duration\":";
  if (summary.replacement_leader_ready_duration) {
    output << *summary.replacement_leader_ready_duration;
  } else {
    output << "null";
  }
  output << ",\"recovery_to_first_success\":";
  if (summary.recovery_to_first_success) {
    output << *summary.recovery_to_first_success;
  } else {
    output << "null";
  }
  output << "}\n";
}

[[nodiscard]] detlog::ClientCommand make_command(
    const Options& options, std::size_t client, std::uint64_t sequence,
    const detlog::SessionId& session) {
  std::string value(options.payload, 'x');
  const std::string marker = std::to_string(sequence);
  std::copy_n(marker.begin(), std::min(marker.size(), value.size()),
              value.begin());
  return detlog::ClientCommand{session, sequence, detlog::CommandKind::put,
                               "bench-client-" + std::to_string(client),
                               std::move(value)};
}

struct SimOperation {
  OperationRecord record;
  detlog::ClientCommand command;
  std::optional<detlog::ClientToken> token;
  detlog::Tick attempt_started{};
};

struct SimClient {
  detlog::SessionId session;
  std::uint64_t next_sequence{1};
  std::optional<SimOperation> operation;
};

void abandon_sim_attempts(std::vector<SimClient>& clients,
                          std::map<detlog::ClientToken, std::size_t>& tokens) {
  tokens.clear();
  for (SimClient& client : clients) {
    if (client.operation && client.operation->token) {
      client.operation->token.reset();
      ++client.operation->record.retries;
    }
  }
}

[[nodiscard]] int run_sim(const Options& options, std::ostream& output) {
  detlog::ClusterConfig config =
      detlog::cluster_config(options.nodes, options.seed);
  config.automatic_invariant_checks = false;
  config.max_semantic_trace_records = 100'000;
  config.max_retained_client_replies =
      std::max<std::size_t>(10'000, options.clients * 16U);
  if (options.scenario == "slow-fsync") {
    config.storage_flush_latency = 20;
    config.raft.election_timeout = 80;
    config.election_jitter = 20;
  } else if (options.scenario == "slow-follower") {
    config.raft.election_timeout = 40;
    config.election_jitter = 10;
  }
  detlog::DeterministicCluster cluster(config);
  const auto warm_leader = cluster.run_until_leader(200'000, true);
  if (!warm_leader) {
    RunSummary failed;
    failed.failures = options.operations;
    failed.status = "initial_election_timeout";
    failed.safety_check = "not_run";
    emit_summary(output, options, failed, "virtual_ticks",
                 "ops_per_virtual_tick");
    return 1;
  }

  std::set<std::pair<detlog::Term, detlog::NodeId>> observed_leaders;
  auto observe_elections = [&] {
    for (const detlog::NodeSnapshot& snapshot : cluster.snapshots()) {
      if (snapshot.alive && snapshot.role == detlog::RaftRole::leader) {
        observed_leaders.emplace(snapshot.term, snapshot.id);
      }
    }
  };
  observe_elections();

  if (options.scenario == "slow-follower") {
    const detlog::NodeId slow =
        *warm_leader == static_cast<detlog::NodeId>(options.nodes)
            ? static_cast<detlog::NodeId>(options.nodes - 1U)
            : static_cast<detlog::NodeId>(options.nodes);
    (void)cluster.set_link_delay(*warm_leader, slow, 15);
    (void)cluster.set_link_delay(slow, *warm_leader, 15);
  }

  std::vector<SimClient> clients;
  clients.reserve(options.clients);
  for (std::size_t index = 0; index < options.clients; ++index) {
    clients.push_back(SimClient{
        detlog::SessionId{options.seed ^ (options.trial << 32U), index + 1U},
        1, std::nullopt});
  }
  std::map<detlog::ClientToken, std::size_t> tokens;
  std::uint64_t next_operation = 1;
  std::size_t issued = 0;
  RunSummary summary;
  const auto observe_sim_queues = [&] {
    const detlog::ClusterQueueDepth depth = cluster.queue_depth();
    summary.sim_event_queue_high_water =
        std::max(summary.sim_event_queue_high_water, depth.scheduled_events);
    summary.sim_network_bytes_high_water =
        std::max(summary.sim_network_bytes_high_water, depth.network_bytes);
    summary.sim_storage_bytes_high_water =
        std::max(summary.sim_storage_bytes_high_water, depth.storage_bytes);
  };
  observe_sim_queues();
  const detlog::Tick benchmark_start = cluster.now();
  const auto wall_start = Clock::now();
  const std::clock_t cpu_start = std::clock();
  const std::size_t fault_after =
      options.operations > 1 ? std::max<std::size_t>(1, options.operations / 3U)
                             : 0;
  bool fault_injected = false;
  bool partition_healed = false;
  detlog::NodeId partitioned_node{};
  std::optional<detlog::NodeId> faulted_leader;
  constexpr detlog::Tick kAttemptTimeout = 500;
  constexpr detlog::Tick kPartitionTicks = 50;
  const std::uint64_t scaled_events =
      options.operations > 5'000U ? 100'000'000ULL
                                  : 100'000ULL +
                                        static_cast<std::uint64_t>(
                                            options.operations) *
                                            20'000ULL;

  for (std::uint64_t events = 0;
       summary.successes < options.operations && events < scaled_events;
       ++events) {
    observe_sim_queues();
    for (detlog::ClientReply& reply : cluster.take_replies()) {
      const auto found = tokens.find(reply.token);
      if (found == tokens.end()) continue;
      SimClient& client = clients[found->second];
      tokens.erase(found);
      if (!client.operation || client.operation->token != reply.token) continue;
      SimOperation& operation = *client.operation;
      operation.token.reset();
      if (reply.status == detlog::ClientStatus::ok) {
        operation.record.end = cluster.now();
        operation.record.latency =
            operation.record.end - operation.record.start;
        operation.record.status = "ok";
        emit_operation(output, options, operation.record, "virtual_ticks");
        summary.successful_latencies.push_back(operation.record.latency);
        summary.retries += operation.record.retries;
        summary.queue_rejections += operation.record.queue_rejections;
        ++summary.successes;
        if (summary.fault_at && !summary.recovery_to_first_success) {
          summary.recovery_to_first_success =
              cluster.now() - *summary.fault_at;
        }
        ++client.next_sequence;
        client.operation.reset();
      } else {
        ++operation.record.retries;
        if (reply.status == detlog::ClientStatus::busy ||
            reply.status == detlog::ClientStatus::leader_not_ready) {
          ++operation.record.queue_rejections;
        }
      }
    }

    if (fault_injected && options.scenario == "partition" &&
        !partition_healed && summary.fault_at &&
        cluster.now() >= *summary.fault_at + kPartitionTicks) {
      (void)cluster.isolate(partitioned_node, false);
      partition_healed = true;
    }

    for (std::size_t index = 0; index < clients.size(); ++index) {
      SimClient& client = clients[index];
      if (!client.operation && issued < options.operations) {
        SimOperation operation;
        operation.record.operation = next_operation++;
        operation.record.client = index + 1U;
        operation.record.sequence = client.next_sequence;
        operation.record.start = cluster.now();
        operation.command = make_command(options, index + 1U,
                                         client.next_sequence, client.session);
        client.operation = std::move(operation);
        ++issued;
      }
      if (!client.operation) continue;
      SimOperation& operation = *client.operation;
      if (operation.token &&
          cluster.now() - operation.attempt_started >= kAttemptTimeout) {
        tokens.erase(*operation.token);
        operation.token.reset();
        ++operation.record.retries;
      }
      if (!operation.token) {
        const auto leader = cluster.leader(true);
        if (leader) {
          const detlog::ClientToken token =
              cluster.submit(*leader, operation.command);
          operation.token = token;
          operation.attempt_started = cluster.now();
          ++operation.record.attempts;
          tokens.emplace(token, index);
        }
      }
    }
    // Inject after the next closed-loop wave has been submitted so crash and
    // partition scenarios include genuinely ambiguous in-flight requests.
    if (!fault_injected && fault_after != 0 &&
        summary.successes >= fault_after &&
        (options.scenario == "leader-crash" ||
         options.scenario == "partition")) {
      const auto leader = cluster.leader(true);
      if (leader) {
        fault_injected = true;
        summary.fault_at = cluster.now();
        faulted_leader = *leader;
        abandon_sim_attempts(clients, tokens);
        if (options.scenario == "leader-crash") {
          (void)cluster.crash(*leader);
        } else {
          partitioned_node = *leader;
          (void)cluster.isolate(partitioned_node, true);
        }
      }
    }
    observe_elections();
    if (faulted_leader && summary.fault_at &&
        !summary.replacement_leader_ready_duration) {
      for (const detlog::NodeId ready : cluster.leaders(true)) {
        if (ready != *faulted_leader) {
          summary.replacement_leader_ready_duration =
              cluster.now() - *summary.fault_at;
          break;
        }
      }
    }
    if (!cluster.step()) break;
  }
  observe_sim_queues();

  const detlog::Tick benchmark_end = cluster.now();
  for (SimClient& client : clients) {
    if (!client.operation) continue;
    client.operation->record.end = benchmark_end;
    client.operation->record.latency =
        benchmark_end - client.operation->record.start;
    client.operation->record.status = "timeout";
    emit_operation(output, options, client.operation->record, "virtual_ticks");
    summary.retries += client.operation->record.retries;
    summary.queue_rejections += client.operation->record.queue_rejections;
  }
  while (issued < options.operations) {
    OperationRecord record;
    record.operation = next_operation++;
    record.client = issued % options.clients + 1U;
    record.status = "not_started";
    emit_operation(output, options, record, "virtual_ticks");
    ++issued;
  }
  summary.failures = options.operations - summary.successes;
  summary.duration = benchmark_end - benchmark_start;
  summary.wall_duration_ns = static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() -
                                                           wall_start)
          .count());
  summary.process_cpu_ns = process_cpu_elapsed_ns(cpu_start);
  summary.peak_resident_bytes = peak_resident_bytes();
  summary.elections = observed_leaders.size();
  const detlog::InvariantResult invariant = cluster.check_invariants();
  summary.safety_check = invariant ? "passed" : "failed";
  if (!invariant) summary.status = "invariant_failure";
  if (summary.failures != 0 && invariant) summary.status = "bounded_timeout";
  emit_summary(output, options, summary, "virtual_ticks",
               "ops_per_virtual_tick");
  return summary.failures == 0 && invariant ? 0 : 1;
}

class TempDirectory {
 public:
  TempDirectory(std::uint64_t seed, std::uint64_t trial) {
    const auto stamp = Clock::now().time_since_epoch().count();
    path_ = std::filesystem::temp_directory_path() /
            ("detlog-bench-" + std::to_string(seed) + "-" +
             std::to_string(trial) + "-" + std::to_string(stamp));
    std::filesystem::create_directories(path_);
  }

  ~TempDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path_, ignored);
  }

  [[nodiscard]] std::filesystem::path wal(detlog::NodeId node) const {
    return path_ / ("node-" + std::to_string(node) + ".wal");
  }

 private:
  std::filesystem::path path_;
};

[[nodiscard]] detlog::NodeHostConfig tcp_host_config(
    const Options& options, const TempDirectory& directory,
    detlog::NodeId node, std::span<const std::uint16_t> earlier_ports) {
  detlog::NodeHostConfig config;
  config.raft.node_id = node;
  for (std::size_t index = 0; index < options.nodes; ++index) {
    config.raft.members.push_back(static_cast<detlog::NodeId>(index + 1U));
  }
  config.raft.election_timeout =
      static_cast<detlog::Tick>(250U + (node - 1U) * 100U);
  config.raft.heartbeat_interval = 25;
  const std::uint64_t cluster_high = 0x42454e4348544350ULL ^ options.seed;
  const std::uint64_t cluster_low =
      0x53414d4550524f43ULL ^ (options.trial << 32U);
  config.identity = {cluster_high, cluster_low, node};
  config.wal_path = directory.wal(node);
  config.tcp.identity = {cluster_high, cluster_low, node};
  config.tcp.incarnation = (options.trial << 32U) | node;
  config.tcp.limits.handshake_timeout_ms = 2000;
  config.tcp.limits.reconnect_delay_ms = 20;
  config.tcp.limits.io_poll_interval_ms = 1;
  for (std::size_t index = 0; index < options.nodes; ++index) {
    const detlog::NodeId peer = static_cast<detlog::NodeId>(index + 1U);
    if (peer == node) continue;
    const std::uint16_t port =
        index < earlier_ports.size() ? earlier_ports[index] : 0;
    config.tcp.peers.push_back(
        detlog::TcpPeerEndpoint{peer, "127.0.0.1", port});
  }
  config.tick_duration = std::chrono::milliseconds(2);
  config.election_jitter = 50;
  config.timer_seed = options.seed + node * 0x10001ULL;
  config.idle_sleep = std::chrono::milliseconds(0);
  return config;
}

struct TcpOperation {
  OperationRecord record;
  detlog::ClientCommand command;
  std::optional<detlog::ClientToken> token;
  detlog::NodeId attempt_node{};
  Clock::time_point attempt_started;
  Clock::time_point retry_after;
};

struct TcpClient {
  detlog::SessionId session;
  std::uint64_t next_sequence{1};
  std::optional<TcpOperation> operation;
};

[[nodiscard]] std::optional<detlog::NodeId> ready_tcp_leader(
    const std::vector<std::unique_ptr<detlog::NodeHost>>& hosts) {
  std::optional<std::pair<detlog::Term, detlog::NodeId>> selected;
  for (std::size_t index = 0; index < hosts.size(); ++index) {
    if (!hosts[index]) continue;
    const detlog::NodeHostStatus status = hosts[index]->status();
    if (status.role != detlog::RaftRole::leader || !status.leader_ready) continue;
    const auto candidate = std::make_pair(status.term, status.node);
    if (!selected || candidate > *selected) selected = candidate;
  }
  return selected ? std::optional<detlog::NodeId>{selected->second}
                  : std::nullopt;
}

[[nodiscard]] std::uint64_t elapsed_ns(Clock::time_point start,
                                       Clock::time_point end) {
  return static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
          .count());
}

[[nodiscard]] int run_tcp(const Options& options, std::ostream& output) {
  TempDirectory directory(options.seed, options.trial);
  std::vector<std::unique_ptr<detlog::NodeHost>> hosts(options.nodes);
  std::vector<std::uint16_t> ports;
  ports.reserve(options.nodes);
  for (std::size_t index = 0; index < options.nodes; ++index) {
    const detlog::NodeId node = static_cast<detlog::NodeId>(index + 1U);
    auto config = tcp_host_config(options, directory, node, ports);
    hosts[index] = std::make_unique<detlog::NodeHost>(std::move(config));
    hosts[index]->start();
    ports.push_back(hosts[index]->listening_port());
  }

  std::set<std::pair<detlog::Term, detlog::NodeId>> observed_leaders;
  auto observe_elections = [&] {
    for (const auto& host : hosts) {
      if (!host) continue;
      const auto status = host->status();
      if (status.role == detlog::RaftRole::leader) {
        observed_leaders.emplace(status.term, status.node);
      }
    }
  };
  const auto warm_deadline = Clock::now() + std::chrono::seconds(8);
  while (!ready_tcp_leader(hosts) && Clock::now() < warm_deadline) {
    bool activity = false;
    for (auto& host : hosts) {
      for (int iteration = 0; iteration < 8; ++iteration) {
        activity = host->poll_once() || activity;
      }
      (void)host->poll_replies();
    }
    observe_elections();
    if (!activity) std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  if (!ready_tcp_leader(hosts)) {
    RunSummary failed;
    failed.failures = options.operations;
    failed.status = "initial_election_timeout";
    failed.safety_check = "not_run";
    emit_summary(output, options, failed, "nanoseconds", "ops_per_second");
    return 1;
  }

  std::vector<TcpClient> clients;
  clients.reserve(options.clients);
  for (std::size_t index = 0; index < options.clients; ++index) {
    clients.push_back(TcpClient{
        detlog::SessionId{options.seed ^ (options.trial << 32U), index + 1U},
        1, std::nullopt});
  }
  using AttemptKey = std::pair<detlog::NodeId, detlog::ClientToken>;
  std::map<AttemptKey, std::size_t> tokens;
  std::size_t issued = 0;
  std::uint64_t next_operation = 1;
  RunSummary summary;
  const auto benchmark_start = Clock::now();
  const std::clock_t cpu_start = std::clock();
  const std::size_t fault_after =
      options.operations > 1 ? std::max<std::size_t>(1, options.operations / 3U)
                             : 0;
  bool fault_injected = false;
  std::optional<detlog::NodeId> faulted_leader;
  detlog::NodeHostMetrics crashed_metrics;
  const std::uint64_t timeout_scale =
      std::min<std::uint64_t>(300'000ULL,
                              30'000ULL +
                                  static_cast<std::uint64_t>(options.operations) *
                                      100ULL);
  const auto deadline = benchmark_start +
                        std::chrono::milliseconds(timeout_scale);
  constexpr auto kAttemptTimeout = std::chrono::seconds(2);

  while (summary.successes < options.operations && Clock::now() < deadline) {
    bool activity = false;
    for (std::size_t node_index = 0; node_index < hosts.size(); ++node_index) {
      auto& host = hosts[node_index];
      if (!host) continue;
      for (int iteration = 0; iteration < 8; ++iteration) {
        activity = host->poll_once() || activity;
      }
      if (faulted_leader && summary.fault_at &&
          !summary.replacement_leader_ready_duration) {
        const auto replacement = ready_tcp_leader(hosts);
        if (replacement && *replacement != *faulted_leader) {
          summary.replacement_leader_ready_duration =
              elapsed_ns(benchmark_start, Clock::now()) - *summary.fault_at;
        }
      }
      for (detlog::ClientReply& reply : host->poll_replies()) {
        const AttemptKey key{static_cast<detlog::NodeId>(node_index + 1U),
                             reply.token};
        const auto found = tokens.find(key);
        if (found == tokens.end()) continue;
        TcpClient& client = clients[found->second];
        tokens.erase(found);
        if (!client.operation || client.operation->token != reply.token ||
            client.operation->attempt_node != key.first) {
          continue;
        }
        TcpOperation& operation = *client.operation;
        operation.token.reset();
        if (reply.status == detlog::ClientStatus::ok) {
          const auto now = Clock::now();
          operation.record.end = elapsed_ns(benchmark_start, now);
          operation.record.latency =
              operation.record.end - operation.record.start;
          operation.record.status = "ok";
          emit_operation(output, options, operation.record, "nanoseconds");
          summary.successful_latencies.push_back(operation.record.latency);
          summary.retries += operation.record.retries;
          summary.queue_rejections += operation.record.queue_rejections;
          ++summary.successes;
          if (summary.fault_at && !summary.recovery_to_first_success) {
            summary.recovery_to_first_success =
                operation.record.end - *summary.fault_at;
          }
          ++client.next_sequence;
          client.operation.reset();
        } else {
          ++operation.record.retries;
          if (reply.status == detlog::ClientStatus::busy ||
              reply.status == detlog::ClientStatus::leader_not_ready) {
            ++operation.record.queue_rejections;
          }
          operation.retry_after = Clock::now() + std::chrono::milliseconds(1);
        }
      }
    }
    observe_elections();

    const auto now = Clock::now();
    for (std::size_t index = 0; index < clients.size(); ++index) {
      TcpClient& client = clients[index];
      if (!client.operation && issued < options.operations) {
        TcpOperation operation;
        operation.record.operation = next_operation++;
        operation.record.client = index + 1U;
        operation.record.sequence = client.next_sequence;
        operation.record.start = elapsed_ns(benchmark_start, now);
        operation.command = make_command(options, index + 1U,
                                         client.next_sequence, client.session);
        operation.retry_after = now;
        client.operation = std::move(operation);
        ++issued;
      }
      if (!client.operation) continue;
      TcpOperation& operation = *client.operation;
      if (operation.token && now - operation.attempt_started >= kAttemptTimeout) {
        tokens.erase(AttemptKey{operation.attempt_node, *operation.token});
        operation.token.reset();
        ++operation.record.retries;
        operation.retry_after = now;
      }
      if (operation.token || now < operation.retry_after) continue;
      const auto leader = ready_tcp_leader(hosts);
      if (!leader) continue;
      const std::size_t leader_index = static_cast<std::size_t>(*leader - 1U);
      if (!hosts[leader_index]) continue;
      const detlog::NodeHostSubmitResult submitted =
          hosts[leader_index]->submit(operation.command);
      ++operation.record.attempts;
      if (submitted.status == detlog::NodeHostSubmitStatus::accepted) {
        operation.token = submitted.token;
        operation.attempt_node = *leader;
        operation.attempt_started = now;
        tokens.emplace(AttemptKey{*leader, submitted.token}, index);
      } else {
        ++operation.record.retries;
        if (submitted.status == detlog::NodeHostSubmitStatus::would_block) {
          ++operation.record.queue_rejections;
        }
        operation.retry_after = now + std::chrono::milliseconds(1);
      }
    }
    if (!fault_injected && fault_after != 0 &&
        summary.successes >= fault_after &&
        options.scenario == "leader-crash") {
      const auto leader = ready_tcp_leader(hosts);
      if (leader) {
        fault_injected = true;
        summary.fault_at = elapsed_ns(benchmark_start, Clock::now());
        faulted_leader = *leader;
        const std::size_t dead_index = static_cast<std::size_t>(*leader - 1U);
        crashed_metrics = hosts[dead_index]->metrics();
        hosts[dead_index]->stop();
        hosts[dead_index].reset();
        for (TcpClient& client : clients) {
          if (client.operation && client.operation->token &&
              client.operation->attempt_node == *leader) {
            tokens.erase(AttemptKey{*leader, *client.operation->token});
            client.operation->token.reset();
            ++client.operation->record.retries;
            client.operation->retry_after = Clock::now();
          }
        }
      }
    }
    if (!activity) std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  const auto benchmark_end = Clock::now();
  for (TcpClient& client : clients) {
    if (!client.operation) continue;
    client.operation->record.end = elapsed_ns(benchmark_start, benchmark_end);
    client.operation->record.latency =
        client.operation->record.end - client.operation->record.start;
    client.operation->record.status = "timeout";
    emit_operation(output, options, client.operation->record, "nanoseconds");
    summary.retries += client.operation->record.retries;
    summary.queue_rejections += client.operation->record.queue_rejections;
  }
  while (issued < options.operations) {
    OperationRecord record;
    record.operation = next_operation++;
    record.client = issued % options.clients + 1U;
    record.status = "not_started";
    emit_operation(output, options, record, "nanoseconds");
    ++issued;
  }
  summary.failures = options.operations - summary.successes;
  summary.duration = elapsed_ns(benchmark_start, benchmark_end);
  summary.wall_duration_ns = summary.duration;
  summary.process_cpu_ns = process_cpu_elapsed_ns(cpu_start);
  summary.peak_resident_bytes = peak_resident_bytes();
  summary.elections = observed_leaders.size();
  summary.safety_check = "covered_by_runtime_integration_tests";
  for (const auto& host : hosts) {
    if (!host) continue;
    const detlog::NodeHostMetrics metrics = host->metrics();
    summary.transport_backpressure += metrics.tcp_backpressure_drops;
    summary.transport_down_drops += metrics.tcp_down_drops;
    summary.storage_errors += metrics.storage_errors;
    summary.owner_queue_high_water =
        std::max(summary.owner_queue_high_water,
                 metrics.owner_queue_high_water);
    summary.client_queue_high_water =
        std::max(summary.client_queue_high_water,
                 metrics.client_queue_high_water);
    host->stop();
  }
  summary.transport_backpressure += crashed_metrics.tcp_backpressure_drops;
  summary.transport_down_drops += crashed_metrics.tcp_down_drops;
  summary.storage_errors += crashed_metrics.storage_errors;
  summary.owner_queue_high_water =
      std::max(summary.owner_queue_high_water,
               crashed_metrics.owner_queue_high_water);
  summary.client_queue_high_water =
      std::max(summary.client_queue_high_water,
               crashed_metrics.client_queue_high_water);
  if (summary.failures != 0) summary.status = "bounded_timeout";
  emit_summary(output, options, summary, "nanoseconds", "ops_per_second");
  return summary.failures == 0 ? 0 : 1;
}

}  // namespace

int main(int argc, char** argv) {
  try {
    const Options options = parse_options(argc, argv);
    const bool tcp_supported =
        options.scenario == "healthy" || options.scenario == "leader-crash";
    if (options.mode == "tcp" && !tcp_supported) {
      const std::string reason =
          "tcp mode supports healthy and leader-crash only; deterministic "
          "partition, slow-follower, and slow-fsync controls exist only in sim mode";
      emit_manifest(std::cout, options, false, reason);
      std::cout << "{\"record\":\"unsupported\",\"mode\":\"tcp\",\"scenario\":";
      json_string(std::cout, options.scenario);
      std::cout << ",\"reason\":";
      json_string(std::cout, reason);
      std::cout << "}\n";
      std::cerr << "detlog-bench: " << reason << '\n';
      return 2;
    }

    emit_manifest(std::cout, options, true);
    return options.mode == "sim" ? run_sim(options, std::cout)
                                  : run_tcp(options, std::cout);
  } catch (const std::exception& error) {
    std::cerr << "detlog-bench: " << error.what() << '\n';
    print_usage(std::cerr);
    return 1;
  }
}
