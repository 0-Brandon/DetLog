#include "detlog/wal.hpp"

#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <ctime>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <vector>

namespace {

using WallClock = std::chrono::steady_clock;

constexpr std::uint64_t kMaxEntries = 1'000'000;
constexpr std::uint64_t kMaxPayloadBytes = 8U * 1024U * 1024U;
constexpr std::uint64_t kEstimatedBytesPerEntry = 256;
constexpr std::uint64_t kMaxEstimatedWorkloadBytes = 256U * 1024U * 1024U;
constexpr std::uint64_t kMaxEstimatedGroupBytes = 64U * 1024U * 1024U;
constexpr std::size_t kMaxGroupSize = 1024;

enum class Policy : std::uint8_t {
  flush_every,
  group,
  unsafe_no_flush,
};

struct Options {
  std::uint64_t entries{};
  std::uint64_t payload_bytes{};
  std::uint64_t trial{};
  Policy policy{Policy::flush_every};
  std::size_t group_size{1};
};

[[nodiscard]] std::string_view policy_name(Policy policy) noexcept {
  switch (policy) {
    case Policy::flush_every:
      return "flush-every";
    case Policy::group:
      return "group";
    case Policy::unsafe_no_flush:
      return "unsafe-no-flush";
  }
  return "unknown";
}

[[nodiscard]] bool durable_during_append(Policy policy) noexcept {
  return policy != Policy::unsafe_no_flush;
}

void json_string(std::ostream& output, std::string_view value) {
  constexpr char hex[] = "0123456789abcdef";
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

void print_usage(std::ostream& output) {
  output
      << "Usage: detlog-wal-bench --entries N --payload BYTES --trial N "
         "--policy flush-every|group|unsafe-no-flush "
         "[--group-size N]\n"
      << "  --group-size is required for group policy (2..1024) and is "
         "invalid for other policies.\n";
}

[[nodiscard]] std::uint64_t parse_u64(std::string_view text,
                                      std::string_view name) {
  if (text.empty()) {
    throw std::invalid_argument(std::string(name) + " cannot be empty");
  }
  std::uint64_t value = 0;
  const auto parsed =
      std::from_chars(text.data(), text.data() + text.size(), value);
  if (parsed.ec != std::errc{} || parsed.ptr != text.data() + text.size()) {
    throw std::invalid_argument(std::string(name) +
                                " must be an unsigned integer");
  }
  return value;
}

[[nodiscard]] Policy parse_policy(std::string_view text) {
  if (text == "flush-every") return Policy::flush_every;
  if (text == "group") return Policy::group;
  if (text == "unsafe-no-flush") return Policy::unsafe_no_flush;
  throw std::invalid_argument(
      "policy must be flush-every, group, or unsafe-no-flush");
}

[[nodiscard]] Options parse_options(int argc, char** argv) {
  Options options;
  bool have_entries = false;
  bool have_payload = false;
  bool have_trial = false;
  bool have_policy = false;
  bool have_group_size = false;

  for (int index = 1; index < argc; ++index) {
    const std::string_view argument(argv[index]);
    if (argument == "--help" || argument == "-h") {
      print_usage(std::cout);
      throw std::runtime_error("help requested");
    }
    if (index + 1 >= argc) {
      throw std::invalid_argument(std::string(argument) +
                                  " requires a value");
    }
    const std::string_view value(argv[++index]);
    if (argument == "--entries") {
      options.entries = parse_u64(value, "entries");
      have_entries = true;
    } else if (argument == "--payload") {
      options.payload_bytes = parse_u64(value, "payload");
      have_payload = true;
    } else if (argument == "--trial") {
      options.trial = parse_u64(value, "trial");
      have_trial = true;
    } else if (argument == "--policy") {
      options.policy = parse_policy(value);
      have_policy = true;
    } else if (argument == "--group-size") {
      const std::uint64_t parsed = parse_u64(value, "group-size");
      if (parsed > std::numeric_limits<std::size_t>::max()) {
        throw std::invalid_argument("group-size exceeds this platform's range");
      }
      options.group_size = static_cast<std::size_t>(parsed);
      have_group_size = true;
    } else {
      throw std::invalid_argument("unknown argument: " +
                                  std::string(argument));
    }
  }

  if (!have_entries || !have_payload || !have_trial || !have_policy) {
    throw std::invalid_argument(
        "entries, payload, trial, and policy are all required");
  }
  if (options.entries == 0 || options.entries > kMaxEntries) {
    throw std::invalid_argument("entries must be in the range 1..1000000");
  }
  if (options.payload_bytes > kMaxPayloadBytes) {
    throw std::invalid_argument("payload must be at most 8388608 bytes");
  }
  if (options.trial == 0) {
    throw std::invalid_argument("trial must be positive");
  }
  if (options.policy == Policy::group) {
    if (!have_group_size || options.group_size < 2 ||
        options.group_size > kMaxGroupSize) {
      throw std::invalid_argument(
          "group policy requires --group-size in the range 2..1024");
    }
  } else if (have_group_size) {
    throw std::invalid_argument(
        "--group-size is valid only with the group policy");
  }

  const std::uint64_t estimated_entry_bytes =
      options.payload_bytes + kEstimatedBytesPerEntry;
  if (options.entries >
      kMaxEstimatedWorkloadBytes / estimated_entry_bytes) {
    throw std::invalid_argument(
        "requested workload exceeds the 256 MiB benchmark bound");
  }
  if (options.policy == Policy::group &&
      static_cast<std::uint64_t>(options.group_size) >
          kMaxEstimatedGroupBytes / estimated_entry_bytes) {
    throw std::invalid_argument(
        "requested group can exceed the WAL's 64 MiB group bound");
  }
  return options;
}

class TempDirectory final {
 public:
  explicit TempDirectory(std::uint64_t trial) {
    const std::filesystem::path root = std::filesystem::temp_directory_path();
    const auto stamp = WallClock::now().time_since_epoch().count();
    for (std::uint32_t attempt = 0; attempt < 128; ++attempt) {
      const std::string name =
          "detlog-wal-bench-" + std::to_string(trial) + "-" +
          std::to_string(stamp) + "-" + std::to_string(attempt);
      const std::filesystem::path candidate = root / name;
      std::error_code error;
      if (std::filesystem::create_directory(candidate, error)) {
        path_ = candidate;
        return;
      }
      if (error && error != std::errc::file_exists) {
        throw std::filesystem::filesystem_error(
            "cannot create benchmark temporary directory", candidate, error);
      }
    }
    throw std::runtime_error("could not allocate a unique temporary directory");
  }

  ~TempDirectory() {
    if (cleaned_ || path_.empty()) return;
    std::error_code ignored;
    (void)std::filesystem::remove_all(path_, ignored);
  }

  TempDirectory(const TempDirectory&) = delete;
  TempDirectory& operator=(const TempDirectory&) = delete;

  [[nodiscard]] std::filesystem::path wal_path() const {
    return path_ / "benchmark.wal";
  }

  void cleanup() {
    std::error_code error;
    (void)std::filesystem::remove_all(path_, error);
    if (error) {
      throw std::filesystem::filesystem_error(
          "cannot remove benchmark temporary directory", path_, error);
    }
    cleaned_ = true;
  }

 private:
  std::filesystem::path path_;
  bool cleaned_{};
};

[[nodiscard]] std::uint64_t elapsed_ns(WallClock::time_point start,
                                       WallClock::time_point end) {
  const auto count =
      std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
  if (count < 0) throw std::runtime_error("steady clock moved backwards");
  return static_cast<std::uint64_t>(count);
}

[[nodiscard]] std::uint64_t elapsed_cpu_ns(std::clock_t start,
                                           std::clock_t end) {
  if (start == static_cast<std::clock_t>(-1) ||
      end == static_cast<std::clock_t>(-1) || end < start) {
    throw std::runtime_error("process CPU clock is unavailable");
  }
  const long double nanoseconds =
      static_cast<long double>(end - start) * 1'000'000'000.0L /
      static_cast<long double>(CLOCKS_PER_SEC);
  if (nanoseconds >
      static_cast<long double>(std::numeric_limits<std::uint64_t>::max())) {
    throw std::runtime_error("process CPU duration overflowed");
  }
  return static_cast<std::uint64_t>(nanoseconds);
}

[[nodiscard]] std::vector<detlog::PersistBatch> make_batches(
    const Options& options) {
  const std::size_t entry_count = static_cast<std::size_t>(options.entries);
  const std::size_t payload_size =
      static_cast<std::size_t>(options.payload_bytes);
  const std::string payload(payload_size, 'w');
  std::vector<detlog::PersistBatch> batches;
  batches.reserve(entry_count);
  for (std::size_t offset = 0; offset < entry_count; ++offset) {
    const auto index = static_cast<detlog::LogIndex>(offset + 1U);
    detlog::ClientCommand command;
    command.session = detlog::SessionId{0x57414c42454e4348ULL, options.trial};
    command.sequence = index;
    command.kind = detlog::CommandKind::put;
    command.key = "benchmark-key";
    command.value = payload;

    detlog::PersistBatch batch;
    if (offset == 0) batch.hard_state = detlog::HardState{1, 1};
    batch.entries.push_back(detlog::LogEntry{index, 1, std::move(command)});
    batch.commit_index = index;
    batches.push_back(std::move(batch));
  }
  return batches;
}

void emit_manifest(const Options& options) {
  const bool durable = durable_during_append(options.policy);
  const auto timestamp =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count();
  std::cout
      << "{\"record\":\"manifest\",\"schema\":\"detlog-wal-bench/v1\""
      << ",\"timestamp_epoch_ms\":" << timestamp << ",\"os\":";
  json_string(std::cout, operating_system());
  std::cout << ",\"compiler\":";
  json_string(std::cout, compiler_description());
  std::cout << ",\"cpp\":" << __cplusplus
      << ",\"hardware_threads\":" << std::thread::hardware_concurrency()
      << ",\"entries\":" << options.entries
      << ",\"payload_bytes\":" << options.payload_bytes
      << ",\"trial\":" << options.trial << ",\"policy\":\""
      << policy_name(options.policy) << "\",\"group_size\":"
      << options.group_size << ",\"durable_during_append\":"
      << std::boolalpha << durable;
  if (durable) {
    std::cout
        << ",\"durability_label\":\"DURABLE_ACKNOWLEDGEMENTS_DURING_APPEND\""
        << ",\"reopen_precondition\":\"append-policy-already-flushed\"";
  } else {
    std::cout
        << ",\"durability_label\":\"UNSAFE_NONDURABLE_DURING_APPEND_ACKNOWLEDGEMENTS\""
        << ",\"reopen_precondition\":\"explicit-final-flush-required-and-performed\"";
  }
  std::cout << ",\"build_commit\":";
  json_string(std::cout, environment_value("DETLOG_BUILD_COMMIT"));
  std::cout << ",\"build_flags\":";
  json_string(std::cout, environment_value("DETLOG_BUILD_FLAGS"));
  std::cout << "}\n";
}

void emit_summary(const Options& options, std::size_t append_calls,
                  std::uint64_t append_wall_ns,
                  std::uint64_t append_cpu_ns,
                  std::uint64_t final_flush_wall_ns,
                  std::uint64_t final_flush_cpu_ns,
                  std::uintmax_t file_bytes,
                  std::uint64_t reopen_scan_wall_ns,
                  std::size_t recovered_entry_count,
                  detlog::LogIndex recovered_commit_index) {
  const bool unsafe = options.policy == Policy::unsafe_no_flush;
  const long double operations_per_second =
      append_wall_ns == 0
          ? 0.0L
          : static_cast<long double>(options.entries) * 1'000'000'000.0L /
                static_cast<long double>(append_wall_ns);
  std::cout << "{\"record\":\"summary\",\"schema\":\"detlog-wal-bench/v1\""
            << ",\"status\":\"ok\",\"entries\":" << options.entries
            << ",\"payload_bytes\":" << options.payload_bytes
            << ",\"trial\":" << options.trial << ",\"policy\":\""
            << policy_name(options.policy) << "\",\"group_size\":"
            << options.group_size << ",\"append_calls\":" << append_calls
            << ",\"durable_during_append\":" << std::boolalpha << !unsafe
            << ",\"append_wall_ns\":" << append_wall_ns
            << ",\"append_cpu_ns\":" << append_cpu_ns
            << ",\"append_ops_per_second\":" << std::fixed
            << std::setprecision(3)
            << static_cast<double>(operations_per_second)
            << ",\"final_explicit_flush_performed\":" << unsafe
            << ",\"final_flush_wall_ns\":" << final_flush_wall_ns
            << ",\"final_flush_cpu_ns\":" << final_flush_cpu_ns
            << ",\"file_bytes\":" << file_bytes
            << ",\"reopen_scan_wall_ns\":" << reopen_scan_wall_ns
            << ",\"recovered_entry_count\":" << recovered_entry_count
            << ",\"recovered_commit_index\":" << recovered_commit_index
            << "}\n";
}

int run(const Options& options) {
  emit_manifest(options);
  std::cerr << "detlog-wal-bench: policy=" << policy_name(options.policy)
            << " entries=" << options.entries
            << " payload=" << options.payload_bytes;
  if (options.policy == Policy::group) {
    std::cerr << " group-size=" << options.group_size;
  }
  std::cerr << '\n';

  TempDirectory directory(options.trial);
  const std::filesystem::path path = directory.wal_path();
  const detlog::WalIdentity identity{0x4445544c4f475741ULL,
                                    0x4c42454e43480000ULL ^ options.trial, 1};
  detlog::WalOptions wal_options;
  wal_options.flush_policy =
      options.policy == Policy::unsafe_no_flush
          ? detlog::WalFlushPolicy::unsafe_no_flush
          : detlog::WalFlushPolicy::flush_every_append;
  wal_options.max_group_frames = static_cast<std::uint32_t>(
      options.policy == Policy::group ? options.group_size : 1U);

  const std::vector<detlog::PersistBatch> batches = make_batches(options);
  std::size_t append_calls = 0;
  std::uint64_t append_wall_ns = 0;
  std::uint64_t append_cpu_ns = 0;
  std::uint64_t final_flush_wall_ns = 0;
  std::uint64_t final_flush_cpu_ns = 0;
  std::uint64_t logical_file_bytes = 0;

  {
    detlog::Wal wal(path, identity, wal_options);
    const WallClock::time_point append_start = WallClock::now();
    const std::clock_t append_cpu_start = std::clock();
    if (options.policy == Policy::group) {
      std::size_t offset = 0;
      while (offset < batches.size()) {
        const std::size_t remaining = batches.size() - offset;
        const std::size_t count =
            remaining < options.group_size ? remaining : options.group_size;
        const std::span<const detlog::PersistBatch> group(batches.data() + offset,
                                                         count);
        const detlog::WalAppendResult result = wal.append_group(group);
        if (!result.durable) {
          throw std::runtime_error(
              "group policy unexpectedly returned a nondurable acknowledgement");
        }
        offset += count;
        ++append_calls;
      }
    } else {
      const bool expected_durable = durable_during_append(options.policy);
      for (const detlog::PersistBatch& batch : batches) {
        const detlog::WalAppendResult result = wal.append(batch);
        if (result.durable != expected_durable) {
          throw std::runtime_error(
              "WAL acknowledgement durability did not match benchmark policy");
        }
        ++append_calls;
      }
    }
    const std::clock_t append_cpu_end = std::clock();
    const WallClock::time_point append_end = WallClock::now();
    append_wall_ns = elapsed_ns(append_start, append_end);
    append_cpu_ns = elapsed_cpu_ns(append_cpu_start, append_cpu_end);

    if (options.policy == Policy::unsafe_no_flush) {
      const detlog::WalState before_flush = wal.state();
      if (before_flush.durable_frame_sequence ==
          before_flush.last_frame_sequence) {
        throw std::runtime_error(
            "unsafe policy was unexpectedly durable before the final flush");
      }
      const WallClock::time_point flush_start = WallClock::now();
      const std::clock_t flush_cpu_start = std::clock();
      wal.flush();
      const std::clock_t flush_cpu_end = std::clock();
      const WallClock::time_point flush_end = WallClock::now();
      final_flush_wall_ns = elapsed_ns(flush_start, flush_end);
      final_flush_cpu_ns = elapsed_cpu_ns(flush_cpu_start, flush_cpu_end);
      const detlog::WalState after_flush = wal.state();
      if (after_flush.durable_frame_sequence !=
          after_flush.last_frame_sequence) {
        throw std::runtime_error("explicit final WAL flush did not become durable");
      }
    }
    logical_file_bytes = wal.state().file_bytes;
  }

  const std::uintmax_t file_bytes = std::filesystem::file_size(path);
  if (file_bytes != logical_file_bytes) {
    throw std::runtime_error("WAL logical and filesystem byte counts disagree");
  }

  const WallClock::time_point reopen_start = WallClock::now();
  std::unique_ptr<detlog::Wal> reopened =
      std::make_unique<detlog::Wal>(path, identity, wal_options);
  const WallClock::time_point reopen_end = WallClock::now();
  const std::uint64_t reopen_scan_wall_ns =
      elapsed_ns(reopen_start, reopen_end);
  const detlog::WalState recovered = reopened->state();
  reopened.reset();
  if (recovered.entries.size() != batches.size() ||
      recovered.commit_index != options.entries) {
    throw std::runtime_error("reopened WAL did not recover the expected log");
  }

  directory.cleanup();
  emit_summary(options, append_calls, append_wall_ns, append_cpu_ns,
               final_flush_wall_ns, final_flush_cpu_ns, file_bytes,
               reopen_scan_wall_ns, recovered.entries.size(),
               recovered.commit_index);
  std::cerr << "detlog-wal-bench: completed and removed temporary WAL\n";
  return 0;
}

}  // namespace

int main(int argc, char** argv) {
  if (argc == 2) {
    const std::string_view argument(argv[1]);
    if (argument == "--help" || argument == "-h") {
      print_usage(std::cout);
      return 0;
    }
  }
  try {
    return run(parse_options(argc, argv));
  } catch (const std::exception& error) {
    std::cerr << "detlog-wal-bench: " << error.what() << '\n';
    print_usage(std::cerr);
    return 1;
  }
}
