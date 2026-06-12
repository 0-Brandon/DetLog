#include "detlog/wal.hpp"

#include <chrono>
#include <cerrno>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>

#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#else
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

namespace {

constexpr detlog::WalIdentity kIdentity{0x4445544c4f470001ULL,
                                        0x4352415348544553ULL, 1};

[[noreturn]] void fail(std::string message, int line) {
  throw std::runtime_error("line " + std::to_string(line) + ": " + message);
}

#define REQUIRE(condition)                                                   \
  do {                                                                       \
    if (!(condition)) fail("requirement failed: " #condition, __LINE__);    \
  } while (false)

[[nodiscard]] detlog::PersistBatch committed_batch() {
  detlog::PersistBatch batch;
  batch.hard_state = detlog::HardState{1, 1};
  batch.entries.push_back(detlog::LogEntry{
      1,
      1,
      detlog::ClientCommand{detlog::SessionId{1, 2}, 1,
                            detlog::CommandKind::put, "key", "value"},
  });
  batch.commit_index = 1;
  return batch;
}

[[noreturn]] void durable_child(const std::filesystem::path& path) {
  detlog::Wal wal(path, kIdentity);
  const auto result = wal.append(committed_batch());
  if (!result.durable) std::_Exit(90);
  // Deliberately bypass destructors after the durability boundary.
  std::_Exit(23);
}

[[noreturn]] void partial_frame_child(const std::filesystem::path& path) {
  detlog::WalState state;
  {
    detlog::Wal wal(path, kIdentity);
    state = wal.state();
  }
  const auto encoded =
      detlog::encode_wal_frame(state, committed_batch());
  const std::size_t cut = encoded.bytes.size() / 2U;
  if (cut == 0 || cut == encoded.bytes.size()) std::_Exit(91);

  std::ofstream output(path, std::ios::binary | std::ios::app);
  if (!output) std::_Exit(92);
  output.write(reinterpret_cast<const char*>(encoded.bytes.data()),
               static_cast<std::streamsize>(cut));
  output.flush();
  if (!output) std::_Exit(93);
  // This models an abrupt process stop at a named short-write boundary.
  std::_Exit(24);
}

void run_child(const std::filesystem::path& executable,
               std::string_view mode, const std::filesystem::path& wal) {
  int expected_exit = -1;
  if (mode == "durable") expected_exit = 23;
  if (mode == "partial") expected_exit = 24;
  REQUIRE(expected_exit >= 0);
#ifdef _WIN32
  std::wstring command = L"\"" + executable.wstring() + L"\" --child ";
  command.append(mode.begin(), mode.end());
  command.append(L" \"");
  command.append(wal.wstring());
  command.push_back(L'"');

  STARTUPINFOW startup{};
  startup.cb = sizeof(startup);
  PROCESS_INFORMATION process{};
  const BOOL created = CreateProcessW(
      executable.c_str(), command.data(), nullptr, nullptr, FALSE, 0, nullptr,
      nullptr, &startup, &process);
  REQUIRE(created != FALSE);
  REQUIRE(WaitForSingleObject(process.hProcess, INFINITE) == WAIT_OBJECT_0);
  DWORD exit_code = 0;
  REQUIRE(GetExitCodeProcess(process.hProcess, &exit_code) != FALSE);
  CloseHandle(process.hThread);
  CloseHandle(process.hProcess);
  REQUIRE(exit_code == static_cast<DWORD>(expected_exit));
#else
  const std::string executable_text = executable.string();
  const std::string mode_text{mode};
  const std::string wal_text = wal.string();
  const pid_t child = fork();
  REQUIRE(child >= 0);
  if (child == 0) {
    execl(executable_text.c_str(), executable_text.c_str(), "--child",
          mode_text.c_str(), wal_text.c_str(), static_cast<char*>(nullptr));
    std::_Exit(95);
  }
  int status = 0;
  pid_t waited = -1;
  do {
    waited = waitpid(child, &status, 0);
  } while (waited < 0 && errno == EINTR);
  REQUIRE(waited == child);
  REQUIRE(WIFEXITED(status));
  REQUIRE(WEXITSTATUS(status) == expected_exit);
#endif
}

void parent_test(const std::filesystem::path& executable) {
  const auto stamp = std::chrono::steady_clock::now()
                         .time_since_epoch()
                         .count();
  const auto directory = std::filesystem::temp_directory_path() /
                         ("detlog-process-crash-" + std::to_string(stamp));
  std::filesystem::create_directories(directory);
  struct Cleanup {
    std::filesystem::path path;
    ~Cleanup() {
      std::error_code ignored;
      std::filesystem::remove_all(path, ignored);
    }
  } cleanup{directory};

  const auto durable_path = directory / "durable.wal";
  run_child(executable, "durable", durable_path);
  {
    detlog::Wal recovered(durable_path, kIdentity);
    const auto state = recovered.state();
    REQUIRE(state.entries.size() == 1);
    REQUIRE(state.commit_index == 1);
    const detlog::HardState expected_hard_state{1, 1};
    REQUIRE(state.hard_state == expected_hard_state);
  }

  const auto partial_path = directory / "partial.wal";
  run_child(executable, "partial", partial_path);
  {
    detlog::Wal recovered(partial_path, kIdentity);
    const auto state = recovered.state();
    REQUIRE(state.repaired_incomplete_tail);
    REQUIRE(state.entries.empty());
    REQUIRE(state.commit_index == 0);
    REQUIRE(state.last_frame_sequence == 0);
  }
}

}  // namespace

int main(int argc, char** argv) {
  try {
    if (argc == 4 && std::string_view(argv[1]) == "--child") {
      const std::string_view mode{argv[2]};
      const std::filesystem::path path{argv[3]};
      if (mode == "durable") durable_child(path);
      if (mode == "partial") partial_frame_child(path);
      return 94;
    }
    if (argc != 1) {
      throw std::invalid_argument("unexpected process-crash test arguments");
    }
    parent_test(std::filesystem::absolute(argv[0]));
    std::cout << "process_crash_tests: all checks passed\n";
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "process_crash_tests: " << error.what() << '\n';
    return 1;
  }
}
