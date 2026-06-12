#include "detlog/wal.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace {

using detlog::ClientCommand;
using detlog::CommandKind;
using detlog::HardState;
using detlog::LogEntry;
using detlog::PersistBatch;
using detlog::Wal;
using detlog::WalError;
using detlog::WalErrorCode;
using detlog::WalFlushPolicy;
using detlog::WalIdentity;
using detlog::WalOptions;

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
    static std::uint64_t counter = 0;
    const auto stamp = std::chrono::steady_clock::now()
                           .time_since_epoch()
                           .count();
    path_ = std::filesystem::temp_directory_path() /
            ("detlog-wal-test-" + std::to_string(stamp) + "-" +
             std::to_string(++counter));
    std::filesystem::create_directories(path_);
  }

  ~TempDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path_, ignored);
  }

  [[nodiscard]] std::filesystem::path file() const { return path_ / "node.wal"; }

 private:
  std::filesystem::path path_;
};

constexpr WalIdentity kIdentity{0x1122334455667788ULL,
                                0x99aabbccddeeff00ULL, 1};

[[nodiscard]] ClientCommand command(std::uint64_t sequence, std::string key,
                                    std::string value) {
  return ClientCommand{{0xfeedfaceULL, 0xcafebeefULL}, sequence,
                       CommandKind::put, std::move(key), std::move(value)};
}

[[nodiscard]] LogEntry entry(std::uint64_t index, std::uint64_t term,
                             std::optional<ClientCommand> value = std::nullopt) {
  return LogEntry{index, term, std::move(value)};
}

template <typename Function>
void expect_error(WalErrorCode expected, Function&& function) {
  try {
    std::forward<Function>(function)();
  } catch (const WalError& error) {
    REQUIRE(error.code() == expected);
    return;
  }
  fail("expected WalError was not thrown", __LINE__);
}

void test_roundtrip_and_group_flush() {
  TempDirectory directory;
  const auto path = directory.file();
  PersistBatch first;
  first.hard_state = HardState{2, 1};
  first.entries.push_back(entry(1, 2, command(1, "alpha", "one")));
  PersistBatch second;
  second.entries.push_back(entry(2, 2));
  second.commit_index = 2;
  const std::vector batches{first, second};

  {
    Wal wal(path, kIdentity);
    const auto appended = wal.append_group(batches);
    REQUIRE(appended.first_frame_sequence == 1);
    REQUIRE(appended.last_frame_sequence == 2);
    REQUIRE(appended.durable);
    const auto state = wal.state();
    REQUIRE(state.hard_state == *first.hard_state);
    REQUIRE(state.entries ==
            std::vector<LogEntry>({first.entries[0], second.entries[0]}));
    REQUIRE(state.commit_index == 2);
    REQUIRE(state.last_frame_sequence == 2);
    REQUIRE(state.durable_frame_sequence == 2);
  }

  Wal recovered(path, kIdentity);
  const auto state = recovered.state();
  REQUIRE(state.entries ==
          std::vector<LogEntry>({first.entries[0], second.entries[0]}));
  REQUIRE(state.commit_index == 2);
  REQUIRE(state.last_frame_sequence == 2);
  REQUIRE(!state.repaired_incomplete_tail);
}

void test_logical_suffix_replacement() {
  TempDirectory directory;
  const auto path = directory.file();
  std::uint64_t size_before = 0;
  {
    Wal wal(path, kIdentity);
    PersistBatch original;
    original.hard_state = HardState{1, 1};
    original.entries = {entry(1, 1), entry(2, 1), entry(3, 1)};
    original.commit_index = 1;
    (void)wal.append(original);
    size_before = wal.state().file_bytes;

    PersistBatch replacement;
    replacement.hard_state = HardState{2, std::nullopt};
    replacement.truncate_from = 2;
    replacement.entries = {entry(2, 2, command(1, "key", "new")),
                           entry(3, 2)};
    replacement.commit_index = 3;
    (void)wal.append(replacement);
    const auto state = wal.state();
    REQUIRE(state.entries.size() == 3);
    REQUIRE(state.entries[1] == replacement.entries[0]);
    REQUIRE(state.entries[2] == replacement.entries[1]);
    REQUIRE(state.commit_index == 3);
    REQUIRE(state.file_bytes > size_before);
  }

  Wal recovered(path, kIdentity);
  REQUIRE(recovered.state().entries[1].term == 2);
  REQUIRE(recovered.state().commit_index == 3);
  REQUIRE(std::filesystem::file_size(path) > size_before);
}

void test_incomplete_tail_is_repaired() {
  TempDirectory directory;
  const auto path = directory.file();
  WalOptions unsafe;
  unsafe.flush_policy = WalFlushPolicy::unsafe_no_flush;
  std::uint64_t valid_size = 0;
  std::uint64_t full_size = 0;
  {
    Wal wal(path, kIdentity, unsafe);
    PersistBatch first;
    first.hard_state = HardState{1, 1};
    first.entries = {entry(1, 1)};
    (void)wal.append(first);
    wal.flush();
    valid_size = wal.state().file_bytes;

    PersistBatch unstable;
    unstable.entries = {entry(2, 1, command(1, "tail", "partial"))};
    (void)wal.append(unstable);
    full_size = wal.state().file_bytes;
    REQUIRE(full_size > valid_size + 8);
  }
  std::filesystem::resize_file(path, full_size - 7);

  Wal recovered(path, kIdentity);
  const auto state = recovered.state();
  REQUIRE(state.repaired_incomplete_tail);
  REQUIRE(state.entries.size() == 1);
  REQUIRE(state.last_frame_sequence == 1);
  REQUIRE(state.file_bytes == valid_size);
  REQUIRE(std::filesystem::file_size(path) == valid_size);
}

void test_complete_bad_checksum_is_fatal() {
  TempDirectory directory;
  const auto path = directory.file();
  {
    Wal wal(path, kIdentity);
    PersistBatch batch;
    batch.hard_state = HardState{1, 1};
    batch.entries = {entry(1, 1)};
    (void)wal.append(batch);
  }
  const auto file_size = std::filesystem::file_size(path);
  REQUIRE(file_size > 25);
  std::fstream file(path, std::ios::in | std::ios::out | std::ios::binary);
  REQUIRE(file.is_open());
  const auto position = static_cast<std::streamoff>(file_size - 25);
  file.seekg(position);
  char value = 0;
  file.read(&value, 1);
  REQUIRE(file.good());
  value = static_cast<char>(static_cast<unsigned char>(value) ^ 0x01U);
  file.seekp(position);
  file.write(&value, 1);
  file.flush();
  file.close();

  expect_error(WalErrorCode::checksum,
               [&] { Wal ignored(path, kIdentity); });
}

void test_identity_mismatch_is_fatal() {
  TempDirectory directory;
  const auto path = directory.file();
  { Wal wal(path, kIdentity); }
  const WalIdentity wrong_cluster{kIdentity.cluster_id_high + 1,
                                  kIdentity.cluster_id_low,
                                  kIdentity.node_id};
  expect_error(WalErrorCode::identity_mismatch,
               [&] { Wal ignored(path, wrong_cluster); });
  const WalIdentity wrong_node{kIdentity.cluster_id_high,
                               kIdentity.cluster_id_low, 2};
  expect_error(WalErrorCode::identity_mismatch,
               [&] { Wal ignored(path, wrong_node); });
}

void test_commit_and_vote_constraints() {
  TempDirectory directory;
  Wal wal(directory.file(), kIdentity);
  PersistBatch initial;
  initial.hard_state = HardState{1, 1};
  initial.entries = {entry(1, 1)};
  initial.commit_index = 1;
  (void)wal.append(initial);

  PersistBatch decrease;
  decrease.commit_index = 0;
  expect_error(WalErrorCode::invariant_violation,
               [&] { (void)wal.append(decrease); });
  PersistBatch past_end;
  past_end.commit_index = 2;
  expect_error(WalErrorCode::invariant_violation,
               [&] { (void)wal.append(past_end); });
  PersistBatch truncate_committed;
  truncate_committed.truncate_from = 1;
  expect_error(WalErrorCode::invariant_violation,
               [&] { (void)wal.append(truncate_committed); });
  PersistBatch switch_vote;
  switch_vote.hard_state = HardState{1, 2};
  expect_error(WalErrorCode::invariant_violation,
               [&] { (void)wal.append(switch_vote); });
  PersistBatch zero_term_vote;
  zero_term_vote.hard_state = HardState{0, 2};
  expect_error(WalErrorCode::invariant_violation,
               [&] {
                 (void)detlog::encode_wal_frame(detlog::WalState{},
                                                zero_term_vote);
               });
  REQUIRE(!wal.failed());

  PersistBatch valid;
  valid.entries = {entry(2, 1)};
  valid.commit_index = 2;
  (void)wal.append(valid);
  REQUIRE(wal.state().commit_index == 2);
}

void test_in_memory_codec_uses_production_scanner() {
  std::vector<std::byte> image = detlog::encode_wal_file_header(kIdentity);
  auto scanned = detlog::scan_wal_bytes(image, kIdentity);
  REQUIRE(scanned.state.file_bytes == image.size());
  PersistBatch first;
  first.hard_state = HardState{3, 1};
  first.entries = {entry(1, 3, command(1, "codec", "shared"))};
  first.commit_index = 1;
  auto encoded_first = detlog::encode_wal_frame(scanned.state, first);
  image.insert(image.end(), encoded_first.bytes.begin(),
               encoded_first.bytes.end());
  scanned = detlog::scan_wal_bytes(image, kIdentity);
  REQUIRE(scanned.state.entries == first.entries);
  REQUIRE(scanned.state.commit_index == 1);
  REQUIRE(scanned.state.last_frame_sequence == 1);

  PersistBatch second;
  second.entries = {entry(2, 3, command(2, "codec", "tail"))};
  second.commit_index = 2;
  auto encoded_second = detlog::encode_wal_frame(scanned.state, second);
  const std::size_t prior_prefix_size = image.size();

  // Every nonempty proper prefix of a valid final frame is an incomplete tail,
  // whether the cut lands in its header, payload, checksum, or trailer.
  for (std::size_t cut = 1; cut < encoded_second.bytes.size(); ++cut) {
    std::vector<std::byte> torn = image;
    torn.insert(torn.end(), encoded_second.bytes.begin(),
                encoded_second.bytes.begin() +
                    static_cast<std::ptrdiff_t>(cut));
    const auto recovered = detlog::scan_wal_bytes(torn, kIdentity);
    REQUIRE(recovered.has_incomplete_tail);
    REQUIRE(recovered.valid_bytes == prior_prefix_size);
    REQUIRE(recovered.state.entries == first.entries);
    REQUIRE(recovered.state.commit_index == 1);
    REQUIRE(recovered.state.last_frame_sequence == 1);
  }

  std::vector<std::byte> complete = image;
  complete.insert(complete.end(), encoded_second.bytes.begin(),
                  encoded_second.bytes.end());
  scanned = detlog::scan_wal_bytes(complete, kIdentity);
  REQUIRE(!scanned.has_incomplete_tail);
  REQUIRE(scanned.state.entries.size() == 2);
  REQUIRE(scanned.state.entries[1] == second.entries[0]);
  REQUIRE(scanned.state.commit_index == 2);
  REQUIRE(scanned.state.last_frame_sequence == 2);

  // A fully present frame with damaged contents is corruption, not a tail cut.
  std::vector<std::byte> corrupted = complete;
  REQUIRE(encoded_second.bytes.size() > 25);
  corrupted[corrupted.size() - 25] ^= std::byte{0x01};
  expect_error(WalErrorCode::checksum, [&] {
    (void)detlog::scan_wal_bytes(corrupted, kIdentity);
  });
}

}  // namespace

int main() {
  const std::vector<std::pair<std::string_view, std::function<void()>>> tests{
      {"roundtrip and group flush", test_roundtrip_and_group_flush},
      {"logical suffix replacement", test_logical_suffix_replacement},
      {"incomplete tail repair", test_incomplete_tail_is_repaired},
      {"complete checksum failure", test_complete_bad_checksum_is_fatal},
      {"identity mismatch", test_identity_mismatch_is_fatal},
      {"commit and vote constraints", test_commit_and_vote_constraints},
      {"shared in-memory codec", test_in_memory_codec_uses_production_scanner},
  };
  for (const auto& [name, test] : tests) {
    try {
      test();
      std::cout << "[pass] " << name << '\n';
    } catch (const std::exception& error) {
      std::cerr << "[fail] " << name << ": " << error.what() << '\n';
      return 1;
    }
  }
  return 0;
}
