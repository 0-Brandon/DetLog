#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <vector>

#include "detlog/model.hpp"

namespace detlog {

struct WalIdentity {
  std::uint64_t cluster_id_high{};
  std::uint64_t cluster_id_low{};
  NodeId node_id{};

  friend bool operator==(const WalIdentity&, const WalIdentity&) = default;
};

enum class WalFlushPolicy : std::uint8_t {
  // append() flushes its frame before returning. append_group() deliberately
  // shares one flush across the supplied frames and returns only afterwards.
  flush_every_append = 0,

  // Intended only for experiments that explicitly permit acknowledged data
  // loss. flush() may still be used to establish a durability barrier.
  unsafe_no_flush = 1,
};

struct WalOptions {
  WalFlushPolicy flush_policy{WalFlushPolicy::flush_every_append};
  std::size_t max_frame_bytes{16U * 1024U * 1024U};
  std::size_t max_group_bytes{64U * 1024U * 1024U};
  std::uint32_t max_group_frames{1024};
  std::uint32_t max_entries_per_frame{65536};
  std::uint32_t max_key_bytes{1U * 1024U * 1024U};
  std::uint32_t max_value_bytes{8U * 1024U * 1024U};
  std::uint64_t max_log_entries{10'000'000};
};

enum class WalErrorCode : std::uint8_t {
  io,
  format,
  checksum,
  identity_mismatch,
  invariant_violation,
  bounds_exceeded,
  failed,
};

class WalError final : public std::runtime_error {
 public:
  WalError(WalErrorCode code, std::string message);

  [[nodiscard]] WalErrorCode code() const noexcept { return code_; }

 private:
  WalErrorCode code_;
};

// This is both the result of recovery and the current logical state after
// successful appends. Entries are always indexed consecutively from one.
// durable_frame_sequence is a lower bound: unsafe writes can reach the medium
// without having passed an application-requested durability barrier.
struct WalState {
  HardState hard_state;
  std::vector<LogEntry> entries;
  LogIndex commit_index{};
  std::uint64_t last_frame_sequence{};
  std::uint64_t durable_frame_sequence{};
  std::uint64_t file_bytes{};
  bool repaired_incomplete_tail{};

  friend bool operator==(const WalState&, const WalState&) = default;
};

struct WalAppendResult {
  std::uint64_t first_frame_sequence{};
  std::uint64_t last_frame_sequence{};
  bool durable{};
  std::size_t bytes_written{};
};

// Byte-level codec seam used by the deterministic stable-storage simulator.
// It deliberately shares the production encoder and recovery scanner.
struct WalEncodedFrame {
  std::vector<std::byte> bytes;
  WalState state_after;
};

struct WalScanResult {
  WalState state;
  // A caller that supplied an incomplete tail must resize its byte image to
  // this prefix before appending more data.
  std::uint64_t valid_bytes{};
  bool has_incomplete_tail{};
};

[[nodiscard]] std::vector<std::byte> encode_wal_file_header(
    WalIdentity identity);

// The frame sequence is state_before.last_frame_sequence + 1. The returned
// state is the logical post-append state; its durable sequence is unchanged.
[[nodiscard]] WalEncodedFrame encode_wal_frame(
    const WalState& state_before, const PersistBatch& batch,
    const WalOptions& options = {});

// Scans a complete file image (header plus frames) without mutating it.
// Incomplete final frames are reported as a valid-prefix cut; all other format,
// checksum, identity, and logical-invariant failures throw WalError.
[[nodiscard]] WalScanResult scan_wal_bytes(
    std::span<const std::byte> bytes, WalIdentity expected_identity,
    const WalOptions& options = {});

// Wal owns one ordered writer. Public operations are serialized, so callers
// may submit from several threads without reordering frames. An I/O failure is
// fail-stop: the instance rejects every later mutation.
class Wal final {
 public:
  static constexpr std::uint16_t format_version = 1;

  Wal(std::filesystem::path path, WalIdentity identity,
      WalOptions options = {});
  ~Wal();

  Wal(const Wal&) = delete;
  Wal& operator=(const Wal&) = delete;
  Wal(Wal&&) = delete;
  Wal& operator=(Wal&&) = delete;

  [[nodiscard]] WalAppendResult append(const PersistBatch& batch);

  // Each batch remains an independently recoverable atomic frame. Under the
  // safe policy the group shares one flush, and no caller should expose any
  // completion until this method returns.
  [[nodiscard]] WalAppendResult append_group(
      std::span<const PersistBatch> batches);

  // Establishes a durability barrier for all frames written so far, including
  // frames written under unsafe_no_flush.
  void flush();

  [[nodiscard]] WalState state() const;
  [[nodiscard]] bool failed() const;
  [[nodiscard]] std::string failure_reason() const;
  [[nodiscard]] std::filesystem::path path() const;

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace detlog
