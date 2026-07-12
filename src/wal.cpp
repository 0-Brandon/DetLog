#include "detlog/wal.hpp"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <limits>
#include <mutex>
#include <optional>
#include <span>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#else
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#endif

namespace detlog {
namespace {

using Bytes = std::vector<std::byte>;

constexpr std::string_view kFileMagic{"DTLGWAL1", 8};
constexpr std::string_view kFrameMagic{"DTLGFRM1", 8};
constexpr std::string_view kTrailerMagic{"DTLGEND1", 8};
constexpr std::size_t kFileHeaderBytes = 48;
constexpr std::size_t kFrameHeaderBytes = 48;
constexpr std::size_t kFrameTrailerBytes = 24;
constexpr std::size_t kBatchPrefixBytes = 8;
constexpr std::size_t kMinimumEntryPayloadBytes = 17;
constexpr std::size_t kMinimumFrameBytes =
    kFrameHeaderBytes + kBatchPrefixBytes + kFrameTrailerBytes;
constexpr std::uint32_t kFileHeaderTag = 0x31444857U;   // WHD1
constexpr std::uint32_t kFrameHeaderTag = 0x31444846U;  // FHD1
constexpr std::uint32_t kFrameTrailerTag = 0x31524c54U; // TLR1
constexpr std::uint32_t kHardStateFlag = 1U << 0U;
constexpr std::uint32_t kTruncateFlag = 1U << 1U;
constexpr std::uint32_t kCommitFlag = 1U << 2U;
constexpr std::uint32_t kKnownBatchFlags =
    kHardStateFlag | kTruncateFlag | kCommitFlag;
constexpr std::size_t kAbsoluteMaxFrameBytes = 256U * 1024U * 1024U;
constexpr std::size_t kAbsoluteMaxGroupBytes = 512U * 1024U * 1024U;
constexpr std::uint32_t kAbsoluteMaxGroupFrames = 65536;
constexpr std::uint32_t kAbsoluteMaxEntriesPerFrame = 1'000'000;
constexpr std::uint64_t kAbsoluteMaxLogEntries = 100'000'000;

[[nodiscard]] std::string with_offset(std::string message,
                                      std::uint64_t offset) {
  message += " at WAL offset ";
  message += std::to_string(offset);
  return message;
}

class ByteWriter {
 public:
  explicit ByteWriter(std::size_t limit) : limit_(limit) {}

  void u8(std::uint8_t value) {
    ensure(1);
    bytes_.push_back(static_cast<std::byte>(value));
  }

  void u16(std::uint16_t value) {
    ensure(2);
    for (unsigned shift = 0; shift < 16; shift += 8) {
      bytes_.push_back(static_cast<std::byte>((value >> shift) & 0xffU));
    }
  }

  void u32(std::uint32_t value) {
    ensure(4);
    for (unsigned shift = 0; shift < 32; shift += 8) {
      bytes_.push_back(static_cast<std::byte>((value >> shift) & 0xffU));
    }
  }

  void u64(std::uint64_t value) {
    ensure(8);
    for (unsigned shift = 0; shift < 64; shift += 8) {
      bytes_.push_back(static_cast<std::byte>((value >> shift) & 0xffU));
    }
  }

  void literal(std::string_view value) {
    ensure(value.size());
    for (const char character : value) {
      bytes_.push_back(
          static_cast<std::byte>(static_cast<unsigned char>(character)));
    }
  }

  void raw(std::span<const std::byte> value) {
    ensure(value.size());
    bytes_.insert(bytes_.end(), value.begin(), value.end());
  }

  void string(std::string_view value) {
    ensure(value.size());
    const auto* begin = reinterpret_cast<const std::byte*>(value.data());
    bytes_.insert(bytes_.end(), begin, begin + value.size());
  }

  [[nodiscard]] const Bytes& bytes() const noexcept { return bytes_; }
  [[nodiscard]] Bytes take() && { return std::move(bytes_); }

 private:
  void ensure(std::size_t amount) const {
    if (amount > limit_ - std::min(limit_, bytes_.size())) {
      throw WalError(WalErrorCode::bounds_exceeded,
                     "encoded WAL object exceeds configured size bound");
    }
  }

  std::size_t limit_;
  Bytes bytes_;
};

class ByteReader {
 public:
  explicit ByteReader(std::span<const std::byte> bytes) : bytes_(bytes) {}

  [[nodiscard]] std::uint8_t u8() {
    require(1);
    return std::to_integer<std::uint8_t>(bytes_[position_++]);
  }

  [[nodiscard]] std::uint16_t u16() {
    std::uint32_t value = 0;
    for (unsigned shift = 0; shift < 16; shift += 8) {
      value |= static_cast<std::uint32_t>(u8()) << shift;
    }
    return static_cast<std::uint16_t>(value);
  }

  [[nodiscard]] std::uint32_t u32() {
    std::uint32_t value = 0;
    for (unsigned shift = 0; shift < 32; shift += 8) {
      value |= static_cast<std::uint32_t>(u8()) << shift;
    }
    return value;
  }

  [[nodiscard]] std::uint64_t u64() {
    std::uint64_t value = 0;
    for (unsigned shift = 0; shift < 64; shift += 8) {
      value |= static_cast<std::uint64_t>(u8()) << shift;
    }
    return value;
  }

  void expect_literal(std::string_view expected, std::string_view name) {
    require(expected.size());
    for (const char character : expected) {
      if (u8() != static_cast<std::uint8_t>(character)) {
        throw WalError(WalErrorCode::format,
                       std::string("invalid ") + std::string(name));
      }
    }
  }

  [[nodiscard]] std::string string(std::size_t size) {
    require(size);
    const auto* begin =
        reinterpret_cast<const char*>(bytes_.data() + position_);
    std::string value(begin, size);
    position_ += size;
    return value;
  }

  [[nodiscard]] std::size_t remaining() const noexcept {
    return bytes_.size() - position_;
  }

  [[nodiscard]] bool empty() const noexcept { return remaining() == 0; }

 private:
  void require(std::size_t amount) const {
    if (amount > remaining()) {
      throw WalError(WalErrorCode::format,
                     "declared WAL object is internally truncated");
    }
  }

  std::span<const std::byte> bytes_;
  std::size_t position_{};
};

[[nodiscard]] std::uint32_t crc32c(std::span<const std::byte> bytes) {
  std::uint32_t crc = 0xffffffffU;
  for (const std::byte byte : bytes) {
    crc ^= std::to_integer<std::uint8_t>(byte);
    for (unsigned bit = 0; bit < 8; ++bit) {
      const std::uint32_t mask =
          0U - static_cast<std::uint32_t>(crc & 1U);
      crc = (crc >> 1U) ^ (0x82f63b78U & mask);
    }
  }
  return ~crc;
}

void validate_options(const WalOptions& options) {
  if (options.flush_policy != WalFlushPolicy::flush_every_append &&
      options.flush_policy != WalFlushPolicy::unsafe_no_flush) {
    throw WalError(WalErrorCode::invariant_violation,
                   "unknown WAL flush policy");
  }
  if (options.max_frame_bytes < kMinimumFrameBytes ||
      options.max_frame_bytes > kAbsoluteMaxFrameBytes) {
    throw WalError(WalErrorCode::bounds_exceeded,
                   "max_frame_bytes is outside the supported range");
  }
  if (options.max_group_bytes < options.max_frame_bytes ||
      options.max_group_bytes > kAbsoluteMaxGroupBytes) {
    throw WalError(WalErrorCode::bounds_exceeded,
                   "max_group_bytes is outside the supported range");
  }
  if (options.max_group_frames == 0 ||
      options.max_group_frames > kAbsoluteMaxGroupFrames) {
    throw WalError(WalErrorCode::bounds_exceeded,
                   "max_group_frames is outside the supported range");
  }
  if (options.max_entries_per_frame == 0 ||
      options.max_entries_per_frame > kAbsoluteMaxEntriesPerFrame) {
    throw WalError(WalErrorCode::bounds_exceeded,
                   "max_entries_per_frame is outside the supported range");
  }
  if (options.max_key_bytes > kAbsoluteMaxFrameBytes ||
      options.max_value_bytes > kAbsoluteMaxFrameBytes) {
    throw WalError(WalErrorCode::bounds_exceeded,
                   "command field bound exceeds the absolute frame bound");
  }
  if (options.max_log_entries == 0 ||
      options.max_log_entries > kAbsoluteMaxLogEntries) {
    throw WalError(WalErrorCode::bounds_exceeded,
                   "max_log_entries is outside the supported range");
  }
}

[[nodiscard]] bool valid_command_kind(CommandKind kind) noexcept {
  return kind == CommandKind::put || kind == CommandKind::erase ||
         kind == CommandKind::get;
}

class ValidationView {
 public:
  explicit ValidationView(const WalState& state)
      : hard_state_(state.hard_state),
        commit_index_(state.commit_index),
        base_entries_(&state.entries),
        base_keep_(state.entries.size()) {}

  void stage(const PersistBatch& batch, const WalOptions& options) {
    if (batch.empty()) {
      throw WalError(WalErrorCode::invariant_violation,
                     "an empty persistence batch is not a WAL frame");
    }
    if (batch.entries.size() > options.max_entries_per_frame) {
      throw WalError(WalErrorCode::bounds_exceeded,
                     "persistence batch has too many entries");
    }

    if (batch.hard_state) {
      const HardState& next = *batch.hard_state;
      if (next.current_term == 0 && next.voted_for) {
        throw WalError(WalErrorCode::invariant_violation,
                       "term-zero hard state cannot contain a vote");
      }
      if (next.current_term < hard_state_.current_term) {
        throw WalError(WalErrorCode::invariant_violation,
                       "hard-state term would decrease");
      }
      if (next.current_term == hard_state_.current_term &&
          hard_state_.voted_for &&
          next.voted_for != hard_state_.voted_for) {
        throw WalError(WalErrorCode::invariant_violation,
                       "vote would change or clear within one term");
      }
      hard_state_ = next;
    }

    if (batch.truncate_from) {
      const LogIndex from = *batch.truncate_from;
      if (from == 0) {
        throw WalError(WalErrorCode::invariant_violation,
                       "truncate_from index zero is invalid");
      }
      if (from <= commit_index_) {
        throw WalError(WalErrorCode::invariant_violation,
                       "cannot truncate a committed log entry");
      }
      const LogIndex current_last = last_index();
      if (from > current_last + 1U) {
        throw WalError(WalErrorCode::invariant_violation,
                       "truncate_from is beyond the end of the log");
      }
      const std::size_t wanted = static_cast<std::size_t>(from - 1U);
      if (wanted <= base_keep_) {
        base_keep_ = wanted;
        suffix_.clear();
      } else {
        suffix_.resize(wanted - base_keep_);
      }
    }

    for (const LogEntry& entry : batch.entries) {
      if (last_index() >= options.max_log_entries) {
        throw WalError(WalErrorCode::bounds_exceeded,
                       "logical WAL exceeds max_log_entries");
      }
      const LogIndex expected = last_index() + 1U;
      if (entry.index != expected) {
        throw WalError(WalErrorCode::invariant_violation,
                       "log entries are not consecutive");
      }
      if (entry.term == 0 || entry.term > hard_state_.current_term) {
        throw WalError(WalErrorCode::invariant_violation,
                       "log entry term is zero or exceeds current term");
      }
      if (entry.index > 1 && entry.term < last_term()) {
        throw WalError(WalErrorCode::invariant_violation,
                       "log entry terms decrease with index");
      }
      if (entry.command) {
        if (!valid_command_kind(entry.command->kind)) {
          throw WalError(WalErrorCode::invariant_violation,
                         "log entry has an unknown command kind");
        }
        if (entry.command->key.size() > options.max_key_bytes ||
            entry.command->value.size() > options.max_value_bytes) {
          throw WalError(WalErrorCode::bounds_exceeded,
                         "command field exceeds configured size bound");
        }
      }
      suffix_.push_back(entry);
    }

    if (batch.commit_index) {
      if (*batch.commit_index < commit_index_) {
        throw WalError(WalErrorCode::invariant_violation,
                       "commit index would decrease");
      }
      if (*batch.commit_index > last_index()) {
        throw WalError(WalErrorCode::invariant_violation,
                       "commit index exceeds the end of the log");
      }
      commit_index_ = *batch.commit_index;
    }
  }

  [[nodiscard]] std::size_t entry_count() const noexcept {
    return base_keep_ + suffix_.size();
  }

  void materialize(WalState& state) {
    state.hard_state = hard_state_;
    state.commit_index = commit_index_;
    state.entries.resize(base_keep_);
    state.entries.insert(state.entries.end(),
                         std::make_move_iterator(suffix_.begin()),
                         std::make_move_iterator(suffix_.end()));
  }

 private:
  [[nodiscard]] LogIndex last_index() const noexcept {
    return static_cast<LogIndex>(base_keep_) +
           static_cast<LogIndex>(suffix_.size());
  }

  [[nodiscard]] Term last_term() const noexcept {
    if (!suffix_.empty()) {
      return suffix_.back().term;
    }
    if (base_keep_ != 0) {
      return (*base_entries_)[base_keep_ - 1U].term;
    }
    return 0;
  }

  HardState hard_state_;
  LogIndex commit_index_{};
  const std::vector<LogEntry>* base_entries_;
  std::size_t base_keep_{};
  std::vector<LogEntry> suffix_;
};

void validate_state_summary(const WalState& state,
                            const WalOptions& options) {
  if (state.entries.size() > options.max_log_entries) {
    throw WalError(WalErrorCode::bounds_exceeded,
                   "WAL state exceeds max_log_entries");
  }
  if (state.hard_state.current_term == 0 && state.hard_state.voted_for) {
    throw WalError(WalErrorCode::invariant_violation,
                   "term-zero WAL state cannot contain a vote");
  }
  if (!state.entries.empty()) {
    if (state.entries.front().index != 1 ||
        state.entries.back().index != state.entries.size()) {
      throw WalError(WalErrorCode::invariant_violation,
                     "WAL state does not use consecutive one-based indexes");
    }
    if (state.entries.back().term > state.hard_state.current_term) {
      throw WalError(WalErrorCode::invariant_violation,
                     "WAL state log term exceeds hard-state term");
    }
  }
  if (state.commit_index > state.entries.size()) {
    throw WalError(WalErrorCode::invariant_violation,
                   "WAL state commit index exceeds its log");
  }
  if (state.durable_frame_sequence > state.last_frame_sequence) {
    throw WalError(WalErrorCode::invariant_violation,
                   "durable frame sequence exceeds last frame sequence");
  }
}

[[nodiscard]] Bytes encode_batch_payload(const PersistBatch& batch,
                                         const WalOptions& options) {
  const std::size_t payload_limit =
      options.max_frame_bytes - kFrameHeaderBytes - kFrameTrailerBytes;
  ByteWriter writer(payload_limit);
  std::uint32_t flags = 0;
  if (batch.hard_state) flags |= kHardStateFlag;
  if (batch.truncate_from) flags |= kTruncateFlag;
  if (batch.commit_index) flags |= kCommitFlag;
  writer.u32(flags);
  writer.u32(static_cast<std::uint32_t>(batch.entries.size()));
  if (batch.hard_state) {
    writer.u64(batch.hard_state->current_term);
    writer.u8(batch.hard_state->voted_for ? 1U : 0U);
    if (batch.hard_state->voted_for) {
      writer.u32(*batch.hard_state->voted_for);
    }
  }
  if (batch.truncate_from) writer.u64(*batch.truncate_from);
  for (const LogEntry& entry : batch.entries) {
    writer.u64(entry.index);
    writer.u64(entry.term);
    writer.u8(entry.command ? 1U : 0U);
    if (entry.command) {
      writer.u64(entry.command->session.high);
      writer.u64(entry.command->session.low);
      writer.u64(entry.command->sequence);
      writer.u8(static_cast<std::uint8_t>(entry.command->kind));
      writer.u32(static_cast<std::uint32_t>(entry.command->key.size()));
      writer.u32(static_cast<std::uint32_t>(entry.command->value.size()));
      writer.string(entry.command->key);
      writer.string(entry.command->value);
    }
  }
  if (batch.commit_index) writer.u64(*batch.commit_index);
  return std::move(writer).take();
}

[[nodiscard]] PersistBatch decode_batch_payload(
    std::span<const std::byte> payload, const WalOptions& options) {
  ByteReader reader(payload);
  const std::uint32_t flags = reader.u32();
  if ((flags & ~kKnownBatchFlags) != 0) {
    throw WalError(WalErrorCode::format,
                   "WAL batch contains unknown flags");
  }
  const std::uint32_t count = reader.u32();
  if (count > options.max_entries_per_frame) {
    throw WalError(WalErrorCode::bounds_exceeded,
                   "WAL frame declares too many entries");
  }
  PersistBatch batch;
  if ((flags & kHardStateFlag) != 0) {
    HardState hard;
    hard.current_term = reader.u64();
    const std::uint8_t has_vote = reader.u8();
    if (has_vote > 1) {
      throw WalError(WalErrorCode::format,
                     "invalid voted-for presence marker");
    }
    if (has_vote != 0) hard.voted_for = reader.u32();
    batch.hard_state = hard;
  }
  if ((flags & kTruncateFlag) != 0) batch.truncate_from = reader.u64();
  const std::size_t trailing_bytes =
      (flags & kCommitFlag) != 0 ? std::size_t{8} : std::size_t{0};
  if (reader.remaining() < trailing_bytes ||
      static_cast<std::size_t>(count) >
          (reader.remaining() - trailing_bytes) / kMinimumEntryPayloadBytes) {
    throw WalError(WalErrorCode::format,
                   "WAL entry count exceeds the remaining frame payload");
  }
  batch.entries.reserve(count);
  for (std::uint32_t number = 0; number < count; ++number) {
    LogEntry entry;
    entry.index = reader.u64();
    entry.term = reader.u64();
    const std::uint8_t has_command = reader.u8();
    if (has_command > 1) {
      throw WalError(WalErrorCode::format,
                     "invalid command presence marker");
    }
    if (has_command != 0) {
      ClientCommand command;
      command.session.high = reader.u64();
      command.session.low = reader.u64();
      command.sequence = reader.u64();
      command.kind = static_cast<CommandKind>(reader.u8());
      if (!valid_command_kind(command.kind)) {
        throw WalError(WalErrorCode::format,
                       "WAL frame contains unknown command kind");
      }
      const std::uint32_t key_size = reader.u32();
      const std::uint32_t value_size = reader.u32();
      if (key_size > options.max_key_bytes ||
          value_size > options.max_value_bytes) {
        throw WalError(WalErrorCode::bounds_exceeded,
                       "WAL command field exceeds configured size bound");
      }
      command.key = reader.string(key_size);
      command.value = reader.string(value_size);
      entry.command = std::move(command);
    }
    batch.entries.push_back(std::move(entry));
  }
  if ((flags & kCommitFlag) != 0) batch.commit_index = reader.u64();
  if (!reader.empty()) {
    throw WalError(WalErrorCode::format,
                   "WAL batch contains trailing bytes");
  }
  return batch;
}

[[nodiscard]] Bytes make_file_header(WalIdentity identity) {
  ByteWriter writer(kFileHeaderBytes);
  writer.literal(kFileMagic);
  writer.u16(Wal::format_version);
  writer.u16(static_cast<std::uint16_t>(kFileHeaderBytes));
  writer.u32(0);
  writer.u64(identity.cluster_id_high);
  writer.u64(identity.cluster_id_low);
  writer.u32(identity.node_id);
  writer.u32(0);
  const std::uint32_t checksum = crc32c(writer.bytes());
  writer.u32(checksum);
  writer.u32(kFileHeaderTag);
  return std::move(writer).take();
}

void validate_file_header(std::span<const std::byte> bytes,
                          WalIdentity expected) {
  if (bytes.size() != kFileHeaderBytes) {
    throw WalError(WalErrorCode::format,
                   "WAL file header has the wrong size");
  }
  ByteReader reader(bytes);
  reader.expect_literal(kFileMagic, "WAL file magic");
  if (reader.u16() != Wal::format_version) {
    throw WalError(WalErrorCode::format,
                   "unsupported WAL format version");
  }
  if (reader.u16() != kFileHeaderBytes || reader.u32() != 0) {
    throw WalError(WalErrorCode::format,
                   "invalid WAL file header fields");
  }
  WalIdentity actual;
  actual.cluster_id_high = reader.u64();
  actual.cluster_id_low = reader.u64();
  actual.node_id = reader.u32();
  if (reader.u32() != 0) {
    throw WalError(WalErrorCode::format,
                   "nonzero reserved WAL header field");
  }
  const std::uint32_t stored_checksum = reader.u32();
  if (reader.u32() != kFileHeaderTag) {
    throw WalError(WalErrorCode::format,
                   "invalid WAL file header trailer");
  }
  if (stored_checksum != crc32c(bytes.first(40))) {
    throw WalError(WalErrorCode::checksum,
                   "WAL file header checksum mismatch");
  }
  if (actual != expected) {
    throw WalError(WalErrorCode::identity_mismatch,
                   "WAL cluster or node identity mismatch");
  }
}

[[nodiscard]] Bytes make_frame(std::uint64_t sequence,
                               const PersistBatch& batch,
                               const WalOptions& options) {
  Bytes payload = encode_batch_payload(batch, options);
  const std::size_t total_size =
      kFrameHeaderBytes + payload.size() + kFrameTrailerBytes;
  if (total_size > options.max_frame_bytes) {
    throw WalError(WalErrorCode::bounds_exceeded,
                   "WAL frame exceeds configured size bound");
  }
  ByteWriter writer(total_size);
  writer.literal(kFrameMagic);
  writer.u16(Wal::format_version);
  writer.u16(static_cast<std::uint16_t>(kFrameHeaderBytes));
  writer.u32(0);
  writer.u64(total_size);
  writer.u64(payload.size());
  writer.u64(sequence);
  const std::uint32_t header_checksum = crc32c(writer.bytes());
  writer.u32(header_checksum);
  writer.u32(kFrameHeaderTag);
  writer.raw(payload);
  const std::uint32_t frame_checksum = crc32c(writer.bytes());
  writer.u32(frame_checksum);
  writer.u32(kFrameTrailerTag);
  writer.u64(total_size);
  writer.literal(kTrailerMagic);
  return std::move(writer).take();
}

struct ParsedFrameHeader {
  std::uint64_t frame_size{};
  std::uint64_t payload_size{};
  std::uint64_t sequence{};
};

[[nodiscard]] ParsedFrameHeader parse_frame_header(
    std::span<const std::byte> bytes, const WalOptions& options) {
  ByteReader reader(bytes);
  reader.expect_literal(kFrameMagic, "WAL frame magic");
  if (reader.u16() != Wal::format_version) {
    throw WalError(WalErrorCode::format,
                   "unsupported WAL frame version");
  }
  if (reader.u16() != kFrameHeaderBytes || reader.u32() != 0) {
    throw WalError(WalErrorCode::format,
                   "invalid WAL frame header fields");
  }
  ParsedFrameHeader header;
  header.frame_size = reader.u64();
  header.payload_size = reader.u64();
  header.sequence = reader.u64();
  const std::uint32_t stored_checksum = reader.u32();
  if (reader.u32() != kFrameHeaderTag) {
    throw WalError(WalErrorCode::format,
                   "invalid WAL frame header trailer");
  }
  if (stored_checksum != crc32c(bytes.first(40))) {
    throw WalError(WalErrorCode::checksum,
                   "WAL frame header checksum mismatch");
  }
  if (header.frame_size < kMinimumFrameBytes ||
      header.frame_size > options.max_frame_bytes ||
      header.frame_size > std::numeric_limits<std::size_t>::max()) {
    throw WalError(WalErrorCode::bounds_exceeded,
                   "WAL frame length is outside the configured bound");
  }
  if (header.payload_size > options.max_frame_bytes ||
      header.payload_size + kFrameHeaderBytes + kFrameTrailerBytes !=
          header.frame_size) {
    throw WalError(WalErrorCode::format,
                   "inconsistent WAL frame and payload lengths");
  }
  return header;
}

[[nodiscard]] PersistBatch validate_complete_frame(
    std::span<const std::byte> frame, const ParsedFrameHeader& header,
    const WalOptions& options) {
  const std::size_t trailer_offset =
      kFrameHeaderBytes + static_cast<std::size_t>(header.payload_size);
  ByteReader trailer(frame.subspan(trailer_offset, kFrameTrailerBytes));
  const std::uint32_t stored_checksum = trailer.u32();
  if (trailer.u32() != kFrameTrailerTag ||
      trailer.u64() != header.frame_size) {
    throw WalError(WalErrorCode::format,
                   "invalid WAL frame trailer fields");
  }
  trailer.expect_literal(kTrailerMagic, "WAL frame trailer magic");
  if (stored_checksum != crc32c(frame.first(trailer_offset))) {
    throw WalError(WalErrorCode::checksum,
                   "WAL frame checksum mismatch");
  }
  return decode_batch_payload(
      frame.subspan(kFrameHeaderBytes,
                    static_cast<std::size_t>(header.payload_size)),
      options);
}

template <typename ReadExact>
[[nodiscard]] WalScanResult scan_source(std::uint64_t byte_count,
                                        ReadExact&& read_exact,
                                        WalIdentity expected_identity,
                                        const WalOptions& options) {
  validate_options(options);
  if (byte_count < kFileHeaderBytes) {
    throw WalError(WalErrorCode::format,
                   "WAL is missing a complete file header");
  }
  std::array<std::byte, kFileHeaderBytes> file_header{};
  read_exact(0, file_header);
  validate_file_header(file_header, expected_identity);

  WalScanResult result;
  result.valid_bytes = kFileHeaderBytes;
  result.state.file_bytes = kFileHeaderBytes;
  std::uint64_t offset = kFileHeaderBytes;
  while (offset < byte_count) {
    const std::uint64_t remaining = byte_count - offset;
    if (remaining < kFrameHeaderBytes) {
      result.has_incomplete_tail = true;
      break;
    }
    std::array<std::byte, kFrameHeaderBytes> header_bytes{};
    read_exact(offset, header_bytes);
    ParsedFrameHeader header;
    try {
      header = parse_frame_header(header_bytes, options);
    } catch (const WalError& error) {
      throw WalError(error.code(), with_offset(error.what(), offset));
    }
    if (header.sequence != result.state.last_frame_sequence + 1U) {
      throw WalError(WalErrorCode::invariant_violation,
                     with_offset("WAL frame sequence is not consecutive",
                                 offset));
    }
    if (header.frame_size > remaining) {
      result.has_incomplete_tail = true;
      break;
    }
    Bytes frame(static_cast<std::size_t>(header.frame_size));
    read_exact(offset, frame);
    PersistBatch batch;
    try {
      batch = validate_complete_frame(frame, header, options);
      ValidationView view(result.state);
      view.stage(batch, options);
      result.state.entries.reserve(view.entry_count());
      view.materialize(result.state);
    } catch (const WalError& error) {
      throw WalError(error.code(), with_offset(error.what(), offset));
    }
    result.state.last_frame_sequence = header.sequence;
    offset += header.frame_size;
    result.valid_bytes = offset;
    result.state.file_bytes = offset;
  }
  result.state.durable_frame_sequence = result.state.last_frame_sequence;
  result.state.repaired_incomplete_tail = result.has_incomplete_tail;
  return result;
}

[[nodiscard]] std::string io_message(std::string_view operation,
                                     const std::filesystem::path& path,
                                     std::uint64_t native_error) {
  std::ostringstream message;
  message << operation << " '" << path.string() << "' failed (error "
          << native_error << ')';
  return message.str();
}

class NativeFile {
 public:
  explicit NativeFile(std::filesystem::path path) : path_(std::move(path)) {
#ifdef _WIN32
    handle_ = CreateFileW(path_.c_str(), GENERIC_READ | GENERIC_WRITE, 0,
                          nullptr, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, nullptr);
    if (handle_ == INVALID_HANDLE_VALUE) {
      throw WalError(WalErrorCode::io,
                     io_message("open", path_, GetLastError()));
    }
    created_ = GetLastError() != ERROR_ALREADY_EXISTS;
#else
    int flags = O_RDWR | O_CREAT | O_EXCL;
#ifdef O_CLOEXEC
    flags |= O_CLOEXEC;
#endif
    descriptor_ = ::open(path_.c_str(), flags, 0644);
    if (descriptor_ >= 0) {
      created_ = true;
    } else if (errno == EEXIST) {
      flags = O_RDWR;
#ifdef O_CLOEXEC
      flags |= O_CLOEXEC;
#endif
      descriptor_ = ::open(path_.c_str(), flags);
    }
    if (descriptor_ < 0) {
      throw WalError(WalErrorCode::io,
                     io_message("open", path_, errno));
    }
#endif
  }

  ~NativeFile() {
#ifdef _WIN32
    if (handle_ != INVALID_HANDLE_VALUE) CloseHandle(handle_);
#else
    if (descriptor_ >= 0) ::close(descriptor_);
#endif
  }

  NativeFile(const NativeFile&) = delete;
  NativeFile& operator=(const NativeFile&) = delete;

  [[nodiscard]] bool created() const noexcept { return created_; }

  [[nodiscard]] std::uint64_t size() const {
#ifdef _WIN32
    LARGE_INTEGER value{};
    if (!GetFileSizeEx(handle_, &value) || value.QuadPart < 0) {
      throw WalError(WalErrorCode::io,
                     io_message("size", path_, GetLastError()));
    }
    return static_cast<std::uint64_t>(value.QuadPart);
#else
    struct stat status {};
    if (::fstat(descriptor_, &status) != 0 || status.st_size < 0) {
      throw WalError(WalErrorCode::io, io_message("size", path_, errno));
    }
    return static_cast<std::uint64_t>(status.st_size);
#endif
  }

  void read_exact(std::uint64_t offset, std::span<std::byte> destination) {
    std::size_t done = 0;
    while (done < destination.size()) {
#ifdef _WIN32
      const std::uint64_t position = offset + done;
      if (position > static_cast<std::uint64_t>(
                         std::numeric_limits<LONGLONG>::max())) {
        throw WalError(WalErrorCode::io, "WAL read offset is too large");
      }
      LARGE_INTEGER target{};
      target.QuadPart = static_cast<LONGLONG>(position);
      if (!SetFilePointerEx(handle_, target, nullptr, FILE_BEGIN)) {
        throw WalError(WalErrorCode::io,
                       io_message("seek", path_, GetLastError()));
      }
      const DWORD amount = static_cast<DWORD>(std::min<std::size_t>(
          destination.size() - done, std::numeric_limits<DWORD>::max()));
      DWORD read = 0;
      if (!ReadFile(handle_, destination.data() + done, amount, &read,
                    nullptr)) {
        throw WalError(WalErrorCode::io,
                       io_message("read", path_, GetLastError()));
      }
      if (read == 0) {
        throw WalError(WalErrorCode::io, "unexpected EOF while reading WAL");
      }
      done += read;
#else
      const std::uint64_t position = offset + done;
      if (position >
          static_cast<std::uint64_t>(std::numeric_limits<off_t>::max())) {
        throw WalError(WalErrorCode::io, "WAL read offset is too large");
      }
      const std::size_t amount = std::min<std::size_t>(
          destination.size() - done,
          static_cast<std::size_t>(std::numeric_limits<ssize_t>::max()));
      const ssize_t read = ::pread(descriptor_, destination.data() + done,
                                   amount, static_cast<off_t>(position));
      if (read < 0) {
        if (errno == EINTR) continue;
        throw WalError(WalErrorCode::io, io_message("read", path_, errno));
      }
      if (read == 0) {
        throw WalError(WalErrorCode::io, "unexpected EOF while reading WAL");
      }
      done += static_cast<std::size_t>(read);
#endif
    }
  }

  void write_exact(std::uint64_t offset,
                   std::span<const std::byte> source) {
    std::size_t done = 0;
    while (done < source.size()) {
#ifdef _WIN32
      const std::uint64_t position = offset + done;
      if (position > static_cast<std::uint64_t>(
                         std::numeric_limits<LONGLONG>::max())) {
        throw WalError(WalErrorCode::io, "WAL write offset is too large");
      }
      LARGE_INTEGER target{};
      target.QuadPart = static_cast<LONGLONG>(position);
      if (!SetFilePointerEx(handle_, target, nullptr, FILE_BEGIN)) {
        throw WalError(WalErrorCode::io,
                       io_message("seek", path_, GetLastError()));
      }
      const DWORD amount = static_cast<DWORD>(std::min<std::size_t>(
          source.size() - done, std::numeric_limits<DWORD>::max()));
      DWORD written = 0;
      if (!WriteFile(handle_, source.data() + done, amount, &written,
                     nullptr)) {
        throw WalError(WalErrorCode::io,
                       io_message("write", path_, GetLastError()));
      }
      if (written == 0) {
        throw WalError(WalErrorCode::io, "zero-length WAL write");
      }
      done += written;
#else
      const std::uint64_t position = offset + done;
      if (position >
          static_cast<std::uint64_t>(std::numeric_limits<off_t>::max())) {
        throw WalError(WalErrorCode::io, "WAL write offset is too large");
      }
      const std::size_t amount = std::min<std::size_t>(
          source.size() - done,
          static_cast<std::size_t>(std::numeric_limits<ssize_t>::max()));
      const ssize_t written = ::pwrite(descriptor_, source.data() + done,
                                       amount, static_cast<off_t>(position));
      if (written < 0) {
        if (errno == EINTR) continue;
        throw WalError(WalErrorCode::io, io_message("write", path_, errno));
      }
      if (written == 0) {
        throw WalError(WalErrorCode::io, "zero-length WAL write");
      }
      done += static_cast<std::size_t>(written);
#endif
    }
  }

  void flush() {
#ifdef _WIN32
    if (!FlushFileBuffers(handle_)) {
      throw WalError(WalErrorCode::io,
                     io_message("flush", path_, GetLastError()));
    }
#else
    if (::fsync(descriptor_) != 0) {
      throw WalError(WalErrorCode::io, io_message("flush", path_, errno));
    }
#endif
  }

  void truncate(std::uint64_t length) {
#ifdef _WIN32
    if (length > static_cast<std::uint64_t>(
                     std::numeric_limits<LONGLONG>::max())) {
      throw WalError(WalErrorCode::io, "WAL truncate offset is too large");
    }
    LARGE_INTEGER target{};
    target.QuadPart = static_cast<LONGLONG>(length);
    if (!SetFilePointerEx(handle_, target, nullptr, FILE_BEGIN) ||
        !SetEndOfFile(handle_)) {
      throw WalError(WalErrorCode::io,
                     io_message("truncate", path_, GetLastError()));
    }
#else
    if (length >
        static_cast<std::uint64_t>(std::numeric_limits<off_t>::max()) ||
        ::ftruncate(descriptor_, static_cast<off_t>(length)) != 0) {
      throw WalError(WalErrorCode::io,
                     io_message("truncate", path_, errno));
    }
#endif
  }

  void sync_parent_directory() {
#ifndef _WIN32
    std::filesystem::path parent = path_.parent_path();
    if (parent.empty()) parent = ".";
    int flags = O_RDONLY;
#ifdef O_DIRECTORY
    flags |= O_DIRECTORY;
#endif
#ifdef O_CLOEXEC
    flags |= O_CLOEXEC;
#endif
    const int directory = ::open(parent.c_str(), flags);
    if (directory < 0) {
      throw WalError(WalErrorCode::io,
                     io_message("open directory", parent, errno));
    }
    const int result = ::fsync(directory);
    const int saved_errno = errno;
    ::close(directory);
    if (result != 0) {
      throw WalError(WalErrorCode::io,
                     io_message("flush directory", parent, saved_errno));
    }
#endif
  }

 private:
  std::filesystem::path path_;
  bool created_{};
#ifdef _WIN32
  HANDLE handle_{INVALID_HANDLE_VALUE};
#else
  int descriptor_{-1};
#endif
};

}  // namespace

WalError::WalError(WalErrorCode code, std::string message)
    : std::runtime_error(std::move(message)), code_(code) {}

std::vector<std::byte> encode_wal_file_header(WalIdentity identity) {
  return make_file_header(identity);
}

WalEncodedFrame encode_wal_frame(const WalState& state_before,
                                 const PersistBatch& batch,
                                 const WalOptions& options) {
  validate_options(options);
  validate_state_summary(state_before, options);
  if (state_before.last_frame_sequence ==
      std::numeric_limits<std::uint64_t>::max()) {
    throw WalError(WalErrorCode::bounds_exceeded,
                   "WAL frame sequence is exhausted");
  }
  ValidationView view(state_before);
  view.stage(batch, options);
  WalEncodedFrame result;
  result.bytes = make_frame(state_before.last_frame_sequence + 1U, batch,
                            options);
  result.state_after = state_before;
  result.state_after.entries.reserve(view.entry_count());
  view.materialize(result.state_after);
  result.state_after.last_frame_sequence += 1U;
  if (result.bytes.size() >
      std::numeric_limits<std::uint64_t>::max() -
          result.state_after.file_bytes) {
    throw WalError(WalErrorCode::bounds_exceeded,
                   "WAL file length would overflow");
  }
  result.state_after.file_bytes += result.bytes.size();
  return result;
}

WalScanResult scan_wal_bytes(std::span<const std::byte> bytes,
                             WalIdentity expected_identity,
                             const WalOptions& options) {
  auto read = [bytes](std::uint64_t offset,
                      std::span<std::byte> destination) {
    if (offset > bytes.size() || destination.size() > bytes.size() - offset) {
      throw WalError(WalErrorCode::format,
                     "WAL byte source ended unexpectedly");
    }
    std::memcpy(destination.data(), bytes.data() + offset,
                destination.size());
  };
  return scan_source(bytes.size(), read, expected_identity, options);
}

class Wal::Impl {
 public:
  Impl(std::filesystem::path path, WalIdentity identity, WalOptions options)
      : path_(std::move(path)),
        identity_(identity),
        options_(options),
        file_(path_) {
    validate_options(options_);
    const std::uint64_t byte_count = file_.size();
    if (byte_count == 0) {
      const Bytes header = make_file_header(identity_);
      file_.write_exact(0, header);
      file_.flush();
      if (file_.created()) file_.sync_parent_directory();
      state_.file_bytes = header.size();
      return;
    }
    auto read = [this](std::uint64_t offset,
                       std::span<std::byte> destination) {
      file_.read_exact(offset, destination);
    };
    WalScanResult recovered =
        scan_source(byte_count, read, identity_, options_);
    state_ = std::move(recovered.state);
    if (recovered.has_incomplete_tail) {
      file_.truncate(recovered.valid_bytes);
      file_.flush();
      state_.file_bytes = recovered.valid_bytes;
      state_.repaired_incomplete_tail = true;
    }
  }

  [[nodiscard]] WalAppendResult append_group(
      std::span<const PersistBatch> batches) {
    std::lock_guard lock(mutex_);
    ensure_live();
    if (batches.empty()) {
      throw WalError(WalErrorCode::invariant_violation,
                     "cannot append an empty WAL group");
    }
    if (batches.size() > options_.max_group_frames) {
      throw WalError(WalErrorCode::bounds_exceeded,
                     "WAL group has too many frames");
    }
    if (batches.size() > std::numeric_limits<std::uint64_t>::max() -
                             state_.last_frame_sequence) {
      throw WalError(WalErrorCode::bounds_exceeded,
                     "WAL frame sequence is exhausted");
    }

    ValidationView view(state_);
    std::vector<Bytes> frames;
    frames.reserve(batches.size());
    std::size_t group_bytes = 0;
    std::uint64_t sequence = state_.last_frame_sequence;
    for (const PersistBatch& batch : batches) {
      view.stage(batch, options_);
      Bytes frame = make_frame(++sequence, batch, options_);
      if (frame.size() > options_.max_group_bytes -
                             std::min(options_.max_group_bytes, group_bytes)) {
        throw WalError(WalErrorCode::bounds_exceeded,
                       "WAL group exceeds configured byte bound");
      }
      group_bytes += frame.size();
      frames.push_back(std::move(frame));
    }
    if (group_bytes > std::numeric_limits<std::uint64_t>::max() -
                          state_.file_bytes) {
      throw WalError(WalErrorCode::bounds_exceeded,
                     "WAL file length would overflow");
    }
    state_.entries.reserve(view.entry_count());

    const std::uint64_t first_sequence = state_.last_frame_sequence + 1U;
    std::uint64_t write_offset = state_.file_bytes;
    const bool should_flush =
        options_.flush_policy == WalFlushPolicy::flush_every_append;
    try {
      for (const Bytes& frame : frames) {
        file_.write_exact(write_offset, frame);
        write_offset += frame.size();
      }
      if (should_flush) file_.flush();
    } catch (const WalError& error) {
      if (error.code() == WalErrorCode::io) fail(error.what());
      throw;
    }

    view.materialize(state_);
    state_.last_frame_sequence = sequence;
    state_.file_bytes = write_offset;
    if (should_flush) state_.durable_frame_sequence = sequence;
    return WalAppendResult{first_sequence, sequence, should_flush,
                           group_bytes};
  }

  void flush() {
    std::lock_guard lock(mutex_);
    ensure_live();
    if (state_.durable_frame_sequence == state_.last_frame_sequence) return;
    try {
      file_.flush();
    } catch (const WalError& error) {
      if (error.code() == WalErrorCode::io) fail(error.what());
      throw;
    }
    state_.durable_frame_sequence = state_.last_frame_sequence;
  }

  [[nodiscard]] WalState state() const {
    std::lock_guard lock(mutex_);
    return state_;
  }

  [[nodiscard]] bool failed() const {
    std::lock_guard lock(mutex_);
    return failed_;
  }

  [[nodiscard]] std::string failure_reason() const {
    std::lock_guard lock(mutex_);
    return failure_reason_;
  }

  [[nodiscard]] std::filesystem::path path() const { return path_; }

 private:
  void ensure_live() const {
    if (failed_) {
      throw WalError(WalErrorCode::failed,
                     "WAL is fail-stop: " + failure_reason_);
    }
  }

  [[noreturn]] void fail(std::string reason) {
    failed_ = true;
    failure_reason_ = std::move(reason);
    throw WalError(WalErrorCode::io, failure_reason_);
  }

  std::filesystem::path path_;
  WalIdentity identity_;
  WalOptions options_;
  NativeFile file_;
  mutable std::mutex mutex_;
  WalState state_;
  bool failed_{};
  std::string failure_reason_;
};

Wal::Wal(std::filesystem::path path, WalIdentity identity, WalOptions options)
    : impl_(std::make_unique<Impl>(std::move(path), identity, options)) {}

Wal::~Wal() = default;

WalAppendResult Wal::append(const PersistBatch& batch) {
  return impl_->append_group(std::span<const PersistBatch>(&batch, 1));
}

WalAppendResult Wal::append_group(std::span<const PersistBatch> batches) {
  return impl_->append_group(batches);
}

void Wal::flush() { impl_->flush(); }

WalState Wal::state() const { return impl_->state(); }

bool Wal::failed() const { return impl_->failed(); }

std::string Wal::failure_reason() const { return impl_->failure_reason(); }

std::filesystem::path Wal::path() const { return impl_->path(); }

}  // namespace detlog
