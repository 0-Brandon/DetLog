#include "detlog/codec.hpp"

#include <algorithm>
#include <array>
#include <limits>
#include <type_traits>

namespace detlog::codec {
namespace {

constexpr std::size_t kHeaderBytes = 12;
constexpr std::size_t kMinimumEntryPayloadBytes = 17;
constexpr std::array<std::uint8_t, 4> kMagic{'D', 'L', 'O', 'G'};
constexpr std::uint8_t kVersion = 1;

enum class WireType : std::uint8_t {
  command = 1,
  log_entry = 2,
  request_vote = 16,
  request_vote_response = 17,
  append_entries = 18,
  append_entries_response = 19,
};

[[nodiscard]] bool known_wire_type(std::uint8_t value) noexcept {
  switch (static_cast<WireType>(value)) {
    case WireType::command:
    case WireType::log_entry:
    case WireType::request_vote:
    case WireType::request_vote_response:
    case WireType::append_entries:
    case WireType::append_entries_response:
      return true;
  }
  return false;
}

[[nodiscard]] bool known_command_kind(CommandKind kind) noexcept {
  switch (kind) {
    case CommandKind::put:
    case CommandKind::erase:
    case CommandKind::get:
      return true;
  }
  return false;
}

[[nodiscard]] bool valid_log_pointer(LogIndex index, Term term) noexcept {
  return (index == 0) == (term == 0);
}

[[nodiscard]] bool valid_command_semantics(
    const ClientCommand& command) noexcept {
  return command.sequence != 0 && !command.key.empty() &&
         known_command_kind(command.kind) &&
         !((command.kind == CommandKind::erase ||
            command.kind == CommandKind::get) &&
           !command.value.empty());
}

template <typename T>
[[nodiscard]] Result<T> failure(Error error, std::size_t offset) {
  return Result<T>{std::nullopt, error, offset};
}

template <typename T>
[[nodiscard]] Result<T> success(T value) {
  return Result<T>{std::move(value), Error::none, 0};
}

class Writer {
 public:
  explicit Writer(std::size_t limit) : limit_(limit) {}

  [[nodiscard]] bool put_u8(std::uint8_t value) {
    if (!ensure(1)) {
      return false;
    }
    bytes_.push_back(static_cast<std::byte>(value));
    return true;
  }

  [[nodiscard]] bool put_u16(std::uint16_t value) {
    return put_u8(static_cast<std::uint8_t>(value >> 8U)) &&
           put_u8(static_cast<std::uint8_t>(value));
  }

  [[nodiscard]] bool put_u32(std::uint32_t value) {
    for (int shift = 24; shift >= 0; shift -= 8) {
      if (!put_u8(static_cast<std::uint8_t>(value >> shift))) {
        return false;
      }
    }
    return true;
  }

  [[nodiscard]] bool put_u64(std::uint64_t value) {
    for (int shift = 56; shift >= 0; shift -= 8) {
      if (!put_u8(static_cast<std::uint8_t>(value >> shift))) {
        return false;
      }
    }
    return true;
  }

  [[nodiscard]] bool put_bytes(std::span<const std::byte> bytes) {
    if (!ensure(bytes.size())) {
      return false;
    }
    bytes_.insert(bytes_.end(), bytes.begin(), bytes.end());
    return true;
  }

  [[nodiscard]] bool put_string(std::string_view value,
                                std::size_t field_limit) {
    if (value.size() > field_limit ||
        value.size() > std::numeric_limits<std::uint32_t>::max()) {
      fail(Error::limit_exceeded);
      return false;
    }
    if (!put_u32(static_cast<std::uint32_t>(value.size()))) {
      return false;
    }
    return put_bytes(std::as_bytes(std::span(value.data(), value.size())));
  }

  void fail(Error error) noexcept {
    if (error_ == Error::none) {
      error_ = error;
      error_offset_ = bytes_.size();
    }
  }

  [[nodiscard]] Error error() const noexcept { return error_; }
  [[nodiscard]] std::size_t error_offset() const noexcept {
    return error_offset_;
  }
  [[nodiscard]] std::size_t size() const noexcept { return bytes_.size(); }
  [[nodiscard]] const ByteBuffer& bytes() const noexcept { return bytes_; }
  [[nodiscard]] ByteBuffer take() noexcept { return std::move(bytes_); }

 private:
  [[nodiscard]] bool ensure(std::size_t count) {
    if (count > limit_ || bytes_.size() > limit_ - count) {
      fail(Error::limit_exceeded);
      return false;
    }
    return true;
  }

  std::size_t limit_{};
  ByteBuffer bytes_;
  Error error_{Error::none};
  std::size_t error_offset_{};
};

class Reader {
 public:
  explicit Reader(std::span<const std::byte> bytes, std::size_t base = 0)
      : bytes_(bytes), base_(base) {}

  [[nodiscard]] bool get_u8(std::uint8_t& value) {
    if (!require(1)) {
      return false;
    }
    value = std::to_integer<std::uint8_t>(bytes_[position_++]);
    return true;
  }

  [[nodiscard]] bool get_u16(std::uint16_t& value) {
    std::uint8_t high{};
    std::uint8_t low{};
    if (!get_u8(high) || !get_u8(low)) {
      return false;
    }
    value = static_cast<std::uint16_t>(
        (static_cast<std::uint16_t>(high) << 8U) | low);
    return true;
  }

  [[nodiscard]] bool get_u32(std::uint32_t& value) {
    value = 0;
    for (int i = 0; i < 4; ++i) {
      std::uint8_t byte{};
      if (!get_u8(byte)) {
        return false;
      }
      value = static_cast<std::uint32_t>((value << 8U) | byte);
    }
    return true;
  }

  [[nodiscard]] bool get_u64(std::uint64_t& value) {
    value = 0;
    for (int i = 0; i < 8; ++i) {
      std::uint8_t byte{};
      if (!get_u8(byte)) {
        return false;
      }
      value = (value << 8U) | byte;
    }
    return true;
  }

  [[nodiscard]] bool get_string(std::string& value,
                                std::size_t field_limit) {
    std::uint32_t length{};
    if (!get_u32(length)) {
      return false;
    }
    if (length > field_limit) {
      fail(Error::limit_exceeded);
      return false;
    }
    if (!require(length)) {
      return false;
    }
    const auto* begin = reinterpret_cast<const char*>(
        bytes_.data() + static_cast<std::ptrdiff_t>(position_));
    value.assign(begin, length);
    position_ += length;
    return true;
  }

  [[nodiscard]] bool get_bool(bool& value) {
    std::uint8_t raw{};
    if (!get_u8(raw)) {
      return false;
    }
    if (raw > 1) {
      fail(Error::invalid_boolean);
      return false;
    }
    value = raw == 1;
    return true;
  }

  void fail(Error error) noexcept {
    if (error_ == Error::none) {
      error_ = error;
      error_offset_ = absolute_position();
    }
  }

  [[nodiscard]] bool at_end() const noexcept {
    return position_ == bytes_.size();
  }
  [[nodiscard]] std::size_t remaining() const noexcept {
    return bytes_.size() - position_;
  }
  [[nodiscard]] std::size_t absolute_position() const noexcept {
    return base_ + position_;
  }
  [[nodiscard]] Error error() const noexcept { return error_; }
  [[nodiscard]] std::size_t error_offset() const noexcept {
    return error_offset_;
  }

 private:
  [[nodiscard]] bool require(std::size_t count) {
    if (count > remaining()) {
      fail(Error::truncated);
      return false;
    }
    return true;
  }

  std::span<const std::byte> bytes_;
  std::size_t base_{};
  std::size_t position_{};
  Error error_{Error::none};
  std::size_t error_offset_{};
};

[[nodiscard]] bool write_command_payload(Writer& writer,
                                         const ClientCommand& command,
                                         const Limits& limits) {
  if (!known_command_kind(command.kind)) {
    writer.fail(Error::invalid_enum);
    return false;
  }
  if (!valid_command_semantics(command)) {
    writer.fail(Error::invalid_value);
    return false;
  }
  return writer.put_u64(command.session.high) &&
         writer.put_u64(command.session.low) &&
         writer.put_u64(command.sequence) &&
         writer.put_u8(static_cast<std::uint8_t>(command.kind)) &&
         writer.put_string(command.key, limits.max_key_bytes) &&
         writer.put_string(command.value, limits.max_value_bytes);
}

[[nodiscard]] bool read_command_payload(Reader& reader,
                                        ClientCommand& command,
                                        const Limits& limits) {
  std::uint8_t kind{};
  if (!reader.get_u64(command.session.high) ||
      !reader.get_u64(command.session.low) ||
      !reader.get_u64(command.sequence) || !reader.get_u8(kind)) {
    return false;
  }
  command.kind = static_cast<CommandKind>(kind);
  if (!known_command_kind(command.kind)) {
    reader.fail(Error::invalid_enum);
    return false;
  }
  if (!reader.get_string(command.key, limits.max_key_bytes) ||
      !reader.get_string(command.value, limits.max_value_bytes)) {
    return false;
  }
  if (!valid_command_semantics(command)) {
    reader.fail(Error::invalid_value);
    return false;
  }
  return true;
}

[[nodiscard]] bool write_entry_payload(Writer& writer, const LogEntry& entry,
                                        const Limits& limits) {
  if (entry.index == 0 || entry.term == 0) {
    writer.fail(Error::invalid_value);
    return false;
  }
  if (!writer.put_u64(entry.index) || !writer.put_u64(entry.term) ||
      !writer.put_u8(entry.command ? 1U : 0U)) {
    return false;
  }
  return !entry.command ||
         write_command_payload(writer, *entry.command, limits);
}

[[nodiscard]] bool read_entry_payload(Reader& reader, LogEntry& entry,
                                      const Limits& limits) {
  bool has_command{};
  if (!reader.get_u64(entry.index) || !reader.get_u64(entry.term) ||
      !reader.get_bool(has_command)) {
    return false;
  }
  if (entry.index == 0 || entry.term == 0) {
    reader.fail(Error::invalid_value);
    return false;
  }
  if (has_command) {
    ClientCommand command;
    if (!read_command_payload(reader, command, limits)) {
      return false;
    }
    entry.command = std::move(command);
  } else {
    entry.command.reset();
  }
  return true;
}

[[nodiscard]] bool consecutive_entries(const AppendEntries& append) noexcept {
  if (append.entries.empty()) {
    return true;
  }
  if (append.prev_log_index == std::numeric_limits<LogIndex>::max()) {
    return false;
  }
  LogIndex expected = append.prev_log_index + 1;
  for (const auto& entry : append.entries) {
    if (entry.index != expected) {
      return false;
    }
    if (expected != std::numeric_limits<LogIndex>::max()) {
      ++expected;
    } else if (&entry != &append.entries.back()) {
      return false;
    }
  }
  return true;
}

[[nodiscard]] std::size_t payload_limit(const Limits& limits) noexcept {
  if (limits.max_frame_bytes < kHeaderBytes) {
    return 0;
  }
  return std::min<std::size_t>(
      limits.max_frame_bytes - kHeaderBytes,
      std::numeric_limits<std::uint32_t>::max());
}

[[nodiscard]] Result<ByteBuffer> frame(WireType type, Writer payload,
                                       const Limits& limits) {
  if (payload.error() != Error::none) {
    return failure<ByteBuffer>(payload.error(),
                               kHeaderBytes + payload.error_offset());
  }
  if (limits.max_frame_bytes < kHeaderBytes ||
      payload.size() > payload_limit(limits)) {
    return failure<ByteBuffer>(Error::limit_exceeded, 0);
  }
  Writer output(limits.max_frame_bytes);
  for (const auto byte : kMagic) {
    if (!output.put_u8(byte)) {
      return failure<ByteBuffer>(output.error(), output.error_offset());
    }
  }
  if (!output.put_u8(kVersion) ||
      !output.put_u8(static_cast<std::uint8_t>(type)) ||
      !output.put_u16(0) ||
      !output.put_u32(static_cast<std::uint32_t>(payload.size())) ||
      !output.put_bytes(payload.bytes())) {
    return failure<ByteBuffer>(output.error(), output.error_offset());
  }
  return success(output.take());
}

struct ParsedFrame {
  WireType type{WireType::command};
  std::span<const std::byte> payload;
};

[[nodiscard]] Result<ParsedFrame> parse_frame(
    std::span<const std::byte> bytes, const Limits& limits) {
  if (bytes.size() > limits.max_frame_bytes) {
    return failure<ParsedFrame>(Error::limit_exceeded, 0);
  }
  Reader reader(bytes);
  for (const auto expected : kMagic) {
    std::uint8_t actual{};
    if (!reader.get_u8(actual)) {
      return failure<ParsedFrame>(reader.error(), reader.error_offset());
    }
    if (actual != expected) {
      return failure<ParsedFrame>(Error::bad_magic,
                                  reader.absolute_position() - 1);
    }
  }
  std::uint8_t version{};
  std::uint8_t raw_type{};
  std::uint16_t reserved{};
  std::uint32_t length{};
  if (!reader.get_u8(version) || !reader.get_u8(raw_type) ||
      !reader.get_u16(reserved) || !reader.get_u32(length)) {
    return failure<ParsedFrame>(reader.error(), reader.error_offset());
  }
  if (version != kVersion) {
    return failure<ParsedFrame>(Error::unsupported_version, 4);
  }
  if (!known_wire_type(raw_type)) {
    return failure<ParsedFrame>(Error::unknown_type, 5);
  }
  if (reserved != 0) {
    return failure<ParsedFrame>(Error::nonzero_reserved, 6);
  }
  if (length > payload_limit(limits)) {
    return failure<ParsedFrame>(Error::limit_exceeded, 8);
  }
  if (length > reader.remaining()) {
    return failure<ParsedFrame>(Error::truncated, kHeaderBytes);
  }
  if (length < reader.remaining()) {
    return failure<ParsedFrame>(Error::trailing_bytes,
                                kHeaderBytes + length);
  }
  return success(ParsedFrame{
      static_cast<WireType>(raw_type), bytes.subspan(kHeaderBytes, length)});
}

template <typename T, typename Decode>
[[nodiscard]] Result<T> decode_exact(const ParsedFrame& parsed,
                                     Decode&& decode) {
  Reader reader(parsed.payload, kHeaderBytes);
  T value;
  if (!decode(reader, value)) {
    return failure<T>(reader.error(), reader.error_offset());
  }
  if (!reader.at_end()) {
    return failure<T>(Error::trailing_bytes, reader.absolute_position());
  }
  return success(std::move(value));
}

}  // namespace

std::string_view to_string(Error error) noexcept {
  switch (error) {
    case Error::none:
      return "none";
    case Error::truncated:
      return "truncated";
    case Error::bad_magic:
      return "bad_magic";
    case Error::unsupported_version:
      return "unsupported_version";
    case Error::unknown_type:
      return "unknown_type";
    case Error::nonzero_reserved:
      return "nonzero_reserved";
    case Error::invalid_boolean:
      return "invalid_boolean";
    case Error::invalid_enum:
      return "invalid_enum";
    case Error::invalid_value:
      return "invalid_value";
    case Error::limit_exceeded:
      return "limit_exceeded";
    case Error::length_overflow:
      return "length_overflow";
    case Error::trailing_bytes:
      return "trailing_bytes";
    case Error::unexpected_type:
      return "unexpected_type";
  }
  return "unknown";
}

Result<ByteBuffer> encode_command(const ClientCommand& command,
                                  const Limits& limits) {
  Writer payload(payload_limit(limits));
  (void)write_command_payload(payload, command, limits);
  return frame(WireType::command, std::move(payload), limits);
}

Result<ClientCommand> decode_command(std::span<const std::byte> bytes,
                                     const Limits& limits) {
  auto parsed = parse_frame(bytes, limits);
  if (!parsed) {
    return failure<ClientCommand>(parsed.error, parsed.offset);
  }
  if (parsed->type != WireType::command) {
    return failure<ClientCommand>(Error::unexpected_type, 5);
  }
  return decode_exact<ClientCommand>(
      *parsed, [&](Reader& reader, ClientCommand& command) {
        return read_command_payload(reader, command, limits);
      });
}

Result<ByteBuffer> encode_log_entry(const LogEntry& entry,
                                    const Limits& limits) {
  Writer payload(payload_limit(limits));
  (void)write_entry_payload(payload, entry, limits);
  return frame(WireType::log_entry, std::move(payload), limits);
}

Result<LogEntry> decode_log_entry(std::span<const std::byte> bytes,
                                  const Limits& limits) {
  auto parsed = parse_frame(bytes, limits);
  if (!parsed) {
    return failure<LogEntry>(parsed.error, parsed.offset);
  }
  if (parsed->type != WireType::log_entry) {
    return failure<LogEntry>(Error::unexpected_type, 5);
  }
  return decode_exact<LogEntry>(*parsed,
                                [&](Reader& reader, LogEntry& entry) {
                                  return read_entry_payload(reader, entry,
                                                            limits);
                                });
}

Result<ByteBuffer> encode_message(const Message& message,
                                  const Limits& limits) {
  Writer payload(payload_limit(limits));
  WireType type = WireType::request_vote;
  std::visit(
      [&](const auto& value) {
        using T = std::decay_t<decltype(value)>;
        if constexpr (std::is_same_v<T, RequestVote>) {
          type = WireType::request_vote;
          if (value.term == 0 || value.candidate_id == 0 ||
              !valid_log_pointer(value.last_log_index,
                                 value.last_log_term)) {
            payload.fail(Error::invalid_value);
            return;
          }
          (void)(payload.put_u64(value.term) &&
                 payload.put_u32(value.candidate_id) &&
                 payload.put_u64(value.last_log_index) &&
                 payload.put_u64(value.last_log_term));
        } else if constexpr (std::is_same_v<T, RequestVoteResponse>) {
          type = WireType::request_vote_response;
          if (value.term == 0) {
            payload.fail(Error::invalid_value);
            return;
          }
          (void)(payload.put_u64(value.term) &&
                 payload.put_u8(value.vote_granted ? 1U : 0U));
        } else if constexpr (std::is_same_v<T, AppendEntries>) {
          type = WireType::append_entries;
          if (value.term == 0 || value.leader_id == 0 || value.rpc_id == 0 ||
              !valid_log_pointer(value.prev_log_index,
                                 value.prev_log_term)) {
            payload.fail(Error::invalid_value);
            return;
          }
          if (value.entries.size() > limits.max_entries_per_append ||
              value.entries.size() >
                  std::numeric_limits<std::uint32_t>::max()) {
            payload.fail(Error::limit_exceeded);
            return;
          }
          if (!consecutive_entries(value)) {
            payload.fail(Error::invalid_value);
            return;
          }
          if (!(payload.put_u64(value.term) &&
                payload.put_u32(value.leader_id) &&
                payload.put_u64(value.rpc_id) &&
                payload.put_u64(value.prev_log_index) &&
                payload.put_u64(value.prev_log_term) &&
                payload.put_u32(
                    static_cast<std::uint32_t>(value.entries.size())))) {
            return;
          }
          for (const auto& entry : value.entries) {
            if (!write_entry_payload(payload, entry, limits)) {
              return;
            }
          }
          (void)payload.put_u64(value.leader_commit);
        } else if constexpr (std::is_same_v<T, AppendEntriesResponse>) {
          type = WireType::append_entries_response;
          if (value.term == 0 || value.rpc_id == 0) {
            payload.fail(Error::invalid_value);
            return;
          }
          (void)(payload.put_u64(value.term) &&
                 payload.put_u64(value.rpc_id) &&
                 payload.put_u8(value.success ? 1U : 0U) &&
                 payload.put_u64(value.match_index) &&
                 payload.put_u64(value.conflict_index) &&
                 payload.put_u64(value.conflict_term));
        }
      },
      message);
  return frame(type, std::move(payload), limits);
}

Result<Message> decode_message(std::span<const std::byte> bytes,
                               const Limits& limits) {
  auto parsed = parse_frame(bytes, limits);
  if (!parsed) {
    return failure<Message>(parsed.error, parsed.offset);
  }

  switch (parsed->type) {
    case WireType::request_vote: {
      auto decoded = decode_exact<RequestVote>(
          *parsed, [](Reader& reader, RequestVote& value) {
            return reader.get_u64(value.term) &&
                   reader.get_u32(value.candidate_id) &&
                   reader.get_u64(value.last_log_index) &&
                   reader.get_u64(value.last_log_term);
          });
      if (!decoded) {
        return failure<Message>(decoded.error, decoded.offset);
      }
      if (decoded->term == 0 || decoded->candidate_id == 0 ||
          !valid_log_pointer(decoded->last_log_index,
                             decoded->last_log_term)) {
        return failure<Message>(Error::invalid_value, kHeaderBytes);
      }
      return success(Message{std::move(*decoded)});
    }
    case WireType::request_vote_response: {
      auto decoded = decode_exact<RequestVoteResponse>(
          *parsed, [](Reader& reader, RequestVoteResponse& value) {
            return reader.get_u64(value.term) &&
                   reader.get_bool(value.vote_granted);
          });
      if (!decoded) {
        return failure<Message>(decoded.error, decoded.offset);
      }
      if (decoded->term == 0) {
        return failure<Message>(Error::invalid_value, kHeaderBytes);
      }
      return success(Message{std::move(*decoded)});
    }
    case WireType::append_entries: {
      auto decoded = decode_exact<AppendEntries>(
          *parsed, [&](Reader& reader, AppendEntries& value) {
            std::uint32_t count{};
            if (!reader.get_u64(value.term) ||
                !reader.get_u32(value.leader_id) ||
                !reader.get_u64(value.rpc_id) ||
                !reader.get_u64(value.prev_log_index) ||
                !reader.get_u64(value.prev_log_term) ||
                !reader.get_u32(count)) {
              return false;
            }
            if (value.term == 0 || value.leader_id == 0 ||
                value.rpc_id == 0 ||
                !valid_log_pointer(value.prev_log_index,
                                   value.prev_log_term)) {
              reader.fail(Error::invalid_value);
              return false;
            }
            if (count > limits.max_entries_per_append) {
              reader.fail(Error::limit_exceeded);
              return false;
            }
            // Every entry needs index + term + command-presence, and the
            // message must still contain leaderCommit. Bound the allocation by
            // bytes physically present, not only by caller-supplied limits.
            constexpr std::size_t kLeaderCommitBytes = 8;
            if (reader.remaining() < kLeaderCommitBytes ||
                static_cast<std::size_t>(count) >
                    (reader.remaining() - kLeaderCommitBytes) /
                        kMinimumEntryPayloadBytes) {
              reader.fail(Error::truncated);
              return false;
            }
            value.entries.reserve(count);
            for (std::uint32_t i = 0; i < count; ++i) {
              LogEntry entry;
              if (!read_entry_payload(reader, entry, limits)) {
                return false;
              }
              value.entries.push_back(std::move(entry));
            }
            if (!reader.get_u64(value.leader_commit)) {
              return false;
            }
            if (!consecutive_entries(value)) {
              reader.fail(Error::invalid_value);
              return false;
            }
            return true;
          });
      if (!decoded) {
        return failure<Message>(decoded.error, decoded.offset);
      }
      return success(Message{std::move(*decoded)});
    }
    case WireType::append_entries_response: {
      auto decoded = decode_exact<AppendEntriesResponse>(
          *parsed, [](Reader& reader, AppendEntriesResponse& value) {
            return reader.get_u64(value.term) &&
                   reader.get_u64(value.rpc_id) &&
                   reader.get_bool(value.success) &&
                   reader.get_u64(value.match_index) &&
                   reader.get_u64(value.conflict_index) &&
                   reader.get_u64(value.conflict_term);
          });
      if (!decoded) {
        return failure<Message>(decoded.error, decoded.offset);
      }
      if (decoded->term == 0 || decoded->rpc_id == 0) {
        return failure<Message>(Error::invalid_value, kHeaderBytes);
      }
      return success(Message{std::move(*decoded)});
    }
    case WireType::command:
    case WireType::log_entry:
      return failure<Message>(Error::unexpected_type, 5);
  }
  return failure<Message>(Error::unknown_type, 5);
}

}  // namespace detlog::codec
