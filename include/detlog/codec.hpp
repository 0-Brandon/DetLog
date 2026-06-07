#pragma once

#include "detlog/model.hpp"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string_view>
#include <utility>
#include <vector>

namespace detlog::codec {

using ByteBuffer = std::vector<std::byte>;

// The wire format is deliberately small and fixed. All integers are unsigned
// big-endian values, all lengths are u32 values, and a decoder must consume the
// complete frame. This keeps the simulated and TCP transports on exactly the
// same parser path.
struct Limits {
  std::size_t max_frame_bytes{1024U * 1024U};
  std::size_t max_key_bytes{4U * 1024U};
  std::size_t max_value_bytes{64U * 1024U};
  std::size_t max_entries_per_append{256U};
};

enum class Error : std::uint8_t {
  none = 0,
  truncated,
  bad_magic,
  unsupported_version,
  unknown_type,
  nonzero_reserved,
  invalid_boolean,
  invalid_enum,
  invalid_value,
  limit_exceeded,
  length_overflow,
  trailing_bytes,
  unexpected_type,
};

[[nodiscard]] std::string_view to_string(Error error) noexcept;

template <typename T>
struct Result {
  std::optional<T> value;
  Error error{Error::none};
  std::size_t offset{};

  [[nodiscard]] explicit operator bool() const noexcept {
    return error == Error::none && value.has_value();
  }

  [[nodiscard]] T& operator*() & { return *value; }
  [[nodiscard]] const T& operator*() const& { return *value; }
  [[nodiscard]] T&& operator*() && { return std::move(*value); }
  [[nodiscard]] T* operator->() { return &*value; }
  [[nodiscard]] const T* operator->() const { return &*value; }
};

[[nodiscard]] Result<ByteBuffer> encode_command(
    const ClientCommand& command, const Limits& limits = {});
[[nodiscard]] Result<ClientCommand> decode_command(
    std::span<const std::byte> frame, const Limits& limits = {});

[[nodiscard]] Result<ByteBuffer> encode_log_entry(
    const LogEntry& entry, const Limits& limits = {});
[[nodiscard]] Result<LogEntry> decode_log_entry(
    std::span<const std::byte> frame, const Limits& limits = {});

[[nodiscard]] Result<ByteBuffer> encode_message(
    const Message& message, const Limits& limits = {});
[[nodiscard]] Result<Message> decode_message(
    std::span<const std::byte> frame, const Limits& limits = {});

}  // namespace detlog::codec
