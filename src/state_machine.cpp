#include "detlog/state_machine.hpp"

#include <limits>
#include <utility>

namespace detlog {
namespace {

constexpr std::uint64_t kFnvOffset = 14695981039346656037ULL;
constexpr std::uint64_t kFnvPrime = 1099511628211ULL;

void hash_byte(std::uint64_t& hash, std::uint8_t byte) noexcept {
  hash ^= byte;
  hash *= kFnvPrime;
}

void hash_u64(std::uint64_t& hash, std::uint64_t value) noexcept {
  for (unsigned shift = 0; shift < 64; shift += 8) {
    hash_byte(hash, static_cast<std::uint8_t>(value >> shift));
  }
}

void hash_string(std::uint64_t& hash, std::string_view value) noexcept {
  hash_u64(hash, static_cast<std::uint64_t>(value.size()));
  for (const char byte : value) {
    hash_byte(hash, static_cast<std::uint8_t>(
                        static_cast<unsigned char>(byte)));
  }
}

}  // namespace

std::uint64_t KvStateMachine::command_digest(
    const ClientCommand& command) noexcept {
  std::uint64_t hash = kFnvOffset;
  hash_byte(hash, static_cast<std::uint8_t>(command.kind));
  hash_string(hash, command.key);
  hash_string(hash, command.value);
  return hash;
}

bool KvStateMachine::structurally_valid(
    const ClientCommand& command) noexcept {
  if (command.sequence == 0 || command.key.empty()) {
    return false;
  }
  switch (command.kind) {
    case CommandKind::put:
      return true;
    case CommandKind::erase:
    case CommandKind::get:
      return command.value.empty();
  }
  return false;
}

ApplyResult KvStateMachine::apply(const ClientCommand& command) {
  if (!structurally_valid(command)) {
    return {.status = ClientStatus::invalid_request,
            .value = {},
            .executed = false,
            .duplicate = false};
  }

  const std::uint64_t digest = command_digest(command);
  const auto existing = sessions_.find(command.session);
  if (existing != sessions_.end()) {
    const SessionRecord& record = existing->second;
    if (command.sequence < record.sequence) {
      return {.status = ClientStatus::stale_sequence,
              .value = {},
              .executed = false,
              .duplicate = false};
    }
    if (command.sequence == record.sequence) {
      if (record.command_digest != digest) {
        return {.status = ClientStatus::request_id_conflict,
                .value = {},
                .executed = false,
                .duplicate = false};
      }
      return {.status = record.status,
              .value = record.result,
              .executed = false,
              .duplicate = true};
    }
    if (record.sequence == std::numeric_limits<std::uint64_t>::max() ||
        command.sequence != record.sequence + 1) {
      return {.status = ClientStatus::sequence_gap,
              .value = {},
              .executed = false,
              .duplicate = false};
    }
  } else if (command.sequence != 1) {
    return {.status = ClientStatus::sequence_gap,
            .value = {},
            .executed = false,
            .duplicate = false};
  }

  std::string result;
  switch (command.kind) {
    case CommandKind::put:
      values_[command.key] = command.value;
      result = command.value;
      break;
    case CommandKind::erase: {
      const auto value = values_.find(command.key);
      if (value != values_.end()) {
        result = value->second;
        values_.erase(value);
      }
      break;
    }
    case CommandKind::get: {
      const auto value = values_.find(command.key);
      if (value != values_.end()) {
        result = value->second;
      }
      break;
    }
  }

  sessions_[command.session] = SessionRecord{
      .sequence = command.sequence,
      .command_digest = digest,
      .status = ClientStatus::ok,
      .result = result,
  };
  return {.status = ClientStatus::ok,
          .value = std::move(result),
          .executed = true,
          .duplicate = false};
}

std::optional<SessionRecord> KvStateMachine::session(
    const SessionId& id) const {
  const auto record = sessions_.find(id);
  if (record == sessions_.end()) {
    return std::nullopt;
  }
  return record->second;
}

std::optional<std::string> KvStateMachine::value_for_test(
    std::string_view key) const {
  const auto value = values_.find(key);
  if (value == values_.end()) {
    return std::nullopt;
  }
  return value->second;
}

}  // namespace detlog
