#pragma once

#include "detlog/model.hpp"

#include <cstddef>
#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <string_view>

namespace detlog {

// The client contract is deliberately small and bounded per session: a client
// uses monotonically increasing sequence numbers and has at most one request in
// flight.  Consequently, retaining the last result is sufficient for retries.
struct SessionRecord {
  std::uint64_t sequence{};
  std::uint64_t command_digest{};
  ClientStatus status{ClientStatus::ok};
  std::string result;

  friend bool operator==(const SessionRecord&, const SessionRecord&) = default;
};

struct ApplyResult {
  ClientStatus status{ClientStatus::ok};
  std::string value;
  bool executed{};
  bool duplicate{};

  friend bool operator==(const ApplyResult&, const ApplyResult&) = default;
};

// A deterministic key/value state machine. Reads are represented by replicated
// CommandKind::get commands; this class intentionally has no local-read API that
// could accidentally be exposed as a linearizable read.
class KvStateMachine {
 public:
  [[nodiscard]] static std::uint64_t command_digest(
      const ClientCommand& command) noexcept;
  [[nodiscard]] static bool structurally_valid(
      const ClientCommand& command) noexcept;

  ApplyResult apply(const ClientCommand& command);

  [[nodiscard]] std::optional<SessionRecord> session(
      const SessionId& id) const;
  [[nodiscard]] std::optional<std::string> value_for_test(
      std::string_view key) const;
  [[nodiscard]] std::size_t key_count() const noexcept { return values_.size(); }
  [[nodiscard]] std::size_t session_count() const noexcept {
    return sessions_.size();
  }

 private:
  std::map<std::string, std::string, std::less<>> values_;
  std::map<SessionId, SessionRecord> sessions_;
};

}  // namespace detlog
