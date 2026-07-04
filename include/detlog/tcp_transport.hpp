#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <limits>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "detlog/model.hpp"

namespace detlog {

struct TcpIdentity {
  std::uint64_t cluster_id_high{};
  std::uint64_t cluster_id_low{};
  NodeId node_id{};

  friend bool operator==(const TcpIdentity&, const TcpIdentity&) = default;
};

struct TcpPeerEndpoint {
  NodeId node_id{};
  // IPv4 is intentional for this small demonstration transport. Loopback is
  // the default and the only permitted destination unless explicitly enabled.
  std::string address{"127.0.0.1"};
  // Zero means accept-only; a nonzero port enables automatic reconnects.
  std::uint16_t port{};
};

struct TcpTransportLimits {
  std::size_t max_frame_bytes{1024U * 1024U};
  std::size_t max_outbound_bytes_per_peer{4U * 1024U * 1024U};
  std::size_t max_total_outbound_bytes{16U * 1024U * 1024U};
  std::size_t max_inbound_events{1024};
  std::size_t max_inbound_event_bytes{8U * 1024U * 1024U};
  std::size_t max_pending_connections{32};
  std::uint32_t handshake_timeout_ms{3000};
  std::uint32_t reconnect_delay_ms{100};
  std::uint32_t io_poll_interval_ms{10};
};

struct TcpTransportConfig {
  TcpIdentity identity;
  // Zero selects an automatically generated per-process incarnation. Callers
  // that persist restart generations may provide a monotonically increasing
  // value for deterministic stale-connection replacement.
  std::uint64_t incarnation{};
  std::string listen_address{"127.0.0.1"};
  std::uint16_t listen_port{};
  bool allow_non_loopback{};
  std::vector<TcpPeerEndpoint> peers;
  TcpTransportLimits limits;
};

enum class TcpTransportErrorCode : std::uint8_t {
  none = 0,
  invalid_configuration,
  self_peer,
  unknown_peer,
  duplicate_peer,
  wrong_cluster,
  wrong_version,
  protocol_error,
  frame_too_large,
  queue_full,
  connection_down,
  io_error,
  stopped,
};

[[nodiscard]] std::string_view to_string(
    TcpTransportErrorCode error) noexcept;

class TcpTransportError final : public std::runtime_error {
 public:
  TcpTransportError(TcpTransportErrorCode code, std::string message);
  [[nodiscard]] TcpTransportErrorCode code() const noexcept { return code_; }

 private:
  TcpTransportErrorCode code_;
};

enum class TcpSendStatus : std::uint8_t {
  queued,
  would_block,
  down,
  error,
};

struct TcpSendResult {
  TcpSendStatus status{TcpSendStatus::error};
  TcpTransportErrorCode error{TcpTransportErrorCode::none};
  // Conservative wire bytes retained for this peer after the operation.
  std::size_t peer_queued_bytes{};
};

enum class TcpEventKind : std::uint8_t {
  peer_up,
  peer_down,
  message,
  peer_rejected,
  transport_error,
};

struct TcpTransportEvent {
  TcpEventKind kind{TcpEventKind::transport_error};
  NodeId peer{};
  std::uint64_t peer_incarnation{};
  TcpTransportErrorCode error{TcpTransportErrorCode::none};
  // For message events, these are exactly the codec bytes supplied to send(),
  // with the transport's u32 length prefix removed.
  std::vector<std::byte> bytes;

  friend bool operator==(const TcpTransportEvent&,
                         const TcpTransportEvent&) = default;
};

// A single native I/O thread owns the listener and every connection. It never
// calls Raft code: the owner drains immutable events through poll().
class TcpTransport final {
 public:
  static constexpr std::uint16_t protocol_version = 1;

  explicit TcpTransport(TcpTransportConfig config);
  ~TcpTransport();

  TcpTransport(const TcpTransport&) = delete;
  TcpTransport& operator=(const TcpTransport&) = delete;
  TcpTransport(TcpTransport&&) = delete;
  TcpTransport& operator=(TcpTransport&&) = delete;

  void start();
  void stop() noexcept;

  [[nodiscard]] TcpSendResult send(NodeId peer,
                                   std::span<const std::byte> codec_frame);

  [[nodiscard]] std::vector<TcpTransportEvent> poll(
      std::size_t max_events = std::numeric_limits<std::size_t>::max());

  [[nodiscard]] bool running() const noexcept;
  [[nodiscard]] std::uint16_t listening_port() const;
  [[nodiscard]] std::uint64_t incarnation() const noexcept;

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace detlog
