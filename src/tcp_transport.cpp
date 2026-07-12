#include "detlog/tcp_transport.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <limits>
#include <map>
#include <mutex>
#include <new>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#include <cerrno>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace detlog {
namespace {

#ifdef _WIN32
using NativeSocket = SOCKET;
using NativeSocketLength = int;
constexpr NativeSocket kInvalidSocket = INVALID_SOCKET;
#else
using NativeSocket = int;
using NativeSocketLength = socklen_t;
constexpr NativeSocket kInvalidSocket = -1;
#endif

constexpr std::string_view kHandshakeMagic{"DTLGPEER", 8};
constexpr std::size_t kHandshakeBytes = 56;
constexpr std::uint32_t kInitiatorFlag = 1U;
constexpr std::size_t kLengthPrefixBytes = 4;
constexpr std::size_t kAbsoluteMaxFrameBytes = 64U * 1024U * 1024U;
constexpr std::size_t kReadChunkBytes = 16U * 1024U;

class SocketRuntime {
 public:
  SocketRuntime() {
#ifdef _WIN32
    WSADATA data{};
    const int result = WSAStartup(MAKEWORD(2, 2), &data);
    if (result != 0) {
      throw TcpTransportError(TcpTransportErrorCode::io_error,
                              "WSAStartup failed: " +
                                  std::to_string(result));
    }
    started_ = true;
#endif
  }

  ~SocketRuntime() {
#ifdef _WIN32
    if (started_) WSACleanup();
#endif
  }

  SocketRuntime(const SocketRuntime&) = delete;
  SocketRuntime& operator=(const SocketRuntime&) = delete;

 private:
#ifdef _WIN32
  bool started_{};
#endif
};

[[nodiscard]] int last_socket_error() noexcept {
#ifdef _WIN32
  return WSAGetLastError();
#else
  return errno;
#endif
}

[[nodiscard]] bool interrupted_error(int error) noexcept {
#ifdef _WIN32
  return error == WSAEINTR;
#else
  return error == EINTR;
#endif
}

[[nodiscard]] bool would_block_error(int error) noexcept {
#ifdef _WIN32
  return error == WSAEWOULDBLOCK;
#else
  return error == EAGAIN || error == EWOULDBLOCK;
#endif
}

[[nodiscard]] bool connect_pending_error(int error) noexcept {
#ifdef _WIN32
  return error == WSAEWOULDBLOCK || error == WSAEINPROGRESS ||
         error == WSAEINVAL;
#else
  return error == EINPROGRESS || error == EALREADY ||
         would_block_error(error);
#endif
}

void close_socket(NativeSocket& socket) noexcept {
  if (socket == kInvalidSocket) return;
#ifdef _WIN32
  (void)shutdown(socket, SD_BOTH);
  (void)closesocket(socket);
#else
  (void)shutdown(socket, SHUT_RDWR);
  (void)::close(socket);
#endif
  socket = kInvalidSocket;
}

[[nodiscard]] bool select_compatible(NativeSocket socket) noexcept {
#ifdef _WIN32
  // Winsock fd_set stores SOCKET values in an array; the numeric value is not
  // used as a bit index.
  return socket != kInvalidSocket;
#else
  return socket >= 0 && socket < FD_SETSIZE;
#endif
}

[[nodiscard]] bool set_nonblocking(NativeSocket socket) noexcept {
#ifdef _WIN32
  u_long enabled = 1;
  return ioctlsocket(socket, static_cast<long>(FIONBIO), &enabled) == 0;
#else
  const int flags = fcntl(socket, F_GETFL, 0);
  return flags >= 0 && fcntl(socket, F_SETFL, flags | O_NONBLOCK) == 0;
#endif
}

void configure_connected_socket(NativeSocket socket) noexcept {
  const int enabled = 1;
  (void)setsockopt(socket, IPPROTO_TCP, TCP_NODELAY,
                   reinterpret_cast<const char*>(&enabled),
                   static_cast<NativeSocketLength>(sizeof(enabled)));
#if !defined(_WIN32) && defined(SO_NOSIGPIPE)
  (void)setsockopt(socket, SOL_SOCKET, SO_NOSIGPIPE, &enabled,
                   static_cast<NativeSocketLength>(sizeof(enabled)));
#endif
}

[[nodiscard]] bool is_loopback(const in_addr& address) noexcept {
  return (ntohl(address.s_addr) & 0xff000000U) == 0x7f000000U;
}

[[nodiscard]] sockaddr_in resolve_ipv4(std::string address,
                                       std::uint16_t port,
                                       bool allow_non_loopback) {
  if (address == "localhost") address = "127.0.0.1";
  sockaddr_in result{};
  result.sin_family = AF_INET;
  result.sin_port = htons(port);
  if (inet_pton(AF_INET, address.c_str(), &result.sin_addr) != 1) {
    addrinfo hints{};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    addrinfo* resolved = nullptr;
    const int status = getaddrinfo(address.c_str(), nullptr, &hints, &resolved);
    if (status != 0 || resolved == nullptr) {
      if (resolved != nullptr) freeaddrinfo(resolved);
      throw TcpTransportError(TcpTransportErrorCode::invalid_configuration,
                              "cannot resolve IPv4 address '" + address +
                                  "'");
    }
    result.sin_addr =
        reinterpret_cast<const sockaddr_in*>(resolved->ai_addr)->sin_addr;
    freeaddrinfo(resolved);
  }
  if (!allow_non_loopback && !is_loopback(result.sin_addr)) {
    throw TcpTransportError(TcpTransportErrorCode::invalid_configuration,
                            "non-loopback TCP address requires explicit opt-in");
  }
  return result;
}

void put_u16(std::span<std::byte> bytes, std::size_t offset,
             std::uint16_t value) noexcept {
  bytes[offset] = static_cast<std::byte>(value >> 8U);
  bytes[offset + 1U] = static_cast<std::byte>(value);
}

void put_u32(std::span<std::byte> bytes, std::size_t offset,
             std::uint32_t value) noexcept {
  for (unsigned index = 0; index < 4; ++index) {
    const unsigned shift = 24U - index * 8U;
    bytes[offset + index] = static_cast<std::byte>(value >> shift);
  }
}

void put_u64(std::span<std::byte> bytes, std::size_t offset,
             std::uint64_t value) noexcept {
  for (unsigned index = 0; index < 8; ++index) {
    const unsigned shift = 56U - index * 8U;
    bytes[offset + index] = static_cast<std::byte>(value >> shift);
  }
}

[[nodiscard]] std::uint16_t get_u16(std::span<const std::byte> bytes,
                                    std::size_t offset) noexcept {
  const std::uint32_t high = std::to_integer<std::uint8_t>(bytes[offset]);
  const std::uint32_t low = std::to_integer<std::uint8_t>(bytes[offset + 1U]);
  return static_cast<std::uint16_t>((high << 8U) | low);
}

[[nodiscard]] std::uint32_t get_u32(std::span<const std::byte> bytes,
                                    std::size_t offset) noexcept {
  std::uint32_t value = 0;
  for (unsigned index = 0; index < 4; ++index) {
    value = (value << 8U) |
            std::to_integer<std::uint8_t>(bytes[offset + index]);
  }
  return value;
}

[[nodiscard]] std::uint64_t get_u64(std::span<const std::byte> bytes,
                                    std::size_t offset) noexcept {
  std::uint64_t value = 0;
  for (unsigned index = 0; index < 8; ++index) {
    value = (value << 8U) |
            std::to_integer<std::uint8_t>(bytes[offset + index]);
  }
  return value;
}

struct Handshake {
  TcpIdentity identity;
  std::uint64_t incarnation{};
  std::uint64_t nonce{};
  bool initiator{};
};

[[nodiscard]] std::array<std::byte, kHandshakeBytes> make_handshake(
    const TcpIdentity& identity, std::uint64_t incarnation,
    std::uint64_t nonce, bool initiator) {
  std::array<std::byte, kHandshakeBytes> bytes{};
  for (std::size_t index = 0; index < kHandshakeMagic.size(); ++index) {
    bytes[index] = static_cast<std::byte>(
        static_cast<unsigned char>(kHandshakeMagic[index]));
  }
  put_u16(bytes, 8, TcpTransport::protocol_version);
  put_u16(bytes, 10, static_cast<std::uint16_t>(kHandshakeBytes));
  put_u32(bytes, 12, initiator ? kInitiatorFlag : 0U);
  put_u64(bytes, 16, identity.cluster_id_high);
  put_u64(bytes, 24, identity.cluster_id_low);
  put_u32(bytes, 32, identity.node_id);
  put_u32(bytes, 36, 0);
  put_u64(bytes, 40, incarnation);
  put_u64(bytes, 48, nonce);
  return bytes;
}

struct HandshakeResult {
  std::optional<Handshake> value;
  TcpTransportErrorCode error{TcpTransportErrorCode::none};
};

[[nodiscard]] HandshakeResult parse_handshake(
    std::span<const std::byte> bytes) noexcept {
  for (std::size_t index = 0; index < kHandshakeMagic.size(); ++index) {
    if (std::to_integer<std::uint8_t>(bytes[index]) !=
        static_cast<std::uint8_t>(kHandshakeMagic[index])) {
      return {std::nullopt, TcpTransportErrorCode::protocol_error};
    }
  }
  if (get_u16(bytes, 8) != TcpTransport::protocol_version) {
    return {std::nullopt, TcpTransportErrorCode::wrong_version};
  }
  if (get_u16(bytes, 10) != kHandshakeBytes || get_u32(bytes, 36) != 0) {
    return {std::nullopt, TcpTransportErrorCode::protocol_error};
  }
  const std::uint32_t flags = get_u32(bytes, 12);
  if ((flags & ~kInitiatorFlag) != 0) {
    return {std::nullopt, TcpTransportErrorCode::protocol_error};
  }
  Handshake handshake;
  handshake.identity.cluster_id_high = get_u64(bytes, 16);
  handshake.identity.cluster_id_low = get_u64(bytes, 24);
  handshake.identity.node_id = get_u32(bytes, 32);
  handshake.incarnation = get_u64(bytes, 40);
  handshake.nonce = get_u64(bytes, 48);
  handshake.initiator = (flags & kInitiatorFlag) != 0;
  return {handshake, TcpTransportErrorCode::none};
}

[[nodiscard]] std::uint64_t automatic_incarnation(NodeId node) noexcept {
  static std::atomic<std::uint64_t> counter{1};
  const auto now = std::chrono::duration_cast<std::chrono::nanoseconds>(
                       std::chrono::system_clock::now().time_since_epoch())
                       .count();
  std::uint64_t value = static_cast<std::uint64_t>(now) ^
                        (static_cast<std::uint64_t>(node) << 32U) ^
                        counter.fetch_add(1, std::memory_order_relaxed);
  if (value == 0) value = 1;
  return value;
}

struct IoResult {
  enum class Status : std::uint8_t { progress, again, eof, error };
  Status status{Status::error};
  std::size_t count{};
  int native_error{};
};

[[nodiscard]] IoResult socket_send(NativeSocket socket,
                                   std::span<const std::byte> bytes) noexcept {
  if (bytes.empty()) return {IoResult::Status::progress, 0, 0};
#ifdef _WIN32
  const int amount = static_cast<int>(std::min<std::size_t>(
      bytes.size(), static_cast<std::size_t>(std::numeric_limits<int>::max())));
  const int sent = ::send(socket, reinterpret_cast<const char*>(bytes.data()),
                          amount, 0);
  if (sent > 0) {
    return {IoResult::Status::progress, static_cast<std::size_t>(sent), 0};
  }
  if (sent == 0) return {IoResult::Status::eof, 0, 0};
#else
  int flags = 0;
#ifdef MSG_NOSIGNAL
  flags |= MSG_NOSIGNAL;
#endif
  const ssize_t sent = ::send(socket, bytes.data(), bytes.size(), flags);
  if (sent > 0) {
    return {IoResult::Status::progress, static_cast<std::size_t>(sent), 0};
  }
  if (sent == 0) return {IoResult::Status::eof, 0, 0};
#endif
  const int error = last_socket_error();
  if (would_block_error(error) || interrupted_error(error)) {
    return {IoResult::Status::again, 0, error};
  }
  return {IoResult::Status::error, 0, error};
}

[[nodiscard]] IoResult socket_receive(NativeSocket socket,
                                      std::span<std::byte> bytes) noexcept {
  if (bytes.empty()) return {IoResult::Status::again, 0, 0};
#ifdef _WIN32
  const int amount = static_cast<int>(std::min<std::size_t>(
      bytes.size(), static_cast<std::size_t>(std::numeric_limits<int>::max())));
  const int received =
      recv(socket, reinterpret_cast<char*>(bytes.data()), amount, 0);
  if (received > 0) {
    return {IoResult::Status::progress, static_cast<std::size_t>(received), 0};
  }
  if (received == 0) return {IoResult::Status::eof, 0, 0};
#else
  const ssize_t received = recv(socket, bytes.data(), bytes.size(), 0);
  if (received > 0) {
    return {IoResult::Status::progress,
            static_cast<std::size_t>(received), 0};
  }
  if (received == 0) return {IoResult::Status::eof, 0, 0};
#endif
  const int error = last_socket_error();
  if (would_block_error(error) || interrupted_error(error)) {
    return {IoResult::Status::again, 0, error};
  }
  return {IoResult::Status::error, 0, error};
}

struct ConnectionKey {
  std::uint64_t lower_node_incarnation{};
  std::uint64_t higher_node_incarnation{};
  NodeId initiator{};
  std::uint64_t nonce{};

  friend bool operator==(const ConnectionKey&, const ConnectionKey&) = default;
};

[[nodiscard]] bool key_less(const ConnectionKey& lhs,
                            const ConnectionKey& rhs) noexcept {
  return std::tie(lhs.lower_node_incarnation, lhs.higher_node_incarnation,
                  lhs.initiator, lhs.nonce) <
         std::tie(rhs.lower_node_incarnation, rhs.higher_node_incarnation,
                  rhs.initiator, rhs.nonce);
}

struct OutboundFrame {
  std::vector<std::byte> wire_bytes;
  std::size_t offset{};
};

enum class ConnectionPhase : std::uint8_t {
  connecting,
  handshake,
  established,
};

struct Connection {
  NativeSocket socket{kInvalidSocket};
  ConnectionPhase phase{ConnectionPhase::handshake};
  bool outbound{};
  std::optional<NodeId> expected_peer;
  std::uint64_t local_nonce{};
  std::array<std::byte, kHandshakeBytes> local_handshake{};
  std::array<std::byte, kHandshakeBytes> remote_handshake{};
  std::size_t handshake_sent{};
  std::size_t handshake_received{};
  std::chrono::steady_clock::time_point deadline;
  NodeId peer{};
  std::uint64_t peer_incarnation{};
  ConnectionKey key;
  std::vector<std::byte> inbound;
};

struct PeerState {
  TcpPeerEndpoint endpoint;
  sockaddr_in address{};
  bool has_outbound_endpoint{};
  bool partitioned{};
  bool connected{};
  NativeSocket active_socket{kInvalidSocket};
  ConnectionKey active_key;
  std::uint64_t peer_incarnation{};
  std::deque<OutboundFrame> outbound;
  std::size_t queued_bytes{};
  std::chrono::steady_clock::time_point next_attempt{};
};

}  // namespace

std::string_view to_string(TcpTransportErrorCode error) noexcept {
  switch (error) {
    case TcpTransportErrorCode::none:
      return "none";
    case TcpTransportErrorCode::invalid_configuration:
      return "invalid_configuration";
    case TcpTransportErrorCode::self_peer:
      return "self_peer";
    case TcpTransportErrorCode::unknown_peer:
      return "unknown_peer";
    case TcpTransportErrorCode::duplicate_peer:
      return "duplicate_peer";
    case TcpTransportErrorCode::wrong_cluster:
      return "wrong_cluster";
    case TcpTransportErrorCode::wrong_version:
      return "wrong_version";
    case TcpTransportErrorCode::protocol_error:
      return "protocol_error";
    case TcpTransportErrorCode::frame_too_large:
      return "frame_too_large";
    case TcpTransportErrorCode::queue_full:
      return "queue_full";
    case TcpTransportErrorCode::connection_down:
      return "connection_down";
    case TcpTransportErrorCode::partitioned:
      return "partitioned";
    case TcpTransportErrorCode::io_error:
      return "io_error";
    case TcpTransportErrorCode::stopped:
      return "stopped";
  }
  return "unknown";
}

TcpTransportError::TcpTransportError(TcpTransportErrorCode code,
                                     std::string message)
    : std::runtime_error(std::move(message)), code_(code) {}

class TcpTransport::Impl {
 public:
  explicit Impl(TcpTransportConfig config)
      : config_(std::move(config)) {
    validate_config();
    if (config_.incarnation == 0) {
      config_.incarnation = automatic_incarnation(config_.identity.node_id);
    }
    for (const TcpPeerEndpoint& endpoint : config_.peers) {
      PeerState peer;
      peer.endpoint = endpoint;
      peer.address = resolve_ipv4(endpoint.address, endpoint.port,
                                  config_.allow_non_loopback);
      peer.has_outbound_endpoint = endpoint.port != 0;
      peers_.emplace(endpoint.node_id, std::move(peer));
    }
  }

  ~Impl() { stop(); }

  void start() {
    std::lock_guard lock(mutex_);
    if (started_) {
      throw TcpTransportError(TcpTransportErrorCode::invalid_configuration,
                              "TCP transport is one-shot and already started");
    }
    listener_ = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (listener_ == kInvalidSocket) {
      throw TcpTransportError(TcpTransportErrorCode::io_error,
                              "cannot create TCP listener: " +
                                  std::to_string(last_socket_error()));
    }
    if (!select_compatible(listener_)) {
      close_socket(listener_);
      throw TcpTransportError(
          TcpTransportErrorCode::io_error,
          "TCP listener descriptor exceeds select() capacity");
    }
    const int enabled = 1;
    (void)setsockopt(listener_, SOL_SOCKET, SO_REUSEADDR,
                     reinterpret_cast<const char*>(&enabled),
                     static_cast<NativeSocketLength>(sizeof(enabled)));
    if (!set_nonblocking(listener_)) {
      const int error = last_socket_error();
      close_socket(listener_);
      throw TcpTransportError(TcpTransportErrorCode::io_error,
                              "cannot make TCP listener nonblocking: " +
                                  std::to_string(error));
    }
    sockaddr_in address = resolve_ipv4(config_.listen_address,
                                       config_.listen_port,
                                       config_.allow_non_loopback);
    if (bind(listener_, reinterpret_cast<const sockaddr*>(&address),
             static_cast<NativeSocketLength>(sizeof(address))) != 0 ||
        listen(listener_, static_cast<int>(config_.limits.max_pending_connections)) !=
            0) {
      const int error = last_socket_error();
      close_socket(listener_);
      throw TcpTransportError(TcpTransportErrorCode::io_error,
                              "cannot bind/listen TCP transport: " +
                                  std::to_string(error));
    }
    sockaddr_in actual{};
    NativeSocketLength actual_size =
        static_cast<NativeSocketLength>(sizeof(actual));
    if (getsockname(listener_, reinterpret_cast<sockaddr*>(&actual),
                    &actual_size) != 0) {
      const int error = last_socket_error();
      close_socket(listener_);
      throw TcpTransportError(TcpTransportErrorCode::io_error,
                              "cannot inspect TCP listener: " +
                                  std::to_string(error));
    }
    listening_port_ = ntohs(actual.sin_port);
    started_ = true;
    stopping_.store(false, std::memory_order_release);
    running_.store(true, std::memory_order_release);
    try {
      io_thread_ = std::thread([this] { io_loop(); });
    } catch (...) {
      running_.store(false, std::memory_order_release);
      close_socket(listener_);
      throw;
    }
  }

  void stop() noexcept {
    stopping_.store(true, std::memory_order_release);
    if (io_thread_.joinable()) io_thread_.join();
    running_.store(false, std::memory_order_release);
  }

  [[nodiscard]] TcpSendResult send(
      NodeId peer_id, std::span<const std::byte> codec_frame) {
    if (codec_frame.empty() ||
        codec_frame.size() > config_.limits.max_frame_bytes ||
        codec_frame.size() > std::numeric_limits<std::uint32_t>::max()) {
      return {TcpSendStatus::error, TcpTransportErrorCode::frame_too_large, 0};
    }
    const std::size_t wire_size = kLengthPrefixBytes + codec_frame.size();
    std::lock_guard lock(mutex_);
    const auto found = peers_.find(peer_id);
    if (found == peers_.end()) {
      return {TcpSendStatus::error, TcpTransportErrorCode::unknown_peer, 0};
    }
    PeerState& peer = found->second;
    if (fatal_error_) {
      return {TcpSendStatus::error, TcpTransportErrorCode::io_error,
              peer.queued_bytes};
    }
    if (peer.partitioned) {
      return {TcpSendStatus::down, TcpTransportErrorCode::partitioned,
              peer.queued_bytes};
    }
    if (!running_.load(std::memory_order_acquire) || !peer.connected) {
      return {TcpSendStatus::down,
              started_ ? TcpTransportErrorCode::connection_down
                       : TcpTransportErrorCode::stopped,
              peer.queued_bytes};
    }
    if (wire_size > config_.limits.max_outbound_bytes_per_peer ||
        wire_size > config_.limits.max_total_outbound_bytes ||
        peer.queued_bytes >
            config_.limits.max_outbound_bytes_per_peer - wire_size ||
        total_queued_bytes_ >
            config_.limits.max_total_outbound_bytes - wire_size) {
      return {TcpSendStatus::would_block, TcpTransportErrorCode::queue_full,
              peer.queued_bytes};
    }
    try {
      OutboundFrame frame;
      frame.wire_bytes.resize(wire_size);
      put_u32(frame.wire_bytes, 0,
              static_cast<std::uint32_t>(codec_frame.size()));
      std::copy(codec_frame.begin(), codec_frame.end(),
                frame.wire_bytes.begin() +
                    static_cast<std::ptrdiff_t>(kLengthPrefixBytes));
      peer.outbound.push_back(std::move(frame));
    } catch (const std::bad_alloc&) {
      return {TcpSendStatus::error, TcpTransportErrorCode::io_error,
              peer.queued_bytes};
    }
    peer.queued_bytes += wire_size;
    total_queued_bytes_ += wire_size;
    return {TcpSendStatus::queued, TcpTransportErrorCode::none,
            peer.queued_bytes};
  }

  [[nodiscard]] bool set_peer_partitioned(NodeId peer_id,
                                           bool partitioned) {
    std::lock_guard lock(mutex_);
    const auto found = peers_.find(peer_id);
    if (found == peers_.end()) return false;
    PeerState& peer = found->second;
    if (peer.partitioned == partitioned) return true;
    peer.partitioned = partitioned;
    if (!partitioned) return true;

    total_queued_bytes_ -= peer.queued_bytes;
    peer.outbound.clear();
    peer.queued_bytes = 0;
    for (auto event = events_.begin(); event != events_.end();) {
      if (event->peer == peer_id && event->kind == TcpEventKind::message) {
        inbound_event_bytes_ -= event->bytes.size();
        event = events_.erase(event);
      } else {
        ++event;
      }
    }
    return true;
  }

  [[nodiscard]] std::vector<TcpTransportEvent> poll(std::size_t max_events) {
    std::lock_guard lock(mutex_);
    const std::size_t count = std::min(max_events, events_.size());
    std::vector<TcpTransportEvent> result;
    result.reserve(count);
    for (std::size_t index = 0; index < count; ++index) {
      inbound_event_bytes_ -= events_.front().bytes.size();
      result.push_back(std::move(events_.front()));
      events_.pop_front();
    }
    return result;
  }

  [[nodiscard]] bool running() const noexcept {
    return running_.load(std::memory_order_acquire);
  }

  [[nodiscard]] std::uint16_t listening_port() const {
    std::lock_guard lock(mutex_);
    return listening_port_;
  }

  [[nodiscard]] std::uint64_t incarnation() const noexcept {
    return config_.incarnation;
  }

  [[nodiscard]] bool peer_partitioned(NodeId peer_id) const {
    std::lock_guard lock(mutex_);
    const auto found = peers_.find(peer_id);
    return found != peers_.end() && found->second.partitioned;
  }

 private:
  void validate_config() {
    if (config_.identity.node_id == 0) {
      throw TcpTransportError(TcpTransportErrorCode::invalid_configuration,
                              "TCP node id zero is invalid");
    }
    const TcpTransportLimits& limits = config_.limits;
    if (limits.max_frame_bytes == 0 ||
        limits.max_frame_bytes > kAbsoluteMaxFrameBytes ||
        limits.max_frame_bytes > std::numeric_limits<std::uint32_t>::max() ||
        limits.max_outbound_bytes_per_peer < kLengthPrefixBytes ||
        limits.max_total_outbound_bytes < kLengthPrefixBytes ||
        limits.max_inbound_events == 0 ||
        limits.max_inbound_event_bytes < limits.max_frame_bytes ||
        limits.max_pending_connections == 0 ||
        limits.max_pending_connections >= FD_SETSIZE ||
        limits.handshake_timeout_ms == 0 || limits.reconnect_delay_ms == 0 ||
        limits.io_poll_interval_ms == 0) {
      throw TcpTransportError(TcpTransportErrorCode::invalid_configuration,
                              "invalid TCP transport limits");
    }
    (void)resolve_ipv4(config_.listen_address, config_.listen_port,
                       config_.allow_non_loopback);
    std::map<NodeId, bool> seen;
    for (const TcpPeerEndpoint& peer : config_.peers) {
      if (peer.node_id == config_.identity.node_id) {
        throw TcpTransportError(TcpTransportErrorCode::self_peer,
                                "TCP peer list contains the local node");
      }
      if (peer.node_id == 0) {
        throw TcpTransportError(TcpTransportErrorCode::unknown_peer,
                                "TCP peer id zero is invalid");
      }
      if (!seen.emplace(peer.node_id, true).second) {
        throw TcpTransportError(TcpTransportErrorCode::duplicate_peer,
                                "TCP peer list contains a duplicate node");
      }
      (void)resolve_ipv4(peer.address, peer.port,
                         config_.allow_non_loopback);
    }
    if (config_.peers.size() + 1U > limits.max_pending_connections) {
      throw TcpTransportError(TcpTransportErrorCode::invalid_configuration,
                              "connection bound is smaller than peer set");
    }
  }

  [[nodiscard]] std::chrono::milliseconds reconnect_delay() const noexcept {
    return std::chrono::milliseconds(config_.limits.reconnect_delay_ms);
  }

  [[nodiscard]] std::chrono::milliseconds handshake_timeout() const noexcept {
    return std::chrono::milliseconds(config_.limits.handshake_timeout_ms);
  }

  [[nodiscard]] bool has_connection_for(
      const std::vector<Connection>& connections, NodeId peer) const noexcept {
    return std::any_of(connections.begin(), connections.end(),
                       [peer](const Connection& connection) {
                         return connection.socket != kInvalidSocket &&
                                ((connection.expected_peer &&
                                  *connection.expected_peer == peer) ||
                                 (connection.phase ==
                                      ConnectionPhase::established &&
                                  connection.peer == peer));
                       });
  }

  void attempt_outgoing(std::vector<Connection>& connections) {
    const auto now = std::chrono::steady_clock::now();
    for (auto& [peer_id, peer] : peers_) {
      if (!peer.has_outbound_endpoint || peer.next_attempt > now ||
          has_connection_for(connections, peer_id)) {
        continue;
      }
      {
        std::lock_guard lock(mutex_);
        if (peer.connected || peer.partitioned) continue;
      }
      if (connections.size() >= config_.limits.max_pending_connections) break;
      NativeSocket socket = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
      if (socket == kInvalidSocket || !select_compatible(socket) ||
          !set_nonblocking(socket)) {
        close_socket(socket);
        peer.next_attempt = now + reconnect_delay();
        continue;
      }
      configure_connected_socket(socket);
      Connection connection;
      connection.socket = socket;
      connection.outbound = true;
      connection.expected_peer = peer_id;
      connection.local_nonce = next_nonce_++;
      if (connection.local_nonce == 0) connection.local_nonce = next_nonce_++;
      connection.local_handshake =
          make_handshake(config_.identity, config_.incarnation,
                         connection.local_nonce, true);
      connection.deadline = now + handshake_timeout();
      const int status = connect(
          socket, reinterpret_cast<const sockaddr*>(&peer.address),
          static_cast<NativeSocketLength>(sizeof(peer.address)));
      if (status == 0) {
        connection.phase = ConnectionPhase::handshake;
      } else if (connect_pending_error(last_socket_error())) {
        connection.phase = ConnectionPhase::connecting;
      } else {
        close_socket(connection.socket);
        peer.next_attempt = now + reconnect_delay();
        continue;
      }
      connections.push_back(std::move(connection));
    }
  }

  void accept_connections(std::vector<Connection>& connections) {
    // Keep an accept flood from monopolizing the single I/O owner or delaying
    // stop(). The listener remains readable and is revisited next poll.
    for (std::size_t accepted = 0;
         accepted < config_.limits.max_pending_connections &&
         !stopping_.load(std::memory_order_acquire);
         ++accepted) {
      sockaddr_in remote{};
      NativeSocketLength remote_size =
          static_cast<NativeSocketLength>(sizeof(remote));
      NativeSocket socket = accept(listener_, reinterpret_cast<sockaddr*>(&remote),
                                   &remote_size);
      if (socket == kInvalidSocket) {
        const int error = last_socket_error();
        if (would_block_error(error) || interrupted_error(error)) return;
        enqueue_diagnostic(TcpEventKind::transport_error, 0, 0,
                           TcpTransportErrorCode::io_error);
        return;
      }
      if ((!config_.allow_non_loopback && !is_loopback(remote.sin_addr)) ||
          connections.size() >= config_.limits.max_pending_connections ||
          !select_compatible(socket) ||
          !set_nonblocking(socket)) {
        close_socket(socket);
        enqueue_diagnostic(TcpEventKind::peer_rejected, 0, 0,
                           TcpTransportErrorCode::protocol_error);
        continue;
      }
      configure_connected_socket(socket);
      Connection connection;
      connection.socket = socket;
      connection.phase = ConnectionPhase::handshake;
      connection.outbound = false;
      connection.local_nonce = next_nonce_++;
      if (connection.local_nonce == 0) connection.local_nonce = next_nonce_++;
      connection.local_handshake =
          make_handshake(config_.identity, config_.incarnation,
                         connection.local_nonce, false);
      connection.deadline =
          std::chrono::steady_clock::now() + handshake_timeout();
      connections.push_back(std::move(connection));
    }
  }

  void enqueue_diagnostic(TcpEventKind kind, NodeId peer,
                          std::uint64_t peer_incarnation,
                          TcpTransportErrorCode error) {
    std::lock_guard lock(mutex_);
    if (events_.size() >= config_.limits.max_inbound_events) return;
    events_.push_back(
        TcpTransportEvent{kind, peer, peer_incarnation, error, {}});
  }

  [[nodiscard]] bool enqueue_message(Connection& connection,
                                     std::size_t payload_offset,
                                     std::size_t payload_size) {
    std::lock_guard lock(mutex_);
    const auto peer = peers_.find(connection.peer);
    if (peer != peers_.end() && peer->second.partitioned) return true;
    if (events_.size() >= config_.limits.max_inbound_events ||
        payload_size >
            config_.limits.max_inbound_event_bytes -
                std::min(config_.limits.max_inbound_event_bytes,
                         inbound_event_bytes_)) {
      return false;
    }
    TcpTransportEvent event;
    event.kind = TcpEventKind::message;
    event.peer = connection.peer;
    event.peer_incarnation = connection.peer_incarnation;
    event.bytes.assign(connection.inbound.begin() +
                           static_cast<std::ptrdiff_t>(payload_offset),
                       connection.inbound.begin() +
                           static_cast<std::ptrdiff_t>(payload_offset +
                                                       payload_size));
    inbound_event_bytes_ += payload_size;
    events_.push_back(std::move(event));
    return true;
  }

  [[nodiscard]] bool process_inbound(Connection& connection) {
    for (;;) {
      if (connection.inbound.size() < kLengthPrefixBytes) return true;
      const std::uint32_t payload_size = get_u32(connection.inbound, 0);
      if (payload_size == 0 || payload_size > config_.limits.max_frame_bytes) {
        return false;
      }
      const std::size_t wire_size =
          kLengthPrefixBytes + static_cast<std::size_t>(payload_size);
      if (connection.inbound.size() < wire_size) return true;
      if (peer_partitioned(connection.peer)) {
        connection.inbound.erase(
            connection.inbound.begin(),
            connection.inbound.begin() +
                static_cast<std::ptrdiff_t>(wire_size));
        continue;
      }
      if (!enqueue_message(connection, kLengthPrefixBytes, payload_size)) {
        return true;
      }
      connection.inbound.erase(
          connection.inbound.begin(),
          connection.inbound.begin() + static_cast<std::ptrdiff_t>(wire_size));
    }
  }

  [[nodiscard]] bool can_read_established(
      const Connection& connection) const {
    if (connection.inbound.size() >=
        kLengthPrefixBytes + config_.limits.max_frame_bytes) {
      return false;
    }
    std::lock_guard lock(mutex_);
    return events_.size() < config_.limits.max_inbound_events &&
           inbound_event_bytes_ < config_.limits.max_inbound_event_bytes;
  }

  [[nodiscard]] bool has_outbound_data(const Connection& connection) const {
    std::lock_guard lock(mutex_);
    const auto found = peers_.find(connection.peer);
    return found != peers_.end() && found->second.connected &&
           found->second.active_socket == connection.socket &&
           !found->second.outbound.empty();
  }

  void reset_outbound_offsets(PeerState& peer) noexcept {
    for (OutboundFrame& frame : peer.outbound) frame.offset = 0;
  }

  void close_connection(Connection& connection, TcpTransportErrorCode error,
                        bool notify) {
    const NativeSocket old_socket = connection.socket;
    close_socket(connection.socket);
    if (connection.expected_peer) {
      peers_.at(*connection.expected_peer).next_attempt =
          std::chrono::steady_clock::now() + reconnect_delay();
    }
    if (connection.phase != ConnectionPhase::established) return;
    std::lock_guard lock(mutex_);
    PeerState& peer = peers_.at(connection.peer);
    if (peer.active_socket != old_socket) return;
    peer.connected = false;
    peer.active_socket = kInvalidSocket;
    reset_outbound_offsets(peer);
    peer.next_attempt = std::chrono::steady_clock::now() + reconnect_delay();
    if (notify && events_.size() < config_.limits.max_inbound_events) {
      events_.push_back(TcpTransportEvent{TcpEventKind::peer_down,
                                          connection.peer,
                                          connection.peer_incarnation, error,
                                          {}});
    }
  }

  [[nodiscard]] TcpTransportErrorCode validate_remote(
      const Connection& connection, const Handshake& remote) const {
    if (remote.initiator == connection.outbound || remote.incarnation == 0 ||
        (remote.initiator && remote.nonce == 0)) {
      return TcpTransportErrorCode::protocol_error;
    }
    if (remote.identity.cluster_id_high !=
            config_.identity.cluster_id_high ||
        remote.identity.cluster_id_low != config_.identity.cluster_id_low) {
      return TcpTransportErrorCode::wrong_cluster;
    }
    if (remote.identity.node_id == config_.identity.node_id) {
      return TcpTransportErrorCode::self_peer;
    }
    if (!peers_.contains(remote.identity.node_id)) {
      return TcpTransportErrorCode::unknown_peer;
    }
    if (connection.expected_peer &&
        *connection.expected_peer != remote.identity.node_id) {
      return TcpTransportErrorCode::unknown_peer;
    }
    return TcpTransportErrorCode::none;
  }

  [[nodiscard]] ConnectionKey connection_key(
      const Connection& connection, const Handshake& remote) const noexcept {
    ConnectionKey key;
    if (config_.identity.node_id < remote.identity.node_id) {
      key.lower_node_incarnation = config_.incarnation;
      key.higher_node_incarnation = remote.incarnation;
    } else {
      key.lower_node_incarnation = remote.incarnation;
      key.higher_node_incarnation = config_.incarnation;
    }
    key.initiator = connection.outbound ? config_.identity.node_id
                                        : remote.identity.node_id;
    key.nonce = connection.outbound ? connection.local_nonce : remote.nonce;
    return key;
  }

  [[nodiscard]] bool finish_handshake(Connection& connection,
                                      std::vector<Connection>& connections) {
    const HandshakeResult parsed = parse_handshake(connection.remote_handshake);
    if (!parsed.value) {
      enqueue_diagnostic(TcpEventKind::peer_rejected, 0, 0, parsed.error);
      close_connection(connection, parsed.error, false);
      return false;
    }
    const Handshake& remote = *parsed.value;
    const TcpTransportErrorCode validation =
        validate_remote(connection, remote);
    if (validation != TcpTransportErrorCode::none) {
      enqueue_diagnostic(TcpEventKind::peer_rejected,
                         remote.identity.node_id, remote.incarnation,
                         validation);
      close_connection(connection, validation, false);
      return false;
    }
    connection.peer = remote.identity.node_id;
    connection.peer_incarnation = remote.incarnation;
    connection.key = connection_key(connection, remote);

    if (peer_partitioned(connection.peer)) {
      close_connection(connection, TcpTransportErrorCode::partitioned, false);
      return false;
    }

    NativeSocket replaced_socket = kInvalidSocket;
    bool reject_duplicate = false;
    {
      std::lock_guard lock(mutex_);
      PeerState& peer = peers_.at(connection.peer);
      if (peer.connected) {
        if (!key_less(peer.active_key, connection.key)) {
          reject_duplicate = true;
        } else {
          replaced_socket = peer.active_socket;
        }
      }
      if (!reject_duplicate) {
        peer.connected = true;
        peer.active_socket = connection.socket;
        peer.active_key = connection.key;
        peer.peer_incarnation = connection.peer_incarnation;
        reset_outbound_offsets(peer);
        if (events_.size() < config_.limits.max_inbound_events) {
          events_.push_back(TcpTransportEvent{TcpEventKind::peer_up,
                                              connection.peer,
                                              connection.peer_incarnation,
                                              TcpTransportErrorCode::none,
                                              {}});
        }
      }
    }
    if (reject_duplicate) {
      enqueue_diagnostic(TcpEventKind::peer_rejected, connection.peer,
                         connection.peer_incarnation,
                         TcpTransportErrorCode::duplicate_peer);
      close_connection(connection, TcpTransportErrorCode::duplicate_peer,
                       false);
      return false;
    }
    connection.phase = ConnectionPhase::established;
    connection.inbound.reserve(kLengthPrefixBytes +
                               config_.limits.max_frame_bytes);
    if (replaced_socket != kInvalidSocket) {
      for (Connection& existing : connections) {
        if (&existing != &connection && existing.socket == replaced_socket) {
          close_socket(existing.socket);
          break;
        }
      }
    }
    return true;
  }

  [[nodiscard]] bool complete_connect(Connection& connection) {
    int socket_error = 0;
    NativeSocketLength size =
        static_cast<NativeSocketLength>(sizeof(socket_error));
    if (getsockopt(connection.socket, SOL_SOCKET, SO_ERROR,
                   reinterpret_cast<char*>(&socket_error), &size) != 0 ||
        socket_error != 0) {
      close_connection(connection, TcpTransportErrorCode::connection_down,
                       false);
      return false;
    }
    connection.phase = ConnectionPhase::handshake;
    connection.deadline =
        std::chrono::steady_clock::now() + handshake_timeout();
    return true;
  }

  [[nodiscard]] bool write_handshake(Connection& connection) {
    const auto remaining = std::span<const std::byte>(
        connection.local_handshake.data() + connection.handshake_sent,
        kHandshakeBytes - connection.handshake_sent);
    const IoResult result = socket_send(connection.socket, remaining);
    if (result.status == IoResult::Status::progress) {
      connection.handshake_sent += result.count;
      return true;
    }
    if (result.status == IoResult::Status::again) return true;
    close_connection(connection, TcpTransportErrorCode::connection_down,
                     false);
    return false;
  }

  [[nodiscard]] bool read_handshake(Connection& connection) {
    auto remaining = std::span<std::byte>(
        connection.remote_handshake.data() + connection.handshake_received,
        kHandshakeBytes - connection.handshake_received);
    const IoResult result = socket_receive(connection.socket, remaining);
    if (result.status == IoResult::Status::progress) {
      connection.handshake_received += result.count;
      return true;
    }
    if (result.status == IoResult::Status::again) return true;
    close_connection(connection, TcpTransportErrorCode::connection_down,
                     false);
    return false;
  }

  [[nodiscard]] bool read_frames(Connection& connection) {
    std::array<std::byte, kReadChunkBytes> buffer{};
    const std::size_t capacity =
        kLengthPrefixBytes + config_.limits.max_frame_bytes;
    const std::size_t available = capacity - connection.inbound.size();
    const std::size_t amount = std::min(available, buffer.size());
    const IoResult result =
        socket_receive(connection.socket,
                       std::span<std::byte>(buffer.data(), amount));
    if (result.status == IoResult::Status::progress) {
      connection.inbound.insert(connection.inbound.end(), buffer.begin(),
                                buffer.begin() +
                                    static_cast<std::ptrdiff_t>(result.count));
      if (!process_inbound(connection)) {
        close_connection(connection, TcpTransportErrorCode::protocol_error,
                         true);
        return false;
      }
      return true;
    }
    if (result.status == IoResult::Status::again) return true;
    close_connection(connection, TcpTransportErrorCode::connection_down, true);
    return false;
  }

  [[nodiscard]] bool write_frame(Connection& connection) {
    std::lock_guard lock(mutex_);
    PeerState& peer = peers_.at(connection.peer);
    if (!peer.connected || peer.active_socket != connection.socket ||
        peer.outbound.empty()) {
      return true;
    }
    OutboundFrame& frame = peer.outbound.front();
    const auto remaining = std::span<const std::byte>(
        frame.wire_bytes.data() + frame.offset,
        frame.wire_bytes.size() - frame.offset);
    const IoResult result = socket_send(connection.socket, remaining);
    if (result.status == IoResult::Status::progress) {
      frame.offset += result.count;
      if (frame.offset == frame.wire_bytes.size()) {
        peer.queued_bytes -= frame.wire_bytes.size();
        total_queued_bytes_ -= frame.wire_bytes.size();
        peer.outbound.pop_front();
      }
      return true;
    }
    if (result.status == IoResult::Status::again) return true;
    return false;
  }

  void mark_fatal(TcpTransportErrorCode error) noexcept {
    std::lock_guard lock(mutex_);
    fatal_error_ = true;
    try {
      if (events_.size() < config_.limits.max_inbound_events) {
        events_.push_back(TcpTransportEvent{TcpEventKind::transport_error, 0,
                                            0, error, {}});
      }
    } catch (...) {
      // A fatal allocation failure cannot safely allocate its own diagnostic.
    }
  }

  void io_loop() noexcept {
    std::vector<Connection> connections;
    try {
      while (!stopping_.load(std::memory_order_acquire)) {
        connections.erase(
            std::remove_if(connections.begin(), connections.end(),
                           [](const Connection& connection) {
                             return connection.socket == kInvalidSocket;
                           }),
            connections.end());
        for (Connection& connection : connections) {
          const std::optional<NodeId> peer =
              connection.phase == ConnectionPhase::established
                  ? std::optional<NodeId>{connection.peer}
                  : connection.expected_peer;
          if (peer && peer_partitioned(*peer)) {
            close_connection(connection, TcpTransportErrorCode::partitioned,
                             connection.phase ==
                                 ConnectionPhase::established);
          }
        }
        attempt_outgoing(connections);
        for (Connection& connection : connections) {
          if (connection.phase == ConnectionPhase::established &&
              !process_inbound(connection)) {
            close_connection(connection,
                             TcpTransportErrorCode::protocol_error, true);
          }
        }

        fd_set read_set;
        fd_set write_set;
        fd_set error_set;
        FD_ZERO(&read_set);
        FD_ZERO(&write_set);
        FD_ZERO(&error_set);
        FD_SET(listener_, &read_set);
        NativeSocket highest = listener_;
        for (Connection& connection : connections) {
          if (connection.socket == kInvalidSocket) continue;
          if (!select_compatible(connection.socket)) {
            close_connection(connection, TcpTransportErrorCode::io_error,
                             connection.phase ==
                                 ConnectionPhase::established);
            continue;
          }
          highest = std::max(highest, connection.socket);
          FD_SET(connection.socket, &error_set);
          if (connection.phase == ConnectionPhase::connecting) {
            FD_SET(connection.socket, &write_set);
          } else if (connection.phase == ConnectionPhase::handshake) {
            if (connection.handshake_received < kHandshakeBytes) {
              FD_SET(connection.socket, &read_set);
            }
            if (connection.handshake_sent < kHandshakeBytes) {
              FD_SET(connection.socket, &write_set);
            }
          } else {
            if (can_read_established(connection)) {
              FD_SET(connection.socket, &read_set);
            }
            if (has_outbound_data(connection)) {
              FD_SET(connection.socket, &write_set);
            }
          }
        }
        timeval timeout{};
        timeout.tv_sec = 0;
        timeout.tv_usec = static_cast<long>(
            std::min<std::uint64_t>(
                static_cast<std::uint64_t>(config_.limits.io_poll_interval_ms) *
                    1000U,
                999999U));
#ifdef _WIN32
        const int selected =
            select(0, &read_set, &write_set, &error_set, &timeout);
#else
        const int selected = select(highest + 1, &read_set, &write_set,
                                    &error_set, &timeout);
#endif
        if (selected < 0) {
          const int error = last_socket_error();
          if (interrupted_error(error)) continue;
          throw TcpTransportError(TcpTransportErrorCode::io_error,
                                  "TCP select failed: " +
                                      std::to_string(error));
        }
        if (FD_ISSET(listener_, &read_set)) accept_connections(connections);

        const auto now = std::chrono::steady_clock::now();
        for (Connection& connection : connections) {
          if (connection.socket == kInvalidSocket) continue;
          if (connection.phase != ConnectionPhase::established &&
              now >= connection.deadline) {
            close_connection(connection,
                             TcpTransportErrorCode::connection_down, false);
            continue;
          }
          if (FD_ISSET(connection.socket, &error_set)) {
            close_connection(connection,
                             TcpTransportErrorCode::connection_down,
                             connection.phase ==
                                 ConnectionPhase::established);
            continue;
          }
          if (connection.phase == ConnectionPhase::connecting &&
              FD_ISSET(connection.socket, &write_set)) {
            if (!complete_connect(connection)) continue;
          }
          if (connection.phase == ConnectionPhase::handshake) {
            if (FD_ISSET(connection.socket, &write_set) &&
                !write_handshake(connection)) {
              continue;
            }
            if (FD_ISSET(connection.socket, &read_set) &&
                !read_handshake(connection)) {
              continue;
            }
            if (connection.handshake_sent == kHandshakeBytes &&
                connection.handshake_received == kHandshakeBytes) {
              (void)finish_handshake(connection, connections);
            }
            continue;
          }
          if (connection.phase == ConnectionPhase::established) {
            if (FD_ISSET(connection.socket, &read_set) &&
                !read_frames(connection)) {
              continue;
            }
            if (connection.socket != kInvalidSocket &&
                FD_ISSET(connection.socket, &write_set) &&
                !write_frame(connection)) {
              close_connection(connection,
                               TcpTransportErrorCode::connection_down, true);
            }
          }
        }
      }
    } catch (const std::exception&) {
      mark_fatal(TcpTransportErrorCode::io_error);
    } catch (...) {
      mark_fatal(TcpTransportErrorCode::io_error);
    }

    for (Connection& connection : connections) close_socket(connection.socket);
    close_socket(listener_);
    {
      std::lock_guard lock(mutex_);
      for (auto& [unused, peer] : peers_) {
        (void)unused;
        peer.connected = false;
        peer.active_socket = kInvalidSocket;
        peer.outbound.clear();
        peer.queued_bytes = 0;
      }
      total_queued_bytes_ = 0;
    }
    running_.store(false, std::memory_order_release);
  }

  SocketRuntime socket_runtime_;
  TcpTransportConfig config_;
  mutable std::mutex mutex_;
  std::map<NodeId, PeerState> peers_;
  std::deque<TcpTransportEvent> events_;
  std::size_t inbound_event_bytes_{};
  std::size_t total_queued_bytes_{};
  NativeSocket listener_{kInvalidSocket};
  std::uint16_t listening_port_{};
  std::uint64_t next_nonce_{1};
  std::thread io_thread_;
  std::atomic<bool> stopping_{false};
  std::atomic<bool> running_{false};
  bool started_{};
  bool fatal_error_{};
};

TcpTransport::TcpTransport(TcpTransportConfig config)
    : impl_(std::make_unique<Impl>(std::move(config))) {}

TcpTransport::~TcpTransport() = default;

void TcpTransport::start() { impl_->start(); }

void TcpTransport::stop() noexcept { impl_->stop(); }

TcpSendResult TcpTransport::send(
    NodeId peer, std::span<const std::byte> codec_frame) {
  return impl_->send(peer, codec_frame);
}

std::vector<TcpTransportEvent> TcpTransport::poll(std::size_t max_events) {
  return impl_->poll(max_events);
}

bool TcpTransport::set_peer_partitioned(NodeId peer,
                                        bool partitioned) {
  return impl_->set_peer_partitioned(peer, partitioned);
}

bool TcpTransport::running() const noexcept { return impl_->running(); }

std::uint16_t TcpTransport::listening_port() const {
  return impl_->listening_port();
}

std::uint64_t TcpTransport::incarnation() const noexcept {
  return impl_->incarnation();
}

}  // namespace detlog
