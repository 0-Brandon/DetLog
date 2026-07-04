#include "detlog/codec.hpp"
#include "detlog/tcp_transport.hpp"

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <iostream>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace {

using detlog::Message;
using detlog::NodeId;
using detlog::RequestVote;
using detlog::RequestVoteResponse;
using detlog::TcpEventKind;
using detlog::TcpIdentity;
using detlog::TcpPeerEndpoint;
using detlog::TcpSendStatus;
using detlog::TcpTransport;
using detlog::TcpTransportConfig;
using detlog::TcpTransportErrorCode;
using detlog::TcpTransportEvent;

[[noreturn]] void fail(std::string message, int line) {
  throw std::runtime_error("line " + std::to_string(line) + ": " + message);
}

#define REQUIRE(condition)                                                   \
  do {                                                                       \
    if (!(condition)) fail("requirement failed: " #condition, __LINE__);    \
  } while (false)

constexpr std::uint64_t kClusterHigh = 0x0102030405060708ULL;
constexpr std::uint64_t kClusterLow = 0x1112131415161718ULL;

[[nodiscard]] TcpTransportConfig config(NodeId node, NodeId peer,
                                        std::uint16_t peer_port,
                                        std::uint64_t cluster_high =
                                            kClusterHigh) {
  TcpTransportConfig result;
  result.identity = TcpIdentity{cluster_high, kClusterLow, node};
  result.incarnation = 1000U + node;
  result.peers = {TcpPeerEndpoint{peer, "127.0.0.1", peer_port}};
  result.limits.max_frame_bytes = 1024;
  result.limits.max_outbound_bytes_per_peer = 256;
  result.limits.max_total_outbound_bytes = 512;
  result.limits.max_inbound_events = 32;
  result.limits.max_inbound_event_bytes = 4096;
  result.limits.max_pending_connections = 8;
  result.limits.handshake_timeout_ms = 1000;
  result.limits.reconnect_delay_ms = 25;
  result.limits.io_poll_interval_ms = 2;
  return result;
}

template <typename Predicate>
[[nodiscard]] std::optional<TcpTransportEvent> wait_for_event(
    TcpTransport& transport, Predicate&& predicate,
    std::chrono::milliseconds timeout = std::chrono::seconds(5)) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    for (TcpTransportEvent& event : transport.poll()) {
      if (std::forward<Predicate>(predicate)(event)) {
        return std::move(event);
      }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
  }
  return std::nullopt;
}

void wait_until_connected(TcpTransport& first, NodeId first_peer,
                          TcpTransport& second, NodeId second_peer) {
  const auto first_up = wait_for_event(
      first, [first_peer](const TcpTransportEvent& event) {
        return event.kind == TcpEventKind::peer_up && event.peer == first_peer;
      });
  REQUIRE(first_up.has_value());
  const auto second_up = wait_for_event(
      second, [second_peer](const TcpTransportEvent& event) {
        return event.kind == TcpEventKind::peer_up && event.peer == second_peer;
      });
  REQUIRE(second_up.has_value());
}

void test_codec_roundtrip_and_bounds() {
  TcpTransport first(config(1, 2, 0));
  first.start();
  REQUIRE(first.listening_port() != 0);

  TcpTransport second(config(2, 1, first.listening_port()));
  second.start();
  wait_until_connected(first, 2, second, 1);

  detlog::codec::Limits codec_limits;
  codec_limits.max_frame_bytes = 1024;
  const Message request = RequestVote{7, 1, 19, 6};
  const auto encoded_request =
      detlog::codec::encode_message(request, codec_limits);
  REQUIRE(encoded_request);
  const auto first_send = first.send(2, *encoded_request);
  REQUIRE(first_send.status == TcpSendStatus::queued);
  REQUIRE(first_send.error == TcpTransportErrorCode::none);

  const auto received_request = wait_for_event(
      second, [](const TcpTransportEvent& event) {
        return event.kind == TcpEventKind::message && event.peer == 1;
      });
  REQUIRE(received_request.has_value());
  REQUIRE(received_request->bytes == *encoded_request);
  const auto decoded_request =
      detlog::codec::decode_message(received_request->bytes, codec_limits);
  REQUIRE(decoded_request);
  REQUIRE(*decoded_request == request);

  const Message response = RequestVoteResponse{7, true};
  const auto encoded_response =
      detlog::codec::encode_message(response, codec_limits);
  REQUIRE(encoded_response);
  REQUIRE(second.send(1, *encoded_response).status == TcpSendStatus::queued);
  const auto received_response = wait_for_event(
      first, [](const TcpTransportEvent& event) {
        return event.kind == TcpEventKind::message && event.peer == 2;
      });
  REQUIRE(received_response.has_value());
  REQUIRE(received_response->bytes == *encoded_response);
  const auto decoded_response =
      detlog::codec::decode_message(received_response->bytes, codec_limits);
  REQUIRE(decoded_response);
  REQUIRE(*decoded_response == response);

  // Wire accounting includes the four-byte transport length prefix. This
  // frame is codec-valid in size but can never fit the configured peer queue.
  const std::vector<std::byte> queue_too_large(253, std::byte{0x5a});
  const auto blocked = first.send(2, queue_too_large);
  REQUIRE(blocked.status == TcpSendStatus::would_block);
  REQUIRE(blocked.error == TcpTransportErrorCode::queue_full);

  const std::vector<std::byte> frame_too_large(1025, std::byte{0x2a});
  const auto rejected = first.send(2, frame_too_large);
  REQUIRE(rejected.status == TcpSendStatus::error);
  REQUIRE(rejected.error == TcpTransportErrorCode::frame_too_large);

  first.stop();
  second.stop();
  REQUIRE(!first.running());
  REQUIRE(!second.running());
}

void test_wrong_cluster_is_rejected() {
  TcpTransport accepting(config(1, 2, 0));
  accepting.start();
  const std::array<std::byte, 1> before_connection{std::byte{0x01}};
  const auto down = accepting.send(2, before_connection);
  REQUIRE(down.status == TcpSendStatus::down);
  REQUIRE(down.error == TcpTransportErrorCode::connection_down);

  TcpTransport wrong_cluster(
      config(2, 1, accepting.listening_port(), kClusterHigh + 1U));
  wrong_cluster.start();

  const auto rejected = wait_for_event(
      accepting, [](const TcpTransportEvent& event) {
        return event.kind == TcpEventKind::peer_rejected && event.peer == 2 &&
               event.error == TcpTransportErrorCode::wrong_cluster;
      });
  REQUIRE(rejected.has_value());

  accepting.stop();
  wrong_cluster.stop();
}

}  // namespace

int main() {
  const std::vector<std::pair<std::string_view, std::function<void()>>> tests{
      {"codec roundtrip and bounds", test_codec_roundtrip_and_bounds},
      {"wrong cluster rejection", test_wrong_cluster_is_rejected},
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
