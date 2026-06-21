#include "detlog/codec.hpp"
#include "detlog/simulator.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <limits>
#include <span>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

namespace {

int failures = 0;

#define CHECK(expression)                                                     \
  do {                                                                        \
    if (!(expression)) {                                                      \
      std::cerr << __FILE__ << ':' << __LINE__ << ": check failed: "         \
                << #expression << '\n';                                      \
      ++failures;                                                             \
    }                                                                         \
  } while (false)

[[nodiscard]] std::vector<std::byte> bytes(std::string_view text) {
  const auto view =
      std::as_bytes(std::span<const char>{text.data(), text.size()});
  return {view.begin(), view.end()};
}

[[nodiscard]] std::string text(std::span<const std::byte> value) {
  return {reinterpret_cast<const char*>(value.data()), value.size()};
}

[[nodiscard]] detlog::ClientCommand sample_command() {
  return detlog::ClientCommand{{0x0102030405060708ULL,
                                0x1112131415161718ULL},
                               9,
                               detlog::CommandKind::put,
                               "alpha",
                               "value"};
}

void codec_round_trips() {
  using namespace detlog;
  using namespace detlog::codec;

  const ClientCommand command = sample_command();
  const auto encoded_command = encode_command(command);
  CHECK(encoded_command);
  if (encoded_command) {
    CHECK(encoded_command->size() >= 12);
    CHECK(std::to_integer<unsigned char>((*encoded_command)[0]) == 'D');
    CHECK(std::to_integer<unsigned char>((*encoded_command)[1]) == 'L');
    CHECK(std::to_integer<unsigned char>((*encoded_command)[2]) == 'O');
    CHECK(std::to_integer<unsigned char>((*encoded_command)[3]) == 'G');
    CHECK(std::to_integer<unsigned char>((*encoded_command)[4]) == 1);
    CHECK(std::to_integer<unsigned char>((*encoded_command)[5]) == 1);
    const auto decoded_command = decode_command(*encoded_command);
    CHECK(decoded_command);
    if (decoded_command) {
      CHECK(*decoded_command == command);
    }
  }

  const LogEntry entry{7, 3, command};
  const auto encoded_entry = encode_log_entry(entry);
  CHECK(encoded_entry);
  if (encoded_entry) {
    const auto decoded_entry = decode_log_entry(*encoded_entry);
    CHECK(decoded_entry);
    if (decoded_entry) {
      CHECK(*decoded_entry == entry);
    }
  }

  const std::array<Message, 4> messages{
      Message{RequestVote{5, 2, 17, 4}},
      Message{RequestVoteResponse{5, true}},
      Message{AppendEntries{5,
                            2,
                            77,
                            6,
                            3,
                            {entry, LogEntry{8, 5, std::nullopt}},
                            6}},
      Message{AppendEntriesResponse{5, 77, false, 0, 4, 2}},
  };
  for (const auto& message : messages) {
    const auto encoded = encode_message(message);
    CHECK(encoded);
    if (!encoded) {
      continue;
    }
    const auto decoded = decode_message(*encoded);
    CHECK(decoded);
    if (decoded) {
      CHECK(*decoded == message);
    }
  }
}

void codec_rejects_noncanonical_and_oversized_input() {
  using namespace detlog;
  using namespace detlog::codec;

  Limits limits;
  limits.max_key_bytes = 2;
  const auto too_large = encode_command(sample_command(), limits);
  CHECK(!too_large);
  CHECK(too_large.error == Error::limit_exceeded);

  ClientCommand invalid = sample_command();
  invalid.kind = CommandKind::get;
  const auto invalid_value = encode_command(invalid);
  CHECK(!invalid_value);
  CHECK(invalid_value.error == Error::invalid_value);

  AppendEntries discontinuous{2,
                              1,
                              3,
                              4,
                              1,
                              {LogEntry{6, 2, std::nullopt}},
                              4};
  const auto invalid_append = encode_message(Message{discontinuous});
  CHECK(!invalid_append);
  CHECK(invalid_append.error == Error::invalid_value);

  const auto zero_term_vote = encode_message(Message{RequestVote{
      .term = 0,
      .candidate_id = 2,
      .last_log_index = 0,
      .last_log_term = 0,
  }});
  CHECK(!zero_term_vote);
  CHECK(zero_term_vote.error == Error::invalid_value);

  auto valid_vote = encode_message(Message{RequestVote{
      .term = 1,
      .candidate_id = 2,
      .last_log_index = 0,
      .last_log_term = 0,
  }});
  CHECK(valid_vote);
  if (valid_vote) {
    for (std::size_t offset = 12; offset < 20; ++offset) {
      (*valid_vote)[offset] = std::byte{0};
    }
    const auto decoded_zero_term = decode_message(*valid_vote);
    CHECK(!decoded_zero_term);
    CHECK(decoded_zero_term.error == Error::invalid_value);
  }

  auto vote = encode_message(Message{RequestVoteResponse{9, true}});
  CHECK(vote);
  if (vote) {
    auto bad_bool = *vote;
    bad_bool[20] = std::byte{2};
    const auto decoded = decode_message(bad_bool);
    CHECK(!decoded);
    CHECK(decoded.error == Error::invalid_boolean);

    auto trailing = *vote;
    trailing.push_back(std::byte{0});
    const auto trailing_result = decode_message(trailing);
    CHECK(!trailing_result);
    CHECK(trailing_result.error == Error::trailing_bytes);

    auto reserved = *vote;
    reserved[7] = std::byte{1};
    const auto reserved_result = decode_message(reserved);
    CHECK(!reserved_result);
    CHECK(reserved_result.error == Error::nonzero_reserved);

    auto unknown = *vote;
    unknown[5] = std::byte{0xff};
    const auto unknown_result = decode_message(unknown);
    CHECK(!unknown_result);
    CHECK(unknown_result.error == Error::unknown_type);

    const auto wrong_type = decode_command(*vote);
    CHECK(!wrong_type);
    CHECK(wrong_type.error == Error::unexpected_type);
  }

  const std::array<std::byte, 3> short_frame{
      std::byte{'D'}, std::byte{'L'}, std::byte{'O'}};
  const auto truncated = decode_message(short_frame);
  CHECK(!truncated);
  CHECK(truncated.error == Error::truncated);

  auto empty_append = encode_message(Message{AppendEntries{
      .term = 1,
      .leader_id = 1,
      .rpc_id = 1,
      .prev_log_index = 0,
      .prev_log_term = 0,
      .entries = {},
      .leader_commit = 0,
  }});
  CHECK(empty_append);
  if (empty_append) {
    // Count begins after the 12-byte frame header and fixed AppendEntries
    // fields. A hostile custom limit must not turn a tiny frame into a huge
    // reserve() request.
    for (std::size_t offset = 48; offset < 52; ++offset) {
      (*empty_append)[offset] = std::byte{0xff};
    }
    Limits hostile_limits;
    hostile_limits.max_entries_per_append =
        std::numeric_limits<std::size_t>::max();
    const auto impossible_count =
        decode_message(*empty_append, hostile_limits);
    CHECK(!impossible_count);
    CHECK(impossible_count.error == Error::truncated);
  }
}

void pinned_rng_and_event_order() {
  using namespace detlog;
  using namespace detlog::sim;

  Pcg32 random(42, 54);
  const std::array<std::uint32_t, 5> expected{
      0xa15c02b7U, 0x7b47f409U, 0xba1d3330U, 0x83d2f293U,
      0xbfa4784bU};
  for (const auto value : expected) {
    CHECK(random.next_u32() == value);
  }

  Pcg32 first(1234, 99);
  Pcg32 second(1234, 99);
  for (int i = 0; i < 100; ++i) {
    CHECK(first.bounded(17) == second.bounded(17));
  }
  CHECK(!first.chance(1, 0));
  CHECK(first.chance(5, 5));
  CHECK(first.inclusive(8, 8) == 8);

  EventQueue queue(EventQueueLimits{3, 3});
  const auto ten_first = queue.schedule_at(
      10, EventPayload{UserEvent{1, 11, 1, bytes("first")}});
  const auto five = queue.schedule_at(
      5, EventPayload{UserEvent{1, 22, 1, bytes("earliest")}});
  const auto ten_second = queue.schedule_at(
      10, EventPayload{UserEvent{1, 33, 1, bytes("second")}});
  CHECK(ten_first && five && ten_second);
  CHECK(ten_first.sequence < five.sequence);
  CHECK(five.sequence < ten_second.sequence);
  CHECK(!queue.can_schedule());

  auto event = queue.pop_next();
  CHECK(event);
  if (event) {
    CHECK(event->time == 5);
    CHECK(std::get<UserEvent>(event->payload).tag == 22);
  }
  event = queue.pop_next();
  CHECK(event);
  if (event) {
    CHECK(event->time == 10);
    CHECK(std::get<UserEvent>(event->payload).tag == 11);
  }
  event = queue.pop_next();
  CHECK(event);
  if (event) {
    CHECK(event->time == 10);
    CHECK(std::get<UserEvent>(event->payload).tag == 33);
  }
  const auto total_limited = queue.schedule_at(
      11, EventPayload{UserEvent{1, 44, 1, {}}});
  CHECK(!total_limited);
  CHECK(total_limited.status == ScheduleStatus::total_limit);
}

void timers_use_generation_and_incarnation_tokens() {
  using namespace detlog;
  using namespace detlog::sim;

  Simulator simulator;
  CHECK(simulator.register_node(1));
  const auto old_timer = simulator.arm_timer(1, TimerKind::election, 10);
  const auto new_timer = simulator.arm_timer(1, TimerKind::election, 5);
  CHECK(old_timer && new_timer);
  CHECK(old_timer->generation < new_timer->generation);

  auto event = simulator.next();
  CHECK(event);
  if (event) {
    const auto& timer = std::get<TimerEvent>(event->payload);
    CHECK(timer.generation == new_timer->generation);
    CHECK(event->time == 5);
  }
  CHECK(!simulator.next());  // consumes the stale generation at t=10
  CHECK(simulator.now() == 10);

  const auto incarnation_before = simulator.incarnation(1);
  CHECK(simulator.crash_node(1));
  CHECK(simulator.restart_node(1));
  CHECK(simulator.incarnation(1) == incarnation_before + 1);
}

void failed_timer_rearm_preserves_existing_timer() {
  using namespace detlog;
  using namespace detlog::sim;

  SimulatorConfig config;
  config.events.max_pending_events = 1;
  Simulator simulator(config);
  CHECK(simulator.register_node(1));
  const auto original = simulator.arm_timer(1, TimerKind::election, 5);
  CHECK(original);
  CHECK(!simulator.arm_timer(1, TimerKind::election, 2));
  const auto event = simulator.next();
  CHECK(event);
  if (event) {
    const auto& timer = std::get<TimerEvent>(event->payload);
    CHECK(timer.generation == original->generation);
    CHECK(event->time == 5);
  }
}

void directional_network_faults_and_bounds() {
  using namespace detlog::sim;

  SimulatorConfig config;
  config.transport.max_frame_bytes = 16;
  config.transport.max_queued_bytes_per_link = 12;
  config.transport.max_total_queued_bytes = 20;
  config.transport.max_extra_duplicates_per_send = 2;
  Simulator simulator(config);
  CHECK(simulator.register_node(1));
  CHECK(simulator.register_node(2));

  const auto slow = bytes("slow");
  const auto fast = bytes("go");
  CHECK(simulator.set_link_delay(1, 2, 10));
  CHECK(simulator.send(1, 2, slow));
  CHECK(simulator.set_link_delay(1, 2, 1));
  CHECK(simulator.send(1, 2, fast));

  auto delivery = simulator.next();
  CHECK(delivery);
  if (delivery) {
    const auto& network = std::get<NetworkEvent>(delivery->payload);
    CHECK(text(network.bytes) == "go");
    CHECK(delivery->time == 1);
  }
  delivery = simulator.next();
  CHECK(delivery);
  if (delivery) {
    const auto& network = std::get<NetworkEvent>(delivery->payload);
    CHECK(text(network.bytes) == "slow");
    CHECK(delivery->time == 10);
  }

  CHECK(simulator.drop_next(1, 2));
  CHECK(simulator.send(1, 2, fast).status == SendStatus::dropped);
  CHECK(simulator.set_partition(1, 2, true));
  CHECK(simulator.send(1, 2, fast).status == SendStatus::partitioned);
  CHECK(simulator.send(2, 1, fast));  // directed: reverse link is healthy
  CHECK(simulator.next());
  CHECK(simulator.set_partition(1, 2, false));

  CHECK(simulator.duplicate_next(1, 2, 2));
  const auto duplicated = simulator.send(1, 2, fast);
  CHECK(duplicated);
  CHECK(duplicated.copies == 3);
  CHECK(duplicated.queued_bytes == 6);
  for (std::uint32_t copy = 0; copy < 3; ++copy) {
    const auto event = simulator.next();
    CHECK(event);
    if (event) {
      CHECK(std::get<NetworkEvent>(event->payload).copy_index == copy);
    }
  }
  CHECK(!simulator.duplicate_next(1, 2, 3));

  CHECK(simulator.set_link_delay(1, 2, 20));
  CHECK(simulator.send(1, 2, slow));
  const auto saturated = simulator.send(1, 2, bytes("123456789"));
  CHECK(!saturated);
  CHECK(saturated.status == SendStatus::queue_limit);
  CHECK(simulator.queued_network_bytes() == slow.size());

  CHECK(simulator.crash_node(2));
  CHECK(simulator.restart_node(2));
  CHECK(!simulator.next());  // old-incarnation delivery is suppressed
  CHECK(simulator.queued_network_bytes() == 0);
}

void partition_at_delivery_suppresses_in_flight_packet() {
  using namespace detlog::sim;

  Simulator simulator;
  CHECK(simulator.register_node(1));
  CHECK(simulator.register_node(2));
  CHECK(simulator.send(1, 2, bytes("packet"), 10));
  CHECK(simulator.set_partition(1, 2, true));
  CHECK(!simulator.next());
  CHECK(simulator.now() == 10);
  CHECK(simulator.queued_network_bytes() == 0);
}

void storage_distinguishes_unstable_and_flushed_bytes() {
  using namespace detlog::sim;

  Simulator simulator;
  CHECK(simulator.register_node(7));
  const auto write = simulator.submit_write(7, bytes("abc"), 5);
  const auto flush = simulator.submit_flush(7, 2);
  CHECK(write && flush);
  CHECK(write.completion_time == 5);
  CHECK(flush.completion_time == 7);  // serialized behind write
  CHECK(simulator.pending_storage_bytes(7) == 3);

  auto completion = simulator.next();
  CHECK(completion);
  if (completion) {
    CHECK(std::get<StorageEvent>(completion->payload).kind ==
          StorageOperationKind::write);
  }
  CHECK(text(simulator.stable_bytes(7)).empty());
  CHECK(text(simulator.unstable_bytes(7)) == "abc");
  CHECK(text(simulator.visible_bytes(7)) == "abc");

  completion = simulator.next();
  CHECK(completion);
  if (completion) {
    const auto& event = std::get<StorageEvent>(completion->payload);
    CHECK(event.kind == StorageOperationKind::flush);
    CHECK(event.byte_count == 3);
  }
  CHECK(text(simulator.stable_bytes(7)) == "abc");
  CHECK(text(simulator.unstable_bytes(7)).empty());

  const auto pending = simulator.submit_write(7, bytes("defgh"), 10);
  CHECK(pending);
  CHECK(simulator.crash_node(7, StorageCrashSpec{0, 2}));
  CHECK(text(simulator.stable_bytes(7)) == "abcde");
  CHECK(text(simulator.unstable_bytes(7)).empty());
  CHECK(simulator.pending_storage_bytes(7) == 0);
  CHECK(simulator.restart_node(7));
  CHECK(!simulator.next());  // consumes cancelled old-incarnation completion

  CHECK(simulator.submit_write(7, bytes("XYZ"), 1));
  CHECK(simulator.next());
  CHECK(text(simulator.unstable_bytes(7)) == "XYZ");
  CHECK(simulator.crash_node(7, StorageCrashSpec{1, 0}));
  CHECK(text(simulator.stable_bytes(7)) == "abcdeX");
}

void stopped_storage_can_repair_a_scanned_tail() {
  using namespace detlog::sim;

  Simulator simulator;
  CHECK(simulator.register_node(9));
  CHECK(simulator.initialize_storage(9, bytes("header")));
  CHECK(!simulator.initialize_storage(9, bytes("again")));
  CHECK(simulator.submit_write(9, bytes("frame"), 10));
  CHECK(simulator.crash_node(9, StorageCrashSpec{0, 3}));
  CHECK(text(simulator.stable_bytes(9)) == "headerfra");
  CHECK(simulator.truncate_stable_storage(9, 6));
  CHECK(text(simulator.stable_bytes(9)) == "header");
  CHECK(simulator.restart_node(9));
  CHECK(!simulator.truncate_stable_storage(9, 3));
  CHECK(!simulator.next());
}

void crash_releases_events_and_does_not_compact_across_a_lost_gap() {
  using namespace detlog::sim;

  SimulatorConfig config;
  config.events.max_pending_events = 2;
  config.transport.max_queued_bytes_per_link = 8;
  config.transport.max_total_queued_bytes = 8;
  Simulator simulator(config);
  CHECK(simulator.register_node(1));
  CHECK(simulator.register_node(2));
  CHECK(simulator.send(1, 2, bytes("12345678"), 1'000));
  CHECK(simulator.pending_events() == 1);
  CHECK(simulator.queued_network_bytes() == 8);
  CHECK(simulator.crash_node(2));
  CHECK(simulator.pending_events() == 0);
  CHECK(simulator.queued_network_bytes() == 0);
  CHECK(simulator.restart_node(2));
  CHECK(simulator.send(1, 2, bytes("ok"), 1));
  CHECK(simulator.next());

  Simulator storage;
  CHECK(storage.register_node(3));
  CHECK(storage.submit_write(3, bytes("abcd"), 1));
  CHECK(storage.next());
  CHECK(storage.submit_write(3, bytes("EFGH"), 10));
  CHECK(storage.crash_node(3, StorageCrashSpec{2, 2}));
  CHECK(text(storage.stable_bytes(3)) == "ab");
}

void storage_faults_complete_explicitly_without_false_durability() {
  using namespace detlog::sim;

  Simulator simulator;
  CHECK(simulator.register_node(4));
  CHECK(simulator.fail_next_write(4));
  CHECK(simulator.submit_write(4, bytes("lost"), 1));
  auto event = simulator.next();
  CHECK(event);
  if (event) {
    const auto& storage = std::get<StorageEvent>(event->payload);
    CHECK(storage.status == StorageCompletionStatus::io_error);
    CHECK(storage.byte_count == 0);
  }
  CHECK(simulator.unstable_bytes(4).empty());

  CHECK(simulator.short_write_next(4, 2));
  CHECK(simulator.submit_write(4, bytes("partial"), 1));
  event = simulator.next();
  CHECK(event);
  if (event) {
    const auto& storage = std::get<StorageEvent>(event->payload);
    CHECK(storage.status == StorageCompletionStatus::short_write);
    CHECK(storage.byte_count == 2);
  }
  CHECK(text(simulator.unstable_bytes(4)) == "pa");

  CHECK(simulator.fail_next_flush(4));
  CHECK(simulator.submit_flush(4, 1));
  event = simulator.next();
  CHECK(event);
  if (event) {
    const auto& storage = std::get<StorageEvent>(event->payload);
    CHECK(storage.kind == StorageOperationKind::flush);
    CHECK(storage.status == StorageCompletionStatus::io_error);
  }
  CHECK(simulator.stable_bytes(4).empty());
  CHECK(text(simulator.unstable_bytes(4)) == "pa");

  CHECK(simulator.submit_flush(4, 1));
  event = simulator.next();
  CHECK(event);
  if (event) {
    CHECK(std::get<StorageEvent>(event->payload).status ==
          StorageCompletionStatus::success);
  }
  CHECK(text(simulator.stable_bytes(4)) == "pa");
}

[[nodiscard]] std::string deterministic_scenario_trace() {
  using namespace detlog;
  using namespace detlog::sim;

  Simulator simulator;
  (void)simulator.register_node(1);
  (void)simulator.register_node(2);
  (void)simulator.set_link_delay(1, 2, 3);
  (void)simulator.duplicate_next(1, 2, 1);
  (void)simulator.send(1, 2, bytes("m"));
  (void)simulator.arm_timer(1, TimerKind::heartbeat, 2);
  (void)simulator.submit_write(2, bytes("wal"), 1);
  while (simulator.next()) {
  }
  return simulator.trace_jsonl();
}

void trace_is_stable_jsonl() {
  const auto first = deterministic_scenario_trace();
  const auto second = deterministic_scenario_trace();
  CHECK(first == second);
  CHECK(first.find("\"time\":") != std::string::npos);
  CHECK(first.find("\"sequence\":") != std::string::npos);
  CHECK(first.find("network_delivered") != std::string::npos);
  CHECK(first.find("0x") == std::string::npos);

  const detlog::sim::TraceRecord record{
      1, 1, detlog::sim::TraceKind::stale_event, 1, 0, 0, 1, 0,
      "quote\" newline\n"};
  const auto escaped = detlog::sim::render_jsonl(
      std::span<const detlog::sim::TraceRecord>{&record, 1});
  CHECK(escaped.find("quote\\\" newline\\n") != std::string::npos);
}

}  // namespace

int main() {
  codec_round_trips();
  codec_rejects_noncanonical_and_oversized_input();
  pinned_rng_and_event_order();
  timers_use_generation_and_incarnation_tokens();
  failed_timer_rearm_preserves_existing_timer();
  directional_network_faults_and_bounds();
  partition_at_delivery_suppresses_in_flight_packet();
  storage_distinguishes_unstable_and_flushed_bytes();
  stopped_storage_can_repair_a_scanned_tail();
  crash_releases_events_and_does_not_compact_across_a_lost_gap();
  storage_faults_complete_explicitly_without_false_durability();
  trace_is_stable_jsonl();

  if (failures != 0) {
    std::cerr << failures << " test check(s) failed\n";
    return 1;
  }
  std::cout << "simulator tests passed\n";
  return 0;
}
