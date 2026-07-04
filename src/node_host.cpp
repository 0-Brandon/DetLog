#include "detlog/node_host.hpp"

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

namespace detlog {
namespace {

struct StorageTask {
  StorageOpId operation{};
  PersistBatch batch;
};

class StorageWorker {
 public:
  StorageWorker(std::unique_ptr<Wal> wal, std::size_t max_tasks,
                std::size_t max_completions)
      : wal_(std::move(wal)),
        max_tasks_(max_tasks),
        max_completions_(max_completions) {}

  ~StorageWorker() { stop(); }

  StorageWorker(const StorageWorker&) = delete;
  StorageWorker& operator=(const StorageWorker&) = delete;

  void start() {
    std::lock_guard lock(mutex_);
    if (started_) throw std::logic_error("storage worker already started");
    started_ = true;
    thread_ = std::thread([this] { run(); });
  }

  [[nodiscard]] bool enqueue(StorageTask task) {
    std::lock_guard lock(mutex_);
    if (!started_ || stopping_ || failed_ || tasks_.size() >= max_tasks_) {
      return false;
    }
    tasks_.push_back(std::move(task));
    task_ready_.notify_one();
    return true;
  }

  [[nodiscard]] std::vector<StorageComplete> take(std::size_t maximum) {
    std::lock_guard lock(mutex_);
    const std::size_t count = std::min(maximum, completions_.size());
    std::vector<StorageComplete> result;
    result.reserve(count);
    for (std::size_t index = 0; index < count; ++index) {
      result.push_back(std::move(completions_.front()));
      completions_.pop_front();
    }
    completion_space_.notify_one();
    return result;
  }

  void stop() noexcept {
    {
      std::lock_guard lock(mutex_);
      stopping_ = true;
      tasks_.clear();
    }
    task_ready_.notify_all();
    completion_space_.notify_all();
    if (thread_.joinable()) thread_.join();
  }

 private:
  void run() noexcept {
    for (;;) {
      StorageTask task;
      {
        std::unique_lock lock(mutex_);
        task_ready_.wait(lock,
                         [this] { return stopping_ || !tasks_.empty(); });
        if (stopping_) return;
        task = std::move(tasks_.front());
        tasks_.pop_front();
      }

      StorageComplete complete;
      complete.op_id = task.operation;
      try {
        const WalAppendResult appended = wal_->append(task.batch);
        if (!appended.durable) wal_->flush();
        complete.success = true;
      } catch (const std::exception& error) {
        complete.success = false;
        complete.error = error.what();
        if (complete.error.size() > 512) complete.error.resize(512);
        std::lock_guard lock(mutex_);
        failed_ = true;
      } catch (...) {
        complete.success = false;
        complete.error = "unknown storage worker failure";
        std::lock_guard lock(mutex_);
        failed_ = true;
      }

      std::unique_lock lock(mutex_);
      completion_space_.wait(lock, [this] {
        return stopping_ || completions_.size() < max_completions_;
      });
      if (stopping_) return;
      completions_.push_back(std::move(complete));
    }
  }

  std::unique_ptr<Wal> wal_;
  std::size_t max_tasks_{};
  std::size_t max_completions_{};
  std::mutex mutex_;
  std::condition_variable task_ready_;
  std::condition_variable completion_space_;
  std::deque<StorageTask> tasks_;
  std::deque<StorageComplete> completions_;
  std::thread thread_;
  bool started_{};
  bool stopping_{};
  bool failed_{};
};

struct HostTimer {
  bool active{};
  TimerRequest request;
  std::chrono::steady_clock::time_point deadline;
};

[[nodiscard]] std::uint64_t next_random(std::uint64_t& state) noexcept {
  state ^= state >> 12U;
  state ^= state << 25U;
  state ^= state >> 27U;
  return state * 0x2545f4914f6cdd1dULL;
}

void validate_host_config(const NodeHostConfig& config) {
  if (config.wal_path.empty() || config.raft.node_id == 0 ||
      config.identity.node_id != config.raft.node_id ||
      config.tcp.identity.node_id != config.raft.node_id ||
      config.identity.cluster_id_high != config.tcp.identity.cluster_id_high ||
      config.identity.cluster_id_low != config.tcp.identity.cluster_id_low) {
    throw std::invalid_argument("NodeHost identity configuration is inconsistent");
  }
  if (config.tick_duration.count() <= 0 || config.idle_sleep.count() < 0 ||
      config.limits.max_owner_events == 0 ||
      config.limits.max_client_queue == 0 ||
      config.limits.max_client_slots == 0 ||
      config.limits.max_trace_records == 0 ||
      config.limits.max_storage_tasks == 0 ||
      config.limits.max_storage_completions == 0 ||
      config.limits.max_tcp_events_per_poll == 0) {
    throw std::invalid_argument("NodeHost queue and timing bounds must be nonzero");
  }
  if (config.codec.max_frame_bytes == 0 ||
      config.codec.max_frame_bytes > config.tcp.limits.max_frame_bytes) {
    throw std::invalid_argument("TCP frame bound is smaller than codec bound");
  }
  if (config.raft.max_key_bytes > config.codec.max_key_bytes ||
      config.raft.max_key_bytes > config.wal.max_key_bytes ||
      config.raft.max_value_bytes > config.codec.max_value_bytes ||
      config.raft.max_value_bytes > config.wal.max_value_bytes ||
      config.raft.max_append_entries_per_rpc >
          config.codec.max_entries_per_append ||
      config.raft.max_append_entries_per_rpc >
          config.wal.max_entries_per_frame ||
      config.raft.max_append_bytes_per_rpc >
          config.codec.max_frame_bytes ||
      config.raft.max_append_bytes_per_rpc > config.wal.max_frame_bytes ||
      config.raft.max_command_bytes > config.codec.max_frame_bytes ||
      config.raft.max_command_bytes > config.wal.max_frame_bytes) {
    throw std::invalid_argument(
        "Raft command/append bounds exceed codec or WAL bounds");
  }
  constexpr std::size_t kTcpLengthPrefixBytes = 4;
  if (config.raft.max_append_bytes_per_rpc >
          std::numeric_limits<std::size_t>::max() -
              kTcpLengthPrefixBytes ||
      config.tcp.limits.max_outbound_bytes_per_peer <
          config.raft.max_append_bytes_per_rpc + kTcpLengthPrefixBytes ||
      config.tcp.limits.max_total_outbound_bytes <
          config.raft.max_append_bytes_per_rpc + kTcpLengthPrefixBytes) {
    throw std::invalid_argument(
        "TCP outbound queue cannot hold one maximum Raft frame");
  }

  std::vector<NodeId> members = config.raft.members;
  std::sort(members.begin(), members.end());
  if ((members.size() != 3 && members.size() != 5) ||
      std::adjacent_find(members.begin(), members.end()) != members.end() ||
      !std::binary_search(members.begin(), members.end(),
                          config.raft.node_id)) {
    throw std::invalid_argument(
        "NodeHost requires 3 or 5 unique members including this node");
  }
  std::vector<NodeId> expected_peers;
  for (const NodeId member : members) {
    if (member != config.raft.node_id) expected_peers.push_back(member);
  }
  std::vector<NodeId> actual_peers;
  actual_peers.reserve(config.tcp.peers.size());
  for (const TcpPeerEndpoint& peer : config.tcp.peers) {
    actual_peers.push_back(peer.node_id);
  }
  std::sort(actual_peers.begin(), actual_peers.end());
  if (actual_peers != expected_peers) {
    throw std::invalid_argument(
        "TCP peer set must exactly match Raft members except this node");
  }
}

}  // namespace

class NodeHost::Impl {
 public:
  explicit Impl(NodeHostConfig config) : config_(std::move(config)) {
    validate_host_config(config_);
    random_state_ = config_.timer_seed ^
                    (static_cast<std::uint64_t>(config_.raft.node_id) << 32U) ^
                    0x9e3779b97f4a7c15ULL;
    if (random_state_ == 0) random_state_ = 1;

    auto wal = std::make_unique<Wal>(config_.wal_path, config_.identity,
                                     config_.wal);
    const WalState recovered_wal = wal->state();
    RecoveredState recovered;
    recovered.hard_state = recovered_wal.hard_state;
    recovered.log = recovered_wal.entries;
    recovered.commit_index = recovered_wal.commit_index;
    raft_ = std::make_unique<RaftNode>(config_.raft, std::move(recovered));
    storage_ = std::make_unique<StorageWorker>(
        std::move(wal), config_.limits.max_storage_tasks,
        config_.limits.max_storage_completions);
    tcp_ = std::make_unique<TcpTransport>(config_.tcp);
  }

  ~Impl() { stop(); }

  void start() {
    if (started_) throw std::logic_error("NodeHost is one-shot and already started");
    owner_thread_ = std::this_thread::get_id();
    storage_->start();
    try {
      tcp_->start();
    } catch (...) {
      storage_->stop();
      throw;
    }
    started_ = true;
    running_ = true;
    handle_effects(raft_->start());
  }

  [[nodiscard]] bool poll_once() {
    ensure_owner();
    if (!running_) return false;
    bool activity = gather_inputs();
    if (owner_events_.empty()) return activity;
    RaftEvent event = std::move(owner_events_.front());
    owner_events_.pop_front();
    handle_effects(raft_->step(std::move(event)));
    return true;
  }

  [[nodiscard]] std::size_t run_for(std::chrono::milliseconds duration) {
    ensure_owner();
    if (duration.count() < 0) {
      throw std::invalid_argument("NodeHost run duration cannot be negative");
    }
    const auto deadline = std::chrono::steady_clock::now() + duration;
    std::size_t work = 0;
    do {
      if (poll_once()) {
        ++work;
      } else if (config_.idle_sleep.count() > 0) {
        const auto now = std::chrono::steady_clock::now();
        if (now < deadline) {
          std::this_thread::sleep_for(
              std::min(config_.idle_sleep,
                       std::chrono::duration_cast<std::chrono::milliseconds>(
                           deadline - now)));
        }
      }
    } while (running_ && std::chrono::steady_clock::now() < deadline);
    return work;
  }

  void stop() noexcept {
    if (!started_ || !running_) return;
    running_ = false;
    tcp_->stop();
    storage_->stop();
  }

  [[nodiscard]] NodeHostSubmitResult submit(ClientCommand command) {
    ensure_owner();
    if (!running_) return {NodeHostSubmitStatus::stopped, 0};
    if (!KvStateMachine::structurally_valid(command) ||
        command.key.size() > config_.raft.max_key_bytes ||
        command.value.size() > config_.raft.max_value_bytes ||
        command.key.size() > config_.raft.max_command_bytes ||
        command.value.size() >
            config_.raft.max_command_bytes - command.key.size()) {
      return {NodeHostSubmitStatus::error, 0};
    }
    if (client_queue_.size() >= config_.limits.max_client_queue ||
        outstanding_tokens_.size() + replies_.size() >=
            config_.limits.max_client_slots) {
      ++metrics_.client_submit_backpressure;
      return {NodeHostSubmitStatus::would_block, 0};
    }
    if (next_client_token_ == 0 ||
        next_client_token_ == std::numeric_limits<ClientToken>::max()) {
      return {NodeHostSubmitStatus::error, 0};
    }
    const ClientToken token = next_client_token_++;
    client_queue_.push_back(ClientRequest{token, std::move(command)});
    record_queue_depths();
    outstanding_tokens_.insert(token);
    ++metrics_.clients_submitted;
    return {NodeHostSubmitStatus::accepted, token};
  }

  [[nodiscard]] std::vector<ClientReply> poll_replies(std::size_t maximum) {
    ensure_owner();
    const std::size_t count = std::min(maximum, replies_.size());
    std::vector<ClientReply> result;
    result.reserve(count);
    for (std::size_t index = 0; index < count; ++index) {
      result.push_back(std::move(replies_.front()));
      replies_.pop_front();
    }
    return result;
  }

  [[nodiscard]] std::vector<TraceRecord> poll_traces(std::size_t maximum) {
    ensure_owner();
    const std::size_t count = std::min(maximum, traces_.size());
    std::vector<TraceRecord> result;
    result.reserve(count);
    for (std::size_t index = 0; index < count; ++index) {
      result.push_back(std::move(traces_.front()));
      traces_.pop_front();
    }
    return result;
  }

  [[nodiscard]] bool running() const noexcept { return running_; }

  [[nodiscard]] std::uint16_t listening_port() const {
    ensure_owner();
    return tcp_->listening_port();
  }

  [[nodiscard]] NodeHostStatus status() const {
    ensure_owner();
    return NodeHostStatus{raft_->id(),
                          fatal_error_ ? RaftRole::unavailable : raft_->role(),
                          raft_->current_term(),
                          raft_->leader_hint(),
                          raft_->leader_ready(),
                          raft_->last_log_index(),
                          raft_->durable_last_log_index(),
                          raft_->commit_index(),
                          raft_->last_applied(),
                          raft_->storage_pending(),
                          owner_events_.size(),
                          client_queue_.size()};
  }

  [[nodiscard]] NodeHostMetrics metrics() const noexcept { return metrics_; }

  [[nodiscard]] std::optional<std::string> value_for_test(
      std::string_view key) const {
    ensure_owner();
    return raft_->state_machine().value_for_test(key);
  }

  [[nodiscard]] const std::vector<LogEntry>& log_for_test() const {
    ensure_owner();
    return raft_->log();
  }

 private:
  void ensure_owner() const {
    if (started_ && std::this_thread::get_id() != owner_thread_) {
      throw std::logic_error("NodeHost consensus API called from non-owner thread");
    }
  }

  void record_queue_depths() noexcept {
    metrics_.owner_queue_high_water =
        std::max(metrics_.owner_queue_high_water, owner_events_.size());
    metrics_.client_queue_high_water =
        std::max(metrics_.client_queue_high_water, client_queue_.size());
  }

  [[nodiscard]] std::uint64_t election_jitter() noexcept {
    if (config_.election_jitter == 0) return 0;
    if (config_.election_jitter ==
        std::numeric_limits<std::uint64_t>::max()) {
      return next_random(random_state_);
    }
    return next_random(random_state_) % (config_.election_jitter + 1U);
  }

  [[nodiscard]] std::chrono::steady_clock::time_point timer_deadline(
      TimerKind kind, Tick delay) {
    std::uint64_t ticks = delay;
    if (kind == TimerKind::election) {
      const std::uint64_t jitter = election_jitter();
      if (jitter > std::numeric_limits<std::uint64_t>::max() - ticks) {
        ticks = std::numeric_limits<std::uint64_t>::max();
      } else {
        ticks += jitter;
      }
    }
    const std::uint64_t milliseconds_per_tick =
        static_cast<std::uint64_t>(config_.tick_duration.count());
    const auto maximum =
        static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max());
    const std::uint64_t delay_ms =
        ticks > maximum / milliseconds_per_tick
            ? maximum
            : ticks * milliseconds_per_tick;
    const auto now = std::chrono::steady_clock::now();
    const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::time_point::max() - now);
    const std::int64_t safe_delay = std::min(
        static_cast<std::int64_t>(delay_ms), remaining.count());
    return now + std::chrono::milliseconds(safe_delay);
  }

  HostTimer& timer(TimerKind kind) noexcept {
    return kind == TimerKind::election ? election_timer_ : heartbeat_timer_;
  }

  void schedule_timer(const TimerRequest& request) {
    HostTimer& selected = timer(request.kind);
    selected.active = true;
    selected.request = request;
    selected.deadline = timer_deadline(request.kind, request.delay);
  }

  void fail_transport() noexcept {
    if (!running_) return;
    fatal_error_ = true;
    running_ = false;
    for (const ClientToken token : outstanding_tokens_) {
      if (replies_.size() >= config_.limits.max_client_slots) break;
      replies_.push_back(ClientReply{token, ClientStatus::unavailable,
                                     raft_->leader_hint(), 0, {}});
      ++metrics_.client_replies;
    }
    outstanding_tokens_.clear();
    client_queue_.clear();
    owner_events_.clear();
    tcp_->stop();
    storage_->stop();
  }

  [[nodiscard]] bool gather_inputs() {
    bool activity = false;
    if (!tcp_->running()) {
      ++metrics_.tcp_error_drops;
      fail_transport();
      return true;
    }
    std::size_t available =
        config_.limits.max_owner_events - owner_events_.size();
    if (available != 0) {
      std::vector<StorageComplete> completions = storage_->take(available);
      for (StorageComplete& complete : completions) {
        ++metrics_.storage_completed;
        if (!complete.success) ++metrics_.storage_errors;
        owner_events_.push_back(std::move(complete));
        activity = true;
      }
    }

    // Timers are consensus-control traffic. Admit due timers before draining
    // data-plane TCP messages so a busy peer cannot keep an election or
    // heartbeat out of a perpetually full bounded owner queue.
    const auto now = std::chrono::steady_clock::now();
    for (HostTimer* selected : {&election_timer_, &heartbeat_timer_}) {
      if (selected->active && selected->deadline <= now &&
          owner_events_.size() < config_.limits.max_owner_events) {
        owner_events_.push_back(
            TimerFired{selected->request.kind, selected->request.generation});
        selected->active = false;
        activity = true;
      }
    }

    // Admit at least one local client before bulk TCP work. Once admitted, it
    // is at most one bounded owner-queue traversal away from the core even if
    // peers continuously supply valid traffic.
    if (!client_queue_.empty() &&
        owner_events_.size() < config_.limits.max_owner_events) {
      owner_events_.push_back(std::move(client_queue_.front()));
      client_queue_.pop_front();
      activity = true;
    }

    available = config_.limits.max_owner_events - owner_events_.size();
    if (available != 0) {
      const std::size_t tcp_limit =
          std::min(available, config_.limits.max_tcp_events_per_poll);
      for (TcpTransportEvent& event : tcp_->poll(tcp_limit)) {
        activity = true;
        if (event.kind == TcpEventKind::message) {
          auto decoded = codec::decode_message(event.bytes, config_.codec);
          if (!decoded) {
            ++metrics_.tcp_decode_drops;
            continue;
          }
          owner_events_.push_back(MessageReceived{MessageEnvelope{
              event.peer, config_.raft.node_id, std::move(*decoded)}});
          ++metrics_.tcp_messages_received;
        } else if (event.kind == TcpEventKind::peer_rejected) {
          ++metrics_.tcp_peer_rejections;
        } else if (event.kind == TcpEventKind::transport_error) {
          ++metrics_.tcp_error_drops;
          record_queue_depths();
          fail_transport();
          return true;
        }
      }
    }

    while (!client_queue_.empty() &&
           owner_events_.size() < config_.limits.max_owner_events) {
      owner_events_.push_back(std::move(client_queue_.front()));
      client_queue_.pop_front();
      activity = true;
    }
    if (owner_events_.size() >= config_.limits.max_owner_events &&
        (!client_queue_.empty() || election_timer_.active ||
         heartbeat_timer_.active)) {
      ++metrics_.owner_queue_backpressure;
    }
    record_queue_depths();
    return activity;
  }

  void handle_effects(std::vector<RaftEffect> effects) {
    for (RaftEffect& effect : effects) {
      std::visit(
          [this](auto& value) {
            using T = std::decay_t<decltype(value)>;
            if constexpr (std::is_same_v<T, PersistEffect>) {
              ++metrics_.storage_submitted;
              if (!storage_->enqueue(
                      StorageTask{value.op_id, std::move(value.batch)})) {
                ++metrics_.storage_errors;
                if (owner_events_.size() <
                    config_.limits.max_owner_events) {
                  owner_events_.push_front(StorageComplete{
                      value.op_id, false, "bounded storage queue unavailable"});
                }
              }
            } else if constexpr (std::is_same_v<T, SendEffect>) {
              auto encoded =
                  codec::encode_message(value.envelope.message, config_.codec);
              if (!encoded) {
                ++metrics_.tcp_error_drops;
                fatal_error_ = true;
                running_ = false;
                tcp_->stop();
                storage_->stop();
                return;
              }
              const TcpSendResult result =
                  tcp_->send(value.envelope.to, *encoded);
              switch (result.status) {
                case TcpSendStatus::queued:
                  ++metrics_.tcp_messages_queued;
                  break;
                case TcpSendStatus::would_block:
                  ++metrics_.tcp_backpressure_drops;
                  break;
                case TcpSendStatus::down:
                  ++metrics_.tcp_down_drops;
                  break;
                case TcpSendStatus::error:
                  ++metrics_.tcp_error_drops;
                  break;
              }
            } else if constexpr (std::is_same_v<T, ScheduleTimerEffect>) {
              schedule_timer(value.timer);
            } else if constexpr (std::is_same_v<T, ClientReplyEffect>) {
              outstanding_tokens_.erase(value.reply.token);
              if (replies_.size() < config_.limits.max_client_slots) {
                replies_.push_back(std::move(value.reply));
                ++metrics_.client_replies;
              }
            } else if constexpr (std::is_same_v<T, TraceEffect>) {
              if (traces_.size() < config_.limits.max_trace_records) {
                traces_.push_back(std::move(value.record));
              } else {
                ++metrics_.trace_drops;
              }
            }
          },
          effect);
    }
    record_queue_depths();
  }

  NodeHostConfig config_;
  std::unique_ptr<RaftNode> raft_;
  std::unique_ptr<StorageWorker> storage_;
  std::unique_ptr<TcpTransport> tcp_;
  std::deque<RaftEvent> owner_events_;
  std::deque<ClientRequest> client_queue_;
  std::set<ClientToken> outstanding_tokens_;
  std::deque<ClientReply> replies_;
  std::deque<TraceRecord> traces_;
  HostTimer election_timer_;
  HostTimer heartbeat_timer_;
  NodeHostMetrics metrics_;
  ClientToken next_client_token_{1};
  std::uint64_t random_state_{};
  std::thread::id owner_thread_;
  bool started_{};
  bool running_{};
  bool fatal_error_{};
};

NodeHost::NodeHost(NodeHostConfig config)
    : impl_(std::make_unique<Impl>(std::move(config))) {}

NodeHost::~NodeHost() = default;

void NodeHost::start() { impl_->start(); }

bool NodeHost::poll_once() { return impl_->poll_once(); }

std::size_t NodeHost::run_for(std::chrono::milliseconds duration) {
  return impl_->run_for(duration);
}

void NodeHost::stop() noexcept { impl_->stop(); }

NodeHostSubmitResult NodeHost::submit(ClientCommand command) {
  return impl_->submit(std::move(command));
}

std::vector<ClientReply> NodeHost::poll_replies(std::size_t maximum) {
  return impl_->poll_replies(maximum);
}

std::vector<TraceRecord> NodeHost::poll_traces(std::size_t maximum) {
  return impl_->poll_traces(maximum);
}

bool NodeHost::running() const noexcept { return impl_->running(); }

std::uint16_t NodeHost::listening_port() const {
  return impl_->listening_port();
}

NodeHostStatus NodeHost::status() const { return impl_->status(); }

NodeHostMetrics NodeHost::metrics() const noexcept { return impl_->metrics(); }

std::optional<std::string> NodeHost::value_for_test(
    std::string_view key) const {
  return impl_->value_for_test(key);
}

const std::vector<LogEntry>& NodeHost::log_for_test() const {
  return impl_->log_for_test();
}

}  // namespace detlog
