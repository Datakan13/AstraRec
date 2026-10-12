// The recorder's control loop: connections, subscriptions, rotation,
// reconnects, REST schedules and the heartbeat. One thread calls tick();
// connection threads only parse and push records.
#pragma once
#include <array>
#include <atomic>
#include <cstdint>
#include <deque>
#include <memory>
#include <map>
#include <mutex>
#include <optional>
#include <random>
#include <set>
#include <string>
#include <vector>

#include "recorder/config.hpp"
#include "recorder/logger.hpp"
#include "recorder/parser.hpp"
#include "recorder/records.hpp"
#include "recorder/sink.hpp"
#include "recorder/symbols.hpp"
#include "recorder/transport.hpp"

namespace acrec {

class Clock {
 public:
  virtual ~Clock() = default;
  virtual int64_t mono_ms() = 0;   // for timers
  virtual int64_t utc_ms() = 0;    // wall clock, for rows and schedules
};

// What the writer thread reports for the heartbeat.
struct WriterStatus {
  uint64_t gaps_today = 0, bytes_today = 0;
  size_t queue_fill = 0, queue_capacity = 0;
  std::string last_error;
};

class Recorder {
 public:
  Recorder(const RecorderConfig& cfg, Transport& transport, RecordSink& sink, SymbolRegistry& registry, Clock& clock,
           Logger& log);
  ~Recorder();

  // Loads the symbol universe and the depth set, then opens the connections.
  // False if exchangeInfo can't be loaded (nothing to record without it).
  bool start(std::string& err);
  // Call every ~250 ms from one thread.
  void tick();
  // Closes every connection and logs the stop.
  void stop(const std::string& reason);

  // heartbeat.json content: counts, rates and states only, never prices.
  std::string heartbeat_json(const WriterStatus& ws, double disk_free_gb);
  std::vector<std::string> warnings(double disk_free_gb);

  // Introspection for tests.
  struct ShardView {
    std::string name, path;
    size_t streams;
    uint32_t active_conn, next_conn;   // 0 = none
  };
  std::vector<ShardView> shards() const;
  std::vector<std::string> depth_set() const;

 private:
  struct Shard;
  struct Conn {
    uint32_t id = 0;
    Shard* shard = nullptr;
    std::unique_ptr<WsConnection> ws;
    std::unique_ptr<MessageParser> parser;
    int64_t opened_mono = 0;
    // Written by the connection thread.
    std::atomic<int64_t> last_msg_mono{0};
    std::atomic<uint64_t> first_E{0}, last_E{0}, records{0}, parse_errors{0};
    std::atomic<bool> closed{false};
    std::mutex mu;
    std::string close_reason, last_error;
    std::vector<std::string> server_errors;
    // Manager thread only.
    std::deque<std::pair<bool, std::vector<std::string>>> pending;   // (subscribe?, streams)
    int64_t last_send_mono = INT64_MIN / 2;
    int64_t subscribed_mono = -1;      // when the last pending message went out
    int64_t next_req_id = 1;
    uint64_t parse_errors_logged = 0;
    ~Conn();
  };
  struct Shard {
    std::string name;                   // e.g. "bbo#2"
    std::string path;
    std::vector<Group> groups;
    Group kind;                         // which stream set it belongs to
    int index = 0;                      // for rotation stagger
    std::set<std::string> streams;      // desired
    std::unique_ptr<Conn> active, next;
    int64_t retry_mono = 0;
    int backoff_s = 0;
    bool reconnecting = false;          // a disconnect is waiting for its reconnect row
    uint64_t last_good_E = 0;
    int64_t next_rotation_try_mono = 0;
  };

  std::unique_ptr<Conn> open_conn(Shard& s, std::string& err);
  void on_message(Conn& c, std::string_view msg, uint64_t recv_ns);
  void queue_streams(Conn& c, bool subscribe, const std::vector<std::string>& streams);
  void pump_subscriptions(Conn& c, int64_t now);
  void check_shard(Shard& s, int64_t now, int64_t utc);
  void lost(Shard& s, std::unique_ptr<Conn> c, const std::string& kind, const std::string& reason, int64_t now, int64_t utc);
  void gap_rows(const Shard& s, uint32_t conn_id, const std::string& kind, const std::string& detail,
                std::optional<uint64_t> last_good, std::optional<uint64_t> first_after, int64_t utc);
  void event(const std::string& kind, const std::string& symbol, const std::string& detail, int64_t utc);
  void reconcile(int64_t utc);
  void announce_depth_set(int64_t utc);
  bool refresh_exchange_info(bool first, std::string& err);
  bool refresh_depth_set(std::string& err);
  void sync_clock();
  void flush_drops(int64_t utc);
  int stall_s(const Shard& s) const;
  std::string stream_name(Group g, const std::string& symbol) const;

  RecorderConfig cfg_;
  Transport& transport_;
  RecordSink& sink_;
  SymbolRegistry& registry_;
  Clock& clock_;
  Logger& log_;
  std::vector<std::unique_ptr<Shard>> shards_;
  uint32_t next_conn_id_ = 1;
  std::mt19937 rng_{std::random_device{}()};

  std::vector<std::string> depth_top_usdt_;     // from the daily 24h ticker
  std::vector<std::string> depth_set_;          // top USDT + every USDC perpetual
  bool depth_announced_ = false;                // false until start() has tried the 24h ticker
  int64_t depth_day_done_ = INT64_MIN;
  int64_t next_info_mono_ = 0, next_clock_mono_ = 0, next_depth_retry_mono_ = 0;
  int64_t start_mono_ = 0, last_hb_mono_ = 0;

  // Per group, written by connection threads.
  std::array<std::atomic<uint64_t>, kGroupCount> group_records_{};
  std::array<std::atomic<uint64_t>, kGroupCount> group_last_E_{};
  std::array<std::atomic<int64_t>, kGroupCount> group_last_mono_{};
  std::array<std::atomic<uint64_t>, kGroupCount> dropped_{}, drop_first_E_{}, drop_last_E_{};
  std::array<uint64_t, kGroupCount> hb_records_{};
  std::array<uint64_t, kGroupCount> dropped_total_{};
  std::mutex err_mu_;
  std::string last_error_;
};

}  // namespace acrec
