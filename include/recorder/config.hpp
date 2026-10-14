// recorder.json. Unknown keys are an error, so a typo can't silently change
// what the holdout records.
#pragma once
#include <string>
#include <vector>

#include "recorder/records.hpp"
#include "recorder/symbols.hpp"

namespace acrec {

struct RecorderConfig {
  std::string root = "acrec_data";
  bool groups[kGroupCount] = {true, true, true, true, true};   // bbo, trades, depth, liq, mark
  UniverseFilter universe;
  int depth_top_usdt = 20;                 // + every USDC perpetual
  int streams_per_connection = 150;        // Binance allows 1024
  int subscribe_msgs_per_sec = 5;          // Binance limit: 10 incoming messages/s per connection
  int params_per_subscribe = 100;          // streams per SUBSCRIBE message
  int zstd_level = 3;
  int frame_interval_s = 60;
  int segment_minutes = 0;                 // >0: new segment file every N minutes (enables intra-day upload)
  int exchange_info_interval_s = 3600;
  int clock_sync_interval_s = 600;
  int depth_refresh_minute_utc = 5;        // daily at 00:05 UTC
  int stall_s[kGroupCount] = {30, 30, 30, 60, 60};
  int rotate_after_s = 23 * 3600;          // Binance drops connections at 24 h
  int rotate_stagger_s = 120;              // spread rotations of different connections
  int rotation_warmup_s = 30;              // new connection must deliver this long before the old one closes
  int reconnect_max_s = 60;
  int day_close_grace_s = 120;
  int heartbeat_interval_s = 10;
  int stats_interval_s = 60;               // one row of <day>/stats.csv this often (0 = off)
  double min_disk_free_gb = 20;
  int group_stall_warn_s = 300;
  std::string rest_host = "fapi.binance.com";
  std::string ws_host = "fstream.binance.com";
  std::string ws_public_path = "/public/stream";   // bookTicker, depth
  std::string ws_market_path = "/market/stream";   // aggTrade, markPrice, forceOrder
  std::string log_file = "recorder.log";           // relative to root
  int log_max_mb = 10;
  int log_files = 5;
};

bool load_recorder_config(const std::string& path, RecorderConfig& cfg, std::string& err);

}  // namespace acrec
