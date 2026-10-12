#include "recorder/config.hpp"

#include <simdjson/simdjson.h>

namespace acrec {
namespace {

using simdjson::dom::element;

bool get_int(element v, int& out) {
  int64_t x;
  if (v.get_int64().get(x)) return false;
  out = static_cast<int>(x);
  return true;
}

bool get_strings(element v, std::vector<std::string>& out) {
  simdjson::dom::array a;
  if (v.get_array().get(a)) return false;
  out.clear();
  for (element e : a) {
    std::string_view s;
    if (e.get_string().get(s)) return false;
    out.emplace_back(s);
  }
  return true;
}

bool group_map(element v, int* ints, bool* bools) {
  simdjson::dom::object o;
  if (v.get_object().get(o)) return false;
  for (auto [k, x] : o) {
    int gi = -1;
    for (int i = 0; i < kGroupCount; ++i)
      if (k == group_name(group_at(i))) gi = i;
    if (gi < 0) return false;
    if (ints && !get_int(x, ints[gi])) return false;
    if (bools && x.get_bool().get(bools[gi])) return false;
  }
  return true;
}

}  // namespace

bool load_recorder_config(const std::string& path, RecorderConfig& c, std::string& err) {
  simdjson::dom::parser parser;
  simdjson::dom::element doc;
  if (auto e = parser.load(path).get(doc)) { err = path + ": " + simdjson::error_message(e); return false; }
  simdjson::dom::object top;
  if (doc.get_object().get(top)) { err = path + ": top level must be an object"; return false; }

  struct IntKey { const char* name; int* p; };
  const IntKey ints[] = {
      {"depth_top_usdt", &c.depth_top_usdt}, {"streams_per_connection", &c.streams_per_connection},
      {"subscribe_msgs_per_sec", &c.subscribe_msgs_per_sec}, {"params_per_subscribe", &c.params_per_subscribe},
      {"zstd_level", &c.zstd_level}, {"frame_interval_s", &c.frame_interval_s},
      {"exchange_info_interval_s", &c.exchange_info_interval_s}, {"clock_sync_interval_s", &c.clock_sync_interval_s},
      {"depth_refresh_minute_utc", &c.depth_refresh_minute_utc}, {"rotate_after_s", &c.rotate_after_s},
      {"rotate_stagger_s", &c.rotate_stagger_s}, {"rotation_warmup_s", &c.rotation_warmup_s},
      {"reconnect_max_s", &c.reconnect_max_s}, {"day_close_grace_s", &c.day_close_grace_s},
      {"heartbeat_interval_s", &c.heartbeat_interval_s}, {"group_stall_warn_s", &c.group_stall_warn_s},
      {"log_max_mb", &c.log_max_mb}, {"log_files", &c.log_files}};
  struct StrKey { const char* name; std::string* p; };
  const StrKey strs[] = {{"root", &c.root}, {"rest_host", &c.rest_host}, {"ws_host", &c.ws_host},
                         {"ws_public_path", &c.ws_public_path}, {"ws_market_path", &c.ws_market_path},
                         {"log_file", &c.log_file}};

  for (auto [key, v] : top) {
    bool ok = false, known = false;
    for (const IntKey& k : ints)
      if (key == k.name) { known = true; ok = get_int(v, *k.p); }
    for (const StrKey& k : strs)
      if (key == k.name) {
        known = true;
        std::string_view s;
        ok = !v.get_string().get(s);
        if (ok) *k.p = s;
      }
    if (key == "groups") { known = true; ok = group_map(v, nullptr, c.groups); }
    if (key == "stall_s") { known = true; ok = group_map(v, c.stall_s, nullptr); }
    if (key == "contract_types") { known = true; ok = get_strings(v, c.universe.contract_types); }
    if (key == "quote_assets") { known = true; ok = get_strings(v, c.universe.quote_assets); }
    if (key == "min_disk_free_gb") { known = true; ok = !v.get_double().get(c.min_disk_free_gb); }
    if (!known) { err = path + ": unknown key '" + std::string(key) + "'"; return false; }
    if (!ok) { err = path + ": bad value for '" + std::string(key) + "'"; return false; }
  }
  if (c.root.empty() || c.streams_per_connection < 1 || c.streams_per_connection > 1024 || c.subscribe_msgs_per_sec < 1 ||
      c.subscribe_msgs_per_sec > 9 || c.params_per_subscribe < 1 || c.frame_interval_s < 1 || c.frame_interval_s > 60 ||
      c.rotate_after_s < 600 || c.rotate_after_s >= 24 * 3600 || c.reconnect_max_s < 1 || c.zstd_level < 1 ||
      c.zstd_level > 19 || c.depth_top_usdt < 0) {
    err = path + ": a setting is out of range (streams_per_connection 1..1024, subscribe_msgs_per_sec 1..9, "
                 "frame_interval_s 1..60, rotate_after_s 600..86399, zstd_level 1..19)";
    return false;
  }
  return true;
}

}  // namespace acrec
