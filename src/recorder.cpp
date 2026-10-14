#include "recorder/recorder.hpp"

#include <algorithm>
#include <cstdio>
#include <cctype>
#include <cmath>
#include <sstream>

#include <simdjson/simdjson.h>

#include "recorder/timeutil.hpp"

namespace acrec {
namespace {

constexpr int64_t kStableConnMs = 60000;   // a connection up this long resets the retry delay

std::string lower(std::string s) {
  for (char& c : s) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  return s;
}

std::string json_str(const std::string& s) {
  std::string o = "\"";
  for (char c : s) {
    if (c == '"' || c == '\\') o += '\\';
    if (static_cast<unsigned char>(c) >= 0x20) o += c;
  }
  return o + '"';
}

std::string http_error(const char* what, int status) {
  std::string e = std::string(what) + " HTTP " + std::to_string(status);
  if (status == 451) e += " (Binance blocks this server's location, e.g. US IPs: run it from an allowed region)";
  if (status == 418 || status == 429) e += " (rate limited by Binance)";
  return e;
}

}  // namespace

Recorder::Conn::~Conn() {
  if (ws) ws->close();   // joins the read thread: no callback can touch this Conn afterwards
}

Recorder::Recorder(const RecorderConfig& cfg, Transport& transport, RecordSink& sink, SymbolRegistry& registry,
                   Clock& clock, Logger& log)
    : cfg_(cfg), transport_(transport), sink_(sink), registry_(registry), clock_(clock), log_(log) {}

Recorder::~Recorder() {
  for (auto& s : shards_) {
    s->next.reset();
    s->active.reset();
  }
}

std::string Recorder::stream_name(Group g, const std::string& symbol) const {
  switch (g) {
    case Group::Bbo: return lower(symbol) + "@bookTicker";
    case Group::Trades: return lower(symbol) + "@aggTrade";
    case Group::Depth: return lower(symbol) + "@depth" + std::to_string(kDepthLevels) + "@100ms";
    default: return "";
  }
}

int Recorder::stall_s(const Shard& s) const {
  int t = INT32_MAX;
  for (Group g : s.groups) t = std::min(t, cfg_.stall_s[group_index(g)]);
  return t;
}

// ------------------------------------------------------------ writer commands

void Recorder::gap_rows(const Shard& s, uint32_t conn_id, const std::string& kind, const std::string& detail,
                        std::optional<uint64_t> last_good, std::optional<uint64_t> first_after, int64_t utc) {
  for (Group g : s.groups) {
    GapRow row;
    row.utc_ms = utc;
    row.group = group_name(g);
    row.connection_id = std::to_string(conn_id);
    row.kind = kind;
    row.detail = s.name + ": " + detail;
    if (last_good && *last_good) row.last_good_E_ms = last_good;
    if (first_after && *first_after) row.first_after_E_ms = first_after;
    sink_.post([row](WriterCore& w) { w.storage().gap(row); });
  }
}

void Recorder::event(const std::string& kind, const std::string& symbol, const std::string& detail, int64_t utc) {
  EventRow row{utc, kind, symbol, detail};
  sink_.post([row](WriterCore& w) { w.storage().event(row); });
}

// ------------------------------------------------------------ connections

std::unique_ptr<Recorder::Conn> Recorder::open_conn(Shard& s, std::string& err) {
  auto c = std::make_unique<Conn>();
  c->id = next_conn_id_++;
  c->shard = &s;
  c->parser = std::make_unique<MessageParser>(registry_);
  Conn* raw = c.get();
  WsHandlers h;
  h.on_message = [this, raw](std::string_view msg, uint64_t recv_ns) { on_message(*raw, msg, recv_ns); };
  h.on_close = [raw](const std::string& reason) {
    std::lock_guard<std::mutex> g(raw->mu);
    raw->close_reason = reason;
    raw->closed.store(true, std::memory_order_release);
  };
  c->opened_mono = clock_.mono_ms();
  c->ws = transport_.connect(cfg_.ws_host, s.path, std::move(h), err);
  if (!c->ws) return nullptr;
  queue_streams(*c, true, std::vector<std::string>(s.streams.begin(), s.streams.end()));
  return c;
}

void Recorder::on_message(Conn& c, std::string_view msg, uint64_t recv_ns) {
  const int64_t now = clock_.mono_ms();
  c.last_msg_mono.store(now, std::memory_order_relaxed);
  ParseResult res = c.parser->parse(msg, recv_ns, c.id, [&](AnyRecord& r) {
    const int gi = group_index(r.group);
    group_records_[gi].fetch_add(1, std::memory_order_relaxed);
    group_last_E_[gi].store(r.E_ms, std::memory_order_relaxed);
    group_last_mono_[gi].store(now, std::memory_order_relaxed);
    uint64_t zero = 0;
    c.first_E.compare_exchange_strong(zero, r.E_ms, std::memory_order_relaxed);
    c.last_E.store(r.E_ms, std::memory_order_relaxed);
    c.records.fetch_add(1, std::memory_order_relaxed);
    if (!sink_.push(r)) {   // writer queue full: count it, never block
      dropped_[gi].fetch_add(1, std::memory_order_relaxed);
      zero = 0;
      drop_first_E_[gi].compare_exchange_strong(zero, r.E_ms, std::memory_order_relaxed);
      drop_last_E_[gi].store(r.E_ms, std::memory_order_relaxed);
    }
  });
  if (res.kind == ParseResult::Kind::Error && res.reply_id >= 0) {
    std::lock_guard<std::mutex> g(c.mu);
    c.server_errors.push_back("request " + std::to_string(res.reply_id) + ": " + res.error);
  } else if (!res.error.empty()) {
    c.parse_errors.fetch_add(1, std::memory_order_relaxed);
    std::lock_guard<std::mutex> g(c.mu);
    c.last_error = res.error;
  }
}

void Recorder::queue_streams(Conn& c, bool subscribe, const std::vector<std::string>& streams) {
  const size_t n = static_cast<size_t>(cfg_.params_per_subscribe);
  for (size_t i = 0; i < streams.size(); i += n)
    c.pending.emplace_back(subscribe, std::vector<std::string>(streams.begin() + i, streams.begin() + std::min(streams.size(), i + n)));
  if (!c.pending.empty()) c.subscribed_mono = -1;
}

void Recorder::pump_subscriptions(Conn& c, int64_t now) {
  if (c.pending.empty() || now - c.last_send_mono < 1000 / cfg_.subscribe_msgs_per_sec) return;
  auto [subscribe, streams] = std::move(c.pending.front());
  c.pending.pop_front();
  const int64_t id = c.next_req_id++;
  std::string msg = std::string("{\"method\":\"") + (subscribe ? "SUBSCRIBE" : "UNSUBSCRIBE") + "\",\"params\":[";
  for (size_t i = 0; i < streams.size(); ++i) msg += (i ? "," : "") + json_str(streams[i]);
  msg += "],\"id\":" + std::to_string(id) + "}";
  c.last_send_mono = now;
  if (!c.ws->send_text(msg)) return;   // the close is picked up by check_shard
  std::string symbol;
  if (streams.size() == 1 && streams[0][0] != '!') {
    symbol = streams[0].substr(0, streams[0].find('@'));
    for (char& ch : symbol) ch = static_cast<char>(std::toupper(static_cast<unsigned char>(ch)));
  }
  event(subscribe ? "subscribe" : "unsubscribe", symbol,
        "conn " + std::to_string(c.id) + " (" + c.shard->name + "): " + std::to_string(streams.size()) + " streams, request " +
            std::to_string(id),
        clock_.utc_ms());
  if (c.pending.empty()) c.subscribed_mono = now;
}

void Recorder::lost(Shard& s, std::unique_ptr<Conn> c, const std::string& kind, const std::string& reason, int64_t now,
                    int64_t utc) {
  c->ws->close();
  const uint64_t last = c->last_E.load();
  if (!kind.empty()) gap_rows(s, c->id, kind, reason, last, std::nullopt, utc);
  if (kind == "disconnect") ++st_disconnects_;
  if (last) s.last_good_E = last;
  log_.warn(s.name + ": connection " + std::to_string(c->id) + " lost (" + reason + ")");
  if (s.next && !s.next->closed.load()) {
    // Mid-rotation: the replacement is already streaming, so there is no gap.
    const uint32_t old_id = c->id;
    sink_.post([old_id, utc](WriterCore& w) { w.overlap_end(old_id, utc); });
    s.active = std::move(s.next);
    log_.info(s.name + ": promoted connection " + std::to_string(s.active->id));
    return;
  }
  s.reconnecting = true;
  s.backoff_s = std::min(cfg_.reconnect_max_s, s.backoff_s ? s.backoff_s * 2 : 1);
  std::uniform_real_distribution<double> jitter(0.8, 1.2);
  s.retry_mono = now + static_cast<int64_t>(s.backoff_s * 1000 * jitter(rng_));
}

void Recorder::check_shard(Shard& s, int64_t now, int64_t utc) {
  // Unexpected close of the active connection.
  if (s.active && s.active->closed.load(std::memory_order_acquire)) {
    std::string reason;
    {
      std::lock_guard<std::mutex> g(s.active->mu);
      reason = s.active->close_reason;
    }
    lost(s, std::move(s.active), "disconnect", reason.empty() ? "closed" : reason, now, utc);
  }
  // A rotation candidate that died: end its overlap window and try later.
  if (s.next && s.next->closed.load(std::memory_order_acquire)) {
    const uint32_t old_id = s.active ? s.active->id : 0;
    sink_.post([old_id, utc](WriterCore& w) { w.overlap_end(old_id, utc); });
    log_.warn(s.name + ": rotation connection " + std::to_string(s.next->id) + " closed before taking over");
    s.next.reset();
    s.next_rotation_try_mono = now + 60000;
  }
  // (Re)connect.
  if (!s.active && now >= s.retry_mono) {
    std::string err;
    s.active = open_conn(s, err);
    if (!s.active) {
      s.backoff_s = std::min(cfg_.reconnect_max_s, s.backoff_s ? s.backoff_s * 2 : 1);
      std::uniform_real_distribution<double> jitter(0.8, 1.2);
      s.retry_mono = now + static_cast<int64_t>(s.backoff_s * 1000 * jitter(rng_));
      log_.warn(s.name + ": connect failed (" + err + "), retry in ~" + std::to_string(s.backoff_s) + "s");
      std::lock_guard<std::mutex> g(err_mu_);
      last_error_ = s.name + ": connect failed: " + err;
      return;
    }
    log_.info(s.name + ": connection " + std::to_string(s.active->id) + " open, " + std::to_string(s.streams.size()) + " streams");
  }
  if (!s.active) return;
  Conn& a = *s.active;
  pump_subscriptions(a, now);

  // First data after a reconnect closes the gap window.
  if (s.reconnecting && a.first_E.load()) {
    gap_rows(s, a.id, "reconnect", "data resumed", s.last_good_E, a.first_E.load(), utc);
    ++st_reconnects_;
    s.reconnecting = false;
  }
  // The retry delay only resets once a connection has stayed up a while: when
  // Binance keeps cutting fresh connections (as in a burst), retries back off
  // 1, 2, 4, 8 s instead of hammering it (repeated reconnects can get an IP banned).
  if (s.backoff_s && !s.reconnecting && now - a.opened_mono >= kStableConnMs) s.backoff_s = 0;

  // Stall: no message at all for the shard's threshold.
  const int64_t since = std::max(a.last_msg_mono.load(), a.opened_mono);
  if (now - since > stall_s(s) * 1000LL) {
    ++st_stalls_;
    gap_rows(s, a.id, "stall", "no message for " + std::to_string((now - since) / 1000) + "s; reconnecting", a.last_E.load(),
             std::nullopt, utc);
    lost(s, std::move(s.active), "", "stall", now, utc);
    return;
  }

  // Proactive rotation before Binance's 24 h disconnect.
  const int64_t rotate_at = (cfg_.rotate_after_s - static_cast<int64_t>(cfg_.rotate_stagger_s) * s.index) * 1000LL;
  if (!s.next && now - a.opened_mono >= rotate_at && now >= s.next_rotation_try_mono) {
    std::string err;
    s.next = open_conn(s, err);
    if (!s.next) {
      log_.warn(s.name + ": rotation connect failed (" + err + "), retry in 60s");
      s.next_rotation_try_mono = now + 60000;
    } else {
      const uint32_t old_id = a.id, new_id = s.next->id;
      const std::vector<Group> groups = s.groups;
      sink_.post([old_id, new_id, groups, utc](WriterCore& w) { w.overlap_begin(old_id, new_id, groups, utc); });
      log_.info(s.name + ": rotating connection " + std::to_string(old_id) + " -> " + std::to_string(new_id));
    }
  }
  if (s.next) {
    Conn& n = *s.next;
    pump_subscriptions(n, now);
    if (n.first_E.load() && n.subscribed_mono >= 0 && now - n.subscribed_mono >= cfg_.rotation_warmup_s * 1000LL) {
      std::unique_ptr<Conn> old = std::move(s.active);
      old->ws->close();   // after this no record from the old connection can arrive
      const uint32_t old_id = old->id;
      sink_.post([old_id, utc](WriterCore& w) { w.overlap_end(old_id, utc); });
      s.active = std::move(s.next);
      log_.info(s.name + ": rotation done, connection " + std::to_string(old_id) + " closed");
    }
  }
}

// ------------------------------------------------------------ subscriptions

void Recorder::reconcile(int64_t utc) {
  auto snap = registry_.snapshot();
  std::vector<std::string> trading;
  for (const auto& [name, key] : snap->trading) trading.push_back(name);
  std::sort(trading.begin(), trading.end());

  // Depth set: today's top USDT perpetuals plus every USDC perpetual.
  std::set<std::string> depth;
  for (const auto& s : depth_top_usdt_)
    if (snap->trading.count(s)) depth.insert(s);
  for (const auto& s : trading)
    if (snap->specs[snap->trading.at(s)].quote == "USDC") depth.insert(s);
  std::vector<std::string> depth_vec(depth.begin(), depth.end());
  if (depth_vec != depth_set_) {
    depth_set_ = depth_vec;
    if (depth_announced_) announce_depth_set(utc);
  }

  for (Group g : {Group::Bbo, Group::Trades, Group::Depth}) {
    if (!cfg_.groups[group_index(g)]) continue;
    std::set<std::string> want;
    for (const auto& s : g == Group::Depth ? depth_vec : trading) want.insert(stream_name(g, s));
    std::vector<Shard*> mine;
    std::set<std::string> have;
    for (auto& s : shards_)
      if (s->kind == g) {
        mine.push_back(s.get());
        have.insert(s->streams.begin(), s->streams.end());
      }
    for (Shard* s : mine) {
      std::vector<std::string> removed;
      for (auto it = s->streams.begin(); it != s->streams.end();) {
        if (!want.count(*it)) { removed.push_back(*it); it = s->streams.erase(it); }
        else ++it;
      }
      if (removed.empty()) continue;
      for (Conn* c : {s->active.get(), s->next.get()})
        if (c) queue_streams(*c, false, removed);
    }
    std::map<Shard*, std::vector<std::string>> added;
    for (const auto& st : want) {
      if (have.count(st)) continue;
      Shard* best = nullptr;
      for (Shard* s : mine)
        if (static_cast<int>(s->streams.size()) < cfg_.streams_per_connection && (!best || s->streams.size() < best->streams.size()))
          best = s;
      if (!best) {
        auto s = std::make_unique<Shard>();
        s->kind = g;
        s->groups = {g};
        s->path = g == Group::Trades ? cfg_.ws_market_path : cfg_.ws_public_path;
        s->name = std::string(group_name(g)) + "#" + std::to_string(mine.size());
        s->index = static_cast<int>(shards_.size());
        best = s.get();
        mine.push_back(best);
        shards_.push_back(std::move(s));
      }
      best->streams.insert(st);
      added[best].push_back(st);
    }
    for (auto& [s, streams] : added)
      for (Conn* c : {s->active.get(), s->next.get()})
        if (c) queue_streams(*c, true, streams);
  }

  // One connection carries the all-market streams.
  const bool mark = cfg_.groups[group_index(Group::Mark)], liq = cfg_.groups[group_index(Group::Liq)];
  if ((mark || liq) && std::none_of(shards_.begin(), shards_.end(), [](auto& s) { return s->kind == Group::Mark; })) {
    auto s = std::make_unique<Shard>();
    s->kind = Group::Mark;
    s->path = cfg_.ws_market_path;
    s->name = "allmarket";
    s->index = static_cast<int>(shards_.size());
    if (mark) { s->groups.push_back(Group::Mark); s->streams.insert("!markPrice@arr@1s"); }
    if (liq) { s->groups.push_back(Group::Liq); s->streams.insert("!forceOrder@arr"); }
    shards_.push_back(std::move(s));
  }
}

void Recorder::announce_depth_set(int64_t utc) {
  const std::vector<std::string> set = depth_set_;
  std::string list;
  for (const auto& s : set) list += (list.empty() ? "" : " ") + s;
  sink_.post([set](WriterCore& w) { w.storage().set_depth_set(set); });
  event("depth_set", "", std::to_string(set.size()) + " symbols: " + list, utc);
}

// ------------------------------------------------------------ REST

bool Recorder::refresh_exchange_info(bool first, std::string& err) {
  std::string body;
  int status = 0;
  if (!transport_.http_get(cfg_.rest_host, "/fapi/v1/exchangeInfo", body, status, err)) return false;
  if (status != 200) { err = http_error("exchangeInfo", status); return false; }
  std::vector<SymbolSpec> specs;
  if (!parse_exchange_info(body, cfg_.universe, specs, err)) return false;
  const int64_t utc = clock_.utc_ms();
  auto changes = registry_.update(specs, utc);
  const size_t trading = registry_.snapshot()->trading.size();
  if (first) {
    event("start", "", "recorder started; " + std::to_string(trading) + " symbols trading", utc);
    log_.info("exchangeInfo: " + std::to_string(trading) + " symbols trading");
  } else if (!changes.empty()) {
    sink_.post([changes, utc](WriterCore& w) { w.symbols_changed(changes, utc); });
    for (const auto& c : changes) log_.info(std::string("exchangeInfo: ") + change_kind_name(c.kind) + " " + c.symbol + " (" + c.detail + ")");
  }
  reconcile(utc);
  return true;
}

bool Recorder::refresh_depth_set(std::string& err) {
  std::string body;
  int status = 0;
  if (!transport_.http_get(cfg_.rest_host, "/fapi/v1/ticker/24hr", body, status, err)) return false;
  if (status != 200) { err = http_error("ticker/24hr", status); return false; }
  simdjson::dom::parser p;
  simdjson::dom::array arr;
  if (p.parse(body).get_array().get(arr)) { err = "ticker/24hr: not an array"; return false; }
  auto snap = registry_.snapshot();
  std::vector<std::pair<double, std::string>> ranked;
  for (simdjson::dom::element t : arr) {
    std::string_view sym, qv;
    if (t["symbol"].get_string().get(sym) || t["quoteVolume"].get_string().get(qv)) continue;
    const uint32_t* key = snap->find_trading(sym);
    if (!key || snap->specs[*key].quote != "USDT") continue;
    ranked.emplace_back(std::strtod(std::string(qv).c_str(), nullptr), std::string(sym));   // ranking only, never stored
  }
  std::sort(ranked.begin(), ranked.end(), [](auto& a, auto& b) { return a.first > b.first; });
  depth_top_usdt_.clear();
  for (size_t i = 0; i < ranked.size() && static_cast<int>(i) < cfg_.depth_top_usdt; ++i) depth_top_usdt_.push_back(ranked[i].second);
  reconcile(clock_.utc_ms());
  return true;
}

void Recorder::sync_clock() {
  std::string body, err;
  int status = 0;
  const int64_t send = clock_.utc_ms();
  const bool ok = transport_.http_get(cfg_.rest_host, "/fapi/v1/time", body, status, err);
  const int64_t recv = clock_.utc_ms();
  int64_t server = 0;
  simdjson::dom::parser p;
  if (!ok || status != 200 || p.parse(body)["serverTime"].get_int64().get(server)) {
    std::lock_guard<std::mutex> g(err_mu_);
    last_error_ = "clock sync failed: " + (err.empty() ? "HTTP " + std::to_string(status) : err);
    return;
  }
  ClockRow row{recv, send, recv, server, static_cast<double>(server) - (send + recv) / 2.0};
  sink_.post([row](WriterCore& w) { w.storage().clock(row); });
}

void Recorder::flush_drops(int64_t utc) {
  for (int gi = 0; gi < kGroupCount; ++gi) {
    const uint64_t n = dropped_[gi].exchange(0);
    if (!n) continue;
    const uint64_t first = drop_first_E_[gi].exchange(0), last = drop_last_E_[gi].load();
    dropped_total_[gi] += n;
    const Group g = group_at(gi);
    sink_.post([g, n, first, last, utc](WriterCore& w) { w.backpressure(g, n, first, last, utc); });
    log_.warn(std::string(group_name(g)) + ": " + std::to_string(n) + " records dropped (writer queue full)");
  }
}

// ------------------------------------------------------------ main loop

bool Recorder::start(std::string& err) {
  const int64_t now = clock_.mono_ms();
  start_mono_ = last_hb_mono_ = now;
  if (!refresh_exchange_info(true, err)) return false;
  next_info_mono_ = now + cfg_.exchange_info_interval_s * 1000LL;
  std::string derr;
  const int64_t utc = clock_.utc_ms();
  if (refresh_depth_set(derr)) {
    if (utc - day_of_ms(utc) * kMsPerDay >= cfg_.depth_refresh_minute_utc * 60000LL) depth_day_done_ = day_of_ms(utc);
  } else {
    log_.warn("depth set: " + derr + "; recording USDC perpetuals only until the 24h ticker loads");
    next_depth_retry_mono_ = now + 60000;
  }
  depth_announced_ = true;
  announce_depth_set(utc);
  sync_clock();
  next_clock_mono_ = now + cfg_.clock_sync_interval_s * 1000LL;
  for (auto& s : shards_) check_shard(*s, now, utc);
  return true;
}

void Recorder::tick() {
  const int64_t now = clock_.mono_ms(), utc = clock_.utc_ms();
  std::string err;
  if (now >= next_info_mono_) {
    if (refresh_exchange_info(false, err)) next_info_mono_ = now + cfg_.exchange_info_interval_s * 1000LL;
    else {
      next_info_mono_ = now + 60000;
      log_.warn("exchangeInfo refresh failed: " + err);
      std::lock_guard<std::mutex> g(err_mu_);
      last_error_ = "exchangeInfo: " + err;
    }
  }
  if (now >= next_clock_mono_) {
    sync_clock();
    next_clock_mono_ = now + cfg_.clock_sync_interval_s * 1000LL;
  }
  const int64_t today = day_of_ms(utc);
  if (today != depth_day_done_ && utc - today * kMsPerDay >= cfg_.depth_refresh_minute_utc * 60000LL &&
      now >= next_depth_retry_mono_) {
    if (refresh_depth_set(err)) depth_day_done_ = today;
    else {
      next_depth_retry_mono_ = now + 60000;
      log_.warn("depth set refresh failed: " + err);
    }
  }
  for (auto& s : shards_) {
    check_shard(*s, now, utc);
    for (Conn* c : {s->active.get(), s->next.get()}) {
      if (!c) continue;
      std::vector<std::string> server_errors;
      std::string parse_error;
      {
        std::lock_guard<std::mutex> g(c->mu);
        server_errors.swap(c->server_errors);
        parse_error = c->last_error;
      }
      for (const auto& e : server_errors) event("subscribe", "", "conn " + std::to_string(c->id) + " server error " + e, utc);
      const uint64_t pe = c->parse_errors.load();
      if (pe > c->parse_errors_logged) {
        gap_rows(*s, c->id, "parse_error", std::to_string(pe - c->parse_errors_logged) + " message(s) not recorded: " + parse_error,
                 std::nullopt, std::nullopt, utc);
        st_parse_errors_ += pe - c->parse_errors_logged;
        c->parse_errors_logged = pe;
      }
    }
  }
  flush_drops(utc);
}

void Recorder::stop(const std::string& reason) {
  const int64_t utc = clock_.utc_ms();
  for (auto& s : shards_) {
    if (s->next) {
      s->next.reset();
      if (s->active) {
        const uint32_t old_id = s->active->id;
        sink_.post([old_id, utc](WriterCore& w) { w.overlap_end(old_id, utc); });
      }
    }
    s->active.reset();
  }
  flush_drops(utc);
  event("stop", "", reason, utc);
  log_.info("recorder stopped: " + reason);
}

// ------------------------------------------------------------ monitoring

std::vector<std::string> Recorder::warnings(double disk_free_gb) {
  std::vector<std::string> w;
  const int64_t now = clock_.mono_ms();
  if (disk_free_gb >= 0 && disk_free_gb < cfg_.min_disk_free_gb)
  {
    char buf[160];
    std::snprintf(buf, sizeof buf, "disk free %.2f GB < %.1f GB at %s", disk_free_gb, cfg_.min_disk_free_gb,
                  cfg_.root.c_str());
    w.push_back(buf);
  }
  for (int gi = 0; gi < kGroupCount; ++gi) {
    if (!cfg_.groups[gi]) continue;
    const int64_t last = group_last_mono_[gi].load();
    const int64_t quiet = now - (last ? last : start_mono_);
    if (quiet > cfg_.group_stall_warn_s * 1000LL)
      w.push_back(std::string(group_name(group_at(gi))) + " stalled for " + std::to_string(quiet / 1000) + "s");
  }
  return w;
}

std::string Recorder::heartbeat_json(const WriterStatus& ws, double disk_free_gb) {
  const int64_t now = clock_.mono_ms(), utc = clock_.utc_ms();
  const double dt = std::max<int64_t>(1, now - last_hb_mono_) / 1000.0;
  last_hb_mono_ = now;
  std::ostringstream o;
  o.setf(std::ios::fixed);
  o.precision(1);
  o << "{\n  \"utc\": \"" << iso_utc_ms(utc) << "\",\n  \"uptime_s\": " << (now - start_mono_) / 1000 << ",\n  \"groups\": {";
  bool firstg = true;
  for (int gi = 0; gi < kGroupCount; ++gi) {
    if (!cfg_.groups[gi]) continue;
    const Group g = group_at(gi);
    int up = 0, total = 0;
    for (auto& s : shards_)
      if (std::find(s->groups.begin(), s->groups.end(), g) != s->groups.end()) {
        ++total;
        if (s->active && !s->active->closed.load()) ++up;
      }
    const uint64_t recs = group_records_[gi].load();
    const int64_t last_mono = group_last_mono_[gi].load();
    o << (firstg ? "\n" : ",\n") << "    \"" << group_name(g) << "\": {\"msgs_per_s\": " << (recs - hb_records_[gi]) / dt
      << ", \"last_exchange_ms\": " << group_last_E_[gi].load() << ", \"conns_up\": " << up << ", \"conns_total\": " << total
      << ", \"dropped_total\": " << dropped_total_[gi] << ", \"quiet_s\": " << (last_mono ? (now - last_mono) / 1000 : -1) << "}";
    hb_records_[gi] = recs;
    firstg = false;
  }
  std::string err = ws.last_error;
  {
    std::lock_guard<std::mutex> g(err_mu_);
    if (err.empty()) err = last_error_;
  }
  o << "\n  },\n  \"gaps_today\": " << ws.gaps_today << ",\n  \"bytes_written_today\": " << ws.bytes_today
    << ",\n  \"disk_free_gb\": " << disk_free_gb << ",\n  \"writer_queue\": {\"fill\": " << ws.queue_fill
    << ", \"capacity\": " << ws.queue_capacity << "},\n  \"depth_set_size\": " << depth_set_.size()
    << ",\n  \"last_error\": " << json_str(err) << ",\n  \"warnings\": [";
  auto w = warnings(disk_free_gb);
  for (size_t i = 0; i < w.size(); ++i) o << (i ? ", " : "") << json_str(w[i]);
  o << "]\n}\n";
  return o.str();
}

std::string Recorder::stats_header() {
  std::string h = "utc_time,interval_s,uptime_s";
  for (int gi = 0; gi < kGroupCount; ++gi) {
    const std::string g = group_name(group_at(gi));
    h += "," + g + "_msgs_per_s," + g + "_conns_up," + g + "_conns_total," + g + "_dropped," + g + "_quiet_s";
  }
  h += ",disconnects,reconnects,stalls,parse_errors,queue_fill,queue_fill_max,queue_capacity,flush_ms_max,"
       "bytes_written_today,gaps_today,disk_free_gb,proc_cpu_pct,proc_rss_mb,proc_threads,sys_cpu_busy_pct,"
       "sys_iowait_pct,sys_steal_pct,load1,mem_avail_mb,swap_used_mb,net_rx_mbps,tcp_retrans_pct";
  return h;
}

void Recorder::write_stats(const WriterStatus& ws, double disk_free_gb, const SysRates& sys) {
  const int64_t now = clock_.mono_ms(), utc = clock_.utc_ms();
  const double dt = std::max<int64_t>(1, now - (st_last_mono_ ? st_last_mono_ : start_mono_)) / 1000.0;
  st_last_mono_ = now;
  char buf[96];
  auto num = [&](double v, int dp) {
    if (v < 0) return std::string("");   // unknown (e.g. /proc field missing)
    std::snprintf(buf, sizeof buf, "%.*f", dp, v);
    return std::string(buf);
  };
  std::string row = num(dt, 1) + "," + std::to_string((now - start_mono_) / 1000);
  for (int gi = 0; gi < kGroupCount; ++gi) {
    const Group g = group_at(gi);
    int up = 0, total = 0;
    for (auto& s : shards_)
      if (std::find(s->groups.begin(), s->groups.end(), g) != s->groups.end()) {
        ++total;
        if (s->active && !s->active->closed.load()) ++up;
      }
    const uint64_t recs = group_records_[gi].load();
    const uint64_t dropped = dropped_total_[gi] + dropped_[gi].load();
    const int64_t last_mono = group_last_mono_[gi].load();
    row += "," + num((recs - st_records_[gi]) / dt, 1) + "," + std::to_string(up) + "," + std::to_string(total) + "," +
           std::to_string(dropped - st_dropped_[gi]) + "," + (last_mono ? std::to_string((now - last_mono) / 1000) : "");
    st_records_[gi] = recs;
    st_dropped_[gi] = dropped;
  }
  row += "," + std::to_string(st_disconnects_) + "," + std::to_string(st_reconnects_) + "," + std::to_string(st_stalls_) +
         "," + std::to_string(st_parse_errors_);
  st_disconnects_ = st_reconnects_ = st_stalls_ = st_parse_errors_ = 0;
  row += "," + std::to_string(ws.queue_fill) + "," + std::to_string(ws.queue_fill_max) + "," +
         std::to_string(ws.queue_capacity) + "," + std::to_string(ws.flush_ms_max) + "," +
         std::to_string(ws.bytes_today) + "," + std::to_string(ws.gaps_today) + "," + num(disk_free_gb, 2);
  row += "," + num(sys.proc_cpu_pct, 1) + "," + num(sys.proc_rss_mb, 1) + "," +
         (sys.proc_threads >= 0 ? std::to_string(sys.proc_threads) : "") + "," + num(sys.sys_cpu_busy_pct, 1) + "," +
         num(sys.sys_iowait_pct, 1) + "," + num(sys.sys_steal_pct, 1) + "," + num(sys.load1, 2) + "," +
         num(sys.mem_avail_mb, 0) + "," + num(sys.swap_used_mb, 0) + "," + num(sys.net_rx_mbps, 2) + "," +
         num(sys.tcp_retrans_pct, 3);
  sink_.post([utc, row](WriterCore& w) { w.storage().stats(utc, stats_header(), row); });
}

std::vector<Recorder::ShardView> Recorder::shards() const {
  std::vector<ShardView> v;
  for (auto& s : shards_)
    v.push_back({s->name, s->path, s->streams.size(), s->active ? s->active->id : 0u, s->next ? s->next->id : 0u});
  return v;
}

std::vector<std::string> Recorder::depth_set() const { return depth_set_; }

}  // namespace acrec
