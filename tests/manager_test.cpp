// Connection-manager tests with a fake transport and clock: sharding, paths,
// subscriptions, disconnect/reconnect rows, stalls, rotation overlap,
// listings/delistings and writer backpressure. No network.
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "recorder/recorder.hpp"
#include "recorder/storage.hpp"
#include "recorder/timeutil.hpp"
#include "recorder/writer_core.hpp"

namespace fs = std::filesystem;
using namespace acrec;

namespace {

int failures = 0;
#define CHECK(cond)                                                                                         \
  do {                                                                                                      \
    if (!(cond)) { ++failures; std::cerr << __FILE__ << ":" << __LINE__ << ": CHECK(" #cond ") failed\n"; } \
  } while (0)

constexpr int64_t kDay0 = 1793491200000;   // 2026-11-01T00:00:00Z

std::string slurp(const std::string& p) {
  std::ifstream in(p);
  std::stringstream s;
  s << in.rdbuf();
  return s.str();
}
int count(const std::string& hay, const std::string& needle) {
  int n = 0;
  for (size_t p = hay.find(needle); p != std::string::npos; p = hay.find(needle, p + 1)) ++n;
  return n;
}

struct FakeClock : Clock {
  int64_t mono = 1000000, utc = kDay0 + 3600000;   // 01:00 UTC
  int64_t mono_ms() override { return mono; }
  int64_t utc_ms() override { return utc; }
  void advance(int64_t ms) { mono += ms; utc += ms; }
};

struct FakeConnState {
  std::string path;
  WsHandlers h;
  std::vector<std::string> sent;
  bool closed = false;
};

struct FakeWs : WsConnection {
  std::shared_ptr<FakeConnState> st;
  bool send_text(const std::string& t) override {
    if (st->closed) return false;
    st->sent.push_back(t);
    return true;
  }
  void close() override { st->closed = true; }
};

struct FakeTransport : Transport {
  std::vector<std::shared_ptr<FakeConnState>> conns;
  std::map<std::string, std::string> http;
  int fail_connects = 0;
  std::unique_ptr<WsConnection> connect(const std::string&, const std::string& path, WsHandlers h, std::string& err) override {
    if (fail_connects > 0) { --fail_connects; err = "refused"; return nullptr; }
    auto st = std::make_shared<FakeConnState>();
    st->path = path;
    st->h = std::move(h);
    conns.push_back(st);
    auto ws = std::make_unique<FakeWs>();
    ws->st = st;
    return ws;
  }
  bool http_get(const std::string&, const std::string& target, std::string& body, int& status, std::string& err) override {
    auto it = http.find(target);
    if (it == http.end()) { err = "no route"; return false; }
    body = it->second;
    status = 200;
    return true;
  }
  // Index of the newest open connection whose subscriptions include `stream`.
  int find(const std::string& stream) {
    for (int i = static_cast<int>(conns.size()) - 1; i >= 0; --i) {
      if (conns[i]->closed) continue;
      for (auto& m : conns[i]->sent)
        if (m.find("\"" + stream + "\"") != std::string::npos && m.find("\"SUBSCRIBE\"") != std::string::npos) return i;
    }
    return -1;
  }
  void deliver(int i, const std::string& msg) {
    if (i >= 0 && !conns[i]->closed) conns[i]->h.on_message(msg, 1);
  }
  void drop(int i) {
    conns[i]->closed = true;
    conns[i]->h.on_close("server closed the connection");
  }
};

struct RejectingSink : RecordSink {
  DirectSink inner;
  bool reject = false;
  explicit RejectingSink(WriterCore& w) : inner(w) {}
  bool push(const AnyRecord& r) override { return reject ? false : inner.push(r); }
  void post(WriterCommand c) override { inner.post(std::move(c)); }
};

std::string trade(const std::string& sym, uint64_t a, int64_t E) {
  std::string l = sym;
  for (char& c : l) c = static_cast<char>(std::tolower(c));
  return R"({"stream":")" + l + R"(@aggTrade","data":{"e":"aggTrade","E":)" + std::to_string(E) + R"(,"a":)" +
         std::to_string(a) + R"(,"s":")" + sym + R"(","p":"100.5","q":"1","nq":"1","f":1,"l":1,"T":)" +
         std::to_string(E) + R"(,"m":false,"st":1}})";
}
std::string mark(int64_t E) {
  return R"({"stream":"!markPrice@arr@1s","data":[{"e":"markPriceUpdate","E":)" + std::to_string(E) +
         R"(,"s":"BTCUSDT","p":"100.12345678","ap":"100.1","P":"100.1","i":"100.1","r":"0.0001","T":1,"st":1}]})";
}

const char* kTicker = R"([{"symbol":"ETHUSDT","quoteVolume":"900000000.5"},{"symbol":"BTCUSDT","quoteVolume":"500000000"},
  {"symbol":"SOLUSDT","quoteVolume":"100"},{"symbol":"1000PEPEUSDC","quoteVolume":"5"}])";

struct World {
  std::string root;
  FakeClock clock;
  FakeTransport net;
  SymbolRegistry registry;
  std::unique_ptr<Storage> storage;
  std::unique_ptr<WriterCore> writer;
  std::unique_ptr<RejectingSink> sink;
  Logger log;
  RecorderConfig cfg;
  std::unique_ptr<Recorder> rec;

  explicit World(const char* name) {
    root = (fs::temp_directory_path() / (std::string("acrec_mgr_") + name)).string();
    fs::remove_all(root);
    net.http["/fapi/v1/exchangeInfo"] = slurp(std::string(FIXTURE_DIR) + "/exchange_info.json");
    net.http["/fapi/v1/ticker/24hr"] = kTicker;
    net.http["/fapi/v1/time"] = R"({"serverTime":)" + std::to_string(clock.utc + 3) + "}";
    cfg.root = root;
    cfg.streams_per_connection = 2;
    cfg.depth_top_usdt = 1;
    for (int& s : cfg.stall_s) s = 100000;   // tests opt in to stalls
    cfg.rotate_after_s = 1000000;
    storage = std::make_unique<Storage>(StorageOptions{root, 3, "test"}, registry);
    writer = std::make_unique<WriterCore>(*storage, registry, WriterOptions{});
    sink = std::make_unique<RejectingSink>(*writer);
  }
  void begin() {
    rec = std::make_unique<Recorder>(cfg, net, *sink, registry, clock, log);
    std::string err;
    CHECK(rec->start(err));
    run(3000);   // let the rate-limited subscriptions go out
  }
  void run(int64_t ms, int64_t step = 200) {
    for (int64_t t = 0; t < ms; t += step) { clock.advance(step); rec->tick(); }
  }
  std::string day() { return root + "/2026-11-01/"; }
  ~World() {
    rec.reset();
    writer->finish();
    if (!std::getenv("ACREC_KEEP")) fs::remove_all(root);   // ACREC_KEEP=1 keeps the output for inspection
  }
};

void test_layout_and_subscriptions() {
  World w("layout");
  w.begin();
  // 3 USDT + 1 USDC perpetuals, 2 streams per connection.
  int bbo = 0, trades = 0, depth = 0, all = 0;
  for (auto& s : w.rec->shards()) {
    if (s.name.rfind("bbo", 0) == 0) { ++bbo; CHECK(s.path == "/public/stream"); }
    if (s.name.rfind("trades", 0) == 0) { ++trades; CHECK(s.path == "/market/stream"); }
    if (s.name.rfind("depth", 0) == 0) { ++depth; CHECK(s.path == "/public/stream"); }
    if (s.name == "allmarket") { ++all; CHECK(s.path == "/market/stream"); }
    CHECK(s.active_conn != 0);
  }
  CHECK(bbo == 2 && trades == 2 && depth == 1 && all == 1);
  // Depth: today's top USDT perpetual by quote volume + every USDC perpetual.
  CHECK((w.rec->depth_set() == std::vector<std::string>{"1000PEPEUSDC", "ETHUSDT"}));
  CHECK(w.net.find("ethusdt@depth20@100ms") >= 0 && w.net.find("1000pepeusdc@depth20@100ms") >= 0);
  CHECK(w.net.find("btcusdt@depth20@100ms") < 0);
  CHECK(w.net.find("!markPrice@arr@1s") >= 0 && w.net.find("!forceOrder@arr") >= 0);
  CHECK(w.net.find("solusdt@bookTicker") >= 0 && w.net.find("xauusdt@bookTicker") < 0);   // TradFi filtered out
  w.writer->storage().end_frames();
  const std::string ev = slurp(w.day() + "events.csv");
  CHECK(ev.find(",start,,recorder started; 4 symbols trading") != std::string::npos);
  CHECK(ev.find(",depth_set,,2 symbols: 1000PEPEUSDC ETHUSDT") != std::string::npos);
  CHECK(count(ev, ",depth_set,") == 1);
  CHECK(count(ev, ",subscribe,") == 6);
  CHECK(fs::exists(w.day() + "clock.csv"));
  const std::string clk = slurp(w.day() + "clock.csv");
  CHECK(clk.find(",3.0,0") != std::string::npos);   // offset = server - (send+recv)/2
}

void test_disconnect_reconnect_and_data() {
  World w("reconnect");
  w.begin();
  int c = w.net.find("btcusdt@aggTrade");
  CHECK(c >= 0);
  w.net.deliver(c, trade("BTCUSDT", 10, kDay0 + 3601000));
  w.net.deliver(c, trade("BTCUSDT", 11, kDay0 + 3602000));
  w.net.drop(c);
  w.run(400);
  CHECK(w.net.find("btcusdt@aggTrade") < 0);   // not reconnected before the backoff
  w.run(1600);
  const int c2 = w.net.find("btcusdt@aggTrade");
  CHECK(c2 > c);
  w.net.deliver(c2, trade("BTCUSDT", 12, kDay0 + 3605000));
  w.run(400);
  w.writer->storage().end_frames();
  const std::string gaps = slurp(w.day() + "gaps.csv");
  CHECK(gaps.find(",trades," + std::to_string(c + 1) + ",disconnect,,trades#0: server closed the connection," +
                  std::to_string(kDay0 + 3602000) + ",") != std::string::npos);
  CHECK(gaps.find(",reconnect,,trades#0: data resumed," + std::to_string(kDay0 + 3602000) + "," +
                  std::to_string(kDay0 + 3605000)) != std::string::npos);
  CHECK(gaps.find("seq_gap") == std::string::npos);
}

void test_stall() {
  World w("stall");
  w.cfg.stall_s[group_index(Group::Mark)] = 60;
  w.cfg.stall_s[group_index(Group::Liq)] = 60;
  w.begin();
  const int c = w.net.find("!markPrice@arr@1s");
  w.net.deliver(c, mark(kDay0 + 3603000));
  w.run(59000, 1000);
  CHECK(!w.net.conns[c]->closed);
  w.run(3000, 1000);
  CHECK(w.net.conns[c]->closed);
  w.run(2000);
  const int c2 = w.net.find("!markPrice@arr@1s");
  CHECK(c2 > c);
  w.writer->storage().end_frames();
  const std::string gaps = slurp(w.day() + "gaps.csv");
  CHECK(count(gaps, ",stall,") == 2);   // one row per group on the connection (mark, liq)
  CHECK(gaps.find(",mark," + std::to_string(c + 1) + ",stall,,allmarket: no message for 6") != std::string::npos);
}

void test_rotation() {
  World w("rotation");
  w.cfg.rotate_after_s = 3600;
  w.cfg.rotate_stagger_s = 0;
  w.cfg.rotation_warmup_s = 5;
  w.begin();
  const int old_c = w.net.find("btcusdt@aggTrade");
  uint64_t a = 100;
  int64_t E = kDay0 + 3700000;
  // An hour of normal traffic.
  for (int i = 0; i < 3600; ++i) {
    w.net.deliver(old_c, trade("BTCUSDT", a++, E += 1000));
    w.clock.advance(1000);
    w.rec->tick();
    if (w.net.find("btcusdt@aggTrade") != old_c) break;
  }
  const int new_c = w.net.find("btcusdt@aggTrade");
  CHECK(new_c > old_c);
  CHECK(!w.net.conns[old_c]->closed);
  // Both connections deliver the same trades for a while.
  for (int i = 0; i < 8; ++i) {
    const std::string m = trade("BTCUSDT", a++, E += 1000);
    w.net.deliver(new_c, m);
    w.net.deliver(old_c, m);
    w.run(1000);
  }
  CHECK(w.net.conns[old_c]->closed);     // old closed once the new one had delivered for 5 s
  CHECK(!w.net.conns[new_c]->closed);
  w.net.deliver(new_c, trade("BTCUSDT", a++, E += 1000));
  w.writer->storage().end_frames();
  const std::string gaps = slurp(w.day() + "gaps.csv");
  CHECK(gaps.find("seq_gap") == std::string::npos);
  CHECK(gaps.find("disconnect") == std::string::npos);
  CHECK(count(gaps, ",rotation_overlap,") >= 1);
  CHECK(gaps.find(",trades," + std::to_string(old_c + 1) + "->" + std::to_string(new_c + 1) + ",rotation_overlap,") !=
        std::string::npos);
}

void test_listing_delisting() {
  World w("listing");
  w.cfg.exchange_info_interval_s = 60;
  w.begin();
  std::string info = w.net.http["/fapi/v1/exchangeInfo"];
  // SOLUSDT goes to SETTLING (delisted); NEWUSDT starts trading.
  const size_t p = info.find("\"symbol\": \"SOLUSDT\"");
  CHECK(p != std::string::npos);
  const size_t st = info.find("\"TRADING\"", p);
  info.replace(st, 9, "\"SETTLING\"");
  const size_t ins = info.find("\"symbols\": [") + 12;
  info.insert(ins, R"({"symbol": "NEWUSDT", "contractType": "PERPETUAL", "status": "TRADING", "quoteAsset": "USDT",
    "filters": [{"filterType": "PRICE_FILTER", "tickSize": "0.001"}, {"filterType": "LOT_SIZE", "stepSize": "1"}]},)");
  w.net.http["/fapi/v1/exchangeInfo"] = info;
  w.run(61000, 1000);
  CHECK(w.net.find("newusdt@bookTicker") >= 0 && w.net.find("newusdt@aggTrade") >= 0);
  bool unsub = false;
  for (auto& c : w.net.conns)
    for (auto& m : c->sent)
      if (m.find("UNSUBSCRIBE") != std::string::npos && m.find("solusdt@bookTicker") != std::string::npos) unsub = true;
  CHECK(unsub);
  w.writer->storage().end_frames();
  const std::string ev = slurp(w.day() + "events.csv");
  CHECK(ev.find(",listing,NEWUSDT,status TRADING") != std::string::npos);
  CHECK(ev.find(",delisting,SOLUSDT,status SETTLING") != std::string::npos);
  CHECK(ev.find(",unsubscribe,") != std::string::npos);
  const std::string meta = slurp(w.day() + "meta.json");
  CHECK(meta.find("\"symbol\": \"NEWUSDT\"") != std::string::npos);
  CHECK(meta.find("\"symbol\": \"SOLUSDT\", \"tick_size\": \"0.0100\", \"step_size\": \"1\", \"quote\": \"USDT\", "
                  "\"contract_type\": \"PERPETUAL\", \"status\": \"SETTLING\"") != std::string::npos);
}

void test_backpressure() {
  World w("backpressure");
  w.begin();
  const int c = w.net.find("btcusdt@aggTrade");
  w.sink->reject = true;
  for (uint64_t a = 1; a <= 3; ++a) w.net.deliver(c, trade("BTCUSDT", a, kDay0 + 3600000 + static_cast<int64_t>(a)));
  w.sink->reject = false;
  w.run(400);
  w.writer->storage().end_frames();
  const std::string gaps = slurp(w.day() + "gaps.csv");
  CHECK(gaps.find(",trades,,writer_backpressure,,3 records dropped: writer queue full," + std::to_string(kDay0 + 3600001) +
                  "," + std::to_string(kDay0 + 3600003)) != std::string::npos);
  const std::string hb = w.rec->heartbeat_json({}, 100);
  CHECK(hb.find("\"dropped_total\": 3") != std::string::npos);
  CHECK(hb.find("100.5") == std::string::npos);   // heartbeat never shows prices
}

}  // namespace

int main() {
  test_layout_and_subscriptions();
  test_disconnect_reconnect_and_data();
  test_stall();
  test_rotation();
  test_listing_delisting();
  test_backpressure();
  if (failures) { std::cerr << failures << " check(s) failed\n"; return 1; }
  std::cout << "all manager tests passed\n";
  return 0;
}
