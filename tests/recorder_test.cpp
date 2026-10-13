// Recorder acceptance tests (spec section 8, tests 2-6) plus decimal and
// restart checks. Test 1 (golden round-trip through the Python reader) is
// test_roundtrip.py. Run with ctest, or ./recorder_test.
#include <sys/wait.h>
#include <unistd.h>

#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include <zstd.h>

#include "recorder/decimal.hpp"
#include "recorder/parser.hpp"
#include "recorder/records.hpp"
#include "recorder/storage.hpp"
#include "recorder/symbols.hpp"
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

std::string temp_root(const char* name) {
  fs::path p = fs::temp_directory_path() / (std::string("acrec_test_") + name + "_" + std::to_string(::getpid()));
  fs::remove_all(p);
  fs::create_directories(p);
  return p.string();
}

std::string slurp(const std::string& path) {
  std::ifstream in(path, std::ios::binary);
  std::stringstream s;
  s << in.rdbuf();
  return s.str();
}

std::vector<std::string> lines(const std::string& path) {
  std::vector<std::string> out;
  std::ifstream in(path);
  std::string l;
  while (std::getline(in, l)) out.push_back(l);
  return out;
}

// Minimal reader: decodes complete zstd frames only (like acrec_read.py).
struct Decoded {
  FileHeader header{};
  std::string body;     // records after the header
  int frames = 0;
  bool ok = false;
};
Decoded decode(const std::string& path) {
  Decoded d;
  const std::string in = slurp(path);
  ZSTD_DCtx* dctx = ZSTD_createDCtx();
  std::string all, frame;
  std::vector<char> buf(ZSTD_DStreamOutSize());
  ZSTD_inBuffer ib{in.data(), in.size(), 0};
  while (ib.pos < ib.size) {
    ZSTD_outBuffer ob{buf.data(), buf.size(), 0};
    const size_t r = ZSTD_decompressStream(dctx, &ob, &ib);
    if (ZSTD_isError(r)) break;
    frame.append(buf.data(), ob.pos);
    if (r == 0) {   // frame complete (checksum verified)
      all += frame;
      frame.clear();
      ++d.frames;
    } else if (ob.pos == 0 && ib.pos == ib.size) {
      break;        // truncated frame: dropped
    }
  }
  ZSTD_freeDCtx(dctx);
  if (all.size() < sizeof(FileHeader)) return d;
  std::memcpy(&d.header, all.data(), sizeof d.header);
  d.body = all.substr(sizeof d.header);
  d.ok = std::memcmp(d.header.magic, "ACREC\0\0\0", 8) == 0 && d.body.size() % d.header.record_size == 0;
  return d;
}

template <typename R>
std::vector<R> records(const Decoded& d) {
  std::vector<R> v(d.body.size() / sizeof(R));
  if (!v.empty()) std::memcpy(v.data(), d.body.data(), v.size() * sizeof(R));
  return v;
}

SymbolSpec spec(const std::string& sym, const std::string& tick, const std::string& step, const char* status = "TRADING") {
  SymbolSpec s;
  s.symbol = sym; s.tick_size = tick; s.step_size = step; s.quote = "USDT"; s.contract_type = "PERPETUAL"; s.status = status;
  return s;
}

std::string trade_msg(const std::string& sym, uint64_t a, int64_t E) {
  return R"({"stream":"x@aggTrade","data":{"e":"aggTrade","E":)" + std::to_string(E) + R"(,"a":)" + std::to_string(a) +
         R"(,"s":")" + sym + R"(","p":"100.5","q":"0.002","nq":"0.002","f":)" + std::to_string(a * 2) + R"(,"l":)" +
         std::to_string(a * 2 + 1) + R"(,"T":)" + std::to_string(E - 1) + R"(,"m":true,"st":1}})";
}

std::string depth_msg(const std::string& sym, uint64_t U, uint64_t u, uint64_t pu, int64_t E) {
  return R"({"stream":"x@depth20@100ms","data":{"e":"depthUpdate","E":)" + std::to_string(E) + R"(,"T":)" +
         std::to_string(E - 1) + R"(,"s":")" + sym + R"(","ps":")" + sym + R"(","U":)" + std::to_string(U) + R"(,"u":)" +
         std::to_string(u) + R"(,"pu":)" + std::to_string(pu) + R"(,"b":[["100.1","1.5"]],"a":[["100.2","2"]],"st":1}})";
}

struct Rig {
  SymbolRegistry registry;
  std::unique_ptr<Storage> storage;
  std::unique_ptr<WriterCore> writer;
  std::unique_ptr<MessageParser> parser;
  explicit Rig(const std::string& root, std::vector<SymbolSpec> specs = {spec("BTCUSDT", "0.10", "0.001"), spec("ETHUSDT", "0.01", "0.001")},
               WriterOptions wopt = {}) {
    registry.update(specs, kDay0 - 1000);
    storage = std::make_unique<Storage>(StorageOptions{root, 3, "test"}, registry);
    writer = std::make_unique<WriterCore>(*storage, registry, wopt);
    parser = std::make_unique<MessageParser>(registry);
  }
  ParseResult feed(const std::string& msg, uint32_t conn = 0) {
    return parser->parse(msg, 123, conn, [&](AnyRecord& r) { writer->record(r, static_cast<int64_t>(r.E_ms)); });
  }
};

// ------------------------------------------------------------------ tests

void test_decimal() {
  int64_t v;
  CHECK(parse_fixed("84523.10", 8, v) && v == 8452310000000);
  CHECK(parse_fixed("0.10", 8, v) && v == 10000000);
  CHECK(parse_fixed("5", 8, v) && v == 500000000);
  CHECK(parse_fixed("-0.00012", 8, v) && v == -12000);
  CHECK(parse_fixed("0.00000001", 8, v) && v == 1);
  CHECK(parse_fixed("0.000000010", 8, v) && v == 1);      // trailing zero past the scale is exact
  CHECK(!parse_fixed("0.000000001", 8, v));                // would need rounding
  CHECK(parse_fixed("-0.00010000", 9, v) && v == -100000);
  CHECK(!parse_fixed("", 8, v) && !parse_fixed(".", 8, v) && !parse_fixed("1e5", 8, v) && !parse_fixed("1.2.3", 8, v));
  CHECK(!parse_fixed("100000000000", 8, v));               // 1e11 * 1e8 overflows int64
  CHECK(parse_fixed("92233720368.54775807", 8, v) && v == INT64_MAX);
  CHECK(format_fixed(8452310000000, 8) == "84523.1");
  CHECK(format_fixed(-12000, 8) == "-0.00012");
  CHECK(format_fixed(0, 8) == "0");
  CHECK(format_fixed(1, 8) == "0.00000001");
}

// Spec test 2: a missing pu/u link and a skipped agg id give exactly those gap rows.
void test_sequence_gaps() {
  const std::string root = temp_root("gaps");
  {
    Rig rig(root);
    for (uint64_t a : {100, 101, 103, 104}) rig.feed(trade_msg("BTCUSDT", a, kDay0 + 1000 + a));
    rig.feed(depth_msg("ETHUSDT", 10, 20, 9, kDay0 + 2000));
    rig.feed(depth_msg("ETHUSDT", 21, 30, 20, kDay0 + 2100));   // ok
    rig.feed(depth_msg("ETHUSDT", 36, 40, 35, kDay0 + 2200));   // pu 35 != 30: gap
    rig.feed(depth_msg("ETHUSDT", 41, 50, 40, kDay0 + 2300));   // ok again
    rig.feed(trade_msg("ETHUSDT", 7, kDay0 + 3000));
    rig.feed(trade_msg("ETHUSDT", 8, kDay0 + 3001));
    rig.writer->finish();
    CHECK(rig.writer->stats().gaps == 2);
  }
  auto rows = lines(root + "/2026-11-01/gaps.csv");
  CHECK(rows.size() == 3);   // header + 2
  if (rows.size() == 3) {
    CHECK(rows[0] == "utc_time,group,connection_id,kind,symbol,detail,last_good_E_ms,first_after_E_ms");
    CHECK(rows[1].find(",trades,0,seq_gap,BTCUSDT,agg_id 101 -> 103 (1 missing),") != std::string::npos);
    CHECK(rows[1].find("," + std::to_string(kDay0 + 1101) + "," + std::to_string(kDay0 + 1103)) != std::string::npos);
    CHECK(rows[2].find(",depth,0,seq_gap,ETHUSDT,pu 35 != previous u 30,") != std::string::npos);
  }
  // Every message is still written: gaps are logged, never repaired.
  auto t = decode(root + "/2026-11-01/trades.000.bin.zst");
  CHECK(t.ok && records<TradeRecord>(t).size() == 6);
  fs::remove_all(root);
}

// Spec test 3: messages straddling 00:00:00.000 UTC land in the right day.
void test_day_rollover() {
  const std::string root = temp_root("rollover");
  {
    Rig rig(root);
    rig.feed(trade_msg("BTCUSDT", 1, kDay0 - 1));       // 2026-10-31T23:59:59.999
    rig.feed(trade_msg("BTCUSDT", 2, kDay0));           // 2026-11-01T00:00:00.000
    rig.feed(trade_msg("BTCUSDT", 3, kDay0 - 2));       // late message for the previous day
    rig.feed(trade_msg("BTCUSDT", 4, kDay0 + kMsPerDay - 1));
    rig.writer->finish();
  }
  auto a = decode(root + "/2026-10-31/trades.000.bin.zst");
  auto b = decode(root + "/2026-11-01/trades.000.bin.zst");
  CHECK(a.ok && b.ok);
  CHECK(a.header.yyyymmdd == 20261031 && b.header.yyyymmdd == 20261101);
  auto ra = records<TradeRecord>(a), rb = records<TradeRecord>(b);
  CHECK(ra.size() == 2 && rb.size() == 2);
  for (auto& r : ra) CHECK(day_of_ms(static_cast<int64_t>(r.E_ms)) == day_of_ms(kDay0 - 1));
  for (auto& r : rb) CHECK(day_of_ms(static_cast<int64_t>(r.E_ms)) == day_of_ms(kDay0));
  CHECK(fs::exists(root + "/2026-10-31/meta.json") && fs::exists(root + "/2026-11-01/meta.json"));
  CHECK(!fs::exists(root + "/2026-11-02"));
  fs::remove_all(root);
}

// Spec test 4: a writer killed mid-segment leaves a .part; the next start renames
// it to .crash, uses the next segment number, and every complete frame decodes.
void test_crash_recovery() {
  const std::string root = temp_root("crash");
  constexpr int kFramed = 500, kUnframed = 20000;
  pid_t pid = ::fork();
  if (pid == 0) {
    Rig rig(root);
    for (int i = 0; i < kFramed; ++i) rig.feed(depth_msg("BTCUSDT", 1 + i * 10, 10 + i * 10, i * 10, kDay0 + i));
    rig.storage->end_frames();   // frame boundary: these 500 records are safe
    // Enough data that zstd has written part of the next frame to disk.
    for (int i = kFramed; i < kFramed + kUnframed; ++i)
      rig.feed(depth_msg("BTCUSDT", 1 + i * 10, 10 + i * 10, i * 10, kDay0 + i));
    ::_exit(0);   // killed: no close, no rename
  }
  int status = 0;
  ::waitpid(pid, &status, 0);
  CHECK(WIFEXITED(status));
  const std::string day = root + "/2026-11-01";
  CHECK(fs::exists(day + "/depth.000.bin.zst.part"));
  CHECK(fs::file_size(day + "/depth.000.bin.zst.part") > 4096);   // the partial frame really is on disk

  {
    Rig rig(root);
    auto renamed = rig.storage->recover();
    CHECK(renamed.size() == 1 && renamed[0] == day + "/depth.000.crash.bin.zst");
    rig.feed(depth_msg("BTCUSDT", 1, 5, 0, kDay0 + 100000));
    rig.writer->finish();
  }
  CHECK(!fs::exists(day + "/depth.000.bin.zst.part"));
  CHECK(fs::exists(day + "/depth.001.bin.zst"));           // next free segment, never appended
  auto c = decode(day + "/depth.000.crash.bin.zst");
  CHECK(c.ok);
  CHECK(c.frames == 2);                                    // header frame + the completed data frame
  auto rec = records<DepthRecord>(c);
  CHECK(rec.size() == kFramed);
  if (rec.size() == kFramed) CHECK(rec.back().u == 10 + (kFramed - 1) * 10);
  auto n = decode(day + "/depth.001.bin.zst");
  CHECK(n.ok && records<DepthRecord>(n).size() == 1);
  fs::remove_all(root);
}

// Spec test 5: a connection swap gives no gap; duplicates are one rotation_overlap row.
void test_rotation_overlap() {
  const std::string root = temp_root("overlap");
  {
    Rig rig(root);
    for (uint64_t a = 1; a <= 5; ++a) rig.feed(trade_msg("BTCUSDT", a, kDay0 + a), 1);
    rig.writer->overlap_begin(1, 2, {Group::Trades}, kDay0 + 10);
    rig.feed(trade_msg("BTCUSDT", 4, kDay0 + 4), 2);   // dup
    rig.feed(trade_msg("BTCUSDT", 5, kDay0 + 5), 2);   // dup
    rig.feed(trade_msg("BTCUSDT", 6, kDay0 + 6), 2);
    rig.feed(trade_msg("BTCUSDT", 6, kDay0 + 6), 1);   // dup
    rig.feed(trade_msg("BTCUSDT", 7, kDay0 + 7), 2);
    rig.feed(trade_msg("BTCUSDT", 7, kDay0 + 7), 1);   // dup
    rig.writer->overlap_end(1, kDay0 + 20);
    rig.feed(trade_msg("BTCUSDT", 8, kDay0 + 8), 2);
    rig.writer->finish();
    CHECK(rig.writer->stats().gaps == 0);
  }
  auto rows = lines(root + "/2026-11-01/gaps.csv");
  CHECK(rows.size() == 2);
  if (rows.size() == 2) {
    CHECK(rows[1].find(",trades,1->2,rotation_overlap,,4 duplicate messages written from both connections," +
                       std::to_string(kDay0 + 4) + "," + std::to_string(kDay0 + 7)) != std::string::npos);
  }
  auto t = decode(root + "/2026-11-01/trades.000.bin.zst");
  CHECK(records<TradeRecord>(t).size() == 12);   // both copies are written
  fs::remove_all(root);
}

// Spec test 6: a mid-day tick change gives a new symbol id and an events.csv row.
void test_tick_change() {
  const std::string root = temp_root("tick");
  {
    Rig rig(root);
    rig.feed(trade_msg("BTCUSDT", 1, kDay0 + 1));
    auto changes = rig.registry.update({spec("BTCUSDT", "0.01", "0.001"), spec("ETHUSDT", "0.01", "0.001")}, kDay0 + 5);
    CHECK(changes.size() == 1 && changes[0].kind == SymbolChange::Kind::TickChange);
    rig.writer->symbols_changed(changes, kDay0 + 5);
    rig.feed(trade_msg("BTCUSDT", 2, kDay0 + 10));   // same stream, new specs
    rig.writer->finish();
    CHECK(rig.writer->stats().gaps == 0);            // the sequence continues across the change
  }
  const std::string day = root + "/2026-11-01";
  auto ev = lines(day + "/events.csv");
  CHECK(ev.size() == 2 && ev[1].find(",tick_change,BTCUSDT,\"tick 0.10->0.01, step 0.001->0.001\"") != std::string::npos);
  const std::string meta = slurp(day + "/meta.json");
  CHECK(meta.find(R"({"id": 0, "symbol": "BTCUSDT", "tick_size": "0.10")") != std::string::npos);
  CHECK(meta.find(R"({"id": 2, "symbol": "BTCUSDT", "tick_size": "0.01")") != std::string::npos);
  CHECK(meta.find(R"("replaced_by": 2)") != std::string::npos);
  auto t = records<TradeRecord>(decode(day + "/trades.000.bin.zst"));
  CHECK(t.size() == 2 && t[0].sym == 0 && t[1].sym == 2);
  fs::remove_all(root);
}

// Restarting mid-day keeps the day's ids and appends new symbols after them.
void test_restart_keeps_ids() {
  const std::string root = temp_root("restart");
  {
    Rig rig(root, {spec("BBBUSDT", "0.1", "1"), spec("CCCUSDT", "0.1", "1")});
    rig.feed(trade_msg("CCCUSDT", 1, kDay0 + 1));
    rig.writer->finish();
  }
  {
    Rig rig(root, {spec("AAAUSDT", "0.1", "1"), spec("BBBUSDT", "0.1", "1"), spec("CCCUSDT", "0.1", "1")});
    rig.storage->recover();
    rig.feed(trade_msg("CCCUSDT", 2, kDay0 + 2));
    rig.feed(trade_msg("AAAUSDT", 1, kDay0 + 3));
    rig.writer->finish();
  }
  const std::string day = root + "/2026-11-01";
  const std::string meta = slurp(day + "/meta.json");
  CHECK(meta.find(R"({"id": 0, "symbol": "BBBUSDT")") != std::string::npos);
  CHECK(meta.find(R"({"id": 1, "symbol": "CCCUSDT")") != std::string::npos);
  CHECK(meta.find(R"({"id": 2, "symbol": "AAAUSDT")") != std::string::npos);
  auto s0 = records<TradeRecord>(decode(day + "/trades.000.bin.zst"));
  auto s1 = records<TradeRecord>(decode(day + "/trades.001.bin.zst"));
  CHECK(s0.size() == 1 && s0[0].sym == 1);
  CHECK(s1.size() == 2 && s1[0].sym == 1 && s1[1].sym == 2);
  fs::remove_all(root);
}

void test_parser_edges() {
  SymbolRegistry reg;
  reg.update({spec("BTCUSDT", "0.10", "0.001")}, 0);
  MessageParser p(reg);
  int n = 0;
  auto emit = [&](AnyRecord&) { ++n; };
  auto r = p.parse(R"({"result":null,"id":7})", 1, 0, emit);
  CHECK(r.kind == ParseResult::Kind::Reply && r.reply_id == 7 && n == 0);
  r = p.parse(R"({"error":{"code":2,"msg":"Invalid request"},"id":3})", 1, 0, emit);
  CHECK(r.kind == ParseResult::Kind::Error && r.reply_id == 3);
  r = p.parse(trade_msg("XAUUSDT", 1, kDay0), 1, 0, emit);   // outside the universe
  CHECK(r.kind == ParseResult::Kind::Data && r.skipped == 1 && n == 0);
  std::string bad = trade_msg("BTCUSDT", 1, kDay0);
  bad.replace(bad.find("100.5"), 5, "1.000000001");           // 9 decimals: not exact
  r = p.parse(bad, 1, 0, emit);
  CHECK(r.kind == ParseResult::Kind::Error && n == 0);
  CHECK(r.error.find("1.000000001") == std::string::npos);    // never echoes the price
  r = p.parse("not json", 1, 0, emit);
  CHECK(r.kind == ParseResult::Kind::Error);
}

// segment_minutes: segments close at each slot boundary of exchange time, so finished
// hours can be uploaded during the day; ids and sequences continue across files.
void test_hourly_segments() {
  const std::string root = temp_root("segments");
  const std::string day = root + "/2026-11-01";
  WriterOptions wo;
  wo.segment_minutes = 60;
  {
    Rig rig(root, {spec("BTCUSDT", "0.10", "0.001"), spec("ETHUSDT", "0.01", "0.001")}, wo);
    rig.feed(trade_msg("BTCUSDT", 1, kDay0 + 10));                  // 00:00
    rig.writer->tick(0);
    rig.feed(trade_msg("BTCUSDT", 2, kDay0 + 3599999));             // 00:59:59.999
    rig.writer->tick(1);
    CHECK(fs::exists(day + "/trades.000.bin.zst.part"));
    rig.feed(trade_msg("BTCUSDT", 3, kDay0 + 3600000));             // 01:00: new hour
    CHECK(fs::exists(day + "/trades.000.bin.zst"));                 // hour 0 finished, ready to upload
    rig.feed(trade_msg("BTCUSDT", 4, kDay0 + 3600001));
    rig.writer->tick(3);
    CHECK(fs::exists(day + "/trades.001.bin.zst.part"));            // hour 1 still open
    rig.feed(trade_msg("BTCUSDT", 5, kDay0 + 7200000));             // 02:00
    rig.writer->tick(4);
    CHECK(fs::exists(day + "/trades.001.bin.zst"));
    rig.writer->finish();
    CHECK(rig.writer->stats().gaps == 0);                           // ids continue across files
  }
  auto s0 = records<TradeRecord>(decode(day + "/trades.000.bin.zst"));
  auto s1 = records<TradeRecord>(decode(day + "/trades.001.bin.zst"));
  auto s2 = records<TradeRecord>(decode(day + "/trades.002.bin.zst"));
  CHECK(s0.size() == 2 && s1.size() == 2 && s2.size() == 1);

  // The uploader deletes uploaded segments and leaves "<name>.pruned" markers:
  // a restart must continue after them, never reuse an uploaded number.
  for (const char* f : {"trades.000.bin.zst", "trades.001.bin.zst", "trades.002.bin.zst"}) {
    fs::remove(day + "/" + f);
    std::ofstream(day + "/" + f + ".pruned");
  }
  {
    Rig rig(root, {spec("BTCUSDT", "0.10", "0.001"), spec("ETHUSDT", "0.01", "0.001")}, wo);
    rig.feed(trade_msg("BTCUSDT", 6, kDay0 + 7200001));
    rig.writer->finish();
  }
  CHECK(fs::exists(day + "/trades.003.bin.zst"));
  fs::remove_all(root);
}

}  // namespace

int main() {
  test_decimal();
  test_parser_edges();
  test_sequence_gaps();
  test_day_rollover();
  test_crash_recovery();
  test_rotation_overlap();
  test_tick_change();
  test_restart_keeps_ids();
  test_hourly_segments();
  if (failures) { std::cerr << failures << " check(s) failed\n"; return 1; }
  std::cout << "all recorder tests passed\n";
  return 0;
}
