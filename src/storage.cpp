#include "recorder/storage.hpp"

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <sstream>

#include <simdjson/simdjson.h>
#include <zstd.h>

#include "recorder/timeutil.hpp"

namespace acrec {
namespace fs = std::filesystem;
namespace {

std::string json_escape(const std::string& s) {
  std::string o;
  for (char c : s) {
    if (c == '"' || c == '\\') { o += '\\'; o += c; }
    else if (static_cast<unsigned char>(c) < 0x20) { char b[8]; std::snprintf(b, sizeof b, "\\u%04x", c); o += b; }
    else o += c;
  }
  return o;
}

std::string csv_field(const std::string& s) {
  if (s.find_first_of(",\"\n\r") == std::string::npos) return s;
  std::string o = "\"";
  for (char c : s) { if (c == '"') o += '"'; o += c; }
  return o + '"';
}

bool write_all(int fd, const char* p, size_t n, std::string& err) {
  while (n) {
    ssize_t w = ::write(fd, p, n);
    if (w < 0) {
      if (errno == EINTR) continue;
      err = std::string("write: ") + std::strerror(errno);
      return false;
    }
    p += w;
    n -= static_cast<size_t>(w);
  }
  return true;
}

void fsync_dir(const std::string& dir) {
  int fd = ::open(dir.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
  if (fd >= 0) { ::fsync(fd); ::close(fd); }
}

}  // namespace

// Writes a file atomically: temp file, fsync, rename, fsync the directory.
bool write_file_atomic(const std::string& path, const std::string& content, std::string& err) {
  const std::string tmp = path + ".tmp";
  int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
  if (fd < 0) { err = "open " + tmp + ": " + std::strerror(errno); return false; }
  bool ok = write_all(fd, content.data(), content.size(), err) && ::fsync(fd) == 0;
  ::close(fd);
  if (!ok) return false;
  if (::rename(tmp.c_str(), path.c_str()) != 0) { err = "rename " + tmp + ": " + std::strerror(errno); return false; }
  fsync_dir(fs::path(path).parent_path().string());
  return true;
}

namespace {

// Segment number in "<group>.NNN.<...>", or -1.
int segment_number(const std::string& name) {
  size_t a = name.find('.');
  if (a == std::string::npos || name.size() < a + 5 || name[a + 4] != '.') return -1;
  int n = 0;
  for (size_t i = a + 1; i < a + 4; ++i) {
    if (name[i] < '0' || name[i] > '9') return -1;
    n = n * 10 + (name[i] - '0');
  }
  return n;
}

// Next free segment number in a day folder: never reuse or append to an existing
// file. Counts .part, .crash and .pruned (uploaded, then deleted locally) names too.
int next_segment(const std::string& dir) {
  std::error_code ec;
  int max_seg = -1;
  for (const auto& f : fs::directory_iterator(dir, ec)) max_seg = std::max(max_seg, segment_number(f.path().filename().string()));
  return max_seg + 1;
}

}  // namespace

// ---------------------------------------------------------------- SegmentWriter

SegmentWriter::~SegmentWriter() {
  if (cctx_) ZSTD_freeCCtx(cctx_);
  if (fd_ >= 0) ::close(fd_);
}

bool SegmentWriter::open(const std::string& final_path, Group group, uint32_t yyyymmdd, int level, std::string& err) {
  final_path_ = final_path;
  part_path_ = final_path + ".part";
  // O_EXCL: never append to or overwrite an existing file.
  fd_ = ::open(part_path_.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0644);
  if (fd_ < 0) { err = "open " + part_path_ + ": " + std::strerror(errno); return false; }
  cctx_ = ZSTD_createCCtx();
  ZSTD_CCtx_setParameter(cctx_, ZSTD_c_compressionLevel, level);
  ZSTD_CCtx_setParameter(cctx_, ZSTD_c_checksumFlag, 1);
  out_.resize(ZSTD_CStreamOutSize());

  FileHeader h;
  h.group_id = static_cast<uint16_t>(group);
  h.record_size = static_cast<uint32_t>(record_size(group));
  h.yyyymmdd = yyyymmdd;
  // The header gets its own frame, so even a file cut short right after opening
  // identifies itself.
  return append(&h, sizeof h, err) && end_frame(err);
}

bool SegmentWriter::drain(int mode, const void* data, size_t n, std::string& err) {
  ZSTD_inBuffer in{data, n, 0};
  for (;;) {
    ZSTD_outBuffer out{out_.data(), out_.size(), 0};
    const size_t left = ZSTD_compressStream2(cctx_, &out, &in, static_cast<ZSTD_EndDirective>(mode));
    if (ZSTD_isError(left)) { err = std::string("zstd: ") + ZSTD_getErrorName(left); return false; }
    if (out.pos && !write_all(fd_, out_.data(), out.pos, err)) return false;
    bytes_out_ += out.pos;
    const bool done = mode == ZSTD_e_continue ? in.pos == in.size : left == 0;
    if (done) return true;
  }
}

bool SegmentWriter::append(const void* data, size_t n, std::string& err) {
  pending_ = true;
  return drain(ZSTD_e_continue, data, n, err);
}

bool SegmentWriter::end_frame(std::string& err) {
  if (!pending_) return true;
  if (!drain(ZSTD_e_end, nullptr, 0, err)) return false;
  pending_ = false;
  if (::fsync(fd_) != 0) { err = "fsync " + part_path_ + ": " + std::strerror(errno); return false; }
  return true;
}

bool SegmentWriter::close(std::string& err) {
  if (fd_ < 0) return true;
  bool ok = end_frame(err);
  ::close(fd_);
  fd_ = -1;
  if (ok && ::rename(part_path_.c_str(), final_path_.c_str()) != 0) {
    err = "rename " + part_path_ + ": " + std::strerror(errno);
    ok = false;
  }
  fsync_dir(fs::path(final_path_).parent_path().string());
  return ok;
}

// ---------------------------------------------------------------- Storage

Storage::Storage(StorageOptions opt, SymbolRegistry& registry) : opt_(std::move(opt)), registry_(registry) {}

Storage::~Storage() { close_all(); }

void Storage::fail(const std::string& err) { last_error_ = err; }

std::vector<std::string> Storage::recover() {
  std::vector<std::string> renamed;
  std::error_code ec;
  if (!fs::exists(opt_.root, ec)) return renamed;
  for (const auto& dir : fs::directory_iterator(opt_.root, ec)) {
    if (!dir.is_directory()) continue;
    for (const auto& f : fs::directory_iterator(dir.path(), ec)) {
      const std::string name = f.path().filename().string();
      const std::string suffix = ".bin.zst.part";
      if (name.size() <= suffix.size() || name.compare(name.size() - suffix.size(), suffix.size(), suffix) != 0) continue;
      // bbo.000.bin.zst.part -> bbo.000.crash.bin.zst
      const std::string target = (dir.path() / (name.substr(0, name.size() - suffix.size()) + ".crash.bin.zst")).string();
      if (::rename(f.path().c_str(), target.c_str()) == 0) renamed.push_back(target);
      else fail("rename " + f.path().string() + ": " + std::strerror(errno));
    }
    fsync_dir(dir.path().string());
  }
  return renamed;
}

Storage::Day& Storage::day(int64_t d) {
  auto it = days_.find(d);
  if (it != days_.end()) return *it->second;
  auto day = std::make_unique<Day>();
  day->day = d;
  day->dir = (fs::path(opt_.root) / day_string(d)).string();
  std::error_code ec;
  fs::create_directories(day->dir, ec);
  if (ec) fail("mkdir " + day->dir + ": " + ec.message());
  day->segment = next_segment(day->dir);
  // A restart or a late record can reopen a day that already has a meta.json:
  // keep its ids so every segment of the day agrees.
  const std::string meta_path = (fs::path(day->dir) / "meta.json").string();
  if (fs::exists(meta_path, ec)) {
    simdjson::dom::parser p;
    simdjson::dom::element doc;
    simdjson::dom::array syms;
    if (p.load(meta_path).get(doc) || doc["symbols"].get_array().get(syms)) {
      fail("cannot read " + meta_path + "; ids for this day restart in a new meta.json");
    } else {
      for (simdjson::dom::element e : syms) {
        SymbolSpec spec;
        std::string_view v;
        int64_t added = 0;
        if (!e["symbol"].get_string().get(v)) spec.symbol = v;
        if (!e["tick_size"].get_string().get(v)) spec.tick_size = v;
        if (!e["step_size"].get_string().get(v)) spec.step_size = v;
        if (!e["quote"].get_string().get(v)) spec.quote = v;
        if (!e["contract_type"].get_string().get(v)) spec.contract_type = v;
        if (!e["status"].get_string().get(v)) spec.status = v;
        if (!e["added_at_ms"].get_int64().get(added)) spec.added_at_ms = added;
        const uint32_t key = registry_.intern(spec);
        day->local[key] = static_cast<uint32_t>(day->keys.size());
        day->keys.push_back(key);
      }
    }
  }
  // Then every symbol trading now that the day doesn't have yet, sorted by name.
  auto snap = registry_.snapshot();
  std::vector<std::pair<std::string, uint32_t>> trading(snap->trading.begin(), snap->trading.end());
  std::sort(trading.begin(), trading.end());
  for (const auto& [name, key] : trading) {
    if (day->local.count(key)) continue;
    day->local[key] = static_cast<uint32_t>(day->keys.size());
    day->keys.push_back(key);
  }
  Day& ref = *day;
  days_[d] = std::move(day);
  write_meta(ref);
  return ref;
}

uint32_t Storage::local_id(Day& d, uint32_t key) {
  auto it = d.local.find(key);
  if (it != d.local.end()) return it->second;
  const uint32_t id = static_cast<uint32_t>(d.keys.size());
  d.local[key] = id;
  d.keys.push_back(key);
  write_meta(d);
  return id;
}

void Storage::write_meta(Day& d) {
  auto snap = registry_.snapshot();
  std::ostringstream o;
  o << "{\n  \"format_version\": " << kFormatVersion << ",\n"
    << "  \"recorder_version\": \"" << json_escape(opt_.recorder_version) << "\",\n"
    << "  \"date\": \"" << day_string(d.day) << "\",\n"
    << "  \"units\": {\"price\": \"1e-8\", \"qty\": \"1e-8\", \"funding_rate\": \"1e-9\", \"recv\": \"ns CLOCK_REALTIME\"},\n"
    << "  \"symbols\": [";
  // A newer version of a symbol in the same day (tick change) supersedes the older id.
  std::unordered_map<std::string, uint32_t> newest;
  for (uint32_t id = 0; id < d.keys.size(); ++id) newest[snap->specs[d.keys[id]].symbol] = id;
  for (uint32_t id = 0; id < d.keys.size(); ++id) {
    const SymbolSpec& s = snap->specs[d.keys[id]];
    o << (id ? ",\n" : "\n") << "    {\"id\": " << id << ", \"symbol\": \"" << json_escape(s.symbol)
      << "\", \"tick_size\": \"" << s.tick_size << "\", \"step_size\": \"" << s.step_size << "\", \"quote\": \""
      << json_escape(s.quote) << "\", \"contract_type\": \"" << json_escape(s.contract_type) << "\", \"status\": \""
      << json_escape(s.status) << "\", \"added_at_ms\": " << s.added_at_ms;
    if (newest[s.symbol] != id) o << ", \"replaced_by\": " << newest[s.symbol];
    o << "}";
  }
  o << "\n  ],\n  \"depth_set\": [";
  for (size_t i = 0; i < depth_set_.size(); ++i) o << (i ? ", " : "") << "\"" << json_escape(depth_set_[i]) << "\"";
  o << "],\n  \"depth_levels\": " << kDepthLevels << ",\n  \"liq_status_codes\": {";
  for (int i = 0; i < 5; ++i) o << (i ? ", " : "") << "\"" << i << "\": \"" << kLiqStatusNames[i] << "\"";
  o << ", \"255\": \"UNKNOWN\"},\n  \"liq_side_codes\": {\"0\": \"BUY\", \"1\": \"SELL\"}\n}\n";
  std::string err;
  if (!write_file_atomic((fs::path(d.dir) / "meta.json").string(), o.str(), err)) fail(err);
}

bool Storage::write(AnyRecord& r) {
  Day& d = day(day_of_ms(static_cast<int64_t>(r.E_ms)));
  r.sym() = local_id(d, r.sym());
  const int gi = group_index(r.group);
  std::string err;
  if (!d.seg[gi]) {
    char name[64];
    std::snprintf(name, sizeof name, "%s.%03d.bin.zst", group_name(r.group), d.segment);
    auto seg = std::make_unique<SegmentWriter>();
    if (!seg->open((fs::path(d.dir) / name).string(), r.group, day_yyyymmdd(d.day), opt_.zstd_level, err)) {
      fail(err);
      return false;
    }
    d.seg[gi] = std::move(seg);
  }
  const uint64_t before = d.seg[gi]->bytes_out();
  const bool ok = d.seg[gi]->append(r.data(), record_size(r.group), err);
  bytes_[d.day] += d.seg[gi]->bytes_out() - before;
  if (!ok) fail(err);
  return ok;
}

FILE* Storage::csv(Day& d, Csv& c, const char* name, const char* header) {
  if (c.f) return c.f;
  const std::string path = (fs::path(d.dir) / name).string();
  const bool fresh = !fs::exists(path);
  c.f = std::fopen(path.c_str(), "a");
  if (!c.f) { fail("open " + path + ": " + std::strerror(errno)); return nullptr; }
  if (fresh) std::fprintf(c.f, "%s\n", header);
  return c.f;
}

void Storage::gap(const GapRow& g) {
  Day& d = day(day_of_ms(g.utc_ms));
  FILE* f = csv(d, d.gaps, "gaps.csv",
                "utc_time,group,connection_id,kind,symbol,detail,last_good_E_ms,first_after_E_ms");
  if (!f) return;
  auto opt = [](const std::optional<uint64_t>& v) { return v ? std::to_string(*v) : std::string(); };
  std::fprintf(f, "%s,%s,%s,%s,%s,%s,%s,%s\n", iso_utc_ms(g.utc_ms).c_str(), csv_field(g.group).c_str(),
               csv_field(g.connection_id).c_str(), csv_field(g.kind).c_str(), csv_field(g.symbol).c_str(),
               csv_field(g.detail).c_str(), opt(g.last_good_E_ms).c_str(), opt(g.first_after_E_ms).c_str());
  std::fflush(f);
  ++d.gap_rows;
  ++gap_count_[d.day];
}

void Storage::event(const EventRow& e) {
  Day& d = day(day_of_ms(e.utc_ms));
  FILE* f = csv(d, d.events, "events.csv", "utc_time,kind,symbol,detail");
  if (!f) return;
  std::fprintf(f, "%s,%s,%s,%s\n", iso_utc_ms(e.utc_ms).c_str(), csv_field(e.kind).c_str(), csv_field(e.symbol).c_str(),
               csv_field(e.detail).c_str());
  std::fflush(f);
}

void Storage::clock(const ClockRow& c) {
  Day& d = day(day_of_ms(c.utc_ms));
  FILE* f = csv(d, d.clock, "clock.csv", "utc_time,local_send_ms,local_recv_ms,server_time_ms,offset_ms,rtt_ms");
  if (!f) return;
  std::fprintf(f, "%s,%lld,%lld,%lld,%.1f,%lld\n", iso_utc_ms(c.utc_ms).c_str(), static_cast<long long>(c.send_ms),
               static_cast<long long>(c.recv_ms), static_cast<long long>(c.server_ms), c.offset_ms,
               static_cast<long long>(c.recv_ms - c.send_ms));
  std::fflush(f);
}

void Storage::stats(int64_t utc_ms, const std::string& header, const std::string& row) {
  Day& d = day(day_of_ms(utc_ms));
  FILE* f = csv(d, d.stats, "stats.csv", header.c_str());
  if (!f) return;
  std::fprintf(f, "%s,%s\n", iso_utc_ms(utc_ms).c_str(), row.c_str());
  std::fflush(f);
}

void Storage::set_depth_set(std::vector<std::string> symbols) {
  depth_set_ = std::move(symbols);
  for (auto& [_, d] : days_) write_meta(*d);
}

void Storage::symbols_changed() {
  auto snap = registry_.snapshot();
  std::vector<std::pair<std::string, uint32_t>> trading(snap->trading.begin(), snap->trading.end());
  std::sort(trading.begin(), trading.end());
  for (auto& [_, d] : days_) {
    for (const auto& [name, key] : trading) {
      if (d->local.count(key)) continue;
      d->local[key] = static_cast<uint32_t>(d->keys.size());
      d->keys.push_back(key);
    }
    write_meta(*d);   // also picks up status changes (delistings)
  }
}

bool Storage::end_frames() {
  bool ok = true;
  for (auto& [_, d] : days_) {
    for (auto& s : d->seg) {
      if (!s) continue;
      std::string err;
      const uint64_t before = s->bytes_out();
      if (!s->end_frame(err)) { fail(err); ok = false; }
      bytes_[d->day] += s->bytes_out() - before;
    }
    for (Csv* c : {&d->gaps, &d->events, &d->clock, &d->stats})
      if (c->f) { std::fflush(c->f); ::fsync(fileno(c->f)); }
  }
  return ok;
}

void Storage::close_day(Day& d) {
  for (auto& s : d.seg) {
    if (!s) continue;
    std::string err;
    const uint64_t before = s->bytes_out();
    if (!s->close(err)) fail(err);
    bytes_[d.day] += s->bytes_out() - before;
    s.reset();
  }
  for (Csv* c : {&d.gaps, &d.events, &d.clock, &d.stats}) {
    if (!c->f) continue;
    std::fflush(c->f);
    ::fsync(fileno(c->f));
    std::fclose(c->f);
    c->f = nullptr;
  }
  write_meta(d);
}

void Storage::close_days_before(int64_t day) {
  for (auto it = days_.begin(); it != days_.end() && it->first < day;) {
    close_day(*it->second);
    it = days_.erase(it);
  }
}

void Storage::rotate_segments() {
  for (auto& [_, d] : days_) {
    bool closed = false;
    for (auto& s : d->seg) {
      if (!s) continue;
      std::string err;
      const uint64_t before = s->bytes_out();
      if (!s->close(err)) fail(err);
      bytes_[d->day] += s->bytes_out() - before;
      s.reset();
      closed = true;
    }
    if (closed) d->segment = next_segment(d->dir);
  }
}

void Storage::close_all() {
  for (auto& [_, d] : days_) close_day(*d);
  days_.clear();
}

uint64_t Storage::bytes_written(int64_t day) const {
  auto it = bytes_.find(day);
  return it == bytes_.end() ? 0 : it->second;
}

uint64_t Storage::gap_rows(int64_t day) const {
  auto it = gap_count_.find(day);
  return it == gap_count_.end() ? 0 : it->second;
}

std::vector<int64_t> Storage::open_days() const {
  std::vector<int64_t> v;
  for (const auto& [d, _] : days_) v.push_back(d);
  return v;
}

}  // namespace acrec
