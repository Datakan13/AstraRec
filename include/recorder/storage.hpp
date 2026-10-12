// Day folders, zstd segments, meta.json and the CSV logs. Single-threaded:
// only the writer thread touches Storage.
//
//   <root>/YYYY-MM-DD/meta.json
//   <root>/YYYY-MM-DD/<group>.NNN.bin.zst      (.part while open, .crash after a crash)
//   <root>/YYYY-MM-DD/{gaps,events,clock}.csv
#pragma once
#include <cstdint>
#include <cstdio>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "recorder/records.hpp"
#include "recorder/symbols.hpp"

struct ZSTD_CCtx_s;

namespace acrec {

// temp file + fsync + rename + directory fsync: readers never see half a file.
bool write_file_atomic(const std::string& path, const std::string& content, std::string& err);

struct GapRow {
  int64_t utc_ms = 0;
  std::string group, connection_id, kind, symbol, detail;
  std::optional<uint64_t> last_good_E_ms, first_after_E_ms;
};
struct EventRow {
  int64_t utc_ms = 0;
  std::string kind, symbol, detail;
};
struct ClockRow {
  int64_t utc_ms = 0, send_ms = 0, recv_ms = 0, server_ms = 0;
  double offset_ms = 0;
};

// One compressed file. Every frame is independently decodable and checksummed,
// so after a crash everything up to the last completed frame survives.
class SegmentWriter {
 public:
  ~SegmentWriter();
  bool open(const std::string& final_path, Group group, uint32_t yyyymmdd, int level, std::string& err);
  bool append(const void* data, size_t n, std::string& err);
  bool end_frame(std::string& err);   // ends the frame, writes it out, fsyncs
  bool close(std::string& err);       // end_frame + rename .part -> final
  uint64_t bytes_out() const { return bytes_out_; }
  bool pending() const { return pending_; }

 private:
  bool drain(int mode, const void* data, size_t n, std::string& err);
  std::string final_path_, part_path_;
  int fd_ = -1;
  ZSTD_CCtx_s* cctx_ = nullptr;
  std::vector<char> out_;
  uint64_t bytes_out_ = 0;
  bool pending_ = false;
};

struct StorageOptions {
  std::string root;
  int zstd_level = 3;
  std::string recorder_version = "0.0.0";
};

class Storage {
 public:
  Storage(StorageOptions opt, SymbolRegistry& registry);
  ~Storage();

  // Renames leftover *.part files to *.crash.bin.zst. Returns the new paths.
  std::vector<std::string> recover();

  // Writes a record whose sym holds a global key; it is replaced by the day's id.
  bool write(AnyRecord& r);

  void gap(const GapRow& row);
  void event(const EventRow& row);
  void clock(const ClockRow& row);

  void set_depth_set(std::vector<std::string> symbols);
  // Gives ids to symbols that started trading (listings, tick changes) in every
  // open day and rewrites their meta.json.
  void symbols_changed();

  bool end_frames();                   // all open segments + CSV fsync
  void close_days_before(int64_t day); // finishes those day folders
  void close_all();

  uint64_t bytes_written(int64_t day) const;
  uint64_t gap_rows(int64_t day) const;
  const std::string& last_error() const { return last_error_; }
  std::vector<int64_t> open_days() const;

 private:
  struct Csv {
    FILE* f = nullptr;
  };
  struct Day {
    int64_t day = 0;
    std::string dir;
    int segment = -1;
    std::unique_ptr<SegmentWriter> seg[kGroupCount];
    std::unordered_map<uint32_t, uint32_t> local;   // global key -> id
    std::vector<uint32_t> keys;                      // id -> global key
    Csv gaps, events, clock;
    uint64_t gap_rows = 0;
  };

  Day& day(int64_t d);
  uint32_t local_id(Day& d, uint32_t key);
  void write_meta(Day& d);
  void close_day(Day& d);
  FILE* csv(Day& d, Csv& c, const char* name, const char* header);
  void fail(const std::string& err);

  StorageOptions opt_;
  SymbolRegistry& registry_;
  std::map<int64_t, std::unique_ptr<Day>> days_;
  std::map<int64_t, uint64_t> bytes_;      // compressed bytes per day, kept after close
  std::map<int64_t, uint64_t> gap_count_;
  std::vector<std::string> depth_set_;
  std::string last_error_;
};

}  // namespace acrec
