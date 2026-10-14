// The writer's logic, single-threaded and queue-free so it can be driven
// directly by tests. The live pipeline (writer thread + AstraLib ring buffer)
// feeds it.
//
// Sequence checks (log, never repair):
//   depth : pu must equal the previous u for the symbol
//   trades: agg id must increase by exactly 1
//   bbo   : upd id must increase (it may skip)
// During a rotation overlap both connections deliver the same messages; those
// duplicates are summarized in one rotation_overlap row per group instead of
// being reported as gaps.
#pragma once
#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "recorder/records.hpp"
#include "recorder/storage.hpp"
#include "recorder/symbols.hpp"

namespace acrec {

struct WriterOptions {
  int frame_interval_s = 60;     // end a zstd frame + fsync at least this often
  int day_close_grace_s = 120;   // keep yesterday's files open this long past midnight (exchange time)
  int segment_minutes = 0;       // >0: close segments every N minutes of exchange time (0 = one per day)
};

struct WriterStats {
  uint64_t records[kGroupCount] = {};
  uint64_t gaps = 0;             // seq_gap rows
  uint64_t write_errors = 0;
  int64_t flush_ms_max = 0;      // slowest frame end + fsync since take_flush_ms_max()
};

class WriterCore {
 public:
  WriterCore(Storage& storage, SymbolRegistry& registry, WriterOptions opt);

  // One record from a network thread. utc_ms stamps any gap row it causes.
  void record(AnyRecord& r, int64_t utc_ms);

  // Rotation: from begin until end, duplicates between old_conn and new_conn are
  // expected. end writes one rotation_overlap row per group with the range.
  void overlap_begin(uint32_t old_conn, uint32_t new_conn, const std::vector<Group>& groups, int64_t utc_ms);
  void overlap_end(uint32_t old_conn, int64_t utc_ms);

  // exchangeInfo changes: one events.csv row each, new ids (a tick change gets
  // a new id), meta.json rewritten.
  void symbols_changed(const std::vector<SymbolChange>& changes, int64_t utc_ms);

  // Records dropped before reaching the writer (queue full).
  void backpressure(Group g, uint64_t dropped, uint64_t first_E_ms, uint64_t last_E_ms, int64_t utc_ms);

  // Frames and day closing. Call about once a second.
  void tick(int64_t mono_ms);

  // Writes all pending data and closes every file (shutdown).
  void finish();

  Storage& storage() { return storage_; }
  const WriterStats& stats() const { return stats_; }
  // Slowest frame flush (zstd frame end + fsync of every open file) since the last call.
  int64_t take_flush_ms_max() { const int64_t m = stats_.flush_ms_max; stats_.flush_ms_max = 0; return m; }

 private:
  struct Seq {
    uint64_t id = 0;
    uint64_t E_ms = 0;
    bool set = false;
  };
  struct OverlapGroup {
    uint64_t dups = 0, first_E = 0, last_E = 0;
  };
  struct Overlap {
    uint32_t new_conn;
    std::map<Group, OverlapGroup> groups;
  };

  // True if r is a duplicate covered by an active overlap (and counts it).
  bool absorb_overlap_dup(const AnyRecord& r);
  void seq_gap(const AnyRecord& r, const std::string& symbol, const std::string& detail, uint64_t last_E, int64_t utc_ms);

  Storage& storage_;
  SymbolRegistry& registry_;
  WriterOptions opt_;
  WriterStats stats_;
  std::unordered_map<std::string, Seq> seq_[kGroupCount];   // by symbol name: survives tick changes
  std::map<uint32_t, Overlap> overlaps_;                     // by old connection id
  std::shared_ptr<const SymbolRegistry::Snapshot> snap_;
  uint64_t snap_version_ = ~0ull;
  uint64_t max_E_ = 0;
  int64_t last_frame_mono_ = -1;
  int64_t segment_slot_ = -1;   // exchange-time slot of the open segments
};

}  // namespace acrec
