#include "recorder/writer_core.hpp"

#include "recorder/timeutil.hpp"

namespace acrec {

WriterCore::WriterCore(Storage& storage, SymbolRegistry& registry, WriterOptions opt)
    : storage_(storage), registry_(registry), opt_(opt) {}

bool WriterCore::absorb_overlap_dup(const AnyRecord& r) {
  for (auto& [old_conn, ov] : overlaps_) {
    if (r.conn_id != old_conn && r.conn_id != ov.new_conn) continue;
    auto it = ov.groups.find(r.group);
    if (it == ov.groups.end()) continue;
    OverlapGroup& g = it->second;
    if (!g.dups++) g.first_E = r.E_ms;
    g.last_E = r.E_ms;
    return true;
  }
  return false;
}

void WriterCore::seq_gap(const AnyRecord& r, const std::string& symbol, const std::string& detail, uint64_t last_E,
                         int64_t utc_ms) {
  GapRow row;
  row.utc_ms = utc_ms;
  row.group = group_name(r.group);
  row.connection_id = std::to_string(r.conn_id);
  row.kind = "seq_gap";
  row.symbol = symbol;
  row.detail = detail;
  row.last_good_E_ms = last_E;
  row.first_after_E_ms = r.E_ms;
  storage_.gap(row);
  ++stats_.gaps;
}

void WriterCore::record(AnyRecord& r, int64_t utc_ms) {
  if (r.group == Group::Bbo || r.group == Group::Trades || r.group == Group::Depth) {
    if (registry_.version() != snap_version_) {
      snap_version_ = registry_.version();
      snap_ = registry_.snapshot();
    }
    const std::string& symbol = snap_->specs[r.sym()].symbol;
    Seq& s = seq_[group_index(r.group)][symbol];
    if (s.set) {
      std::string problem;
      bool dup = false;
      uint64_t id = 0;
      switch (r.group) {
        case Group::Trades:
          id = r.trade.agg_id;
          if (id <= s.id) dup = true;
          else if (id != s.id + 1)
            problem = "agg_id " + std::to_string(s.id) + " -> " + std::to_string(id) + " (" + std::to_string(id - s.id - 1) + " missing)";
          break;
        case Group::Depth:
          id = r.depth.u;
          if (id <= s.id) dup = true;
          else if (r.depth.pu != s.id)
            problem = "pu " + std::to_string(r.depth.pu) + " != previous u " + std::to_string(s.id);
          break;
        default:
          id = r.bbo.upd_id;
          if (id <= s.id) dup = true;
          break;
      }
      if (dup) {
        // Duplicates are still written; analysis dedupes by id.
        if (!absorb_overlap_dup(r))
          seq_gap(r, symbol, "id " + std::to_string(id) + " <= previous " + std::to_string(s.id) + " (duplicate or out of order)",
                  s.E_ms, utc_ms);
      } else {
        if (!problem.empty()) seq_gap(r, symbol, problem, s.E_ms, utc_ms);
        s.id = id;
        s.E_ms = r.E_ms;
      }
    } else {
      s.set = true;
      s.id = r.group == Group::Trades ? r.trade.agg_id : r.group == Group::Depth ? r.depth.u : r.bbo.upd_id;
      s.E_ms = r.E_ms;
    }
  }

  if (r.E_ms > max_E_) max_E_ = r.E_ms;
  if (storage_.write(r)) ++stats_.records[group_index(r.group)];
  else ++stats_.write_errors;
}

void WriterCore::overlap_begin(uint32_t old_conn, uint32_t new_conn, const std::vector<Group>& groups, int64_t) {
  Overlap ov;
  ov.new_conn = new_conn;
  for (Group g : groups) ov.groups[g] = {};
  overlaps_[old_conn] = ov;
}

void WriterCore::overlap_end(uint32_t old_conn, int64_t utc_ms) {
  auto it = overlaps_.find(old_conn);
  if (it == overlaps_.end()) return;
  for (const auto& [g, og] : it->second.groups) {
    GapRow row;
    row.utc_ms = utc_ms;
    row.group = group_name(g);
    row.connection_id = std::to_string(old_conn) + "->" + std::to_string(it->second.new_conn);
    row.kind = "rotation_overlap";
    row.detail = std::to_string(og.dups) + " duplicate messages written from both connections";
    if (og.dups) {
      row.last_good_E_ms = og.first_E;   // for rotation_overlap: start of the overlapping range
      row.first_after_E_ms = og.last_E;  // and its end
    }
    storage_.gap(row);
  }
  overlaps_.erase(it);
}

void WriterCore::symbols_changed(const std::vector<SymbolChange>& changes, int64_t utc_ms) {
  for (const SymbolChange& c : changes) storage_.event({utc_ms, change_kind_name(c.kind), c.symbol, c.detail});
  storage_.symbols_changed();
}

void WriterCore::backpressure(Group g, uint64_t dropped, uint64_t first_E_ms, uint64_t last_E_ms, int64_t utc_ms) {
  GapRow row;
  row.utc_ms = utc_ms;
  row.group = group_name(g);
  row.kind = "writer_backpressure";
  row.detail = std::to_string(dropped) + " records dropped: writer queue full";
  row.last_good_E_ms = first_E_ms;
  row.first_after_E_ms = last_E_ms;
  storage_.gap(row);
}

void WriterCore::tick(int64_t mono_ms) {
  if (last_frame_mono_ < 0) last_frame_mono_ = mono_ms;
  if (mono_ms - last_frame_mono_ >= opt_.frame_interval_s * 1000LL) {
    storage_.end_frames();
    last_frame_mono_ = mono_ms;
  }
  // Exchange time drives day closing, so a quiet feed never closes a day early.
  if (max_E_) {
    const int64_t current = day_of_ms(static_cast<int64_t>(max_E_) - opt_.day_close_grace_s * 1000LL);
    storage_.close_days_before(current);
  }
}

void WriterCore::finish() {
  storage_.end_frames();
  storage_.close_all();
}

}  // namespace acrec
