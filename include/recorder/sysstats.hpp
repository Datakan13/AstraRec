// Machine and process health from /proc, for stats.csv. Two snapshots are
// turned into per-interval rates and percentages. Linux only; any file that
// can't be read leaves its fields at -1.
#pragma once
#include <cstdint>
#include <string>

namespace acrec {

struct SysSnapshot {
  int64_t mono_ms = 0;
  // /proc/stat "cpu" line, in clock ticks
  uint64_t cpu_total = 0, cpu_idle = 0, cpu_iowait = 0, cpu_steal = 0;
  bool cpu_ok = false;
  // /proc/self/stat utime + stime, in clock ticks; threads
  uint64_t proc_ticks = 0;
  int proc_threads = -1;
  bool proc_ok = false;
  double proc_rss_mb = -1;
  double mem_avail_mb = -1, swap_used_mb = -1;
  double load1 = -1;
  uint64_t net_rx_bytes = 0;          // all interfaces except lo
  bool net_ok = false;
  uint64_t tcp_out_segs = 0, tcp_retrans_segs = 0;
  bool tcp_ok = false;
};

struct SysRates {
  double sys_cpu_busy_pct = -1, sys_iowait_pct = -1, sys_steal_pct = -1;
  double proc_cpu_pct = -1;           // of one core
  double proc_rss_mb = -1;
  int proc_threads = -1;
  double load1 = -1, mem_avail_mb = -1, swap_used_mb = -1;
  double net_rx_mbps = -1;
  double tcp_retrans_pct = -1;        // retransmitted / sent segments
};

class SysSampler {
 public:
  explicit SysSampler(std::string proc_root = "/proc") : root_(std::move(proc_root)) {}
  SysSnapshot sample(int64_t mono_ms) const;
  static SysRates rates(const SysSnapshot& before, const SysSnapshot& after);

 private:
  std::string root_;
};

}  // namespace acrec
