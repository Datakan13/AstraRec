#include "recorder/sysstats.hpp"

#include <unistd.h>

#include <fstream>
#include <sstream>
#include <vector>

namespace acrec {
namespace {

bool read_file(const std::string& path, std::string& out) {
  std::ifstream in(path);
  if (!in) return false;
  std::stringstream s;
  s << in.rdbuf();
  out = s.str();
  return true;
}

// Value of "Key:   1234 kB" in /proc/meminfo or /proc/self/status, in MB.
double kb_field_mb(const std::string& text, const std::string& key) {
  const size_t p = text.find("\n" + key + ":");
  const size_t start = p == std::string::npos ? (text.rfind(key + ":", 0) == 0 ? 0 : std::string::npos) : p + 1;
  if (start == std::string::npos) return -1;
  std::istringstream in(text.substr(start + key.size() + 1));
  double kb = -1;
  in >> kb;
  return kb < 0 ? -1 : kb / 1024.0;
}

double pct(uint64_t part, uint64_t whole) { return whole ? 100.0 * static_cast<double>(part) / static_cast<double>(whole) : -1; }

}  // namespace

SysSnapshot SysSampler::sample(int64_t mono_ms) const {
  SysSnapshot s;
  s.mono_ms = mono_ms;
  std::string t;

  if (read_file(root_ + "/stat", t)) {
    std::istringstream in(t);
    std::string cpu;
    uint64_t v[10] = {};
    in >> cpu;
    int n = 0;
    while (n < 10 && in >> v[n]) ++n;
    if (cpu == "cpu" && n >= 8) {
      // user nice system idle iowait irq softirq steal [guest guest_nice: already in user]
      for (int i = 0; i < 8; ++i) s.cpu_total += v[i];
      s.cpu_idle = v[3] + v[4];
      s.cpu_iowait = v[4];
      s.cpu_steal = v[7];
      s.cpu_ok = true;
    }
  }
  if (read_file(root_ + "/self/stat", t)) {
    // Fields after the ")" that closes the command name: state is field 3.
    const size_t close = t.rfind(')');
    if (close != std::string::npos) {
      std::istringstream in(t.substr(close + 2));
      std::vector<std::string> f;
      std::string x;
      while (in >> x) f.push_back(x);
      // f[0] = field 3 (state); utime = field 14 -> f[11], stime = 15 -> f[12], threads = 20 -> f[17]
      if (f.size() > 17) {
        s.proc_ticks = std::stoull(f[11]) + std::stoull(f[12]);
        s.proc_threads = std::stoi(f[17]);
        s.proc_ok = true;
      }
    }
  }
  if (read_file(root_ + "/self/status", t)) s.proc_rss_mb = kb_field_mb(t, "VmRSS");
  if (read_file(root_ + "/meminfo", t)) {
    s.mem_avail_mb = kb_field_mb(t, "MemAvailable");
    const double total = kb_field_mb(t, "SwapTotal"), free = kb_field_mb(t, "SwapFree");
    if (total >= 0 && free >= 0) s.swap_used_mb = total - free;
  }
  if (read_file(root_ + "/loadavg", t)) {
    std::istringstream in(t);
    in >> s.load1;
  }
  if (read_file(root_ + "/net/dev", t)) {
    std::istringstream in(t);
    std::string line;
    while (std::getline(in, line)) {
      const size_t colon = line.find(':');
      if (colon == std::string::npos) continue;
      std::string iface = line.substr(0, colon);
      iface.erase(0, iface.find_first_not_of(' '));
      if (iface == "lo") continue;
      std::istringstream f(line.substr(colon + 1));
      uint64_t rx = 0;
      if (f >> rx) { s.net_rx_bytes += rx; s.net_ok = true; }
    }
  }
  if (read_file(root_ + "/net/snmp", t)) {
    // Two "Tcp:" lines: names, then values.
    std::istringstream in(t);
    std::string line, names;
    while (std::getline(in, line)) {
      if (line.rfind("Tcp:", 0) != 0) continue;
      if (names.empty()) { names = line; continue; }
      std::istringstream n(names), v(line);
      std::string name, val;
      while (n >> name && v >> val) {
        if (name == "OutSegs") s.tcp_out_segs = std::stoull(val);
        if (name == "RetransSegs") s.tcp_retrans_segs = std::stoull(val);
      }
      s.tcp_ok = true;
      break;
    }
  }
  return s;
}

SysRates SysSampler::rates(const SysSnapshot& a, const SysSnapshot& b) {
  SysRates r;
  const double secs = (b.mono_ms - a.mono_ms) / 1000.0;
  if (a.cpu_ok && b.cpu_ok && b.cpu_total > a.cpu_total) {
    const uint64_t total = b.cpu_total - a.cpu_total;
    r.sys_cpu_busy_pct = 100.0 - pct(b.cpu_idle - a.cpu_idle, total);
    r.sys_iowait_pct = pct(b.cpu_iowait - a.cpu_iowait, total);
    r.sys_steal_pct = pct(b.cpu_steal - a.cpu_steal, total);
  }
  if (a.proc_ok && b.proc_ok && secs > 0) {
    const double hz = static_cast<double>(sysconf(_SC_CLK_TCK));
    r.proc_cpu_pct = 100.0 * static_cast<double>(b.proc_ticks - a.proc_ticks) / hz / secs;
  }
  r.proc_rss_mb = b.proc_rss_mb;
  r.proc_threads = b.proc_threads;
  r.load1 = b.load1;
  r.mem_avail_mb = b.mem_avail_mb;
  r.swap_used_mb = b.swap_used_mb;
  if (a.net_ok && b.net_ok && secs > 0 && b.net_rx_bytes >= a.net_rx_bytes)
    r.net_rx_mbps = static_cast<double>(b.net_rx_bytes - a.net_rx_bytes) * 8 / 1e6 / secs;
  if (a.tcp_ok && b.tcp_ok && b.tcp_out_segs > a.tcp_out_segs)
    r.tcp_retrans_pct = pct(b.tcp_retrans_segs - a.tcp_retrans_segs, b.tcp_out_segs - a.tcp_out_segs);
  return r;
}

}  // namespace acrec
