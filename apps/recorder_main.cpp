// recorder: records Binance USD-M futures market data into a sealed forward
// dataset (see recorder/README.md).
//
//   recorder [recorder.json]
//
// Runs until SIGTERM/SIGINT (systemd stop), then closes every file cleanly.
// Prints and logs counts, rates and states only, never prices.
// Exit codes: 0 clean stop, 1 bad config or data folder.
#include <sys/statvfs.h>

#include <atomic>
#include <chrono>
#include <csignal>
#include <filesystem>
#include <thread>

#include <AstraLib/Time/timeNow.hpp>

#include "recorder/astra_transport.hpp"
#include "recorder/config.hpp"
#include "recorder/logger.hpp"
#include "recorder/queue_sink.hpp"
#include "recorder/recorder.hpp"
#include "recorder/storage.hpp"
#include "recorder/timeutil.hpp"
#include "recorder/writer_core.hpp"

#ifndef RECORDER_VERSION
#define RECORDER_VERSION "1.0.0"
#endif

namespace {

std::atomic<bool> g_stop{false};
void on_signal(int) { g_stop.store(true); }

struct RealClock : acrec::Clock {
  int64_t mono_ms() override { return acrec::monotonic_ms(); }
  int64_t utc_ms() override { return AstraLib::Time::unixTimestampMS(); }
};

double disk_free_gb(const std::string& path) {
  struct statvfs s;
  if (statvfs(path.c_str(), &s) != 0) return -1;
  return static_cast<double>(s.f_bavail) * static_cast<double>(s.f_frsize) / 1e9;
}

}  // namespace

int main(int argc, char** argv) {
  const std::string cfg_path = argc > 1 ? argv[1] : "recorder.json";
  acrec::RecorderConfig cfg;
  std::string err;
  if (!acrec::load_recorder_config(cfg_path, cfg, err)) {
    std::fprintf(stderr, "config: %s\n", err.c_str());
    return 1;
  }
  std::error_code ec;
  std::filesystem::create_directories(cfg.root, ec);
  if (ec) {
    std::fprintf(stderr, "cannot create %s: %s\n", cfg.root.c_str(), ec.message().c_str());
    return 1;
  }
  acrec::Logger log;
  log.open(cfg.log_file.empty() ? "" : (std::filesystem::path(cfg.root) / cfg.log_file).string(), cfg.log_max_mb, cfg.log_files);
  log.info(std::string("recorder ") + RECORDER_VERSION + " starting, data in " + cfg.root);

  std::signal(SIGTERM, on_signal);
  std::signal(SIGINT, on_signal);
  std::signal(SIGPIPE, SIG_IGN);

  acrec::SymbolRegistry registry;
  acrec::Storage storage({cfg.root, cfg.zstd_level, RECORDER_VERSION}, registry);
  const auto recovered = storage.recover();
  acrec::WriterCore writer(storage, registry, {cfg.frame_interval_s, cfg.day_close_grace_s, cfg.segment_minutes});
  acrec::QueueSink sink(writer);
  sink.start();
  for (const auto& p : recovered) {
    log.warn("recovered crash file " + p);
    sink.post([p](acrec::WriterCore& w) {
      w.storage().event({AstraLib::Time::unixTimestampMS(), "start", "", "recovered crash file " + p});
    });
  }

  acrec::AstraTransport transport;
  RealClock clock;
  acrec::Recorder rec(cfg, transport, sink, registry, clock, log);

  // Without exchangeInfo there is nothing to record: retry until it loads.
  for (int backoff = 1; !g_stop.load(); backoff = std::min(backoff * 2, cfg.reconnect_max_s)) {
    if (rec.start(err)) break;
    log.error("startup failed: " + err + "; retrying in " + std::to_string(backoff) + "s");
    for (int i = 0; i < backoff * 10 && !g_stop.load(); ++i) std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  const std::string hb_path = (std::filesystem::path(cfg.root) / "heartbeat.json").string();
  int64_t next_hb = 0;
  while (!g_stop.load()) {
    rec.tick();
    const int64_t now = clock.mono_ms();
    if (now >= next_hb) {
      next_hb = now + cfg.heartbeat_interval_s * 1000LL;
      const double free_gb = disk_free_gb(cfg.root);
      if (!acrec::write_file_atomic(hb_path, rec.heartbeat_json(sink.status(), free_gb), err)) log.error("heartbeat: " + err);
      for (const auto& w : rec.warnings(free_gb)) log.warn(w);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(250));
  }

  rec.stop("signal");
  sink.stop();   // drains the queue, ends frames, renames every .part
  log.info("recorder stopped cleanly");
  return 0;
}
