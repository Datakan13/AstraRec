#include "recorder/queue_sink.hpp"

#include <chrono>

#include <AstraLib/Time/timeNow.hpp>

#include "recorder/timeutil.hpp"

namespace acrec {

QueueSink::QueueSink(WriterCore& writer)
    : writer_(writer), ring_(std::make_unique<AstraLib::Buffers::AtomicRingBuffer<Item, kCapacity>>()) {}

QueueSink::~QueueSink() { stop(); }

bool QueueSink::push(const AnyRecord& r) {
  Item it;
  it.kind = Item::Kind::Record;
  it.record = r;
  return ring_->tryEnqueue(it);   // full -> false, the caller counts the drop
}

void QueueSink::post(WriterCommand cmd) {
  Item it;
  it.kind = Item::Kind::Command;
  {
    std::lock_guard<std::mutex> g(cmd_mu_);
    it.command_id = next_command_++;
    commands_[it.command_id] = std::move(cmd);
  }
  // Commands are never dropped. Only the manager thread posts, so waiting here
  // never holds up a network thread.
  while (!ring_->tryEnqueue(it)) std::this_thread::sleep_for(std::chrono::milliseconds(1));
}

void QueueSink::handle(Item& it) {
  if (it.kind == Item::Kind::Record) {
    writer_.record(it.record, AstraLib::Time::unixTimestampMS());
    return;
  }
  WriterCommand cmd;
  {
    std::lock_guard<std::mutex> g(cmd_mu_);
    auto f = commands_.find(it.command_id);
    if (f == commands_.end()) return;
    cmd = std::move(f->second);
    commands_.erase(f);
  }
  cmd(writer_);
}

void QueueSink::run() {
  int64_t last_tick = 0;
  Item it;
  for (;;) {
    int n = 0;
    while (n < 4096 && ring_->tryDequeue(it)) {
      handle(it);
      ++n;
    }
    const int64_t now = monotonic_ms();
    if (now - last_tick >= 250) {
      last_tick = now;
      writer_.tick(now);
      const size_t fill = ring_->approxSize();
      // High-water: one row per crossing, re-armed below half full.
      if (!above_high_water_ && fill > kCapacity * 3 / 4) {
        above_high_water_ = true;
        GapRow row;
        row.utc_ms = AstraLib::Time::unixTimestampMS();
        row.group = "all";
        row.kind = "writer_backpressure";
        row.detail = "writer queue above high-water mark (" + std::to_string(fill) + "/" + std::to_string(kCapacity) + ")";
        writer_.storage().gap(row);
      } else if (above_high_water_ && fill < kCapacity / 2) {
        above_high_water_ = false;
      }
      const int64_t today = day_of_ms(AstraLib::Time::unixTimestampMS());
      std::lock_guard<std::mutex> g(status_mu_);
      status_.gaps_today = writer_.storage().gap_rows(today);
      status_.bytes_today = writer_.storage().bytes_written(today);
      status_.queue_fill = fill;
      status_.queue_capacity = kCapacity;
      status_.last_error = writer_.storage().last_error();
    }
    if (n == 0) {
      if (stopping_.load(std::memory_order_acquire) && ring_->approxSize() == 0) break;
      std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
  }
  writer_.finish();
}

void QueueSink::start() { thread_ = std::thread(&QueueSink::run, this); }

void QueueSink::stop() {
  if (!thread_.joinable()) return;
  stopping_.store(true, std::memory_order_release);
  thread_.join();
}

WriterStatus QueueSink::status() {
  std::lock_guard<std::mutex> g(status_mu_);
  return status_;
}

}  // namespace acrec
