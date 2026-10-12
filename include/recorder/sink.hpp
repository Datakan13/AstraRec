// How records and writer commands reach the writer.
//
// Commands (gap rows, overlap begin/end, symbol changes) travel in the same
// ordered stream as records: a command posted after a connection closed runs
// after every record that connection pushed. That ordering is what keeps a
// rotation's last duplicates inside its overlap window.
#pragma once
#include <functional>
#include <mutex>

#include "recorder/records.hpp"
#include "recorder/writer_core.hpp"

namespace acrec {

using WriterCommand = std::function<void(WriterCore&)>;

class RecordSink {
 public:
  virtual ~RecordSink() = default;
  // Network threads. Never blocks; false = dropped (caller counts it).
  virtual bool push(const AnyRecord& r) = 0;
  // Manager thread. May wait briefly for queue space; never drops.
  virtual void post(WriterCommand cmd) = 0;
};

// Synchronous sink: runs everything immediately under a mutex. Used by tests
// and offline tools; the live recorder uses the queued writer thread instead.
class DirectSink : public RecordSink {
 public:
  explicit DirectSink(WriterCore& w) : w_(w) {}
  bool push(const AnyRecord& r) override {
    std::lock_guard<std::mutex> g(mu_);
    AnyRecord copy = r;
    w_.record(copy, static_cast<int64_t>(r.E_ms));
    return true;
  }
  void post(WriterCommand cmd) override {
    std::lock_guard<std::mutex> g(mu_);
    cmd(w_);
  }
  template <typename F>
  auto with_writer(F&& f) {
    std::lock_guard<std::mutex> g(mu_);
    return f(w_);
  }

 private:
  std::mutex mu_;
  WriterCore& w_;
};

}  // namespace acrec
