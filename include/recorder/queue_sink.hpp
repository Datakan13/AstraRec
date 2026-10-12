// Live writer: network threads push into an AstraLib ring buffer without ever
// blocking; one writer thread drains it into WriterCore.
//
// Needs AtomicRingBuffer::tryEnqueue / tryDequeue / approxSize
// (recorder/ASTRACONNECT_SPEC.md, section A1).
#pragma once
#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>

#include <AstraLib/Buffers/atomicRingBuffer.hpp>

#include "recorder/recorder.hpp"
#include "recorder/sink.hpp"
#include "recorder/writer_core.hpp"

namespace acrec {

class QueueSink : public RecordSink {
 public:
  static constexpr size_t kCapacity = size_t{1} << 16;   // ~50 MB of slots; several seconds at peak rates

  explicit QueueSink(WriterCore& writer);
  ~QueueSink() override;

  bool push(const AnyRecord& r) override;
  void post(WriterCommand cmd) override;

  void start();
  // Drains everything queued so far, then finishes the writer (closes all files).
  void stop();
  WriterStatus status();

 private:
  struct Item {
    enum class Kind : uint8_t { Record, Command } kind = Kind::Record;
    uint64_t command_id = 0;
    AnyRecord record;
  };
  void run();
  void handle(Item& it);

  WriterCore& writer_;
  std::unique_ptr<AstraLib::Buffers::AtomicRingBuffer<Item, kCapacity>> ring_;
  std::mutex cmd_mu_;
  std::unordered_map<uint64_t, WriterCommand> commands_;
  uint64_t next_command_ = 1;
  std::atomic<bool> stopping_{false};
  std::thread thread_;
  bool above_high_water_ = false;
  std::mutex status_mu_;
  WriterStatus status_;
};

}  // namespace acrec
