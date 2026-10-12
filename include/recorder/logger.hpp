// Text log: stderr (the systemd journal) plus a size-rotated file.
// Callers log counts, rates and states only, never prices.
#pragma once
#include <cstdio>
#include <mutex>
#include <string>

namespace acrec {

class Logger {
 public:
  // Empty path: stderr only.
  void open(const std::string& path, int max_mb, int files);
  void info(const std::string& msg) { write("INFO", msg); }
  void warn(const std::string& msg) { write("WARN", msg); }
  void error(const std::string& msg) { write("ERROR", msg); }
  ~Logger();

 private:
  void write(const char* level, const std::string& msg);
  void rotate();
  std::mutex mu_;
  std::string path_;
  FILE* f_ = nullptr;
  long max_bytes_ = 10L << 20;
  int files_ = 5;
};

}  // namespace acrec
