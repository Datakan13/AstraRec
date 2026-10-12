#include "recorder/logger.hpp"

#include <cstdio>

#include <AstraLib/Time/timeNow.hpp>

#include "recorder/timeutil.hpp"

namespace acrec {

void Logger::open(const std::string& path, int max_mb, int files) {
  std::lock_guard<std::mutex> g(mu_);
  path_ = path;
  max_bytes_ = static_cast<long>(max_mb) << 20;
  files_ = files < 1 ? 1 : files;
  if (!path_.empty()) f_ = std::fopen(path_.c_str(), "a");
}

Logger::~Logger() {
  if (f_) std::fclose(f_);
}

void Logger::rotate() {
  std::fclose(f_);
  for (int i = files_ - 1; i >= 1; --i) {
    const std::string from = i == 1 ? path_ : path_ + "." + std::to_string(i - 1);
    std::rename(from.c_str(), (path_ + "." + std::to_string(i)).c_str());
  }
  f_ = std::fopen(path_.c_str(), "a");
}

void Logger::write(const char* level, const std::string& msg) {
  const std::string line = iso_utc_ms(AstraLib::Time::unixTimestampMS()) + " " + level + " " + msg + "\n";
  std::lock_guard<std::mutex> g(mu_);
  std::fputs(line.c_str(), stderr);
  if (!f_) return;
  std::fputs(line.c_str(), f_);
  std::fflush(f_);
  if (std::ftell(f_) > max_bytes_) rotate();
}

}  // namespace acrec
