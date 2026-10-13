#include "recorder/timeutil.hpp"

#include <cstdio>
#include <ctime>

namespace acrec {
namespace {

// Howard Hinnant's civil-from-days / days-from-civil.
void civil_from_days(int64_t z, int& y, unsigned& m, unsigned& d) {
  z += 719468;
  const int64_t era = (z >= 0 ? z : z - 146096) / 146097;
  const unsigned doe = static_cast<unsigned>(z - era * 146097);
  const unsigned yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
  const unsigned doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
  const unsigned mp = (5 * doy + 2) / 153;
  d = doy - (153 * mp + 2) / 5 + 1;
  m = mp < 10 ? mp + 3 : mp - 9;
  y = static_cast<int>(yoe + era * 400 + (m <= 2));
}

int64_t days_from_civil(int y, unsigned m, unsigned d) {
  y -= m <= 2;
  const int64_t era = (y >= 0 ? y : y - 399) / 400;
  const unsigned yoe = static_cast<unsigned>(y - era * 400);
  const unsigned doy = (153 * (m > 2 ? m - 3 : m + 9) + 2) / 5 + d - 1;
  const unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
  return era * 146097 + static_cast<int64_t>(doe) - 719468;
}

}  // namespace

int64_t day_of_ms(int64_t ms) { return ms >= 0 ? ms / kMsPerDay : -((-ms + kMsPerDay - 1) / kMsPerDay); }

std::string day_string(int64_t day) {
  int y; unsigned m, d;
  civil_from_days(day, y, m, d);
  char buf[32];   // room for any int year, so GCC 15 sees no truncation
  std::snprintf(buf, sizeof buf, "%04d-%02u-%02u", y, m, d);
  return buf;
}

uint32_t day_yyyymmdd(int64_t day) {
  int y; unsigned m, d;
  civil_from_days(day, y, m, d);
  return static_cast<uint32_t>(y) * 10000 + m * 100 + d;
}

bool parse_day_string(const std::string& s, int64_t& day) {
  int y; unsigned m, d;
  if (s.size() != 10 || std::sscanf(s.c_str(), "%4d-%2u-%2u", &y, &m, &d) != 3 || m < 1 || m > 12 || d < 1 || d > 31)
    return false;
  day = days_from_civil(y, m, d);
  return day_string(day) == s;
}

std::string iso_utc_ms(int64_t ms) {
  const int64_t day = day_of_ms(ms), rem = ms - day * kMsPerDay;
  char buf[48];
  std::snprintf(buf, sizeof buf, "%sT%02d:%02d:%02d.%03dZ", day_string(day).c_str(), int(rem / 3600000),
                int(rem / 60000 % 60), int(rem / 1000 % 60), int(rem % 1000));
  return buf;
}

int64_t monotonic_ms() {
  timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return static_cast<int64_t>(ts.tv_sec) * 1000 + ts.tv_nsec / 1000000;
}

}  // namespace acrec
