// UTC calendar helpers. No time zone database: days are plain 86,400,000 ms
// blocks since the epoch, which is what UTC dates are.
#pragma once
#include <cstdint>
#include <string>

namespace acrec {

inline constexpr int64_t kMsPerDay = 86400000;

int64_t day_of_ms(int64_t ms);                 // days since 1970-01-01
std::string day_string(int64_t day);           // "YYYY-MM-DD"
uint32_t day_yyyymmdd(int64_t day);            // 20261101
bool parse_day_string(const std::string& s, int64_t& day);
std::string iso_utc_ms(int64_t ms);            // "2026-11-01T00:00:00.000Z"

// Wall clock: use AstraLib::Time::unixTimestampNS() / unixTimestampMS().
int64_t monotonic_ms();                        // CLOCK_MONOTONIC, for timers

}  // namespace acrec
