// Exact decimal <-> fixed-point conversion. Binance sends prices and quantities
// as decimal strings with at most 8 decimals; they are stored as int64 scaled
// by 10^digits, never via double.
#pragma once
#include <cstdint>
#include <string>
#include <string_view>

namespace acrec {

inline constexpr int kPriceDigits = 8;     // prices and quantities: units of 1e-8
inline constexpr int kFundingDigits = 9;   // funding rate: units of 1e-9

// "123.45" -> 12345000000 for digits = 8. Returns false if the string is malformed,
// overflows int64, or has a non-zero digit beyond `digits` decimals (which would
// need rounding).
bool parse_fixed(std::string_view s, int digits, int64_t& out);

// Inverse of parse_fixed: canonical decimal, trailing zeros trimmed ("84523.1", "0").
std::string format_fixed(int64_t v, int digits);

}  // namespace acrec
