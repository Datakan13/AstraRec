#include "recorder/decimal.hpp"

#include <limits>

namespace acrec {

bool parse_fixed(std::string_view s, int digits, int64_t& out) {
  size_t i = 0;
  bool neg = false;
  if (i < s.size() && (s[i] == '-' || s[i] == '+')) neg = s[i++] == '-';
  // Accumulate as a positive value with headroom for INT64_MIN.
  constexpr uint64_t kLimit = static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) + 1;
  uint64_t v = 0;
  int frac = -1;          // decimals consumed so far; -1 = before the point
  bool any_digit = false;
  for (; i < s.size(); ++i) {
    const char c = s[i];
    if (c == '.') {
      if (frac >= 0) return false;
      frac = 0;
      continue;
    }
    if (c < '0' || c > '9') return false;
    any_digit = true;
    if (frac >= digits) {          // beyond the scale: only zeros are exact
      if (c != '0') return false;
      continue;
    }
    if (v > (kLimit - (c - '0')) / 10) return false;
    v = v * 10 + (c - '0');
    if (frac >= 0) ++frac;
  }
  if (!any_digit) return false;
  for (int k = frac < 0 ? 0 : frac; k < digits; ++k) {
    if (v > kLimit / 10) return false;
    v *= 10;
  }
  if (v > kLimit || (!neg && v == kLimit)) return false;
  out = neg ? static_cast<int64_t>(0 - v) : static_cast<int64_t>(v);
  return true;
}

std::string format_fixed(int64_t v, int digits) {
  const bool neg = v < 0;
  uint64_t a = neg ? 0 - static_cast<uint64_t>(v) : static_cast<uint64_t>(v);
  uint64_t scale = 1;
  for (int k = 0; k < digits; ++k) scale *= 10;
  std::string s = std::to_string(a / scale);
  uint64_t frac = a % scale;
  if (frac) {
    std::string f = std::to_string(frac);
    f.insert(0, static_cast<size_t>(digits) - f.size(), '0');
    while (!f.empty() && f.back() == '0') f.pop_back();
    s += '.' + f;
  }
  return neg && a ? "-" + s : s;
}

}  // namespace acrec
