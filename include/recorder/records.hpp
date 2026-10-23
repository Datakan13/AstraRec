// On-disk record layouts (little-endian, packed). This file and
// python/acrec_read.py are the format contract; change both together.
//
// Units: prices and quantities are int64 in 1e-8 (kPriceDigits), the funding
// rate in 1e-9. *_ms are exchange milliseconds, recv_ns is local CLOCK_REALTIME.
// `sym` is the symbol id from that day's meta.json.
#pragma once
#include <cstddef>
#include <cstdint>

namespace acrec {

inline constexpr uint16_t kFormatVersion = 1;

enum class Group : uint16_t { Bbo = 1, Trades = 2, Depth = 3, Liq = 4, Mark = 5 };
inline constexpr int kGroupCount = 5;
inline constexpr int kDepthLevels = 20;

inline const char* group_name(Group g) {
  switch (g) {
    case Group::Bbo: return "bbo";
    case Group::Trades: return "trades";
    case Group::Depth: return "depth";
    case Group::Liq: return "liq";
    case Group::Mark: return "mark";
  }
  return "?";
}
inline int group_index(Group g) { return static_cast<int>(g) - 1; }
inline Group group_at(int index) { return static_cast<Group>(index + 1); }

#pragma pack(push, 1)

struct FileHeader {
  char magic[8] = {'A', 'C', 'R', 'E', 'C', 0, 0, 0};
  uint16_t format_version = kFormatVersion;
  uint16_t group_id = 0;
  uint32_t record_size = 0;
  uint32_t yyyymmdd = 0;
  uint8_t reserved[12] = {};
};

struct BboRecord {
  uint64_t E_ms, T_ms, recv_ns;
  uint32_t sym;
  uint64_t upd_id;
  int64_t bid_px, bid_qty, ask_px, ask_qty;
};

struct TradeRecord {
  uint64_t T_ms, E_ms, recv_ns;
  uint32_t sym;
  uint64_t agg_id, first_id, last_id;
  int64_t px, qty;
  uint8_t buyer_is_maker;
};

struct DepthRecord {
  uint64_t E_ms, T_ms, recv_ns;
  uint32_t sym;
  uint64_t U, u, pu;
  uint8_t n_bid, n_ask;
  int64_t bid_px[kDepthLevels], bid_qty[kDepthLevels];
  int64_t ask_px[kDepthLevels], ask_qty[kDepthLevels];
};

struct LiqRecord {
  uint64_t E_ms, T_ms, recv_ns;
  uint32_t sym;
  uint8_t side;      // 0 buy, 1 sell
  int64_t px, avg_px, qty, filled_qty;
  uint8_t status;    // LiqStatus
};

struct MarkRecord {
  uint64_t E_ms, recv_ns;
  uint32_t sym;
  int64_t mark_px, index_px;
  int64_t funding_rate_e9;
  uint64_t next_funding_ms;
};

#pragma pack(pop)

static_assert(sizeof(FileHeader) == 32);
static_assert(sizeof(BboRecord) == 68);
static_assert(sizeof(TradeRecord) == 69);
static_assert(sizeof(DepthRecord) == 694);
static_assert(sizeof(LiqRecord) == 62);
static_assert(sizeof(MarkRecord) == 52);

// Liquidation order status codes (also written to meta.json).
enum class LiqStatus : uint8_t { New = 0, PartiallyFilled = 1, Filled = 2, Canceled = 3, Expired = 4, Unknown = 255 };
inline constexpr const char* kLiqStatusNames[] = {"NEW", "PARTIALLY_FILLED", "FILLED", "CANCELED", "EXPIRED"};

inline size_t record_size(Group g) {
  switch (g) {
    case Group::Bbo: return sizeof(BboRecord);
    case Group::Trades: return sizeof(TradeRecord);
    case Group::Depth: return sizeof(DepthRecord);
    case Group::Liq: return sizeof(LiqRecord);
    case Group::Mark: return sizeof(MarkRecord);
  }
  return 0;
}

// One parsed record of any group, as it travels from a network thread to the
// writer. `sym` holds a global symbol key until the writer maps it to the
// day's symbol id.
struct AnyRecord {
  Group group;
  uint32_t conn_id;
  uint64_t E_ms;          // routes the record to its UTC day
  union {
    BboRecord bbo;
    TradeRecord trade;
    DepthRecord depth;
    LiqRecord liq;
    MarkRecord mark;
  };
  AnyRecord() : group(Group::Bbo), conn_id(0), E_ms(0), depth{} {}
  const void* data() const { return &bbo; }
  uint32_t& sym() {
    switch (group) {
      case Group::Bbo: return bbo.sym;
      case Group::Trades: return trade.sym;
      case Group::Depth: return depth.sym;
      case Group::Liq: return liq.sym;
      case Group::Mark: return mark.sym;
    }
    return bbo.sym;
  }
};

}  // namespace acrec
